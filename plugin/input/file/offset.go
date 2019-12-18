package file

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
)

type offsetDB struct {
	offsetsFile string
	tmpFile     string
	mu          *sync.Mutex
	saves       *atomic.Int64
	buf         []string // content buffer to avoid allocation every offsets saving
	jobs        []*job   // temporary list of jobs
}

type inodeOffsets struct {
	streams  streamsOffsets
	filename string
}
type streamsOffsets map[pipeline.StreamName]int64
type inodesOffsets map[inode]*inodeOffsets

func newOffsetDB(offsetsFile string, tmpFile string) *offsetDB {
	return &offsetDB{
		offsetsFile: offsetsFile,
		tmpFile:     tmpFile,
		mu:          &sync.Mutex{},
		saves:       &atomic.Int64{},
		buf:         make([]string, 0, 65536),
		jobs:        make([]*job, 0, 0),
	}
}

func (o *offsetDB) load() inodesOffsets {
	info, err := os.Stat(o.offsetsFile)
	if os.IsNotExist(err) {
		return make(inodesOffsets)
	}

	if info.IsDir() {
		logger.Fatalf("can't load offsets, offsets file %s is dir")
	}

	content, err := ioutil.ReadFile(o.offsetsFile)
	if err != nil {
		logger.Panicf("can't load offsets %s: ", err.Error())
	}

	return o.collapse(o.parse(string(content)))
}

// collapseOffsets all streams are in one file, so we should seek file to
// min offset to make sure logs from all streams will be delivered at least once
func (o *offsetDB) collapse(inodeOffsets inodesOffsets) inodesOffsets {
	for _, inode := range inodeOffsets {
		minOffset := int64(math.MaxInt64)
		for _, offset := range inode.streams {
			if offset < minOffset {
				minOffset = offset
			}
		}

		for key := range inode.streams {
			inode.streams[key] = minOffset
		}
	}

	return inodeOffsets
}

func (o *offsetDB) parse(content string) inodesOffsets {
	offsets := make(inodesOffsets)
	inodeStr := "- file: "
	inodeStrLen := len(inodeStr)
	source := content
	for len(content) != 0 {
		linePos := strings.IndexByte(content, '\n')
		line := content[0:linePos]
		if linePos < 0 {
			logger.Panicf("wrong offsets format, no new line: %q", source)
		}
		content = content[linePos+1:]

		if linePos < inodeStrLen+1 || line[0:inodeStrLen] != inodeStr {
			logger.Panicf("wrong offsets format, file expected: %s", source)
		}

		fullFile := line[inodeStrLen:linePos]
		pos := strings.IndexByte(fullFile, ' ')
		if pos < 0 {
			logger.Panicf("wrong offsets format, inode expected: %s", source)
		}

		sysInode, err := strconv.ParseUint(line[inodeStrLen:inodeStrLen+pos], 10, 64)
		inode := inode(sysInode)
		if err != nil {
			logger.Panicf("wrong offsets format, can't get inode: %s", err.Error())
		}

		_, has := offsets[inode]
		if has {
			logger.Panicf("wrong offsets format, duplicate inode %d, %s", inode, source)
		}
		offsets[inode] = &inodeOffsets{
			streams:  make(streamsOffsets),
			filename: fullFile[pos+1:],
		}

		for len(content) != 0 && content[0] != '-' {
			linePos = strings.IndexByte(content, '\n')
			if linePos < 0 {
				logger.Panicf("wrong offsets format, no new line %s", source)
			}
			line = content[0:linePos]
			if linePos < 3 || line[0:2] != "  " {
				logger.Panicf("wrong offsets format, no leading whitespaces %q", line)
			}
			content = content[linePos+1:]

			pos = strings.IndexByte(line, ':')
			if pos < 0 {
				logger.Panicf("wrong offsets format, no separator %q", line)
			}
			stream := pipeline.StreamName(line[2:pos])
			if len(stream) == 0 {
				logger.Panicf("wrong offsets format, empty stream in inode %d, %s", inode, source)
			}
			_, has = offsets[inode].streams[stream]
			if has {
				logger.Panicf("wrong offsets format, duplicate stream %q in inode %d, %s", stream, inode, source)
			}

			offsetStr := line[pos+2:]
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				logger.Panicf("wrong offsets format, can't parse offset: %q, %q", offsetStr, err.Error())
			}

			offsets[inode].streams[stream] = offset
		}
	}

	return offsets
}

func (o *offsetDB) save(jobs map[inode]*job, mu *sync.RWMutex) {
	o.saves.Inc()

	o.mu.Lock()
	defer o.mu.Unlock()

	// snapshot jobs to avoid long locks
	o.jobs = o.jobs[:0]
	mu.RLock()
	for _, job := range jobs {
		o.jobs = append(o.jobs, job)
	}
	mu.RUnlock()

	file, err := os.OpenFile(o.tmpFile, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		logger.Errorf("can't open temp offsets file %s, %s", o.tmpFile, err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			logger.Errorf("can't close offsets file: %s, %s", o.tmpFile, err.Error())
		}
	}()

	o.buf = o.buf[:0]
	for _, job := range o.jobs {
		job.mu.Lock()
		if len(job.offsets) == 0 {
			job.mu.Unlock()
			continue
		}

		o.buf = append(o.buf, "- file: ")
		o.buf = append(o.buf, fmt.Sprintf("%d", uint64(job.inode)))
		o.buf = append(o.buf, " ")
		o.buf = append(o.buf, job.filename)
		o.buf = append(o.buf, "\n")
		for stream, offset := range job.offsets {
			o.buf = append(o.buf, "  ")
			o.buf = append(o.buf, string(stream))
			o.buf = append(o.buf, ": ")
			o.buf = append(o.buf, fmt.Sprintf("%d", uint64(offset)))
			o.buf = append(o.buf, "\n")
		}
		job.mu.Unlock()
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Errorf("can't seek offsets file %s, %s", o.tmpFile, err.Error())
	}
	err = file.Truncate(0)
	if err != nil {
		logger.Errorf("can't truncate offsets file %s, %s", o.tmpFile, err.Error())
	}

	for _, s := range o.buf {
		_, err = file.WriteString(s)
		if err != nil {
			logger.Errorf("can't write offsets file %s, %s", o.tmpFile, err.Error())
		}
	}

	err = file.Sync()
	if err != nil {
		logger.Errorf("can't sync offsets file %s, %s", o.tmpFile, err.Error())
	}

	err = os.Rename(o.tmpFile, o.offsetsFile)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to actual: %s", err.Error())
	}
}
