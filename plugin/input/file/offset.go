package file

import (
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"go.uber.org/atomic"
)

type offsetDB struct {
	curOffsetsFile string
	tmpOffsetsFile string
	savesTotal     *atomic.Int64
	jobsSnapshot   []*job
	buf            []byte
	mu             *sync.Mutex
}

type inodeOffsets struct {
	filename string
	sourceID pipeline.SourceID
	streams  streamsOffsets
}

type streamsOffsets map[pipeline.StreamName]int64
type fpOffsets map[pipeline.SourceID]*inodeOffsets

func newOffsetDB(curOffsetsFile string, tmpOffsetsFile string) *offsetDB {
	return &offsetDB{
		curOffsetsFile: curOffsetsFile,
		tmpOffsetsFile: tmpOffsetsFile,
		mu:             &sync.Mutex{},
		savesTotal:     &atomic.Int64{},
		buf:            make([]byte, 0, 65536),
		jobsSnapshot:   make([]*job, 0, 0),
	}
}

func (o *offsetDB) load() fpOffsets {
	info, err := os.Stat(o.curOffsetsFile)
	if os.IsNotExist(err) {
		return make(fpOffsets)
	}

	if info.IsDir() {
		logger.Fatalf("can't load offsets, file %s is dir")
	}

	content, err := ioutil.ReadFile(o.curOffsetsFile)
	if err != nil {
		logger.Panicf("can't load offsets %s: ", err.Error())
	}

	return o.collapse(o.parse(string(content)))
}

// collapse all streams are in one file, so we should seek file to
// min offset to make sure logs from all streams will be delivered at-least-once
func (o *offsetDB) collapse(inodeOffsets fpOffsets) fpOffsets {
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

func (o *offsetDB) parse(content string) fpOffsets {
	offsets := make(fpOffsets)
	for len(content) != 0 {
		content = o.parseOne(content, offsets)
	}

	return offsets
}

func (o *offsetDB) parseOne(content string, offsets fpOffsets) string {
	filename := ""
	inodeStr := ""
	sourceIDStr := ""
	filename, content = o.parseLine(content, "- file: ")
	inodeStr, content = o.parseLine(content, "  inode: ")
	sourceIDStr, content = o.parseLine(content, "  source_id: ")

	sysInode, err := strconv.ParseUint(inodeStr, 10, 64)
	if err != nil {
		logger.Panicf("wrong offsets format, can't parse inode: %s", err.Error())
	}
	inode := inode(sysInode)

	fpVal, err := strconv.ParseUint(sourceIDStr, 10, 64)
	if err != nil {
		logger.Panicf("wrong offsets format, can't parse source id: %s", err.Error())
	}
	fp := pipeline.SourceID(fpVal)

	_, has := offsets[fp]
	if has {
		logger.Panicf("wrong offsets format, duplicate inode %d", inode)
	}

	offsets[fp] = &inodeOffsets{
		streams:  make(map[pipeline.StreamName]int64),
		filename: filename,
		sourceID: fp,
	}

	return o.parseStreams(content, offsets[fp].streams)
}

func (o *offsetDB) parseStreams(content string, streams streamsOffsets) string {
	_, content = o.parseLine(content, "  streams:")
	for len(content) != 0 && content[0] != '-' {
		linePos := strings.IndexByte(content, '\n')
		if linePos < 0 {
			logger.Panicf("wrong offsets format, no new line %s", content)
		}
		line := content[0:linePos]
		if linePos < 5 || line[0:4] != "    " {
			logger.Panicf("wrong offsets format, no leading whitespaces %q", line)
		}
		content = content[linePos+1:]

		pos := strings.IndexByte(line, ':')
		if pos < 0 {
			logger.Panicf("wrong offsets format, no separator %q", line)
		}
		stream := pipeline.StreamName(line[4:pos])
		if len(stream) == 0 {
			logger.Panicf("wrong offsets format, empty stream %d, %s", content)
		}

		_, has := streams[stream]
		if has {
			logger.Panicf("wrong offsets format, duplicate stream %q", stream)
		}

		offsetStr := line[pos+2:]
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			logger.Panicf("wrong offsets format, can't parse offset: %q, %q", offsetStr, err.Error())
		}

		streams[stream] = offset
	}

	return content
}

func (o *offsetDB) parseLine(content string, start string) (string, string) {
	l := len(start)

	linePos := strings.IndexByte(content, '\n')
	line := content[0:linePos]
	if linePos < 0 {
		logger.Panicf("wrong offsets format, no nl: %q", content)
	}
	content = content[linePos+1:]
	if linePos < l || line[0:l] != start {
		logger.Panicf("wrong offsets file format expected=%q, got=%q", start, line[0:l])
	}

	return line[l:], content
}

func (o *offsetDB) save(jobs map[pipeline.SourceID]*job, mu *sync.RWMutex) {
	o.savesTotal.Inc()

	o.mu.Lock()
	defer o.mu.Unlock()

	// snapshot jobs to avoid long locks
	snapshot := o.snapshotJobs(mu, jobs)

	file, err := os.OpenFile(o.tmpOffsetsFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		logger.Errorf("can't open temp offsets file %s, %s", o.tmpOffsetsFile, err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			logger.Errorf("can't close offsets file: %s, %s", o.tmpOffsetsFile, err.Error())
		}
	}()

	o.buf = o.buf[:0]
	for _, job := range snapshot {
		job.mu.Lock()
		if len(job.offsets) == 0 {
			job.mu.Unlock()
			continue
		}

		o.buf = append(o.buf, "- file: "...)
		o.buf = append(o.buf, job.filename...)
		o.buf = append(o.buf, '\n')

		o.buf = append(o.buf, "  inode: "...)
		o.buf = strconv.AppendUint(o.buf, uint64(job.inode), 10)
		o.buf = append(o.buf, '\n')

		o.buf = append(o.buf, "  source_id: "...)
		o.buf = strconv.AppendUint(o.buf, uint64(job.sourceID), 10)
		o.buf = append(o.buf, '\n')

		o.buf = append(o.buf, "  streams:\n"...)
		for stream, offset := range job.offsets {
			o.buf = append(o.buf, "    "...)
			o.buf = append(o.buf, string(stream)...)
			o.buf = append(o.buf, ": "...)
			o.buf = strconv.AppendUint(o.buf, uint64(offset), 10)
			o.buf = append(o.buf, '\n')
		}
		job.mu.Unlock()
	}

	_, err = file.Write(o.buf)
	if err != nil {
		logger.Errorf("can't write offsets file %s, %s", o.tmpOffsetsFile, err.Error())
	}

	err = file.Sync()
	if err != nil {
		logger.Errorf("can't sync offsets file %s, %s", o.tmpOffsetsFile, err.Error())
	}

	err = os.Rename(o.tmpOffsetsFile, o.curOffsetsFile)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to current: %s", err.Error())
	}
}

func (o *offsetDB) snapshotJobs(mu *sync.RWMutex, jobs map[pipeline.SourceID]*job) []*job {
	o.jobsSnapshot = o.jobsSnapshot[:0]
	mu.RLock()
	for _, job := range jobs {
		o.jobsSnapshot = append(o.jobsSnapshot, job)
	}
	mu.RUnlock()

	return o.jobsSnapshot
}
