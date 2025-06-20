package file

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type offsetDB struct {
	curOffsetsFile string
	tmpOffsetsFile string
	savesTotal     *atomic.Int64
	jobsSnapshot   []*Job
	buf            []byte
	mu             *sync.Mutex
	reloadCh       chan bool

	// offset metrics
	invalidStreamsCountMetric prometheus.Counter
}

type inodeOffsets struct {
	filename string
	sourceID pipeline.SourceID
	streams  map[pipeline.StreamName]int64
}

type (
	streamsOffsets map[pipeline.StreamName]int64
	fpOffsets      map[pipeline.SourceID]*inodeOffsets
)

type offsetDbMetricCollection struct {
	invalidStreamsCountMetric prometheus.Counter
}

func newOffsetDbMetricCollection(invalidStreamsCountMetric prometheus.Counter) *offsetDbMetricCollection {
	return &offsetDbMetricCollection{
		invalidStreamsCountMetric: invalidStreamsCountMetric,
	}
}

func newOffsetDB(curOffsetsFile string, tmpOffsetsFile string, metrics *offsetDbMetricCollection) *offsetDB {
	return &offsetDB{
		curOffsetsFile:            curOffsetsFile,
		tmpOffsetsFile:            tmpOffsetsFile,
		mu:                        &sync.Mutex{},
		savesTotal:                &atomic.Int64{},
		buf:                       make([]byte, 0, 65536),
		jobsSnapshot:              make([]*Job, 0),
		reloadCh:                  make(chan bool),
		invalidStreamsCountMetric: metrics.invalidStreamsCountMetric,
	}
}

func (o *offsetDB) load() (fpOffsets, error) {
	logger.Infof("loading offsets: %s", o.curOffsetsFile)

	info, err := os.Stat(o.curOffsetsFile)
	if os.IsNotExist(err) {
		return make(fpOffsets), nil
	}

	if info.IsDir() {
		logger.Fatalf("can't load offsets, file %s is dir")
	}

	content, err := os.ReadFile(o.curOffsetsFile)
	if err != nil {
		logger.Panicf("can't read offset file: %s", err.Error())
	}

	offsets, err := o.parse(string(content))
	if err != nil {
		return make(fpOffsets), fmt.Errorf("can't load offsets file: %w", err)
	}

	return offsets, nil
}

func (o *offsetDB) parse(content string) (fpOffsets, error) {
	offsets := make(fpOffsets)
	for content != "" {
		one, err := o.parseOne(content, offsets)
		if err != nil {
			return make(fpOffsets), fmt.Errorf("can't parseOne: %w", err)
		}
		content = one
	}

	return offsets, nil
}

func (o *offsetDB) parseOne(content string, offsets fpOffsets) (string, error) {
	filename := ""
	inodeStr := ""
	sourceIDStr := ""
	var err error

	filename, content, err = o.parseLine(content, "- file: ")
	if err != nil {
		return "", fmt.Errorf("can't parse file: %w", err)
	}
	inodeStr, content, err = o.parseLine(content, "  inode: ")
	if err != nil {
		return "", fmt.Errorf("can't parse inode: %w", err)
	}
	sourceIDStr, content, err = o.parseLine(content, "  source_id: ")
	if err != nil {
		return "", fmt.Errorf("can't parse source_id: %w", err)
	}

	sysInode, err := strconv.ParseUint(inodeStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("wrong offsets format, can't parse inode: %s: %w", inodeStr, err)
	}
	inode := inodeID(sysInode)

	fpVal, err := strconv.ParseUint(sourceIDStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("wrong offsets format, can't parse source id: %s: %w", sourceIDStr, err)
	}
	fp := pipeline.SourceID(fpVal)

	_, has := offsets[fp]
	if has {
		return "", fmt.Errorf("wrong offsets format, duplicate inode %d", inode)
	}

	offsets[fp] = &inodeOffsets{
		streams:  make(map[pipeline.StreamName]int64),
		filename: filename,
		sourceID: fp,
	}

	return o.parseStreams(content, offsets[fp].streams)
}

func (o *offsetDB) parseStreams(content string, streams streamsOffsets) (string, error) {
	_, content, err := o.parseLine(content, "  streams:")
	if err != nil {
		return "", fmt.Errorf("can''t parse line: %w", err)
	}

	for content != "" && content[0] != '-' {
		linePos := strings.IndexByte(content, '\n')
		if linePos < 0 {
			return "", fmt.Errorf("wrong offsets format, no new line %s", content)
		}
		line := content[0:linePos]
		if linePos < 5 || line[0:4] != "    " {
			return "", fmt.Errorf("wrong offsets format, no leading whitespaces %q", line)
		}
		content = content[linePos+1:]

		pos := strings.IndexByte(line, ':')
		if pos < 0 {
			return "", fmt.Errorf("wrong offsets format, no separator %q", line)
		}

		if line[pos+1] == ':' {
			logger.Warnf("invalid stream name: %s, skipping stream", line[4:pos+2])
			continue
		}

		stream := pipeline.StreamName(line[4:pos])
		if len(stream) == 0 {
			return "", fmt.Errorf("wrong offsets format, empty stream, %s", content)
		}

		_, has := streams[stream]
		if has {
			return "", fmt.Errorf("wrong offsets format, duplicate stream %q", stream)
		}

		offsetStr := line[pos+2:]
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("wrong offsets format, can't parse offset: %q: %w", offsetStr, err)
		}

		streams[stream] = offset
	}

	return content, nil
}

func (o *offsetDB) parseLine(content string, start string) (string, string, error) {
	l := len(start)

	linePos := strings.IndexByte(content, '\n')
	if linePos < 0 {
		return "", "", fmt.Errorf("wrong offsets format, no nl: %q", content)
	}
	line := content[0:linePos]

	content = content[linePos+1:]
	if linePos < l || line[0:l] != start {
		return "", "", fmt.Errorf("wrong offsets file format expected=%q, got=%q", start, line[0:l])
	}

	return line[l:], content, nil
}

func (o *offsetDB) save(jobs map[pipeline.SourceID]*Job, mu *sync.RWMutex) {
	o.savesTotal.Inc()

	o.mu.Lock()
	defer o.mu.Unlock()

	// snapshot jobs to avoid long locks
	snapshot := o.snapshotJobs(mu, jobs)

	tmpWithRandom := append(make([]byte, 0), o.tmpOffsetsFile...)
	tmpWithRandom = append(tmpWithRandom, '.')
	tmpWithRandom = strconv.AppendUint(tmpWithRandom, rand.Uint64(), 8)

	file, err := os.OpenFile(string(tmpWithRandom), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
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
		for _, strOff := range job.offsets {
			if strOff.Stream[len(strOff.Stream)-1] == ':' {
				o.invalidStreamsCountMetric.Inc()
				continue
			}
			o.buf = append(o.buf, "    "...)
			o.buf = append(o.buf, string(strOff.Stream)...)
			o.buf = append(o.buf, ": "...)
			o.buf = strconv.AppendUint(o.buf, uint64(strOff.Offset), 10)
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

	err = os.Rename(string(tmpWithRandom), o.curOffsetsFile)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to current: %s", err.Error())
	}
}

func (o *offsetDB) snapshotJobs(mu *sync.RWMutex, jobs map[pipeline.SourceID]*Job) []*Job {
	o.jobsSnapshot = o.jobsSnapshot[:0]
	mu.RLock()
	for _, job := range jobs {
		o.jobsSnapshot = append(o.jobsSnapshot, job)
	}
	mu.RUnlock()

	return o.jobsSnapshot
}
