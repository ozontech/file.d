package input_file

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
)

const (
	InfoReportInterval = time.Second * 1
)

type job struct {
	id   int
	file *os.File

	isDone          bool
	isRunning       bool
	wasOffsetLoaded bool

	stream string

	mu *sync.Mutex
}

type jobProvider struct {
	config *Config
	done   *sync.WaitGroup

	jobs          map[string]*job
	jobsDoneCount *atomic.Int32

	activeJobs chan *job
	nextJob    chan *job

	offsets        offsets
	offsetsContent []string
	offsetsMu      *sync.Mutex

	eventsCommitted int

	jobMu    *sync.Mutex
	commitMu *sync.Mutex

	shouldStop bool

	jobsAddedLog []string
}

type offsets map[string]map[string]int64

func NewJobProvider(config *Config, done *sync.WaitGroup) *jobProvider {
	jp := &jobProvider{
		config: config,

		jobs:          make(map[string]*job, config.ChanLength),
		jobsDoneCount: atomic.NewInt32(0),
		done:          done,

		activeJobs: make(chan *job, config.ChanLength),
		nextJob:    make(chan *job, config.ChanLength),

		offsets:        make(map[string]map[string]int64),
		offsetsContent: make([]string, 0, config.ChanLength),
		offsetsMu:      &sync.Mutex{},

		jobMu:    &sync.Mutex{},
		commitMu: &sync.Mutex{},

		jobsAddedLog: make([]string, 0, 16),
	}

	if jp.config.persistenceMode == PersistenceModeAsync {
		go jp.saveOffsetsCyclic(time.Millisecond * 100)
	} else if jp.config.persistenceMode == PersistenceModeTimer {
		go jp.saveOffsetsCyclic(time.Second * 5)
	}

	go jp.reportStats()

	return jp
}

func (jp *jobProvider) start() {
	jp.loadOffsets()
	jp.loadJobs()
	go jp.process()
}

func (jp *jobProvider) stop() {
	jp.shouldStop = true
	// unblock process function to allow goroutine to exit
	jp.nextJob <- nil
}

func (jp *jobProvider) Commit(event *pipeline.Event) {
	jp.commitMu.Lock()
	defer jp.commitMu.Unlock()

	stream, hasStream := jp.offsets[event.Stream]
	if !hasStream {
		logger.Panicf("no offsets for stream %s", event.Stream)
	}

	currentOffset := stream[event.SubStream]
	if currentOffset >= event.Offset {
		logger.Panicf("commit offset(%d) for stream %q/%q less than current(%d)", event.Offset, event.Stream, event.SubStream, currentOffset)
	}

	stream[event.SubStream] = event.Offset

	jp.eventsCommitted++

	if jp.config.persistenceMode == PersistenceModeSync {
		jp.saveOffsets()
	}
}

func (jp *jobProvider) process() {
	logger.Infof("job provider started with %q persistence mode", jp.config.PersistenceMode)
	for {
		job := <-jp.activeJobs
		if jp.shouldStop {
			return
		}

		job.mu.Lock()
		if job.isDone || job.isRunning {
			logger.Panicf("why run? it's at end or already running")
		}
		job.isRunning = true
		job.mu.Unlock()

		jp.nextJob <- job
	}
}

func (jp *jobProvider) addJob(filename string, loadOffset bool) {
	if filename == jp.config.OffsetsFilename || filename == jp.config.offsetsTmpFilename {
		logger.Fatalf("you can't place offset file %s in watching dir %s", jp.config.OffsetsFilename, jp.config.WatchingDir)
	}

	jp.jobMu.Lock()
	defer jp.jobMu.Unlock()

	existingJob, has := jp.jobs[filename]

	if has {
		jp.resumeJob(existingJob)
		return
	}

	job := jp.makeJob(filename, loadOffset)
	jp.jobs[filename] = job

	jp.jobsAddedLog = append(jp.jobsAddedLog, filename)

	jp.resumeJob(job)
}

func (jp *jobProvider) resumeJob(job *job) {
	job.mu.Lock()
	if !job.isDone {
		job.mu.Unlock()
		return
	}

	job.isDone = false

	jp.done.Add(1)

	v := jp.jobsDoneCount.Dec()
	if v < 0 {
		logger.Panicf("done jobs counter less than zero")
	}
	job.mu.Unlock()

	jp.activeJobs <- job
}

func (jp *jobProvider) releaseJob(job *job, isAtEnd bool) {
	job.mu.Lock()
	if !job.isRunning || job.isDone {
		logger.Panicf("job isn't running, why release?")
	}

	job.isRunning = false
	job.isDone = isAtEnd
	v := 0
	if isAtEnd {
		v = int(jp.jobsDoneCount.Inc())
		if v > len(jp.jobs) {
			logger.Panicf("done jobs counter more than job count")
		}
		jp.done.Done()
	}
	job.mu.Unlock()

	if !isAtEnd {
		jp.activeJobs <- job
	}
}

func (jp *jobProvider) makeJob(filename string, loadOffset bool) *job {
	if !loadOffset && jp.offsets[filename] != nil {
		logger.Panicf("offsets already created for job %s", filename)
	}

	jp.jobsDoneCount.Inc()
	_, has := jp.offsets[filename]

	job := &job{
		id:              len(jp.jobs),
		file:            nil,
		isDone:          true,
		wasOffsetLoaded: !loadOffset,
		stream:          filename,
		mu:              &sync.Mutex{},
	}

	file, err := os.Open(filename)
	if err != nil {
		// file hasn't been opened, but we'll try later
		logger.Errorf("can't open file %s: ", filename, err.Error())
		return job
	}
	job.file = file

	if loadOffset && has {
		subOffsets := jp.offsets[filename]
		if len(subOffsets) == 0 {
			fmt.Println()
			logger.Panicf("no sub steams in stream %q", filename)
		}
		minOffset := int64(math.MaxInt64)
		for _, offset := range subOffsets {
			if offset < minOffset {
				minOffset = offset
			}
		}

		offset, err := file.Seek(minOffset, io.SeekStart)
		if err != nil || offset != minOffset {
			logger.Panicf("can't seek file %s", filename)
		}
	} else {
		jp.offsets[filename] = make(map[string]int64)
	}

	job.wasOffsetLoaded = true

	return job
}

func (jp *jobProvider) saveOffsetsCyclic(duration time.Duration) {
	lastCommitted := 0
	for {
		time.Sleep(duration)
		if jp.shouldStop {
			return
		}
		// by having separate var won't get race condition
		eventsCommitted := jp.eventsCommitted
		if lastCommitted != eventsCommitted {
			lastCommitted = eventsCommitted
			jp.saveOffsets()
		}
	}
}

func (jp *jobProvider) saveOffsets() {
	jp.offsetsMu.Lock()
	defer jp.offsetsMu.Unlock()

	tmpFilename := jp.config.offsetsTmpFilename
	file, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		logger.Errorf("can't open temp offsets file %s, %s", jp.config.OffsetsFilename, err.Error())
		return
	}

	jp.offsetsContent = jp.offsetsContent[:0]
	for stream, subStreams := range jp.offsets {
		if len(subStreams) == 0 {
			continue
		}

		jp.offsetsContent = append(jp.offsetsContent, "- stream: ")
		jp.offsetsContent = append(jp.offsetsContent, stream)
		jp.offsetsContent = append(jp.offsetsContent, "\n")
		for subStream, offset := range subStreams {
			jp.offsetsContent = append(jp.offsetsContent, "  ")
			jp.offsetsContent = append(jp.offsetsContent, subStream)
			jp.offsetsContent = append(jp.offsetsContent, ": ")
			jp.offsetsContent = append(jp.offsetsContent, strconv.FormatInt(offset, 10))
			jp.offsetsContent = append(jp.offsetsContent, "\n")
		}
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Errorf("can't seek offsets file %s, %s", tmpFilename, err.Error())
	}
	err = file.Truncate(0)
	if err != nil {
		logger.Errorf("can't truncate offsets file %s, %s", tmpFilename, err.Error())
	}

	for _, s := range jp.offsetsContent {
		_, err = file.WriteString(s)
		if err != nil {
			logger.Errorf("can't write offsets file %s, %s", tmpFilename, err.Error())
		}
	}

	err = file.Sync()
	if err != nil {
		logger.Errorf("can't sync offsets file %s, %s", tmpFilename, err.Error())
	}

	err = file.Close()
	if err != nil {
		logger.Errorf("can't close offsets file: %s, %s", tmpFilename, err.Error())
	}

	err = os.Rename(tmpFilename, jp.config.OffsetsFilename)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to actual: %s", err.Error())
	}
}

func (jp *jobProvider) loadOffsets() {
	filename := jp.config.OffsetsFilename
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return
	}

	if info.IsDir() {
		logger.Fatalf("Can't load offsets, offsets file %s is dir")
	}

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Panicf("Can't load offsets %s: ", err.Error())
	}

	jp.offsets = parseOffsets(string(content))
}

func (jp *jobProvider) loadJobs() {
	files, err := filepath.Glob(filepath.Join(jp.config.WatchingDir, "*"))
	if err != nil {
		logger.Fatalf("Can't get file list from watching dir %s:", err.Error())
	}
	for _, filename := range files {
		stat, err := os.Stat(filename)
		if err != nil {
			logger.Warnf("can't open file %s", filename)
			continue
		}
		if stat.IsDir() {
			continue
		}

		jp.addJob(filename, true)
	}
	logger.Infof("jobs loaded, count=%d", len(files))
}

func (jp *jobProvider) reportStats() {
	for {
		time.Sleep(InfoReportInterval)
		if jp.shouldStop {
			return
		}

		added := len(jp.jobsAddedLog)

		if added == 0 {
			continue
		}

		if added <= 3 {
			for _, job := range jp.jobsAddedLog {
				logger.Infof("found new file for watching %s ", job)
			}
		}
		if added > 3 {
			logger.Infof("found %d new files for watching", added)
		}

		jp.jobsAddedLog = jp.jobsAddedLog[:0]
	}
}

func parseOffsets(data string) offsets {
	result := make(offsets)
	for len(data) != 0 {
		linePos := strings.IndexByte(data, '\n')
		line := data[0:linePos]
		data = data[linePos+1:]

		if linePos < 11 || line[0:10] != "- stream: " {
			logger.Panicf("wrong offsets format, stream name expected, but found: %s", line)
		}

		stream := line[10:linePos]
		_, has := result[stream]
		if has {
			logger.Panicf("wrong offsets format, duplicate stream %s", stream)
		}
		result[stream] = make(map[string]int64)

		for len(data) != 0 && data[0] != '-' {
			linePos = strings.IndexByte(data, '\n')
			line = data[0:linePos]
			data = data[linePos+1:]

			if linePos < 3 || line[0:2] != "  " {
				logger.Panicf("wrong offsets format, no leading whitespaces on line: %q", line)
			}

			separatorPos := strings.IndexByte(line, ':')
			if separatorPos < 0 {
				logger.Panicf("wrong offsets format, no separator on line: %q", line)
			}
			subStream := line[2:separatorPos]
			if len(subStream) == 0 {
				logger.Panicf("wrong offsets format, empty sub stream in stream %s on line: %q", stream, line)
			}
			_, has = result[stream][subStream]
			if has {
				logger.Panicf("wrong offsets format, duplicate sub stream %q in stream %q", subStream, stream)
			}
			offsetStr := line[separatorPos+2:]
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				logger.Panicf("wrong offsets format, can't parse offset: %q, %q", offsetStr, err.Error())
			}

			result[stream][subStream] = offset
		}
	}

	return result
}
