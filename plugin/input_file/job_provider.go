package input_file

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
)

const (
	InfoReportInterval = time.Second * 1
)

type job struct {
	file *os.File

	isDone    bool
	isRunning bool

	sourceId uint64

	mu *sync.Mutex
}

type jobProvider struct {
	config *Config
	done   *sync.WaitGroup

	sourceIdToFile sourceIdToFile

	jobs     map[uint64]*job
	jobsDone *atomic.Int32

	activeJobs chan *job
	nextJob    chan *job

	offsets    offsets
	offsetsMu  *sync.Mutex
	offsetsBuf []string // content buffer to avoid allocation every offsets dump

	eventsCommitted int

	jobMu *sync.Mutex
	//commitMu *sync.Mutex

	shouldStop bool

	jobsAddedLog []string
}

type offsets map[uint64]map[string]int64
type sourceIdToFile map[uint64]string

func NewJobProvider(config *Config, done *sync.WaitGroup) *jobProvider {
	jp := &jobProvider{
		config: config,

		sourceIdToFile: make(sourceIdToFile),

		jobs:     make(map[uint64]*job, config.ChanLength),
		jobsDone: atomic.NewInt32(0),
		done:     done,

		activeJobs: make(chan *job, config.ChanLength),
		nextJob:    make(chan *job, config.ChanLength),

		offsets:    make(offsets),
		offsetsBuf: make([]string, 0, config.ChanLength),
		offsetsMu:  &sync.Mutex{},

		jobMu: &sync.Mutex{},
		//commitMu: &sync.Mutex{},

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

func (jp *jobProvider) Commit(event *pipeline.Event) {
	jp.offsetsMu.Lock()
	defer jp.offsetsMu.Unlock()

	sourceId, has := jp.offsets[event.SourceId]
	if !has {
		logger.Panicf("no offsets for source %s", event.SourceId)
	}

	offset := sourceId[event.Stream]
	if offset >= event.Offset {
		logger.Panicf("commit offset(%d) for source %d:%s less than current(%d)", event.Offset, event.SourceId, event.Stream, offset)
	}

	sourceId[event.Stream] = event.Offset

	jp.eventsCommitted++

	if jp.config.persistenceMode == PersistenceModeSync {
		jp.saveOffsets()
	}
}

func (jp *jobProvider) addJob(filename string, loadOffset bool) {
	if filename == jp.config.OffsetsFilename || filename == jp.config.offsetsTmpFilename {
		logger.Fatalf("you can't place offset file %s in watching dir %s", jp.config.OffsetsFilename, jp.config.WatchingDir)
	}

	file, err := os.Open(filename)
	// file may be renamed or deleted while we get from create notification to this moment, so only warn about this
	if err != nil {
		logger.Warnf("file was already moved from creation place %s", filename)
		return
	}
	stat, err := file.Stat()
	if err != nil {
		logger.Errorf("can't instantiate job, can't stat file %s", filename)
		return
	}
	sysStat := stat.Sys().(*syscall.Stat_t)
	inode := sysStat.Ino

	jp.jobMu.Lock()
	defer jp.jobMu.Unlock()

	jp.sourceIdToFile[inode] = filename

	existingJob, has := jp.jobs[inode]
	if has {
		jp.resumeJob(existingJob)
		return
	}

	job := jp.instantiateJob(file, inode, loadOffset)
	if job == nil {
		return
	}

	jp.jobs[inode] = job
	jp.jobsAddedLog = append(jp.jobsAddedLog, filename)

	jp.jobsDone.Inc()
	jp.resumeJob(job)
}

func (jp *jobProvider) instantiateJob(file *os.File, inode uint64, loadOffset bool) *job {
	jp.offsetsMu.Lock()
	defer jp.offsetsMu.Unlock()

	offsets, has := jp.offsets[inode]
	if !loadOffset && has {
		logger.Panicf("can't instantiate job, offsets already created for job %d:%s", file.Name())
	}

	job := &job{
		sourceId: inode,
		file:     file,
		isDone:   true,
		mu:       &sync.Mutex{},
	}

	if loadOffset && has {
		if len(offsets) == 0 {
			logger.Panicf("can't instantiate job, no streams in source %d:%q", inode, file.Name())
		}

		// as all streams are in one file, so we should find min offset
		// to make sure all logs from all streams will be delivered at least once
		minOffset := int64(math.MaxInt64)
		for _, offset := range offsets {
			if offset < minOffset {
				minOffset = offset
			}
		}

		offset, err := file.Seek(minOffset, io.SeekStart)
		if err != nil || offset != minOffset {
			logger.Panicf("can't make job, can't seek file %d:%s", inode, file.Name())
		}
	} else {
		jp.offsets[inode] = make(map[string]int64)
	}

	return job
}

func (jp *jobProvider) resumeJob(job *job) {
	job.mu.Lock()
	if !job.isDone {
		job.mu.Unlock()
		return
	}

	job.isDone = false

	jp.done.Add(1)

	v := jp.jobsDone.Dec()
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
		v = int(jp.jobsDone.Inc())
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

func (jp *jobProvider) resetJob(job *job) {
	_, err := job.file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Fatalf("job reset error, file % s seek error: %s", job.file.Name(), err.Error())
	}

	jp.offsetsMu.Lock()
	defer jp.offsetsMu.Unlock()

	offsets, has := jp.offsets[job.sourceId]
	if !has {
		logger.Panicf("job reset error, no offsets for source %s found", job.sourceId)
	}

	for k := range offsets {
		offsets[k] = 0
	}
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
	defer func() {
		err := file.Close()
		if err != nil {
			logger.Errorf("can't close offsets file: %s, %s", tmpFilename, err.Error())
		}
	}()

	jp.offsetsBuf = jp.offsetsBuf[:0]
	for sourceId, streams := range jp.offsets {
		if len(streams) == 0 {
			continue
		}

		file, has := jp.sourceIdToFile[sourceId]
		if !has {
			logger.Panicf("no file name for source id %d", sourceId)
		}

		jp.offsetsBuf = append(jp.offsetsBuf, "- file: ")
		jp.offsetsBuf = append(jp.offsetsBuf, strconv.FormatUint(sourceId, 10))
		jp.offsetsBuf = append(jp.offsetsBuf, " ")
		jp.offsetsBuf = append(jp.offsetsBuf, file)
		jp.offsetsBuf = append(jp.offsetsBuf, "\n")
		for stream, offset := range streams {
			jp.offsetsBuf = append(jp.offsetsBuf, "  ")
			jp.offsetsBuf = append(jp.offsetsBuf, stream)
			jp.offsetsBuf = append(jp.offsetsBuf, ": ")
			jp.offsetsBuf = append(jp.offsetsBuf, strconv.FormatInt(offset, 10))
			jp.offsetsBuf = append(jp.offsetsBuf, "\n")
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

	for _, s := range jp.offsetsBuf {
		_, err = file.WriteString(s)
		if err != nil {
			logger.Errorf("can't write offsets file %s, %s", tmpFilename, err.Error())
		}
	}

	err = file.Sync()
	if err != nil {
		logger.Errorf("can't sync offsets file %s, %s", tmpFilename, err.Error())
	}

	err = os.Rename(tmpFilename, jp.config.OffsetsFilename)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to actual: %s", err.Error())
	}
}

func (jp *jobProvider) loadOffsets() {
	jp.offsetsMu.Lock()
	defer jp.offsetsMu.Unlock()

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

	jp.offsets, jp.sourceIdToFile = parseOffsets(string(content))
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

func parseOffsets(data string) (offsets, sourceIdToFile) {
	offsets := make(offsets)
	sourceIdToFile := make(sourceIdToFile)
	inodeStr := "- file: "
	inodeStrLen := len(inodeStr)
	for len(data) != 0 {
		linePos := strings.IndexByte(data, '\n')
		line := data[0:linePos]
		data = data[linePos+1:]

		if linePos < inodeStrLen+1 || line[0:inodeStrLen] != inodeStr {
			logger.Panicf("wrong offsets format, file expected, but found: %s", line)
		}

		fullFile := line[inodeStrLen:linePos]
		pos := strings.IndexByte(fullFile, ' ')
		if pos < 0 {
			logger.Panicf("wrong offsets format, inode expected, but found: %s", line)
		}

		filename := line[inodeStrLen+pos+1 : linePos]
		inode, err := strconv.ParseUint(line[inodeStrLen:inodeStrLen+pos], 10, 64)
		if err != nil {
			logger.Panicf("wrong offsets format, can't get inode: %s", err.Error())
		}

		_, has := offsets[inode]
		if has {
			logger.Panicf("wrong offsets format, duplicate inode %s", inode)
		}
		offsets[inode] = make(map[string]int64)
		sourceIdToFile[inode] = filename

		for len(data) != 0 && data[0] != '-' {
			linePos = strings.IndexByte(data, '\n')
			line = data[0:linePos]
			data = data[linePos+1:]

			if linePos < 3 || line[0:2] != "  " {
				logger.Panicf("wrong offsets format, no leading whitespaces on line: %q", line)
			}

			pos = strings.IndexByte(line, ':')
			if pos < 0 {
				logger.Panicf("wrong offsets format, no separator on line: %q", line)
			}
			stream := line[2:pos]
			if len(stream) == 0 {
				logger.Panicf("wrong offsets format, empty stream in inode %s on line: %q", inode, line)
			}
			_, has = offsets[inode][stream]
			if has {
				logger.Panicf("wrong offsets format, duplicate stream %q in inode %q", stream, inode)
			}

			offsetStr := line[pos+2:]
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				logger.Panicf("wrong offsets format, can't parse offset: %q, %q", offsetStr, err.Error())
			}

			offsets[inode][stream] = offset
		}
	}

	return offsets, sourceIdToFile
}
