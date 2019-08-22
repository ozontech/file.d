package file

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
	infoReportInterval  = time.Second * 1
	maintenanceInterval = time.Second * 5
)

type job struct {
	file *os.File

	isDone    bool
	isRunning bool

	sourceId   uint64
	offsets    offsetsByStream
	latestSize int64

	mu *sync.Mutex
}

type jobProvider struct {
	config *Config
	done   *sync.WaitGroup

	jobs     map[uint64]*job
	jobsDone *atomic.Int32
	jobsMu   *sync.Mutex
	jobsChan chan *job
	jobsLog  []string
	jobsList []*job // temporary list of jobs

	offsetsBuf      []string // content buffer to avoid allocation every offsets dump
	loadedOffsets   offsetsAll
	fileByInode     fileByInode
	shouldStop      bool
	eventsCommitted *atomic.Int64
}

type offsetsByStream map[string]int64
type offsetsAll map[uint64]map[string]int64
type fileByInode map[uint64]string

func NewJobProvider(config *Config, done *sync.WaitGroup) *jobProvider {
	jp := &jobProvider{
		config: config,
		done:   done,


		jobs:     make(map[uint64]*job, config.ChanLength),
		jobsDone: atomic.NewInt32(0),
		jobsMu:   &sync.Mutex{},
		jobsChan: make(chan *job, config.ChanLength),
		jobsLog:  make([]string, 0, 16),

		offsetsBuf:      make([]string, 0, 65536),
		fileByInode:     make(fileByInode),
		eventsCommitted: &atomic.Int64{},
	}

	if jp.config.persistenceMode == PersistenceModeAsync {
		go jp.saveOffsetsCyclic(time.Millisecond * 100)
	} else if jp.config.persistenceMode == PersistenceModeTimer {
		go jp.saveOffsetsCyclic(time.Second * 5)
	}

	return jp
}

func (jp *jobProvider) start() {
	jp.shouldStop = false
	if !jp.config.ResetOffsets {
		jp.loadOffsets()
	}
	jp.loadJobs()

	go jp.reportStats()
	go jp.maintenance()
}

func (jp *jobProvider) stop() {
	jp.shouldStop = true
	// unblock process function to allow goroutine to exit
	jp.jobsChan <- nil
}

func (jp *jobProvider) sendToWorkers(job *job) {
	if job.isDone || job.isRunning {
		logger.Panicf("why run? it's at end or already running")
	}
	job.isRunning = true
	jp.jobsChan <- job
}

func (jp *jobProvider) Commit(event *pipeline.Event) {
	jp.jobsMu.Lock()
	job, has := jp.jobs[event.SourceId]
	jp.jobsMu.Unlock()
	if !has {
		logger.Panicf("can't find job for event, source=%s", event.SourceId)
	}

	job.mu.Lock()
	offsets := job.offsets
	if offsets[event.Stream] >= event.Offset {
		logger.Panicf("commit offset(%d) for source %d:%s less than current(%d)", event.Offset, event.SourceId, event.Stream, offsets[event.Stream])
	}
	offsets[event.Stream] = event.Offset
	job.mu.Unlock()

	jp.eventsCommitted.Inc()

	if jp.config.persistenceMode == PersistenceModeSync {
		jp.saveOffsets()
	}
}

func (jp *jobProvider) addJob(filename string, loadOffset bool) {
	if filename == jp.config.OffsetsFile || filename == jp.config.offsetsTmpFilename {
		logger.Fatalf("you can't place offset file %s in watching dir %s", jp.config.OffsetsFile, jp.config.WatchingDir)
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

	jp.jobsMu.Lock()
	defer jp.jobsMu.Unlock()

	jp.fileByInode[inode] = filename

	existingJob, has := jp.jobs[inode]
	if has {
		existingJob.mu.Lock()
		defer existingJob.mu.Unlock()

		jp.resumeJob(existingJob, stat.Size())
		return
	}

	job := jp.instantiateJob(file, inode, loadOffset)
	if job == nil {
		return
	}

	jp.jobs[inode] = job
	jp.jobsLog = append(jp.jobsLog, filename)

	jp.jobsDone.Inc()

	job.mu.Lock()
	defer job.mu.Unlock()
	jp.resumeJob(job, stat.Size())
}

func (jp *jobProvider) instantiateJob(file *os.File, inode uint64, loadOffsets bool) *job {
	job := &job{
		sourceId: inode,
		file:     file,
		isDone:   true,
		mu:       &sync.Mutex{},
		offsets:  make(offsetsByStream),
	}

	if loadOffsets {
		offsetsByStream, has := jp.loadedOffsets[inode]
		if has && len(offsetsByStream) == 0 {
			logger.Panicf("can't instantiate job, no streams in source %d:%q", inode, file.Name())
		}

		if has {
			job.offsets = offsetsByStream

			// all streams are in one file, so we should seek file to
			// min offset to make sure logs from all streams will be delivered at least once
			minOffset := int64(math.MaxInt64)
			for _, offset := range offsetsByStream {
				if offset < minOffset {
					minOffset = offset
				}
			}

			offset, err := file.Seek(minOffset, io.SeekStart)
			if err != nil || offset != minOffset {
				logger.Panicf("can't make job, can't seek file %d:%s", inode, file.Name())
			}
		}
	}

	return job
}

// resumeJob job should be already locked
func (jp *jobProvider) resumeJob(job *job, size int64) {
	job.latestSize = size

	if !job.isDone {
		return
	}

	jp.done.Add(1)
	job.isDone = false
	v := jp.jobsDone.Dec()
	if v < 0 {
		logger.Panicf("done jobs counter less than zero")
	}

	jp.sendToWorkers(job)
}

func (jp *jobProvider) releaseJob(job *job, wasEOF bool, offset int64) {
	job.mu.Lock()
	defer job.mu.Unlock()

	if !job.isRunning || job.isDone {
		logger.Panicf("job isn't running, why release?")
	}

	job.isRunning = false
	if wasEOF && offset >= job.latestSize {
		job.isDone = true
		job.latestSize = offset

		v := int(jp.jobsDone.Inc())
		if v > len(jp.jobs) {
			logger.Panicf("done jobs counter more than job count")
		}
		jp.done.Done()
	} else {
		jp.sendToWorkers(job)
	}
}

func (jp *jobProvider) resetJob(job *job) {
	_, err := job.file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Fatalf("job reset error, file % s seek error: %s", job.file.Name(), err.Error())
	}

	for k := range job.offsets {
		job.offsets[k] = 0
	}
}

func (jp *jobProvider) saveOffsetsCyclic(duration time.Duration) {
	lastCommitted := int64(0)
	for {
		time.Sleep(duration)
		if jp.shouldStop {
			return
		}
		// by having separate var won't get race condition
		eventsCommitted := jp.eventsCommitted.Load()
		if lastCommitted != eventsCommitted {
			lastCommitted = eventsCommitted
			jp.saveOffsets()
		}
	}
}

func (jp *jobProvider) saveOffsets() {
	jp.jobsMu.Lock()
	defer jp.jobsMu.Unlock()

	tmpFilename := jp.config.offsetsTmpFilename
	file, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		logger.Errorf("can't open temp offsets file %s, %s", jp.config.OffsetsFile, err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			logger.Errorf("can't close offsets file: %s, %s", tmpFilename, err.Error())
		}
	}()

	jp.offsetsBuf = jp.offsetsBuf[:0]
	for sourceId, job := range jp.jobs {
		job.mu.Lock()
		if len(job.offsets) == 0 {
			job.mu.Unlock()
			continue
		}

		file, has := jp.fileByInode[sourceId]
		if !has {
			logger.Panicf("no file name for source id %d", sourceId)
		}

		jp.offsetsBuf = append(jp.offsetsBuf, "- file: ")
		jp.offsetsBuf = append(jp.offsetsBuf, strconv.FormatUint(sourceId, 10))
		jp.offsetsBuf = append(jp.offsetsBuf, " ")
		jp.offsetsBuf = append(jp.offsetsBuf, file)
		jp.offsetsBuf = append(jp.offsetsBuf, "\n")
		for stream, offset := range job.offsets {
			jp.offsetsBuf = append(jp.offsetsBuf, "  ")
			jp.offsetsBuf = append(jp.offsetsBuf, stream)
			jp.offsetsBuf = append(jp.offsetsBuf, ": ")
			jp.offsetsBuf = append(jp.offsetsBuf, strconv.FormatInt(offset, 10))
			jp.offsetsBuf = append(jp.offsetsBuf, "\n")
		}
		job.mu.Unlock()
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

	err = os.Rename(tmpFilename, jp.config.OffsetsFile)
	if err != nil {
		logger.Errorf("failed renaming temporary offsets file to actual: %s", err.Error())
	}
}

func (jp *jobProvider) loadOffsets() {
	filename := jp.config.OffsetsFile
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

	jp.loadedOffsets, jp.fileByInode = parseOffsets(string(content))
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
		time.Sleep(infoReportInterval)
		if jp.shouldStop {
			return
		}

		added := len(jp.jobsLog)

		if added == 0 {
			continue
		}

		if added <= 3 {
			for _, job := range jp.jobsLog {
				logger.Infof("job added for new file %s ", job)
			}
		}
		if added > 3 {
			logger.Infof("jobs added for new files count=%d", added)
		}

		jp.jobsLog = jp.jobsLog[:0]
	}
}

func (jp *jobProvider) maintenance() {
	for {
		time.Sleep(maintenanceInterval)
		if jp.shouldStop {
			return
		}

		// snapshot current jobs to not lock jobs map for a long time
		jp.jobsList = jp.jobsList[:0]
		jp.jobsMu.Lock()
		for _, job := range jp.jobs {
			jp.jobsList = append(jp.jobsList, job)
		}
		jp.jobsMu.Unlock()

		for _, job := range jp.jobsList {
			job.mu.Lock()
			if !job.isDone {
				job.mu.Unlock()
				continue
			}

			stat, err := job.file.Stat()
			if err != nil {
				logger.Warnf("can't stat file %s", job.file.Name())
				job.mu.Unlock()
				continue
			}

			if job.latestSize != stat.Size() && job.isDone {
				jp.resumeJob(job, job.latestSize)
				job.mu.Unlock()
				continue
			}

			// try release file descriptor in the case file was deleted
			// for that reason just close it and try to open
			sysStat := stat.Sys().(*syscall.Stat_t)

			filename := job.file.Name()
			inode := sysStat.Ino

			offset, err := job.file.Seek(0, io.SeekCurrent)
			if err != nil {
				logger.Warnf("can't seek file %s", filename)
				job.mu.Unlock()
				continue
			}

			if offset != job.latestSize {
				logger.Warnf("something strange happened with offsets of file %s", filename)
				jp.resumeJob(job, job.latestSize)
				job.mu.Unlock()
			}

			err = job.file.Close()
			if err != nil {
				logger.Warnf("can't close file %s", filename)
				job.mu.Unlock()
				continue
			}

			file, err := os.Open(filename)
			if err != nil {
				jp.jobsMu.Lock()

				delete(jp.jobs, inode)
				// job isn't done we've delete it
				v := jp.jobsDone.Dec()
				if v < 0 {
					logger.Panicf("done jobs counter less than zero")

				}

				jp.jobsMu.Unlock()

				logger.Infof("job for file %s was released", filename)
				job.mu.Unlock()
				continue
			}

			_, err = file.Seek(offset, io.SeekStart)
			if err != nil {
				logger.Panicf("can't seek file %s after reopen", filename)
			}

			job.file = file
			job.mu.Unlock()
		}
	}
}

func parseOffsets(data string) (offsetsAll, fileByInode) {
	offsets := make(offsetsAll)
	sourceIdToFile := make(fileByInode)
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
