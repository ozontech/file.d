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
	infoReportInterval  = time.Second * 10
	maintenanceInterval = time.Second * 5
)

type job struct {
	file *os.File

	isDone    bool
	isRunning bool

	inode   inode
	offsets offsetsStreams
	size    int64

	mu *sync.Mutex
}

type jobProvider struct {
	config *Config
	head   pipeline.Head

	jobs                map[inode]*job
	jobsMu              *sync.RWMutex
	jobsChan            chan *job
	jobsLog             []string
	jobsMaintenanceList []*job // temporary list of jobs
	jobsSaveOffsetsList []*job // temporary list of jobs

	jobsDoneCounter *atomic.Int32
	doneWg          *sync.WaitGroup

	offsetsSaveMu      *sync.Mutex
	offsetsSaveCounter *atomic.Int64
	offsetsSaveBuf     []string // content buffer to avoid allocation every offsets saving
	loadedOffsets      offsetsInode
	files              files

	stopSaveOffsetsCh chan bool
	stopReportCh      chan bool
	stopMaintenanceCh chan bool

	// some debugging shit
	eventsCommitted *atomic.Int64
}

type inode uint64

type offsetsStreams map[pipeline.StreamName]int64
type offsetsInode map[inode]map[pipeline.StreamName]int64
type files map[inode]string

func NewJobProvider(config *Config, done *sync.WaitGroup, head pipeline.Head) *jobProvider {
	jp := &jobProvider{
		config: config,
		doneWg: done,

		head: head,

		jobs:            make(map[inode]*job, config.ChanLength),
		jobsDoneCounter: atomic.NewInt32(0),
		jobsMu:          &sync.RWMutex{},
		jobsChan:        make(chan *job, config.ChanLength),
		jobsLog:         make([]string, 0, 16),

		offsetsSaveMu:      &sync.Mutex{},
		offsetsSaveCounter: &atomic.Int64{},
		offsetsSaveBuf:     make([]string, 0, 65536),
		files:              make(files),
		eventsCommitted:    &atomic.Int64{},

		stopSaveOffsetsCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
		stopReportCh:      make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
		stopMaintenanceCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
	}

	return jp
}

func (jp *jobProvider) start() {
	logger.Infof("starting job provider persistence mode=%s", jp.config.PersistenceMode)
	if jp.config.offsetsOp == offsetsOpContinue {
		jp.loadOffsets()
	}
	jp.loadJobs()

	if jp.config.persistenceMode == persistenceModeAsync {
		go jp.saveOffsetsCyclic(time.Millisecond * 100)
	} else if jp.config.persistenceMode == persistenceModeTimer {
		go jp.saveOffsetsCyclic(time.Second * 5)
	}

	go jp.reportStats()
	go jp.maintenance()
}

func (jp *jobProvider) stop() {
	jp.stopReportCh <- true
	jp.stopMaintenanceCh <- true
	if jp.config.persistenceMode == persistenceModeAsync || jp.config.persistenceMode == persistenceModeTimer {
		jp.stopSaveOffsetsCh <- true
	}
}

func (jp *jobProvider) sendToWorkers(job *job) {
	if job.isDone || job.isRunning {
		logger.Panicf("why run? it's at end or already running")
	}
	job.isRunning = true
	jp.jobsChan <- job
}

func (jp *jobProvider) commit(event *pipeline.Event) {
	isActual := event.IsActual()

	jp.jobsMu.RLock()
	job, has := jp.jobs[inode(event.Source)]
	jp.jobsMu.RUnlock()
	if !has && isActual {
		logger.Panicf("can't find job for event, source=%d:%s", event.Source, event.Stream)
	}

	// file and job was deleted, so simply skip commit :)
	if !has {
		return
	}

	job.mu.Lock()
	if job.offsets[event.Stream] >= event.Offset && isActual {
		logger.Panicf("commit offset=%d for source=%d:%s should be more than current=%d for event id=", event.Offset, event.Source, event.Stream, job.offsets[event.Stream], event.ID)
	}
	if isActual {
		job.offsets[event.Stream] = event.Offset
	}
	job.mu.Unlock()

	jp.eventsCommitted.Inc()

	if jp.config.persistenceMode == persistenceModeSync {
		jp.saveOffsets()
	}
}

func (jp *jobProvider) addJob(filename string, isStartup bool) {
	if filename == jp.config.OffsetsFile || filename == jp.config.offsetsTmpFilename {
		logger.Fatalf("sorry, you can't place offsets file %s inside watching dir %s", jp.config.OffsetsFile, jp.config.WatchingDir)
	}

	file, err := os.Open(filename)
	// file may be renamed or deleted while we get to this line, so only warn about this
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
	inode := inode(sysStat.Ino)

	jp.files[inode] = filename

	jp.jobsMu.RLock()
	existingJob, has := jp.jobs[inode]
	jp.jobsMu.RUnlock()

	if has {
		existingJob.mu.Lock()
		jp.resumeJob(existingJob, stat.Size())
		existingJob.mu.Unlock()

		return
	}

	job := jp.instantiateJob(file, inode, isStartup)
	if job == nil {
		return
	}

	jp.jobsMu.Lock()
	jp.jobs[inode] = job
	jp.jobsLog = append(jp.jobsLog, filename)
	jp.jobsMu.Unlock()

	jp.jobsDoneCounter.Inc()

	job.mu.Lock()
	logger.Debugf("job added for %d:%s", job.inode, job.file.Name())
	jp.resumeJob(job, stat.Size())
	job.mu.Unlock()

}

func (jp *jobProvider) instantiateJob(file *os.File, inode inode, isStartup bool) *job {
	job := &job{
		inode:   inode,
		file:    file,
		isDone:  true,
		mu:      &sync.Mutex{},
		offsets: make(offsetsStreams),
	}

	if isStartup {
		if jp.config.offsetsOp == offsetsOpTail {
			_, err := file.Seek(0, io.SeekEnd)
			if err != nil {
				logger.Panicf("can't make job, can't seek file %d:%s", inode, file.Name())
			}
		}

		offsets, has := jp.loadedOffsets[inode]
		if has && len(offsets) == 0 {
			logger.Panicf("can't instantiate job, no streams in source %d:%q", inode, file.Name())
		}

		if has {
			job.offsets = offsets

			// all streams are in one file, so we should seek file to
			// min offset to make sure logs from all streams will be delivered at least once
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
		}
	}

	return job
}

func (jp *jobProvider) releaseJob(job *job, wasEOF bool, offset int64) {
	job.mu.Lock()
	defer job.mu.Unlock()

	if !job.isRunning || job.isDone {
		logger.Panicf("job isn't running, why release?")
	}

	job.isRunning = false
	if wasEOF {
		jp.doneJob(job)
	} else {
		jp.sendToWorkers(job)
	}
}

// resumeJob job should be already locked
func (jp *jobProvider) resumeJob(job *job, size int64) {
	logger.Debugf("job for %d:%s resumed", job.inode, job.file.Name())

	job.size = size

	if !job.isDone {
		return
	}

	jp.doneWg.Add(1)
	job.isDone = false
	v := jp.jobsDoneCounter.Dec()
	if v < 0 {
		logger.Panicf("done jobs counter less than zero")
	}

	jp.sendToWorkers(job)
}

// doneJob job should already be locked
func (jp *jobProvider) doneJob(job *job) {
	job.isDone = true
	v := int(jp.jobsDoneCounter.Inc())
	if v > len(jp.jobs) {
		logger.Panicf("done jobs counter more than job count")
	}
	jp.doneWg.Done()
}

// doneJob job should be already locked
func (jp *jobProvider) truncateJob(job *job, offset int64, size int64) {
	job.mu.Lock()
	defer job.mu.Unlock()

	jp.head.DeprecateEvents(pipeline.SourceId(job.inode))

	_, err := job.file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Fatalf("job reset error, file % s seek error: %s", job.file.Name(), err.Error())
	}

	job.size = size
	for k := range job.offsets {
		job.offsets[k] = 0
	}

	logger.Infof("job %d:%s was truncated, reading will start over, offset=%s, size=%d", job.inode, job.file.Name(), offset, size)
}

func (jp *jobProvider) saveOffsetsCyclic(duration time.Duration) {
	lastCommitted := int64(0)
	for {
		select {
		case <-jp.stopSaveOffsetsCh:
			return
		default:
			eventsAccepted := jp.eventsCommitted.Load()
			if lastCommitted != eventsAccepted {
				lastCommitted = eventsAccepted
				jp.saveOffsets()
			}
			time.Sleep(duration)
		}
	}
}

func (jp *jobProvider) saveOffsets() {
	jp.offsetsSaveCounter.Inc()

	jp.offsetsSaveMu.Lock()
	defer jp.offsetsSaveMu.Unlock()

	// snapshot current jobs to avoid jobs map locking for a long time
	jp.jobsSaveOffsetsList = jp.jobsSaveOffsetsList[:0]
	jp.jobsMu.RLock()
	for _, job := range jp.jobs {
		jp.jobsSaveOffsetsList = append(jp.jobsSaveOffsetsList, job)
	}
	jp.jobsMu.RUnlock()

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

	jp.offsetsSaveBuf = jp.offsetsSaveBuf[:0]
	for _, job := range jp.jobsSaveOffsetsList {
		job.mu.Lock()
		if len(job.offsets) == 0 {
			job.mu.Unlock()
			continue
		}

		file, has := jp.files[job.inode]
		if !has {
			logger.Panicf("no file name for source id %d", job.inode)
		}

		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, "- file: ")
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, strconv.FormatUint(uint64(job.inode), 10))
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, " ")
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, file)
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, "\n")
		for stream, offset := range job.offsets {
			jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, "  ")
			jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, string(stream))
			jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, ": ")
			jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, strconv.FormatInt(offset, 10))
			jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, "\n")
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

	for _, s := range jp.offsetsSaveBuf {
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

	jp.loadedOffsets, jp.files = parseOffsets(string(content))
}

func (jp *jobProvider) loadJobs() {
	files, err := filepath.Glob(filepath.Join(jp.config.WatchingDir, "*"))
	if err != nil {
		logger.Fatalf("can't get file list from watching dir %s:", err.Error())
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
	time.Sleep(infoReportInterval)
	for {
		select {
		case <-jp.stopReportCh:
			return
		default:
			jp.jobsMu.RLock()

			saveCount := jp.offsetsSaveCounter.Swap(0)
			//if saveCount != 0 {
			logger.Infof("file plugin stats for last %d seconds: offsets saves=%d, jobs done=%d, jobs total=%d", infoReportInterval/time.Second, saveCount, jp.jobsDoneCounter.Load(), len(jp.jobs))
			//}

			added := len(jp.jobsLog)
			if added <= 3 {
				for _, job := range jp.jobsLog {
					logger.Infof("job added for a new file %s", job)
				}
			}
			if added > 3 {
				logger.Infof("jobs added for %d new files", added)
			}
			jp.jobsLog = jp.jobsLog[:0]
			jp.jobsMu.RUnlock()

			time.Sleep(infoReportInterval)
		}
	}
}

func (jp *jobProvider) maintenance() {
	time.Sleep(maintenanceInterval)
	for {
		select {
		case <-jp.stopMaintenanceCh:
			return
		default:
			// snapshot current jobs to avoid jobs map locking for a long time
			jp.jobsMaintenanceList = jp.jobsMaintenanceList[:0]
			jp.jobsMu.RLock()
			for _, job := range jp.jobs {
				jp.jobsMaintenanceList = append(jp.jobsMaintenanceList, job)
			}
			jp.jobsMu.RUnlock()

			logger.Debugf("running maintenance for %d jobs", len(jp.jobsMaintenanceList))
			jobsNotDone := 0
			jobsResumed := 0
			jobsReopened := 0
			for _, job := range jp.jobsMaintenanceList {
				job.mu.Lock()
				if !job.isDone {
					jobsNotDone++
					job.mu.Unlock()
					continue
				}

				stat, err := job.file.Stat()
				if err != nil {
					logger.Warnf("can't stat file %s", job.file.Name())
					job.mu.Unlock()
					continue
				}

				if job.size != stat.Size() && job.isDone {
					jobsResumed++
					jp.resumeJob(job, job.size)
					job.mu.Unlock()
					continue
				}

				// try release file descriptor in the case file have been deleted
				// for that reason just close it and try to open
				sysStat := stat.Sys().(*syscall.Stat_t)

				filename := job.file.Name()
				inode := inode(sysStat.Ino)

				offset, err := job.file.Seek(0, io.SeekCurrent)
				if err != nil {
					logger.Warnf("can't seek file %s", filename)
					job.mu.Unlock()
					continue
				}

				if offset != job.size {
					logger.Errorf("something strange happened with offsets of file %s", filename)
					jp.resumeJob(job, job.size)
					job.mu.Unlock()
					continue
				}

				err = job.file.Close()
				if err != nil {
					logger.Warnf("can't close file %s", filename)
					job.mu.Unlock()
					continue
				}

				file, err := os.Open(filename)
				if err != nil {
					job.mu.Unlock()

					jp.jobsMu.Lock()
					jp.head.DeprecateEvents(pipeline.SourceId(job.inode))

					delete(jp.jobs, inode)
					// job isn't done we've delete it
					v := jp.jobsDoneCounter.Dec()
					if v < 0 {
						logger.Panicf("done jobs counter less than zero")

					}

					jp.jobsMu.Unlock()

					logger.Infof("job for file %d:%s was released", job.inode, filename)
					continue
				}

				jobsReopened++
				_, err = file.Seek(offset, io.SeekStart)
				if err != nil {
					logger.Panicf("can't seek file %s after reopen", filename)
				}

				job.file = file
				job.mu.Unlock()
			}

			logger.Infof("maintenance stats: not done jobs=%d, resumed jobs=%d, reopened=%d", jobsNotDone, jobsResumed, jobsReopened)
			time.Sleep(maintenanceInterval)
		}
	}
}

func parseOffsets(data string) (offsetsInode, files) {
	offsets := make(offsetsInode)
	sourceIdToFile := make(files)
	inodeStr := "- file: "
	inodeStrLen := len(inodeStr)
	source := data
	for len(data) != 0 {
		linePos := strings.IndexByte(data, '\n')
		line := data[0:linePos]
		if linePos < 0 {
			logger.Panicf("wrong offsets format, no new line: %q", source)
		}
		data = data[linePos+1:]

		if linePos < inodeStrLen+1 || line[0:inodeStrLen] != inodeStr {
			logger.Panicf("wrong offsets format, file expected: %s", source)
		}

		fullFile := line[inodeStrLen:linePos]
		pos := strings.IndexByte(fullFile, ' ')
		if pos < 0 {
			logger.Panicf("wrong offsets format, inode expected: %s", source)
		}

		filename := line[inodeStrLen+pos+1 : linePos]
		sysInode, err := strconv.ParseUint(line[inodeStrLen:inodeStrLen+pos], 10, 64)
		inode := inode(sysInode)
		if err != nil {
			logger.Panicf("wrong offsets format, can't get inode: %s", err.Error())
		}

		_, has := offsets[inode]
		if has {
			logger.Panicf("wrong offsets format, duplicate inode %d, %s", inode, source)
		}
		offsets[inode] = make(map[pipeline.StreamName]int64)
		sourceIdToFile[inode] = filename

		for len(data) != 0 && data[0] != '-' {
			linePos = strings.IndexByte(data, '\n')
			if linePos < 0 {
				logger.Panicf("wrong offsets format, no new line %s", source)
			}
			line = data[0:linePos]
			if linePos < 3 || line[0:2] != "  " {
				logger.Panicf("wrong offsets format, no leading whitespaces %q", line)
			}
			data = data[linePos+1:]

			pos = strings.IndexByte(line, ':')
			if pos < 0 {
				logger.Panicf("wrong offsets format, no separator %q", line)
			}
			stream := pipeline.StreamName(line[2:pos])
			if len(stream) == 0 {
				logger.Panicf("wrong offsets format, empty stream in inode %d, %s", inode, source)
			}
			_, has = offsets[inode][stream]
			if has {
				logger.Panicf("wrong offsets format, duplicate stream %q in inode %d, %s", stream, inode, source)
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
