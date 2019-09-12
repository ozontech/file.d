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
	maintenanceInterval = time.Second * 10

	maintenanceResultError    = 0
	maintenanceResultNotDone  = 1
	maintenanceResultResumed  = 2
	maintenanceResultReleased = 3
	maintenanceResultNoop     = 4
)

type job struct {
	file     *os.File
	inode    inode
	filename string
	symlink  string

	size   int64
	isDone bool

	offsets streamsOffsets

	mu *sync.Mutex
}

type jobProvider struct {
	config    *Config
	head      pipeline.Head
	isStarted bool

	watcher *watcher

	jobs                map[inode]*job
	jobsMu              *sync.RWMutex
	jobsChan            chan *job
	jobsLog             []string
	jobsSaveOffsetsList []*job // temporary list of jobs

	symlinks   map[inode]string
	symlinksMu *sync.Mutex

	jobsDoneCounter *atomic.Int32
	doneWg          *sync.WaitGroup

	offsetsSaveMu      *sync.Mutex
	offsetsSaveCounter *atomic.Int64
	offsetsSaveBuf     []string // content buffer to avoid allocation every offsets saving
	loadedOffsets      inodesOffsets

	stopSaveOffsetsCh chan bool
	stopReportCh      chan bool
	stopMaintenanceCh chan bool

	// some debugging shit
	eventsCommitted *atomic.Int64
}

type inode uint64
type streamsOffsets map[pipeline.StreamName]int64
type inodesOffsets map[inode]map[pipeline.StreamName]int64

type symlinkInfo struct {
	filename string
	inode    inode
}

func NewJobProvider(config *Config, done *sync.WaitGroup, head pipeline.Head) *jobProvider {
	jp := &jobProvider{
		config: config,
		doneWg: done,

		head: head,

		jobs:            make(map[inode]*job, config.MaxFiles),
		jobsDoneCounter: atomic.NewInt32(0),
		jobsMu:          &sync.RWMutex{},
		jobsChan:        make(chan *job, config.MaxFiles),
		jobsLog:         make([]string, 0, 16),

		symlinks:   make(map[inode]string),
		symlinksMu: &sync.Mutex{},

		offsetsSaveMu:      &sync.Mutex{},
		offsetsSaveCounter: &atomic.Int64{},
		offsetsSaveBuf:     make([]string, 0, 65536),
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

	jp.watcher = NewWatcher(jp.config.WatchingDir, jp.config.FilenamePattern, jp)
	jp.watcher.start()

	if jp.config.persistenceMode == persistenceModeAsync {
		go jp.saveOffsetsCyclic(time.Millisecond * 100)
	} else if jp.config.persistenceMode == persistenceModeTimer {
		go jp.saveOffsetsCyclic(time.Second * 5)
	}

	jp.isStarted = true

	go jp.reportStats()
	go jp.maintenance()
}

func (jp *jobProvider) stop() {
	jp.stopReportCh <- true
	jp.stopMaintenanceCh <- true
	if jp.config.persistenceMode == persistenceModeAsync || jp.config.persistenceMode == persistenceModeTimer {
		jp.stopSaveOffsetsCh <- true
	}

	jp.watcher.stop()
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
		logger.Panicf("commit offset=%d for source=%d:%s should be more than current=%d for event id=%d", event.Offset, event.Source, event.Stream, job.offsets[event.Stream], event.ID)
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

func (jp *jobProvider) actualizeSomething(filename string, stat os.FileInfo) {
	if filename == jp.config.OffsetsFile || filename == jp.config.offsetsTmpFilename {
		logger.Fatalf("sorry, you can't place offsets file %s inside watching dir %s", jp.config.OffsetsFile, jp.config.WatchingDir)
	}

	if stat.Mode()&os.ModeSymlink != 0 {
		jp.actualizeSymlink(filename, getInode(stat))
		return
	}

	jp.actualizeFile(stat, filename, "")
}

func getInode(stat os.FileInfo) inode {
	return inode(stat.Sys().(*syscall.Stat_t).Ino)
}

func (jp *jobProvider) actualizeFile(stat os.FileInfo, filename string, symlink string) {
	inode := getInode(stat)

	jp.jobsMu.RLock()
	job, has := jp.jobs[inode]
	jp.jobsMu.RUnlock()

	if has {
		jp.resumeJob(job, stat, filename)
		return
	}

	file, err := os.Open(filename)
	if err != nil {
		logger.Warnf("file was already moved from creation place %s: %s", filename, err.Error())
		return
	}

	jp.addJob(file, stat, inode, filename, symlink)
}

func (jp *jobProvider) actualizeSymlink(symlink string, inode inode) {
	jp.symlinksMu.Lock()
	jp.symlinks[inode] = symlink
	jp.symlinksMu.Unlock()

	filename, err := filepath.EvalSymlinks(symlink)
	if err != nil {
		logger.Warnf("symlink have been removed %s", symlink)

		jp.symlinksMu.Lock()
		delete(jp.symlinks, inode)
		jp.symlinksMu.Unlock()
		return
	}

	filename, err = filepath.Abs(filename)
	if err != nil {
		logger.Warnf("can't follow symlink to %s: %s", filename, err.Error())
		return
	}

	stat, err := os.Stat(filename)
	if err != nil {
		logger.Warnf("can't follow symlink to %s: %s", filename, err.Error())
		return
	}

	jp.actualizeFile(stat, filename, symlink)
}

func (jp *jobProvider) addJob(file *os.File, stat os.FileInfo, inode inode, filename string, symlink string) {
	job := &job{
		file:     file,
		inode:    inode,
		filename: filename,
		symlink:  symlink,

		size:   0,
		isDone: true,

		offsets: make(streamsOffsets),

		mu: &sync.Mutex{},
	}

	// load saved offsets only on start phase
	if !jp.isStarted {
		jp.loadJobOffsets(job, inode)
	}

	jp.jobsMu.Lock()
	jp.jobs[inode] = job
	if len(jp.jobs) > jp.config.MaxFiles {
		logger.Fatalf("max_files reached for input plugin, consider increase this parameter")
	}
	jp.jobsLog = append(jp.jobsLog, filename)
	jp.jobsMu.Unlock()

	jp.jobsDoneCounter.Inc()

	logger.Infof("job added for a file %d:%s", inode, filename)

	jp.resumeJob(job, stat, filename)
}

func (jp *jobProvider) loadJobOffsets(job *job, inode inode) {
	file := job.file
	if jp.config.offsetsOp == offsetsOpTail {
		_, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			logger.Panicf("can't make job, can't seek file %d:%s: %s", inode, job.filename, err.Error())
		}
	}

	offsets, has := jp.loadedOffsets[inode]
	if has && len(offsets) == 0 {
		logger.Panicf("can't instantiate job, no streams in source %d:%q", inode, job.filename)
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

		_, err := file.Seek(minOffset, io.SeekStart)
		if err != nil {
			logger.Panicf("can't make job, can't seek file %d:%s: %s", inode, job.filename, err.Error())
		}
	}
}

// releaseJob job should be already locked, and after this call it will be unlocked
func (jp *jobProvider) releaseJob(job *job, wasEOF bool, offset int64) {
	if job.isDone {
		logger.Panicf("job isn't running, why release?")
	}

	if wasEOF {
		jp.doneJob(job)
	} else {
		jp.jobsChan <- job
	}
}

func (jp *jobProvider) resumeJob(job *job, stat os.FileInfo, filename string) {
	logger.Debugf("job for %d:%s resumed", job.inode, job.filename)

	job.mu.Lock()
	if !job.isDone {
		job.mu.Unlock()
		return
	}

	job.size = stat.Size()
	job.filename = filename
	job.isDone = false
	job.mu.Unlock()

	if jp.jobsDoneCounter.Dec() < 0 {
		logger.Panicf("done jobs counter less than zero")
	}

	jp.doneWg.Add(1)

	jp.jobsChan <- job
}

// doneJob job should already be locked
func (jp *jobProvider) doneJob(job *job) {
	job.mu.Lock()
	defer job.mu.Unlock()

	job.isDone = true
	v := int(jp.jobsDoneCounter.Inc())
	if v > len(jp.jobs) {
		logger.Panicf("done jobs counter more than job count")
	}
	jp.doneWg.Done()
}

func (jp *jobProvider) truncateJob(job *job, offset int64, size int64) {
	jp.head.DeprecateEvents(pipeline.SourceId(job.inode))

	job.mu.Lock()
	defer job.mu.Unlock()

	_, err := job.file.Seek(0, io.SeekStart)
	if err != nil {
		logger.Fatalf("job reset error, file % s seek error: %s", job.filename, err.Error())
	}

	job.size = size
	for k := range job.offsets {
		job.offsets[k] = 0
	}

	logger.Infof("job %d:%s was truncated, reading will start over, offset=%d, size=%d", job.inode, job.filename, offset, size)
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

		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, "- file: ")
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, strconv.FormatUint(uint64(job.inode), 10))
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, " ")
		jp.offsetsSaveBuf = append(jp.offsetsSaveBuf, job.filename)
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

	jp.loadedOffsets = parseOffsets(string(content))
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
			jp.maintenanceJobs()
			jp.maintenanceSymlinks()

			time.Sleep(maintenanceInterval)
		}
	}
}

func (jp *jobProvider) maintenanceSymlinks() {

	jp.symlinksMu.Lock()
	symlinks := make([]symlinkInfo, 0, len(jp.symlinks))
	for inode, filename := range jp.symlinks {
		symlinks = append(symlinks, symlinkInfo{
			filename:
			filename, inode: inode,
		})
	}
	jp.symlinksMu.Unlock()

	for _, info := range symlinks {
		jp.actualizeSymlink(info.filename, info.inode)
	}
}

func (jp *jobProvider) maintenanceJobs() {
	// snapshot jobs to avoid jobs map locking for a long time
	jp.jobsMu.RLock()
	jobs := make([]*job, 0, len(jp.jobs))
	for _, job := range jp.jobs {
		jobs = append(jobs, job)
	}
	jp.jobsMu.RUnlock()

	resumed := 0
	reopened := 0
	notDone := 0
	released := 0
	errors := 0
	for _, job := range jobs {
		result := jp.maintenanceJob(job)
		switch result {
		case maintenanceResultResumed:
			resumed++
		case maintenanceResultNoop:
			reopened++
		case maintenanceResultNotDone:
			notDone++
		case maintenanceResultReleased:
			released++
		case maintenanceResultError:
			errors++
		}
	}

	logger.Infof("file plugin maintenance stats: not done=%d, resumed=%d, reopened=%d, released=%d errors=%d", notDone, resumed, reopened, released, errors)
}

func (jp *jobProvider) maintenanceJob(job *job) int {
	job.mu.Lock()
	isDone := job.isDone
	filename := job.filename
	file := job.file
	size := job.size
	inode := job.inode
	job.mu.Unlock()

	if !isDone {
		return maintenanceResultNotDone
	}

	stat, err := file.Stat()
	if err != nil {
		logger.Warnf("can't stat file %s", filename)

		return maintenanceResultError
	}

	if size != stat.Size() {
		jp.resumeJob(job, stat, filename)

		return maintenanceResultResumed
	}

	// try release file descriptor in the case file have been deleted
	// for that reason just close it and immediately try to open
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatalf("can't seek file %s: %s", filename, err.Error())
		panic("")
	}

	if offset != size {
		logger.Panicf("something strange happened with offsets of file %s", filename)
	}

	err = file.Close()
	if err != nil {
		logger.Fatalf("can't close file %s: %s", filename, err.Error())
		panic("")
	}

	//todo: here we may have symlink opened, so handle it
	file, err = os.Open(filename)
	if err != nil {
		jp.deleteJob(job)
		logger.Infof("job for file %d:%s was released", inode, filename)

		return maintenanceResultReleased
	}

	stat, err = file.Stat()
	if err != nil {
		logger.Panicf("can't stat file %s: %s", filename, err.Error())
	}

	// it's not a file that was in the job, don't process it
	newInode := getInode(stat)
	if newInode != inode {
		jp.deleteJob(job)

		return maintenanceResultReleased
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		logger.Fatalf("can't seek file %s after reopen: %s", filename, err.Error())
		panic("")
	}

	job.mu.Lock()
	job.file = file
	job.mu.Unlock()

	return maintenanceResultNoop
}

// deleteJob job should be already locked
func (jp *jobProvider) deleteJob(job *job) {
	job.mu.Lock()
	if !job.isDone {
		logger.Panicf("can't delete job, it isn't done: %d:%s", job.inode, job.filename)
	}
	jp.head.DeprecateEvents(pipeline.SourceId(job.inode))
	job.mu.Unlock()

	jp.jobsMu.Lock()
	delete(jp.jobs, job.inode)
	c := jp.jobsDoneCounter.Dec()
	jp.jobsMu.Unlock()

	if c < 0 {
		logger.Panicf("done jobs counter less than zero")
	}
}

func parseOffsets(data string) inodesOffsets {
	offsets := make(inodesOffsets)
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

	return offsets
}
