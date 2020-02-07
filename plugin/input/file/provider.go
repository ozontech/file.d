package file

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	maintenanceResultError   = 0
	maintenanceResultNotDone = 1
	maintenanceResultResumed = 2
	maintenanceResultDeleted = 3
	maintenanceResultNoop    = 4
)

type jobProvider struct {
	config     *Config
	controller pipeline.InputPluginController
	watcher    *watcher
	offsetDB   *offsetDB

	isStarted bool

	jobs     map[pipeline.SourceID]*job
	jobsMu   *sync.RWMutex
	jobsChan chan *job
	jobsLog  []string

	symlinks   map[inode]string
	symlinksMu *sync.Mutex

	jobsDone *atomic.Int32

	loadedOffsets fpOffsets

	stopSaveOffsetsCh chan bool
	stopReportCh      chan bool
	stopMaintenanceCh chan bool

	// some debugging shit
	offsetsCommitted *atomic.Int64
	logger           *zap.SugaredLogger
}

type job struct {
	file     *os.File
	inode    inode
	sourceID pipeline.SourceID // some value to distinguish jobs with same inode
	filename string
	symlink  string

	ignoreEventsLE uint64 // events with seq id less or equal than this should be ignored in terms offset commitment
	lastEventSeq   uint64

	isVirgin   bool // it should be set to false if job hits isDone=true at the first time
	isDone     bool
	shouldSkip bool

	offsets streamsOffsets

	mu *sync.Mutex
}

type inode uint64

type symlinkInfo struct {
	filename string
	inode    inode
}

func NewJobProvider(config *Config, controller pipeline.InputPluginController, logger *zap.SugaredLogger) *jobProvider {
	jp := &jobProvider{
		config:     config,
		controller: controller,
		offsetDB:   newOffsetDB(config.OffsetsFile, config.OffsetsFileTmp),

		jobs:     make(map[pipeline.SourceID]*job, config.MaxFiles),
		jobsDone: atomic.NewInt32(0),
		jobsMu:   &sync.RWMutex{},
		jobsChan: make(chan *job, config.MaxFiles),
		jobsLog:  make([]string, 0, 16),

		symlinks:   make(map[inode]string),
		symlinksMu: &sync.Mutex{},

		offsetsCommitted: &atomic.Int64{},

		stopSaveOffsetsCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
		stopReportCh:      make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
		stopMaintenanceCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop

		logger: logger,
	}

	jp.watcher = NewWatcher(config.WatchingDir, config.FilenamePattern, config.DirPattern, jp.processNotification, logger)

	return jp
}

func (jp *jobProvider) start() {
	jp.logger.Infof("starting job provider persistence mode=%s", jp.config.PersistenceMode)
	if jp.config.OffsetsOp_ == offsetsOpContinue {
		jp.loadedOffsets = jp.offsetDB.load()
	}

	jp.watcher.start()

	if jp.config.PersistenceMode_ == persistenceModeAsync {
		go jp.saveOffsetsCyclic(jp.config.AsyncInterval_)
	}

	jp.isStarted = true

	go jp.reportStats()
	go jp.maintenance()
}

func (jp *jobProvider) stop() {
	jp.stopReportCh <- true
	jp.stopMaintenanceCh <- true
	if jp.config.PersistenceMode_ == persistenceModeAsync {
		jp.stopSaveOffsetsCh <- true
	}

	jp.watcher.stop()

	jp.logger.Infof("saving last known offsets...")
	jp.offsetDB.save(jp.jobs, jp.jobsMu)
}

func (jp *jobProvider) commit(event *pipeline.Event) {
	streamName := pipeline.StreamName(pipeline.ByteToStringUnsafe(event.StreamNameBytes()))

	jp.jobsMu.RLock()
	job, has := jp.jobs[event.SourceID]
	jp.jobsMu.RUnlock()

	if !has {
		return
	}

	job.mu.Lock()
	// commit offsets only not ignored AND regular events
	if !event.IsRegularKind() || event.SeqID <= job.ignoreEventsLE {
		job.mu.Unlock()
		return
	}

	value, has := job.offsets[streamName]
	if value >= event.Offset {
		jp.logger.Panicf("offset corruption: committing=%d, current=%d, event id=%d, source=%d:%s", event.Offset, value, event.SeqID, event.SourceID, event.SourceName)
	}

	if value == 0 && event.Offset >= 16*1024*1024 {
		jp.logger.Errorf("it maybe an offset corruption: committing=%d, current=%d, event id=%d, source=%d:%s", event.Offset, value, event.SeqID, event.SourceID, event.SourceName)
	}

	// streamName isn't actually a string, but unsafe []byte, so copy it when adding to map
	if has {
		job.offsets[streamName] = event.Offset
	} else {
		streamNameCopy := pipeline.StreamName(event.StreamNameBytes())
		job.offsets[streamNameCopy] = event.Offset
	}

	job.mu.Unlock()

	jp.offsetsCommitted.Inc()
	if jp.config.PersistenceMode_ == persistenceModeSync {
		jp.offsetDB.save(jp.jobs, jp.jobsMu)
	}
}

func (jp *jobProvider) processNotification(filename string, stat os.FileInfo) {
	if filename == jp.config.OffsetsFile || filename == jp.config.OffsetsFileTmp {
		jp.logger.Fatalf("sorry, you can't place offsets file %s inside watching dir %s", jp.config.OffsetsFile, jp.config.WatchingDir)
	}

	if stat.Mode()&os.ModeSymlink != 0 {
		inode := getInode(stat)
		jp.addSymlink(inode, filename)
		jp.refreshSymlink(filename, inode)
		return
	}

	jp.refreshFile(stat, filename, "")
}

func (jp *jobProvider) addSymlink(inode inode, filename string) {
	jp.symlinksMu.Lock()
	jp.symlinks[inode] = filename
	jp.symlinksMu.Unlock()
}

func (jp *jobProvider) refreshSymlink(symlink string, inode inode) {
	filename, err := filepath.EvalSymlinks(symlink)
	if err != nil {
		jp.logger.Warnf("symlink have been removed %s", symlink)

		jp.symlinksMu.Lock()
		delete(jp.symlinks, inode)
		jp.symlinksMu.Unlock()
		return
	}

	filename, err = filepath.Abs(filename)
	if err != nil {
		jp.logger.Warnf("can't follow symlink to %s: %s", filename, err.Error())
		return
	}

	stat, err := os.Stat(filename)
	if err != nil {
		jp.logger.Warnf("can't follow symlink to %s: %s", filename, err.Error())
		return
	}

	jp.refreshFile(stat, filename, symlink)
}

func (jp *jobProvider) refreshFile(stat os.FileInfo, filename string, symlink string) {
	sourceID := sourceIDByStat(stat, symlink)
	jp.jobsMu.RLock()
	job, has := jp.jobs[sourceID]
	jp.jobsMu.RUnlock()

	if has {
		job.mu.Lock()
		jp.tryResumeJobAndUnlock(job, filename)
		return
	}

	file, err := os.Open(filename)
	if err != nil {
		jp.logger.Warnf("file was already moved from creation place %s: %s", filename, err.Error())
		return
	}

	jp.addJob(file, stat, filename, symlink)
}

func (jp *jobProvider) addJob(file *os.File, stat os.FileInfo, filename string, symlink string) {
	sourceID := sourceIDByStat(stat, symlink)
	inode := getInode(stat)
	job := &job{
		file:     file,
		inode:    inode,
		filename: filename,
		symlink:  symlink,
		sourceID: sourceID,

		isVirgin:   true,
		isDone:     true,
		shouldSkip: false,

		offsets: make(streamsOffsets),

		mu: &sync.Mutex{},
	}

	// load saved offsets only on start phase
	if jp.isStarted {
		jp.initJobOffset(offsetsOpReset, job)
	} else {
		jp.initJobOffset(jp.config.OffsetsOp_, job)
	}

	jp.jobsMu.Lock()
	jp.jobs[sourceID] = job
	if len(jp.jobs) > jp.config.MaxFiles {
		jp.logger.Fatalf("max_files reached for input plugin, consider increase this parameter")
	}
	jp.jobsLog = append(jp.jobsLog, filename)
	jp.jobsDone.Inc()
	jp.jobsMu.Unlock()

	if symlink != "" {
		jp.logger.Infof("job added for a file %d:%s, symlink=%s", sourceID, filename, symlink)
	} else {
		jp.logger.Infof("job added for a file %d:%s", sourceID, filename)
	}

	job.mu.Lock()
	jp.tryResumeJobAndUnlock(job, filename)
}

func sourceIDByStat(s os.FileInfo, symlink string) pipeline.SourceID {
	inode := int64(s.Sys().(*syscall.Stat_t).Ino)

	symHash := inode * 8922886018542929
	for _, c := range symlink {
		symHash <<= 2
		symHash -= 1
		symHash += int64(c) * 8460724049
	}

	// since the inode number is more likely only 32 bit, use upper bits to store hash symlink
	return pipeline.SourceID(inode + symHash&math.MaxUint32)
}

func (jp *jobProvider) initJobOffset(operation offsetsOp, job *job) {
	switch operation {
	case offsetsOpTail:
		offset, err := job.file.Seek(0, io.SeekEnd)
		if err != nil {
			jp.logger.Panicf("can't make job, can't seek file %d:%s: %s", job.sourceID, job.filename, err.Error())
		}

		if offset == 0 {
			return
		}

		// current offset may be in the middle of an event, so worker skips data to the next line
		// by applying this offset it'll guarantee that full log won't be skipped
		magicOffset := int64(-1)
		_, err = job.file.Seek(magicOffset, io.SeekEnd)
		if err != nil {
			jp.logger.Panicf("can't make job, can't seek file %d:%s: %s", job.sourceID, job.filename, err.Error())
		}
		job.shouldSkip = true

	case offsetsOpReset:
		_, err := job.file.Seek(0, io.SeekStart)
		if err != nil {
			jp.logger.Panicf("can't make job, can't seek file %d:%s: %s", job.sourceID, job.filename, err.Error())
		}
	case offsetsOpContinue:
		offsets, has := jp.loadedOffsets[job.sourceID]
		if has && len(offsets.streams) == 0 {
			jp.logger.Panicf("can't instantiate job, no streams in source %d:%q", job.sourceID, job.filename)
		}
		if !has {
			_, err := job.file.Seek(0, io.SeekStart)
			if err != nil {
				jp.logger.Panicf("can't make job, can't seek file %d:%s: %s", job.sourceID, job.filename, err.Error())
			}

			return
		}

		job.offsets = offsets.streams
		// seek to any offset since whey all equal at start time
		for _, offset := range offsets.streams {
			_, err := job.file.Seek(offset, io.SeekStart)
			if err != nil {
				jp.logger.Panicf("can't make job, can't seek file %d:%s: %s", job.sourceID, job.filename, err.Error())
			}
			return
		}
	default:
		jp.logger.Panicf("unknown offsets op: %d", jp.config.OffsetsOp_)
	}
}

//tryResumeJob job should be already locked and it'll be unlocked
func (jp *jobProvider) tryResumeJobAndUnlock(job *job, filename string) bool {
	jp.logger.Debugf("job for %d:%s resumed", job.sourceID, job.filename)

	if !job.isDone {
		job.mu.Unlock()
		return false
	}

	job.filename = filename
	job.isDone = false

	if jp.jobsDone.Dec() < 0 {
		jp.logger.Panicf("done jobs counter is less than zero")
	}

	job.mu.Unlock()
	jp.jobsChan <- job
	return true
}

func (jp *jobProvider) continueJob(job *job) {
	jp.jobsChan <- job
}

func (jp *jobProvider) doneJob(job *job) {
	job.mu.Lock()
	if job.isDone {
		jp.logger.Panicf("job is already done")
	}
	job.isDone = true
	job.isVirgin = false

	jp.jobsMu.Lock()
	v := int(jp.jobsDone.Inc())
	if v > len(jp.jobs) {
		jp.logger.Panicf("done jobs counter is more than job count")
	}
	jp.jobsMu.Unlock()

	job.mu.Unlock()
}

func (jp *jobProvider) truncateJob(job *job) {
	job.mu.Lock()
	defer job.mu.Unlock()

	job.ignoreEventsLE = job.lastEventSeq

	_, err := job.file.Seek(0, io.SeekStart)
	if err != nil {
		jp.logger.Fatalf("job reset error, file %s seek error: %s", job.filename, err.Error())
	}

	for stream := range job.offsets {
		job.offsets[stream] = 0
	}

	jp.logger.Infof("job %d:%s was truncated, reading will start over, events with id less than %d will be ignored", job.sourceID, job.filename, job.ignoreEventsLE)
}

func (jp *jobProvider) saveOffsetsCyclic(duration time.Duration) {
	lastCommitted := int64(0)
	for {
		select {
		case <-jp.stopSaveOffsetsCh:
			return
		default:
			offsetsCommitted := jp.offsetsCommitted.Load()
			if lastCommitted != offsetsCommitted {
				lastCommitted = offsetsCommitted
				jp.offsetDB.save(jp.jobs, jp.jobsMu)
			}
			time.Sleep(duration)
		}
	}
}

func (jp *jobProvider) reportStats() {
	time.Sleep(jp.config.ReportInterval_)
	lastSaves := jp.offsetDB.savesTotal.Load()
	for {
		select {
		case <-jp.stopReportCh:
			return
		default:
			jp.jobsMu.RLock()
			l := len(jp.jobs)
			jp.jobsMu.RUnlock()

			savesTotal := jp.offsetDB.savesTotal.Load() - lastSaves
			lastSaves = savesTotal
			jp.logger.Infof("file plugin stats for last %d seconds: offsets saves=%d, jobs done=%d, jobs total=%d", jp.config.ReportInterval_/time.Second, savesTotal, jp.jobsDone.Load(), l)

			jp.jobsLog = jp.jobsLog[:0]

			time.Sleep(jp.config.ReportInterval_)
		}
	}
}

/*{ maintenance
For now maintenance consists of two stages:
* Symlinks
* Jobs

Symlinks maintenance detects if underlying file of symlink is changed.
Job maintenance `fstat` tracked files to detect if new portion of data have been written to the file. If job is in `done` state when it releases and reopens file descriptor to allow third party software delete the file.
}*/
func (jp *jobProvider) maintenance() {
	time.Sleep(jp.config.MaintenanceInterval_)
	for {
		select {
		case <-jp.stopMaintenanceCh:
			return
		default:
			jp.maintenanceJobs()
			jp.maintenanceSymlinks()

			time.Sleep(jp.config.MaintenanceInterval_)
		}
	}
}

func (jp *jobProvider) maintenanceSymlinks() {
	jp.symlinksMu.Lock()
	symlinks := make([]symlinkInfo, 0, len(jp.symlinks))
	for inode, filename := range jp.symlinks {
		symlinks = append(symlinks, symlinkInfo{
			filename: filename,
			inode:    inode,
		})
	}
	jp.symlinksMu.Unlock()

	for _, info := range symlinks {
		jp.refreshSymlink(info.filename, info.inode)
	}
}

func (jp *jobProvider) maintenanceJobs() {
	// snapshot jobs to avoid long lock
	jp.jobsMu.RLock()
	jobs := make([]*job, 0, len(jp.jobs))
	for _, job := range jp.jobs {
		jobs = append(jobs, job)
	}
	jp.jobsMu.RUnlock()

	resumed := 0
	reopened := 0
	notDone := 0
	deleted := 0
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
		case maintenanceResultDeleted:
			deleted++
		case maintenanceResultError:
			errors++
		}
	}

	jp.logger.Infof("file plugin maintenance stats: not done=%d, resumed=%d, reopened=%d, deleted=%d, errors=%d", notDone, resumed, reopened, deleted, errors)
}

func (jp *jobProvider) maintenanceJob(job *job) int {
	job.mu.Lock()
	isDone := job.isDone
	filename := job.filename
	file := job.file
	inode := job.inode

	if !isDone {
		job.mu.Unlock()
		return maintenanceResultNotDone
	}

	stat, err := file.Stat()
	if err != nil {
		job.mu.Unlock()
		jp.logger.Warnf("can't stat file %s", filename)

		return maintenanceResultError
	}

	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		jp.logger.Fatalf("can't seek file %s: %s", filename, err.Error())
		panic("")
	}

	if stat.Size() != offset {
		jp.tryResumeJobAndUnlock(job, filename)

		return maintenanceResultResumed
	}

	// filename was changed
	if filepath.Base(job.filename) != stat.Name() {
		job.filename = filepath.Dir(job.filename) + stat.Name()
		job.mu.Unlock()

		return maintenanceResultNoop
	}

	filename = job.filename
	file = job.file
	inode = job.inode

	// try release file descriptor in the case file have been deleted
	// for that reason just close it and immediately try to open

	err = file.Close()
	if err != nil {
		jp.logger.Fatalf("can't close a file %s: %s", filename, err.Error())
		panic("")
	}

	//todo: here we may have symlink opened, so handle it
	file, err = os.Open(filename)
	if err != nil {
		jp.deleteJobAndUnlock(job)
		jp.logger.Infof("job for a file %d:%s have been released", inode, filename)

		return maintenanceResultDeleted
	}

	stat, err = file.Stat()
	if err != nil {
		jp.logger.Panicf("can't stat a file %s: %s", filename, err.Error())
	}

	// it isn't a file that was in the job, don't process it
	newInode := getInode(stat)
	if newInode != inode {
		jp.deleteJobAndUnlock(job)

		return maintenanceResultDeleted
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		jp.logger.Fatalf("can't seek a file %s after reopen: %s", filename, err.Error())
		panic("")
	}

	job.file = file
	job.mu.Unlock()

	return maintenanceResultNoop
}

// deleteJob job should be already locked and it'll be unlocked
func (jp *jobProvider) deleteJobAndUnlock(job *job) {
	if !job.isDone {
		jp.logger.Panicf("can't delete job, it isn't done: %d:%s", job.sourceID, job.filename)
	}
	sourceID := job.sourceID
	filename := job.filename
	job.mu.Unlock()

	jp.jobsMu.Lock()
	delete(jp.jobs, sourceID)
	c := jp.jobsDone.Dec()
	jp.jobsMu.Unlock()

	jp.logger.Infof("job %d:%s deleted", job.sourceID, filename)
	if c < 0 {
		jp.logger.Panicf("done jobs counter less than zero")
	}
}

func getInode(stat os.FileInfo) inode {
	return inode(stat.Sys().(*syscall.Stat_t).Ino)
}
