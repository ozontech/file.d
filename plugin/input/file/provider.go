package file

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rjeczalik/notify"
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
	config   *Config
	watcher  *watcher
	offsetDB *offsetDB

	isStarted atomic.Bool

	jobs     map[pipeline.SourceID]*Job
	jobsMu   *sync.RWMutex
	jobsChan chan *Job
	jobsLog  []string

	symlinks   map[inodeID]string
	symlinksMu *sync.Mutex

	jobsDone *atomic.Int32

	loadedOffsets fpOffsets

	stopSaveOffsetsCh chan bool
	stopReportCh      chan bool
	stopMaintenanceCh chan bool

	// some debugging stuff

	offsetsCommitted *atomic.Int64
	logger           *zap.SugaredLogger

	// provider metrics
	possibleOffsetCorruptionMetric prometheus.Counter
	errorOpenFileMetric            prometheus.Counter
	numberOfCurrentJobsMetric      prometheus.Gauge
}

type Job struct {
	file      *os.File
	inode     inodeID
	sourceID  pipeline.SourceID // some value to distinguish jobs with same inode
	filename  string
	symlink   string
	curOffset int64  // offset to not call Seek() everytime
	tail      []byte // some data of a new line read by worker, to not seek backwards to read from line start

	ignoreEventsLE uint64 // events with seq id less or equal than this should be ignored in terms offset commitment
	lastEventSeq   uint64

	isVirgin   bool // it should be set to false if job hits isDone=true at the first time
	isDone     bool
	shouldSkip atomic.Bool

	// offsets is a sliceMap of streamName to offset.
	// Unlike map[string]int, sliceMap can work with mutable strings when using unsafe conversion from []byte.
	// Also it is likely not slower than map implementation for 1-2 streams case.
	offsets sliceMap

	mu *sync.Mutex
}

func (j *Job) seek(offset int64, whence int, hint string) int64 {
	n, err := j.file.Seek(offset, whence)
	if err != nil {
		logger.Infof("file seek error hint=%s, name=%s, err=%s", hint, j.filename, err.Error())
	}
	j.curOffset = n

	return n
}

type inodeID uint64

type symlinkInfo struct {
	filename string
	inode    inodeID
}

type metricCollection struct {
	possibleOffsetCorruptionMetric prometheus.Counter
	errorOpenFileMetric            prometheus.Counter
	notifyChannelLengthMetric      prometheus.Gauge
	numberOfCurrentJobsMetric      prometheus.Gauge
}

func newMetricCollection(
	possibleOffsetCorruptionMetric, errorOpenFileMetric prometheus.Counter,
	notifyChannelLengthMetric prometheus.Gauge, numberOfCurrentJobsMetric prometheus.Gauge,
) *metricCollection {
	return &metricCollection{
		possibleOffsetCorruptionMetric: possibleOffsetCorruptionMetric,
		errorOpenFileMetric:            errorOpenFileMetric,
		notifyChannelLengthMetric:      notifyChannelLengthMetric,
		numberOfCurrentJobsMetric:      numberOfCurrentJobsMetric,
	}
}

func NewJobProvider(config *Config, metrics *metricCollection, sugLogger *zap.SugaredLogger) *jobProvider {
	jp := &jobProvider{
		config:   config,
		offsetDB: newOffsetDB(config.OffsetsFile, config.OffsetsFileTmp),

		jobs:     make(map[pipeline.SourceID]*Job, config.MaxFiles),
		jobsDone: atomic.NewInt32(0),
		jobsMu:   &sync.RWMutex{},
		jobsChan: make(chan *Job, config.MaxFiles),
		jobsLog:  make([]string, 0, 16),

		symlinks:   make(map[inodeID]string),
		symlinksMu: &sync.Mutex{},

		offsetsCommitted: &atomic.Int64{},

		stopSaveOffsetsCh: make(chan bool, 1), // non-zero channel cause we don't wanna wait goroutine to stop
		stopReportCh:      make(chan bool, 1), // non-zero channel cause we don't wanna wait goroutine to stop
		stopMaintenanceCh: make(chan bool, 1), // non-zero channel cause we don't wanna wait goroutine to stop

		logger:                         sugLogger,
		possibleOffsetCorruptionMetric: metrics.possibleOffsetCorruptionMetric,
		errorOpenFileMetric:            metrics.errorOpenFileMetric,
		numberOfCurrentJobsMetric:      metrics.numberOfCurrentJobsMetric,
	}

	if len(config.Paths.Include) == 0 {
		if config.DirPattern == "*" {
			config.Paths.Include = append(
				config.Paths.Include,
				filepath.Join(config.WatchingDir, filepath.Join("**", config.FilenamePattern)),
			)
		} else {
			config.Paths.Include = append(
				config.Paths.Include,
				filepath.Join(config.WatchingDir, filepath.Join(config.DirPattern, config.FilenamePattern)),
			)
		}
	}

	jp.watcher = NewWatcher(
		config.WatchingDir,
		config.Paths,
		jp.processNotification,
		config.ShouldWatchChanges,
		metrics.notifyChannelLengthMetric,
		sugLogger,
	)

	return jp
}

func (jp *jobProvider) start() {
	jp.logger.Infof("starting job provider persistence mode=%s", jp.config.PersistenceMode)
	if jp.config.OffsetsOp_ == offsetsOpContinue {
		offsets, err := jp.offsetDB.load()
		if err != nil {
			logger.Panicf("can't load offsets: %s", err.Error())
		}
		jp.loadedOffsets = offsets
	}

	jp.watcher.start()

	if jp.config.PersistenceMode_ == persistenceModeAsync {
		go jp.saveOffsetsCyclic(jp.config.AsyncInterval_)
	}

	jp.isStarted.Store(true)

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
	if (!event.IsRegularKind() && !event.IsChildParentKind()) || event.SeqID <= job.ignoreEventsLE {
		job.mu.Unlock()
		return
	}

	value, has := job.offsets.get(streamName)
	if value >= event.Offset {
		defer job.mu.Unlock()
		jp.logger.Panicf("offset corruption: committing=%d, current=%d, event id=%d, source=%d:%s", event.Offset, value, event.SeqID, event.SourceID, event.SourceName)
	}

	if value == 0 && event.Offset >= 16*1024*1024 {
		jp.possibleOffsetCorruptionMetric.Inc()
		jp.logger.Errorf("it maybe an offset corruption: committing=%d, current=%d, event id=%d, source=%d:%s", event.Offset, value, event.SeqID, event.SourceID, event.SourceName)
	}

	// streamName isn't actually a string, but unsafe []byte, so copy it when adding to the sliceMap
	if has {
		job.offsets.set(streamName, event.Offset)
	} else {
		streamNameCopy := pipeline.StreamName(event.StreamNameBytes())
		job.offsets.set(streamNameCopy, event.Offset)
	}

	job.mu.Unlock()

	jp.offsetsCommitted.Inc()
	if jp.config.PersistenceMode_ == persistenceModeSync {
		jp.offsetDB.save(jp.jobs, jp.jobsMu)
	}
}

func (jp *jobProvider) processNotification(e notify.Event, filename string, stat os.FileInfo) {
	if filename == jp.config.OffsetsFile || filename == jp.config.OffsetsFileTmp {
		return
	}

	isWrite := e == notify.Write

	if stat.Mode()&os.ModeSymlink != 0 {
		inode := getInode(stat)
		jp.addSymlink(inode, filename)
		jp.refreshSymlink(filename, inode, isWrite)
		return
	}

	jp.refreshFile(stat, filename, "", isWrite)
}

func (jp *jobProvider) addSymlink(inode inodeID, filename string) {
	jp.symlinksMu.Lock()
	jp.symlinks[inode] = filename
	jp.symlinksMu.Unlock()
}

func (jp *jobProvider) refreshSymlink(symlink string, inode inodeID, isWrite bool) {
	if _, err := os.Lstat(symlink); err != nil {
		jp.logger.Warnf("symlink has been removed %s: %s", symlink, err.Error())

		jp.symlinksMu.Lock()
		delete(jp.symlinks, inode)
		jp.symlinksMu.Unlock()
		return
	}

	filename, err := os.Readlink(symlink)
	if err != nil {
		jp.logger.Warnf("symlink %s is broken: %s", symlink, err.Error())
		// for example, in the case of rotating the k8s pod logs symlink will be fixed soon
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

	jp.refreshFile(stat, filename, symlink, isWrite)
}

func (jp *jobProvider) refreshFile(stat os.FileInfo, filename string, symlink string, isWrite bool) {
	sourceID := sourceIDByStat(stat, symlink)
	jp.jobsMu.RLock()
	job, has := jp.jobs[sourceID]
	jp.jobsMu.RUnlock()

	if has {
		if isWrite {
			jp.checkFileWasTruncated(job, stat.Size())
		}
		job.mu.Lock()
		jp.tryResumeJobAndUnlock(job, filename)
		return
	}

	file, err := os.Open(filename)
	if err != nil {
		jp.logger.Warnf("file was already moved from creation place %s: %s", filename, err.Error())
		jp.errorOpenFileMetric.Inc()
		return
	}

	jp.addJob(file, stat, filename, symlink)
}

func (jp *jobProvider) checkFileWasTruncated(job *Job, size int64) {
	lastOffset := job.seek(0, io.SeekCurrent, "check file truncation")

	if lastOffset > size {
		jp.truncateJob(job)
	}
}

func (jp *jobProvider) addJob(file *os.File, stat os.FileInfo, filename string, symlink string) {
	sourceID := sourceIDByStat(stat, symlink)

	jp.jobsMu.Lock()
	defer jp.jobsMu.Unlock()
	// check again in case when the file was created, removed (or renamed) and created again.
	_, has := jp.jobs[sourceID]
	if has {
		jp.logger.Warnf("job for a file %q was already created", filename)
		if err := file.Close(); err != nil {
			jp.logger.Errorf("can't close file %s %v in case of already created file", filename, err)
		}
		return
	}

	inode := getInode(stat)
	job := &Job{
		file:     file,
		inode:    inode,
		filename: filename,
		symlink:  symlink,
		sourceID: sourceID,

		isVirgin:   true,
		isDone:     true,
		shouldSkip: *atomic.NewBool(false),

		offsets: nil,

		mu: &sync.Mutex{},
	}

	// set curOffset
	job.seek(0, io.SeekCurrent, "add job")

	// load saved offsets only on start phase
	if jp.isStarted.Load() {
		jp.initJobOffset(offsetsOpReset, job)
	} else {
		jp.initJobOffset(jp.config.OffsetsOp_, job)
	}
	jp.jobs[sourceID] = job

	jobsLen := len(jp.jobs)
	jp.numberOfCurrentJobsMetric.Set(float64(jobsLen))

	if jobsLen > jp.config.MaxFiles {
		jp.logger.Fatalf(
			"limit max_files=%d is reached for input plugin, consider increase this parameter",
			jp.config.MaxFiles,
		)
	}
	jp.jobsLog = append(jp.jobsLog, filename)
	jp.jobsDone.Inc()

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

func (jp *jobProvider) initJobOffset(operation offsetsOp, job *Job) {
	switch operation {
	case offsetsOpTail:
		offset := job.seek(0, io.SeekEnd, "job initialization")

		if offset == 0 {
			return
		}

		// current offset may be in the middle of an event, so worker skips data to the next line
		// by applying this offset it'll guarantee that full log won't be skipped
		magicOffset := int64(-1)
		_ = job.seek(magicOffset, io.SeekEnd, "job initialization")
		job.shouldSkip.Store(true)

	case offsetsOpReset:
		job.seek(0, io.SeekStart, "job initialization")
	case offsetsOpContinue:
		offsets, has := jp.loadedOffsets[job.sourceID]
		if has && len(offsets.streams) == 0 {
			jp.logger.Panicf("can't instantiate job, no streams in source %d:%q", job.sourceID, job.filename)
		}
		if !has {
			job.seek(0, io.SeekStart, "job initialization")
			return
		}

		job.offsets = sliceFromMap(offsets.streams)
		// find min Offset to start read from it
		minOffset := int64(math.MaxInt64)
		for _, offset := range offsets.streams {
			if offset < minOffset {
				minOffset = offset
			}
		}
		job.seek(minOffset, io.SeekStart, "job initialization")
	default:
		jp.logger.Panicf("unknown offsets op: %d", jp.config.OffsetsOp_)
	}
}

// tryResumeJob job should be already locked and it'll be unlocked.
func (jp *jobProvider) tryResumeJobAndUnlock(job *Job, filename string) {
	jp.logger.Debugf("job for %d:%s resumed", job.sourceID, job.filename)

	if !job.isDone {
		job.mu.Unlock()
		return
	}

	job.filename = filename
	job.isDone = false

	if jp.jobsDone.Dec() < 0 {
		jp.logger.Panicf("done jobs counter is less than zero")
	}

	job.mu.Unlock()
	jp.jobsChan <- job
}

func (jp *jobProvider) continueJob(job *Job) {
	jp.jobsChan <- job
}

func (jp *jobProvider) doneJob(job *Job) {
	job.mu.Lock()
	if job.isDone {
		jp.logger.Panicf("job is already done")
	}
	job.isDone = true
	job.isVirgin = false

	jp.jobsMu.Lock()
	v := int(jp.jobsDone.Inc())

	jobsLen := len(jp.jobs)
	jp.numberOfCurrentJobsMetric.Set(float64(jobsLen))

	if v > jobsLen {
		jp.logger.Panicf("done jobs counter is more than job count")
	}
	jp.jobsMu.Unlock()

	job.mu.Unlock()
}

func (jp *jobProvider) truncateJob(job *Job) {
	job.mu.Lock()
	defer job.mu.Unlock()

	job.ignoreEventsLE = job.lastEventSeq

	job.seek(0, io.SeekStart, "truncation")

	for _, strOff := range job.offsets {
		job.offsets.set(strOff.stream, 0)
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
			jp.jobsMu.Lock()
			l := len(jp.jobs)
			jp.jobsLog = jp.jobsLog[:0]
			jp.jobsMu.Unlock()

			savesTotal := jp.offsetDB.savesTotal.Load() - lastSaves
			lastSaves = savesTotal
			jp.logger.Infof("file plugin stats for last %d seconds: offsets saves=%d, jobs done=%d, jobs total=%d", jp.config.ReportInterval_/time.Second, savesTotal, jp.jobsDone.Load(), l)

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
		jp.refreshSymlink(info.filename, info.inode, false)
	}
}

func (jp *jobProvider) maintenanceJobs() {
	// snapshot jobs to avoid long lock
	jp.jobsMu.RLock()
	jobs := make([]*Job, 0, len(jp.jobs))
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

func (jp *jobProvider) maintenanceJob(job *Job) int {
	// can't use defer here because of jp.deleteJobAndUnlock()
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

	offset := job.seek(0, io.SeekCurrent, "maintenance")

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

	// try release file descriptor in the case file have been deleted
	// for that reason just close it and immediately try to open
	err = file.Close()
	if err != nil {
		jp.logger.Fatalf("can't close a file %s: %s", filename, err.Error())
		panic("")
	}

	// todo: here we may have symlink opened, so handle it
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
		if err = file.Close(); err != nil {
			jp.logger.Errorf("can't close file %s %v in case of different inodes", filename, err)
		}
		return maintenanceResultDeleted
	}

	// seek to saved offset
	job.file = file
	job.seek(offset, io.SeekStart, "maintenance")

	job.mu.Unlock()

	return maintenanceResultNoop
}

// deleteJob job should be already locked and it'll be unlocked
func (jp *jobProvider) deleteJobAndUnlock(job *Job) {
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

func getInode(stat os.FileInfo) inodeID {
	return inodeID(stat.Sys().(*syscall.Stat_t).Ino)
}
