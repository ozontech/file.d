package input_file

import (
	"gitlab.ozon.ru/sre/filed/global"
	"go.uber.org/atomic"
	"os"
	"sync"
)

type job struct {
	file      *os.File
	id        int
	isAtEnd   bool
	isRunning bool
	mu        *sync.Mutex
}

type jobProvider struct {
	activeJobs chan *job
	jobs       map[string]*job

	mu      *sync.Mutex
	nextJob chan *job

	atEndJobs *atomic.Int32

	jobsDone sync.WaitGroup
}

func NewJobProvider() *jobProvider {
	jp := &jobProvider{
		jobsDone:   sync.WaitGroup{},
		activeJobs: make(chan *job, 1000),
		jobs:       make(map[string]*job, 1000),
		mu:         &sync.Mutex{},
		nextJob:    make(chan *job, 4),
		atEndJobs:  atomic.NewInt32(0),
	}

	return jp
}

func (jp *jobProvider) run() {
	global.Logger.Infof("job provider started")
	for {
		job := <-jp.activeJobs

		job.mu.Lock()
		if job.isAtEnd || job.isRunning {
			panic("why run? is't at end or already running")
		}
		job.isRunning = true
		job.mu.Unlock()

		jp.nextJob <- job
	}
}

func (jp *jobProvider) addJob(filename string) {
	jp.mu.Lock()
	defer jp.mu.Unlock()

	existingJob, has := jp.jobs[filename]

	if has {
		jp.resumeJob(existingJob)
		return
	}

	job := jp.makeJob(filename)
	jp.jobs[filename] = job

	global.Logger.Infof("job %q added len=%d", filename, len(jp.jobs))
	jp.resumeJob(job)
}

func (jp *jobProvider) resumeJob(job *job) {
	job.mu.Lock()
	if !job.isAtEnd {
		job.mu.Unlock()
		return
	}

	job.isAtEnd = false

	jp.jobsDone.Add(1)

	v := jp.atEndJobs.Dec()
	if v < 0 {
		global.Logger.Panicf("at end jobs counter less than zero")
	}
	job.mu.Unlock()

	jp.activeJobs <- job
}

func (jp *jobProvider) releaseJob(job *job, isAtEnd bool) {
	job.mu.Lock()
	if !job.isRunning || job.isAtEnd {
		global.Logger.Panicf("job isn't running, why release?")
	}

	job.isRunning = false
	job.isAtEnd = isAtEnd
	v := 0
	if isAtEnd {
		v = int(jp.atEndJobs.Inc())
		if v > len(jp.jobs) {
			global.Logger.Panicf("at end jobs counter more than job count")
		}
		jp.jobsDone.Done()
	}
	job.mu.Unlock()

	if !isAtEnd {
		jp.activeJobs <- job
	}
}

func (jp *jobProvider) makeJob(filename string) *job {
	file, err := os.Open(filename)
	if err != nil {
		global.Logger.Panic(err)
	}
	jp.atEndJobs.Inc()
	return &job{file: file, mu: &sync.Mutex{}, id: len(jp.jobs), isAtEnd: true}
}
