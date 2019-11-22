package file

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

const (
	defaultWorkersCount   = 16
	defaultReadBufferSize = 128 * 1024
	defaultMaxFiles       = 16384

	defaultPersistenceMode = "timer"
	defaultOffsetsOp       = "continue"

	persistenceModeSync  persistenceMode = 0
	persistenceModeAsync persistenceMode = 1
	persistenceModeTimer persistenceMode = 2

	offsetsOpContinue offsetsOp = 0
	offsetsOpReset    offsetsOp = 1
	offsetsOpTail     offsetsOp = 2
)

type (
	persistenceMode int
	offsetsOp       int
)

type Config struct {
	WatchingDir     string `json:"watching_dir"`
	FilenamePattern string `json:"filename_pattern"`
	OffsetsFile     string `json:"offsets_file"`
	PersistenceMode string `json:"persistence_mode"`
	ReadBufferSize  int    `json:"read_buffer_size"`
	MaxFiles        int    `json:"max_files"`
	OffsetsOp       string `json:"offsets_op"` // continue|tail|reset
	WorkersCount    int    `json:"workers_count"`

	offsetsTmpFilename string
	persistenceMode    persistenceMode
	offsetsOp          offsetsOp
}

type Plugin struct {
	config *Config
	params *pipeline.InputPluginParams

	workers     []*worker
	jobProvider *jobProvider

	isFinalSaveDisabled bool
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "file",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	logger.Info("starting file input plugin")

	p.params = params
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the file plugin")
	}

	if p.config.OffsetsOp == "" {
		p.config.OffsetsOp = defaultOffsetsOp
	}

	switch p.config.OffsetsOp {
	case "continue":
		p.config.offsetsOp = offsetsOpContinue
	case "reset":
		p.config.offsetsOp = offsetsOpReset
	case "tail":
		p.config.offsetsOp = offsetsOpTail
	default:
		logger.Fatalf("wrong offsets operation %q provided, should be one of continue|reset|tail", p.config.OffsetsOp)
	}

	if p.config.PersistenceMode == "" {
		p.config.PersistenceMode = defaultPersistenceMode
	}

	switch p.config.PersistenceMode {
	case "timer":
		p.config.persistenceMode = persistenceModeTimer
	case "async":
		p.config.persistenceMode = persistenceModeAsync
	case "sync":
		p.config.persistenceMode = persistenceModeSync
	default:
		logger.Fatalf("wrong persistence mode %q provided, should be one of timer|async|sync", p.config.PersistenceMode)
	}

	if p.config.WatchingDir == "" {
		logger.Fatalf("no watching_dir provided in config for the file plugin")
	}

	if p.config.FilenamePattern == "" {
		p.config.FilenamePattern = "*"
	}

	if p.config.WorkersCount == 0 {
		p.config.WorkersCount = defaultWorkersCount
	}

	if p.config.OffsetsFile == "" {
		logger.Fatalf("no offsets_file provided in config for the file plugin")
	}

	if p.config.ReadBufferSize == 0 {
		p.config.ReadBufferSize = defaultReadBufferSize
	}

	if p.config.MaxFiles == 0 {
		p.config.MaxFiles = defaultMaxFiles
	}

	p.config.offsetsTmpFilename = p.config.OffsetsFile + ".atomic"

	p.jobProvider = NewJobProvider(p.config, p.params.DoneWg, p.params.Controller)
	p.startWorkers()
	p.jobProvider.start()
}

func (p *Plugin) startWorkers() {
	p.workers = make([]*worker, p.config.WorkersCount)
	for i := range p.workers {
		p.workers[i] = &worker{}
		p.workers[i].start(i, p.params.Controller, p.jobProvider, p.config.ReadBufferSize)
	}

	logger.Infof("file read workers created, count=%d", len(p.workers))
}

func (p *Plugin) Commit(event *pipeline.Event) {
	p.jobProvider.commit(event)
}

func (p *Plugin) Stop() {
	logger.Infof("stopping %d workers", len(p.workers))
	for range p.workers {
		p.jobProvider.jobsChan <- nil
	}

	logger.Infof("stopping job provider")
	p.jobProvider.stop()

	if !p.isFinalSaveDisabled {
		logger.Infof("saving last known offsets")
		p.jobProvider.saveOffsets()
	}
}

func (p *Plugin) disableFinalSave() {
	p.isFinalSaveDisabled = true
}
