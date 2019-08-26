package inputfile

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

const (
	defaultReadBufferSize = 256 * 1024
	defaultChanLength     = 256

	defaultPersistenceMode = "timer"

	PersistenceModeSync  = 0
	PersistenceModeAsync = 1
	PersistenceModeTimer = 2
)

type Config struct {
	WatchingDir     string `json:"watching_dir"`
	OffsetsFile     string `json:"offsets_file"`
	PersistenceMode string `json:"persistence_mode"`
	ReadBufferSize  int    `json:"read_buffer_size"`
	ChanLength      int    `json:"chan_length"`
	ResetOffsets    bool   `json:"reset_offsets"`

	offsetsTmpFilename string
	persistenceMode    byte
}

type FilePlugin struct {
	config *Config
	heads  []*pipeline.Head

	workers     []*worker
	jobProvider *jobProvider
	watcher     *watcher

	isFinalSaveDisabled bool
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "file",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPluginPointer, pipeline.AnyConfigPointer) {
	return &FilePlugin{}, &Config{}
}

func (p *FilePlugin) Start(config pipeline.AnyConfigPointer, pipeline pipeline.Pipeline) {
	logger.Info("starting file input plugin")

	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the file plugin")
	}

	if p.config.PersistenceMode == "" {
		p.config.PersistenceMode = defaultPersistenceMode
	}

	switch p.config.PersistenceMode {
	case "timer":
		p.config.persistenceMode = PersistenceModeTimer
	case "async":
		p.config.persistenceMode = PersistenceModeAsync
	case "sync":
		p.config.persistenceMode = PersistenceModeSync
	default:
		logger.Fatalf("wrong persistence mode %q provided, should be one of timer|async|sync", p.config.PersistenceMode)
	}

	if p.config.WatchingDir == "" {
		logger.Fatalf("no watching_dir provided in config for the file plugin")
	}

	if p.config.OffsetsFile == "" {
		logger.Fatalf("no offsets_file provided in config for the file plugin")
	}

	if p.config.ReadBufferSize == 0 {
		p.config.ReadBufferSize = defaultReadBufferSize
	}

	if p.config.ChanLength == 0 {
		p.config.ChanLength = defaultChanLength
	}

	p.config.offsetsTmpFilename = p.config.OffsetsFile + ".atomic"

	p.heads = pipeline.GetHeads()
	p.jobProvider = NewJobProvider(p.config, pipeline.GetDoneWg())
	p.watcher = NewWatcher(p.config.WatchingDir, p.jobProvider)

	p.watcher.start()
	p.startWorkers()
	p.jobProvider.start()
}

func (p *FilePlugin) disableFinalSave() {
	p.isFinalSaveDisabled = true
}

func (p *FilePlugin) Stop() {
	if p.watcher == nil {
		return
	}

	logger.Infof("stopping %d workers", len(p.workers))
	for i := range p.workers {
		p.workers[i].stop()
	}

	logger.Infof("stopping job provider")
	p.jobProvider.stop()

	logger.Infof("stopping watcher")
	p.watcher.stop()

	if !p.isFinalSaveDisabled {
		logger.Infof("saving last known offsets")
		p.jobProvider.saveOffsets()
	}
}

func (p *FilePlugin) startWorkers() {
	p.workers = make([]*worker, len(p.heads))
	for i := range p.workers {
		p.workers[i] = &worker{stopCh: make(chan bool)}
		p.workers[i].start(p.heads[i], p.jobProvider, p.config.ReadBufferSize)
	}
	logger.Infof("file read workers created, count=%d", len(p.workers))
}
