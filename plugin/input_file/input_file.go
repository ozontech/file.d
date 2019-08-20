package input_file

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

func init() {
	params := &pipeline.Params{
		"path":        pipeline.Param{},
		"offset_file": pipeline.Param{},
	}

	info := &pipeline.PluginInfo{
		Params:  params,
		Factory: newInputFilePlugin,
	}

	filed.DefaultPluginRegistry.RegisterInput("file", info)
}

type Config struct {
	WatchingDir     string
	OffsetsFilename string
	PersistenceMode string
	ReadBufferSize  int
	ChanLength      int

	offsetsTmpFilename string
	persistenceMode    byte
}

type InputFilePlugin struct {
	config  *Config
	parsers []*pipeline.Parser

	workers     []*worker
	jobProvider *jobProvider
	watcher     *watcher
}

func newInputFilePlugin(config interface{}, controller pipeline.ControllerForPlugin) pipeline.Plugin {
	cfg := config.(*Config)

	if cfg.PersistenceMode == "" {
		cfg.PersistenceMode = defaultPersistenceMode
	}

	switch cfg.PersistenceMode {
	case "timer":
		cfg.persistenceMode = PersistenceModeTimer
	case "async":
		cfg.persistenceMode = PersistenceModeAsync
	case "sync":
		cfg.persistenceMode = PersistenceModeSync
	default:
		logger.Fatalf("wrong persistence mode %q provided, should be one of timer|async|sync", cfg.PersistenceMode)
	}

	if cfg.WatchingDir == "" {
		logger.Fatalf("no watch path")
	}

	if cfg.OffsetsFilename == "" {
		logger.Fatalf("no offset path")
	}

	if cfg.ReadBufferSize == 0 {
		cfg.ReadBufferSize = defaultReadBufferSize
	}

	if cfg.ChanLength == 0 {
		cfg.ChanLength = defaultChanLength
	}

	cfg.offsetsTmpFilename = cfg.OffsetsFilename + ".atomic"

	jobProvider := NewJobProvider(cfg, controller.GetDone())
	watcher := NewWatcher(cfg.WatchingDir, jobProvider)
	plugin := &InputFilePlugin{
		config:      cfg,
		parsers:     controller.GetParsers(),
		jobProvider: jobProvider,
		watcher:     watcher,
	}

	return plugin
}

func (p *InputFilePlugin) Start() {
	logger.Info("starting input_file")


	p.watcher.start()
	p.startWorkers()
	p.jobProvider.start()
}

func (p *InputFilePlugin) Stop() {
	if p.watcher == nil {
		return
	}

	for i := range p.workers {
		p.workers[i].stop()
	}

	p.jobProvider.stop()

	for range p.workers {
		// unblock worker channels to allow goroutines to exit
		p.jobProvider.nextJob <- nil
	}

	p.watcher.stop()

	p.jobProvider.saveOffsets()
}

func (p *InputFilePlugin) startWorkers() {
	p.workers = make([]*worker, len(p.parsers))
	for i := range p.workers {
		p.workers[i] = &worker{}
		p.workers[i].start(p.parsers[i], p.jobProvider, p.config.ReadBufferSize)
	}
	logger.Infof("file read workers created, count=%d", len(p.workers))
}