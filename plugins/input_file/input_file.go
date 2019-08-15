package input_file

import (
	"github.com/bitly/go-simplejson"
	"github.com/fsnotify/fsnotify"
	"github.com/satori/go.uuid"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/global"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"os"
	"path/filepath"
)

func init() {
	params := &filed.Params{
		"path":        filed.Param{},
		"offset_file": filed.Param{},
	}

	info := &filed.PluginInfo{
		Params:  params,
		Factory: newInputFilePlugin,
	}

	filed.DefaultPluginRegistry.RegisterInput("file", info)
}

type InputFilePlugin struct {
	args         *simplejson.Json
	createEvents int
	workers      []*worker
	jobProvider  *jobProvider
	watcher      *fsnotify.Watcher
	parsers      []*pipeline.Parser
	path         string
}

func newInputFilePlugin(args *simplejson.Json, parsers []*pipeline.Parser) filed.Plugin {
	plugin := &InputFilePlugin{
		args:        args,
		parsers:     parsers,
		jobProvider: NewJobProvider(),
		path:        args.Get("path").MustString(),
	}

	readBufferSize := args.Get("read_buffer_size").MustInt()
	if readBufferSize == 0 {
		readBufferSize = 256 * 1024
	}
	plugin.createWorkers(readBufferSize)
	plugin.loadJobs()

	return plugin
}

func (p *InputFilePlugin) Start() {
	global.Logger.Info("starting input_file")
	p.startWatcher(p.path)

	go p.jobProvider.run()
}

func (p *InputFilePlugin) startWatcher(path string) {
	watcher, err := fsnotify.NewWatcher()
	p.watcher = watcher
	if err != nil {
		global.Logger.Panic(err)
	}
	go p.watchEvents(p.watcher)
	go p.watchErrors(p.watcher)

	err = p.watcher.Add(path)
	if err != nil {
		global.Logger.Panic(err)
	}
}

func (p *InputFilePlugin) createWorkers(readBufferSize int) {
	global.Logger.Info("creating workers")
	workersCount := len(p.parsers)
	p.workers = make([]*worker, workersCount)
	for i := range p.workers {
		p.workers[i] = &worker{
			id: uuid.NewV4().String(),
		}
		go p.workers[i].start(p.parsers[i], p.jobProvider, readBufferSize)
	}
	global.Logger.Info("workers created")
}

func (p *InputFilePlugin) loadJobs() {
	global.Logger.Info("loading jobs")
	files, err := filepath.Glob(filepath.Join(p.path, "*"))
	if err != nil {
		global.Logger.Panic(err)
	}
	for _, filename := range files {
		stat, err := os.Stat(filename)
		if err != nil {
			continue
		}
		if stat.IsDir() {
			continue
		}

		p.jobProvider.addJob(filename)
	}
	global.Logger.Info("jobs loaded")
}

func (p *InputFilePlugin) Stop() {
	err := p.watcher.Close()
	if err != nil {
		global.Logger.Panic(err)
	}

}

func (p *InputFilePlugin) watchEvents(watcher *fsnotify.Watcher) {
	global.Logger.Info("watching events started")
	for {
		event, ok := <-watcher.Events
		if !ok {
			return
		}

		filename := event.Name
		if event.Op&fsnotify.Create == fsnotify.Create {
			global.Logger.Infof("found new file %q", filename)
			p.createEvents++

			p.jobProvider.addJob(filename)
		}

		if event.Op&fsnotify.Write == fsnotify.Write {
			p.jobProvider.addJob(filename)
		}
	}
}

func (p *InputFilePlugin) watchErrors(watcher *fsnotify.Watcher) {
	global.Logger.Info("watching errors started")
	for {
		err, ok := <-watcher.Errors
		if !ok {
			return
		}
		global.Logger.Panic(err)
	}
}
