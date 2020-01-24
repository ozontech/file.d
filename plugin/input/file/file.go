package file

import (
	"time"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working (in this case reopen will fail).
Watcher is trying to use file system events detect file creation and updates.
But update events don't work with symlinks, so watcher also manually `fstat` all tracking files to detect changes.

**Config example for reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    type: file
    watching_dir: /var/lib/docker/containers
    offsets_file: /data/offsets.yaml
    filename_pattern: "*-json.log"
    persistence_mode: async
```
}*/
type Plugin struct {
	config *Config
	params *pipeline.InputPluginParams

	workers     []*worker
	jobProvider *jobProvider
}

type persistenceMode int

const (
	//! persistenceMode #1 /`([a-z]+)`/
	persistenceModeAsync persistenceMode = iota //* `async` – periodically saves offsets using `async_interval`. Saving is skipped if offsets haven't been changed. Suitable in most cases, guarantees at least once delivery and makes almost no overhead.
	persistenceModeSync                         //* `sync` – saves offsets as part of event commitment. It's very slow, but excludes possibility of events duplication in extreme situations like power loss.
)

type offsetsOp int

const (
	//! offsetsOp #1 /`(.+)`/
	offsetsOpContinue offsetsOp = iota //* `continue` – use offset file
	offsetsOpTail                      //* `tail` – set offset to the end of the file
	offsetsOpReset                     //* `reset` – reset offset to the beginning of the file
)

type Config struct {
	//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
	//^ _ _ code /`default=%s`/ code /`options=%s`/

	WatchingDir string `json:"watching_dir" required:"true"` //* @3 @4 @5 @6 <br> <br> Directory to watch for new files.

	//> @3 @4 @5 @6
	//>
	//> File name to store offsets of processing files. Offsets are loaded only on initialization.
	//> > It's simply a `yaml` file. You can modify it manually.
	OffsetsFile    string `json:"offsets_file" required:"true"` //*
	OffsetsFileTmp string

	//> @3 @4 @5 @6
	//>
	//> Files which doesn't match this pattern will be ignored.
	//> > Check out https://golang.org/pkg/path/filepath/#Glob for details.
	FilenamePattern string `json:"filename_pattern" default:"*"` //*

	//> @3 @4 @5 @6
	//> 
	//>  Defines how to save the offsets file:
	//>  @persistenceMode|comment-list
	//>
	//>  Saving is done in three steps:
	//>  * Write temporary file with all offsets
	//>  * Call `fsync()` on it
	//>  * Rename temporary file to the original one
	PersistenceMode  string `json:"persistence_mode" default:"async" options:"async|sync"` //*
	PersistenceMode_ persistenceMode

	AsyncInterval  fd.Duration `json:"async_interval" default:"1s" parse:"duration"` // *! @3 @4 @5 @6 <br> <br> Offsets save interval. Only used if `persistence_mode` is `async`.
	AsyncInterval_ time.Duration

	//> @3 @4 @5 @6
	//>
	//>  Size of buffer to use for file reading.
	//>  > Each worker use own buffer, so final memory consumption will be `read_buffer_size*workers_count`.
	ReadBufferSize int `json:"read_buffer_size" default:"131072"` //*

	//>  @3 @4 @5 @6
	//>
	//>  Max amount of opened files. If the limit is exceeded `file-d` will exit with fatal.
	//>  > Also check your system file descriptors limit: `ulimit -n`.
	MaxFiles int `json:"max_files" default:"16384"` //*

	//>  @3 @4 @5 @6
	//>
	//>  Offset operation which will be preformed when adding file as a job:
	//>  @offsetsOp|comment-list
	//>  > It is only used on initial scan of `watching_dir`. Files which will be caught up later during work, will always use `reset` operation.
	OffsetsOp  string `json:"offsets_op" default:"continue" options:"continue|tail|reset"` //*
	OffsetsOp_ offsetsOp

	//>  @3 @4 @5 @6
	//>
	//>  How much workers will be instantiated. Each worker:
	//>  * Read files (I/O bound)
	//>  * Decode events (CPU bound)
	//>  > It's recommended to set it to 4x-8x of CPU cores.
	WorkersCount int `json:"workers_count" default:"16"` //*

	ReportInterval  fd.Duration `json:"report_interval" default:"10s" parse:"duration"` //* @3 @4 @5 @6 <br> <br> How often to report statistical information to stdout
	ReportInterval_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> How often to perform maintenance.
	//> @maintenance
	MaintenanceInterval  fd.Duration `json:"maintenance_interval" default:"10s" parse:"duration"` //*
	MaintenanceInterval_ time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
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

	p.config.OffsetsFileTmp = p.config.OffsetsFile + ".atomic"

	p.jobProvider = NewJobProvider(p.config, p.params.Controller)
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
}
