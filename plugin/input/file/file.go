package file

import (
	"time"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/* {! comment @description
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working(in this case reopen will fail).
Watcher trying to use file system events to watch for files.
But events isn't work with symlinks, so watcher also manually rescans directory by some interval.

**Config example:**
```yaml
pipelines:
  docker:
    type: file
    persistence_mode: async
    watching_dir: /var/lib/docker/containers
    filename_pattern: "*-json.log"
    offsets_file: /data/offsets.yaml
```
!} */
type Plugin struct {
	config *Config
	params *pipeline.InputPluginParams

	workers     []*worker
	jobProvider *jobProvider
}

// {! options #2 #6
type persistenceMode int

const (
	persistenceModeSync  persistenceMode = 0 // sync *! saves offsets file with event commitment. It's very slow, but excludes possibility of events duplication in extreme situations like power loss.
	persistenceModeAsync persistenceMode = 1 // async *! saves offsets file by timer using `persist_interval`. Saving is skipped if offsets haven't been changed. Suitable in most cases, guarantees at least once delivery and makes almost no overhead.
) // !}

// {! options #2 #6
type offsetsOp int

const (
	offsetsOpContinue offsetsOp = 0 // continue *! set offset as saved in offsets file.
	offsetsOpReset    offsetsOp = 1 // reset *! set offset to the beginning of the file.
	offsetsOpTail     offsetsOp = 2 // tail *! set offset to the end of the file.
) // !}

type Config struct {
	// {! params @config /json:\"(.+)\"`/
	WatchingDir     string `json:"watching_dir"`     // *! `string` `required` <br> <br> Directory to watch for files.
	OffsetsFile     string `json:"offsets_file"`     // *! `string` `required` <br> <br> File name into which offsets will be saving. It's simply yaml file. You can read and manually modify it. Offsets are loaded only on initialization.
	FilenamePattern string `json:"filename_pattern"` // *! `string` `default=[! @defaults.defaultFilenamePattern !]` <br> <br> Files which doesn't match this pattern will be ignored. Check out https://golang.org/pkg/path/filepath/#Glob for details.
	/*
		>! `string="[! @persistenceMode.variants !]"` `default=[! @defaults.defaultPersistenceMode !]`
		>!
		>! Defines how to save offsets file:
		>! [! @persistenceMode.legend !]
		>!
		>! Save means doing three stages:
		>! * Write temporary file with all offsets
		>! * Call `fsync()` on it
		>! * Rename temporary file to original
		>!
	*/
	PersistenceMode string `json:"persistence_mode"` // *!

	PersistInterval time.Duration `json:"persist_interval"` // *! `time` `default=[! @defaults.defaultPersistInterval !]` <br> <br> Offsets save interval. Used only if `persistence_mode` is `async`.
	ReadBufferSize  int           `json:"read_buffer_size"` // *! `number` `default=[! @defaults.defaultReadBufferSize !]` <br> <br> Size of buffer to use for file reading. Each worker use own buffer, so memory consumption will be `read_buffer_size*workers_count`.
	MaxFiles        int           `json:"max_files"`        // *! `number` `default=[! @defaults.defaultMaxFiles !]` <br> <br> Max amount of opened files. If the limit is exceeded `file-d` will exit with fatal. Also check your file descriptors limit: `ulimit -n`.

	/*
		>! `string="[! @offsetsOp.variants !]"` default=`[! @defaults.defaultOffsetsOp !]`
		>!
		>! Offset operation which will be preformed when adding file as a job:
		>! [! @offsetsOp.legend !]
		>! > It is only used on initial scan of `watching_dir`. Files which will be catched up later during work always use `reset` operation.
	*/
	OffsetsOp string `json:"offsets_op"` // *!

	/*
		>! `number` default=`[! @defaults.defaultWorkersCount !]`
		>!
		>! How much workers will be instantiated. Each worker:
		>! * Read files (I/O bound)
		>! * Decode events (CPU bound)
		>! > It's recommended to set it to 4x-8x of CPU cores.
	*/
	WorkersCount int `json:"workers_count"` // *!
	// !}

	offsetsTmpFilename string
	persistenceMode    persistenceMode
	offsetsOp          offsetsOp
}

// {! values @defaults #1 #3
const (
	defaultWorkersCount    = 16          // *!
	defaultReadBufferSize  = 131072      // *!
	defaultMaxFiles        = 16384       // *!
	defaultPersistInterval = time.Second // *!
	defaultPersistenceMode = "async"     // *!
	defaultOffsetsOp       = "continue"  // *!
	defaultFilenamePattern = "*"         // *!
)

// !}

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
	case "async":
		p.config.persistenceMode = persistenceModeAsync
	case "sync":
		p.config.persistenceMode = persistenceModeSync
	default:
		logger.Fatalf("wrong persistence mode %q provided, should be one of async|sync", p.config.PersistenceMode)
	}

	if p.config.PersistInterval == 0 {
		p.config.PersistInterval = defaultPersistInterval
	}
	if p.config.WatchingDir == "" {
		logger.Fatalf("no watching_dir provided in config for the file plugin")
	}

	if p.config.FilenamePattern == "" {
		p.config.FilenamePattern = defaultFilenamePattern
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
