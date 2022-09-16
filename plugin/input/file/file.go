package file

import (
	"net/http"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It watches for files in the provided directory and reads them line by line.

Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.

From time to time, it instantly releases and reopens descriptors of the completely processed files.
Such behavior allows files to be deleted by a third party software even though `file.d` is still working (in this case the reopening will fail).

A watcher is trying to use the file system events to detect file creation and updates.
But update events don't work with symlinks, so watcher also periodically manually `fstat` all tracking files to detect changes.

> ⚠ It supports the commitment mechanism. But "least once delivery" is guaranteed only if files aren't being truncated.
> However, `file.d` correctly handles file truncation, there is a little chance of data loss.
> It isn't a `file.d` issue. The data may have been written just before the file truncation. In this case, you may miss to read some events.
> If you care about the delivery, you should also know that the `logrotate` manual clearly states that copy/truncate may cause data loss even on a rotating stage.
> So use copy/truncate or similar actions only if your data isn't critical.
> In order to reduce potential harm of truncation, you can turn on notifications of file changes.
> By default the plugin is notified only on file creations. Note that following for changes is more CPU intensive.


**Reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    input:
        type: file
        watching_dir: /var/lib/docker/containers
        offsets_file: /data/offsets.yaml
        filename_pattern: "*-json.log"
        persistence_mode: async
```
}*/

const (
	subsystemName                      = "input_file"
	possibleOffsetCorruptionCounter    = "possible_offset_corruptions_total"
	alreadyWrittenEventsSkippedCounter = "already_written_event_skipped_total"
)

type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
	params *pipeline.InputPluginParams

	workers     []*worker
	jobProvider *jobProvider
}

type persistenceMode int

const (
	// ! "persistenceMode" #1 /`([a-z]+)`/
	persistenceModeAsync persistenceMode = iota // * `async` – it periodically saves the offsets using `async_interval`. The saving operation is skipped if offsets haven't been changed. Suitable, in most cases, it guarantees at least one delivery and makes almost no overhead.
	persistenceModeSync                         // * `sync` – saves offsets as part of event commitment. It's very slow but excludes the possibility of event duplication in extreme situations like power loss.
)

type offsetsOp int

const (
	// ! "offsetsOp" #1 /`(.+)`/
	offsetsOpContinue offsetsOp = iota // * `continue` – uses an offset file
	offsetsOpTail                      // * `tail` – sets an offset to the end of the file
	offsetsOpReset                     // * `reset` – resets an offset to the beginning of the file
)

type Config struct {
	// ! config-params
	// ^ config-params

	// > @3@4@5@6
	// >
	// > The source directory to watch for files to process. All subdirectories also will be watched. E.g. if files have
	// > `/var/my-logs/$YEAR/$MONTH/$DAY/$HOST/$FACILITY-$PROGRAM.log` structure, `watching_dir` should be `/var/my-logs`.
	// > Also the `filename_pattern`/`dir_pattern` is useful to filter needless files/subdirectories. In the case of using two or more
	// > different directories, it's recommended to setup separate pipelines for each.
	WatchingDir string `json:"watching_dir" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The filename to store offsets of processed files. Offsets are loaded only on initialization.
	// > > It's a `yaml` file. You can modify it manually.
	OffsetsFile    string `json:"offsets_file" required:"true"` // *
	OffsetsFileTmp string

	// > @3@4@5@6
	// >
	// > Files that don't meet this pattern will be ignored.
	// > > Check out [func Glob docs](https://golang.org/pkg/path/filepath/#Glob) for details.
	FilenamePattern string `json:"filename_pattern" default:"*"` // *

	// > @3@4@5@6
	// >
	// > Dirs that don't meet this pattern will be ignored.
	// > > Check out [func Glob docs](https://golang.org/pkg/path/filepath/#Glob) for details.
	DirPattern string `json:"dir_pattern" default:"*"` // *

	// > @3@4@5@6
	// >
	// > It defines how to save the offsets file:
	// > @persistenceMode|comment-list
	// >
	// > Save operation takes three steps:
	// > *  Write the temporary file with all offsets;
	// > *  Call `fsync()` on it;
	// > *  Rename the temporary file to the original one.
	PersistenceMode  string `json:"persistence_mode" default:"async" options:"async|sync"` // *
	PersistenceMode_ persistenceMode

	AsyncInterval  cfg.Duration `json:"async_interval" default:"1s" parse:"duration"` // *! @3 @4 @5 @6 <br> <br> Offsets saving interval. Only used if `persistence_mode` is set to `async`.
	AsyncInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > The buffer size used for the file reading.
	// > > Each worker uses its own buffer so that final memory consumption will be `read_buffer_size*workers_count`.
	ReadBufferSize int `json:"read_buffer_size" default:"131072"` // *

	// > @3@4@5@6
	// >
	// > The max amount of opened files. If the limit is exceeded, `file.d` will exit with fatal.
	// > > Also, it checks your system's file descriptors limit: `ulimit -n`.
	MaxFiles int `json:"max_files" default:"16384"` // *

	// > @3@4@5@6
	// >
	// > An offset operation which will be performed when you add a file as a job:
	// > @offsetsOp|comment-list
	// > > It is only used on an initial scan of `watching_dir`. Files that will be caught up later during work will always use `reset` operation.
	OffsetsOp  string `json:"offsets_op" default:"continue" options:"continue|tail|reset"` // *
	OffsetsOp_ offsetsOp

	// > @3@4@5@6
	// >
	// > It defines how many workers will be instantiated.
	// > Each worker:
	// > * Reads files (I/O bound)
	// > * Decodes events (CPU bound)
	// > > We recommend to set it to 4x-8x of CPU cores.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*8" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > It defines how often to report statistical information to stdout
	ReportInterval  cfg.Duration `json:"report_interval" default:"5s" parse:"duration"` // *
	ReportInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how often to perform maintenance
	// > @maintenance
	MaintenanceInterval  cfg.Duration `json:"maintenance_interval" default:"10s" parse:"duration"` // *
	MaintenanceInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > It turns on watching for file modifications. Turning it on cause more CPU work, but it is more probable to catch file truncation
	ShouldWatchChanges bool `json:"should_watch_file_changes" default:"false"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "file",
		Factory: Factory,
		Endpoints: map[string]func(http.ResponseWriter, *http.Request){
			"reset": ResetterRegistryInstance.Reset,
		},
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.logger = params.Logger
	p.params = params
	p.config = config.(*Config)

	p.registerPluginMetrics()

	p.config.OffsetsFileTmp = p.config.OffsetsFile + ".atomic"

	p.jobProvider = NewJobProvider(p.config, p.params.Controller, p.logger)

	ResetterRegistryInstance.AddResetter(params.PipelineName, p)

	p.startWorkers()
	p.jobProvider.start()
}

func (p *Plugin) registerPluginMetrics() {
	metric.RegisterCounter(&metric.MetricDesc{
		Subsystem: subsystemName,
		Name:      possibleOffsetCorruptionCounter,
		Help:      "Total number of possible offset corruptions",
	})
	metric.RegisterCounter(&metric.MetricDesc{
		Subsystem: subsystemName,
		Name:      alreadyWrittenEventsSkippedCounter,
		Help:      "Total number of skipped events that was already written",
	})
}

func (p *Plugin) startWorkers() {
	p.workers = make([]*worker, p.config.WorkersCount_)
	for i := range p.workers {
		p.workers[i] = &worker{
			maxEventSize: p.params.PipelineSettings.MaxEventSize,
		}
		p.workers[i].start(p.params.Controller, p.jobProvider, p.config.ReadBufferSize, p.logger)
	}

	p.logger.Infof("workers created, count=%d", len(p.workers))
}

func (p *Plugin) Commit(event *pipeline.Event) {
	p.jobProvider.commit(event)
}

func (p *Plugin) Stop() {
	p.logger.Infof("stopping %d workers", len(p.workers))
	for range p.workers {
		p.jobProvider.jobsChan <- nil
	}

	p.logger.Infof("stopping job provider")
	p.jobProvider.stop()
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	p.jobProvider.jobsMu.RLock()
	job := p.jobProvider.jobs[event.SourceID]
	p.jobProvider.jobsMu.RUnlock()

	savedOffset, exist := job.offsets.get(pipeline.StreamName(event.StreamNameBytes()))
	if !exist {
		// this is new savedOffset therefore message new as well.
		return true
	}
	// event.Offset must be newer that saved one. Otherwise, this event was passed&committed
	// and file-d went down after commit
	pass := event.Offset > savedOffset
	if !pass {
		metric.GetCounter(subsystemName, alreadyWrittenEventsSkippedCounter).Inc()
		return false
	}

	return true
}
