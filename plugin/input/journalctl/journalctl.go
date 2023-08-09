//go:build linux

package journalctl

import (
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/offset"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
Reads `journalctl` output.
}*/

type Plugin struct {
	params        *pipeline.InputPluginParams
	config        *Config
	reader        *journalReader
	currentOffset int64
	logger        *zap.Logger

	commiter Commiter

	//  plugin metrics

	offsetErrorsMetric        *prometheus.CounterVec
	journalCtlStopErrorMetric *prometheus.CounterVec
	readerErrorsMetric        *prometheus.CounterVec
}

type persistenceMode int

const (
	// ! "persistenceMode" #1 /`([a-z]+)`/
	persistenceModeAsync persistenceMode = iota // * `async` – it periodically saves the offsets using `async_interval`. The saving operation is skipped if offsets haven't been changed. Suitable, in most cases, it guarantees at least one delivery and makes almost no overhead.
	persistenceModeSync                         // * `sync` – saves offsets as part of event commitment. It's very slow but excludes the possibility of event duplication in extreme situations like power loss.
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The filename to store offsets of processed messages.
	OffsetsFile string `json:"offsets_file" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Additional args for `journalctl`.
	// > Plugin forces "-o json" and "-c *cursor*" or "-n all", otherwise
	// > you can use any additional args.
	// >> Have a look at https://man7.org/linux/man-pages/man1/journalctl.1.html
	JournalArgs []string `json:"journal_args" default:"-f -a"` // *

	// for testing mostly
	MaxLines int `json:"max_lines"`

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

	// > @3@4@5@6
	// >
	// > Offsets saving interval. Only used if `persistence_mode` is set to `async`.
	AsyncInterval  cfg.Duration `json:"async_interval" default:"1s" parse:"duration"`
	AsyncInterval_ time.Duration
}

type offsetInfo struct {
	Offset int64  `json:"offset"`
	Cursor string `json:"cursor"`
}

func (o *offsetInfo) set(cursor string) {
	o.Cursor = cursor
	o.Offset++
}

func (p *Plugin) Write(bytes []byte) (int, error) {
	p.params.Controller.In(0, "journalctl", p.currentOffset, bytes, false)
	p.currentOffset++
	return len(bytes), nil
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "journalctl",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.params = params
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.registerMetrics(params.MetricCtl)

	offInfo := &offsetInfo{}
	if err := offset.LoadYAML(p.config.OffsetsFile, offInfo); err != nil {
		p.offsetErrorsMetric.WithLabelValues().Inc()
		p.logger.Error("can't load offset file", zap.Error(err))
	}

	if p.config.PersistenceMode_ == persistenceModeAsync {
		if p.config.AsyncInterval_ < 0 {
			p.logger.Fatal("invalid async interval", zap.Duration("interval", p.config.AsyncInterval_))
		}
		p.commiter = NewAsyncCommiter(NewDebouncer(p.config.AsyncInterval_), p.sync)
	} else {
		p.commiter = NewSyncCommiter(p.sync)
	}

	readConfig := &journalReaderConfig{
		output:   p,
		cursor:   offInfo.Cursor,
		maxLines: p.config.MaxLines,
		logger:   p.logger,
	}
	p.reader = newJournalReader(readConfig, p.readerErrorsMetric)
	p.reader.args = append(p.reader.args, p.config.JournalArgs...)
	if err := p.reader.start(); err != nil {
		p.logger.Fatal("failure during start", zap.Error(err))
	}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.offsetErrorsMetric = ctl.RegisterCounter("input_journalctl_offset_errors", "Number of errors occurred when saving/loading offset")
	p.journalCtlStopErrorMetric = ctl.RegisterCounter("input_journalctl_stop_errors", "Total journalctl stop errors")
	p.readerErrorsMetric = ctl.RegisterCounter("input_journalctl_reader_errors", "Total reader errors")
}

func (p *Plugin) Stop() {
	err := p.reader.stop()
	if err != nil {
		p.journalCtlStopErrorMetric.WithLabelValues().Inc()
		p.logger.Error("can't stop journalctl cmd", zap.Error(err))
	}

	p.commiter.Shutdown()
}

func (p *Plugin) Commit(event *pipeline.Event) {
	p.commiter.Commit(event)
}

func (p *Plugin) sync(offInfo offsetInfo) {
	if err := offset.SaveYAML(p.config.OffsetsFile, offInfo); err != nil {
		p.offsetErrorsMetric.WithLabelValues().Inc()
		p.logger.Error("can't save offset file", zap.Error(err))
	}
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}
