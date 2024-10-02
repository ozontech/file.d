//go:build linux

package journalctl

import (
	"strings"
	"sync/atomic"

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
	offInfo       atomic.Pointer[offsetInfo]
	currentOffset int64
	logger        *zap.Logger

	//  plugin metrics
	offsetErrorsMetric        prometheus.Counter
	journalCtlStopErrorMetric prometheus.Counter
	readerErrorsMetric        prometheus.Counter
}

type Config struct {
	// ! config-params
	// ^ config-params

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
	p.params.Controller.In(0, "journalctl", Offset(p.currentOffset), bytes, false, nil)
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
		p.offsetErrorsMetric.Inc()
		p.logger.Error("can't load offset file", zap.Error(err))
	}
	p.offInfo.Store(offInfo)

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
		p.journalCtlStopErrorMetric.Inc()
		p.logger.Error("can't stop journalctl cmd", zap.Error(err))
	}

	offsets := *p.offInfo.Load()
	if err := offset.SaveYAML(p.config.OffsetsFile, offsets); err != nil {
		p.offsetErrorsMetric.Inc()
		p.logger.Error("can't save offset file", zap.Error(err))
	}
}

func (p *Plugin) Commit(event *pipeline.Event) {
	offInfo := *p.offInfo.Load()
	offInfo.set(strings.Clone(event.Root.Dig("__CURSOR").AsString()))
	p.offInfo.Store(&offInfo)

	if err := offset.SaveYAML(p.config.OffsetsFile, offInfo); err != nil {
		p.offsetErrorsMetric.Inc()
		p.logger.Error("can't save offset file", zap.Error(err))
	}
}

func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	return true
}

type Offset int64

func (o Offset) Current() int64 {
	return int64(o)
}

func (o Offset) ByStream(_ string) int64 {
	panic("unimplemented")
}
