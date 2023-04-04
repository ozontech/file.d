//go:build linux

package journalctl

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/offset"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

/*{ introduction
Reads `journalctl` output.
}*/

type Plugin struct {
	params  *pipeline.InputPluginParams
	config  *Config
	reader  *journalReader
	offInfo *offsetInfo

	//  plugin metrics

	offsetErrorsMetric        *prometheus.CounterVec
	journalCtlStopErrorMetric *prometheus.CounterVec
	readerErrorsMetric        *prometheus.CounterVec
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

	current int64
}

func (o *offsetInfo) set(cursor string) {
	o.Cursor = cursor
	o.Offset++
}

func (p *Plugin) Write(bytes []byte) (int, error) {
	p.params.Controller.In(0, "journalctl", p.offInfo.current, bytes, false)
	p.offInfo.current++
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
	p.registerMetrics(params.MetricCtl)

	p.offInfo = &offsetInfo{}
	if err := offset.LoadYAML(p.config.OffsetsFile, p.offInfo); err != nil {
		p.offsetErrorsMetric.WithLabelValues().Inc()
		p.params.Logger.Error("can't load offset file: %s", err.Error())
	}

	readConfig := &journalReaderConfig{
		output:   p,
		cursor:   p.offInfo.Cursor,
		maxLines: p.config.MaxLines,
		logger:   p.params.Logger,
	}
	p.reader = newJournalReader(readConfig, p.readerErrorsMetric)
	p.reader.args = append(p.reader.args, p.config.JournalArgs...)
	if err := p.reader.start(); err != nil {
		p.params.Logger.Error("failure during start: %s", err.Error())
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
		p.params.Logger.Error("can't stop journalctl cmd: %s", err.Error())
	}

	if err := offset.SaveYAML(p.config.OffsetsFile, p.offInfo); err != nil {
		p.offsetErrorsMetric.WithLabelValues().Inc()
		p.params.Logger.Error("can't save offset file: %s", err.Error())
	}
}

func (p *Plugin) Commit(event *pipeline.Event) {
	p.offInfo.set(pipeline.CloneString(event.Root.Dig("__CURSOR").AsString()))

	if err := offset.SaveYAML(p.config.OffsetsFile, p.offInfo); err != nil {
		p.offsetErrorsMetric.WithLabelValues().Inc()
		p.params.Logger.Error("can't save offset file: %s", err.Error())
	}
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	return true
}
