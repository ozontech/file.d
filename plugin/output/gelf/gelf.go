package gelf

import (
	"context"
	"strings"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be greater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version` *`string=1.1`*
* `host` *`string`*
* `short_message` *`string`*
* `full_message` *`string`*
* `timestamp` *`number`*
* `level` *`number`*
* `_extra_field_1` *`string`*
* `_extra_field_2` *`string`*
* `_extra_field_3` *`string`*

Every field with an underscore prefix `_` will be treated as an extra field.
Allowed characters in field names are letters, numbers, underscores, dashes, and dots.
}*/

const (
	outPluginType = "gelf"
)

type Plugin struct {
	config       *Config
	logger       *zap.SugaredLogger
	avgEventSize int
	batcher      *pipeline.RetriableBatcher
	controller   pipeline.OutputPluginController

	// plugin metrics
	sendErrorMetric prometheus.Counter
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > An address of gelf endpoint. Format: `HOST:PORT`. E.g. `localhost:12201`.
	Endpoint string `json:"endpoint" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The plugin reconnects to endpoint periodically using this interval. It is useful if an endpoint is a load balancer.
	ReconnectInterval  cfg.Duration `json:"reconnect_interval" default:"1m" parse:"duration"` // *
	ReconnectInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > How much time to wait for the connection?
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s" parse:"duration"` // *
	ConnectionTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > How much time to wait for the connection?
	WriteTimeout  cfg.Duration `json:"write_timeout" default:"10s" parse:"duration"` // *
	WriteTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Which field of the event should be used as `host` GELF field.
	HostField string `json:"host_field" default:"host"` // *

	// > @3@4@5@6
	// >
	// >  Which field of the event should be used as `short_message` GELF field.
	ShortMessageField string `json:"short_message_field" default:"message"` // *

	// > @3@4@5@6
	// >
	// >  The default value for `short_message` GELF field if nothing is found in the event.
	DefaultShortMessageValue string `json:"default_short_message_value" default:"not set"` // *

	// > @3@4@5@6
	// >
	// > Which field of the event should be used as `full_message` GELF field.
	FullMessageField string `json:"full_message_field" default:""` // *

	// > @3@4@5@6
	// >
	// > Which field of the event should be used as `timestamp` GELF field.
	TimestampField string `json:"timestamp_field" default:"time"` // *

	// > @3@4@5@6
	// >
	// > In which format timestamp field should be parsed.
	TimestampFieldFormat string `json:"timestamp_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|unixtime"` // *

	// > @3@4@5@6
	// >
	// > Which field of the event should be used as a `level` GELF field. Level field should contain level number or string according to RFC 5424:
	// > * `7` or `debug`
	// > * `6` or `info`
	// > * `5` or `notice`
	// > * `4` or `warning`
	// > * `3` or `error`
	// > * `2` or `critical`
	// > * `1` or `alert`
	// > * `0` or `emergency`
	// >
	// > Otherwise `6` will be used.
	LevelField string `json:"level_field" default:"level"` // *

	// > @3@4@5@6
	// >
	// > How many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > A maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` // *
	BatchSize_ int

	// > @3@4@5@6
	// >
	// > A minimum size of events in a batch to send.
	// > If both batch_size and batch_size_bytes are set, they will work together.
	BatchSizeBytes  cfg.Expression `json:"batch_size_bytes" default:"0" parse:"expression"` // *
	BatchSizeBytes_ int

	// > @3@4@5@6
	// >
	// > After this timeout the batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"0"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not
	// > **Experimental feature**
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// fields converted to extra fields GELF format
	hostField                string
	shortMessageField        string
	defaultShortMessageValue string
	fullMessageField         string
	timestampField           string
	timestampFieldFormat     string
	levelField               string
}

type data struct {
	outBuf    []byte
	encodeBuf []byte
	gelf      *client
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)

	p.config.hostField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.HostField))
	p.config.shortMessageField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.ShortMessageField))
	p.config.defaultShortMessageValue = strings.TrimSpace(p.config.DefaultShortMessageValue)
	p.config.fullMessageField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.FullMessageField))
	p.config.timestampField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.TimestampField))
	format, err := pipeline.ParseFormatName(p.config.TimestampFieldFormat)
	if err != nil {
		params.Logger.Errorf("unknown time format: %s", err.Error())
	}
	p.config.timestampFieldFormat = format
	p.config.levelField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.LevelField))

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:        params.PipelineName,
		OutputType:          outPluginType,
		MaintenanceFn:       p.maintenance,
		Controller:          p.controller,
		Workers:             p.config.WorkersCount_,
		BatchSizeCount:      p.config.BatchSize_,
		BatchSizeBytes:      p.config.BatchSizeBytes_,
		FlushTimeout:        p.config.BatchFlushTimeout_,
		MaintenanceInterval: p.config.ReconnectInterval_,
		MetricCtl:           params.MetricCtl,
	}

	backoffOpts := pipeline.BackoffOpts{
		MinRetention: p.config.Retention_,
		Multiplier:   float64(p.config.RetentionExponentMultiplier),
		AttemptNum:   p.config.Retry,
	}

	onError := func(err error) {
		var level zapcore.Level
		if p.config.FatalOnFailedInsert {
			level = zapcore.FatalLevel
		} else {
			level = zapcore.ErrorLevel
		}

		p.logger.Desugar().Log(level, "can't send to gelf", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)
	}

	p.batcher = pipeline.NewRetriableBatcher(
		&batcherOpts,
		p.out,
		backoffOpts,
		onError,
	)

	p.batcher.Start(context.TODO())
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_gelf_send_error", "Total GELF send errors")
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		*workerData = &data{
			outBuf:    make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
			encodeBuf: make([]byte, 0),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	outBuf := data.outBuf[:0]
	encodeBuf := data.encodeBuf[:0]

	batch.ForEach(func(event *pipeline.Event) {
		encodeBuf = p.formatEvent(encodeBuf, event)
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, byte(0))
	})

	data.outBuf = outBuf
	data.encodeBuf = encodeBuf

	if data.gelf == nil {
		p.logger.Infof("connecting to gelf address=%s", p.config.Endpoint)

		gelf, err := newClient(p.config.Endpoint, p.config.ConnectionTimeout_, p.config.WriteTimeout_, false, nil)
		if err != nil {
			p.sendErrorMetric.Inc()
			p.logger.Errorf("can't connect to gelf endpoint address=%s: %s", p.config.Endpoint, err.Error())
			time.Sleep(time.Second)
			return err
		}
		data.gelf = gelf
	}

	_, err := data.gelf.send(outBuf)
	if err != nil {
		p.sendErrorMetric.Inc()
		p.logger.Errorf("can't send data to gelf address=%s, err: %s", p.config.Endpoint, err.Error())
		_ = data.gelf.close()
		data.gelf = nil
		time.Sleep(time.Second)
		return err
	}

	return nil
}

func (p *Plugin) maintenance(workerData *pipeline.WorkerData) {
	if *workerData == nil {
		return
	}

	p.logger.Infof("reconnecting worker...")
	data := (*workerData).(*data)
	_ = data.gelf.close()
	data.gelf = nil
}

func (p *Plugin) formatEvent(encodeBuf []byte, event *pipeline.Event) []byte {
	root := event.Root

	encodeBuf = p.makeExtraFields(encodeBuf, root)

	root.AddFieldNoAlloc(root, "version").MutateToString("1.1")

	p.makeBaseField(root, "host", p.config.hostField, "unknown")
	p.makeBaseField(root, "short_message", p.config.shortMessageField, p.config.defaultShortMessageValue)
	p.makeBaseField(root, "full_message", p.config.fullMessageField, "")

	p.makeTimestampField(root, p.config.timestampField, p.config.timestampFieldFormat)
	p.makeLevelField(root, p.config.levelField)

	return encodeBuf
}

func (p *Plugin) makeBaseField(root *insaneJSON.Root, gelfFieldName string, configFieldName string, defaultValue string) {
	if configFieldName == "" {
		return
	}

	field := root.DigField(configFieldName)
	if field == nil {
		if defaultValue == "" {
			return
		}
		root.AddFieldNoAlloc(root, configFieldName).MutateToString(defaultValue)
		field = root.DigField(configFieldName)
	}

	field.MutateToField(gelfFieldName)
	value := field.AsFieldValue()

	if !value.IsString() {
		value.MutateToString(value.AsString())
	}

	if p.isBlank(value.AsString()) {
		value.MutateToString(defaultValue)
	}
}

func (p *Plugin) makeTimestampField(root *insaneJSON.Root, timestampField string, timestampFieldFormat string) {
	node := root.Dig(timestampField)
	if node == nil {
		return
	}

	now := float64(time.Now().UnixNano()) / float64(time.Second)
	ts := now
	if node.IsNumber() {
		ts = node.AsFloat()
		// is it in millis?
		if ts > 1000000000000 {
			ts /= 1000
		}
		// is it still in millis?
		if ts > 1000000000000 {
			ts /= 1000
		}
	} else if node.IsString() {
		t, err := pipeline.ParseTime(timestampFieldFormat, node.AsString())
		if err == nil {
			ts = float64(t.UnixNano()) / float64(time.Second)
		}
	}

	// is event in the past? earlier than "Sunday, September 9, 2001 1:46:40 AM"
	if ts < 1000000000 {
		ts = now
	}

	root.AddFieldNoAlloc(root, "timestamp").MutateToFloat(ts)
	node.Suicide()
}

func (p *Plugin) makeLevelField(root *insaneJSON.Root, levelField string) {
	if levelField == "" {
		return
	}

	node := root.Dig(levelField)
	if node == nil {
		return
	}

	level := -1
	if node.IsString() {
		parsedLevel := pipeline.ParseLevelAsNumber(node.AsString())
		if parsedLevel == pipeline.LevelUnknown {
			parsedLevel = pipeline.LevelInformational
		}
		level = int(parsedLevel)
	}
	if node.IsNumber() {
		level = node.AsInt()
	}

	if level == -1 {
		return
	}

	root.AddFieldNoAlloc(root, "level").MutateToInt(level)
	node.Suicide()
}

func (p *Plugin) makeExtraFields(encodeBuf []byte, root *insaneJSON.Root) []byte {
	fields := root.AsFields()
	// convert all fields to extra
	for _, field := range fields {
		// rename to gelf extra field format
		l := len(encodeBuf)
		encodeBuf = p.formatExtraField(encodeBuf, field.AsString())
		field.MutateToField(pipeline.ByteToStringUnsafe(encodeBuf[l:]))

		// make sure extra fields are strings and numbers
		value := field.AsFieldValue()
		if !value.IsString() && !value.IsNumber() {
			l := len(encodeBuf)
			encodeBuf = value.Encode(encodeBuf)
			value.MutateToString(pipeline.ByteToStringUnsafe(encodeBuf[l:]))
		}
	}

	return encodeBuf
}

func (p *Plugin) isBlank(s string) bool {
	for _, c := range s {
		isBlankChar := c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\u000B' || c == '\f' || c == '\u001C' || c == '\u001D' || c == '\u001E' || c == '\u001F'
		if !isBlankChar {
			return false
		}
	}

	return true
}

func (p *Plugin) formatExtraField(encodeBuf []byte, name string) []byte {
	if name == "" {
		return encodeBuf
	}

	encodeBuf = append(encodeBuf, '_')
	for _, c := range name {
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isNumber := c >= '0' && c <= '9'
		isAllowedChar := c == '_' || c == '-' || c == '.'
		if !isLetter && !isNumber && !isAllowedChar {
			c = '-'
		}
		encodeBuf = append(encodeBuf, byte(c))
	}

	return encodeBuf
}
