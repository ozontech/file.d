package gelf

import (
	"strings"
	"time"

	"github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
Plugin sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be grater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version`, string, should be `1.1`
* `host`, string
* `short_message`, string
* `full_message`, string
* `timestamp`, number
* `level`, number
* `_extra_field_1`, string
* `_extra_field_2`, string
* `_extra_field_3`, string

Every field with an underscore prefix (_) will be treated as an extra field.
Allowed characters in a field names are any word character(letter, number, underscore), dashes and dots.
}*/

type Plugin struct {
	config     *Config
	logger     *zap.SugaredLogger
	avgLogSize int
	batcher    *pipeline.Batcher
	controller pipeline.OutputPluginController
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Address of gelf endpoint. Format: `HOST:PORT`. E.g. `localhost:12201`.
	Endpoint string `json:"endpoint" required:"true"` //*

	//> @3 @4 @5 @6
	//>
	//> Plugin reconnects to endpoint periodically using this interval. Useful if endpoint is a load balancer.
	ReconnectInterval  cfg.Duration `json:"reconnect_interval" default:"1m" parse:"duration"` //*
	ReconnectInterval_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> How much time to wait for connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s"` //*
	ConnectionTimeout_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> Which field of event should be used as `host` GELF field.
	HostField string `json:"host_field" default:"host"` //*

	//> @3 @4 @5 @6
	//>
	//>  Which field of event should be used as `short_message` GELF field.
	ShortMessageField string `json:"short_message_field" default:"message"` //*

	//> @3 @4 @5 @6
	//>
	//>  Default value for `short_message` GELF field if nothing is found in the event.
	DefaultShortMessageValue string `json:"default_short_message_value" default:"not set"` //*

	//> @3 @4 @5 @6
	//>
	//> Which field of event should be used as `full_message` GELF field.
	FullMessageField string `json:"full_message_field" default:""` //*

	//> @3 @4 @5 @6
	//>
	//> Which field of event should be used as `timestamp` GELF field.
	TimestampField string `json:"timestamp_field" default:"time"` //*

	//> @3 @4 @5 @6
	//>
	//> In which format timestamp field should be parsed.
	TimestampFieldFormat string `json:"timestamp_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"` //*

	//> @3 @4 @5 @6
	//>
	//> Which field of event should be used as `level` GELF field. Level field should contain level number or string according to RFC 5424:
	//> * `7`/`debug`
	//> * `6`/`info`
	//> * `5`/`notice`
	//> * `4`/`warning`
	//> * `3`/`error`
	//> * `2`/`critical`
	//> * `1`/`alert`
	//> * `0`/`emergency`
	//> Otherwise `6` will be used.
	LevelField string `json:"level_field" default:"level"` //*

	//> @3 @4 @5 @6
	//>
	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3 @4 @5 @6
	//>
	//> Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> @3 @4 @5 @6
	//>
	//> After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms"` //*
	BatchFlushTimeout_ time.Duration

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
		Type:    "gelf",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.avgLogSize = params.PipelineSettings.AvgLogSize
	p.config = config.(*Config)

	p.config.hostField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.HostField))
	p.config.shortMessageField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.ShortMessageField))
	p.config.defaultShortMessageValue = strings.TrimSpace(p.config.DefaultShortMessageValue)
	p.config.fullMessageField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.FullMessageField))
	p.config.timestampField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.TimestampField))
	format, err := pipeline.ParseFormatName(p.config.TimestampFieldFormat)
	if err != nil {
		p.logger.Errorf("can't parse time format: %s", err.Error())
	}
	p.config.timestampFieldFormat = format
	p.config.levelField = pipeline.ByteToStringUnsafe(p.formatExtraField(nil, p.config.LevelField))

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"gelf",
		p.out,
		p.maintenance,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		p.config.ReconnectInterval_,
	)
	p.batcher.Start()
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			outBuf:    make([]byte, 0, p.config.BatchSize_*p.avgLogSize),
			encodeBuf: make([]byte, 0, 0),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgLogSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgLogSize)
	}

	outBuf := data.outBuf[:0]
	encodeBuf := data.encodeBuf[:0]
	for _, event := range batch.Events {
		encodeBuf = p.formatEvent(encodeBuf, event)
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, byte(0))
	}
	data.outBuf = outBuf
	data.encodeBuf = encodeBuf

	for {
		if data.gelf == nil {
			p.logger.Infof("connecting to gelf address=%s", p.config.Endpoint)

			gelf, err := newClient(transportTCP, p.config.Endpoint, p.config.ConnectionTimeout_, false, nil)
			if err != nil {
				p.logger.Errorf("can't connect to gelf endpoint address=%s: %s", p.config.Endpoint, err.Error())
				time.Sleep(time.Second)
				continue
			}
			data.gelf = gelf

		}

		_, err := data.gelf.send(outBuf)

		if err != nil {
			p.logger.Errorf("can't send data to gelf address=%s", p.config.Endpoint, err.Error())
			_ = data.gelf.close()
			data.gelf = nil
			time.Sleep(time.Second)
			continue
		}

		break
	}
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
			ts = ts / 1000
		}
		// is it still in millis?
		if ts > 1000000000000 {
			ts = ts / 1000
		}
	} else if node.IsString() {
		t, err := time.Parse(timestampFieldFormat, node.AsString())
		if err == nil {
			ts = float64(t.UnixNano()) / float64(time.Second)
		}
	}

	// are event in the past?
	if ts < 100000000 {
		p.logger.Warnf("found too old event, falling back to current time: %s", root.EncodeToString())
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
		level = pipeline.ParseLevel(node.AsString())
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
	if len(name) == 0 {
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
