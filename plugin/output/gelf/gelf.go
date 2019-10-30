package gelf

import (
	"runtime"
	"time"

	"github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

/*
GELF messages are separated by null byte sequence. Each GELF message is a JSON with the following fields:
1.  string version SHOULD be "1.1"
2.  string host
3.  string short_message
4.  string full_message
5.  number timestamp
6.  number level
7.  string _extra_field_1
8.  string _extra_field_2
10. string _extra_field_3

Every field with an underscore prefix (_) will be treated as an extra field.
Allowed characters in field names are any word character (letter, number, underscore), dashes and dots.
*/

const (
	defaultPort                 = 12201
	defaultFlushTimeout         = time.Millisecond * 200
	defaultConnectionTimeout    = time.Second * 5
	defaultHostField            = "host"
	defaultShortMessageField    = "message"
	defaultShortMessageValue    = "not set"
	defaultTimestampField       = "time"
	defaultTimestampFieldFormat = "RFC3339Nano"
	defaultLevelField           = "level"
)

type Config struct {
	Host              string            `json:"host"`
	Port              uint              `json:"port"`
	FlushTimeout      pipeline.Duration `json:"flush_timeout"`
	ReconnectInterval pipeline.Duration `json:"reconnect_interval"`
	ConnectionTimeout pipeline.Duration `json:"connection_timeout"`
	WorkersCount      int               `json:"workers_count"`
	BatchSize         int               `json:"batch_size"`

	HostField                string `json:"host_field"`
	ShortMessageField        string `json:"short_message_field"`
	DefaultShortMessageValue string `json:"default_short_message_value"`
	FullMessageField         string `json:"full_message_field"`
	TimestampField           string `json:"timestamp_field"`
	TimestampFieldFormat     string `json:"timestamp_field_format"`
	LevelField               string `json:"level_field"`

	// fields converted to extra fields GELF format
	hostField                string
	shortMessageField        string
	defaultShortMessageValue string
	fullMessageField         string
	timestampField           string
	timestampFieldFormat     string
	levelField               string
}

type Plugin struct {
	config     *Config
	batcher    *pipeline.Batcher
	tail       pipeline.Tail
	avgLogSize int
}

type data struct {
	outBuf    []byte
	encodeBuf []byte
	gelf      *client
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginInfo{
		Type:    "gelf",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.tail = params.Tail
	p.avgLogSize = params.PipelineSettings.AvgLogSize
	p.config = config.(*Config)

	if p.config.Host == "" {
		logger.Errorf(`no "address" provided for gelf output`)
	}

	if p.config.Port == 0 {
		p.config.Port = defaultPort
	}

	if p.config.WorkersCount == 0 {
		p.config.WorkersCount = runtime.GOMAXPROCS(0) * 8
	}

	if p.config.FlushTimeout.Duration == 0 {
		p.config.FlushTimeout.Duration = defaultFlushTimeout
	}

	if p.config.ConnectionTimeout.Duration == 0 {
		p.config.ConnectionTimeout.Duration = defaultConnectionTimeout
	}

	if p.config.Port == 0 {
		logger.Errorf(`no "port" provided for gelf output`)
	}

	if p.config.HostField == "" {
		p.config.HostField = defaultHostField
	}
	p.config.hostField = pipeline.ByteToString(p.formatExtraField(nil, p.config.HostField))

	if p.config.ShortMessageField == "" {
		p.config.ShortMessageField = defaultShortMessageField
	}
	p.config.shortMessageField = pipeline.ByteToString(p.formatExtraField(nil, p.config.ShortMessageField))

	if p.config.DefaultShortMessageValue == "" {
		p.config.defaultShortMessageValue = defaultShortMessageValue
	}

	p.config.fullMessageField = pipeline.ByteToString(p.formatExtraField(nil, p.config.FullMessageField))

	if p.config.TimestampField == "" {
		p.config.TimestampField = defaultTimestampField
	}
	p.config.timestampField = pipeline.ByteToString(p.formatExtraField(nil, p.config.TimestampField))

	if p.config.TimestampFieldFormat == "" {
		p.config.TimestampFieldFormat = defaultTimestampFieldFormat
	}
	format, err := pipeline.ParseFormatName(p.config.TimestampFieldFormat)
	if err != nil {
		logger.Errorf("can't convert format for gelf output: %s", err.Error())
	}
	p.config.timestampFieldFormat = format

	if p.config.LevelField == "" {
		p.config.LevelField = defaultLevelField
	}
	p.config.levelField = pipeline.ByteToString(p.formatExtraField(nil, p.config.LevelField))

	if p.config.BatchSize == 0 {
		p.config.BatchSize = params.PipelineSettings.Capacity / 4
	}

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"gelf",
		p.out,
		p.maintenance,
		p.tail,
		p.config.WorkersCount,
		p.config.BatchSize,
		p.config.FlushTimeout.Duration,
		p.config.ReconnectInterval.Duration,
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
			outBuf:    make([]byte, 0, p.config.BatchSize*p.avgLogSize),
			encodeBuf: make([]byte, 0, 0),
		}
	}

	// handle to much memory consumption
	data := (*workerData).(*data)
	if cap(data.outBuf) > p.config.BatchSize*p.avgLogSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize*p.avgLogSize)
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
			logger.Infof("connecting to gelf endpoint host=%s, port=%d", p.config.Host, p.config.Port)

			gelf, err := newClient(p.config.Host, p.config.Port, false, transportTCP, p.config.ConnectionTimeout.Duration, nil)
			if err != nil {
				logger.Errorf("can't connect to gelf endpoint host=%s, port=%d, will retry: %s", p.config.Host, p.config.Port, err.Error())
				time.Sleep(time.Second)
				continue
			}
			data.gelf = gelf

		}

		_, err := data.gelf.send(outBuf)

		if err != nil {
			logger.Errorf("can't send data to gelf endpoint host=%s, port=%d, will retry: %s", p.config.Host, p.config.Port, err.Error())
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

	logger.Infof("reconnecting worker to gelf...")
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

func (p *Plugin) makeBaseField(root *insaneJSON.Root, baseFieldName string, rootFieldName string, defaultValue string) {
	if rootFieldName == "" {
		return
	}

	field := root.DigField(rootFieldName)
	if field != nil {
		field.MutateToField(baseFieldName)
		value := field.AsFieldValue()

		if !value.IsString() {
			value.MutateToString(value.AsString())
		}

		if p.isBlank(value.AsString()) {
			value.MutateToString(defaultValue)
		}
		return
	}

	if defaultValue != "" {
		root.AddFieldNoAlloc(root, baseFieldName).MutateToString(defaultValue)
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
		logger.Warnf("found too old event for gelf output, falling back to current time: %s", root.EncodeToString())
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
		field.MutateToField(pipeline.ByteToString(encodeBuf[l:]))

		// make sure extra fields are strings and numbers
		value := field.AsFieldValue()
		if !value.IsString() && !value.IsNumber() {
			l := len(encodeBuf)
			encodeBuf = value.Encode(encodeBuf)
			value.MutateToString(pipeline.ByteToString(encodeBuf[l:]))
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
