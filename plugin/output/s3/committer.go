package s3

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/kafka"
	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	// commiter type
	KafkaType = "kafka"
)

var ErrKafkaInNonStrictMode = errors.New("can't use kafka as committer while pipeline settings mode isn't strict")

type CommitterFabric interface {
	CommitterFunc() CommiterStarter
}

type CommiterStarter func() *CommiterWrapper

type CommiterWrapper struct {
	commiter pipeline.OutputPlugin
}

// NewCommiterWrapper wrapps commiter.
func NewCommiterWrapper(plugin pipeline.OutputPlugin) *CommiterWrapper {
	return &CommiterWrapper{commiter: plugin}
}

// CommitUpload prepearing data and passes it to comitter.
func (csw *CommiterWrapper) CommitUpload(values map[string]interface{}, constantValue string) error {
	ev := pipeline.Event{}
	ev.SetCustomCommitKind()

	ev.Root = insaneJSON.Spawn()
	defer insaneJSON.Release(ev.Root)

	err := ev.Root.DecodeString(fmt.Sprintf("{%s}", constantValue))
	if err != nil {
		return err
	}

	for i := range values {
		switch values[i].(type) {
		case int:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToInt(values[i].(int))
		case int8:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToInt(int(values[i].(int8)))
		case int16:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToInt(int(values[i].(int16)))
		case int32:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToInt(int(values[i].(int32)))
		case int64:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToInt(int(values[i].(int64)))
		case uint:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToUint64(uint64(values[i].(uint)))
		case uint8:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToUint64(uint64(values[i].(uint8)))
		case uint16:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToUint64(uint64(values[i].(uint16)))
		case uint32:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToUint64(uint64(values[i].(uint32)))
		case uint64:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToUint64(values[i].(uint64))
		case float32:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToFloat(float64(values[i].(float32)))
		case float64:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToFloat(values[i].(float64))
		case string:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToString(values[i].(string))
		case nil:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToNull()
		case bool:
			ev.Root.AddFieldNoAlloc(ev.Root, i).MutateToBool(values[i].(bool))
		default:
			logger.Infof("field %v skipped due unsupported type\n", i)
		}
	}

	csw.commiter.Out(&ev)
	return nil
}

type KafkaCommitterFabric struct {
	config kafka.Config
	params *pipeline.OutputPluginParams
}

// NewCommitterFabric returns fabric that creates Commiters.
func NewCommitterFabric(config *Config, params *pipeline.OutputPluginParams) (CommitterFabric, error) {
	if config.CommitCfg == nil {
		return nil, nil
	}
	switch config.CommitCfg.CommitterType {
	case KafkaType:
		return &KafkaCommitterFabric{config: config.CommitCfg.KafkaCfg, params: params}, nil
	default:
		return nil, fmt.Errorf("unknown commiter type: %s", config.CommitCfg.CommitterType)
	}
}

// CommiterFunc returns func that starts plugin and returns it.
func (cf *KafkaCommitterFabric) CommitterFunc() CommiterStarter {
	anyPlugin, _ := kafka.Factory()
	outPlugin := anyPlugin.(*kafka.Plugin)

	return func() *CommiterWrapper {
		outPlugin.Start(&cf.config, cf.params)
		return NewCommiterWrapper(outPlugin)
	}
}
