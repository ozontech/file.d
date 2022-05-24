package s3

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/kafka"
)

var ErrKafkaInNonStrictMode = errors.New("can't use kafka as committer while pipeline settings mode isn't strict")
var ErrEmptyKafkaCfg = errors.New("empty Kafka config")

type CommitterFabric interface {
	CommitterFunc() CommiterStarter
	IsNil(f CommitterFabric) bool
}

type CommiterStarter func() pipeline.OutputPlugin

type kafkaCommitterFabric struct {
	config *kafka.Config
	params *pipeline.OutputPluginParams
}

// NewCommitterFabric returns fabric that creates Commiters.
func NewCommitterFabric(config *Config, params *pipeline.OutputPluginParams) (*kafkaCommitterFabric, error) {
	if config.CommitCfg == nil {
		return nil, nil
	}

	switch config.CommitCfg.CommitterType {
	case kafkaType:
		if !params.PipelineSettings.IsStrict {
			// If mode not strict files will be downloaded to s3 but err suring sending to Kaffka will
			// lead to deleting zip file and never retrying if send.
			return nil, ErrKafkaInNonStrictMode
		}
		kafkaCfg := config.CommitCfg.KafkaCfg
		if kafkaCfg == nil {
			return nil, ErrEmptyKafkaCfg
		}
		return &kafkaCommitterFabric{config: kafkaCfg}, nil

	default:
		return nil, fmt.Errorf("unknown commiter type: %s", config.CommitCfg.CommitterType)
	}
}

// CommiterFunc returns func that starts plugin and returns it.
func (cf *kafkaCommitterFabric) CommitterFunc() CommiterStarter {
	anyPlugin, _ := kafka.Factory()
	outPlugin := anyPlugin.(*kafka.Plugin)

	return func() pipeline.OutputPlugin {
		outPlugin.Start(cf.config, cf.params)
		return outPlugin
	}
}

func (cf *kafkaCommitterFabric) IsNil(f CommitterFabric) bool {
	return f.(*kafkaCommitterFabric) == nil
}
