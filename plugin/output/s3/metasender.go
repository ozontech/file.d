package s3

import (
	"fmt"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/kafka"
)

const (
	// metasender type
	KafkaType = "kafka"
)

type MetaSenderFabric interface {
	MetaSenderStarterFunc() MetaSenderStarter
}

type MetaSenderStarter func() pipeline.OutputPlugin

type KafkaMetaSenderFabric struct {
	config kafka.Config
	params *pipeline.OutputPluginParams
}

type metadataDTO struct {
	leastTimestamp   uint64
	largestTimestamp uint64
	bucketName       string
	s3Url            string
}

// NewMetaSenderFabric returns fabric that creates metasender plugin.
func NewMetaSenderFabric(config *Config, params *pipeline.OutputPluginParams) (MetaSenderFabric, error) {
	if config.MetadataSenderCfg == nil {
		// return typed nil for test purposes.
		var nilFabric *KafkaMetaSenderFabric
		return nilFabric, nil
	}
	switch config.MetadataSenderCfg.MetasenderType {
	case KafkaType:
		return &KafkaMetaSenderFabric{config: config.MetadataSenderCfg.KafkaCfg, params: params}, nil
	default:
		return nil, fmt.Errorf("unknown metasender type: %s", config.MetadataSenderCfg.MetasenderType)
	}
}

// MetaSenderStarterFunc returns fn that starts plugin and returns it.
func (cf *KafkaMetaSenderFabric) MetaSenderStarterFunc() MetaSenderStarter {
	anyPlugin, _ := kafka.Factory()
	outPlugin := anyPlugin.(*kafka.Plugin)

	return func() pipeline.OutputPlugin {
		outPlugin.Start(&cf.config, cf.params)
		return outPlugin
	}
}
