package s3

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/kafka"
)

func TestNewMetasenderFabric(t *testing.T) {
	cases := []struct {
		name     string
		config   *Config
		params   *pipeline.OutputPluginParams
		expected *KafkaMetaSenderFabric
	}{
		{
			name:     "no_config",
			config:   &Config{},
			params:   nil,
			expected: nil,
		},
		{
			name: "kafka_config",
			config: &Config{
				MetadataSenderCfg: &metadataSenderConfig{
					MetasenderType: "kafka",
					KafkaCfg:       kafka.Config{Brokers: []string{"b1", "b2", "b3"}},
				},
			},
			params: &pipeline.OutputPluginParams{},
			expected: &KafkaMetaSenderFabric{
				config: kafka.Config{Brokers: []string{"b1", "b2", "b3"}},
				params: &pipeline.OutputPluginParams{},
			},
		},
	}
	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			metasenderFabric, err := NewMetaSenderFabric(tCase.config, tCase.params)
			require.NoError(t, err)
			require.Equal(t, tCase.expected, metasenderFabric)
		})
	}
}

func TestNewMetasenderFabricError(t *testing.T) {
	metasenderFabric, err := NewMetaSenderFabric(&Config{
		MetadataSenderCfg: &metadataSenderConfig{
			MetasenderType: "bad type",
		}},
		nil,
	)
	require.Error(t, err)
	require.EqualError(t, errors.New("unknown metasender type: bad type"), err.Error())
	require.Nil(t, metasenderFabric)
}

func TestFabricMetasenderFunc(t *testing.T) {
	kafkaFabric, err := NewMetaSenderFabric(&Config{MetadataSenderCfg: &metadataSenderConfig{MetasenderType: "kafka"}}, nil)
	require.NoError(t, err)
	require.NotNil(t, kafkaFabric)

	starter := kafkaFabric.MetaSenderStarterFunc()

	var starterType MetaSenderStarter
	require.NotNil(t, starter)
	require.IsType(t, starterType, starter)
}

func TestFabricIsNotNil(t *testing.T) {
	kafkaFabric, err := NewMetaSenderFabric(&Config{MetadataSenderCfg: &metadataSenderConfig{MetasenderType: "kafka"}}, nil)
	require.NoError(t, err)
	require.NotNil(t, kafkaFabric)
}
