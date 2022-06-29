package gelf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestFormatEvent(t *testing.T) {
	stats.InitStats()

	tests := []struct {
		configJSON    string
		eventJSON     string
		formattedJSON string
	}{
		{
			configJSON: `
				{
					"endpoint":"host:1000",
					"host_field":"my_host_field",
					"short_message_field":"my_short_message_field",
					"full_message_field":"my_full_message_field",
					"timestamp_field":"my_timestamp_field",
					"timestamp_field_format":"rfc3339nano",
					"level_field":"my_level_field"
				}`,
			eventJSON: `
				{
					"my_host_field":"my_host_value",
					"my_short_message_field":"my_short_message_value",
					"my_full_message_field":"my_full_message_value",
					"my_timestamp_field":"2009-11-10T23:00:00.423141234Z",
					"my_level_field":"emergency",
					"payload":"payload_value",
					"extra":"extra_value",
					"extra_array":["extra_array_value"],
					"m&m":"tasty"
				}`,
			formattedJSON: `
			{
				"host":"my_host_value",
				"short_message":"my_short_message_value",
				"full_message":"my_full_message_value",
				"timestamp":1257894000.423141,
				"level":0,
				"_payload":"payload_value",
				"_extra":"extra_value",
				"_extra_array":"[\"extra_array_value\"]",
				"_m-m":"tasty",
				"version":"1.1"
			}`,
		},
		{
			configJSON: `
				{
					"endpoint":"host:1000",
					"host_field":"my_host_field",
					"short_message_field":"my_short_message_field",
					"full_message_field":"my_full_message_field",
					"timestamp_field":"my_timestamp_field",
					"timestamp_field_format":"rfc3339nano",
					"level_field":"my_level_field"
				}`,
			eventJSON: `
				{
					"my_host_field":"my_host_value",
					"my_short_message_field":"   ",
					"my_timestamp_field":"2009-11-10T23:00:00.423141234Z",
					"my_level_field":"emergency",
					"payload":"payload_value",
					"extra":"extra_value",
					"extra_array":["extra_array_value"],
					"m&m":"tasty"
				}`,
			formattedJSON: `
			{
				"host":"my_host_value",
				"short_message":"not set",
				"timestamp":1257894000.423141,
				"level":0,
				"_payload":"payload_value",
				"_extra":"extra_value",
				"_extra_array":"[\"extra_array_value\"]",
				"_m-m":"tasty",
				"version":"1.1"
			}`,
		},
	}

	for _, test := range tests {
		root, err := insaneJSON.DecodeString(test.eventJSON)
		if err != nil {
			panic(err.Error())
		}
		event := &pipeline.Event{Root: root}

		plugin := Plugin{}
		config := &Config{}
		err = json.Unmarshal([]byte(test.configJSON), config)
		if err != nil {
			logger.Panicf(err.Error())
		}

		err = cfg.Parse(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
		if err != nil {
			logger.Panicf(err.Error())
		}

		params := &pipeline.OutputPluginParams{
			PluginDefaultParams: &pipeline.PluginDefaultParams{
				PipelineName: "name",
				PipelineSettings: &pipeline.Settings{
					Capacity:     128,
					AvgEventSize: 128,
				},
			},
			Controller: nil,
		}
		plugin.Start(config, params)
		plugin.formatEvent([]byte{}, event)

		resultJSON := event.Root.EncodeToString()

		expected := strings.ReplaceAll(strings.ReplaceAll(test.formattedJSON, "\t", ""), "\n", "")
		assert.Equal(t, expected, resultJSON, "wrong formatted event")
	}
}

func TestMakeTimestampField(t *testing.T) {
	cases := []struct {
		name           string
		jsonString     string
		format         string
		timestampField string
		expectedOutput float64
	}{
		{
			name:           "unix_timestamp",
			jsonString:     `{"time": 1655739567}`,
			format:         time.UnixDate,
			timestampField: "time",
			expectedOutput: float64(1.655739567e+09),
		},
		{
			name:           "rfc3339nano",
			jsonString:     `{"abc": "2023-06-09T10:50:25.384982073Z"}`,
			format:         time.RFC3339Nano,
			timestampField: "abc",
			expectedOutput: float64(1.686307825384982e+09),
		},
	}

	p := Plugin{}
	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			root, err := insaneJSON.DecodeString(tCase.jsonString)
			require.NoError(t, err)
			defer insaneJSON.Release(root)

			p.makeTimestampField(root, tCase.timestampField, tCase.format)
			tsFloat := root.Dig("timestamp").AsFloat()
			require.Equal(t, tCase.expectedOutput, tsFloat)
		})
	}
}

func TestMakeTimestampFieldFromNull(t *testing.T) {
	root, err := insaneJSON.DecodeString("{}")
	require.NoError(t, err)
	defer insaneJSON.Release(root)

	p := Plugin{}
	p.makeTimestampField(root, "", "")

	notExistsNode := root.Dig("timestamp")
	require.Nil(t, notExistsNode)
}
