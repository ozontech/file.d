package gelf

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

const (
	configJSON = `
		{
			"host":"host",
			"port":1000,
			"host_field":"my_host_field",
			"short_message_field":"my_short_message_field",
			"full_message_field":"my_full_message_field",
			"timestamp_field":"my_timestamp_field",
			"timestamp_field_format":"RFC3339Nano",
			"level_field":"my_level_field"
		}`
	eventJSON = `
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
		}`
	formattedJSON = `{
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
		}`
)

func TestFormatEvent(t *testing.T) {
	root, err := insaneJSON.DecodeString(eventJSON)
	if err != nil {
		panic(err.Error())
	}
	event := &pipeline.Event{Root: root}

	plugin := Plugin{}
	config := &Config{}
	err = json.Unmarshal([]byte(configJSON), config)
	if err != nil {
		panic(err.Error())
	}

	params := &pipeline.OutputPluginParams{
		PluginDefaultParams: &pipeline.PluginDefaultParams{
			PipelineName: "name",
			PipelineSettings: &pipeline.Settings{
				StreamField:     "stream",
				ProcessorsCount: 8,
				Capacity:        128,
				AvgLogSize:      128,
			},
		},
		Tail: nil,
	}
	plugin.Start(config, params)
	plugin.formatEvent([]byte{}, event)

	resultJSON := event.Root.EncodeToString()

	expected := strings.ReplaceAll(strings.ReplaceAll(formattedJSON, "\t", ""), "\n", "")
	assert.Equal(t, expected, resultJSON, "wrong formatted event")
}
