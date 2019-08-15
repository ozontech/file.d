package config

import (
	"github.com/bitly/go-simplejson"
	"gitlab.ozon.ru/sre/filed/global"
)

func parseConfig(json *simplejson.Json) (*Config, error) {
	config := NewConfig()

	inputsJson := json.Get("inputs")
	inputs := inputsJson.MustMap()
	if inputs != nil {
		for i := range inputs {
			raw := inputsJson.Get(i)
			config.inputs[i] = &PluginConfig{raw: raw}
		}
	}

	processorsJson := json.Get("processors")
	processors := processorsJson.MustMap()
	if processors != nil {
		for i := range inputs {
			raw := processorsJson.Get(i)
			config.processors[i] = &PluginConfig{raw: raw}
		}
	}

	outputsJson := json.Get("outputs")
	outputs := outputsJson.MustMap()
	if outputs != nil {
		for i := range inputs {
			raw := outputsJson.Get(i)
			config.outputs[i] = &PluginConfig{raw: raw}
		}
	}

	pipelinesJson := json.Get("pipelines")
	pipelines := pipelinesJson.MustMap()
	if inputs != nil {
		for i := range pipelines {
			raw := pipelinesJson.Get(i)
			config.pipelines[i] = &PipelineConfig{raw: raw}
		}
	}

	global.Logger.Infof(
		"config parsed: %d inputs, %d outputs, %d pipelines",
		len(config.inputs),
		len(config.outputs),
		len(config.pipelines),
	)

	return config, nil
}
