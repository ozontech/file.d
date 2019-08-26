package filed

import (
	"io/ioutil"

	"github.com/bitly/go-simplejson"
	"github.com/ghodss/yaml"
	"gitlab.ozon.ru/sre/filed/logger"
)

type Config struct {
	actions   map[string]*PluginConfig
	pipelines map[string]*PipelineConfig
}

type PluginConfig struct {
	raw *simplejson.Json
}

type PipelineConfig struct {
	raw *simplejson.Json
}

func NewConfig() *Config {
	return &Config{
		actions:   make(map[string]*PluginConfig, 20),
		pipelines: make(map[string]*PipelineConfig, 20),
	}
}

func NewConfigFromFile(path string) (*Config) {
	logger.Infof("reading config %q", path)
	yamlContents, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalf("can't read config file %q", path)
	}

	jsonContents, err := yaml.YAMLToJSON([]byte(yamlContents))
	if err != nil {
		logger.Fatalf("can't parse config file yaml %q", path)
	}

	json, err := simplejson.NewJson(jsonContents)
	if err != nil {
		logger.Fatalf("can't convert config to json %q", path)
	}

	return parseConfig(json)
}

func parseConfig(json *simplejson.Json) (*Config) {
	config := NewConfig()

	processorsJson := json.Get("processors")
	processors := processorsJson.MustMap()
	for i := range processors {
		raw := processorsJson.Get(i)
		config.actions[i] = &PluginConfig{raw: raw}
	}

	pipelinesJson := json.Get("pipelines")
	pipelines := pipelinesJson.MustMap()
	if len(pipelines) == 0 {
		logger.Fatalf("no pipelines defined in config")
	}
	for i := range pipelines {
		raw := pipelinesJson.Get(i)
		config.pipelines[i] = &PipelineConfig{raw: raw}
	}

	logger.Infof("config parsed, found %d pipelines", len(config.pipelines), )

	return config
}
