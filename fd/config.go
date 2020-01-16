package fd

import (
	"io/ioutil"

	"github.com/bitly/go-simplejson"
	"github.com/ghodss/yaml"
	"gitlab.ozon.ru/sre/file-d/logger"
)

type Config struct {
	Pipelines map[string]*PipelineConfig
}

type PipelineConfig struct {
	Raw *simplejson.Json
}

func NewConfig() *Config {
	return &Config{
		Pipelines: make(map[string]*PipelineConfig, 20),
	}
}

func NewConfigFromFile(path string) *Config {
	logger.Infof("reading config %q", path)
	yamlContents, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalf("can't read config file %q: %s", path, err)
	}

	jsonContents, err := yaml.YAMLToJSON(yamlContents)
	if err != nil {
		logger.Infof("config content:\n%s", logger.Numerate(string(yamlContents)))
		logger.Fatalf("can't parse config file yaml %q: %s", path, err.Error())
	}

	json, err := simplejson.NewJson(jsonContents)
	if err != nil {
		logger.Fatalf("can't convert config to json %q: %s", path, err.Error())
	}

	return parseConfig(json)
}

func parseConfig(json *simplejson.Json) *Config {
	config := NewConfig()

	pipelinesJson := json.Get("pipelines")
	pipelines := pipelinesJson.MustMap()
	if len(pipelines) == 0 {
		logger.Fatalf("no pipelines defined in config")
	}
	for i := range pipelines {
		raw := pipelinesJson.Get(i)
		config.Pipelines[i] = &PipelineConfig{Raw: raw}
	}

	logger.Infof("config parsed, found %d pipelines", len(config.Pipelines), )

	return config
}
