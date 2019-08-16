package filed

import (
	"io/ioutil"

	"github.com/bitly/go-simplejson"
	"github.com/ghodss/yaml"
	"gitlab.ozon.ru/sre/filed/logger"
)

type Config struct {
	inputs     map[string]*PluginConfig
	outputs    map[string]*PluginConfig
	processors map[string]*PluginConfig
	pipelines  map[string]*PipelineConfig
}

func NewConfig() *Config {
	return &Config{
		inputs:     make(map[string]*PluginConfig, 20),
		processors: make(map[string]*PluginConfig, 20),
		outputs:    make(map[string]*PluginConfig, 20),
		pipelines:  make(map[string]*PipelineConfig, 20),
	}
}

func NewConfigFromFile(path string) (*Config, error) {
	logger.Infof("reading config %q", path)
	yamlContents, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, WrapError(err, ErrCodeConfigReadFailed, "can't read config file")
	}

	jsonContents, err := yaml.YAMLToJSON([]byte(yamlContents))
	if err != nil {
		return nil, WrapError(err, ErrCodeConfigWrongYAML, "can't parse config YAML")
	}

	json, err := simplejson.NewJson(jsonContents)
	if err != nil {
		return nil, WrapError(err, ErrCodeConfigConvertFailed, "can't convert config to JSON")
	}

	config, parseErr := parseConfig(json)
	if parseErr != nil {
		return nil, WrapError(err, ErrCodeConfigProcessFailed, "can't process config")
	}

	return config, nil
}

type PluginConfig struct {
	raw *simplejson.Json
}

type PipelineConfig struct {
	raw *simplejson.Json
}
