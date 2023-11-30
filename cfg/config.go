package cfg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/logger"
	"sigs.k8s.io/yaml"
)

type (
	Duration      string
	ListMap       string
	Expression    string
	FieldSelector string
	Regexp        string
	Base8         string
)

var _ json.Unmarshaler = new(Expression)

type Config struct {
	Vault     VaultConfig
	Pipelines map[string]*PipelineConfig
}

type PipelineConfig struct {
	Raw *simplejson.Json
}

type VaultConfig struct {
	Token     string
	Address   string
	ShouldUse bool
}

func NewConfig() *Config {
	return &Config{
		Vault: VaultConfig{
			Token:     "",
			Address:   "",
			ShouldUse: false,
		},
		Pipelines: make(map[string]*PipelineConfig, 20),
	}
}

func NewConfigFromFile(path string) *Config {
	logger.Infof("reading config %q", path)
	yamlContents, err := os.ReadFile(path)
	if err != nil {
		logger.Fatalf("can't read config file %q: %s", path, err)
	}

	jsonContents, err := yaml.YAMLToJSON(yamlContents)
	if err != nil {
		logger.Infof("config content:\n%s", logger.Numerate(string(yamlContents)))
		logger.Fatalf("can't parse config file yaml %q: %s", path, err.Error())
	}

	object, err := simplejson.NewJson(jsonContents)
	if err != nil {
		logger.Fatalf("can't convert config to json %q: %s", path, err.Error())
	}

	err = applyEnvs(object)
	if err != nil {
		logger.Fatalf("can't get config values from environments: %s", err.Error())
	}

	config := parseConfig(object)
	var apps []funcApplier

	// add applicator for env variables
	apps = append(apps, &envs{})

	// if vault is used then set value otherwise it is empty variable
	vault := &vault{}
	if config.Vault.ShouldUse {
		vault, err = newVault(config.Vault.Address, config.Vault.Token)
		if err != nil {
			logger.Fatalf("can't create vault client: %s", err.Error())
		}
	}

	// add applicator for vault
	apps = append(apps, vault)

	for _, p := range config.Pipelines {
		applyConfigFuncs(apps, p.Raw)
	}

	logger.Infof("config parsed, found %d pipelines", len(config.Pipelines))

	return config
}

func applyEnvs(object *simplejson.Json) error {
	for _, env := range os.Environ() {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("can't parse env %s", env)
		}

		k, v := kv[0], kv[1]
		if strings.HasPrefix(k, "FILED_") {
			lower := strings.ToLower(k)
			path := strings.Split(lower, "_")[1:]
			object.SetPath(path, v)
		}
	}

	return nil
}

func parseConfig(object *simplejson.Json) *Config {
	config := NewConfig()
	vault := object.Get("vault")
	var err error

	addr := vault.Get("address")
	if addr.Interface() != nil {
		config.Vault.Address, err = addr.String()
		if err != nil {
			logger.Panicf("can't parse vault address: %s", err.Error())
		}
	}

	token := vault.Get("token")
	if token.Interface() != nil {
		config.Vault.Token, err = token.String()
		if err != nil {
			logger.Panicf("can't parse vault token: %s", err.Error())
		}
	}
	config.Vault.ShouldUse = config.Vault.Address != "" && config.Vault.Token != ""

	pipelinesJson := object.Get("pipelines")
	pipelines := pipelinesJson.MustMap()
	if len(pipelines) == 0 {
		logger.Fatalf("no pipelines defined in config")
	}
	for name := range pipelines {
		if err := validatePipelineName(name); err != nil {
			logger.Fatal(err)
		}
		raw := pipelinesJson.Get(name)
		config.Pipelines[name] = &PipelineConfig{Raw: raw}
	}

	return config
}

func validatePipelineName(name string) error {
	matched, err := regexp.MatchString("^[a-zA-Z0-9_]+$", name)
	if err != nil {
		return err
	}
	if !matched {
		return fmt.Errorf(`pipeline name "%s" not satisfy regexp pattern ^[a-zA-Z0-9_]+$`, name)
	}
	return nil
}

func applyConfigFuncs(apps []funcApplier, object *simplejson.Json) {
	for _, app := range apps {
		applyConfigFunc(app, object)
	}
}

func applyConfigFunc(app funcApplier, object *simplejson.Json) {
	if a, err := object.Array(); err == nil {
		for i := range a {
			field := object.GetIndex(i)
			if value, ok := tryApplyFunc(app, field); ok {
				a[i] = value

				continue
			}
			applyConfigFunc(app, field)
		}
	}

	if m, err := object.Map(); err == nil {
		for k := range m {
			field := object.Get(k)
			if value, ok := tryApplyFunc(app, field); ok {
				object.Set(k, value)

				continue
			}
			applyConfigFunc(app, field)
		}
	}
}

func tryApplyFunc(app funcApplier, field *simplejson.Json) (string, bool) {
	s, err := field.String()
	if err != nil {
		return "", false
	}

	if value, ok := app.tryApply(s); ok {
		return value, ok
	}

	return "", false
}

func (e *Expression) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	err := decoder.Decode(&value)
	if err != nil {
		return err
	}

	switch value := value.(type) {
	case json.Number:
		*e = Expression(value.String())
	case string:
		*e = Expression(value)
	default:
		return fmt.Errorf("can't cast %T to the Expression", value)
	}

	return nil
}
