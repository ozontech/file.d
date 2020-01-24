package fd

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"gitlab.ozon.ru/sre/file-d/logger"
)

type Config struct {
	Pipelines map[string]*PipelineConfig
}

type Duration string
type ListMap string

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

func ParseOptions(title string, options string, value string) int {
	parts := strings.Split(options, "|")
	for i, variant := range parts {
		if value == variant {
			return i
		}
	}

	logger.Fatalf("wrong %s %q provided, should be one of %s", title, options)
	return -1
}

func ParseDuration(value string) time.Duration {
	result, err := time.ParseDuration(string(value))
	if err != nil {
		return -1
	}

	return result
}

var (
	errPointerRequired = fmt.Errorf("pointer to struct is required")
)

func Parse(ptr interface{}) error {
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		return errPointerRequired
	}

	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()

	if t.Kind() != reflect.Struct {
		return errPointerRequired
	}

	for i := 0; i < t.NumField(); i++ {
		vField := v.Field(i)
		tField := t.Field(i)

		tag := tField.Tag.Get("required")
		required := tag == "true"

		tag = tField.Tag.Get("default")
		if tag != "" {
			switch vField.Kind() {
			case reflect.String:
				if vField.String() == "" {
					vField.SetString(tag)
				}
			case reflect.Int:
				val, err := strconv.Atoi(tag)
				if err != nil {
					return errors.Wrapf(err, "default value for field %s should be int, got=%s", tField.Name, tag)
				}
				vField.SetInt(int64(val))
			}
		}

		tag = tField.Tag.Get("options")
		if tag != "" {
			parts := strings.Split(tag, "|")
			if vField.Kind() != reflect.String {
				return fmt.Errorf("options deals with strings only, but field %s has %s type", tField.Name, tField.Type.Name())
			}

			found := false
			for _, part := range parts {
				if vField.String() == part {
					found = true
					break
				}
			}

			if !found {
				return fmt.Errorf("field %s should be one of %s, got=%s", t.Field(i).Type.Name(), tag, vField.String())
			}
		}

		tag = tField.Tag.Get("parse")
		if tag != "" {
			switch tag {
			case "duration":
				if vField.Kind() != reflect.String {
					return fmt.Errorf("duration deals only with strings, but field %s has %s type", tField.Name, tField.Type.Name())
				}

				result, err := time.ParseDuration(vField.String())
				if err != nil {
					return fmt.Errorf("field %s has wrong duration format: %s", t.Field(i).Name, err.Error())
				}
				field := v.FieldByName(t.Field(i).Name + "_")
				field.SetInt(int64(result))

				if field.String() == "" {
					return fmt.Errorf("field %s is required to be set to string", t.Field(i).Name)
				}
			case "list-map":
				if vField.Kind() != reflect.String {
					return fmt.Errorf("list-map deals only with strings, but field %s has %s type", tField.Name, tField.Type.Name())
				}

				listMap := make(map[string]bool)

				parts := strings.Split(vField.String(), ",")
				for _, part := range parts {
					cleanPart := strings.TrimSpace(part)
					listMap[cleanPart] = true
				}
				field := v.FieldByName(t.Field(i).Name + "_")
				field.Set(reflect.ValueOf(listMap))

			default:
				return fmt.Errorf("unsupported parse %s for field %s", tag, t.Field(i).Type.Name())
			}
		}

		if required {
			switch vField.Kind() {
			case reflect.String:
				if vField.String() == "" {
					return fmt.Errorf("field %s should be set as not empty string", t.Field(i).Name)
				}
			case reflect.Int:
				if vField.Int() == 0 {
					return fmt.Errorf("field %s should be set as not zero int", t.Field(i).Name)
				}
			}
		}
	}

	return nil
}
