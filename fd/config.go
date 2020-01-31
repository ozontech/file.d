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
type Expression string

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

func Parse(ptr interface{}, values map[string]int) error {
	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()

	if t.Kind() != reflect.Struct {
		return nil
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
				return fmt.Errorf("field %s should be one of %s, got=%s", t.Field(i).Name, tag, vField.String())
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
			case "list":
				if vField.Kind() != reflect.String {
					return fmt.Errorf("list deals only with strings, but field %s has %s type", tField.Name, tField.Type.Name())
				}

				list := make([]string, 0)

				parts := strings.Split(vField.String(), ",")
				for _, part := range parts {
					cleanPart := strings.TrimSpace(part)
					list = append(list, cleanPart)
				}
				field := v.FieldByName(t.Field(i).Name + "_")
				field.Set(reflect.ValueOf(list))
			case "expression":
				if vField.Kind() != reflect.String {
					return fmt.Errorf("expression deals only with strings, but field %s has %s type", tField.Name, tField.Type.Name())
				}
				field := v.FieldByName(t.Field(i).Name + "_")

				pos := strings.IndexAny(vField.String(), "*/+-")
				if pos == -1 {
					i, err := strconv.Atoi(vField.String())
					if err != nil {
						return fmt.Errorf("can't convert %s to int", vField.String())
					}
					field.SetInt(int64(i))
					return nil
				}

				op1 := strings.TrimSpace(vField.String()[:pos])
				op := vField.String()[pos]
				op2 := strings.TrimSpace(vField.String()[pos+1:])

				op1_, err := strconv.Atoi(op1)
				if err != nil {
					has := false
					op1_, has = values[op1]
					if ! has {
						return fmt.Errorf("can't find value for %q in expression", op1)
					}
				}

				op2_, err := strconv.Atoi(op2)
				if err != nil {
					has := false
					op2_, has = values[op2]
					if ! has {
						return fmt.Errorf("can't find value for %q in expression", op2)
					}
				}

				result := 0
				switch op {
				case '+':
					result = op1_ + op2_
				case '-':
					result = op1_ - op2_
				case '*':
					result = op1_ * op2_
				case '/':
					result = op1_ / op2_
				default:
					return fmt.Errorf("unknown operation %q", op)
				}

				field.SetInt(int64(result))
			default:
				return fmt.Errorf("unsupported parse type %q for field %s", tag, t.Field(i).Name)
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
