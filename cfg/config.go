package cfg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/logger"
	"gopkg.in/yaml.v2"
	k8s_yaml "sigs.k8s.io/yaml"
)

const trueValue = "true"

type Config struct {
	Vault     VaultConfig
	Pipelines map[string]*PipelineConfig
}

type (
	Duration      string
	ListMap       string
	Expression    string
	FieldSelector string
	Regexp        string
	Base8         string
)

var _ json.Unmarshaler = new(Expression)

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

func NewConfigFromFile(paths []string) *Config {
	mergedConfig := make(map[interface{}]interface{})

	for _, path := range paths {
		logger.Infof("reading config %q", path)
		yamlContents, err := os.ReadFile(path)
		if err != nil {
			logger.Fatalf("can't read config file %q: %s", path, err)
		}
		var currentConfig map[interface{}]interface{}
		if err := yaml.Unmarshal(yamlContents, &currentConfig); err != nil {
			logger.Fatalf("can't parse config file yaml %q: %s", path, err)
		}

		mergedConfig = mergeYAMLs(mergedConfig, currentConfig)
	}

	mergedYAML, err := yaml.Marshal(mergedConfig)
	if err != nil {
		logger.Fatalf("can't marshal merged config to YAML: %s", err)
	}

	jsonContents, err := k8s_yaml.YAMLToJSON(mergedYAML)
	if err != nil {
		logger.Infof("config content:\n%s", logger.Numerate(string(mergedYAML)))
		logger.Fatalf("can't parse config file yaml %q: %s", paths, err.Error())
	}

	object, err := simplejson.NewJson(jsonContents)
	if err != nil {
		logger.Fatalf("can't convert config to json %q: %s", paths, err.Error())
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

func DecodeConfig(config any, configJson []byte) error {
	err := SetDefaultValues(config)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(bytes.NewReader(configJson))
	dec.DisallowUnknownFields()
	return dec.Decode(config)
}

// Parse holy shit! who write this function?
func Parse(ptr any, values map[string]int) error {
	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()

	if t.Kind() != reflect.Struct {
		return nil
	}

	childs := make([]reflect.Value, 0)
	for i := 0; i < t.NumField(); i++ {
		vField := v.Field(i)
		tField := t.Field(i)

		childTag := tField.Tag.Get("child")
		if childTag == trueValue {
			childs = append(childs, vField)
			continue
		}

		sliceTag := tField.Tag.Get("slice")
		if sliceTag == trueValue {
			if err := ParseSlice(vField, values); err != nil {
				return err
			}
			continue
		}

		err := ParseField(v, vField, &tField, values)
		if err != nil {
			return err
		}
	}

	for _, child := range childs {
		if err := ParseChild(v, child, values); err != nil {
			return err
		}
	}

	return nil
}

// it isn't just a recursion
// it also captures values with the same name from parent
// i.e. take this config:
//
//	{
//		"T": 10,
//		"Child": { // has `child:true` in a tag
//			"T": null
//		}
//	}
//
// this function will set `config.Child.T = config.T`
// see file.d/cfg/config_test.go:TestHierarchy for an example
func ParseChild(parent reflect.Value, v reflect.Value, values map[string]int) error {
	if v.CanAddr() {
		for i := 0; i < v.NumField(); i++ {
			name := v.Type().Field(i).Name
			val := parent.FieldByName(name)
			if val.CanAddr() {
				v.Field(i).Set(val)
			}
		}

		err := Parse(v.Addr().Interface(), values)
		if err != nil {
			return err
		}
	}
	return nil
}

// ParseSlice recursively parses elements of an slice
// calls Parse, not ParseChild (!)
func ParseSlice(v reflect.Value, values map[string]int) error {
	for i := 0; i < v.Len(); i++ {
		if err := Parse(v.Index(i).Addr().Interface(), values); err != nil {
			return err
		}
	}
	return nil
}

func ParseField(v reflect.Value, vField reflect.Value, tField *reflect.StructField, values map[string]int) error {
	tag := tField.Tag.Get("options")
	if tag != "" {
		parts := strings.Split(tag, "|")
		if vField.Kind() != reflect.String {
			return fmt.Errorf("options deals with strings only, but field %s has %s type", tField.Name, tField.Type.Name())
		}

		idx := -1
		found := false
		for i, part := range parts {
			if vField.String() == part {
				found = true
				idx = i
				break
			}
		}

		if !found {
			return fmt.Errorf("field %s should be one of %s, got=%s", tField.Name, tag, vField.String())
		}

		finalField := v.FieldByName(tField.Name + "_")
		if finalField != (reflect.Value{}) {
			switch finalField.Kind() {
			case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
				finalField.SetInt(int64(idx))
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
				finalField.SetUint(uint64(idx))
			default:
				return fmt.Errorf("final field must be an integer")
			}
		}
	}

	tag = tField.Tag.Get("parse")
	if tag != "" {
		if vField.Kind() != reflect.String {
			return fmt.Errorf("field %s should be a string, but it's %s", tField.Name, tField.Type.Name())
		}

		finalField := v.FieldByName(tField.Name + "_")

		switch tag {
		case "regexp":
			re, err := CompileRegex(vField.String())
			if err != nil {
				return fmt.Errorf("can't compile regexp for field %s: %s", tField.Name, err.Error())
			}
			finalField.Set(reflect.ValueOf(re))
		case "selector":
			fields := ParseFieldSelector(vField.String())
			finalField.Set(reflect.ValueOf(fields))
		case "duration":
			var result time.Duration

			fieldValue := vField.String()
			if fieldValue != "" {
				var err error
				result, err = time.ParseDuration(fieldValue)
				if err != nil {
					return fmt.Errorf("field %s has wrong duration format: %s", tField.Name, err.Error())
				}
			}

			finalField.SetInt(int64(result))
		case "list-map":
			parts := strings.Split(vField.String(), ",")
			listMap := make(map[string]bool, len(parts))

			for _, part := range parts {
				cleanPart := strings.TrimSpace(part)
				listMap[cleanPart] = true
			}

			finalField.Set(reflect.ValueOf(listMap))
		case "list":
			parts := strings.Split(vField.String(), ",")
			list := make([]string, 0, len(parts))

			for _, part := range parts {
				cleanPart := strings.TrimSpace(part)
				list = append(list, cleanPart)
			}

			finalField.Set(reflect.ValueOf(list))
		case "expression":
			pos := strings.IndexAny(vField.String(), "*/+-")
			if pos == -1 {
				i, err := strconv.Atoi(vField.String())
				if err != nil {
					return fmt.Errorf("can't convert %s to int", vField.String())
				}
				finalField.SetInt(int64(i))
				return nil
			}

			op1 := strings.TrimSpace(vField.String()[:pos])
			op := vField.String()[pos]
			op2 := strings.TrimSpace(vField.String()[pos+1:])

			op1_, err := strconv.Atoi(op1)
			if err != nil {
				has := false
				op1_, has = values[op1]
				if !has {
					return fmt.Errorf("can't find value for %q in expression", op1)
				}
			}

			op2_, err := strconv.Atoi(op2)
			if err != nil {
				has := false
				op2_, has = values[op2]
				if !has {
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

			finalField.SetInt(int64(result))

		case "base8":
			value, err := strconv.ParseInt(vField.String(), 8, 64)
			if err != nil {
				return fmt.Errorf("could not parse field %s, err: %s", tField.Name, err.Error())
			}
			finalField.SetInt(value)

		case "data_unit":
			parts := strings.Split(vField.String(), " ")
			if len(parts) != 2 {
				return fmt.Errorf("invalid data format, the string must contain 2 parts separated by a space")
			}
			value, err := strconv.Atoi(parts[0])
			if err != nil {
				return fmt.Errorf(`can't parse uint: "%s" is not a number`, parts[0])
			}
			if value < 0 {
				return fmt.Errorf("value must be positive")
			}
			alias := strings.ToLower(strings.TrimSpace(parts[1]))
			if multiplier, ok := DataUnitAliases[alias]; ok {
				finalField.SetUint(uint64(value * multiplier))
			} else {
				return fmt.Errorf(`unexpected alias "%s"`, alias)
			}

		default:
			return fmt.Errorf("unsupported parse type %q for field %s", tag, tField.Name)
		}
	}

	tag = tField.Tag.Get("required")
	required := tag == trueValue

	if required && vField.IsZero() {
		return fmt.Errorf("field %s should set as non-zero value", tField.Name)
	}

	return nil
}

func ParseFieldSelector(selector string) []string {
	result := make([]string, 0)
	tail := ""
	for {
		pos := strings.IndexByte(selector, '.')
		if pos == -1 {
			break
		}
		if pos > 0 && selector[pos-1] == '\\' {
			tail = tail + selector[:pos-1] + "."
			selector = selector[pos+1:]
			continue
		}

		if len(selector) > pos+1 {
			if selector[pos+1] == '.' {
				tail = selector[:pos+1]
				selector = selector[pos+2:]
				continue
			}
		}

		result = append(result, tail+selector[:pos])
		selector = selector[pos+1:]
		tail = ""
	}

	if len(selector)+len(tail) != 0 {
		result = append(result, tail+selector)
	}

	return result
}

func SetDefaultValues(data interface{}) error {
	t := reflect.TypeOf(data).Elem()
	v := reflect.ValueOf(data).Elem()

	if t.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < t.NumField(); i++ {
		tField := t.Field(i)
		vField := v.Field(i)

		vFieldKind := vField.Kind()

		var err error
		switch vFieldKind {
		case reflect.Struct:
			err = SetDefaultValues(vField.Addr().Interface())
			if err != nil {
				return err
			}
		case reflect.Slice:
			for i := 0; i < vField.Len(); i++ {
				item := vField.Index(i)
				if item.Kind() == reflect.Struct {
					err = SetDefaultValues(item.Addr().Interface())
					if err != nil {
						return err
					}
				}
			}
		}

		defaultValue := tField.Tag.Get("default")
		if defaultValue != "" {
			switch vFieldKind {
			case reflect.Bool:
				currentValue := vField.Bool()
				if !currentValue {
					if defaultValue == "true" {
						vField.SetBool(true)
					} else if defaultValue == "false" {
						vField.SetBool(false)
					}
				}
			case reflect.String:
				if vField.String() == "" {
					vField.SetString(defaultValue)
				}
			case reflect.Int:
				if vField.Int() == 0 { // like in vField.IsZero
					val, err := strconv.Atoi(defaultValue)
					if err != nil {
						return fmt.Errorf("default value for field %s should be int, got=%s: %w", tField.Name, defaultValue, err)
					}
					vField.SetInt(int64(val))
				}
			case reflect.Slice:
				if vField.Len() == 0 {
					val := strings.Fields(defaultValue)
					vField.Set(reflect.MakeSlice(vField.Type(), len(val), len(val)))
					for i, v := range val {
						vField.Index(i).SetString(v)
					}
				}
			}
		}
	}

	return nil
}

func ListToMap(a []string) map[string]bool {
	result := make(map[string]bool, len(a))
	for _, key := range a {
		result[key] = true
	}

	return result
}

func CompileRegex(s string) (*regexp.Regexp, error) {
	if s == "" {
		return nil, fmt.Errorf(`regexp is empty`)
	}

	if s == "" || s[0] != '/' || s[len(s)-1] != '/' {
		return nil, fmt.Errorf(`regexp "%s" should be surrounded by "/"`, s)
	}

	return regexp.Compile(s[1 : len(s)-1])
}

func mergeYAMLs(a, b map[interface{}]interface{}) map[interface{}]interface{} {
	merged := make(map[interface{}]interface{})
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		if existingValue, exists := merged[k]; exists {
			if existingMap, ok := existingValue.(map[interface{}]interface{}); ok {
				if newMap, ok := v.(map[interface{}]interface{}); ok {
					merged[k] = mergeYAMLs(existingMap, newMap)
					continue
				}
			}
		}
		merged[k] = v
	}
	return merged
}
