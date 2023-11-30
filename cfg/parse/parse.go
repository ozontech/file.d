package parse

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const trueValue = "true"

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

func CompileRegex(s string) (*regexp.Regexp, error) {
	if s == "" {
		return nil, fmt.Errorf(`regexp is empty`)
	}

	if s == "" || s[0] != '/' || s[len(s)-1] != '/' {
		return nil, fmt.Errorf(`regexp "%s" should be surrounded by "/"`, s)
	}

	return regexp.Compile(s[1 : len(s)-1])
}

func ListToMap(a []string) map[string]bool {
	result := make(map[string]bool, len(a))
	for _, key := range a {
		result[key] = true
	}

	return result
}
