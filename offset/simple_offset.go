package offset

import (
	"io"

	"github.com/ghodss/yaml"
)

type yamlValue struct {
	value any
}

func (o *yamlValue) Load(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, o.value)
}

func (o *yamlValue) Save(w io.Writer) error {
	b, err := yaml.Marshal(o.value)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func newYAMLOffset(path string, value any) *Offset {
	res := NewOffset(path)
	res.Callback = &yamlValue{value}
	return res
}

func LoadYAML(path string, value any) error {
	return newYAMLOffset(path, value).Load()
}

func SaveYAML(path string, value any) error {
	return newYAMLOffset(path, value).Save()
}
