package rename

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	insaneJSON "github.com/ozontech/insane-json"
)

type Config []string

const (
	NotFoundIdx = -1
)

var _ json.Unmarshaler = (*Config)(nil)

func (c *Config) Find(key string) (string, int) {
	keyValues := *c
	for i := 0; i < len(keyValues); i += 2 {
		if keyValues[i] == key {
			return keyValues[i+1], i
		}
	}
	return "", NotFoundIdx
}

func (c *Config) Remove(key string) {
	_, idx := c.Find(key)
	if idx == NotFoundIdx {
		return
	}

	*c = append((*c)[:idx], (*c)[idx+2:]...)
}

func (c *Config) ForEach(cb func(key, value string)) {
	keyValues := *c
	for i := 0; i < len(keyValues); i += 2 {
		cb((*c)[i], (*c)[i+1])
	}
}

func (c *Config) Clone() Config {
	return append(Config{}, *c...)
}

func (c *Config) Append(key, value string) {
	*c = append(*c, key, value)
}

func (c *Config) UnmarshalJSON(bytes []byte) error {
	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	if err := root.DecodeBytes(bytes); err != nil {
		return fmt.Errorf("decoding rename config: %w", err)
	}

	if !root.IsObject() {
		return errors.New("it is not an object")
	}

	fields := root.AsFields()

	*c = make([]string, 0, len(fields)*2)
	for _, field := range fields {
		key := strings.Clone(field.AsString())
		value := strings.Clone(field.AsFieldValue().AsString())
		c.Append(key, value)
	}

	return nil
}
