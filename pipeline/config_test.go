package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/assert"
)

type boolDefault struct {
	T bool `default:"true"`
}

type sliceChild struct {
	Value string `default:"child"`
}

func (s *sliceChild) UnmarshalJSON(raw []byte) error {
	cfg.SetDefaultValues(s)
	var childPtr struct {
		Value *string
	}

	if err := json.Unmarshal(raw, &childPtr); err != nil {
		return err
	}

	if childPtr.Value != nil {
		s.Value = *childPtr.Value
	}

	return nil
}

type sliceStruct struct {
	Value  string       `default:"parent"`
	Childs []sliceChild `default:"" slice:"true"`
}

type sliceStructBool struct {
	Value  string           `default:"parent"`
	Childs []sliceChildBool `default:"" slice:"true"`
}

type sliceChildBool struct {
	Value bool `default:"true"`
}

func (s *sliceChildBool) UnmarshalJSON(raw []byte) error {
	cfg.SetDefaultValues(s)
	var childBoolPtr struct {
		Value *bool
	}

	if err := json.Unmarshal(raw, &childBoolPtr); err != nil {
		return err
	}

	if childBoolPtr.Value != nil {
		s.Value = *childBoolPtr.Value
	}

	return nil
}

func TestSlice(t *testing.T) {
	jsonData := []byte(`{"Value":"parent_value","Childs":[{"Value":"child_1"},{"Value":""},{}]}`)
	pluginInfo := &PluginStaticInfo{
		Type: "sliceStruct",
		Factory: func() (AnyPlugin, AnyConfig) {
			return &sliceStruct{}, &sliceStruct{}
		},
		Config: &sliceStruct{},
	}

	config, err := GetConfig(pluginInfo, jsonData, map[string]int{})
	s := config.(*sliceStruct)

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.Equal(t, "child_1", s.Childs[0].Value, "wrong value")
	assert.Equal(t, "", s.Childs[1].Value, "wrong value")      // default value
	assert.Equal(t, "child", s.Childs[2].Value, "wrong value") // default value
}

func TestSliceBool(t *testing.T) {
	jsonData := []byte(`{"Value":"parent_value","Childs":[{"Value":false},{"Value":true},{}]}`)
	pluginInfo := &PluginStaticInfo{
		Type: "sliceStructBool",
		Factory: func() (AnyPlugin, AnyConfig) {
			return &sliceStructBool{}, &sliceStructBool{}
		},
		Config: &sliceStructBool{},
	}

	config, err := GetConfig(pluginInfo, jsonData, map[string]int{})
	s := config.(*sliceStructBool)

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.Equal(t, false, s.Childs[0].Value, "wrong value")
	assert.Equal(t, true, s.Childs[1].Value, "wrong value") // default value
	assert.Equal(t, true, s.Childs[2].Value, "wrong value") // default value
}

func TestBoolDefaultTrue(t *testing.T) {
	jsonData := []byte(`{}`)
	pluginInfo := &PluginStaticInfo{
		Type: "boolDefault",
		Factory: func() (AnyPlugin, AnyConfig) {
			return &boolDefault{}, &boolDefault{}
		},
		Config: &boolDefault{},
	}

	config, err := GetConfig(pluginInfo, jsonData, map[string]int{})
	s := config.(*boolDefault)

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, true, s.T, "wrong value")
}

func TestBoolSetFalse(t *testing.T) {
	in := &boolDefault{
		T: false,
	}
	jsonData, _ := json.Marshal(in)
	pluginInfo := &PluginStaticInfo{
		Type: "boolDefault",
		Factory: func() (AnyPlugin, AnyConfig) {
			return &boolDefault{}, &boolDefault{}
		},
		Config: &boolDefault{},
	}

	config, err := GetConfig(pluginInfo, jsonData, map[string]int{})
	s := config.(*boolDefault)

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, false, s.T, "wrong value")
}

func TestBoolSetTrue(t *testing.T) {
	in := &boolDefault{
		T: true,
	}
	jsonData, _ := json.Marshal(in)
	pluginInfo := &PluginStaticInfo{
		Type: "boolDefault",
		Factory: func() (AnyPlugin, AnyConfig) {
			return &boolDefault{}, &boolDefault{}
		},
		Config: &boolDefault{},
	}

	config, err := GetConfig(pluginInfo, jsonData, map[string]int{})
	s := config.(*boolDefault)

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, true, s.T, "wrong value")
}
