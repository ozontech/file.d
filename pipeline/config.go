package pipeline

import (
	"bytes"
	"encoding/json"

	"github.com/ozontech/file.d/cfg"
)

func GetConfig(info *PluginStaticInfo, configJson []byte, values map[string]int) (AnyConfig, error) {
	_, config := info.Factory()
	if err := decodeConfig(config, configJson); err != nil {
		return nil, err
	}

	err := cfg.Parse(config, values)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func decodeConfig(config any, configJson []byte) error {
	err := cfg.SetDefaultValues(config)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(bytes.NewReader(configJson))
	dec.DisallowUnknownFields()
	return dec.Decode(config)
}
