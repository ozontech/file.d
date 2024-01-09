package pipeline

import "github.com/ozontech/file.d/cfg"

func GetConfig(info *PluginStaticInfo, configJson []byte, values map[string]int) (AnyConfig, error) {
	_, config := info.Factory()
	if err := cfg.DecodeConfig(config, configJson); err != nil {
		return nil, err
	}

	err := cfg.Parse(config, values)
	if err != nil {
		return nil, err
	}

	return config, nil
}
