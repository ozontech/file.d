package filed

import (
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

var DefaultPluginRegistry = &PluginRegistry{
	plugins: map[string]*pipeline.PluginRegistryItem{},
}

type PluginRegistry struct {
	plugins map[string]*pipeline.PluginRegistryItem
}

func (r *PluginRegistry) RegisterInput(pluginType string, info *pipeline.PluginInfo) {
	err := r.register(pipeline.PluginKindInput, pluginType, info)
	if err != nil {
		logger.Panicf("can't register plugin %q: %s", pluginType, err.Error())
	}
}

func (r *PluginRegistry) MakeName(pluginKind string, pluginType string) string {
	return pluginKind + "_" + pluginType
}
func (r *PluginRegistry) register(pluginKind string, pluginType string, info *pipeline.PluginInfo) error {
	name := r.MakeName(pluginKind, pluginType)
	_, alreadyHave := r.plugins[name]
	if alreadyHave {
		return NewError(ErrCodePluginAlreadyRegistered, "plugin %q is already registered", name)
	}

	r.plugins[name] = &pipeline.PluginRegistryItem{
		Name: name,
		Info: info,
	}

	return nil
}
