package filed

import (
	"gitlab.ozon.ru/sre/filed/global"
)

var DefaultPluginRegistry = &PluginRegistry{
	plugins: map[string]*PluginRegistryItem{},
}

type PluginRegistry struct {
	plugins map[string]*PluginRegistryItem
}

func (r *PluginRegistry) RegisterInput(pluginType string, info *PluginInfo) {
	err := r.register(PluginKindInput, pluginType, info)
	if err != nil {
		global.Logger.Panicf("can't register plugin %q: %s", pluginType, err.Error())
	}
}

func (r *PluginRegistry) MakeName(pluginKind string, pluginType string) string {
	return pluginKind + "_" + pluginType
}
func (r *PluginRegistry) register(pluginKind string, pluginType string, info *PluginInfo) error {
	name := r.MakeName(pluginKind, pluginType)
	_, alreadyHave := r.plugins[name]
	if alreadyHave {
		return global.New(global.ErrCodePluginRegistryAlreadyRegistered, "plugin %q is already registered", name)
	}

	r.plugins[name] = &PluginRegistryItem{
		Name: name,
		Info: info,
	}

	return nil
}
