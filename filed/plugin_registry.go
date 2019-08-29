package filed

import (
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

var DefaultPluginRegistry = &PluginRegistry{
	plugins: make(map[string]*pipeline.PluginRegistryItem),
}

type PluginRegistry struct {
	plugins map[string]*pipeline.PluginRegistryItem
}

func (r *PluginRegistry) GetInputByType(t string) *pipeline.PluginInfo {
	id := r.MakeId(pipeline.PluginKindInput, t)

	info := r.plugins[id]
	if info == nil {
		logger.Fatalf("can't find input plugin with type %q", t)
		return nil
	}

	return info.Info
}

func (r *PluginRegistry) GetActionByType(t string) *pipeline.PluginInfo {
	id := r.MakeId(pipeline.PluginKindAction, t)

	info := r.plugins[id]
	if info == nil {
		logger.Fatalf("can't find action plugin with type %q", t)
		return nil
	}

	return info.Info
}

func (r *PluginRegistry) GetOutputByType(t string) *pipeline.PluginInfo {
	id := r.MakeId(pipeline.PluginKindOutput, t)

	info := r.plugins[id]
	if info == nil {
		logger.Fatalf("can't find output plugin with type %q", t)
		return nil
	}

	return info.Info
}

func (r *PluginRegistry) RegisterInput(info *pipeline.PluginInfo) {
	err := r.register(pipeline.PluginKindInput, info)
	if err != nil {
		logger.Fatalf("can't register input plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) RegisterAction(info *pipeline.PluginInfo) {
	err := r.register(pipeline.PluginKindAction, info)
	if err != nil {
		logger.Fatalf("can't register action plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) RegisterOutput(info *pipeline.PluginInfo) {
	err := r.register(pipeline.PluginKindOutput, info)
	if err != nil {
		logger.Fatalf("can't register output plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) MakeId(pluginKind string, pluginType string) string {
	return pluginKind + "_" + pluginType
}

func (r *PluginRegistry) register(pluginKind string, info *pipeline.PluginInfo) error {
	id := r.MakeId(pluginKind, info.Type)
	_, alreadyHave := r.plugins[id]
	if alreadyHave {
		logger.Fatalf("plugin %s/%s is already registered", pluginKind, info.Type)
	}

	r.plugins[id] = &pipeline.PluginRegistryItem{
		Id:   id,
		Info: info,
	}
	logger.Infof("plugin %s/%s registered", pluginKind, info.Type)

	return nil
}
