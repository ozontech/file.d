package filed

import (
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

var DefaultPluginRegistry = &PluginRegistry{
	plugins: make(map[string]*pipeline.PluginStaticInfo),
}

type PluginRegistry struct {
	plugins map[string]*pipeline.PluginStaticInfo
}

func (r *PluginRegistry) Get(kind pipeline.PluginKind, t string) *pipeline.PluginStaticInfo {
	id := r.MakeID(pipeline.PluginKindInput, t)

	info := r.plugins[id]
	if info == nil {
		logger.Fatalf("can't find input plugin with type %q", t)
		return nil
	}

	return info
}

func (r *PluginRegistry) GetActionByType(t string) *pipeline.PluginStaticInfo {
	id := r.MakeID(pipeline.PluginKindAction, t)

	info := r.plugins[id]
	if info == nil {
		logger.Fatalf("can't find action plugin with type %q", t)
		return nil
	}

	return info
}

func (r *PluginRegistry) RegisterInput(info *pipeline.PluginStaticInfo) {
	err := r.register(pipeline.PluginKindInput, info)
	if err != nil {
		logger.Fatalf("can't register input plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) RegisterAction(info *pipeline.PluginStaticInfo) {
	err := r.register(pipeline.PluginKindAction, info)
	if err != nil {
		logger.Fatalf("can't register action plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) RegisterOutput(info *pipeline.PluginStaticInfo) {
	err := r.register(pipeline.PluginKindOutput, info)
	if err != nil {
		logger.Fatalf("can't register output plugin %q: %s", info.Type, err.Error())
	}
}

func (r *PluginRegistry) MakeID(pluginKind pipeline.PluginKind, pluginType string) string {
	return string(pluginKind) + "_" + pluginType
}

func (r *PluginRegistry) register(pluginKind pipeline.PluginKind, info *pipeline.PluginStaticInfo) error {
	id := r.MakeID(pluginKind, info.Type)
	_, alreadyHave := r.plugins[id]
	if alreadyHave {
		logger.Fatalf("plugin %s/%s is already registered", pluginKind, info.Type)
	}

	r.plugins[id] = info
	logger.Infof("plugin %s/%s registered", pluginKind, info.Type)

	return nil
}
