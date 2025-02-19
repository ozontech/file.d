package fd

import (
	"fmt"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

var DefaultPluginRegistry = &PluginRegistry{
	plugins: make(map[string]*pipeline.PluginStaticInfo),
}

type PluginRegistry struct {
	plugins map[string]*pipeline.PluginStaticInfo
}

func (r *PluginRegistry) Get(kind pipeline.PluginKind, t string) (*pipeline.PluginStaticInfo, error) {
	id := r.MakeID(kind, t)

	info := r.plugins[id]
	if info == nil {
		return nil, fmt.Errorf("can't find plugin kind=%s type=%s", kind, t)
	}

	return info, nil
}

func (r *PluginRegistry) GetActionByType(t string) (*pipeline.PluginStaticInfo, error) {
	id := r.MakeID(pipeline.PluginKindAction, t)

	info := r.plugins[id]
	if info == nil {
		return nil, fmt.Errorf("can't find action plugin with type %q", t)
	}
	return info, nil
}

func (r *PluginRegistry) RegisterInput(info *pipeline.PluginStaticInfo) {
	r.register(pipeline.PluginKindInput, info)
}

func (r *PluginRegistry) RegisterAction(info *pipeline.PluginStaticInfo) {
	r.register(pipeline.PluginKindAction, info)
}

func (r *PluginRegistry) RegisterOutput(info *pipeline.PluginStaticInfo) {
	r.register(pipeline.PluginKindOutput, info)
}

func (r *PluginRegistry) MakeID(pluginKind pipeline.PluginKind, pluginType string) string {
	return string(pluginKind) + "_" + pluginType
}

func (r *PluginRegistry) register(pluginKind pipeline.PluginKind, info *pipeline.PluginStaticInfo) {
	id := r.MakeID(pluginKind, info.Type)
	_, alreadyHave := r.plugins[id]
	if alreadyHave {
		logger.Fatalf("plugin %s/%s is already registered", pluginKind, info.Type)
	}

	r.plugins[id] = info
	logger.Debugf("plugin %s/%s registered", pluginKind, info.Type)
}
