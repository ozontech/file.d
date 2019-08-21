package pipeline

const (
	PluginKindInput     = "input"
	PluginKindProcessor = "processor"
	PluginKindOutput    = "output"
)

type Param struct {
}

type Params map[string]Param

type Plugin interface {
	Start(config Config, controller Controller)
	Stop()
}
type InputPlugin interface {
	Commit(*Event)
}

type PluginFactory func() (Plugin, Config)

type PluginRegistryItem struct {
	Id   string
	Info *PluginInfo
}

type PluginInfo struct {
	Type    string
	Factory PluginFactory
}

type PluginWithConfig struct {
	Instance Plugin
	Config   Config
}

type Config interface{}
