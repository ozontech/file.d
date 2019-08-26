package pipeline

const (
	PluginKindInput  = "input"
	PluginKindAction = "action"
	PluginKindOutput = "output"
)

type ActionPlugin interface {
	Start(config AnyConfigPointer, track *Track)
	Stop()
	Do(*Event)
}

type InputPlugin interface {
	Start(config AnyConfigPointer, pipeline Pipeline)
	Stop()
}

type InputPluginAcceptor interface {
	Accept(*Event)
}

type PluginFactory func() (AnyPluginPointer, AnyConfigPointer)

type PluginRegistryItem struct {
	Id   string
	Info *PluginInfo
}

type PluginInfo struct {
	Type    string
	Factory PluginFactory
}

type PluginDescription struct {
	Plugin AnyPluginPointer
	Config AnyConfigPointer
}

type AnyPluginPointer interface{}

type AnyConfigPointer interface{}
