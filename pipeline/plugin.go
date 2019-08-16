package pipeline

import "sync"

const (
	PluginKindInput     = "input"
	PluginKindProcessor = "processor"
	PluginKindOutput    = "output"
)

type Param struct {
}

type Params map[string]Param

type Plugin interface {
	Start()
	Stop()
}
type InputPlugin interface {
	Commit(*Event)
}

type PluginFactory func(config interface{}, parsers []*Parser, done *sync.WaitGroup) Plugin

type PluginRegistryItem struct {
	Name string
	Info *PluginInfo
}

type PluginInfo struct {
	Params  *Params
	Factory PluginFactory
}
