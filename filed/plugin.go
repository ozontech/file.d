package filed

import (
	"github.com/bitly/go-simplejson"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

const (
	PluginKindInput     = "input"
	PluginKindProcessor = "processor"
	PluginKindOutput    = "output"
)

type Param struct {
}

type Params map[string]Param

type PluginFactory func(args *simplejson.Json, pipelineInputs []*pipeline.Parser) Plugin

type PluginRegistryItem struct {
	Name string
	Info *PluginInfo
}

type PluginInfo struct {
	Params  *Params
	Factory PluginFactory
}
