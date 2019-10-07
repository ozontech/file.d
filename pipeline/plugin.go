package pipeline

import (
	"regexp"
	"sync"
)

const (
	PluginKindInput  PluginKind = "input"
	PluginKindAction PluginKind = "action"
	PluginKindOutput PluginKind = "output"
)

type PluginKind string

type InputPlugin interface {
	Start(config AnyConfig, head Head, doneWg *sync.WaitGroup)
	Stop()
	Commit(*Event)
}

type ActionPlugin interface {
	Start(config AnyConfig, params *ActionPluginParams)
	Stop()
	Do(*Event) ActionResult
	Reset()
}

type ActionPluginParams struct {
	PipelineName     string
	PipelineCapacity int
}

type OutputPlugin interface {
	Start(config AnyConfig, capacity int, controller Tail)
	Stop()
	Out(*Event)
}

type PluginRegistryItem struct {
	Id   string
	Info *PluginInfo
}

type PluginInfo struct {
	Type    string
	Factory PluginFactory
}

type InputPluginData struct {
	Plugin InputPlugin

	PluginDesc
}

type ActionPluginData struct {
	Plugin ActionPlugin

	PluginDesc

	MatchConditions MatchConditions
	MatchMode       MatchMode
}

type OutputPluginData struct {
	Plugin OutputPlugin

	PluginDesc
}

type PluginDesc struct {
	ID     string
	T      string // plugin type
	Config AnyConfig
}

type AnyPlugin interface{}

type AnyConfig interface{}

type PluginFactory func() (AnyPlugin, AnyConfig)

type MatchConditions []MatchCondition

type MatchCondition struct {
	Field  string
	Value  string
	Regexp *regexp.Regexp
}

type MatchMode int

const (
	ModeAnd     MatchMode = 0
	ModeOr      MatchMode = 1
	ModeUnknown MatchMode = 2
)
