package pipeline

import (
	"regexp"
	"sync"
)

const (
	PluginKindInput  = "input"
	PluginKindAction = "action"
	PluginKindOutput = "output"
)

type InputPlugin interface {
	Start(config AnyConfig, head Head, doneWg *sync.WaitGroup)
	Stop()
	Commit(*Event)
}

type ActionPlugin interface {
	Start(config AnyConfig, controller ActionController)
	Stop()
	Do(*Event)
}

type OutputPlugin interface {
	Start(config AnyConfig, controller OutputController)
	Stop()
	Dump(*Event)
}

type PluginRegistryItem struct {
	Id   string
	Info *PluginInfo
}

type PluginInfo struct {
	Type    string
	Factory PluginFactory
}

type InputPluginDescription struct {
	Type   string
	Plugin InputPlugin
	Config AnyConfig
}

type ActionPluginDescription struct {
	Type            string
	Plugin          ActionPlugin
	Config          AnyConfig
	MatchConditions MatchConditions
	MatchMode       MatchMode
}

type OutputPluginDescription struct {
	Type   string
	Plugin OutputPlugin
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
	ModeAnd MatchMode = 0
	ModeOr  MatchMode = 1
)
