package pipeline

import (
	"regexp"
)

const (
	PluginKindInput  PluginKind = "input"
	PluginKindAction PluginKind = "action"
	PluginKindOutput PluginKind = "output"
)

type PluginKind string

type InputPlugin interface {
	Start(config AnyConfig, params *InputPluginParams)
	Stop()
	Commit(*Event)
}

type ActionPlugin interface {
	Start(config AnyConfig, params *ActionPluginParams)
	Stop()
	Do(*Event) ActionResult
}

type OutputPlugin interface {
	Start(config AnyConfig, params *OutputPluginParams)
	Stop()
	Out(*Event)
}

type PluginDefaultParams struct {
	PipelineName     string
	PipelineSettings *Settings
}

type ActionPluginParams struct {
	*PluginDefaultParams
	Controller ActionPluginController
}

type OutputPluginParams struct {
	*PluginDefaultParams
	Controller OutputPluginController
}

type InputPluginParams struct {
	*PluginDefaultParams
	Controller InputPluginController
}

type PluginStaticInfo struct {
	Type    string
	Factory PluginFactory
	Config  AnyConfig
}

type PluginRuntimeInfo struct {
	Plugin AnyPlugin
	ID     string
}

type InputPluginInfo struct {
	*PluginStaticInfo
	*PluginRuntimeInfo
}

type ActionPluginStaticInfo struct {
	*PluginStaticInfo

	MetricName string
	MetricLabels []string
	MatchConditions MatchConditions
	MatchMode       MatchMode
}

type ActionPluginInfo struct {
	*ActionPluginStaticInfo
	*PluginRuntimeInfo
}

type OutputPluginInfo struct {
	*PluginStaticInfo
	*PluginRuntimeInfo
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
	MatchModeAnd     MatchMode = 0
	MatchModeOr      MatchMode = 1
	MatchModeUnknown MatchMode = 2
)
