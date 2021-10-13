package pipeline

import (
	"net/http"
	"regexp"

	"go.uber.org/zap"
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
	Logger     *zap.SugaredLogger
}

type OutputPluginParams struct {
	*PluginDefaultParams
	Controller OutputPluginController
	Logger     *zap.SugaredLogger
}

type InputPluginParams struct {
	*PluginDefaultParams
	Controller InputPluginController
	Logger     *zap.SugaredLogger
}

type PluginStaticInfo struct {
	Type    string
	Factory PluginFactory
	Config  AnyConfig

	// Endpoints is the map of endpoint name to handlerFunc.
	// Every plugin can provide their own API through Endpoints.
	Endpoints         map[string]func(http.ResponseWriter, *http.Request)
	AdditionalActions []string // used only for input plugins, defines actions that should be run right after input plugin with input config
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

	MetricName      string
	MetricLabels    []string
	MatchConditions MatchConditions
	MatchMode       MatchMode
	MatchInvert     bool
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
