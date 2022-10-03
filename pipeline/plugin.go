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
	PassEvent(event *Event) bool
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

type PluginsStarterData struct {
	Config AnyConfig
	Params *OutputPluginParams
}
type PluginsStarterMap map[string]PluginsStarterData

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

type AnyPlugin any

type AnyConfig any

type PluginFactory func() (AnyPlugin, AnyConfig)

type MatchConditions []MatchCondition

type MatchCondition struct {
	// Slice for nested fields. Separator is a dot symbol.
	Field  []string
	Value  string
	Regexp *regexp.Regexp
}

type MatchMode int

const (
	MatchModeAnd     MatchMode = 0
	MatchModeOr      MatchMode = 1
	MatchModeUnknown MatchMode = 2
)

// PluginSelector the only valid value for now is ByNameSelector
// and only value is string type. It can be expanded with a custom type in the future.
type PluginSelector struct {
	CondType  ConditionType
	CondValue string
}

type ConditionType int

const (
	// UnknownSelector value is default, therefore it's safer to use it as default unknown value.
	UnknownSelector ConditionType = iota
	ByNameSelector
)
