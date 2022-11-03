package pipeline

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/ozontech/file.d/metric"
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
	RegisterMetrics(ctl *metric.Ctl)
	PassEvent(event *Event) bool
}

type ActionPlugin interface {
	Start(config AnyConfig, params *ActionPluginParams)
	Stop()
	Do(*Event) ActionResult
	RegisterMetrics(ctl *metric.Ctl)
}

type OutputPlugin interface {
	Start(config AnyConfig, params *OutputPluginParams)
	Stop()
	Out(*Event)
	RegisterMetrics(ctl *metric.Ctl)
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
	Values []string
	Regexp *regexp.Regexp
}

func (mc *MatchCondition) valueExists(s string, byPrefix bool) bool {
	var match bool
	for i := range mc.Values {
		if byPrefix {
			match = strings.HasPrefix(s, mc.Values[i])
		} else {
			match = mc.Values[i] == s
		}
		if match {
			break
		}
	}
	return match
}

type MatchMode int

const (
	MatchModeAnd MatchMode = iota
	MatchModeOr
	MatchModeAndPrefix
	MatchModeOrPrefix
	MatchModeUnknown
)

// ! match-modes
// ^ match-modes

var MatchModes = map[string]MatchMode{
	"": MatchModeAnd,
	// > @3 — matches fields with AND operator
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         match_fields:
	// >           k8s_namespace: [payment, tarifficator] # use exact match
	// >           k8s_pod: /^payment-api.*/              # use regexp match
	// >         match_mode: and
	// > ```
	// >
	// > result:
	// > ```
	// > {"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # won't be discarded
	// > {"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # discarded
	// > ```
	"and": MatchModeAnd, // *
	// > Or mode
	"or": MatchModeOr, // *
	// > And prefix mode
	"and_prefix": MatchModeAndPrefix, // *
	// > Or prefix mode
	"or_prefix": MatchModeOr, // *
}

func MatchModeFromString(mm string) MatchMode {
	switch strings.ToLower(strings.TrimSpace(mm)) {
	case "", "and":
		return MatchModeAnd
	case "or":
		return MatchModeOr
	case "and_prefix":
		return MatchModeAndPrefix
	case "or_prefix":
		return MatchModeOrPrefix
	default:
		return MatchModeUnknown
	}
}

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
