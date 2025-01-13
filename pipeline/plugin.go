package pipeline

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline/doif"
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
	MetricCtl        *metric.Ctl
}

type ActionPluginParams struct {
	PluginDefaultParams
	Controller ActionPluginController
	Logger     *zap.SugaredLogger
}

type OutputPluginParams struct {
	PluginDefaultParams
	Controller OutputPluginController
	Logger     *zap.SugaredLogger
}

type InputPluginParams struct {
	PluginDefaultParams
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

	MetricName       string
	MetricLabels     []string
	MetricSkipStatus bool
	MatchConditions  MatchConditions
	MatchMode        MatchMode
	MatchInvert      bool

	DoIfChecker *doif.Checker
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
	// > {"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # discarded
	// > {"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # discarded
	// > {"k8s_namespace": "payment-tarifficator", "k8s_pod":"payment-api"} # won't be discarded
	// > {"k8s_namespace": "tarifficator", "k8s_pod":"no-payment-api"}      # won't be discarded
	// > ```
	"and": MatchModeAnd, // *

	// > @3 — matches fields with OR operator
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
	// >         match_mode: or
	// > ```
	// >
	// > result:
	// > ```
	// > {"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # discarded
	// > {"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # discarded
	// > {"k8s_namespace": "map", "k8s_pod":"payment-api"}                  # discarded
	// > {"k8s_namespace": "payment", "k8s_pod":"map-api"}                  # discarded
	// > {"k8s_namespace": "tarifficator", "k8s_pod":"tarifficator-go-api"} # discarded
	// > {"k8s_namespace": "sre", "k8s_pod":"cpu-quotas-abcd-1234"}         # won't be discarded
	// > ```
	"or": MatchModeOr, // *

	// > @3 — matches fields with AND operator
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         match_fields:
	// >           k8s_namespace: payment # use prefix match
	// >           k8s_pod: payment-api- # use prefix match
	// >         match_mode: and_prefix
	// >  ```
	// >
	// > result:
	// > ```
	// > {"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"}    # discarded
	// > {"k8s_namespace": "payment-2", "k8s_pod":"payment-api-abcd-1234"}  # discarded
	// > {"k8s_namespace": "payment", "k8s_pod":"checkout"}                 # won't be discarded
	// > {"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"}        # won't be discarded
	// > {"k8s_namespace": "payment-abcd", "k8s_pod":"payment-api"}         # won't be discarded
	// > ```
	"and_prefix": MatchModeAndPrefix, // *

	// > @3 — matches fields with OR operator
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         match_fields:
	// >           k8s_namespace: [payment, tarifficator] # use prefix match
	// >           k8s_pod: /-api-.*/ # use regexp match
	// >         match_mode: or_prefix
	// > ```
	// >
	// > result:
	// > ```
	// > {"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"}    # discarded
	// > {"k8s_namespace": "payment", "k8s_pod":"checkout"}                 # discarded
	// > {"k8s_namespace": "map", "k8s_pod":"map-go-api-abcd-1234"}         # discarded
	// > {"k8s_namespace": "map", "k8s_pod":"payment-api"}                  # won't be discarded
	// > {"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"}        # discarded
	// > {"k8s_namespace": "tariff", "k8s_pod":"tarifficator"}              # won't be discarded
	// > ```
	"or_prefix": MatchModeOrPrefix, // *
}

func MatchModeFromString(mm string) MatchMode {
	mode, ok := MatchModes[strings.ToLower(strings.TrimSpace(mm))]
	if !ok {
		return MatchModeUnknown
	}
	return mode
}

// PluginSelector the only valid value for now is ByNameSelector
// and only value is string type. It can be expanded with a custom type in the future.
type PluginSelector struct {
	CondType  ConditionType
	CondValue string
}

type ConditionType int

const (
	ByNameSelector = iota + 1
)
