package pipeline

import (
	"regexp"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
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
	Start(config AnyConfig)
	Stop()
	Do(*Event) ActionResult
	Reset()
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
	PluginMetrics
}

type ActionPluginData struct {
	Plugin ActionPlugin

	PluginDesc
	PluginMetrics

	MatchConditions MatchConditions
	MatchMode       MatchMode
}

type OutputPluginData struct {
	Plugin OutputPlugin

	PluginDesc
	PluginMetrics
}

type PluginDesc struct {
	ID     string
	T      string // plugin type
	Config AnyConfig
}

type PluginMetrics struct {
	MetricName  string
	LabelNames  []string
	labelValues []string

	counter     *prometheus.CounterVec
	counterPrev *prometheus.CounterVec
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
