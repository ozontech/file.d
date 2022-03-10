package mask

import (
	"regexp"
	"unicode/utf8"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      metric_subsystem_name: "some_name"
      masks:
      - mask:
        re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

}*/

const (
	substitution = byte('*')

	metricName = "mask_plugin"
)

var MaskPromCounter = prometheus.NewCounter(prometheus.CounterOpts{})

type Plugin struct {
	config          *Config
	sourceBuf       []byte
	maskBuf         []byte
	logMaskAppeared bool
	valueNodes      []*insaneJSON.Node
	logger          *zap.SugaredLogger
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> If set counterMetric with this name would be sent on metric_subsystem_name.mask_plugin
	MetricSubsystemName *string `json:"metric_subsystem_name" required:"false"` //*

	//> @3@4@5@6
	//>
	//> List of masks.
	Masks []Mask `json:"masks"` //*
}

type Mask struct {
	//> @3@4@5@6
	//>
	//> Regular expression for masking
	Re  string `json:"re" default:"" required:"true"` //*
	Re_ *regexp.Regexp

	//> @3@4@5@6
	//>
	//> Numbers of masking groups in expression, zero for mask all expression
	Groups []int `json:"groups" required:"true"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "mask",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func compileMasks(masks []Mask, logger *zap.SugaredLogger) []Mask {
	out := make([]Mask, 0, len(masks))
	for _, m := range masks {
		out = append(out, compileMask(m, logger))
	}
	return out
}

func compileMask(m Mask, logger *zap.SugaredLogger) Mask {
	logger.Infof("compiling, re=%s, groups=%v", m.Re, m.Groups)
	re, err := regexp.Compile(m.Re)
	if err != nil {
		logger.Fatalf("error on compiling regexp, regexp=%s", m.Re)
	}

	groups := verifyGroupNumbers(m.Groups, re.NumSubexp(), logger)
	return Mask{Re: m.Re, Re_: re, Groups: groups}
}

func isGroupsUnique(groups []int) bool {
	uniqueGrp := make(map[int]struct{}, len(groups))
	var exists struct{}
	for _, g := range groups {
		if _, isContains := uniqueGrp[g]; isContains {
			return false
		}
		uniqueGrp[g] = exists
	}
	return true
}

func verifyGroupNumbers(groups []int, totalGroups int, logger *zap.SugaredLogger) []int {
	if len(groups) == 0 {
		logger.Fatal("groups is empty")
	}

	if !isGroupsUnique(groups) {
		logger.Fatalf("groups numbers must be unique, groups numbers=%v", groups)
	}

	if len(groups) > totalGroups {
		logger.Fatalf("there are many groups, groups=%d, totalGroups=%d", len(groups), totalGroups)
	}

	for _, g := range groups {
		if g > totalGroups || g < 0 {
			logger.Fatalf("wrong group number, number=%d", g)
		} else if g == 0 {
			return []int{0}
		}
	}
	return groups
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.maskBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.sourceBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.valueNodes = make([]*insaneJSON.Node, 0)
	p.logger = params.Logger
	p.config.Masks = compileMasks(p.config.Masks, p.logger)
	if p.config.MetricSubsystemName != nil {
		p.logMaskAppeared = true
		p.registerPluginMetrics(pipeline.PromNamespace, *p.config.MetricSubsystemName, metricName)
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) registerPluginMetrics(namespace, subsystem, metricName string) {
	// can't declare counter as property on p.counter, because multiple cores
	// will create multiple metrics and all but last one will be unregistered.
	MaskPromCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      metricName,
		Help:      "",
	})
	prometheus.DefaultRegisterer.Unregister(MaskPromCounter)
	prometheus.DefaultRegisterer.MustRegister(MaskPromCounter)
}

func appendMask(dst, src []byte, begin, end int) ([]byte, int) {
	runeCounter := utf8.RuneCount(src[begin:end])
	for j := 0; j < runeCounter; j++ {
		dst = append(dst, substitution)
	}
	offset := len(src[begin:end]) - runeCounter
	return dst, offset
}

func maskSection(dst, src []byte, begin, end int) ([]byte, int) {
	dst = append(dst, src[:begin]...)

	dst, offset := appendMask(dst, src, begin, end)

	if len(dst)+offset < len(src) {
		dst = append(dst, src[end:]...)
	}

	return dst, offset
}

// mask value returns masked value and bool answer was buf masked at all.
func maskValue(value, buf []byte, re *regexp.Regexp, groups []int) ([]byte, bool) {
	indexes := re.FindAllSubmatchIndex(value, -1)
	if len(indexes) == 0 {
		return value, false
	}

	buf = buf[:0]

	offset := 0
	for _, index := range indexes {
		for _, grp := range groups {
			value, offset = maskSection(
				buf,
				value,
				index[grp*2]-offset,
				index[grp*2+1]-offset,
			)
		}
	}

	return value, true
}

func getValueNodeList(currentNode *insaneJSON.Node, valueNodes []*insaneJSON.Node) []*insaneJSON.Node {
	switch {
	case currentNode.IsField():
		valueNodes = getValueNodeList(currentNode.AsFieldValue(), valueNodes)
	case currentNode.IsArray():
		for _, n := range currentNode.AsArray() {
			valueNodes = getValueNodeList(n, valueNodes)
		}
	case currentNode.IsObject():
		for _, n := range currentNode.AsFields() {
			valueNodes = getValueNodeList(n, valueNodes)
		}
	default:
		valueNodes = append(valueNodes, currentNode)
	}
	return valueNodes
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	root := event.Root.Node

	// apply vars need to check if mask was applied to event data and send metric.
	maskApplied := false
	locApplied := false

	p.valueNodes = p.valueNodes[:0]
	p.valueNodes = getValueNodeList(root, p.valueNodes)
	for _, v := range p.valueNodes {
		value := v.AsBytes()
		p.sourceBuf = append(p.sourceBuf[:0], value...)
		p.maskBuf = append(p.maskBuf[:0], p.sourceBuf...)
		for _, mask := range p.config.Masks {
			p.maskBuf, locApplied = maskValue(p.sourceBuf, p.maskBuf, mask.Re_, mask.Groups)
			p.sourceBuf = p.maskBuf
			if locApplied {
				maskApplied = true
			}
		}
		v.MutateToString(string(p.maskBuf))
	}
	if p.logMaskAppeared && maskApplied {
		MaskPromCounter.Inc()
		p.logger.Infof("mask appeared to event, output string: %s", event.Root.EncodeToString())
	}

	return pipeline.ActionPass
}
