package mask

import (
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
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
      ignore_fields:
      - trace_id
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

}*/

const (
	substitution = byte('*')
)

type Plugin struct {
	config Config

	// sourceBuf buffer for storing node value initial and transformed
	sourceBuf []byte
	// maskBuf buffer for storing data in the process of masking
	// (data before masked entry, its replacement and data after masked entry)
	maskBuf []byte

	// common match regex
	matchRe *regexp.Regexp

	fieldPaths   [][]string
	isWhitelist  bool
	ignoredNodes []*insaneJSON.Node

	valueNodes []*insaneJSON.Node
	logger     *zap.Logger

	//  plugin metrics
	maskAppliedMetric *prometheus.CounterVec
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > List of masks.
	Masks []Mask `json:"masks"` // *

	// > @3@4@5@6
	// >
	// > **Experimental feature** for best performance. Skips events with mismatched masks.
	SkipMismatched bool `json:"skip_mismatched" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If any mask has been applied then `mask_applied_field` will be set to `mask_applied_value` in the event.
	MaskAppliedField string `json:"mask_applied_field"` // *

	// > @3@4@5@6
	// >
	MaskAppliedValue string `json:"mask_applied_value"` // *

	// > @3@4@5@6
	// >
	// > List of the ignored event fields.
	// > If name of some field contained in this list
	// > then all nested fields will be ignored (even if they are not listed).
	IgnoreFields []cfg.FieldSelector `json:"ignore_fields" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > List of the processed event fields.
	// > If name of some field contained in this list
	// > then all nested fields will be processed (even if they are not listed).
	// > If ignored fields list is empty and processed fields list is empty
	// > we consider this as empty ignored fields list (all fields will be processed).
	// > It is wrong to set non-empty ignored fields list and non-empty processed fields list at the same time.
	ProcessFields []cfg.FieldSelector `json:"process_fields" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > The metric name of the regular expressions applied.
	AppliedMetricName string `json:"applied_metric_name" default:"mask_applied_total"` // *

	// > @3@4@5@6
	// >
	// > Lists the event fields to add to the metric. Blank list means no labels.
	// > Important note: labels metrics are not currently being cleared.
	AppliedMetricLabels []string `json:"applied_metric_labels"` // *
}

type mode int

const (
	modeMask mode = iota
	modeReplace
	modeCut
)

type Mask struct {
	// > @3@4@5@6
	// >
	// > List of matching rules to filter out events before checking regular expression for masking.
	MatchRules matchrule.RuleSets `json:"match_rules"` // *

	// > @3@4@5@6
	// >
	// > Regular expression for masking.
	Re  string `json:"re" default:""` // *
	Re_ *regexp.Regexp

	// > @3@4@5@6
	// >
	// > Groups are numbers of masking groups in expression, zero for mask all expression.
	Groups []int `json:"groups"` // *

	// > @3@4@5@6
	// >
	// > MaxCount limits the number of masked symbols in the masked output, if zero, no limit is set.
	MaxCount int `json:"max_count"` // *

	// > @3@4@5@6
	// >
	// > ReplaceWord, if set, is used instead of asterisks for masking patterns that are of the same length or longer.
	ReplaceWord string `json:"replace_word"` // *

	// > @3@4@5@6
	// >
	// > CutValues, if set, masking parts will be cut instead of being replaced with ReplaceWord or asterisks.
	CutValues bool `json:"cut_values"` // *

	mode mode

	// > @3@4@5@6
	// >
	// > If the mask has been applied then `applied_field` will be set to `applied_value` in the event.
	AppliedField string `json:"applied_field"` // *

	// > @3@4@5@6
	// >
	// > Value to be set in `applied_field`.
	AppliedValue string `json:"applied_value"` // *

	// > @3@4@5@6
	// >
	// > The metric name of the regular expressions applied.
	// > The metric name for a mask cannot be the same as metric name for plugin.
	MetricName string `json:"metric_name"` // *

	// > @3@4@5@6
	// >
	// > Lists the event fields to add to the metric. Blank list means no labels.
	// > Important note: labels metrics are not currently being cleared.
	MetricLabels []string `json:"metric_labels"` // *

	// mask metric
	appliedMetric *prometheus.CounterVec
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

func (p *Plugin) makeMetric(ctl *metric.Ctl, name, help string, labels ...string) *prometheus.CounterVec {
	if name == "" {
		return nil
	}

	uniq := make(map[string]struct{})
	labelNames := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			p.logger.Fatal("empty label name")
		}
		if _, ok := uniq[label]; ok {
			p.logger.Fatal("metric labels must be unique")
		}
		uniq[label] = struct{}{}

		labelNames = append(labelNames, label)
	}

	return ctl.RegisterCounterVec(name, help, labelNames...)
}

func compileMasks(masks []Mask, logger *zap.Logger) ([]Mask, *regexp.Regexp) {
	patterns := make([]string, 0, len(masks))
	for i := range masks {
		compileMask(&masks[i], logger)
		if masks[i].Re != "" {
			patterns = append(patterns, masks[i].Re)
		}
	}

	combinedPattern := strings.Join(patterns, "|")
	logger.Info("compiling match regexp", zap.String("re", combinedPattern))
	matchRegex, err := regexp.Compile(combinedPattern)
	if err != nil {
		logger.Warn("error on compiling match regexp", zap.Error(err))
	}

	return masks, matchRegex
}

func compileMask(m *Mask, logger *zap.Logger) {
	if m.Re == "" && len(m.MatchRules) == 0 {
		logger.Fatal("mask must have either nonempty regex or ruleset, or both")
	}

	setModeReplace := m.ReplaceWord != ""
	setModeCut := m.CutValues
	if setModeReplace && setModeCut {
		logger.Fatal("replace mode and cut mode are incompatible")
	}

	switch {
	case setModeReplace:
		m.mode = modeReplace
	case setModeCut:
		m.mode = modeCut
	default:
		m.mode = modeMask
	}

	if m.Re != "" {
		logger.Info("compiling", zap.String("re", m.Re), zap.Ints("groups", m.Groups))
		re, err := regexp.Compile(m.Re)
		if err != nil {
			logger.Fatal("error on compiling regexp", zap.String("re", m.Re))
		}
		m.Re_ = re
		m.Groups = cfg.VerifyGroupNumbers(m.Groups, re.NumSubexp(), logger)
	}
	for i, matchRule := range m.MatchRules {
		if len(matchRule.Rules) == 0 {
			logger.Fatal("ruleset must contain at least one rule")
		}
		for _, rule := range matchRule.Rules {
			if len(rule.Values) == 0 {
				logger.Fatal("rule in ruleset must have at least one value")
			}
		}
		m.MatchRules[i].Prepare()
	}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = *config.(*Config)                            // copy shared config
	p.config.Masks = append([]Mask(nil), p.config.Masks...) // copy shared masks

	for i := range p.config.Masks {
		mask := &p.config.Masks[i]
		if mask.MaxCount > 0 && mask.ReplaceWord != "" {
			p.logger.Fatal("invalid mask configuration")
		}
	}
	p.maskBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.sourceBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.valueNodes = make([]*insaneJSON.Node, 0)
	p.logger = params.Logger.Desugar()
	p.config.Masks, p.matchRe = compileMasks(p.config.Masks, p.logger)

	isBlacklist := len(p.config.IgnoreFields) > 0
	isWhitelist := len(p.config.ProcessFields) > 0
	if isBlacklist && isWhitelist {
		p.logger.Fatal("ignored fields list and processed fields list are both non-empty")
	}

	p.isWhitelist = isWhitelist

	var fieldPaths []cfg.FieldSelector
	if isBlacklist {
		fieldPaths = p.config.IgnoreFields
	} else if isWhitelist {
		fieldPaths = p.config.ProcessFields
	}

	if len(fieldPaths) > 0 {
		p.fieldPaths = make([][]string, 0, len(fieldPaths))
		for _, fieldPath := range fieldPaths {
			p.fieldPaths = append(p.fieldPaths, cfg.ParseFieldSelector(string(fieldPath)))
		}
	}

	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.maskAppliedMetric = p.makeMetric(ctl,
		p.config.AppliedMetricName,
		"Number of times mask plugin found the provided pattern",
		p.config.AppliedMetricLabels...,
	)
	for i := range p.config.Masks {
		mask := &p.config.Masks[i]
		if mask.MetricName == p.config.AppliedMetricName {
			p.logger.Error(
				"mask cannot have metric with the same name as the plugin",
				zap.String("metric_name", mask.MetricName),
			)
			continue
		}
		mask.appliedMetric = p.makeMetric(ctl,
			mask.MetricName,
			"Number of times mask found in the provided pattern",
			mask.MetricLabels...,
		)
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) maskSection(mask *Mask, dst, src []byte, begin, end int) []byte {
	switch mask.mode {
	case modeReplace:
		return append(dst, mask.ReplaceWord...)
	case modeCut:
		return dst
	case modeMask:
		n := utf8.RuneCount(src[begin:end])
		if mask.MaxCount > 0 {
			n = min(n, mask.MaxCount)
		}

		for i := 0; i < n; i++ {
			dst = append(dst, substitution)
		}

		return dst
	default:
		panic("invalid masking mode")
	}
}

func (p *Plugin) applyMaskMetric(mask *Mask, event *pipeline.Event) {
	if mask.appliedMetric == nil {
		return
	}

	labelValues := make([]string, 0, len(mask.MetricLabels))
	for _, labelValuePath := range mask.MetricLabels {
		value := "not_set"
		if node := event.Root.Dig(labelValuePath); node != nil {
			value = strings.Clone(node.AsString())
		}

		labelValues = append(labelValues, value)
	}

	mask.appliedMetric.WithLabelValues(labelValues...).Inc()

	if ce := p.logger.Check(zap.DebugLevel, "mask appeared to event"); ce != nil {
		ce.Write(zap.String("event", event.Root.EncodeToString()))
	}
}

// mask value returns masked value and bool answer was buf masked at all.
func (p *Plugin) maskValue(mask *Mask, value, buf []byte) ([]byte, bool) {
	buf = append(buf[:0], value...)
	skip := true
	for i := range mask.MatchRules {
		if mask.MatchRules[i].Match(value) {
			skip = false
			break
		}
	}
	if skip && len(mask.MatchRules) > 0 {
		return buf, false
	}

	if mask.Re == "" {
		return buf, true
	}

	indexes := mask.Re_.FindAllSubmatchIndex(value, -1)
	if len(indexes) == 0 {
		return buf, false
	}
	// special case, groups can be an empty slice,
	// but the mask is considered as applied for accounting metrics
	if len(mask.Groups) == 0 {
		return buf, true
	}

	buf = buf[:0]

	prevFinish := 0
	curStart, curFinish := 0, 0
	for _, index := range indexes {
		for _, grp := range mask.Groups {
			curStart = index[grp*2]
			curFinish = index[grp*2+1]

			buf = append(buf, value[prevFinish:curStart]...)
			prevFinish = curFinish

			buf = p.maskSection(
				mask,
				buf,
				value,
				curStart,
				curFinish,
			)
		}
	}

	return append(buf, value[curFinish:]...), true
}

func getNestedValueNodes(currentNode *insaneJSON.Node, ignoredNodes []*insaneJSON.Node, valueNodes []*insaneJSON.Node) []*insaneJSON.Node {
	switch {
	case currentNode == nil:
		break
	case currentNode.IsField():
		valueNodes = getNestedValueNodes(currentNode.AsFieldValue(), ignoredNodes, valueNodes)
	case slices.Contains(ignoredNodes, currentNode):
		break
	case currentNode.IsArray():
		for _, n := range currentNode.AsArray() {
			valueNodes = getNestedValueNodes(n, ignoredNodes, valueNodes)
		}
	case currentNode.IsObject():
		for _, n := range currentNode.AsFields() {
			valueNodes = getNestedValueNodes(n, ignoredNodes, valueNodes)
		}
	default:
		valueNodes = append(valueNodes, currentNode)
	}

	return valueNodes
}

func (p *Plugin) getValueNodes(root *insaneJSON.Node, valueNodes []*insaneJSON.Node) []*insaneJSON.Node {
	if p.isWhitelist {
		for _, fieldPath := range p.fieldPaths {
			valueNodes = getNestedValueNodes(root.Dig(fieldPath...), nil, valueNodes)
		}
	} else {
		p.ignoredNodes = p.ignoredNodes[:0]
		for _, fieldPath := range p.fieldPaths {
			p.ignoredNodes = append(p.ignoredNodes, root.Dig(fieldPath...))
		}
		valueNodes = getNestedValueNodes(root, p.ignoredNodes, valueNodes)
	}

	return valueNodes
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	root := event.Root.Node

	// apply vars need to check if mask was applied to event data and send metric
	maskApplied := false
	locApplied := false

	p.valueNodes = p.valueNodes[:0]
	p.valueNodes = p.getValueNodes(root, p.valueNodes)
	for _, v := range p.valueNodes {
		value := v.AsBytes()
		var valueIsCommonMatched bool
		if p.config.SkipMismatched {
			// is matched by common mask
			valueIsCommonMatched = p.matchRe.Match(value)
		} else {
			// to always try to apply a mask
			valueIsCommonMatched = true
		}

		p.sourceBuf = append(p.sourceBuf[:0], value...)
		// valueMasked is not the same as maskApplied;
		// it shows if current node is masked and really needs to be changed
		valueMasked := false
		for i := range p.config.Masks {
			mask := &p.config.Masks[i]
			if mask.Re != "" && !valueIsCommonMatched {
				// skips messages not matched common regex
				continue
			}

			p.maskBuf, locApplied = p.maskValue(mask, p.sourceBuf, p.maskBuf)
			p.sourceBuf = append(p.sourceBuf[:0], p.maskBuf...)
			if !locApplied {
				continue
			}
			valueMasked = true

			if mask.AppliedField != "" {
				event.Root.AddFieldNoAlloc(event.Root, mask.AppliedField).MutateToString(mask.AppliedValue)
			}
			if mask.MetricName != "" {
				p.applyMaskMetric(mask, event)
			}

			maskApplied = true
		}

		if valueMasked {
			v.MutateToString(string(p.maskBuf))
		}
	}

	if p.config.MaskAppliedField != "" && maskApplied {
		event.Root.AddFieldNoAlloc(event.Root, p.config.MaskAppliedField).MutateToString(p.config.MaskAppliedValue)
	}

	if maskApplied && p.config.AppliedMetricName != "" {
		labelValues := make([]string, 0, len(p.config.AppliedMetricLabels))
		for _, labelValuePath := range p.config.AppliedMetricLabels {
			value := "not_set"
			if node := event.Root.Dig(labelValuePath); node != nil {
				value = strings.Clone(node.AsString())
			}

			labelValues = append(labelValues, value)
		}

		p.maskAppliedMetric.WithLabelValues(labelValues...).Inc()

		if ce := p.logger.Check(zap.DebugLevel, "mask appeared to event"); ce != nil {
			ce.Write(zap.String("event", event.Root.EncodeToString()))
		}
	}

	return pipeline.ActionPass
}
