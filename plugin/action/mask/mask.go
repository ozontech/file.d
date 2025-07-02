package mask

import (
	"strconv"
	"strings"

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

	fieldPaths [][]string

	fieldMasksRoot *fieldMasksNode
	emptyFMNode    *fieldMasksNode // alloc optimization for tree traversal

	hasProcessOrIgnoreFields  bool
	hasGlobalIgnoreFields     bool
	hasGlobalProcessFields    bool
	hasMaskSpecificFieldsList bool
	hasMasksIgnoreFields      []bool
	hasMasksProcessFields     []bool

	logger *zap.Logger

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
	IgnoreFields []string `json:"ignore_fields" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > List of the processed event fields.
	// > If name of some field contained in this list
	// > then all nested fields will be processed (even if they are not listed).
	// > If ignored fields list is empty and processed fields list is empty
	// > we consider this as empty ignored fields list (all fields will be processed).
	// > It is wrong to set non-empty ignored fields list and non-empty processed fields list at the same time.
	ProcessFields []string `json:"process_fields" slice:"true"` // *

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

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "mask",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = *config.(*Config)                            // copy shared config
	p.config.Masks = append([]Mask(nil), p.config.Masks...) // copy shared masks

	p.maskBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.sourceBuf = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.logger = params.Logger.Desugar()
	p.config.Masks = compileMasks(p.config.Masks, p.logger)

	if err := p.gatherFieldPaths(); err != nil {
		p.logger.Fatal("failed to gather ignore/process fields paths", zap.Error(err))
	}

	if err := p.gatherFieldMasksTree(); err != nil {
		p.logger.Fatal("failed to build field masks tree", zap.Error(err))
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

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for i := range p.config.Masks {
		if p.config.Masks[i].DoIfChecker != nil {
			p.config.Masks[i].use = p.config.Masks[i].DoIfChecker.Check(event.Root)
		}
	}

	maskApplied := false
	if p.hasGlobalProcessFields && !p.hasMaskSpecificFieldsList { // fast path when there are no mask-specific fields
		for _, fieldPath := range p.fieldPaths {
			node := event.Root.Dig(fieldPath...)
			if p.traverseTreeIterative(event, node, nil) {
				maskApplied = true
			}
			// if p.traverseTree(event, node, nil) {
			// 	maskApplied = true
			// }
		}
	} else {
		maskApplied = p.traverseTreeIterative(event, event.Root.Node, p.fieldMasksRoot)
		// maskApplied = p.traverseTree(event, event.Root.Node, p.fieldMasksRoot)
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

func (p *Plugin) traverseTreeIterative(event *pipeline.Event, rootNode *insaneJSON.Node, rootFmNode *fieldMasksNode) bool {
	var nextFmNode *fieldMasksNode
	result := false

	nodeStack := newStack(nodePair{rootNode, rootFmNode})

	for !nodeStack.isEmpty() {
		curNodePair := nodeStack.pop()
		curNode := curNodePair.jNode
		curFmNode := curNodePair.fmNode
		nextFmNode = nil

		shouldCheckFmNode := curFmNode != nil && len(curFmNode.children) > 0
		if !shouldCheckFmNode {
			nextFmNode = curFmNode
		}

		switch {
		case curNode == nil:
			continue
		case curNode.IsField():
			if shouldCheckFmNode {
				if _fmNode, has := curFmNode.children[curNode.AsString()]; has {
					// fast path when there are no mask-specific fields
					if _fmNode.globalIgnore && !p.hasMaskSpecificFieldsList {
						continue
					}
					nextFmNode = _fmNode
				} else {
					nextFmNode = p.emptyFMNode
				}
			}
			nextNode := curNode.AsFieldValue()
			nodeStack.add(nodePair{nextNode, nextFmNode})
			continue
		case curNode.IsArray():
			for i, nextNode := range curNode.AsArray() {
				if shouldCheckFmNode {
					if _fmNode, has := curFmNode.children[strconv.Itoa(i)]; has {
						nextFmNode = _fmNode
					} else {
						nextFmNode = p.emptyFMNode
					}
				}
				nodeStack.add(nodePair{nextNode, nextFmNode})
			}
			continue
		case curNode.IsObject():
			for _, nextNode := range curNode.AsFields() {
				nodeStack.add(nodePair{nextNode, curFmNode})
			}
			continue
		default:
			if p.processMaskOps(event, curNode, curFmNode) {
				result = true
			}
		}
	}

	return result
}

func (p *Plugin) traverseTree(event *pipeline.Event, curNode *insaneJSON.Node, curFmNode *fieldMasksNode) bool {
	var nextFmNode *fieldMasksNode

	shouldCheckFmNode := curFmNode != nil && len(curFmNode.children) > 0
	result := false

	if !shouldCheckFmNode {
		nextFmNode = curFmNode
	}

	switch {
	case curNode == nil:
		return false
	case curNode.IsField():
		if shouldCheckFmNode {
			if _fmNode, has := curFmNode.children[curNode.AsString()]; has {
				// fast path when there are no mask-specific fields
				if _fmNode.globalIgnore && !p.hasMaskSpecificFieldsList {
					return false
				}
				nextFmNode = _fmNode
			} else {
				nextFmNode = p.emptyFMNode
			}
		}
		nextNode := curNode.AsFieldValue()
		return p.traverseTree(event, nextNode, nextFmNode)
	case curNode.IsArray():
		for i, nextNode := range curNode.AsArray() {
			if shouldCheckFmNode {
				if _fmNode, has := curFmNode.children[strconv.Itoa(i)]; has {
					nextFmNode = _fmNode
				} else {
					nextFmNode = p.emptyFMNode
				}
			}
			if p.traverseTree(event, nextNode, nextFmNode) {
				result = true
			}
		}
	case curNode.IsObject():
		for _, nextNode := range curNode.AsFields() {
			if p.traverseTree(event, nextNode, curFmNode) {
				result = true
			}
		}
	default:
		return p.processMaskOps(event, curNode, curFmNode)
	}

	return result
}

func (p *Plugin) processMaskOps(event *pipeline.Event, curNode *insaneJSON.Node, fmNode *fieldMasksNode) bool {
	locApplied := false
	maskApplied := false
	shouldUpdateValue := false
	value := curNode.AsBytes()
	p.sourceBuf = p.sourceBuf[:0]
	for i := range p.config.Masks {
		if p.hasProcessOrIgnoreFields { // check process/ignore fields lists
			switch {
			case p.hasMasksIgnoreFields[i]:
				if fmNode != nil {
					if _, has := fmNode.ignoreMasks[i]; has {
						continue
					}
				}
			case p.hasMasksProcessFields[i]:
				if fmNode == nil {
					continue
				}
				if _, has := fmNode.processMasks[i]; !has {
					continue
				}
			case p.hasGlobalIgnoreFields:
				if fmNode != nil && fmNode.globalIgnore {
					continue
				}
			case p.hasGlobalProcessFields:
				if fmNode != nil && !fmNode.globalProcess {
					continue
				}
			}
		}
		mask := &p.config.Masks[i]
		if !(mask.use && mask.checkMatchRules(value)) {
			continue
		}
		shouldApplyMask := mask.Re != "" && len(mask.Groups) > 0
		if shouldApplyMask {
			if len(p.sourceBuf) == 0 {
				// copy node value to process in mask only once when required
				p.sourceBuf = append(p.sourceBuf[:0], value...)
			}
			p.maskBuf, locApplied = mask.maskValue(p.sourceBuf, p.maskBuf)
			if !locApplied {
				continue
			}
			p.sourceBuf = append(p.sourceBuf[:0], p.maskBuf...)
			shouldUpdateValue = true
		}
		if mask.AppliedField != "" {
			event.Root.AddFieldNoAlloc(event.Root, mask.AppliedField).MutateToString(mask.AppliedValue)
		}
		if mask.MetricName != "" {
			p.applyMaskMetric(mask, event)
		}
		maskApplied = true
	}
	if shouldUpdateValue {
		curNode.MutateToString(string(p.maskBuf))
	}

	return maskApplied
}
