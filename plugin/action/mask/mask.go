package mask

import (
	"strconv"
	"strings"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Note**: masks are applied only to string or number values.

**Example 1:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

Mask plugin can have white and black lists for fields using `process_fields` and `ignore_fields` parameters respectively.
Elements of `process_fields` and `ignore_fields` lists are json paths (e.g. `message` — field `message` in root,
`field.subfield` — field `subfield` inside object value of field `field`).

**Note**: `process_fields` and `ignore_fields` cannot be used simultaneously.

**Example 2:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      ignore_fields:
      - trace_id
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

All masks will be applied to all fields in the event except for the `trace_id` field in the root of the event.

**Example 3:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      process_fields:
      - message
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

All masks will be applied only to `message` field in the root of the event.

Also `process_fields` and `ignore_fields` lists can be used on per mask basis. In that case, if a mask has
non-empty `process_fields` or `ignore_fields` and there is non-empty `process_fields` or `ignore_fields`
in plugin parameters, mask fields lists will override plugin lists.

**Example 3:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      ignore_fields:
      - trace_id
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
      - re: "(test)"
        groups: [1]
		process_fields:
		- message
    ...
```

The first mask will be applied to all fields in the event except for the `trace_id` field in the root of the event.
The second mask will be applied only to `message` field in the root of the event.
}*/

const (
	substitution = byte('*')
)

type Plugin struct {
	config Config
	params *pipeline.ActionPluginParams

	// sourceBuf buffer for storing node value initial and transformed
	sourceBuf []byte
	// maskBuf buffer for storing data in the process of masking
	// (data before masked entry, its replacement and data after masked entry)
	maskBuf []byte

	fieldPaths [][]string // list of fields for fast path when only global process fields list is used

	fieldMasksRoot *fieldMasksNode // fields lists converted to a tree in [(*Plugin).gatherFieldMasksTree]
	emptyFMNode    *fieldMasksNode // nil value node for tree traversal

	hasProcessOrIgnoreFields  bool   // fast check for process ignore fields lists global or mask-specific
	hasGlobalIgnoreFields     bool   // fast check for global ignore fields list
	hasGlobalProcessFields    bool   // fast check for global process fields list
	hasMaskSpecificFieldsList bool   // fast check for mask-specific ignore or process fields lists
	hasMasksIgnoreFields      []bool // flags for which masks have ignore fields lists
	hasMasksProcessFields     []bool // flags for which masks have process fields lists
	hasMaskSpecificDoIf       bool

	maskApplyCount        []uint64
	hasMaskSpecificMetric bool

	logger *zap.Logger

	// plugin metrics
	maskAppliedMetric *metric.CounterVec
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
	// > **Deprecated** currently does nothing.
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
	p.params = params

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

	for i := range p.config.Masks {
		if p.config.Masks[i].MetricName != "" {
			p.hasMaskSpecificMetric = true
			p.maskApplyCount = make([]uint64, len(p.config.Masks))
			break
		}
	}

	for i := range p.config.Masks {
		if p.config.Masks[i].DoIfChecker != nil {
			p.hasMaskSpecificDoIf = true
			break
		}
	}

	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.maskAppliedMetric = metric.NewHeldCounterVec(
		p.makeMetric(ctl,
			p.config.AppliedMetricName,
			"Number of times mask plugin found the provided pattern",
			p.config.AppliedMetricLabels...,
		),
		p.params.PipelineSettings.MetricMaxLabelValueLength,
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
		mask.appliedMetric = metric.NewHeldCounterVec(
			p.makeMetric(ctl,
				mask.MetricName,
				"Number of times mask found in the provided pattern",
				mask.MetricLabels...,
			),
			p.params.PipelineSettings.MetricMaxLabelValueLength,
		)
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// check which masks to apply to the event
	if p.hasMaskSpecificDoIf {
		for i := range p.config.Masks {
			if p.config.Masks[i].DoIfChecker != nil {
				p.config.Masks[i].use = p.config.Masks[i].DoIfChecker.Check(event.Root)
			}
		}
	}

	maskApplied := false
	if p.hasGlobalProcessFields && !p.hasMaskSpecificFieldsList { // fast path when there are no mask-specific fields
		for _, fieldPath := range p.fieldPaths {
			node := event.Root.Dig(fieldPath...)
			if node == nil {
				continue
			}
			if p.traverseTree(event, node, nil) {
				maskApplied = true
			}
		}
	} else {
		maskApplied = p.traverseTree(event, event.Root.Node, p.fieldMasksRoot)
	}

	if maskApplied {
		if p.config.MaskAppliedField != "" {
			event.Root.AddFieldNoAlloc(event.Root, p.config.MaskAppliedField).MutateToString(p.config.MaskAppliedValue)
		}
		if p.config.AppliedMetricName != "" {
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
		if p.hasMaskSpecificMetric {
			for i := range p.config.Masks {
				delta := p.maskApplyCount[i]
				if delta == 0 {
					continue
				}
				p.applyMaskMetric(&p.config.Masks[i], event, delta)
				p.maskApplyCount[i] = 0
			}
		}
	}

	return pipeline.ActionPass
}

// traverseTree traverses JSON tree in DFS manner and applies masks to its leaves. Masks are applied only to strings
// and numbers since only these values can have some sensitive data in them, other value types are bool, null.
//
// If field mask node is provided, traverses its tree simultaneously with JSON tree. If field mask leaf node is matched
// with non-leaf JSON node, it is applied to all JSON node subtree.
// For example:
//
// Field mask tree (FM tree):
//
//	  a
//	 / \
//	b   c
//
// JSON tree:
//
//	  a
//	 / \
//	b   c
//	   / \
//	  d   e
//
// FM node "b" is matched with JSON node "b", FM node "c" is matched with JSON nodes "c", "d", "e".
func (p *Plugin) traverseTree(event *pipeline.Event, curNode *insaneJSON.Node, curFmNode *fieldMasksNode) bool {
	var nextFmNode *fieldMasksNode

	result := false

	shouldCheckFmNode := curFmNode != nil && len(curFmNode.children) > 0

	if !shouldCheckFmNode {
		nextFmNode = curFmNode
	}

	switch {
	case curNode == nil:
		return false
	case curNode.IsField():
		if shouldCheckFmNode {
			if _fmNode, has := curFmNode.children[curNode.AsString()]; has {
				// fast path for global ignore fields list when there are no mask-specific fields
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
	case curNode.IsString() || curNode.IsNumber():
		// there is no point in checking other values except for the strings or numbers
		// since there won't be any sensitive data
		return p.processMask(event, curNode, curFmNode)
	default:
		return false
	}

	return result
}

// processMask runs through all masks, checks whether a mask should be applied, applies masks and updates
// node value if it was changed. All global and mask-specific process/ignore fields lists are considered.
// Returns flag whether any of the masks was applied (node value might have not been changed).
func (p *Plugin) processMask(event *pipeline.Event, curNode *insaneJSON.Node, fmNode *fieldMasksNode) bool {
	locApplied := false
	maskApplied := false
	shouldUpdateValue := false
	value := curNode.AsBytes()
	if len(value) == 0 { // no need to check empty value
		return false
	}
	p.sourceBuf = p.sourceBuf[:0]
	for i := range p.config.Masks {
		if p.hasProcessOrIgnoreFields { // check process/ignore fields lists
			switch {
			case p.hasMasksIgnoreFields[i]: // mask-specific ignore fields list
				if fmNode != nil {
					if _, has := fmNode.ignoreMasks[i]; has {
						continue
					}
				}
			case p.hasMasksProcessFields[i]: // mask-specific process fields list
				if fmNode == nil {
					continue
				}
				if _, has := fmNode.processMasks[i]; !has {
					continue
				}
			case p.hasGlobalIgnoreFields: // global ignroe fields list
				if fmNode != nil && fmNode.globalIgnore {
					continue
				}
			case p.hasGlobalProcessFields: // global process fields list
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
			// check value for mask application and apply mask if it matches
			// maskBuf is used for allocation optimization, we cannot use only sourceBuf
			// because it might either be less effective or corrupt data
			p.maskBuf, locApplied = mask.maskValue(p.sourceBuf, p.maskBuf)
			if !locApplied {
				continue
			}
			// update sourceBuf with maskBuf data to apply next mask on the processed data
			// e.g. first mask masked some substring and the next mask is applied
			// on the value with the masked substring
			p.sourceBuf = append(p.sourceBuf[:0], p.maskBuf...)
			shouldUpdateValue = true
		}
		if mask.AppliedField != "" {
			event.Root.AddFieldNoAlloc(event.Root, mask.AppliedField).MutateToString(mask.AppliedValue)
		}
		if mask.MetricName != "" {
			p.maskApplyCount[i]++
		}
		maskApplied = true
	}
	if shouldUpdateValue { // update node value only if the data was masked
		curNode.MutateToString(string(p.sourceBuf))
	}

	return maskApplied
}
