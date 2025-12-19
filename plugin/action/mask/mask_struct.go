package mask

import (
	"regexp"
	"unicode/utf8"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline/doif"
	"go.uber.org/zap"
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

	DoIfCheckerMap map[string]any `json:"do_if"`
	DoIfChecker    *doif.Checker

	use bool

	// > @3@4@5@6
	// >
	// > List of the mask-specific ignored event fields.
	// > If name of some field contained in this list
	// > then all nested fields will be ignored (even if they are not listed).
	// > Overrides plugin process/ignore fields lists for the mask.
	IgnoreFields []string `json:"ignore_fields" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > List of the mask-specific processed event fields.
	// > If name of some field contained in this list
	// > then all nested fields will be processed (even if they are not listed).
	// > If ignored fields list is empty and processed fields list is empty
	// > we consider this as empty ignored fields list (all fields will be processed).
	// > It is wrong to set non-empty ignored fields list and non-empty processed fields list at the same time.
	// > Overrides plugin process/ignore fields lists for the mask.
	ProcessFields []string `json:"process_fields" slice:"true"` // *

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
	appliedMetric metric.HeldCounterVec
}

func compileMasks(masks []Mask, logger *zap.Logger) []Mask {
	for i := range masks {
		mask := &masks[i]
		if mask.MaxCount > 0 && mask.ReplaceWord != "" {
			logger.Fatal("invalid mask configuration")
		}
	}

	for i := range masks {
		compileMask(&masks[i], logger)
	}

	return masks
}

func compileMask(m *Mask, logger *zap.Logger) {
	if m.Re == "" && len(m.MatchRules) == 0 {
		logger.Fatal("mask must have either nonempty regex or ruleset, or both")
	}

	if m.DoIfCheckerMap != nil {
		var err error
		m.DoIfChecker, err = doif.NewFromMap(m.DoIfCheckerMap)
		if err != nil {
			logger.Fatal("can't init do_if for mask", zap.Error(err))
		}
	} else {
		m.use = true
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

func (m *Mask) checkMatchRules(value []byte) bool {
	if len(m.MatchRules) == 0 {
		return true
	}
	for i := range m.MatchRules {
		if m.MatchRules[i].Match(value) {
			return true
		}
	}
	return false
}

func (m *Mask) maskSection(dst, src []byte, begin, end int) []byte {
	switch m.mode {
	case modeReplace:
		return append(dst, m.ReplaceWord...)
	case modeCut:
		return dst
	case modeMask:
		n := utf8.RuneCount(src[begin:end])
		if m.MaxCount > 0 {
			n = min(n, m.MaxCount)
		}

		for i := 0; i < n; i++ {
			dst = append(dst, substitution)
		}

		return dst
	default:
		panic("invalid masking mode")
	}
}

// mask value returns masked value and bool answer was buf masked at all.
func (m *Mask) maskValue(value, buf []byte) ([]byte, bool) {
	indexes := m.Re_.FindAllSubmatchIndex(value, -1)
	if len(indexes) == 0 {
		return buf, false
	}

	buf = buf[:0]

	prevFinish := 0
	curStart, curFinish := 0, 0
	for _, index := range indexes {
		for _, grp := range m.Groups {
			curStart = index[grp*2]
			curFinish = index[grp*2+1]
			if curStart < 0 || curFinish < 0 { // invalid idx check
				continue
			}

			buf = append(buf, value[prevFinish:curStart]...)
			prevFinish = curFinish

			buf = m.maskSection(
				buf,
				value,
				curStart,
				curFinish,
			)
		}
	}

	return append(buf, value[curFinish:]...), true
}
