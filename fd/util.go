package fd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	capacity := pipeline.DefaultCapacity
	antispamThreshold := 0
	var antispamExceptions matchrule.RuleSets
	avgInputEventSize := pipeline.DefaultAvgInputEventSize
	maxInputEventSize := pipeline.DefaultMaxInputEventSize
	streamField := pipeline.DefaultStreamField
	maintenanceInterval := pipeline.DefaultMaintenanceInterval
	decoder := "auto"
	isStrict := false
	eventTimeout := pipeline.DefaultEventTimeout

	if settings != nil {
		val := settings.Get("capacity").MustInt()
		if val != 0 {
			capacity = val
		}

		val = settings.Get("avg_log_size").MustInt()
		if val != 0 {
			avgInputEventSize = val
		}

		val = settings.Get("max_event_size").MustInt()
		if val != 0 {
			maxInputEventSize = val
		}

		str := settings.Get("decoder").MustString()
		if str != "" {
			decoder = str
		}

		str = settings.Get("stream_field").MustString()
		if str != "" {
			streamField = str
		}

		str = settings.Get("maintenance_interval").MustString()
		if str != "" {
			i, err := time.ParseDuration(str)
			if err != nil {
				logger.Fatalf("can't parse pipeline maintenance interval: %s", err.Error())
			}
			maintenanceInterval = i
		}

		str = settings.Get("event_timeout").MustString()
		if str != "" {
			i, err := time.ParseDuration(str)
			if err != nil {
				logger.Fatalf("can't parse pipeline event timeout: %s", err.Error())
			}
			eventTimeout = i
		}

		antispamThreshold = settings.Get("antispam_threshold").MustInt()
		antispamThreshold *= int(maintenanceInterval / time.Second)
		if antispamThreshold < 0 {
			logger.Warn("negative antispam_threshold value, antispam disabled")
			antispamThreshold = 0
		}

		var err error
		antispamExceptions, err = extractExceptions(settings)
		if err != nil {
			logger.Fatalf("extract exceptions: %s", err)
		}
		antispamExceptions.Prepare()

		isStrict = settings.Get("is_strict").MustBool()
	}

	return &pipeline.Settings{
		Decoder:             decoder,
		Capacity:            capacity,
		AvgEventSize:        avgInputEventSize,
		MaxEventSize:        maxInputEventSize,
		AntispamThreshold:   antispamThreshold,
		AntispamExceptions:  antispamExceptions,
		MaintenanceInterval: maintenanceInterval,
		EventTimeout:        eventTimeout,
		StreamField:         streamField,
		IsStrict:            isStrict,
	}
}

func extractExceptions(settings *simplejson.Json) (matchrule.RuleSets, error) {
	raw, err := settings.Get("antispam_exceptions").MarshalJSON()
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var exceptions matchrule.RuleSets
	if err := dec.Decode(&exceptions); err != nil {
		return nil, err
	}

	return exceptions, nil
}

func extractMatchMode(actionJSON *simplejson.Json) pipeline.MatchMode {
	mm := actionJSON.Get("match_mode").MustString()
	return pipeline.MatchModeFromString(mm)
}

func extractMatchInvert(actionJSON *simplejson.Json) bool {
	invertMatchMode := actionJSON.Get("match_invert").MustBool()
	return invertMatchMode
}

func extractConditions(condJSON *simplejson.Json) (pipeline.MatchConditions, error) {
	conditions := make(pipeline.MatchConditions, 0)
	for field := range condJSON.MustMap() {
		obj := condJSON.Get(field).Interface()

		condition := pipeline.MatchCondition{
			Field: cfg.ParseFieldSelector(field),
		}

		if value, ok := obj.(string); ok {
			if len(value) > 0 && value[0] == '/' {
				r, err := cfg.CompileRegex(value)
				if err != nil {
					return nil, fmt.Errorf("can't compile regexp %s: %w", value, err)
				}
				condition.Regexp = r
			} else {
				condition.Values = []string{value}
			}

			conditions = append(conditions, condition)
			continue
		}

		if jsonValues, ok := obj.([]any); ok {
			condition.Values = make([]string, 0, len(jsonValues))

			for _, jsonValue := range jsonValues {
				val, ok := jsonValue.(string)
				if !ok {
					return nil, fmt.Errorf("can't parse %v as string", jsonValue)
				}
				condition.Values = append(condition.Values, val)
			}

			conditions = append(conditions, condition)
			continue
		}
	}

	return conditions, nil
}

func extractMetrics(actionJSON *simplejson.Json) (string, []string, bool) {
	metricName := actionJSON.Get("metric_name").MustString()
	metricLabels := actionJSON.Get("metric_labels").MustStringArray()
	if metricLabels == nil {
		metricLabels = []string{}
	}
	skipStatus := actionJSON.Get("metric_skip_status").MustBool()
	return metricName, metricLabels, skipStatus
}

func extractDoIfNode(jsonNode *simplejson.Json) (matchrule.DoIfNode, error) {
	var result, operand matchrule.DoIfNode
	var err error
	logicalOpNode, has := jsonNode.CheckGet("logical_op")
	if has {
		// logical op node
		logicalOp := logicalOpNode.MustString()
		operands := jsonNode.Get("operands")
		operandsList := make([]matchrule.DoIfNode, 0)
		for i := range operands.MustArray() {
			opNode := operands.GetIndex(i)
			operand, err = extractDoIfNode(opNode)
			if err != nil {
				return nil, fmt.Errorf("failed to extract operand node for logical op %q", logicalOp)
			}
			operandsList = append(operandsList, operand)
		}
		result, err = matchrule.NewLogicalNode([]byte(logicalOp), operandsList)
		if err != nil {
			return nil, fmt.Errorf("failed to init logical node: %w", err)
		}
		return result, nil
	}
	// field op node
	fieldOpNode, has := jsonNode.CheckGet("field_op")
	if !has {
		return nil, errors.New("unknown type of node")
	}
	fieldOp := fieldOpNode.MustString()
	fieldPath := jsonNode.Get("field").MustString()
	caseSensitiveNode, has := jsonNode.CheckGet("case_sensitive")
	caseSensitive := true
	if has {
		caseSensitive = caseSensitiveNode.MustBool()
	}
	values := jsonNode.Get("values")
	vals := make([][]byte, 0)
	for i := range values.MustArray() {
		curValue := values.GetIndex(i).Interface()
		if curValue == nil {
			vals = append(vals, nil)
		} else {
			vals = append(vals, []byte(curValue.(string)))
		}
	}
	result, err = matchrule.NewFieldOpNode(fieldOp, fieldPath, caseSensitive, vals)
	if err != nil {
		return nil, fmt.Errorf("failed to init field op: %w", err)
	}

	return result, nil
}

func extractDoIfChecker(actionJSON *simplejson.Json) (*matchrule.DoIfChecker, error) {
	if actionJSON.MustMap() == nil {
		return nil, nil
	}

	root, err := extractDoIfNode(actionJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to extract nodes: %w", err)
	}
	result := matchrule.NewDoIfChecker(root)
	return result, nil
}

func makeActionJSON(actionJSON *simplejson.Json) []byte {
	actionJSON.Del("type")
	actionJSON.Del("match_fields")
	actionJSON.Del("match_mode")
	actionJSON.Del("metric_name")
	actionJSON.Del("metric_labels")
	actionJSON.Del("metric_skip_status")
	actionJSON.Del("match_invert")
	actionJSON.Del("do_if")
	configJson, err := actionJSON.Encode()
	if err != nil {
		logger.Panicf("can't create action json")
	}
	return configJson
}
