package fd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/antispam"
	"github.com/ozontech/file.d/pipeline/doif"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	capacity := pipeline.DefaultCapacity
	antispamThreshold := pipeline.DefaultAntispamThreshold
	var antispamExceptions antispam.Exceptions
	sourceNameMetaField := pipeline.DefaultSourceNameMetaField
	avgInputEventSize := pipeline.DefaultAvgInputEventSize
	maxInputEventSize := pipeline.DefaultMaxInputEventSize
	cutOffEventByLimit := pipeline.DefaultCutOffEventByLimit
	cutOffEventByLimitField := pipeline.DefaultCutOffEventByLimitField
	streamField := pipeline.DefaultStreamField
	maintenanceInterval := pipeline.DefaultMaintenanceInterval
	decoder := pipeline.DefaultDecoder
	decoderParams := make(map[string]any)
	isStrict := pipeline.DefaultIsStrict
	eventTimeout := pipeline.DefaultEventTimeout
	metricHoldDuration := pipeline.DefaultMetricHoldDuration
	metaCacheSize := pipeline.DefaultMetaCacheSize
	pool := ""

	if settings != nil {
		val := settings.Get("capacity").MustInt()
		if val != 0 {
			capacity = val
		}

		val = settings.Get("meta_cache_size").MustInt()
		if val != 0 {
			metaCacheSize = val
		}

		val = settings.Get("avg_log_size").MustInt()
		if val != 0 {
			avgInputEventSize = val
		}

		val = settings.Get("max_event_size").MustInt()
		if val != 0 {
			maxInputEventSize = val
		}

		cutOffEventByLimit = settings.Get("cut_off_event_by_limit").MustBool()
		cutOffEventByLimitField = settings.Get("cut_off_event_by_limit_field").MustString()

		str := settings.Get("decoder").MustString()
		if str != "" {
			decoder = str
		}

		decoderParams = settings.Get("decoder_params").MustMap()

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
		antispamExceptions, err = extractAntispamExceptions(settings)
		if err != nil {
			logger.Fatalf("extract exceptions: %s", err)
		}
		antispamExceptions.Prepare()

		sourceNameMetaField = settings.Get("source_name_meta_field").MustString()
		isStrict = settings.Get("is_strict").MustBool()

		str = settings.Get("metric_hold_duration").MustString()
		if str != "" {
			i, err := time.ParseDuration(str)
			if err != nil {
				logger.Fatalf("can't parse pipeline metric hold duration: %s", err.Error())
			}
			metricHoldDuration = i
		}

		if str := settings.Get("pool").MustString(); str != "" {
			pool = str
		}
	}

	return &pipeline.Settings{
		Decoder:                 decoder,
		DecoderParams:           decoderParams,
		Capacity:                capacity,
		MetaCacheSize:           metaCacheSize,
		AvgEventSize:            avgInputEventSize,
		MaxEventSize:            maxInputEventSize,
		CutOffEventByLimit:      cutOffEventByLimit,
		CutOffEventByLimitField: cutOffEventByLimitField,
		AntispamThreshold:       antispamThreshold,
		AntispamExceptions:      antispamExceptions,
		SourceNameMetaField:     sourceNameMetaField,
		MaintenanceInterval:     maintenanceInterval,
		EventTimeout:            eventTimeout,
		StreamField:             streamField,
		IsStrict:                isStrict,
		MetricHoldDuration:      metricHoldDuration,
		Pool:                    pipeline.PoolType(pool),
	}
}

func extractAntispamExceptions(settings *simplejson.Json) (antispam.Exceptions, error) {
	raw, err := settings.Get("antispam_exceptions").MarshalJSON()
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var exceptions antispam.Exceptions
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
			if value != "" && value[0] == '/' {
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

var (
	doIfLogicalOpNodes = map[string]struct{}{
		"and": {},
		"not": {},
		"or":  {},
	}
	doIfFieldOpNodes = map[string]struct{}{
		"equal":    {},
		"contains": {},
		"prefix":   {},
		"suffix":   {},
		"regex":    {},
	}
	doIfLengthCmpOpNodes = map[string]struct{}{
		"byte_len_cmp":  {},
		"array_len_cmp": {},
	}
	doIfTimestampCmpOpNodes = map[string]struct{}{
		"ts_cmp": {},
	}
	doIfСheckTypeOpNode = "check_type"
)

func extractFieldOpVals(jsonNode *simplejson.Json) [][]byte {
	values, has := jsonNode.CheckGet("values")
	if !has {
		return nil
	}
	vals := make([][]byte, 0)
	iFaceVal := values.Interface()
	if iFaceVal == nil {
		vals = append(vals, nil)
		return vals
	}
	if strVal, ok := iFaceVal.(string); ok {
		vals = append(vals, []byte(strVal))
		return vals
	}
	for i := range values.MustArray() {
		curValue := values.GetIndex(i).Interface()
		if curValue == nil {
			vals = append(vals, nil)
		} else {
			vals = append(vals, []byte(curValue.(string)))
		}
	}
	return vals
}

func extractFieldOpNode(opName string, jsonNode *simplejson.Json) (doif.Node, error) {
	var result doif.Node
	var err error
	fieldPath := jsonNode.Get("field").MustString()
	caseSensitiveNode, has := jsonNode.CheckGet("case_sensitive")
	caseSensitive := true
	if has {
		caseSensitive = caseSensitiveNode.MustBool()
	}
	vals := extractFieldOpVals(jsonNode)
	result, err = doif.NewFieldOpNode(opName, fieldPath, caseSensitive, vals)
	if err != nil {
		return nil, fmt.Errorf("failed to init field op: %w", err)
	}

	return result, nil
}

func noRequiredFieldError(field string) error {
	return fmt.Errorf("no required field: %s", field)
}

func requiredString(jsonNode *simplejson.Json, fieldName string) (string, error) {
	node, has := jsonNode.CheckGet(fieldName)
	if !has {
		return "", noRequiredFieldError(fieldName)
	}

	result, err := node.String()
	if err != nil {
		return "", err
	}

	return result, nil
}

func requiredInt(jsonNode *simplejson.Json, fieldName string) (int, error) {
	node, has := jsonNode.CheckGet(fieldName)
	if !has {
		return 0, noRequiredFieldError(fieldName)
	}

	result, err := node.Int()
	if err != nil {
		return 0, err
	}

	return result, nil
}

const (
	fieldNameField    = "field"
	fieldNameCmpOp    = "cmp_op"
	fieldNameCmpValue = "value"
)

func extractLengthCmpOpNode(opName string, jsonNode *simplejson.Json) (doif.Node, error) {
	fieldPath, err := requiredString(jsonNode, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := requiredString(jsonNode, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	cmpValue, err := requiredInt(jsonNode, fieldNameCmpValue)
	if err != nil {
		return nil, err
	}

	return doif.NewLenCmpOpNode(opName, fieldPath, cmpOp, cmpValue)
}

const (
	fieldNameFormat         = "format"
	fieldNameUpdateInterval = "update_interval"
	fieldNameCmpValueShift  = "value_shift"
)

const (
	tsCmpModeNowTag   = "now"
	tsCmpModeConstTag = "const"

	tsCmpValueNowTag   = "now"
	tsCmpValueStartTag = "file_d_start"
)

const (
	defaultTsCmpValUpdateInterval = 10 * time.Second
	defaultTsFormat               = time.RFC3339Nano
)

func extractTsCmpOpNode(_ string, jsonNode *simplejson.Json) (doif.Node, error) {
	fieldPath, err := requiredString(jsonNode, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := requiredString(jsonNode, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	rawCmpValue, err := requiredString(jsonNode, fieldNameCmpValue)
	if err != nil {
		return nil, err
	}

	var cmpMode string
	var cmpValue time.Time

	switch rawCmpValue {
	case tsCmpValueNowTag:
		cmpMode = tsCmpModeNowTag
	case tsCmpValueStartTag:
		cmpMode = tsCmpModeConstTag
		cmpValue = time.Now()
	default:
		cmpMode = tsCmpModeConstTag
		cmpValue, err = time.Parse(time.RFC3339Nano, rawCmpValue)
		if err != nil {
			return nil, fmt.Errorf("parse ts cmp value: %w", err)
		}
	}

	format := defaultTsFormat
	str := jsonNode.Get(fieldNameFormat).MustString()
	if str != "" {
		format = str
	}

	cmpValueShift := time.Duration(0)
	str = jsonNode.Get(fieldNameCmpValueShift).MustString()
	if str != "" {
		cmpValueShift, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse cmp value shift: %w", err)
		}
	}

	updateInterval := defaultTsCmpValUpdateInterval
	str = jsonNode.Get(fieldNameUpdateInterval).MustString()
	if str != "" {
		updateInterval, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse update interval: %w", err)
		}
	}

	return doif.NewTsCmpOpNode(fieldPath, format, cmpOp, cmpMode, cmpValue, cmpValueShift, updateInterval)
}

func extractCheckTypeOpNode(_ string, jsonNode *simplejson.Json) (doif.Node, error) {
	fieldPath, err := requiredString(jsonNode, "field")
	if err != nil {
		return nil, err
	}
	vals := extractFieldOpVals(jsonNode)
	result, err := doif.NewCheckTypeOpNode(fieldPath, vals)
	if err != nil {
		return nil, fmt.Errorf("failed to init check_type op: %w", err)
	}
	return result, nil
}

func extractLogicalOpNode(opName string, jsonNode *simplejson.Json) (doif.Node, error) {
	var result, operand doif.Node
	var err error
	operands := jsonNode.Get("operands")
	operandsList := make([]doif.Node, 0)
	for i := range operands.MustArray() {
		opNode := operands.GetIndex(i)
		operand, err = extractDoIfNode(opNode)
		if err != nil {
			return nil, fmt.Errorf("failed to extract operand node for logical op %q", opName)
		}
		operandsList = append(operandsList, operand)
	}
	result, err = doif.NewLogicalNode(opName, operandsList)
	if err != nil {
		return nil, fmt.Errorf("failed to init logical node: %w", err)
	}
	return result, nil
}

func extractDoIfNode(jsonNode *simplejson.Json) (doif.Node, error) {
	opNameNode, has := jsonNode.CheckGet("op")
	if !has {
		return nil, errors.New(`"op" field not found`)
	}
	opName := opNameNode.MustString()
	if _, has := doIfLogicalOpNodes[opName]; has {
		return extractLogicalOpNode(opName, jsonNode)
	} else if _, has := doIfFieldOpNodes[opName]; has {
		return extractFieldOpNode(opName, jsonNode)
	} else if _, has := doIfLengthCmpOpNodes[opName]; has {
		return extractLengthCmpOpNode(opName, jsonNode)
	} else if _, has := doIfTimestampCmpOpNodes[opName]; has {
		return extractTsCmpOpNode(opName, jsonNode)
	} else if opName == doIfСheckTypeOpNode {
		return extractCheckTypeOpNode(opName, jsonNode)
	} else {
		return nil, fmt.Errorf("unknown op %q", opName)
	}
}

func extractDoIfChecker(actionJSON *simplejson.Json) (*doif.Checker, error) {
	if actionJSON.MustMap() == nil {
		return nil, nil
	}

	root, err := extractDoIfNode(actionJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to extract nodes: %w", err)
	}
	result := doif.NewChecker(root)
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
