package fd

import (
	"fmt"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/antispam"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	capacity := pipeline.DefaultCapacity
	antispamThreshold := 0
	var antispamExceptions []antispam.Exception
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

		var err error
		antispamExceptions, err = extractExceptions(settings)
		if err != nil {
			logger.Fatalf("extract exceptions: %s", err)
		}

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

func extractExceptions(settings *simplejson.Json) ([]antispam.Exception, error) {
	var excepts []antispam.Exception

	exceptionsRaw := settings.Get("antispam_exceptions")
	arr, err := exceptionsRaw.Array()
	if err != nil {
		return nil, fmt.Errorf("cast to array: %s", err)
	}

	for i := range arr {
		exception := exceptionsRaw.GetIndex(i)

		var cond antispam.Condition
		condRaw := strings.ToLower(exception.Get("condition").MustString())
		switch condRaw {
		case "prefix":
			cond = antispam.ConditionPrefix
		case "contains":
			cond = antispam.ConditionContains
		case "suffix":
			cond = antispam.ConditionSuffix
		default:
			return nil, fmt.Errorf("condition %s does not exist", condRaw)
		}

		caseInsensitive := exception.Get("case_insensitive").MustBool()

		if cond == antispam.ConditionContains && caseInsensitive {
			return nil, fmt.Errorf("сase insensitive for the 'contains' condtition is not supported")
		}

		val := exception.Get("value").MustString()

		excepts = append(excepts, antispam.NewException(val, cond, caseInsensitive))
	}

	return excepts, nil
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

func extractMetrics(actionJSON *simplejson.Json) (string, []string) {
	metricName := actionJSON.Get("metric_name").MustString()
	metricLabels := actionJSON.Get("metric_labels").MustStringArray()
	if metricLabels == nil {
		metricLabels = []string{}
	}
	return metricName, metricLabels
}

func makeActionJSON(actionJSON *simplejson.Json) []byte {
	actionJSON.Del("type")
	actionJSON.Del("match_fields")
	actionJSON.Del("match_mode")
	actionJSON.Del("metric_name")
	actionJSON.Del("metric_labels")
	actionJSON.Del("match_invert")
	configJson, err := actionJSON.Encode()
	if err != nil {
		logger.Panicf("can't create action json")
	}
	return configJson
}
