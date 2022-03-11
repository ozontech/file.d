package fd

import (
	"fmt"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	capacity := pipeline.DefaultCapacity
	antispamThreshold := 0
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

		val = settings.Get("max_log_size").MustInt()
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

		isStrict = settings.Get("is_strict").MustBool()
	}

	return &pipeline.Settings{
		Decoder:             decoder,
		Capacity:            capacity,
		AvgEventSize:        avgInputEventSize,
		MaxEventSize:        maxInputEventSize,
		AntispamThreshold:   antispamThreshold,
		MaintenanceInterval: maintenanceInterval,
		EventTimeout:        eventTimeout,
		StreamField:         streamField,
		IsStrict:            isStrict,
	}
}

func extractMatchMode(actionJSON *simplejson.Json) (pipeline.MatchMode, error) {
	mm := actionJSON.Get("match_mode").MustString()
	if mm != "or" && mm != "and" && mm != "" {
		return pipeline.MatchModeUnknown, fmt.Errorf("unknown match mode %q must be or/and", mm)
	}
	matchMode := pipeline.MatchModeAnd
	if mm == "or" {
		matchMode = pipeline.MatchModeOr
	}
	return matchMode, nil
}

func extractMatchInvert(actionJSON *simplejson.Json) (bool, error) {
	invertMatchMode := actionJSON.Get("match_invert").MustBool()
	return invertMatchMode, nil
}

func extractConditions(condJSON *simplejson.Json) (pipeline.MatchConditions, error) {
	conditions := make(pipeline.MatchConditions, 0)
	for field := range condJSON.MustMap() {
		value := condJSON.Get(field).MustString()

		condition := pipeline.MatchCondition{
			Field: cfg.ParseFieldSelector(field),
		}

		if len(value) > 0 && value[0] == '/' {
			r, err := cfg.CompileRegex(value)
			if err != nil {
				return nil, fmt.Errorf("can't compile regexp %s: %w", value, err)
			}
			condition.Regexp = r
		} else {
			condition.Value = value
		}
		conditions = append(conditions, condition)
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
