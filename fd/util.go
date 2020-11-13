package fd

import (
	"fmt"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/pkg/errors"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	capacity := pipeline.DefaultCapacity
	antispamThreshold := 0
	avgLogSize := pipeline.DefaultAvgLogSize
	streamField := pipeline.DefaultStreamField
	maintenanceInterval := pipeline.DefaultMaintenanceInterval
	decoder := "auto"
	isStrict := false

	if settings != nil {
		val := settings.Get("capacity").MustInt()
		if val != 0 {
			capacity = val
		}

		val = settings.Get("avg_log_size").MustInt()
		if val != 0 {
			avgLogSize = val
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

		antispamThreshold = settings.Get("antispam_threshold").MustInt()
		antispamThreshold *= int(maintenanceInterval / time.Second)

		isStrict = settings.Get("is_strict").MustBool()
	}

	return &pipeline.Settings{
		Decoder:             decoder,
		Capacity:            capacity,
		AvgLogSize:          avgLogSize,
		AntispamThreshold:   antispamThreshold,
		MaintenanceInterval: maintenanceInterval,
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

func extractConditions(condJSON *simplejson.Json) (pipeline.MatchConditions, error) {
	conditions := make(pipeline.MatchConditions, 0, 0)
	for field := range condJSON.MustMap() {
		value := condJSON.Get(field).MustString()

		condition := pipeline.MatchCondition{
			Field: field,
		}

		if len(value) > 0 && value[0] == '/' {
			r, err := cfg.CompileRegex(value)
			if err != nil {
				return nil, errors.Wrapf(err, "can't compile regexp %s: %s", value, err)
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
	configJson, err := actionJSON.Encode()
	if err != nil {
		logger.Panicf("can't create action json")
	}
	return configJson
}
