package fd

import (
	"bytes"
	"encoding/json"
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
	metricMaxLabelValueLength := pipeline.DefaultMetricMaxLabelValueLength

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

		if str := settings.Get("pool").MustString(); str != "" {
			pool = str
		}

		if metrics := settings.Get("metrics"); metrics != nil {
			str = metrics.Get("metric_hold_duration").MustString()
			if str != "" {
				i, err := time.ParseDuration(str)
				if err != nil {
					logger.Fatalf("can't parse pipeline metric hold duration: %s", err.Error())
				}
				metricHoldDuration = i
			}

			val = metrics.Get("metric_max_label_value_length").MustInt()
			if val != 0 {
				metricMaxLabelValueLength = val
			}
		}

		str = settings.Get("metric_hold_duration").MustString()
		if str != "" && metricHoldDuration == pipeline.DefaultMetricHoldDuration {
			i, err := time.ParseDuration(str)
			if err != nil {
				logger.Fatalf("can't parse pipeline metric hold duration: %s", err.Error())
			}
			metricHoldDuration = i
		}
	}

	return &pipeline.Settings{
		Decoder:                   decoder,
		DecoderParams:             decoderParams,
		Capacity:                  capacity,
		MetaCacheSize:             metaCacheSize,
		AvgEventSize:              avgInputEventSize,
		MaxEventSize:              maxInputEventSize,
		CutOffEventByLimit:        cutOffEventByLimit,
		CutOffEventByLimitField:   cutOffEventByLimitField,
		AntispamThreshold:         antispamThreshold,
		AntispamExceptions:        antispamExceptions,
		SourceNameMetaField:       sourceNameMetaField,
		MaintenanceInterval:       maintenanceInterval,
		EventTimeout:              eventTimeout,
		StreamField:               streamField,
		IsStrict:                  isStrict,
		MetricHoldDuration:        metricHoldDuration,
		Pool:                      pipeline.PoolType(pool),
		MetricMaxLabelValueLength: metricMaxLabelValueLength,
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

func extractDoIfChecker(actionJSON *simplejson.Json) (*doif.Checker, error) {
	m := actionJSON.MustMap()
	if m == nil {
		return nil, nil
	}

	return doif.NewFromMap(m)
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
