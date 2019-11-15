package filed

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"github.com/bitly/go-simplejson"
	"github.com/pkg/errors"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

func extractPipelineParams(settings *simplejson.Json) *pipeline.Settings {
	procs := runtime.GOMAXPROCS(0)
	processorsCount := procs * 8
	capacity := pipeline.DefaultCapacity
	avgLogSize := pipeline.DefaultAvgLogSize
	streamField := "stream"
	if settings != nil {
		val := settings.Get("processors_count").MustInt()
		if val != 0 {
			processorsCount = val
		}

		val = settings.Get("capacity").MustInt()
		if val != 0 {
			capacity = val
		}

		val = settings.Get("avg_log_size").MustInt()
		if val != 0 {
			avgLogSize = val
		}

		str := settings.Get("stream_field").MustString()
		if str != "" {
			streamField = str
		}
	}

	return &pipeline.Settings{
		Capacity:        capacity,
		AvgLogSize:      avgLogSize,
		ProcessorsCount: processorsCount,
		StreamField:     streamField,
	}
}

func extractMatchMode(actionJSON *simplejson.Json) (pipeline.MatchMode, error) {
	mm := actionJSON.Get("match_mode").MustString()
	if mm != "or" && mm != "and" && mm != "" {
		return pipeline.ModeUnknown, fmt.Errorf("unknown match mode %q must be or/and", mm)

	}
	matchMode := pipeline.ModeAnd
	if mm == "or" {
		matchMode = pipeline.ModeOr
	}
	return matchMode, nil
}

func extractConditions(condJSON *simplejson.Json) (pipeline.MatchConditions, error) {
	conditions := make(pipeline.MatchConditions, 0, 0)
	for field := range condJSON.MustMap() {
		value := strings.Trim(condJSON.Get(field).MustString(), " ")
		if value == "" {
			return nil, fmt.Errorf("no value for field matching condition %q", field)
		}

		condition := pipeline.MatchCondition{
			Field: strings.Trim(field, " "),
		}
		if value[0] == '/' {
			r, err := CompileRegex(value)
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

func CompileRegex(s string) (*regexp.Regexp, error) {
	if len(s) == 0 || s[0] != '/' || s[len(s)-1] != '/' {
		return nil, fmt.Errorf(`regexp "%s" should be surounded by "/"`, s)
	}

	return regexp.Compile(s[1 : len(s)-1])
}
