package event_to_metrics

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestEventToMetrics(t *testing.T) {
	config := &Config{
		TimeField_:      []string{"time"},
		TimeFieldFormat: time.RFC3339Nano,
		Metrics: []Metric{
			{
				Name: "status",
				Type: "counter",
				TTL_: 1 * time.Hour,
				Labels: map[string]cfg.FieldSelector{
					"status": "status",
					"host":   "host",
				},
			},
			{
				Name:  "checkout_response_time",
				Value: []cfg.FieldSelector{"info.response_time"},
				Type:  "gauge",
				Labels: map[string]cfg.FieldSelector{
					"zone": "info.zone",
				},
				DoIfCheckerMap: map[string]any{
					"op":     "equal",
					"field":  "info.zone",
					"values": []any{"checkout"},
				},
			},
			{
				Name:  "auth_response_time",
				Value: []cfg.FieldSelector{"info.response_time"},
				Type:  "gauge",
				Labels: map[string]cfg.FieldSelector{
					"zone": "info.zone",
				},
				DoIfCheckerMap: map[string]any{
					"op":     "equal",
					"field":  "info.zone",
					"values": []any{"auth"},
				},
			},
		},
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	now := time.Now().Add(-10 * time.Minute)

	wrongEventsCnt := 0
	outWg := sync.WaitGroup{}
	outWg.Add(1) // Now we get 1 event with array instead of 2 separate events

	output.SetOutFn(func(e *pipeline.Event) {
		message := e.Root

		// Handle array of metrics
		if message.IsArray() {
			arr := message.AsArray()
			for _, metricNode := range arr {
				if !metricNode.IsObject() {
					continue
				}

				metricName := metricNode.Dig("name").AsString()
				if metricName == "" {
					continue
				}

				metricType := metricNode.Dig("type").AsString()
				timestamp := metricNode.Dig("timestamp").AsInt64()
				ttl := metricNode.Dig("ttl").AsInt64()
				value := metricNode.Dig("value").AsFloat()

				switch metricName {
				case "status":
					assert.Equal(t, "counter", metricType)
					assert.Equal(t, (1 * time.Hour).Milliseconds(), ttl)
					assert.Equal(t, float64(1), value)
				case "checkout_response_time":
					assert.Equal(t, "gauge", metricType)
					assert.Equal(t, int64(0), ttl)
					assert.Equal(t, float64(0.1), value)
				default:
					assert.Fail(t, "unknown metric name", metricName)
				}

				assert.Equal(t, now.UnixMilli(), timestamp)
			}
		} else {
			// Single metric (backward compatibility)
			metricName := message.Dig("name").AsString()
			if metricName == "" {
				return
			}

			metricType := message.Dig("type").AsString()
			timestamp := message.Dig("timestamp").AsInt64()
			ttl := message.Dig("ttl").AsInt64()
			value := message.Dig("value").AsFloat()

			switch metricName {
			case "status":
				assert.Equal(t, "counter", metricType)
				assert.Equal(t, (1 * time.Hour).Milliseconds(), ttl)
				assert.Equal(t, float64(1), value)
			case "checkout_response_time":
				assert.Equal(t, "gauge", metricType)
				assert.Equal(t, int64(0), ttl)
				assert.Equal(t, float64(0.1), value)
			default:
				assert.Fail(t, "unknown metric name", metricName)
			}

			assert.Equal(t, now.UnixMilli(), timestamp)
		}

		defer outWg.Done()
	})

	json := fmt.Sprintf(`{"host":"localhost","status":"200","info":{"zone":"checkout","response_time": 0.1},"time":"%s"}`, now.Format(time.RFC3339Nano))
	input.In(10, "test", test.NewOffset(0), []byte(json))
	outWg.Wait()

	p.Stop()
	assert.Equal(t, 0, wrongEventsCnt)
}
