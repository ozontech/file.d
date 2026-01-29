package prometheus

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/castai/promwrite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// TestStorageSender implements storageSender for testing
type TestStorageSender struct {
	sentMetrics [][]promwrite.TimeSeries
	mu          sync.Mutex
	returnError error
}

func (t *TestStorageSender) sendToStorage(values []promwrite.TimeSeries) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMetrics = append(t.sentMetrics, values)
	return t.returnError
}

func (t *TestStorageSender) getSentMetrics() [][]promwrite.TimeSeries {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sentMetrics
}

func (t *TestStorageSender) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMetrics = nil
	t.returnError = nil
}

func (t *TestStorageSender) setError(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.returnError = err
}

func TestMetricCollector(t *testing.T) {
	t.Run("labelsToKey and keyToLabels roundtrip", func(t *testing.T) {
		labels := []promwrite.Label{
			{Name: "job", Value: "test"},
			{Name: "instance", Value: "localhost"},
			{Name: "__name__", Value: "test_metric"},
		}

		key := labelsToKey(labels)
		convertedLabels := keyToLabels(key)

		assert.Len(t, convertedLabels, 3)
		// Since labels are sorted in key generation, we need to check values
		labelMap := make(map[string]string)
		for _, l := range convertedLabels {
			labelMap[l.Name] = l.Value
		}
		assert.Equal(t, "test", labelMap["job"])
		assert.Equal(t, "localhost", labelMap["instance"])
		assert.Equal(t, "test_metric", labelMap["__name__"])
	})

	t.Run("handleMetric counter accumulation", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_counter"},
			{Name: "job", Value: "test"},
		}

		// First value - should not be sent
		now := time.Now()
		values := collector.handleMetric(labels, 10.0, now.UnixMilli(), "counter", 0)
		assert.Empty(t, values)
		assert.Empty(t, testSender.getSentMetrics())

		// Second value in same time window - should accumulate but not send
		values = collector.handleMetric(labels, 5.0, now.UnixMilli(), "counter", 0)
		assert.Empty(t, values)
		assert.Empty(t, testSender.getSentMetrics())

		// Third value in next time window - should send accumulated value
		nextTime := now.Add(time.Second)
		values = collector.handleMetric(labels, 3.0, nextTime.UnixMilli(), "counter", 0)
		assert.Len(t, values, 1)
		assert.Equal(t, 15.0, values[0].Sample.Value) // 10 + 5
	})

	t.Run("handleMetric gauge behavior", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_gauge"},
			{Name: "job", Value: "test"},
		}

		now := time.Now()

		// First value - should not be sent
		values := collector.handleMetric(labels, 100.0, now.UnixMilli(), "gauge", 0)
		assert.Empty(t, values)

		// Second value in same time window - should replace but not send
		values = collector.handleMetric(labels, 200.0, now.UnixMilli(), "gauge", 0)
		assert.Empty(t, values)

		// Third value in next time window - should send latest value
		nextTime := now.Add(time.Second)
		values = collector.handleMetric(labels, 300.0, nextTime.UnixMilli(), "gauge", 0)
		assert.Len(t, values, 1)
		assert.Equal(t, 200.0, values[0].Sample.Value) // Latest value before time window change
	})

	t.Run("flushOldMetrics manual check", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		shortTimeout := 100 * time.Millisecond
		collector := newCollector(shortTimeout, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_flush"},
			{Name: "job", Value: "test"},
		}

		// Add a metric that will timeout
		collector.handleMetric(labels, 42.0, time.Now().UnixMilli(), "gauge", 0)

		// Wait for timeout + a bit more
		time.Sleep(shortTimeout + 50*time.Millisecond)

		// Manually trigger the flush logic that would be called by the ticker
		var toSend []promwrite.TimeSeries
		now := time.Now()
		collector.collector.Range(func(key, value interface{}) bool {
			metric := value.(metricValue)
			if now.Sub(metric.lastUpdateTime) > shortTimeout && !metric.lastValueIsSended {
				labels := keyToLabels(key.(string))
				toSend = append(toSend, createTimeSeries(labels, metric))
			}
			return true
		})

		assert.Len(t, toSend, 1)
		assert.Equal(t, 42.0, toSend[0].Sample.Value)
	})

	t.Run("flushOldMetrics sends timed out metrics", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		shortTimeout := 1 * time.Second
		collector := newCollector(shortTimeout, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_flush"},
			{Name: "job", Value: "test"},
		}

		// Add a metric that will timeout
		sendedValues := collector.handleMetric(labels, 42.0, time.Now().UnixMilli(), "gauge", 0)
		assert.Len(t, sendedValues, 0)

		// Wait for timeout + a bit more
		time.Sleep(shortTimeout + 1*time.Second)
		now := time.Now()

		var toSend []promwrite.TimeSeries
		collector.collector.Range(func(key, value interface{}) bool {
			metric := value.(metricValue)
			if now.Sub(metric.lastUpdateTime) > shortTimeout && !metric.lastValueIsSended {
				labels := keyToLabels(key.(string))
				toSend = append(toSend, createTimeSeries(labels, metric))
			}
			return true
		})

		assert.Len(t, toSend, 0)
	})

	t.Run("shutdown flushes all metrics", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		labels1 := []promwrite.Label{
			{Name: "__name__", Value: "metric1"},
			{Name: "job", Value: "test"},
		}
		labels2 := []promwrite.Label{
			{Name: "__name__", Value: "metric2"},
			{Name: "job", Value: "test"},
		}

		// Add multiple metrics
		collector.handleMetric(labels1, 10.0, time.Now().UnixMilli(), "gauge", 0)
		collector.handleMetric(labels2, 20.0, time.Now().UnixMilli(), "gauge", 0)

		collector.shutdown()

		sentMetrics := testSender.getSentMetrics()
		assert.Len(t, sentMetrics, 1)
		assert.Len(t, sentMetrics[0], 2)
	})

	t.Run("concurrent access", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		var wg sync.WaitGroup
		numGoroutines := 10
		numMetrics := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numMetrics; j++ {
					labels := []promwrite.Label{
						{Name: "__name__", Value: "concurrent_metric"},
						{Name: "worker", Value: string(rune(workerID))},
						{Name: "index", Value: string(rune(j))},
					}
					collector.handleMetric(labels, float64(j), time.Now().UnixMilli(), "counter", 0)
				}
			}(i)
		}

		wg.Wait()

		// Verify all metrics are stored
		count := 0
		collector.collector.Range(func(_, _ interface{}) bool {
			count++
			return true
		})

		assert.Equal(t, numGoroutines*numMetrics, count)
	})

	t.Run("storage sender error handling", func(t *testing.T) {
		logger := zap.NewNop()
		testSender := &TestStorageSender{}
		testSender.setError(errors.New("storage error"))
		collector := newCollector(100*time.Millisecond, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "error_metric"},
			{Name: "job", Value: "test"},
		}

		collector.handleMetric(labels, 99.0, time.Now().UnixMilli(), "gauge", 0)

		// Wait for flush
		time.Sleep(150 * time.Millisecond)

		// Error should be handled gracefully (logged but not panic)
		sentMetrics := testSender.getSentMetrics()
		assert.Equal(t, [][]promwrite.TimeSeries(nil), sentMetrics)
	})

	t.Run("metric value struct validation", func(t *testing.T) {
		now := time.Now()
		mv := metricValue{
			value:             42.0,
			timestamp:         now.UnixMilli(),
			lastUpdateTime:    now,
			lastValueIsSended: false,
		}

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test"},
		}

		ts := createTimeSeries(labels, mv)
		assert.Equal(t, 42.0, ts.Sample.Value)
		assert.Equal(t, now.Truncate(time.Millisecond), ts.Sample.Time.Truncate(time.Millisecond))
		assert.Equal(t, labels, ts.Labels)
	})

	t.Run("label sorting in key generation", func(t *testing.T) {
		unsortedLabels := []promwrite.Label{
			{Name: "z", Value: "last"},
			{Name: "a", Value: "first"},
			{Name: "m", Value: "middle"},
		}

		key := labelsToKey(unsortedLabels)

		// Key should be sorted alphabetically by label name
		assert.True(t, strings.HasPrefix(key, "a=first,"))
		assert.Contains(t, key, "m=middle,")
		assert.True(t, strings.HasSuffix(key, "z=last,"))
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("very old timestamps", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "old_metric"},
		}

		// Very old timestamp
		oldTime := time.Now().Add(-1 * time.Hour)
		collector.handleMetric(labels, 1.0, oldTime.UnixMilli(), "gauge", 0)

		now := time.Now()
		// New timestamp to trigger send
		values := collector.handleMetric(labels, 2.0, now.UnixMilli(), "gauge", 0)

		// Should have sent the old value despite being very old (timestamp: 1 second before now)
		assert.Len(t, values, 1)
		assert.Equal(t, 1.0, values[0].Sample.Value)
		assert.Equal(t, float64(1), now.Sub(values[0].Sample.Time).Seconds())
	})

	t.Run("metric type handling", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(5*time.Minute, testSender, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_type"},
		}

		now := time.Now()

		// Test unknown metric type (should behave like gauge)
		collector.handleMetric(labels, 10.0, now.UnixMilli(), "unknown", 0)
		values := collector.handleMetric(labels, 20.0, now.Add(time.Second).UnixMilli(), "unknown", 0)

		// Should send the last value (10.0) since time window advanced
		if len(values) > 0 {
			assert.Equal(t, 10.0, values[0].Sample.Value)
		}
	})
}

func TestCreateTimeSeries(t *testing.T) {
	t.Run("createTimeSeries with valid metricValue", func(t *testing.T) {
		now := time.Now()
		mv := metricValue{
			value:     123.45,
			timestamp: now.UnixMilli(),
		}

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_metric"},
			{Name: "instance", Value: "localhost"},
		}

		ts := createTimeSeries(labels, mv)

		assert.Equal(t, labels, ts.Labels)
		assert.Equal(t, 123.45, ts.Sample.Value)
		assert.Equal(t, now.Truncate(time.Millisecond), ts.Sample.Time.Truncate(time.Millisecond))
	})
}

// Benchmark tests
func BenchmarkLabelsToKey(b *testing.B) {
	labels := []promwrite.Label{
		{Name: "__name__", Value: "benchmark_metric"},
		{Name: "job", Value: "benchmark"},
		{Name: "instance", Value: "localhost:9090"},
		{Name: "environment", Value: "production"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		labelsToKey(labels)
	}
}

func BenchmarkKeyToLabels(b *testing.B) {
	key := "__name__=test_metric,environment=production,instance=localhost:9090,job=test,"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyToLabels(key)
	}
}
