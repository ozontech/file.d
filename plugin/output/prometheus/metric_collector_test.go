package prometheus

import (
	"sync"
	"testing"
	"time"

	"github.com/castai/promwrite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// TestStorageSender implements storageSender for testing
type TestStorageSender struct {
	sentMetrics []promwrite.TimeSeries
	mu          sync.Mutex
	returnError error
}

func (t *TestStorageSender) sendToStorage(values []promwrite.TimeSeries) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMetrics = append(t.sentMetrics, values...)
	return t.returnError
}

func (t *TestStorageSender) getSentMetrics() []promwrite.TimeSeries {
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

	t.Run("keyToLabels with commas in values", func(t *testing.T) {
		// Create a key that matches the format produced by labelsToKey
		// (commas inside values are escaped as \,)
		key := "job=test,instance=localhost:9090,path=/a\\,b\\,c/temp,user=admin,"

		labels := keyToLabels(key)

		labelMap := make(map[string]string)
		for _, l := range labels {
			labelMap[l.Name] = l.Value
		}

		assert.Equal(t, 4, len(labels))
		assert.Equal(t, "test", labelMap["job"])
		assert.Equal(t, "localhost:9090", labelMap["instance"])
		assert.Equal(t, "/a,b,c/temp", labelMap["path"]) // commas should be unescaped
		assert.Equal(t, "admin", labelMap["user"])
	})

	t.Run("labelsToKey and keyToLabels roundtrip with commas in values", func(t *testing.T) {
		labels := []promwrite.Label{
			{Name: "job", Value: "test"},
			{Name: "path", Value: "/a,b,c/temp"}, // value with commas
			{Name: "instance", Value: "localhost"},
		}

		key := labelsToKey(labels)
		convertedLabels := keyToLabels(key)

		assert.Len(t, convertedLabels, 3)
		labelMap := make(map[string]string)
		for _, l := range convertedLabels {
			labelMap[l.Name] = l.Value
		}
		assert.Equal(t, "test", labelMap["job"])
		assert.Equal(t, "/a,b,c/temp", labelMap["path"])
		assert.Equal(t, "localhost", labelMap["instance"])
	})

	t.Run("handleMetric counter accumulation", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		testSender.reset()
		collector := newCollector(testSender, 1*time.Second, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_counter"},
			{Name: "job", Value: "test"},
		}

		// First value - should not be sent
		now := time.Now()
		collector.handleMetric(metricData{labels: labels, value: 10.0, timestamp: now.UnixMilli(), metricType: metricTypeCounter, ttl: 0})
		assert.Empty(t, testSender.getSentMetrics())

		// Second value in same time window - should accumulate but not send
		collector.handleMetric(metricData{labels: labels, value: 5.0, timestamp: now.UnixMilli(), metricType: metricTypeCounter, ttl: 0})
		assert.Empty(t, testSender.getSentMetrics())

		// Third value in next time window - should send accumulated value
		nextTime := now.Add(10 * time.Second)
		collector.handleMetric(metricData{labels: labels, value: 3.0, timestamp: nextTime.UnixMilli(), metricType: metricTypeCounter, ttl: 0})
		time.Sleep(2 * time.Second)
		sendedMetrics := testSender.getSentMetrics()

		assert.Equal(t, 1, len(sendedMetrics))
		assert.Equal(t, 18.0, sendedMetrics[0].Sample.Value) // 10 + 5 + 3
	})

	t.Run("handleMetric counter accumulation with ttl", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		testSender.reset()
		collector := newCollector(testSender, 1*time.Second, logger)

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_counter"},
			{Name: "job", Value: "test"},
		}

		// First value - should not be sent
		now := time.Now()
		collector.handleMetric(metricData{labels: labels, value: 10.0, timestamp: now.UnixMilli(), metricType: metricTypeCounter, ttl: 5000})

		time.Sleep(3 * time.Second)
		sendedMetrics := testSender.getSentMetrics()

		assert.GreaterOrEqual(t, 3, len(sendedMetrics))
		assert.Equal(t, 10.0, sendedMetrics[0].Sample.Value)
	})

	t.Run("concurrent access", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		testSender := &TestStorageSender{}
		collector := newCollector(testSender, 1*time.Second, logger)

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
					collector.handleMetric(metricData{labels: labels, value: float64(j), timestamp: time.Now().UnixMilli(), metricType: metricTypeCounter, ttl: 0})
				}
			}(i)
		}

		wg.Wait()

		// Stop collector to prevent race with flush routine
		collector.shutdown()

		// Verify all metrics are stored
		count := 0
		for range collector.metrics {
			count++
		}

		assert.Equal(t, numGoroutines*numMetrics, count)
	})
}

func TestCreateTimeSeries(t *testing.T) {
	t.Run("createTimeSeries with valid metricValue", func(t *testing.T) {
		now := time.Now()
		mv := &metricCollectorValue{
			value:     123.45,
			timestamp: now.UnixMilli(),
		}

		labels := []promwrite.Label{
			{Name: "__name__", Value: "test_metric"},
			{Name: "instance", Value: "localhost"},
		}

		roundPeriod := 30 * time.Second
		ts := createTimeSeries(labels, mv, roundPeriod)

		assert.Equal(t, labels, ts.Labels)
		assert.Equal(t, 123.45, ts.Sample.Value)
		assert.Equal(t, now.Truncate(roundPeriod), ts.Sample.Time.Truncate(time.Millisecond))
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
