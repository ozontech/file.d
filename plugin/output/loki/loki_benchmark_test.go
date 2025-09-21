package loki

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

// Benchmark data structures
type benchmarkEvent struct {
	message   string
	timestamp string
	metadata  map[string]string
}

// generateBenchmarkEvents creates a slice of benchmark events with realistic data
func generateBenchmarkEvents(count int) []benchmarkEvent {
	events := make([]benchmarkEvent, count)

	sampleMessages := []string{
		`{"level":"info","message":"User login successful","user_id":"12345","ip":"192.168.1.1"}`,
		`{"level":"error","message":"Database connection failed","error":"connection timeout","retry_count":"3"}`,
		`{"level":"debug","message":"Processing request","request_id":"req-abc123","duration_ms":"45"}`,
		`{"level":"warn","message":"High memory usage detected","memory_usage":"85%","threshold":"80%"}`,
		`{"level":"info","message":"File uploaded successfully","file_size":"2.5MB","file_type":"image/jpeg"}`,
		`{"level":"error","message":"API rate limit exceeded","endpoint":"/api/users","limit":"100/min"}`,
		`{"level":"info","message":"Cache miss for key","cache_key":"user:12345:profile"}`,
		`{"level":"debug","message":"Query executed","query":"SELECT * FROM users WHERE id = ?","execution_time":"12ms"}`,
	}

	// Sample metadata fields
	metadataFields := []string{"service", "version", "environment", "hostname", "region", "cluster"}
	metadataValues := map[string][]string{
		"service":     {"api-gateway", "user-service", "auth-service", "payment-service"},
		"version":     {"v1.2.3", "v1.2.4", "v1.3.0", "v2.0.0"},
		"environment": {"production", "staging", "development"},
		"hostname":    {"web-01", "web-02", "api-01", "api-02"},
		"region":      {"us-east-1", "us-west-2", "eu-west-1"},
		"cluster":     {"prod-cluster", "staging-cluster"},
	}

	now := time.Now()

	for i := 0; i < count; i++ {
		// Random timestamp within the last hour
		ts := now.Add(-time.Duration(rand.Intn(3600)) * time.Second)

		// Random message
		message := sampleMessages[rand.Intn(len(sampleMessages))]

		// Random metadata
		metadata := make(map[string]string)
		for _, field := range metadataFields {
			if values, exists := metadataValues[field]; exists {
				metadata[field] = values[rand.Intn(len(values))]
			}
		}

		events[i] = benchmarkEvent{
			message:   message,
			timestamp: strconv.FormatInt(ts.UnixNano(), 10),
			metadata:  metadata,
		}
	}

	return events
}

// createBenchmarkEvent creates a pipeline event from benchmark data
func createBenchmarkEvent(eventData benchmarkEvent) *pipeline.Event {
	root := insaneJSON.Spawn()

	// Parse the JSON message
	if err := root.DecodeBytes([]byte(eventData.message)); err != nil {
		// If parsing fails, create a simple structure
		root.AddField("message").MutateToString(eventData.message)
	}

	// Add timestamp
	root.AddField("timestamp").MutateToString(eventData.timestamp)

	// Add metadata
	for key, value := range eventData.metadata {
		root.AddField(key).MutateToString(value)
	}

	return &pipeline.Event{
		Root: root,
	}
}

// DataFormat.String() method
func (df DataFormat) String() string {
	switch df {
	case FormatJSON:
		return "json"
	case FormatProto:
		return "proto"
	default:
		return "json"
	}
}

// BenchmarkLokiJSONRequestBuilding benchmarks JSON request building
func BenchmarkLokiJSONRequestBuilding(b *testing.B) {
	benchmarkCases := []struct {
		name       string
		eventCount int
	}{
		{"SmallBatch", 10},
		{"MediumBatch", 100},
		{"LargeBatch", 1000},
		{"VeryLargeBatch", 10000},
	}

	for _, bc := range benchmarkCases {
		b.Run(fmt.Sprintf("%s_%d_events", bc.name, bc.eventCount), func(b *testing.B) {
			// Create a minimal plugin for request building
			plugin := &Plugin{
				config: &Config{
					MessageField:   "message",
					TimestampField: "timestamp",
					DataFormat_:    FormatJSON,
					Labels: []Label{
						{Label: "service", Value: "benchmark"},
						{Label: "format", Value: "json"},
					},
				},
				labels: map[string]string{
					"service": "benchmark",
					"format":  "json",
				},
			}

			events := generateBenchmarkEvents(bc.eventCount)

			b.ResetTimer()
			b.ReportAllocs()

			var root *insaneJSON.Root

			for i := 0; i < b.N; i++ {
				// Create a batch of events for each iteration
				root = insaneJSON.Spawn()
				dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()

				for _, eventData := range events {
					event := createBenchmarkEvent(eventData)
					dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
				}

				_, err := plugin.buildJSONRequestBody(root)
				insaneJSON.Release(root)

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLokiProtoRequestBuilding benchmarks protobuf request building
func BenchmarkLokiProtoRequestBuilding(b *testing.B) {
	benchmarkCases := []struct {
		name       string
		eventCount int
	}{
		{"SmallBatch", 10},
		{"MediumBatch", 100},
		{"LargeBatch", 1000},
		{"VeryLargeBatch", 10000},
	}

	for _, bc := range benchmarkCases {
		b.Run(fmt.Sprintf("%s_%d_events", bc.name, bc.eventCount), func(b *testing.B) {
			// Create a minimal plugin for request building
			plugin := &Plugin{
				config: &Config{
					MessageField:   "message",
					TimestampField: "timestamp",
					DataFormat_:    FormatProto,
					Labels: []Label{
						{Label: "service", Value: "benchmark"},
						{Label: "format", Value: "proto"},
					},
				},
				labels: map[string]string{
					"service": "benchmark",
					"format":  "proto",
				},
			}

			events := generateBenchmarkEvents(bc.eventCount)

			b.ResetTimer()
			b.ReportAllocs()

			var root *insaneJSON.Root

			for i := 0; i < b.N; i++ {
				// Create a batch of events for each iteration
				root = insaneJSON.Spawn()
				dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()

				for _, eventData := range events {
					event := createBenchmarkEvent(eventData)
					dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
				}

				_, err := plugin.buildProtoRequestBody(root)
				insaneJSON.Release(root)

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLokiJSONvsProto compares JSON vs protobuf request building performance
func BenchmarkLokiJSONvsProto(b *testing.B) {
	eventCount := 1000

	// Benchmark JSON
	b.Run("JSON", func(b *testing.B) {
		plugin := &Plugin{
			config: &Config{
				MessageField:   "message",
				TimestampField: "timestamp",
				DataFormat_:    FormatJSON,
				Labels: []Label{
					{Label: "service", Value: "benchmark"},
					{Label: "format", Value: "json"},
				},
			},
			labels: map[string]string{
				"service": "benchmark",
				"format":  "json",
			},
		}

		events := generateBenchmarkEvents(eventCount)

		b.ResetTimer()
		b.ReportAllocs()

		var root *insaneJSON.Root

		for i := 0; i < b.N; i++ {
			// Create a batch of events for each iteration
			root = insaneJSON.Spawn()
			dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()

			for _, eventData := range events {
				event := createBenchmarkEvent(eventData)
				dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
			}

			_, err := plugin.buildJSONRequestBody(root)
			insaneJSON.Release(root)

			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Benchmark Protobuf
	b.Run("Proto", func(b *testing.B) {
		plugin := &Plugin{
			config: &Config{
				MessageField:   "message",
				TimestampField: "timestamp",
				DataFormat_:    FormatProto,
				Labels: []Label{
					{Label: "service", Value: "benchmark"},
					{Label: "format", Value: "proto"},
				},
			},
			labels: map[string]string{
				"service": "benchmark",
				"format":  "proto",
			},
		}

		events := generateBenchmarkEvents(eventCount)

		b.ResetTimer()
		b.ReportAllocs()

		var root *insaneJSON.Root

		for i := 0; i < b.N; i++ {
			// Create a batch of events for each iteration
			root = insaneJSON.Spawn()
			dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()

			for _, eventData := range events {
				event := createBenchmarkEvent(eventData)
				dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
			}

			_, err := plugin.buildProtoRequestBody(root)
			insaneJSON.Release(root)

			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
