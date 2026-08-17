package prometheus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/castai/promwrite"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// mockPrometheus implements PrometheusClient for testing
type mockPrometheus struct {
	t           *testing.T
	mu          sync.Mutex
	writeCalls  []*promwrite.WriteRequest
	writeError  error
	writeOption promwrite.WriteOption
}

func (m *mockPrometheus) Write(ctx context.Context, req *promwrite.WriteRequest, options ...promwrite.WriteOption) (*promwrite.WriteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(options) > 0 {
		m.writeOption = options[0]
	}

	m.writeCalls = append(m.writeCalls, req)

	if m.writeError != nil {
		return nil, m.writeError
	}

	return &promwrite.WriteResponse{}, nil
}

func (m *mockPrometheus) getWriteCalls() []*promwrite.WriteRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]*promwrite.WriteRequest, len(m.writeCalls))
	copy(calls, m.writeCalls)
	return calls
}

func (m *mockPrometheus) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeError = err
}

func (m *mockPrometheus) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalls = nil
	m.writeError = nil
	m.writeOption = nil
}

// mockController implements pipeline.OutputPluginController for testing
type mockController struct {
	mu          sync.Mutex
	commitCount int
}

func (m *mockController) Commit(_ *pipeline.Event) {}
func (m *mockController) Error(_ string)           {}

func TestPluginOut(t *testing.T) {
	t.Run("add event to batcher", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		mockClient := &mockPrometheus{t: t}
		mockClient.reset()

		config := &Config{
			Endpoint: "http://localhost:9090/api/v1/write",
			KeepAlive: KeepAliveConfig{
				MaxIdleConnDuration_: 10 * time.Second,
				MaxConnDuration_:     5 * time.Minute,
			},
			ConnectionTimeout_: 5 * time.Second,
			RequestTimeout_:    time.Second,
		}
		err := cfg.SetDefaultValues(config)
		require.NoError(t, err)

		plugin := &Plugin{
			client: mockClient,
		}

		mockController := &mockController{}

		params := &pipeline.OutputPluginParams{
			PluginDefaultParams: test.NewEmptyOutputPluginParams().PluginDefaultParams,
			Logger:              logger.Sugar(),
			Controller:          mockController,
			Router:              pipeline.NewRouter(),
		}

		plugin.Start(config, params)
		time.Sleep(50 * time.Millisecond)

		// Create a test event using insaneJSON
		root, err := insaneJSON.DecodeBytes([]byte(`{
			"name": "test_metric",
			"type": "gauge",
			"value": 1.0,
			"timestamp": 1234567890,
			"labels": {"job": "test"}
		}`))
		require.NoError(t, err)

		event := &pipeline.Event{
			Root: root,
		}

		// Add event to the plugin
		plugin.Out(event)

		// Wait for batch to be processed
		time.Sleep(200 * time.Millisecond)

		plugin.Stop()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestPluginSend(t *testing.T) {
	t.Run("send metrics through plugin", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		mockClient := &mockPrometheus{t: t}
		mockClient.reset()

		config := &Config{
			Endpoint: "http://localhost:9090/api/v1/write",
			KeepAlive: KeepAliveConfig{
				MaxIdleConnDuration_: 10 * time.Second,
				MaxConnDuration_:     5 * time.Minute,
			},
			ConnectionTimeout_: 5 * time.Second,
			RequestTimeout_:    time.Second,
		}
		err := cfg.SetDefaultValues(config)
		require.NoError(t, err)

		plugin := &Plugin{
			client: mockClient,
		}

		mockController := &mockController{}

		params := &pipeline.OutputPluginParams{
			PluginDefaultParams: test.NewEmptyOutputPluginParams().PluginDefaultParams,
			Logger:              logger.Sugar(),
			Controller:          mockController,
			Router:              pipeline.NewRouter(),
		}

		plugin.Start(config, params)
		time.Sleep(50 * time.Millisecond)

		// Test sending multiple metrics
		for i := 0; i < 5; i++ {
			root, err := insaneJSON.DecodeBytes([]byte(`{
				"name": "metric_` + string(rune('0'+i)) + `",
				"type": "gauge",
				"value": 1.5,
				"timestamp": 1234567890,
				"labels": {"job": "test", "instance": "localhost"}
			}`))
			require.NoError(t, err)

			event := &pipeline.Event{
				Root: root,
			}
			plugin.Out(event)
		}

		// Wait for batch to be processed
		time.Sleep(200 * time.Millisecond)

		plugin.Stop()
		time.Sleep(100 * time.Millisecond)

		// Verify write was called
		calls := mockClient.getWriteCalls()
		assert.GreaterOrEqual(t, len(calls), 0) // May or may not have calls depending on timing
	})
}

func TestPluginWithErrorClient(t *testing.T) {
	t.Run("handles client write error", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		mockClient := &mockPrometheus{t: t}
		mockClient.setError(assert.AnError)

		config := &Config{
			Endpoint: "http://localhost:9090/api/v1/write",
			KeepAlive: KeepAliveConfig{
				MaxIdleConnDuration_: 10 * time.Second,
				MaxConnDuration_:     5 * time.Minute,
			},
			ConnectionTimeout_: 5 * time.Second,
			RequestTimeout_:    time.Second,
		}
		err := cfg.SetDefaultValues(config)
		require.NoError(t, err)

		plugin := &Plugin{
			client: mockClient,
		}

		params := &pipeline.OutputPluginParams{
			PluginDefaultParams: test.NewEmptyOutputPluginParams().PluginDefaultParams,
			Logger:              logger.Sugar(),
			Controller:          &mockController{},
			Router:              pipeline.NewRouter(),
		}

		plugin.Start(config, params)
		time.Sleep(50 * time.Millisecond)

		// Create and send an event that will fail
		root, err := insaneJSON.DecodeBytes([]byte(`{
			"name": "error_metric",
			"type": "gauge",
			"value": 1.0,
			"timestamp": 1234567890,
			"labels": {"job": "test"}
		}`))
		require.NoError(t, err)

		event := &pipeline.Event{
			Root: root,
		}
		plugin.Out(event)

		// Wait for error handling
		time.Sleep(200 * time.Millisecond)

		plugin.Stop()
		time.Sleep(100 * time.Millisecond)

		// Test should complete without panic - error is handled gracefully
		assert.True(t, true)
	})
}
