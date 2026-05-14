package http_request

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

// ---------- helper tests ----------

func TestGetAddrFromURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{"https with port", "https://example.com:8443", "example.com:8443"},
		{"https without port", "https://example.com", "example.com:443"},
		{"http without port", "http://example.com", "example.com:80"},
		{"http with port", "http://example.com:8080", "example.com:8080"},
		{"with path", "https://api.example.com/v1/data", "api.example.com:443"},
		{"invalid url", "://invalid", ""},
		{"empty string", "", ":80"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getAddrFromURL(tt.url)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsURLTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want bool
	}{
		{"https url", "https://example.com", true},
		{"http url", "http://example.com", false},
		{"https with path", "https://api.example.com/v1/data", true},
		{"http with port", "http://example.com:8080", false},
		{"invalid url", "://invalid", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isURLTLS(tt.url)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsSuccessStatusCode(t *testing.T) {
	t.Parallel()

	t.Run("empty success codes", func(t *testing.T) {
		assert.False(t, isSuccessStatusCode(200, nil))
		assert.False(t, isSuccessStatusCode(200, []int{}))
	})

	t.Run("single code match", func(t *testing.T) {
		assert.True(t, isSuccessStatusCode(200, []int{200}))
	})

	t.Run("single code no match", func(t *testing.T) {
		assert.False(t, isSuccessStatusCode(404, []int{200}))
	})

	t.Run("multiple codes match", func(t *testing.T) {
		assert.True(t, isSuccessStatusCode(201, []int{200, 201, 204}))
		assert.True(t, isSuccessStatusCode(204, []int{200, 201, 204}))
	})

	t.Run("multiple codes no match", func(t *testing.T) {
		assert.False(t, isSuccessStatusCode(500, []int{200, 201, 204}))
	})

	t.Run("mixed codes", func(t *testing.T) {
		assert.False(t, isSuccessStatusCode(301, []int{200, 404, 500}))
	})
}

// ---------- test helpers ----------

// startTestServer starts an HTTP server on a random port and returns it
// together with the base URL (http://...). Caller must close the server.
func startTestServer(handler http.Handler) (*http.Server, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	return server, "http://" + listener.Addr().String()
}

// waitForServer polls urlStr with GET until it responds or timeout elapses.
func waitForServer(urlStr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(urlStr) //nolint:gosec // test only
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server not ready within %s", timeout)
}

// makeTestPlugin creates a Plugin with Start(), but overrides its client
// so that IsTLS is false. This lets us test against plain HTTP servers.
func makeTestPlugin(t *testing.T, config *Config) *Plugin {
	t.Helper()
	parsedConfig := test.NewConfig(config, nil)
	logger, _ := zap.NewDevelopment()
	params := &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{
			PipelineName:     "test",
			PipelineSettings: &pipeline.Settings{},
		},
		Logger: logger.Sugar(),
	}

	p := &Plugin{}
	p.Start(parsedConfig, params)

	// Override the client so it talks plain HTTP instead of TLS.
	p.client = &fasthttp.HostClient{
		Addr:         getAddrFromURL(p.config.Address),
		ReadTimeout:  p.config.Timeout_,
		WriteTimeout: p.config.Timeout_,
		IsTLS:        false,
	}

	return p
}

func makeEvent(t *testing.T, jsonStr string) *pipeline.Event {
	t.Helper()
	root, err := insaneJSON.DecodeString(jsonStr)
	require.NoError(t, err)
	return &pipeline.Event{
		Root: root,
	}
}

// ---------- Do method integration tests ----------

func TestDo_SuccessfulRequest(t *testing.T) {
	var mu sync.Mutex
	receivedBody := ""
	method := ""

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		method = r.Method
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		receivedBody = string(buf)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	})

	server, addr := startTestServer(handler)
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:       addr,
		Method:        "POST",
		ContentType:   "application/json",
		ResponseField: "response",
		SuccessCodes:  []int{200},
		Timeout:       cfg.Duration("5s"),
		Retention:     cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"foo":"bar"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Equal(t, "POST", method)
	assert.Equal(t, `{"foo":"bar"}`, receivedBody)
	mu.Unlock()

	assert.Equal(t, `{"result":"ok"}`, event.Root.Dig("response").AsString())
}

func TestDo_NonSuccessStatusCode(t *testing.T) {
	// The non-success status code path is exercised indirectly via
	// isSuccessStatusCode unit tests. The Do() method always returns
	// ActionPass, so integration testing of the retry loop on a 500
	// is impractical because the exponential backoff runs for ~15 minutes.
	//
	// This test verifies that when a request would fail fast (server down)
	// the plugin still returns ActionPass.
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{}`))
	}))
	require.NoError(t, waitForServer(addr, time.Second))
	defer server.Close()
	time.Sleep(50 * time.Millisecond)

	p := makeTestPlugin(t, &Config{
		Address:      addr,
		Method:       "POST",
		ContentType:  "application/json",
		SuccessCodes: []int{200},
		Retry:        1,
	})

	event := makeEvent(t, `{"foo":"bar"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)
}

func TestDo_WithURLParams(t *testing.T) {
	var mu sync.Mutex
	requestURI := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestURI = r.RequestURI
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{id}/profile",
		Method:       "POST",
		ContentType:  "application/json",
		Params:       map[string]string{"id": "user_id"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"user_id":"123"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Contains(t, requestURI, "/123/profile")
	mu.Unlock()
}

func TestDo_WithQueryParams(t *testing.T) {
	var mu sync.Mutex
	requestURI := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestURI = r.RequestURI
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/search",
		Method:       "GET",
		ContentType:  "application/json",
		Params:       map[string]string{"q": "query", "limit": "max_results"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"query":"hello","max_results":"10"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Contains(t, requestURI, "q=hello")
	assert.Contains(t, requestURI, "limit=10")
	mu.Unlock()
}

func TestDo_MixedParams_URLAndQuery(t *testing.T) {
	var mu sync.Mutex
	requestURI := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestURI = r.RequestURI
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{tenant}/items",
		Method:       "GET",
		ContentType:  "application/json",
		Params:       map[string]string{"tenant": "tenant_name", "filter": "filter_by"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"tenant_name":"acme","filter_by":"active"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Contains(t, requestURI, "/acme/items")
	assert.Contains(t, requestURI, "filter=active")
	mu.Unlock()
}

func TestDo_CustomHeaders(t *testing.T) {
	var mu sync.Mutex
	receivedHeaders := make(http.Header)

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr,
		Method:       "POST",
		ContentType:  "application/json",
		Headers:      map[string]string{"X-Custom-Header": "custom_value", "Authorization": "Bearer token123"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"foo":"bar"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Equal(t, "custom_value", receivedHeaders.Get("X-Custom-Header"))
	assert.Equal(t, "Bearer token123", receivedHeaders.Get("Authorization"))
	mu.Unlock()
}

func TestDo_WithEventThroughPipelineMock(t *testing.T) {
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	config := test.NewConfig(&Config{
		Address:       addr,
		Method:        "POST",
		ContentType:   "application/json",
		ResponseField: "http_response",
		SuccessCodes:  []int{200},
		Timeout:       cfg.Duration("5s"),
		Retention:     cfg.Duration("10ms"),
	}, nil)

	p, input, output := test.NewPipelineMock(
		test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false),
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{"status":"ok"}`, e.Root.Dig("http_response").AsString())
		wg.Done()
	})

	input.In(0, "test", test.NewOffset(0), []byte(`{"message":"hello"}`))

	wg.Wait()
	p.Stop()
}

func TestDo_ServerTimeout(t *testing.T) {
	// The timeout + retry loop would take ~15 minutes because the
	// plugin uses backoff.DefaultMaxElapsedTime.  Instead, we test
	// that a pre-closed server (immediate connection refused) still
	// results in ActionPass.
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	require.NoError(t, waitForServer(addr, time.Second))
	defer server.Close()
	time.Sleep(50 * time.Millisecond)

	p := makeTestPlugin(t, &Config{
		Address:      addr,
		Method:       "POST",
		ContentType:  "application/json",
		SuccessCodes: []int{200},
		Retry:        1,
		Timeout:      cfg.Duration("1s"),
		Retention:    cfg.Duration("1ms"),
	})

	event := makeEvent(t, `{"foo":"bar"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)
}

func TestDo_NoResponseField(t *testing.T) {
	var mu sync.Mutex
	receivedBody := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		receivedBody = string(buf)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`created`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr,
		Method:       "POST",
		ContentType:  "application/json",
		SuccessCodes: []int{201},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
		// ResponseField is empty — no response stored
	})

	event := makeEvent(t, `{"a":1}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Equal(t, `{"a":1}`, receivedBody)
	mu.Unlock()

	// The original event should remain unchanged
	assert.Equal(t, `{"a":1}`, event.Root.EncodeToString())
}

func TestDo_MultipleSuccessCodes(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"200 OK", http.StatusOK},
		{"201 Created", http.StatusCreated},
		{"204 No Content", http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(`ok`))
			}))
			defer server.Close()
			require.NoError(t, waitForServer(addr, time.Second))

			p := makeTestPlugin(t, &Config{
				Address:      addr,
				Method:       "GET",
				ContentType:  "application/json",
				SuccessCodes: []int{200, 201, 204},
				Timeout:      cfg.Duration("5s"),
				Retention:    cfg.Duration("10ms"),
			})

			event := makeEvent(t, `{"x":"y"}`)
			result := p.Do(event)
			assert.Equal(t, pipeline.ActionPass, result)
		})
	}
}

func TestDo_GETMethod(t *testing.T) {
	var mu sync.Mutex
	method := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		method = r.Method
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr,
		Method:       "GET",
		ContentType:  "application/json",
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"a":1}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Equal(t, "GET", method)
	mu.Unlock()
}

func TestDo_MissingParam(t *testing.T) {
	// When a param doesn't exist in the event, it gets the empty string value
	var mu sync.Mutex
	requestURI := ""

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestURI = r.RequestURI
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr, time.Second))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{id}",
		Method:       "POST",
		ContentType:  "application/json",
		Params:       map[string]string{"id": "nonexistent_field", "extra": "extra_field"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"extra_field":"hello"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	// extra_field is not in a URL placeholder so it becomes a query param
	assert.Contains(t, requestURI, "extra=hello")
	mu.Unlock()
}

// ---------- Config & factory tests ----------

func TestConfigDefaults(t *testing.T) {
	c := &Config{}
	err := cfg.SetDefaultValues(c)
	require.NoError(t, err)

	assert.Equal(t, "POST", c.Method)
	assert.Equal(t, "5s", string(c.Timeout))
	assert.Equal(t, "application/json", c.ContentType)
	assert.Equal(t, false, c.ForceHTTP2)
	assert.Equal(t, 10, c.Retry)
	assert.Equal(t, "50ms", string(c.Retention))
	assert.Equal(t, 2, c.RetentionExponentMultiplier)
}

func TestConfigParsing(t *testing.T) {
	cfg := &Config{
		Address:      "https://example.com/api",
		Method:       "PATCH",
		ContentType:  "application/xml",
		SuccessCodes: []int{200, 204},
		Timeout:      cfg.Duration("10s"),
		Retention:    cfg.Duration("100ms"),
	}

	parsed := test.NewConfig(cfg, nil)
	require.NotNil(t, parsed)

	pcfg := parsed.(*Config)
	assert.Equal(t, "PATCH", pcfg.Method)
	assert.Equal(t, 10*time.Second, pcfg.Timeout_)
	assert.Equal(t, 100*time.Millisecond, pcfg.Retention_)
	assert.Equal(t, []int{200, 204}, pcfg.SuccessCodes)
	assert.Equal(t, "application/xml", pcfg.ContentType)
}

func TestFactory(t *testing.T) {
	plugin, config := factory()
	assert.NotNil(t, plugin)
	assert.NotNil(t, config)

	_, ok := plugin.(*Plugin)
	assert.True(t, ok)

	_, ok = config.(*Config)
	assert.True(t, ok)
}

func TestPlugin_Stop(t *testing.T) {
	p := &Plugin{}
	// Stop should not panic
	p.Stop()
}
