package http_request

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

// ---------- helper tests ----------

func TestGetAddrFromURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		wantAddr string
		wantTLS  bool
		wantErr  bool
	}{
		{"https with port", "https://example.com:8443", "example.com:8443", true, false},
		{"https without port", "https://example.com", "example.com:443", true, false},
		{"http without port", "http://example.com", "example.com:80", false, false},
		{"http with port", "http://example.com:8080", "example.com:8080", false, false},
		{"with path", "https://api.example.com/v1/data", "api.example.com:443", true, false},
		{"uppercase scheme", "HTTPS://example.com", "example.com:443", true, false},
		{"ipv6 with port", "https://[::1]:8443", "[::1]:8443", true, false},
		{"ipv6 without port", "http://[::1]", "[::1]:80", false, false},
		{"invalid url", "://invalid", "", false, true},
		{"empty string", "", "", false, true},
		{"unsupported scheme", "ftp://example.com", "", false, true},
		{"missing scheme", "example.com/api", "", false, true},
		{"empty host", "http://", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, isTLS, err := getAddrFromURL(tt.url)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, addr)
				assert.False(t, isTLS)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantAddr, addr)
			assert.Equal(t, tt.wantTLS, isTLS)
		})
	}
}

func TestValidateTLSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  *cfg.TLSConfig
		wantErr bool
	}{
		{name: "nil config"},
		{name: "empty config", config: &cfg.TLSConfig{}},
		{name: "cert and key", config: &cfg.TLSConfig{ClientCert: "cert.pem", ClientKey: "key.pem"}},
		{name: "cert without key", config: &cfg.TLSConfig{ClientCert: "cert.pem"}, wantErr: true},
		{name: "key without cert", config: &cfg.TLSConfig{ClientKey: "key.pem"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTLSConfig(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRetryConfig(t *testing.T) {
	t.Parallel()

	configWith := func(mutate func(*Config)) *Config {
		c := &Config{
			Retry:                       10,
			Timeout_:                    time.Second,
			Retention_:                  time.Millisecond,
			RetentionExponentMultiplier: 2,
			MaxElapsedTime_:             time.Minute,
		}
		mutate(c)
		return c
	}

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{name: "defaults", config: configWith(func(*Config) {})},
		{name: "zero retry", config: configWith(func(c *Config) { c.Retry = 0 })},
		{name: "negative retry", config: configWith(func(c *Config) { c.Retry = -1 })},
		{name: "zero max elapsed time", config: configWith(func(c *Config) { c.MaxElapsedTime_ = 0 })},
		{name: "negative max elapsed time", config: configWith(func(c *Config) { c.MaxElapsedTime_ = -time.Second }), wantErr: true},
		{name: "zero timeout", config: configWith(func(c *Config) { c.Timeout_ = 0 }), wantErr: true},
		{name: "negative timeout", config: configWith(func(c *Config) { c.Timeout_ = -time.Second }), wantErr: true},
		{name: "zero retention", config: configWith(func(c *Config) { c.Retention_ = 0 }), wantErr: true},
		{name: "negative retention", config: configWith(func(c *Config) { c.Retention_ = -time.Millisecond }), wantErr: true},
		{name: "zero multiplier", config: configWith(func(c *Config) { c.RetentionExponentMultiplier = 0 }), wantErr: true},
		{name: "negative multiplier", config: configWith(func(c *Config) { c.RetentionExponentMultiplier = -2 }), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRetryConfig(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewBackoffStrategyUnlimitedRetries(t *testing.T) {
	t.Parallel()

	p := &Plugin{config: &Config{
		Retry:                       -1,
		Retention_:                  time.Millisecond,
		RetentionExponentMultiplier: 2,
		MaxElapsedTime_:             0,
	}}

	b := p.newBackoffStrategy()
	b.Reset()
	for i := 0; i < 1000; i++ {
		require.NotEqual(t, backoff.Stop, b.NextBackOff())
	}
}

func TestValidateForceHTTP2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		forceHTTP2 bool
		isTLS      bool
		wantErr    bool
	}{
		{name: "disabled with http", forceHTTP2: false, isTLS: false},
		{name: "disabled with https", forceHTTP2: false, isTLS: true},
		{name: "enabled with https", forceHTTP2: true, isTLS: true},
		{name: "enabled with http", forceHTTP2: true, isTLS: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateForceHTTP2(tt.forceHTTP2, tt.isTLS)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetRequiredParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		address  string
		expected map[string]struct{}
	}{
		{
			name:     "no params",
			address:  "http://example.com/api",
			expected: map[string]struct{}{},
		},
		{
			name:     "single required param",
			address:  "http://example.com/api/{{ .id }}",
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "param with default - not required",
			address:  "http://example.com/api/{{ .id | default \"unknown\" }}",
			expected: map[string]struct{}{},
		},
		{
			name:     "multiple params with default",
			address:  "http://example.com/{{ .tenant | default \"default\" }}/{{ .user | default \"guest\" }}",
			expected: map[string]struct{}{},
		},
		{
			name:     "mixed required and optional",
			address:  "http://example.com/api/{{ .id }}/items?filter={{ .filter | default \"all\" }}",
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "multiple required params",
			address:  "http://example.com/{{ .tenant }}/{{ .user }}/profile",
			expected: map[string]struct{}{"tenant": {}, "user": {}},
		},
		{
			name:     "path with required",
			address:  "http://example.com/{{ .id }}/profile",
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "other filter is still required",
			address:  `http://example.com/{{ .id | printf "%s" }}`,
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "printf param is required",
			address:  `http://example.com/{{ printf "%s" .id }}`,
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "param with default and other filter - not required",
			address:  `http://example.com/{{ .id | printf "%s" | default "unknown" }}`,
			expected: map[string]struct{}{},
		},
		{
			name:     "default as leading function - not required",
			address:  "http://example.com/{{ default \"unknown\" .id }}",
			expected: map[string]struct{}{},
		},
		{
			name:     "param in if condition is required",
			address:  "http://example.com/{{ if .id }}{{ .id }}{{ end }}",
			expected: map[string]struct{}{"id": {}},
		},
		{
			name:     "param used with and without default is required",
			address:  "http://example.com/{{ .id | default \"unknown\" }}/{{ .id }}",
			expected: map[string]struct{}{"id": {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getRequiredParams(newAddressTemplate(tt.address))
			assert.Equal(t, tt.expected, result)
		})
	}
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
func waitForServer(urlStr string) error {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(urlStr) //nolint:gosec // test only
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server not ready within %s", time.Second)
}

// makeTestPlugin creates a Plugin with Start(), but overrides its client
// so that IsTLS is false. This lets us test against plain HTTP servers.
func makeTestPlugin(t *testing.T, config *Config) *Plugin {
	t.Helper()
	parsedConfig := test.NewConfig(config, nil)
	logger, _ := zap.NewDevelopment()
	registry := prometheus.NewRegistry()
	metricCtl := metric.NewCtl("action_http_request", registry, 0, 0)
	params := &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{
			PipelineName:     "test",
			PipelineSettings: &pipeline.Settings{},
			MetricCtl:        metricCtl,
		},
		Logger: logger.Sugar(),
	}

	p := &Plugin{}
	p.Start(parsedConfig, params)

	// Override the client so it talks plain HTTP instead of TLS.
	addr, _, _ := getAddrFromURL(p.config.Address)
	p.client = &fasthttp.HostClient{
		Addr:                   addr,
		ReadTimeout:            p.config.Timeout_,
		WriteTimeout:           p.config.Timeout_,
		IsTLS:                  false,
		DisablePathNormalizing: true,
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
	method := ""

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		method = r.Method
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	})

	server, addr := startTestServer(handler)
	defer server.Close()
	require.NoError(t, waitForServer(addr))

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
	mu.Unlock()

	assert.Equal(t, `{"result":"ok"}`, event.Root.Dig("response").AsString())
}

func TestDo_NonSuccessStatusCode(t *testing.T) {
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{}`))
	}))
	require.NoError(t, waitForServer(addr))
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
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{{ .id }}/profile",
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

func TestDo_URLTemplateEscaping(t *testing.T) {
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
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/api/{{ .id }}",
		Method:       "GET",
		ContentType:  "application/json",
		Params:       map[string]string{"id": "user_id"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"user_id":"a/b?c&d=e #f"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Contains(t, requestURI, "/api/a%2Fb%3Fc%26d%3De%20%23f")
	assert.NotContains(t, requestURI, "a/b?c")
	mu.Unlock()
}

func TestDo_URLTemplateDefaultEscaping(t *testing.T) {
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
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/api/{{ .id | default \"a/b c\" }}",
		Method:       "GET",
		ContentType:  "application/json",
		Params:       map[string]string{"id": "missing_id"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)

	mu.Lock()
	assert.Contains(t, requestURI, "/api/a%2Fb%20c")
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
	require.NoError(t, waitForServer(addr))

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
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{{ .tenant }}/items",
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
	require.NoError(t, waitForServer(addr))

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
	require.NoError(t, waitForServer(addr))

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
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	require.NoError(t, waitForServer(addr))
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
	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`created`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr))

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
			require.NoError(t, waitForServer(addr))

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
	require.NoError(t, waitForServer(addr))

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

func TestDo_EmptyParamUsesDefault(t *testing.T) {
	// When a param used in the address template is empty/missing, its default is used.
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
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{{ .id | default \"missing\" }}",
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
	assert.Contains(t, requestURI, "/missing")
	assert.Contains(t, requestURI, "extra=hello")
	mu.Unlock()
}

func TestDo_MissingRequiredParamWithoutDefaultDiscards(t *testing.T) {
	var mu sync.Mutex
	requests := 0

	server, addr := startTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/{{ .id }}" {
			mu.Lock()
			requests++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()
	require.NoError(t, waitForServer(addr))

	p := makeTestPlugin(t, &Config{
		Address:      addr + "/{{ .id }}",
		Method:       "GET",
		ContentType:  "application/json",
		Params:       map[string]string{"id": "field.id"},
		SuccessCodes: []int{200},
		Timeout:      cfg.Duration("5s"),
		Retention:    cfg.Duration("10ms"),
	})

	event := makeEvent(t, `{"other": "value"}`)
	result := p.Do(event)
	assert.Equal(t, pipeline.ActionDiscard, result)

	mu.Lock()
	assert.Equal(t, 0, requests, "no HTTP request must be sent for a discarded event")
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
	assert.Equal(t, "1m", string(c.MaxElapsedTime))
}

func TestConfigParsing(t *testing.T) {
	c := &Config{
		Address:        "https://example.com/api",
		Method:         "PATCH",
		ContentType:    "application/xml",
		SuccessCodes:   []int{200, 204},
		Timeout:        cfg.Duration("10s"),
		Retention:      cfg.Duration("100ms"),
		MaxElapsedTime: cfg.Duration("30s"),
	}

	parsed := test.NewConfig(c, nil)
	require.NotNil(t, parsed)

	pcfg := parsed.(*Config)
	assert.Equal(t, "PATCH", pcfg.Method)
	assert.Equal(t, 10*time.Second, pcfg.Timeout_)
	assert.Equal(t, 100*time.Millisecond, pcfg.Retention_)
	assert.Equal(t, 30*time.Second, pcfg.MaxElapsedTime_)
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
