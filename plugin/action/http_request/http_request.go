package http_request

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgrr/http2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtls"
	"github.com/valyala/fasthttp"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
Makes HTTP requests with URL templating, query params, and custom headers. Writes response to event field. Supports retry with exponential backoff.
}*/

/*{ examples
```yaml
pipelines:
  - example:
      actions:
        ...
        - type: http_request
          address: "http://example.com/api/{{ .id | default 'unknown' }}"
          method: GET
          content_type: "application/json"
          params:
            id: "field.id"
            user_id: "user"
          response_field: "http_response"
          retry: 3
          retention: 100ms
          timeout: 5s
```

### example of request to server:
```GET http://example.com/api/id_value?id=id_value&user_id=user```

All params are added to the query string, even if they are also used in the address template.
In this example `id` is sent both in the path and in the query string.

}*/

type Plugin struct {
	config          *Config
	logger          *zap.Logger
	client          *fasthttp.HostClient
	requestsMetric  *metric.CounterVec
	latencyMetric   *metric.Histogram
	backoffStrategy backoff.BackOff
	paramFields     map[string][]string
	requiredParams  map[string]struct{}
	addressTmpl     *template.Template
	buf             bytes.Buffer
	ctx             context.Context
	cancel          context.CancelFunc
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Parameters to use in the address template and in the query string.
	// > Each parameter is available in the `address` template as `{{ .name }}`
	// > and is also added to the query string, even if it is already used in the template.
	// > Parameter values are URL-escaped before being substituted into the address.
	Params map[string]string `json:"params"` // *

	// > @3@4@5@6
	// >
	// > HTTP method to use.
	Method string `json:"method" default:"POST" options:"POST|GET|PATCH"` // *

	// > @3@4@5@6
	// >
	// > URL address to send requests to. Only `http` and `https` schemes are supported.
	// > Example: `http://localhost:8080/api/v1/events`
	Address string `json:"address" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Timeout for the HTTP request. Must be positive.
	Timeout  cfg.Duration `json:"timeout" default:"5s" parse:"duration"` // *
	Timeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Value of the Content-Type header.
	ContentType string `json:"content_type" default:"application/json"` // *

	// > @3@4@5@6
	// >
	// > Field name to store the HTTP response body.
	ResponseField string `json:"response_field" default:""` // *

	// > @3@4@5@6
	// >
	// > Force HTTP/2 for the request. Only supported for https addresses.
	// > If the server doesn't support HTTP/2 or the connection can't be
	// > established, the client falls back to HTTP/1.1.
	ForceHTTP2 bool `json:"force_http2" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Custom headers to add to the HTTP request.
	Headers map[string]string `json:"headers"` // *

	// > @3@4@5@6
	// >
	// > Number of retry attempts for failed HTTP requests.
	// > A negative value means unlimited attempts, bounded by `max_elapsed_time`.
	// > Uses exponential backoff strategy between retries.
	// > If all retries fail, the event is passed through without being sent.
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > Initial interval for exponential backoff between retries. Must be positive.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retry interval.
	// > Each retry interval will be multiplied by this value. Must be positive.
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > Maximum total time spent retrying a single request, including the time
	// > spent executing requests. Zero disables the limit. Must be non-negative.
	MaxElapsedTime  cfg.Duration `json:"max_elapsed_time" default:"1m" parse:"duration"` // *
	MaxElapsedTime_ time.Duration

	// > @3@4@5@6
	// >
	// > List of HTTP status codes that are considered successful.
	SuccessCodes []int `json:"success_codes" default:"200"` // *

	// > @3@4@5@6
	// >
	// > TLS configuration for the HTTP client.
	// >
	// > `TLSConfig` params:
	// > * **`ca_cert`** *`string`* - path or content of a PEM-encoded CA file
	// > * **`client_cert`** *`string`* - path or content of a PEM-encoded client certificate file
	// > * **`client_key`** *`string`* - path or content of a PEM-encoded client key file
	// > * **`insecure`** *`bool`* - if set, the client will skip SSL/TLS verification
	// >
	// > `client_cert` and `client_key` must be set together.
	TLS *cfg.TLSConfig `json:"tls"` // *

	// > @3@4@5@6
	// >
	// > Prefix added to metric names for better organization.
	// > Useful when running multiple instances to avoid metric name collisions.
	// > Leave empty for default metric naming.
	MetricPrefix string `json:"metric_prefix" default:""` // *

	// > @3@4@5@6
	// >
	// > If set to false, errors will be logged instead of causing a fatal failure.
	// > Useful for non-critical pipelines where you want to avoid stopping the entire pipeline on template errors.
	FailOnError bool `json:"fail_on_error" default:"false"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "http_request",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()

	if err := validateRetryConfig(p.config); err != nil {
		p.logger.Fatal("invalid config", zap.Error(err))
	}

	addr, isTLS, err := getAddrFromURL(p.config.Address)
	if err != nil {
		p.logger.Fatal("invalid address", zap.String("address", p.config.Address), zap.Error(err))
	}
	p.client = &fasthttp.HostClient{
		Addr:                   addr,
		ReadTimeout:            p.config.Timeout_,
		WriteTimeout:           p.config.Timeout_,
		IsTLS:                  isTLS,
		DisablePathNormalizing: true,
	}

	var tlsConfig *cfg.TLSConfig
	if t := p.config.TLS; t != nil {
		tlsConfig = t
	}

	if err := validateTLSConfig(tlsConfig); err != nil {
		p.logger.Fatal("invalid TLS config", zap.Error(err))
	}

	if tlsConfig != nil && (tlsConfig.CACert != "" || tlsConfig.ClientCert != "" || tlsConfig.ClientKey != "" || tlsConfig.Insecure) {
		tlsBuilder := xtls.NewConfigBuilder()
		if tlsConfig.CACert != "" {
			if err := tlsBuilder.AppendCARoot(tlsConfig.CACert); err != nil {
				p.logger.Fatal("can't append CA root", zap.Error(err))
			}
		}
		if tlsConfig.ClientCert != "" && tlsConfig.ClientKey != "" {
			if err := tlsBuilder.AppendX509KeyPair(tlsConfig.ClientCert, tlsConfig.ClientKey); err != nil {
				p.logger.Fatal("can't append X509 key pair", zap.Error(err))
			}
		}
		if tlsConfig.Insecure {
			tlsBuilder.SetSkipVerify(true)
		}
		p.client.TLSConfig = tlsBuilder.Build()
	}

	if err := validateForceHTTP2(p.config.ForceHTTP2, isTLS); err != nil {
		p.logger.Fatal("invalid config",
			zap.String("address", p.config.Address),
			zap.Error(err),
		)
	}

	if p.config.ForceHTTP2 {
		prevDial := p.client.Dial

		dial, clearInitialDeadline := newHTTP2DialFunc(p.config.Timeout_)
		p.client.Dial = dial

		err := http2.ConfigureClient(p.client, http2.ClientOpts{MaxResponseTime: p.config.Timeout_})
		clearInitialDeadline()
		if err != nil {
			p.client.Dial = prevDial

			msg := "can't establish HTTP/2 connection, falling back to HTTP/1.1"
			if errors.Is(err, http2.ErrServerSupport) {
				msg = "server does not support HTTP/2, falling back to HTTP/1.1"
			}
			p.logger.Warn(msg,
				zap.String("address", p.client.Addr),
				zap.Error(err),
			)
		}
	}

	p.registerMetrics(params.MetricCtl, p.config.MetricPrefix)

	p.backoffStrategy = p.newBackoffStrategy()

	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.paramFields = make(map[string][]string, len(p.config.Params))
	for name, fieldSelector := range p.config.Params {
		p.paramFields[name] = cfg.ParseFieldSelector(fieldSelector)
	}

	p.addressTmpl = newAddressTemplate(p.config.Address)
	p.requiredParams = getRequiredParams(p.addressTmpl)
}

func newAddressTemplate(address string) *template.Template {
	return template.Must(template.New("").Funcs(template.FuncMap{
		"default": func(defaultValue string, value any) any {
			if value == nil || value == "" {
				return escapeURLValue(defaultValue)
			}
			return value
		},
	}).Parse(address))
}

// escapeURLValue percent-encodes the value so it can be safely embedded into
// a path segment or a query value without changing the URL structure.
func escapeURLValue(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func validateRetryConfig(config *Config) error {
	if config.Timeout_ <= 0 {
		return errors.New("'timeout' must be positive")
	}
	if config.Retention_ <= 0 {
		return errors.New("'retention' must be positive")
	}
	if config.RetentionExponentMultiplier <= 0 {
		return errors.New("'retention_exponentially_multiplier' must be positive")
	}
	if config.MaxElapsedTime_ < 0 {
		return errors.New("'max_elapsed_time' can't be negative")
	}
	return nil
}

func validateForceHTTP2(forceHTTP2, isTLS bool) error {
	if forceHTTP2 && !isTLS {
		return errors.New("force_http2 is only supported for https addresses")
	}
	return nil
}

// newHTTP2DialFunc returns a dialer and a cleanup func. The dialer bounds the
// initial TCP dial, TLS handshake and HTTP/2 handshake with the connection
// deadline, because dgrr/http2 doesn't support handshake timeouts itself.
// The deadline is cleared after ConfigureClient returns so it doesn't affect
// the established connection. Later reconnects are bounded only by the TCP
// dial timeout, as dgrr/http2 doesn't expose a handshake timeout hook.
func newHTTP2DialFunc(timeout time.Duration) (fasthttp.DialFunc, func()) {
	var (
		firstDial   atomic.Bool
		initialConn net.Conn
	)
	firstDial.Store(true)

	dial := func(addr string) (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return nil, err
		}
		if firstDial.CompareAndSwap(true, false) {
			if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			initialConn = conn
		}
		return conn, nil
	}

	clearInitialDeadline := func() {
		if initialConn != nil {
			_ = initialConn.SetDeadline(time.Time{})
		}
	}

	return dial, clearInitialDeadline
}

func validateTLSConfig(tlsConfig *cfg.TLSConfig) error {
	if tlsConfig == nil {
		return nil
	}
	if (tlsConfig.ClientCert == "") != (tlsConfig.ClientKey == "") {
		return fmt.Errorf("both client_cert and client_key must be set")
	}
	return nil
}

func getAddrFromURL(rawURL string) (string, bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false, fmt.Errorf("invalid URL: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false, fmt.Errorf("unsupported URL scheme %q: only http and https are supported", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return "", false, errors.New("URL host is empty")
	}

	port := u.Port()
	isTLS := scheme == "https"

	if port == "" {
		if isTLS {
			port = "443"
		} else {
			port = "80"
		}
	}

	return net.JoinHostPort(host, port), isTLS, nil
}

func getRequiredParams(tmpl *template.Template) map[string]struct{} {
	required := make(map[string]struct{})
	if tmpl == nil || tmpl.Tree == nil || tmpl.Tree.Root == nil {
		return required
	}
	collectRequiredParams(tmpl.Tree.Root, required)
	return required
}

func collectRequiredParams(node parse.Node, required map[string]struct{}) {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil {
			return
		}
		for _, child := range n.Nodes {
			collectRequiredParams(child, required)
		}
	case *parse.ActionNode:
		collectPipeParams(n.Pipe, required)
	case *parse.IfNode:
		collectPipeParams(n.Pipe, required)
		collectRequiredParams(n.List, required)
		collectRequiredParams(n.ElseList, required)
	case *parse.RangeNode:
		collectPipeParams(n.Pipe, required)
		collectRequiredParams(n.List, required)
		collectRequiredParams(n.ElseList, required)
	case *parse.WithNode:
		collectPipeParams(n.Pipe, required)
		collectRequiredParams(n.List, required)
		collectRequiredParams(n.ElseList, required)
	case *parse.TemplateNode:
		collectPipeParams(n.Pipe, required)
	}
}

func collectPipeParams(pipe *parse.PipeNode, required map[string]struct{}) {
	if pipe == nil {
		return
	}
	for _, cmd := range pipe.Cmds {
		if len(cmd.Args) == 0 {
			continue
		}
		if ident, ok := cmd.Args[0].(*parse.IdentifierNode); ok && ident.Ident == "default" {
			return
		}
	}
	for _, cmd := range pipe.Cmds {
		for _, arg := range cmd.Args {
			collectNodeParams(arg, required)
		}
	}
}

func collectNodeParams(node parse.Node, required map[string]struct{}) {
	switch n := node.(type) {
	case *parse.FieldNode:
		if len(n.Ident) > 0 {
			required[n.Ident[0]] = struct{}{}
		}
	case *parse.ChainNode:
		collectNodeParams(n.Node, required)
	case *parse.PipeNode:
		collectPipeParams(n, required)
	case *parse.CommandNode:
		for _, arg := range n.Args {
			collectNodeParams(arg, required)
		}
	}
}

func (p *Plugin) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	if p.client != nil {
		p.client.CloseIdleConnections()
	}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl, prefix string) {
	var requestsMetricName string
	var latencyMetricName string
	if prefix == "" {
		requestsMetricName = "action_http_request_total"
		latencyMetricName = "action_http_request_latency_seconds"
	} else {
		requestsMetricName = fmt.Sprintf("action_%s_http_request_total", prefix)
		latencyMetricName = fmt.Sprintf("action_%s_http_request_latency_seconds", prefix)
	}

	p.requestsMetric = ctl.RegisterCounterVec(requestsMetricName, "Total HTTP requests", "result")
	p.latencyMetric = ctl.RegisterHistogram(latencyMetricName, "HTTP request latency in seconds", metric.SecondsBucketsDetailed)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// Reset backoff strategy for new attempt
	p.backoffStrategy.Reset()

	// Extract parameter values using pre-parsed field selectors.
	params := make(map[string]string, len(p.paramFields))
	for name, fields := range p.paramFields {
		value := event.Root.Dig(fields...).AsString()
		params[name] = value
	}

	// Check if required params are empty (no default filter)
	for fieldName := range p.requiredParams {
		if params[fieldName] == "" {
			return p.handleConfigError("required template field is empty",
				zapcore.WarnLevel,
				zap.String("field", fieldName),
				zap.String("address", p.config.Address),
			)
		}
	}

	// Build the address using template execution
	templateParams := make(map[string]string, len(params))
	for name, value := range params {
		templateParams[name] = escapeURLValue(value)
	}

	p.buf.Reset()
	if err := p.addressTmpl.Execute(&p.buf, templateParams); err != nil {
		return p.handleConfigError("failed to execute URL template",
			zapcore.ErrorLevel,
			zap.String("address", p.config.Address),
			zap.Error(err),
		)
	}
	address := p.buf.String()

	// Measure total request time including retries
	startTime := time.Now()

	// Set up backoff for retries
	operation := func() error {
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)

		req.SetRequestURI(address)

		// Keep the escaped path as is, otherwise fasthttp decodes and
		// normalizes it, which can turn escaped values into path segments.
		uri := req.URI()
		if len(uri.PathOriginal()) == 0 {
			uri.SetPath("/")
		}

		// Add all params as query string parameters.
		for name, value := range params {
			req.URI().QueryArgs().Add(name, value)
		}
		req.Header.SetMethod(p.config.Method)
		req.Header.SetContentType(p.config.ContentType)

		// Add custom headers from config.
		for key, value := range p.config.Headers {
			req.Header.Set(key, value)
		}

		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		if err := p.client.DoTimeout(req, resp, p.config.Timeout_); err != nil {
			p.logger.Error("http request failed",
				zap.String("address", address),
				zap.String("method", p.config.Method),
				zap.Error(err),
			)
			p.requestsMetric.WithLabelValues("error").Inc()
			return err
		}

		statusCode := resp.Header.StatusCode()
		if !slices.Contains(p.config.SuccessCodes, statusCode) {
			p.logger.Error("http request returned non-success status code",
				zap.String("address", address),
				zap.String("method", p.config.Method),
				zap.Int("status_code", statusCode),
			)
			p.logger.Debug("error response body",
				zap.Any("request", params),
				zap.ByteString("response", resp.Body()),
			)

			p.requestsMetric.WithLabelValues("not_success").Inc()

			err := fmt.Errorf("non-success status code: %d", statusCode)
			return err
		}

		// Write the response body to the configured response_field.
		if p.config.ResponseField != "" {
			event.Root.AddFieldNoAlloc(event.Root, p.config.ResponseField).MutateToBytesCopy(event.Root, resp.Body())
		}

		p.requestsMetric.WithLabelValues("success").Inc()
		return nil
	}

	err := backoff.Retry(operation, backoff.WithContext(p.backoffStrategy, p.ctx))
	latency := time.Since(startTime)
	p.latencyMetric.Observe(latency.Seconds())

	if err != nil {
		p.logger.Error("http request failed after retries",
			zap.String("address", address),
			zap.String("method", p.config.Method),
			zap.Error(err),
		)
	}

	return pipeline.ActionPass
}

func (p *Plugin) handleConfigError(msg string, level zapcore.Level, fields ...zap.Field) pipeline.ActionResult {
	if p.config.FailOnError {
		p.logger.Fatal(msg, fields...)
	}
	switch level {
	case zapcore.ErrorLevel:
		p.logger.Error(msg, fields...)
	default:
		p.logger.Warn(msg, fields...)
	}
	return pipeline.ActionDiscard
}

func (p *Plugin) newBackoffStrategy() backoff.BackOff {
	expBackoff := backoff.ExponentialBackOff{
		InitialInterval:     p.config.Retention_,
		Multiplier:          float64(p.config.RetentionExponentMultiplier),
		RandomizationFactor: 0.5,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      p.config.MaxElapsedTime_,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	expBackoff.Reset()
	if p.config.Retry < 0 {
		// Negative retry means unlimited attempts; MaxElapsedTime still applies.
		return &expBackoff
	}
	return backoff.WithMaxRetries(&expBackoff, uint64(p.config.Retry))
}
