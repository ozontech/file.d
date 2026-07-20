package http_request

import (
	"bytes"
	"fmt"
	"net/url"
	"slices"
	"text/template"
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
)

/*{ introduction
Sends HTTP requests with event data as body. Writes response body to the configured response_field. Supports retry with exponential backoff, custom headers, URL templating.
}*/

/*{ examples
```yaml
pipelines:
  - actions:
       	...
       - type: http_request
        address: "http://example.com/api/{{ .id | default "unknown" }}"
        method: GET
        content_type: "application/json"
        params:
          id: "field.id"
          user_id: "user"
        response_field: "http_response"
        retry: 3
        retention: 100ms
        timeout: 5s
    ...
```

# example of request to server:
# GET http://example.com/api/id_value?user_id=user

}*/

type Plugin struct {
	config          *Config
	logger          *zap.Logger
	client          *fasthttp.HostClient
	requestsMetric  *metric.CounterVec
	latencyMetric   *metric.Histogram
	backoffStrategy backoff.BackOff
	paramFields     map[string][]string // pre-parsed field selectors for params
	addressTmpl     *template.Template  // compiled URL template
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Query parameters to add to the request.
	Params map[string]string `json:"params"` // *

	// > @3@4@5@6
	// >
	// > HTTP method to use.
	Method string `json:"method" default:"POST" options:"POST|GET|PATCH"` // *

	// > @3@4@5@6
	// >
	// > URL address to send requests to.
	// > Example: `http://localhost:8080/api`.com/v1/events`
	Address string `json:"address" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Timeout for the HTTP request.
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
	// > Force HTTP/2 for the request.
	ForceHTTP2 bool `json:"force_http2" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Custom headers to add to the HTTP request.
	Headers map[string]string `json:"headers"` // *

	// > @3@4@5@6
	// >
	// > Number of retry attempts for failed HTTP requests.
	// > Uses exponential backoff strategy between retries.
	// > If all retries fail, the event is passed through without being sent.
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > Initial interval for exponential backoff between retries.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retry interval.
	// > Each retry interval will be multiplied by this value.
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > List of HTTP status codes that are considered successful.
	SuccessCodes []int `json:"success_codes" default:"200"` // *

	// > @3@4@5@6
	// >
	// > Path or content of a PEM-encoded CA file.
	CACert string `json:"ca_cert"` // *

	// > @3@4@5@6
	// >
	// > Prefix added to metric names for better organization.
	// > Useful when running multiple instances to avoid metric name collisions.
	// > Leave empty for default metric naming.
	MetricPrefix string `json:"metric_prefix" default:""` // *
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

	addr, isTLS := getAddrFromURL(p.config.Address)
	p.client = &fasthttp.HostClient{
		Addr:         addr,
		ReadTimeout:  p.config.Timeout_,
		WriteTimeout: p.config.Timeout_,
		IsTLS:        isTLS,
	}

	if p.config.ForceHTTP2 {
		if err := http2.ConfigureClient(p.client, http2.ClientOpts{}); err != nil {
			p.logger.Warn("Server does not support HTTP/2",
				zap.String("address", p.client.Addr),
				zap.Error(err),
			)
		}
	}

	if p.config.CACert != "" {
		tlsBuilder := xtls.NewConfigBuilder()
		if err := tlsBuilder.AppendCARoot(p.config.CACert); err != nil {
			p.logger.Fatal("can't append CA root", zap.Error(err))
		}
		p.client.TLSConfig = tlsBuilder.Build()
	}

	p.registerMetrics(params.MetricCtl, p.config.MetricPrefix)

	p.backoffStrategy = p.newBackoffStrategy()

	// Pre-parse field selectors for params
	p.paramFields = make(map[string][]string, len(p.config.Params))
	for name, fieldSelector := range p.config.Params {
		p.paramFields[name] = cfg.ParseFieldSelector(fieldSelector)
	}

	// Compile URL template
	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"default": func(defaultValue string, value interface{}) interface{} {
			if value == nil || value == "" {
				return defaultValue
			}
			return value
		},
	}).Parse(p.config.Address))
	p.addressTmpl = tmpl
}

func getAddrFromURL(rawURL string) (string, bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}

	host := u.Hostname()
	port := u.Port()

	isTLS := u.Scheme == "https"

	if port == "" {
		if isTLS {
			port = "443"
		} else {
			port = "80"
		}
	}

	return host + ":" + port, isTLS
}

func (p *Plugin) Stop() {
	if p.client != nil {
		p.client.CloseIdleConnections()
	}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl, prefix string) {
	if ctl == nil {
		return
	}

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

	// Build the address using template execution
	var buf bytes.Buffer
	if err := p.addressTmpl.Execute(&buf, params); err != nil {
		p.logger.Fatal("failed to execute URL template",
			zap.String("address", p.config.Address),
			zap.Error(err),
		)
	}
	address := buf.String()

	// Measure total request time including retries
	startTime := time.Now()

	// Set up backoff for retries
	operation := func() error {
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)

		req.SetRequestURI(address)

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

	err := backoff.Retry(operation, p.backoffStrategy)
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

func (p *Plugin) newBackoffStrategy() backoff.BackOff {
	expBackoff := backoff.ExponentialBackOff{
		InitialInterval:     p.config.Retention_,
		Multiplier:          float64(p.config.RetentionExponentMultiplier),
		RandomizationFactor: 0.5,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	expBackoff.Reset()
	return backoff.WithMaxRetries(&expBackoff, uint64(p.config.Retry))
}
