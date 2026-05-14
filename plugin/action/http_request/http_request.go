package http_request

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgrr/http2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/valyala/fasthttp"

	"go.uber.org/zap"
)

type Plugin struct {
	config *Config
	logger *zap.Logger
	client *fasthttp.HostClient
}

// ! config-params
// ^ config-params
type Config struct {
	Params map[string]string `json:"params"` // *

	Method string `json:"method" default:"POST" options:"POST|GET|PATCH"` // *

	Address string `json:"address" default:"" required:"true"` // *

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
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	// >
	// > There are situations when one of the brokers is disconnected and the client does not have time to
	// > update the metadata before all the remaining retries are finished. To avoid this situation,
	// > the client.ForceMetadataRefresh() function is used for some ProduceSync errors:
	// > - kerr.LeaderNotAvailable - There is no leader for this topic-partition as we are in the middle of a leadership election.
	// > - kerr.NotLeaderForPartition - This server is not the leader for that topic-partition.
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > List of HTTP status codes that are considered successful.
	SuccessCodes []int `json:"success_codes"` // *

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

	p.client = &fasthttp.HostClient{
		Addr:         getAddrFromURL(p.config.Address),
		ReadTimeout:  p.config.Timeout_,
		WriteTimeout: p.config.Timeout_,
		IsTLS:        isURLTLS(p.config.Address),
	}

	if p.config.ForceHTTP2 {
		if err := http2.ConfigureClient(p.client, http2.ClientOpts{}); err != nil {
			log.Printf("Server %s does not support HTTP/2: %v\n", p.client.Addr, err)
		}
	}
}

func isURLTLS(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme == "https"
}

func getAddrFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	host := u.Hostname()
	port := u.Port()

	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	return host + ":" + port
}

func isSuccessStatusCode(statusCode int, successCodes []int) bool {
	for _, code := range successCodes {
		if statusCode == code {
			return true
		}
	}
	return false
}

func (p *Plugin) Stop() {

}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// Extract parameter values from the event using field selectors.
	params := make(map[string]string, len(p.config.Params))
	for name, fieldSelector := range p.config.Params {
		fields := cfg.ParseFieldSelector(fieldSelector)
		value := event.Root.Dig(fields...).AsString()
		params[name] = value
	}

	// Build the address by replacing {param_name} placeholders.
	address := p.config.Address
	usedParams := make(map[string]bool)
	for name, value := range params {
		address = strings.ReplaceAll(address, "{"+name+"}", value)
		// Mark param as used if placeholder was replaced
		if strings.Contains(p.config.Address, "{"+name+"}") {
			usedParams[name] = true
		}
	}

	// Encode the event as the request body.
	body := []byte(event.Root.EncodeToString())

	// Set up backoff for retries
	backoffStrategy := p.getBackoffStrategy()

	operation := func() error {
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)

		req.SetRequestURI(address)

		// Add unused params as query string parameters.
		for name, value := range params {
			if !usedParams[name] {
				req.URI().QueryArgs().Add(name, value)
			}
		}
		req.Header.SetMethod(p.config.Method)
		req.Header.SetContentType(p.config.ContentType)

		// Add custom headers from config.
		for key, value := range p.config.Headers {
			req.Header.Set(key, value)
		}
		req.SetBodyRaw(body)

		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		if err := p.client.DoTimeout(req, resp, p.config.Timeout_); err != nil {
			p.logger.Error("http request failed",
				zap.String("address", address),
				zap.String("method", p.config.Method),
				zap.Error(err),
			)
			return err
		}

		statusCode := resp.Header.StatusCode()
		if !isSuccessStatusCode(statusCode, p.config.SuccessCodes) {
			err := fmt.Errorf("non-success status code: %d", statusCode)
			p.logger.Error("http request returned non-success status code",
				zap.String("address", address),
				zap.String("method", p.config.Method),
				zap.Int("status_code", statusCode),
				zap.ByteString("response", resp.Body()),
			)
			return err
		}

		// Write the response body to the configured response_field.
		if p.config.ResponseField != "" {
			event.Root.AddFieldNoAlloc(event.Root, p.config.ResponseField).MutateToBytesCopy(event.Root, resp.Body())
		}

		return nil
	}

	err := backoff.Retry(operation, backoffStrategy)
	if err != nil {
		p.logger.Error("http request failed after retries",
			zap.String("address", address),
			zap.String("method", p.config.Method),
			zap.Error(err),
		)
	}

	return pipeline.ActionPass
}

func (p *Plugin) getBackoffStrategy() backoff.BackOff {
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
