package xhttp

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/ozontech/file.d/xtls"
	"github.com/valyala/fasthttp"
)

const gzipContentEncoding = "gzip"

type ClientTLSConfig struct {
	CACert             string
	InsecureSkipVerify bool
}

type ClientKeepAliveConfig struct {
	MaxConnDuration     time.Duration
	MaxIdleConnDuration time.Duration
}

type ClientConfig struct {
	Endpoints            []string
	ConnectionTimeout    time.Duration
	AuthHeader           string
	CustomHeaders        map[string]string
	GzipCompressionLevel string
	TLS                  *ClientTLSConfig
	KeepAlive            *ClientKeepAliveConfig
	BanPeriod            time.Duration
	ReconnectInterval    time.Duration
}

type Client struct {
	client               *fasthttp.Client
	endpoints            []*fasthttp.URI
	cb                   *circuitBreaker
	reconnectInterval    time.Duration
	authHeader           string
	customHeaders        map[string]string
	gzipCompressionLevel int
}

func NewClient(cfg *ClientConfig) (*Client, error) {
	client := &fasthttp.Client{
		ReadTimeout:  cfg.ConnectionTimeout,
		WriteTimeout: cfg.ConnectionTimeout,
	}

	if cfg.KeepAlive != nil {
		client.MaxConnDuration = cfg.KeepAlive.MaxConnDuration
		client.MaxIdleConnDuration = cfg.KeepAlive.MaxIdleConnDuration
	}

	if cfg.TLS != nil {
		b := xtls.NewConfigBuilder()
		if cfg.TLS.CACert != "" {
			err := b.AppendCARoot(cfg.TLS.CACert)
			if err != nil {
				return nil, fmt.Errorf("can't append CA root: %w", err)
			}
		}
		b.SetSkipVerify(cfg.TLS.InsecureSkipVerify)

		client.TLSConfig = b.Build()
	}

	endpoints, err := parseEndpoints(cfg.Endpoints)
	if err != nil {
		return nil, err
	}

	c := &Client{
		client:               client,
		endpoints:            endpoints,
		reconnectInterval:    cfg.ReconnectInterval,
		authHeader:           cfg.AuthHeader,
		customHeaders:        cfg.CustomHeaders,
		gzipCompressionLevel: parseGzipCompressionLevel(cfg.GzipCompressionLevel),
	}

	if cfg.BanPeriod > 0 {
		c.cb = newCircuitBreaker(endpoints, cfg.BanPeriod)
	}

	return c, nil
}

func (c *Client) Start(ctx context.Context) {
	if c.cb == nil {
		return
	}

	go c.cb.checkBannedEndpoints(ctx, c.reconnectInterval)
}

func (c *Client) DoTimeout(
	method, contentType string,
	body []byte,
	timeout time.Duration,
	processResponse func([]byte) error,
) (int, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	endpoint := c.getEndpoint()
	if endpoint == nil {
		return 0, fmt.Errorf("no available endpoints")
	}

	c.prepareRequest(req, endpoint, method, contentType, body)

	if err := c.client.DoTimeout(req, resp, timeout); err != nil {
		c.banEndpoint(endpoint)
		return 0, fmt.Errorf("can't send request to %s: %w", endpoint.String(), err)
	}

	respContent := resp.Body()
	statusCode := resp.Header.StatusCode()

	if !(http.StatusOK <= statusCode && statusCode <= http.StatusAccepted) {
		if shouldBanEndpoint(statusCode) {
			c.banEndpoint(endpoint)
		}
		return statusCode, fmt.Errorf("response status from %s isn't OK: status=%d, body=%s", endpoint.String(), statusCode, string(respContent))
	}

	if processResponse != nil {
		return statusCode, processResponse(respContent)
	}
	return statusCode, nil
}

func (c *Client) prepareRequest(req *fasthttp.Request, endpoint *fasthttp.URI, method, contentType string, body []byte) {
	req.SetURI(endpoint)
	req.Header.SetMethod(method)
	if contentType != "" {
		req.Header.SetContentType(contentType)
	}
	if c.authHeader != "" {
		req.Header.Set(fasthttp.HeaderAuthorization, c.authHeader)
	}

	for header, value := range c.customHeaders {
		req.Header.Set(header, value)
	}

	if c.gzipCompressionLevel != -1 {
		if _, err := fasthttp.WriteGzipLevel(req.BodyWriter(), body, c.gzipCompressionLevel); err != nil {
			req.SetBodyRaw(body)
		} else {
			req.Header.SetContentEncoding(gzipContentEncoding)
		}
	} else {
		req.SetBodyRaw(body)
	}
}

func parseEndpoints(endpoints []string) ([]*fasthttp.URI, error) {
	res := make([]*fasthttp.URI, 0, len(endpoints))
	for _, e := range endpoints {
		uri := &fasthttp.URI{}
		if err := uri.Parse(nil, []byte(e)); err != nil {
			return nil, fmt.Errorf("can't parse endpoint %s: %w", e, err)
		}
		res = append(res, uri)
	}
	return res, nil
}

func parseGzipCompressionLevel(level string) int {
	switch level {
	case "default":
		return fasthttp.CompressDefaultCompression
	case "no":
		return fasthttp.CompressNoCompression
	case "best-speed":
		return fasthttp.CompressBestSpeed
	case "best-compression":
		return fasthttp.CompressBestCompression
	case "huffman-only":
		return fasthttp.CompressHuffmanOnly
	default:
		return -1
	}
}

func (c *Client) getEndpoint() *fasthttp.URI {
	if c.cb != nil {
		return c.cb.getEndpoint()
	}

	switch len(c.endpoints) {
	case 0:
		return nil
	case 1:
		return c.endpoints[0]
	default:
		return c.endpoints[rand.Int()%len(c.endpoints)]
	}
}

func (c *Client) banEndpoint(endpoint *fasthttp.URI) {
	if c.cb != nil {
		c.cb.banEndpoint(endpoint)
	}
}

func shouldBanEndpoint(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}
