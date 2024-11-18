package xhttp

import (
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
	GzipCompressionLevel string
	TLS                  *ClientTLSConfig
	KeepAlive            *ClientKeepAliveConfig
}

type Client struct {
	client               *fasthttp.Client
	endpoints            []*fasthttp.URI
	authHeader           string
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

	return &Client{
		client:               client,
		endpoints:            endpoints,
		authHeader:           cfg.AuthHeader,
		gzipCompressionLevel: parseGzipCompressionLevel(cfg.GzipCompressionLevel),
	}, nil
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

	var endpoint *fasthttp.URI
	if len(c.endpoints) == 1 {
		endpoint = c.endpoints[0]
	} else {
		endpoint = c.endpoints[rand.Int()%len(c.endpoints)]
	}

	c.prepareRequest(req, endpoint, method, contentType, body)

	if err := c.client.DoTimeout(req, resp, timeout); err != nil {
		return 0, fmt.Errorf("can't send request to %s: %w", endpoint.String(), err)
	}

	respContent := resp.Body()
	statusCode := resp.Header.StatusCode()

	if !(http.StatusOK <= statusCode && statusCode <= http.StatusAccepted) {
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
