package parse_http

import (
	"encoding/base64"
	"net/url"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDo(t *testing.T) {
	sourceName := "http_"

	login := "login"
	ip := "127.0.0.1"

	vals := url.Values{}
	whiteListParam := "whiteListParam"
	whiteListValue := "whiteListValue"
	vals.Add(whiteListParam, whiteListValue)
	blackListParam := "blackListParam"
	blackListValue := "blackListValue"
	vals.Add(blackListParam, blackListValue)

	config := test.NewConfig(&Config{
		Field: "source_name",
		AllowedParams: []string{
			whiteListParam,
		},
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	infoStr := login + "_" + ip + "_" + base64.StdEncoding.EncodeToString([]byte(vals.Encode()))
	sourceName += base64.StdEncoding.EncodeToString([]byte(infoStr))

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, login, e.Root.Dig("login").AsString(), "wrong field value")
		assert.Equal(t, ip, e.Root.Dig("remote_ip").AsString(), "wrong field value")
		assert.Equal(t, whiteListValue, e.Root.Dig(whiteListParam).AsString(), "wrong field value")
		assert.Empty(t, e.Root.Dig(blackListParam).AsString(), "wrong field value")

		wg.Done()
	})

	input.In(0, sourceName, 0, []byte(`{"source_name":"`+sourceName+`"}`))

	wg.Wait()
	p.Stop()
}
