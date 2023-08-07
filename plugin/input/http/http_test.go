package http

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getInputInfo(config *Config) *pipeline.InputPluginInfo {
	_ = cfg.Parse(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "http",
			Factory: nil,
			Config:  config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "http",
		},
	}
}

func TestProcessChunksMany(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo(&Config{}))
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}`)
	eventBuff := make([]byte, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff, true)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo(&Config{}))
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}`)
	eventBuff := make([]byte, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff, false)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo(&Config{}))
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := []byte(`{"a":`)
	eventBuff = input.processChunk(0, chunk, eventBuff, false)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo(&Config{}))
	input := p.GetInput().(*Plugin)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	eventBuff := []byte(``)

	eventBuff = input.processChunk(0, []byte(`{`), eventBuff, false)
	eventBuff = input.processChunk(0, []byte(`"a"`), eventBuff, false)
	eventBuff = input.processChunk(0, []byte(`:`), eventBuff, false)
	eventBuff = input.processChunk(0, []byte(`"1"}`), eventBuff, true)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestServeChunks(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	input := getInputInfo(&Config{})
	p.SetInput(input)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	resp := httptest.NewRecorder()
	input.Plugin.(*Plugin).serve(resp, httptest.NewRequest(http.MethodPost, "/logger", strings.NewReader(`{"a":"1"}`)))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	resp = httptest.NewRecorder()
	input.Plugin.(*Plugin).serve(resp, httptest.NewRequest(http.MethodPost, "/logger", strings.NewReader(`{"b":"2"}`+"\n")))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	wg.Wait()
	p.Stop()

	require.Equal(t, []string{`{"a":"1"}`, `{"b":"2"}`}, outEvents)
}

type PartialReader struct {
	body   []byte
	eof    bool
	m      *sync.Mutex
	offset int
}

func NewPartialReader(body []byte) *PartialReader {
	return &PartialReader{
		body: body,
		m:    &sync.Mutex{},
	}
}

func (c *PartialReader) Read(to []byte) (int, error) {
	c.m.Lock()
	defer c.m.Unlock()

	n := copy(to, c.body[c.offset:])
	c.offset += n

	if n == 0 && c.eof {
		return 0, io.EOF
	}

	return n, nil
}

func (c *PartialReader) AppendBody(body string, isLast bool) {
	c.m.Lock()
	defer c.m.Unlock()

	c.body = append(c.body, body...)
	c.eof = isLast
}

func (c *PartialReader) WaitRead() {
	for {
		c.m.Lock()
		done := len(c.body) == c.offset
		c.m.Unlock()

		if done {
			return
		}

		time.Sleep(time.Millisecond * 10)
	}
}

func TestServePartialRequest(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	input := getInputInfo(&Config{})
	p.SetInput(input)

	wg := sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	p.Start()

	resp := httptest.NewRecorder()
	reader := NewPartialReader([]byte(`{"hello":"`))

	doneCh := make(chan struct{})
	go func() {
		input.Plugin.(*Plugin).serve(resp, httptest.NewRequest(http.MethodPost, "/_bulk", reader))
		close(doneCh)
	}()

	reader.WaitRead()
	reader.AppendBody("world", false)
	reader.WaitRead()
	reader.AppendBody(`"}`+"\n", false)
	reader.WaitRead()
	reader.AppendBody(`{"next":"`, false)
	reader.WaitRead()
	reader.AppendBody(`ok"}`, true)

	<-doneCh

	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	wg.Wait()
	p.Stop()

	require.Equal(t, []string{`{"hello":"world"}`, `{"next":"ok"}`}, outEvents)
}

func TestServeChunksContinue(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	input := getInputInfo(&Config{})
	p.SetInput(input)
	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]string, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	body := make([]byte, 0, readBufDefaultLen*2)
	body = append(body, `{"a":"`...)
	body = append(body, strings.Repeat("a", cap(body))...)
	body = append(body, `"}`...)

	resp := httptest.NewRecorder()
	input.Plugin.(*Plugin).serve(resp, httptest.NewRequest(http.MethodPost, "/logger", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	wg.Wait()
	p.Stop()

	require.Equal(t, []string{string(body)}, outEvents)
}

func TestPluginAuth(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	conf := &Config{}
	inputInfo := getInputInfo(conf)

	newReq := func(authHeader string) *http.Request {
		return &http.Request{
			Header: map[string][]string{
				"Authorization": {authHeader},
			},
		}
	}
	encBasic := func(s string) string {
		return base64.StdEncoding.EncodeToString([]byte(s))
	}

	tests := []struct {
		Name       string
		Strategy   AuthStrategy
		Request    *http.Request
		Tokens     map[string]struct{}
		Passwords  map[string]string
		ShouldPass bool
	}{
		{
			Name:       "disabled ok",
			Strategy:   StrategyDisabled,
			ShouldPass: true,
			Request:    &http.Request{},
		},
		// basic test
		{
			Name:     "basic ok",
			Strategy: StrategyBasic,
			Passwords: map[string]string{
				"ozon": "zonzon",
			},
			Request:    newReq("Basic " + encBasic("ozon:zonzon")),
			ShouldPass: true,
		},
		{
			Name:     "basic reject",
			Strategy: StrategyBasic,
			Passwords: map[string]string{
				"ozon": "zonzon",
			},
			Request:    newReq("Basic " + encBasic("user:password")),
			ShouldPass: false,
		},
		// bearer test
		{
			Name:       "bearer reject",
			Strategy:   StrategyBearer,
			Tokens:     map[string]struct{}{},
			Request:    newReq("Bearer test"),
			ShouldPass: false,
		},
		{
			Name:     "bearer ok",
			Strategy: StrategyBearer,
			Tokens: map[string]struct{}{
				"ozon": {},
			},
			Request:    newReq("Bearer ozon"),
			ShouldPass: true,
		},
		{
			Name:     "bearer pass one char",
			Strategy: StrategyBearer,
			Tokens: map[string]struct{}{
				"1": {},
			},
			Request:    newReq("Bearer 1"),
			ShouldPass: true,
		},
		{
			Name:       "empty bearer",
			Strategy:   StrategyBearer,
			Tokens:     map[string]struct{}{},
			Request:    newReq("Bearer"),
			ShouldPass: false,
		},
	}

	plugin := inputInfo.Plugin.(*Plugin)
	plugin.config = conf
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			conf.Auth.Strategy_ = tc.Strategy
			conf.Auth.Passwords = tc.Passwords
			plugin.uniqBearerTokens = tc.Tokens

			ok := plugin.auth(tc.Request)
			r.Equal(tc.ShouldPass, ok)
		})
	}
}

func BenchmarkHttpInputJson(b *testing.B) {
	const NumWorkers = 128
	const DocumentCount = 128 * 128 * 8

	json, err := os.ReadFile("../../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	var worker = func(jobs <-chan struct{}) {
		for range jobs {
			body := bytes.NewReader(json)
			req, err := http.NewRequest(http.MethodPost, "http://localhost:9200", body)
			if err != nil {
				panic(err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body) // https://github.com/google/go-github/pull/317
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				panic(resp.Status)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p, _, output := test.NewPipelineMock(nil, "passive", "perf")

		p.SetInput(getInputInfo(&Config{Address: "127.0.0.1:9200"}))

		wg := &sync.WaitGroup{}
		wg.Add(DocumentCount * 2) // 2 rows in each file

		output.SetOutFn(func(event *pipeline.Event) {
			wg.Done()
		})

		jobs := make(chan struct{})
		for w := 1; w <= NumWorkers; w++ {
			go worker(jobs)
		}

		p.Start()
		time.Sleep(100 * time.Millisecond) // http listen start delay

		go func() {
			for j := 0; j < DocumentCount; j++ {
				jobs <- struct{}{}
			}
			close(jobs)
		}()

		wg.Wait()

		p.Stop()
	}
}
