package http

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
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
	test.NewConfig(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
	config.Meta = cfg.MetaTemplates{
		"remote_addr": "{{ .remote_addr }}",
		"method":      `{{ .request.Method }}`,
		"login":       "{{ .login }}",
	}
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
	p.SetInput(getInputInfo(&Config{Address: ":0"}))
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
	eventBuff = input.processChunk(0, chunk, eventBuff, true, nil)

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
	p.SetInput(getInputInfo(&Config{Address: ":0"}))
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
	eventBuff = input.processChunk(0, chunk, eventBuff, false, nil)

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
	p.SetInput(getInputInfo(&Config{Address: ":0"}))
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
	eventBuff = input.processChunk(0, chunk, eventBuff, false, nil)

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
	p.SetInput(getInputInfo(&Config{Address: ":0"}))
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

	eventBuff = input.processChunk(0, []byte(`{`), eventBuff, false, nil)
	eventBuff = input.processChunk(0, []byte(`"a"`), eventBuff, false, nil)
	eventBuff = input.processChunk(0, []byte(`:`), eventBuff, false, nil)
	eventBuff = input.processChunk(0, []byte(`"1"}`), eventBuff, true, nil)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestServeChunks(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	input := getInputInfo(&Config{Address: ":0"})
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
	input.Plugin.(*Plugin).ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/logger", strings.NewReader(`{"a":"1"}`)))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	resp = httptest.NewRecorder()
	input.Plugin.(*Plugin).ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/logger", strings.NewReader(`{"b":"2"}`+"\n")))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	wg.Wait()
	p.Stop()

	require.True(t, compareResultMaps(
		map[string]string{
			"a": "1",
			"b": "2",
		},
		outEvents,
	))
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
	input := getInputInfo(&Config{Address: ":0"})
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
		input.Plugin.(*Plugin).ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/_bulk", reader))
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

	require.True(t, compareResultMaps(
		map[string]string{
			"hello": "world",
			"next":  "ok",
		},
		outEvents,
	))
}

func TestServeChunksContinue(t *testing.T) {
	t.Parallel()
	p, _, output := test.NewPipelineMock(nil, "passive")
	input := getInputInfo(&Config{Address: ":0"})
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
	value := strings.Repeat("a", cap(body))
	body = append(body, `{"a":"`...)
	body = append(body, value...)
	body = append(body, `"}`...)

	resp := httptest.NewRecorder()
	input.Plugin.(*Plugin).ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/logger", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, resp.Result().StatusCode)

	wg.Wait()
	p.Stop()

	require.True(t, compareResultMaps(
		map[string]string{
			"a": value,
		},
		outEvents,
	))
}

func compareResultMaps(expectedValues map[string]string, outEvents []string) bool {
	result := make([]map[string]string, 0, len(outEvents))
	for i := range outEvents {
		var value map[string]string
		json.Unmarshal([]byte(outEvents[i]), &value)
		result = append(result, value)
	}

	expectedResult := make([]map[string]string, 0, len(expectedValues))
	for field, value := range expectedValues {
		expectedResult = append(expectedResult, map[string]string{
			field:         value,
			"method":      "POST",
			"remote_addr": "192.0.2.1",
			"login":       "",
		})
	}

	resultSet := make(map[string]struct{})
	for _, m := range result {
		jsonValue, _ := json.Marshal(m)
		resultSet[string(jsonValue)] = struct{}{}
	}

	expectedResultSet := make(map[string]struct{})
	for _, m := range expectedResult {
		jsonValue, _ := json.Marshal(m)
		expectedResultSet[string(jsonValue)] = struct{}{}
	}

	return reflect.DeepEqual(resultSet, expectedResultSet)
}

func TestPluginAuth(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	newReq := func(authHeader string) *http.Request {
		return &http.Request{
			Header: map[string][]string{
				"Log-Authorization": {authHeader},
			},
		}
	}
	encBasic := func(s string) string {
		return base64.StdEncoding.EncodeToString([]byte(s))
	}

	tests := []struct {
		Name       string
		Strategy   string
		Request    *http.Request
		Secrets    map[string]string
		ShouldPass bool
		Login      string
	}{
		{
			Name:       "disabled ok",
			Strategy:   "disabled",
			ShouldPass: true,
			Request:    &http.Request{},
		},
		// basic test
		{
			Name:     "basic ok",
			Strategy: "basic",
			Secrets: map[string]string{
				"ozon": "zonzon",
			},
			Request:    newReq("Basic " + encBasic("ozon:zonzon")),
			ShouldPass: true,
			Login:      "ozon",
		},
		{
			Name:     "basic reject",
			Strategy: "basic",
			Secrets: map[string]string{
				"ozon": "zonzon",
			},
			Request:    newReq("Basic " + encBasic("user:password")),
			ShouldPass: false,
		},
		// bearer test
		{
			Name:       "bearer reject",
			Strategy:   "bearer",
			Secrets:    map[string]string{},
			Request:    newReq("Bearer test"),
			ShouldPass: false,
		},
		{
			Name:     "bearer ok",
			Strategy: "bearer",
			Secrets: map[string]string{
				"ozon": "ozon",
			},
			Request:    newReq("Bearer ozon"),
			ShouldPass: true,
			Login:      "ozon",
		},
		{
			Name:     "bearer pass one char",
			Strategy: "bearer",
			Secrets: map[string]string{
				"1": "1",
			},
			Request:    newReq("Bearer 1"),
			ShouldPass: true,
			Login:      "1",
		},
		{
			Name:       "empty bearer",
			Strategy:   "bearer",
			Secrets:    map[string]string{},
			Request:    newReq("Bearer"),
			ShouldPass: false,
		},
	}

	pipelineMock, _, _ := test.NewPipelineMock(nil, "passive")

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			conf := &Config{
				Auth: AuthConfig{
					Strategy: tc.Strategy,
					Header:   "Log-Authorization",
					Secrets:  tc.Secrets,
				},
				Address: "off",
			}
			inputInfo := getInputInfo(conf)
			pipelineMock.SetInput(inputInfo)
			// init http plugin
			pipelineMock.Start()
			pipelineMock.Stop()

			ok, login := inputInfo.Plugin.(*Plugin).auth(tc.Request)

			r.Equal(tc.ShouldPass, ok)
			r.Equal(tc.Login, login)
		})
	}
}

func BenchmarkHttpInputJson(b *testing.B) {
	const NumWorkers = 128
	const DocumentCount = 128 * 128 * 8

	inputJson, err := os.ReadFile("../../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	var worker = func(jobs <-chan struct{}) {
		for range jobs {
			body := bytes.NewReader(inputJson)
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

		p.SetInput(getInputInfo(&Config{Address: ":0"}))

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

func TestGzip(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	gzipHeaders := http.Header{"Content-Encoding": []string{"gzip"}}

	newReq := func(rawUrl, method string, body string, headers http.Header, useGzip bool) *http.Request {
		bodyReader := io.Reader(strings.NewReader(body))
		if useGzip {
			buf := new(bytes.Buffer)

			gzw := gzip.NewWriter(buf)
			_, _ = gzw.Write([]byte(body))
			r.NoError(gzw.Close())

			bodyReader = buf
		}

		u, err := url.Parse(rawUrl)
		r.NoError(err)

		return &http.Request{
			Header: headers,
			Body:   io.NopCloser(bodyReader),
			URL:    u,
			Method: method,
		}
	}

	tests := []struct {
		Name               string
		Request            *http.Request
		ExpectedStatusCode int
		ExpectedBody       string
		ExpectedEvents     int
	}{
		{
			Name:               "process bulk",
			Request:            newReq("_bulk", http.MethodPost, strings.Repeat(`{"ping": "pong"}`+"\n", 10), nil, false),
			ExpectedStatusCode: http.StatusOK,
			ExpectedBody:       string(result),
			ExpectedEvents:     10,
		},
		{
			Name:               "process gzipped bulk",
			Request:            newReq("_bulk", http.MethodPost, strings.Repeat(`{"ok": "google"}`+"\n", 10), gzipHeaders, true),
			ExpectedStatusCode: http.StatusOK,
			ExpectedBody:       string(result),
			ExpectedEvents:     10,
		},
		{
			Name:               "discards invalid requests",
			Request:            newReq("_bulk", http.MethodPost, `{"ok": "google"}`, gzipHeaders, false),
			ExpectedStatusCode: http.StatusBadRequest,
			ExpectedEvents:     0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			pipelineMock, _, output := test.NewPipelineMock(nil, "passive")

			conf := &Config{Address: "off"}
			inputInfo := getInputInfo(conf)

			pipelineMock.SetInput(inputInfo)
			// init http plugin
			pipelineMock.Start()

			wg := sync.WaitGroup{}
			wg.Add(tc.ExpectedEvents)
			cnt := 0
			output.SetOutFn(func(event *pipeline.Event) {
				cnt++
				wg.Done()
			})

			rec := httptest.NewRecorder()

			inputInfo.Plugin.(*Plugin).ServeHTTP(rec, tc.Request)

			r.Equal(tc.ExpectedStatusCode, rec.Code)
			if tc.ExpectedBody != "" {
				r.Equal(tc.ExpectedBody, rec.Body.String())
			}

			wg.Wait()
			pipelineMock.Stop()
		})
	}
}
