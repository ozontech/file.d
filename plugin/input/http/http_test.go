package http

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func getInputInfo() *pipeline.InputPluginInfo {
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "",
			Factory: nil,
			Config:  &Config{Address: "off"},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "",
		},
	}
}

func getInputInfoWithAddr() *pipeline.InputPluginInfo {
	config := &Config{Address: ":9200"}
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
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
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
{"a":"3"}
`)
	eventBuff := make([]byte, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
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
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
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
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
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

	eventBuff = input.processChunk(0, []byte(`{`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"a"`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`:`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"1"}`+"\n"), eventBuff)

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func BenchmarkHttpInputJson(b *testing.B) {
	const NumWorkers = 128
	const DocumentCount = 128 * 128 * 8

	json, err := ioutil.ReadFile("../../../testdata/json/light.json")
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
			_, _ = io.Copy(ioutil.Discard, resp.Body) // https://github.com/google/go-github/pull/317
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				panic(resp.Status)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p, _, output := test.NewPipelineMock(nil, "passive", "perf")

		p.SetInput(getInputInfoWithAddr())

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
