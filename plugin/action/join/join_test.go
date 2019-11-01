package join

import (
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

const content = `# ===next===
panic: assignment to entry in nil map

goroutine 1 [running]:
gitlab.ozon.ru/tariffication/tarifficatorGoApi/services/cache.(*Cache).getGeoRules(0xc420438780, 0xef36b8, 0xc42bb7e600, 0xc42bb77ce0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:69 +0x538
gitlab.ozon.ru/tariffication/tarifficatorGoApi/services/cache.(*Cache).createAddressIndex(0xc420438780, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:166 +0x5e
gitlab.ozon.ru/tariffication/tarifficatorGoApi/services/cache.(*Cache).createIndexes(0xc420438780, 0x0, 0xc44ec607d0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:211 +0x8a
gitlab.ozon.ru/tariffication/tarifficatorGoApi/services/cache.(*Cache).updateDbCache(0xc420438780, 0xc420438780, 0xc4200845c0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:84 +0x182
gitlab.ozon.ru/tariffication/tarifficatorGoApi/services/cache.NewCache(0xc4200985f0, 0xc4203fdad0, 0xc420084440, 0x0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:66 +0xa7
main.initialize(0xec3270, 0x1d, 0xee7133, 0x9e, 0xec53d7, 0x1f, 0xc40000000a, 0x14, 0xc420086e00)
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:41 +0x389
main.main()
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:65 +0x2ae
# ===next===
panic: repeated read on failed websocket connection

goroutine 82 [running]:
github.com/gorilla/websocket.(*Conn).NextReader(0xc0001166e0, 0xffffffffffffffff, 0x0, 0xc0006d7ec8, 0xc0006d7ec8, 0x40c528)
	/go/pkg/mod/github.com/gorilla/websocket@v1.4.0/conn.go:967 +0x356
github.com/gorilla/websocket.(*Conn).ReadJSON(0xc0001166e0, 0xb43880, 0xc000266ba0, 0xcf5260, 0x129a8c0)
	/go/pkg/mod/github.com/gorilla/websocket@v1.4.0/json.go:50 +0x2f
github.com/nlopes/slack.(*RTM).receiveIncomingEvent(0xc000119040)
	/go/pkg/mod/github.com/nlopes/slack@v0.2.0/websocket_managed_conn.go:288 +0xa0
github.com/nlopes/slack.(*RTM).handleIncomingEvents(0xc000119040, 0xc000f4a720)
	/go/pkg/mod/github.com/nlopes/slack@v0.2.0/websocket_managed_conn.go:223 +0x2d
created by github.com/nlopes/slack.(*RTM).ManageConnection
	/go/pkg/mod/github.com/nlopes/slack@v0.2.0/websocket_managed_conn.go:54 +0x229
panic(0xb6afc0, 0xd7c240)
	/usr/local/go/src/runtime/panic.go:513 +0x1b9
github.com/go-chi/chi.(*Mux).ServeHTTP(0xc000080c00, 0xd89760, 0xc0000dcb60, 0xc000c13900)
	/builds/rp/product-review-service/.cache/go/pkg/mod/github.com/go-chi/chi@v0.0.0-20171222161133-e83ac2304db3/mux.go:63 +0x4c9
net/http.serverHandler.ServeHTTP(0xc0003d05b0, 0xd89760, 0xc0000dcb60, 0xc000c13900)
	/usr/local/go/src/net/http/server.go:2741 +0xab
net/http.(*conn).serve(0xc00032cfa0, 0xd8a3a0, 0xc000c7a300)
	/usr/local/go/src/net/http/server.go:1847 +0x646
created by net/http.(*Server).Serve
	/usr/local/go/src/net/http/server.go:2851 +0x2f5
# ===next===
2019/01/11 12:13:27 http: panic serving 10.27.35.113:44190: chi: attempting to route to a mux with no handlers.
goroutine 223517 [running]:
net/http.(*conn).serve.func1(0xc000a54dc0)
	/usr/local/go/src/net/http/server.go:1746 +0xd0
panic(0xb6afc0, 0xd7c240)
	/usr/local/go/src/runtime/panic.go:513 +0x1b9
github.com/go-chi/chi.(*Mux).ServeHTTP(0xc000080c00, 0xd89760, 0xc0006c8a80, 0xc000a5a600)
	/builds/rp/product-review-service/.cache/go/pkg/mod/github.com/go-chi/chi@v0.0.0-20171222161133-e83ac2304db3/mux.go:63 +0x4c9
net/http.serverHandler.ServeHTTP(0xc0003d05b0, 0xd89760, 0xc0006c8a80, 0xc000a5a600)
	/usr/local/go/src/net/http/server.go:2741 +0xab
net/http.(*conn).serve(0xc000a54dc0, 0xd8a3a0, 0xc0008565c0)
	/usr/local/go/src/net/http/server.go:1847 +0x646
created by net/http.(*Server).Serve
	/usr/local/go/src/net/http/server.go:2851 +0x2f5
Isn't panic
Isn't panic also
# ===next===
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x8 pc=0xad0a0c]

goroutine 1 [running]:
main.main()
	/builds/marketplace/go/product-parser/.gopath/src/product-parser/cmd/parser/parser.go:56 +0x26c
# ===next===
2019/01/15 23:47:22 http: panic serving 10.20.72.106:51028: chi: attempting to route to a mux with no handlers.
panic(0xd6e180, 0xfcdf90)
	/usr/local/go/src/runtime/panic.go:502 +0x229
`

func startPipeline(first, next string) (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	p := pipeline.NewTestPipeLine(false)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: fake.Config{}}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{Field: "log", First: first, Next: next}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}
//
//func TestFlatten(t *testing.T) {
//	format := `{"log":"%s"}`
//	content := strings.ReplaceAll(content, "# ===next===\n", "")
//	lines := make([]string, 0, 0)
//	for _, line := range strings.Split(content, "\n") {
//		lines = append(lines, fmt.Sprintf(format, line))
//	}
//
//	p, input, output := startPipeline("/panic: /", `/(^$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)/`)
//	defer p.Stop()
//
//	acceptedEvents := make([]*pipeline.Event, 0, 0)
//	input.SetAcceptFn(func(e *pipeline.Event) {
//		acceptedEvents = append(acceptedEvents, e)
//	})
//
//	dumpedEvents := make([]*pipeline.Event, 0, 0)
//	output.SetOutFn(func(e *pipeline.Event) {
//		dumpedEvents = append(dumpedEvents, e)
//	})
//
//	for _, line := range lines {
//		input.In(0, "test.log", 0, 0, []byte(line))
//	}
//	p.HandleEventFlowFinish(false)
//	p.WaitUntilDone(false)
//
//	assert.Equal(t, len(lines), len(acceptedEvents), "wrong accepted events count")
//	assert.Equal(t, 7, len(dumpedEvents), "wrong dumped events count")
//}
