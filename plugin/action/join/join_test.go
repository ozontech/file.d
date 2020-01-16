package join

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"gitlab.ozon.ru/sre/file-d/test"
	"go.uber.org/atomic"
)

const content = `# ===next===
panic: assignment to entry in nil map

goroutine 1 [running]:
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).getGeoRules(0xc420438780, 0xef36b8, 0xc42bb7e600, 0xc42bb77ce0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:69 +0x538
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createAddressIndex(0xc420438780, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:166 +0x5e
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createIndexes(0xc420438780, 0x0, 0xc44ec607d0)
	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:211 +0x8a
example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).updateDbCache(0xc420438780, 0xc420438780, 0xc4200845c0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:84 +0x182
example.com/tariffication/tarifficatorGoApi/services/cache.NewCache(0xc4200985f0, 0xc4203fdad0, 0xc420084440, 0x0, 0x0, 0x0)
	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:66 +0xa7
main.initialize(0xec3270, 0x1d, 0xee7133, 0x9e, 0xec53d7, 0x1f, 0xc40000000a, 0x14, 0xc420086e00)
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:41 +0x389
main.main()
	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:65 +0x2ae
ISN'T PANIC!
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
# ===next===
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x1067776]

goroutine 84 [running]:
sync.(*WaitGroup).state(...)
	/usr/local/Cellar/go/1.12.4/libexec/src/sync/waitgroup.go:33
sync.(*WaitGroup).Add(0x0, 0x1)
	/usr/local/Cellar/go/1.12.4/libexec/src/sync/waitgroup.go:54 +0x26
example.com/sre/filed/plugin/input/file.(*jobProvider).tryResumeJob(0xc000fa6000, 0xc0013f6000, 0xc0013de1c0, 0x69, 0xc0013f2ca8)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:366 +0x192
example.com/sre/filed/plugin/input/file.(*jobProvider).addJob(0xc000fa6000, 0xc0000bc020, 0x1634300, 0xc0000c8820, 0x18bf610, 0xc0013de1c0, 0x69, 0x0, 0x0)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:283 +0x3a4
example.com/sre/filed/plugin/input/file.(*jobProvider).actualizeFile(0xc000fa6000, 0x1634300, 0xc0000c8820, 0xc0013de1c0, 0x69, 0x0, 0x0)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:212 +0x2a8
example.com/sre/filed/plugin/input/file.(*jobProvider).actualize(0xc000fa6000, 0xc0013de1c0, 0x69, 0x1634300, 0xc0000c8820)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:191 +0x143
example.com/sre/filed/plugin/input/file.(*watcher).notify(0xc0001523c0, 0xc000dc5f48)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/watcher.go:100 +0x155
example.com/sre/filed/plugin/input/file.(*watcher).watch(0xc0001523c0)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/watcher.go:115 +0x147
created by example.com/sre/filed/plugin/input/file.(*watcher).start
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/watcher.go:40 +0x1a2

# ===next===
panic: done jobs counter more than job count

goroutine 227 [running]:
go.uber.org/zap/zapcore.(*CheckedEntry).Write(0xc000133d90, 0x0, 0x0, 0x0)
	/Users/root/go/pkg/mod/go.uber.org/zap@v1.9.1/zapcore/entry.go:229 +0x546
go.uber.org/zap.(*SugaredLogger).log(0xc00000e0d0, 0x15a4604, 0x152b2a9, 0x25, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0)
	/Users/root/go/pkg/mod/go.uber.org/zap@v1.9.1/sugar.go:234 +0x101
go.uber.org/zap.(*SugaredLogger).Panicf(...)
	/Users/root/go/pkg/mod/go.uber.org/zap@v1.9.1/sugar.go:159
example.com/sre/filed/logger.Panicf(...)
	/Users/root/go/src/example.com/sre/filed/logger/logger.go:74
example.com/sre/filed/plugin/input/file.(*jobProvider).doneJob(0xc006142000, 0xc004d35c70)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:366 +0x110
example.com/sre/filed/plugin/input/file.(*jobProvider).releaseJob(0xc006142000, 0xc004d35c70, 0xc000bdf101)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/job_provider.go:329 +0x48
example.com/sre/filed/plugin/input/file.(*worker).work(0x22ff260, 0x1708160, 0xc000108dc0, 0xc006142000, 0x20000)
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/worker.go:126 +0x52f
created by example.com/sre/filed/plugin/input/file.(*worker).start
	/Users/root/go/src/example.com/sre/filed/plugin/input/file/worker.go:14 +0x67

# ===next===
panic: runtime error: slice bounds out of range

goroutine 227 [running]:
github.com/vitkovskii/insane-json.(*Node).unescapeField(0xc0005892c0)
	/Users/root/go/pkg/mod/github.com/vitkovskii/insane-json@v0.0.15/insane.go:1313 +0xf5
github.com/vitkovskii/insane-json.(*Node).Dig(0xc000589130, 0xc009895e70, 0x1, 0x1, 0xc0000497dd)
	/Users/root/go/pkg/mod/github.com/vitkovskii/insane-json@v0.0.15/insane.go:773 +0x1ba
example.com/sre/filed/pipeline.(*Pipeline).countEvent(0xc0002afa40, 0xc0002e99e0, 0x2, 0x150d768, 0x8, 0xc00987a580, 0x1, 0x4, 0xc00987a580, 0x4, ...)
	/Users/root/go/src/example.com/sre/filed/pipeline/pipeline.go:351 +0x154
example.com/sre/filed/pipeline.(*processor).countEvent(...)
	/Users/root/go/src/example.com/sre/filed/pipeline/processor.go:172
example.com/sre/filed/pipeline.(*processor).processEvent(0xc00052fb80, 0xc0002e99e0, 0xc0081b23c0, 0xc0081b23c0)
	/Users/root/go/src/example.com/sre/filed/pipeline/processor.go:125 +0xcc
example.com/sre/filed/pipeline.(*processor).process(0xc00052fb80, 0x7f89840e3178, 0xc005e06680)
	/Users/root/go/src/example.com/sre/filed/pipeline/processor.go:110 +0x56
created by example.com/sre/filed/pipeline.(*processor).start
	/Users/root/go/src/example.com/sre/filed/pipeline/processor.go:81 +0xe4
Isn't panic
`

func TestJoin(t *testing.T) {
	format := `{"log":"%s\n"}`
	content := strings.ReplaceAll(content, "# ===next===\n", "")
	lines := make([]string, 0, 0)
	for _, line := range strings.Split(content, "\n") {
		if line == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf(format, line))
	}

	panics := 12
	iterations := 100

	config := &Config{
		Field: "log",
		First: `/^(panic:)|(http: panic serving)/`,
		Next:  `/(^$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)/`,
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))

	wg := &sync.WaitGroup{}
	wg.Add(panics * iterations)

	inEvents := atomic.Int32{}
	input.SetInFn(func() {
		inEvents.Inc()
	})

	outEvents := atomic.Int32{}
	lastID := atomic.Uint64{}
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents.Inc()
		wg.Done()
		id := lastID.Swap(e.SeqID)
		if id != 0 && id >= e.SeqID {
			panic("wrong id")
		}
	})

	for i := 0; i < iterations; i++ {
		for m, line := range lines {
			input.In(0, "test.log", int64(i*10000+m), int64(len(line)), []byte(line))
		}
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, int32(panics*iterations), outEvents.Load(), "wrong out events count")
}
