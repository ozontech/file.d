package join_template

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

const contentPanics = `# ===next===
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
github.com/ozontech/insane-json.(*Node).unescapeField(0xc0005892c0)
	/Users/root/go/pkg/mod/github.com/ozontech/insane-json@v0.0.15/insane.go:1313 +0xf5
github.com/ozontech/insane-json.(*Node).Dig(0xc000589130, 0xc009895e70, 0x1, 0x1, 0xc0000497dd)
	/Users/root/go/pkg/mod/github.com/ozontech/insane-json@v0.0.15/insane.go:773 +0x1ba
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

# ===next===
panic: interface conversion: *card.CheckUserDeleteResponse is not protoreflect.ProtoMessage: missing method ProtoReflect

goroutine 1112 [running]:
example.com/platform/somlib/pkg/mw/grpc/callopts.unaryClientInterceptor.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0x1, 0xc002e471a0, ...)
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/callopts/interceptors.go:49 +0x205
google.golang.org/grpc.getChainUnaryInvoker.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0xc0007d14c0, {0x0, ...})
    /builds/.cache/go/pkg/mod/google.golang.org/grpc@v1.44.0/clientconn.go:360 +0x154
example.com/platform/somlib/pkg/mw/grpc/circuitbreaker.(*RTCircuitBreakerGroup).UnaryClientInterceptor.func1.1({0x18bda78, 0xc002e47080})
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/circuitbreaker/interceptors.go:23 +0x6b
example.com/platform/circuit/v3.(*Circuit).run(0xc002e449c0, {0x18bda78, 0xc002e47080}, 0xc0007d1710)
    /builds/.cache/go/pkg/mod/example.com/platform/circuit/v3@v3.1.2-0.20210115121924-4b7fb14c90d5/circuit.go:298 +0x2b7
example.com/platform/circuit/v3.(*Circuit).Execute(0xc002e449c0, {0x18bda78, 0xc002e47080}, 0x162a945, 0x1a)
    /builds/.cache/go/pkg/mod/example.com/platform/circuit/v3@v3.1.2-0.20210115121924-4b7fb14c90d5/circuit.go:235 +0x65
example.com/platform/somlib/pkg/mw/grpc/circuitbreaker.(*RTCircuitBreakerGroup).UnaryClientInterceptor.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0xc0008bcf00, 0xc002e470b0, ...)
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/circuitbreaker/interceptors.go:20 +0x18c
google.golang.org/grpc.getChainUnaryInvoker.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0x7fd128bcbfff, {0x0, ...})
    /builds/.cache/go/pkg/mod/google.golang.org/grpc@v1.44.0/clientconn.go:360 +0x154

# ===next===
panic: failed to create app

goroutine 1 [running]:
main.NewApp({{0x104dbf64c?, 0x14000180018?}, 0x1400018c008?}, {0x104d4b114?, 0x104ee8a68?})
		/Users/root/go/src/app/main.go:172 +0x68
main.initApp()
		/Users/root/go/src/app/main.go:198 +0x394
main.main()
		/Users/root/go/src/app/main.go:205 +0x20

# ===next===
panic: operation failed

goroutine 1 [running]:
main.__(...)
		/Users/root/go/src/app/main.go:209
main.___()
		/Users/root/go/src/app/main.go:203 +0xb4
main.main()
		/Users/root/go/src/app/main.go:217 +0x60

# ===next===
panic: process data failed
        panic: close failed

goroutine 18 [running]:
main.CloseChannel(...)
        /Users/root/go/src/app/main.go:214
main.RunTask.func1.1()
        /Users/root/go/src/app/main.go:226 +0x74
panic({0x102feb9c0, 0x102ffc8d0})
        /usr/local/go/src/runtime/panic.go:838 +0x204
main.ProcessData(...)
        /Users/root/go/src/app/main.go:218
main.RunTask.func1()
        /Users/root/go/src/app/main.go:231 +0x9c
created by main.RunTask
        /Users/root/go/src/app/main.go:223 +0x68

# ===next===
fatal error: unexpected signal during runtime execution
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x0]runtime stack:
runtime.throw({0xa41e95?, 0x43997b82d0?})
	/usr/lib/sdk/golang/src/runtime/panic.go:1047 +0x5d fp=0x7fffefd63c90 sp=0x7fffefd63c60 pc=0x4c28dd
runtime.sigpanic()
	/usr/lib/sdk/golang/src/runtime/signal_unix.go:819 +0x369 fp=0x7fffefd63ce0 sp=0x7fffefd63c90 pc=0x4d8309goroutine 1 [syscall, locked to thread]:
runtime.cgocall(0x8fc1e0, 0xc00012ef20)
`

const contentPanicsWithNilNodes = `# ===next===
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
NilNode:some message
NilNode:some message 2
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
NilNode:some other message
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
github.com/ozontech/insane-json.(*Node).unescapeField(0xc0005892c0)
	/Users/root/go/pkg/mod/github.com/ozontech/insane-json@v0.0.15/insane.go:1313 +0xf5
github.com/ozontech/insane-json.(*Node).Dig(0xc000589130, 0xc009895e70, 0x1, 0x1, 0xc0000497dd)
	/Users/root/go/pkg/mod/github.com/ozontech/insane-json@v0.0.15/insane.go:773 +0x1ba
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

# ===next===
panic: interface conversion: *card.CheckUserDeleteResponse is not protoreflect.ProtoMessage: missing method ProtoReflect

goroutine 1112 [running]:
example.com/platform/somlib/pkg/mw/grpc/callopts.unaryClientInterceptor.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0x1, 0xc002e471a0, ...)
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/callopts/interceptors.go:49 +0x205
google.golang.org/grpc.getChainUnaryInvoker.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0xc0007d14c0, {0x0, ...})
    /builds/.cache/go/pkg/mod/google.golang.org/grpc@v1.44.0/clientconn.go:360 +0x154
example.com/platform/somlib/pkg/mw/grpc/circuitbreaker.(*RTCircuitBreakerGroup).UnaryClientInterceptor.func1.1({0x18bda78, 0xc002e47080})
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/circuitbreaker/interceptors.go:23 +0x6b
example.com/platform/circuit/v3.(*Circuit).run(0xc002e449c0, {0x18bda78, 0xc002e47080}, 0xc0007d1710)
    /builds/.cache/go/pkg/mod/example.com/platform/circuit/v3@v3.1.2-0.20210115121924-4b7fb14c90d5/circuit.go:298 +0x2b7
example.com/platform/circuit/v3.(*Circuit).Execute(0xc002e449c0, {0x18bda78, 0xc002e47080}, 0x162a945, 0x1a)
    /builds/.cache/go/pkg/mod/example.com/platform/circuit/v3@v3.1.2-0.20210115121924-4b7fb14c90d5/circuit.go:235 +0x65
example.com/platform/somlib/pkg/mw/grpc/circuitbreaker.(*RTCircuitBreakerGroup).UnaryClientInterceptor.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0xc0008bcf00, 0xc002e470b0, ...)
    /builds/.cache/go/pkg/mod/example.com/platform/somlib@v1.12.4-alpha.3.0.20220525161738-c7c89a81036d/pkg/mw/grpc/circuitbreaker/interceptors.go:20 +0x18c
google.golang.org/grpc.getChainUnaryInvoker.func1({0x18bda78, 0xc002e47080}, {0x162a945, 0x1a}, {0x158e100, 0xc002e46e40}, {0x158d840, 0xc002e46e70}, 0x7fd128bcbfff, {0x0, ...})
    /builds/.cache/go/pkg/mod/google.golang.org/grpc@v1.44.0/clientconn.go:360 +0x154

# ===next===
fatal error: unexpected signal during runtime execution
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x0]runtime stack:
runtime.throw({0xa41e95?, 0x43997b82d0?})
	/usr/lib/sdk/golang/src/runtime/panic.go:1047 +0x5d fp=0x7fffefd63c90 sp=0x7fffefd63c60 pc=0x4c28dd
runtime.sigpanic()
	/usr/lib/sdk/golang/src/runtime/signal_unix.go:819 +0x369 fp=0x7fffefd63ce0 sp=0x7fffefd63c90 pc=0x4d8309goroutine 1 [syscall, locked to thread]:
NilNode:some message
runtime.cgocall(0x8fc1e0, 0xc00012ef20)
`

func TestSimpleJoin(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		expEvents  int32
		iterations int
	}{
		{
			name:       "should_ok_for_panics",
			content:    contentPanics,
			iterations: 100,
			expEvents:  17 * 100,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			format := `{"log":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if line == "" {
					continue
				}
				lines = append(lines, fmt.Sprintf(format, line))
			}

			config := test.NewConfig(&Config{
				Field:    "log",
				Template: "go_panic",
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				require.False(t, id != 0 && id >= e.SeqID)
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.Offset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i     = 0
				iters = 100
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				break
			}

			p.Stop()

			require.True(t, iters > i, "test timed out")
			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		})
	}
}

func TestJoinAfterNilNode(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		expEvents  int32
		iterations int
	}{
		{
			name:       "should_ok_for_panics",
			content:    contentPanicsWithNilNodes,
			iterations: 100,
			expEvents:  23 * 100,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			formatNode := `{"log":"%s\n"}`
			formatNilNode := `{"notlog":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "NilNode:") {
					lines = append(lines, fmt.Sprintf(formatNilNode, line))
					continue
				}
				lines = append(lines, fmt.Sprintf(formatNode, line))
			}

			config := test.NewConfig(&Config{
				Field:    "log",
				Template: "go_panic",
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				require.False(t, id != 0 && id >= e.SeqID)
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.Offset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i           = 0
				iters       = 100
				prevX       = int32(0)
				repeatCount = 0
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					if x == prevX {
						repeatCount++
					}
					prevX = x
					continue
				}
				break
			}

			p.Stop()

			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		})
	}
}
