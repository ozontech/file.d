package join_template

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
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

const randomLines = `
g%zTvTeD4&d0a4r3BW1*R^4S-OBO.80QBARGNuX;XX^CrnDdu$,#fAUm0aJosPi"8Kd#eJb]UU("+Q^X"VgfMJDIfGfQ&z0g8=iC8X,VN4u)Qn-Y]C#eP.?MEKqAK9\ksY5N%/DYLz4$x22zJEk?<KN^5^m<uLoygyOaD"@?;+
5-E;T4eqg/k;T*[(+TNLC(VGl'G(c4^Ih"GX(ac$@NROYQ([feF8$J,_'9.@/b0#F'D\[oC50e:!i);A0S;;4iFI<c;oKA=L7EuR$B[QD@h)*9
S!\7"Hk2']XkaCGNixxHvaaN+oW4q?ORRT#)_XY8.gH;z)M5 DrBx FA$Di*7;,0w*#xDy>A4(X%8.58.^?fY'RE@2B4/]'f$Eh%=@*V.Xa*e=GIZI"YOiD1lBQi*'ULmY6v[y_Lyn=C$e-10f-X+c7dY;hD'oM5_&xo4<lQ*F@w7D)]:)4i-!fL)%f2]Q@4Afg"B&ly5yT_3I/:a%)m?8;"]8] yeeC=#w:Tx2UM#pW3#8E^/*E2K'f?0xR[J^#@l,[,Xp*qnL0Out5LAM'OFn%2\![A/Ar>6+@_pQO:qS5X_/(Ig'WQnf7"4E^o"L,_,.E=6zth]!e?-:ZS9IHm(<W>;PVw#+J+R%8.#<x143IAs$\Re.i%5f73rqHAE,/@*o4"K:bYd(ybd:3NxOai_7a\;Wnh[8ey @\(%D"<XwcZtHbRa:^[Zb:'I^_JHp9DDwX$R&V_^,W6 Fn<MaDwab#QBR\+d8D5fMTer&z'<6QO#WYUrh?cv2+J<?,ed^EgOArWPTJ-$e6R\9'php\4CL4zctPC*LZC8WHfnz.E;a2Wh-o[Me2P3Z0dWp5OKDy"7#nf-@QUW$B< 27_ER'&9WPj_#$^kh4&Y(@[8)a95h*ulEruKSP88a#+'@Olc9Eq+Q#nj"&Lc&*WErAaZ@;[%;Ad_F5K
RW3axFQ_ ^9prTz]?jQ M.UC#-!iP4?]#5pJya]u)Se.*!_QhahPA,PMIS)P\inL0t#X=j49zHh6PI(d()<AenO.v=^BcDOr?)&ZoE@j>'(.DMCMeq?^T;9qyQZ6.eHVhD\WKltm>97<-w$Xm-YehbhiB0,P+"0!-Th.PdIW?x'DgRn <8bGb da"ZGeQ+&5b3Jyu6jq&6Bg3ehbMow 85U;M3UaBa[6cwv2aT8),+ ,@aail Yz!][L;6;VGC8585\MRl,Xzs"p<:CJ=_%hv-j^m (E2E^W+%&aHyl?DFQdR]EHXc9cZce!i'6s+DThfQY<]t)=4ri6V(t$v9Y4W)p%]:SkP=K\aa0T%eaPWw1U+ou.xhk1K.($t?Tr;cB.GJ/=hs#Y,iRp]-b/A;a7,US
jV3deRhf@A2Pe]@a)=yW%5-J6J [El]UmP# VH_=MHV3I^SNcwFj*91DLf8yOSH-dWR!d&GU4=PX)x_EvEBY1n[K7RouVA$ecW1 q.)7a2j-=G]K+nI;!b"[ALH$\-+!A"$OwAo3nXQDhhQ @]QpJBFj"Y$+CMX;B3BcGkvYoQ"to4?yenq&Yem5OD+c!'i_Q%Gz\pl1S-b)5<u0<5RiNWVR]3 Qu%_DhiQMT9H@ Yet67Ji.;CDkQTO6%)EeV 2aVHrGW;)Z)3,/=vXJ(C2-\GsmOYId*d0qA4]jz=*S%yy"M>(hmM6J"Ee?e;>5l-4[:gz,[mmab[56fJg0m4&IHC"oCHjQ$]2a">/)$N0xW kS%QPid1;J@Tm)UFL9Yk$f:F?ebAcm,9C"<Q9^<FaaHFB^X<_@%LY)5na5?A^e(QAQggDmJb%Q Xfi8;hcN#3^nHwhs?*wXG0>w+QX;Uu?^XliGbLbK.i5FA!SZa>y2:@ZF?606Vke*YZ:&SZ1UNjf5*9rE+QW[a5:H-$ec@tGQx7@fN>3cGH5]1H3VG/W_97,6#L#P0+PjP3aoC-:1;XeKn114xs@&bFl6t^/tw(>Iu
[31 IJydA^,#YK's5g![p6$.FGJMDlTcJ""WoTjAu$Y=N&c@zR8]Fhcaua1G>\DFYi"3h8S$=s<14$\Jp/g2WNOA+1wa'1.f8.p)sml/Q(y D]=\4u"CM$2!fOdz'SX Lgi=_QS"V[I=Sa^>uk+%G3Y]Z'FWv2\h1/E.#f,ld?4>!wWdU^4,@P9gIg4*1vAm@8a
#glhr!a>)-+F!T&$gI4ZQf4*ZY?UN[?KYO+AVa)]z#7BD)Aj$-s8?@Y fD;K!rD1PHI:5f^]JPKzL/R8SI_JU+u)/0!5_a*Pga=iMwzB<_2=pm:N4%?%9#I=7&9$i\:&!!>D"gyW/!say(&X(hJ_aU;\6*s\JE9(7u8eG;U\KO>WyaULgb)%4(lxFw_2^*x;i<MN-o]$MKKaWFb/o1vKCj:MPCn!$,BEjYPNY:*4B#np?Xf223u^BwT%HjK['dL=2R5UsZ["3/?0N33XJ'ad>Cw^t&]7e6^W_
aLD?NO 7c6)$N.L<L\RiYA#XcJi@3(ZBSc=#2#IADcal5y?+$Q %6Ut^hEqTfs3/pgMj7&yl7nKoyv >g8Ry@i,h)k:H0qY+c/rC!APt49R(XU0@/jV9Qa+faVBgMf>IUAAi6:_U0/2clTvevuvL#u_1[:d\YN\WK@O:b7yC0/M$1jE2079zq2n<9Lr"'#_Tbi(j4EU2*pfip)+ e@Zz80f1Koq$-cQ,K_wk3w(;5W1aZ=1M\L y0OX>_[,cM\?mysE* &+aJEHK37nZP@N 5/&0aD;<wVmer?3Q.>XN$_>a!0%82JnGI6<5srW9oJE!;U,GG^PT$-\+0gR&9gm [HGpsJAES8/(qW>Zi2QkP'N"ZtD3H7;a/Xu-<:*XbMB/.D^4Q ?;4)b+a_h)5Hxw?-2f"FSJ8E3x=@6i,-$AKNxDKX)hx%0sUrl/d*@+]2jil>'bAELXu3-Wb245f0v)934'4D]#@&RF?<5J WthOA+k5A;!Ap]P+aPh *s7B7XhaVGh+;LqnY1>)Bh\Th>!*;R%q&SvhU$?1W5@+a%5N(qA&-K^So-ZR#2PA'zfx9V#a]:2)oh=,9:S3K66P<=QT^ah8F%>aiI'C%t#Zj)TIOk#M)S^_=$s xbJC)4#%\-i1PT7L8-od]lTyOWl66/EKP8cu<+ kV?,8by^"2To)==KY0;WnH;)i4kWZ)Q?QV.YL;z;PDFU*_00%846RO"BFp1TCe^KSKL hX?D1fhva_&zK)+[?O8AQBfS!<Hb*Vx-;-;Hy%I*1hXP=iIyODGD%6O8W
"_'UI1%+6>h4"T4UCk:/gv1QmZf(wXD20G?4@2$%)avd^:0k[Ss?HAbB2H(rOSw[Mvj*omP$;@VHn,B_\b;zBJJJ?&gCA8;%g#wFv^ b>,V(8ok Of5%h-of+FZxW#K6a>;Z/*t-v7-=^xC/DEC9d9.loS?B/aa7?OL3 4EC#[*[4"lRrL0k"6[I?4>:2Va<&-R//0AWlGthPf!hwtX[WZ -Qfm@.wh12a+&M</I'\=vlrYfaAa.RW>l74i'n:S?L#)X>WJ8*B&JaFh)UYgbla8a!AP7du4-)Bw#I+O;z^I8 <]C@7^3(Z_(H>! H-yimVVVRK C8":=J7XTcd-T=q/pc,]J''VHaqP5SQeR\bSy#]j#Y-/8xN-]47d'e)4ia:uOuXSM6uY%dl,x6 cP8HL5Aa*V+"&VJC$:)nN#L@ed5Xj&c,$>QHK1]zLenq]r-Huj<E1<KKo<dXy9#ob ?HSbX>& 8X[O<9uEIok?St ,X9jl45cP%8a9l=u9VZi%"\47#GBf.BN,LbcK2(U,d4KDKCPC*T'050s*aX)DKP)4.Zd>G?h*-p)0Js37Ia'/T<;?M5A%JCTxDu-Ux
J - oTxJbaX7rpg<q'BB!K[?)l$qy-<NGkbhHbAap.Sq<aXFF\P[iPR7"]=YmaDHtL_p@]G*L$E;'?54EY_jeaLeg# BScK&< d=d;G;h<7J"oTfjnS5a@B M#d@KXDFS4+FhyL!d.Gl_KWLYxERa0t!-KN!a4W7P*dSaYT0t^d#&. 5y-4.I0Ea3 r\)u/<c*a,och\0C_%z[;Latv@i/+hg3fMh%)"kCXPiMHDdUnKS^(\f)7w419.+^%,%TwaSKUn3q)1EQQm!B[<kk'"tq#DL@gCs2Pg_i\<Cue0$+)UzSz(z)XYQK,&;<EV/Y:J$S.$WOn-ldDasK ^=;=]$f0C(W;a(K2!g%N1e9NNvgH&'+khi>?>DMg5'EPffqeR-aEC67>/d;291N-i-$yB;X-P3-b!BZh<qVnx8Tqu^e\ Pcf,+@cRKW=pw9E?q4l0IT[=gvf^=$1Cjx=*Y#EbIer'\7<8m-,1.19DSdY:uUG>5n4.,&]LeW#)/.+p/0^#u5a]>*xBjDd:BE@"!<k^WUX@n[[_=PQ
6;*
"WhWpO J*U]AFA15l(_axE?c<Y+;i]mJ,zt[bNnjOyEicfCu*+6I4[!_wP:\B.a%OY*?5Vi.R[zawVC5D7U8y^28rtpSt]8[Th+O\=b7+xGTJ((6 '\! F)(rHh"hJDQq<4FeC?14@0s=.a?>a]9WvR>GzvJMrhax8X$-8,^R%0=]nZ=L)5lt:oVsQyQDr:2\CtcC!qFllFFWH[kwEB d?3'aMOZ:OvaaV^q?!O 5]TaPP$Kf5V5Vm\J%Ea#b4Zl])c<fuzs\)Hr&Dn>>LEBBXe0UI/7Od_,T*e(KDX!il+@XzFdMtT/xGW>iOT.A'C[rYO L#.< hU!X??F!zdlXA(0m3g;>qQHeWL141%*2 ^!LXLoff?w7UXt n'xvgEJhMVzWawK<tXI0bf*[A&BkM'x)ye*1yo:rbebQ_J3CM+zWSNp/rP+fw0nN&Gi+_k%@"le :b2io&Z@n@_t5$4c-Q-IjCxOHySyH1j%IC"MxW%3+h,4Tfsa@gw(tB:]l!^8V%]:1*b-r2#[h@ZmCP3dEJ8k_IJ<B*aYHza)rhN[JA@QBg7uFZTv+/)?p#lshi_MpQL1'xYCSL="V)h*E
n"e;,AZC<^LataV*4T(!NgnMZ/bWWi;4'[rD'2j3FGQ?+E%okHVa-:<V7D"9LWeJ>%&gxRH0#SU=t/RXk$LP=]HG4xY%YX5+vtLi?U;'==A]m3MhZk?;P^wZ5de<+Ui;4SN# _eN.O7FNa>XD*LMTH(fI
_+1q2FGyh*!loKAxjQXu.80s":WF5c<c\-X^ciMl-nfJ-!n=a0TTS0c>!P6FKg0czNTvG>f#,hUY&Hq\TG :97&gE:*9v3B=&U,iX$[g-bi;haoQ\ Jh:.L'XNgb3$.S"u>$r-2u%(3IF%O8md'Git/)V!Cm^=:YhB3D6q9*UJZ@,1H6Ya2&F-*Ch025C$b8#i jlza")8Z9R(^$#aP8LPc\fKr:WJM@1tr/=RW<Z[OJ*TM1e^C%?i0,dMo'I:31%646NCeGV7cwj+*M+8KV.7a+sd.F[4O1&t=CE;-'g^#y5@aWU^+X[>2<6_eZ@[4J+q8_Qb4VGdBVJ"R%8i_xfr:cYI2!1Z&CeGAK##"^Rae6Z=5G23Aem\)xWXi_qb+<&,UBlZ%i<&*LA+x<Iz>>$c]5)cL%l4+Bqc*NIaK<; ll zL"Yj]$U54XCo\eR
.'X7@aFUSJ:/DYUa5RY95@d=/pKaa' E[e3,.LWg _A='\-m/pXcREHS@WY1J#6^4;fqNS V8<@e^(,wGT6a9lw/?,*dWS40gXI\w"cqalM$WU 6qRP]gYH;Zg>W+^f.&TNipXBL,M;8C'BO1x_cW?>g[EEUO PA+g-KSAWgc3]Y\der3nP7[0G!9dMzp?7\g/aYd6OL-0zw+*B-p^/-RGbzMAOBGPJQJ24O2VyB?08)QELQNl'2PTp*3B:U(z/JDif&SP+b^o@(QQWo2e/&Thk4"D^c=q02 LBh/M/9ZIeEAiXBGTU HPIz6YK)d#&XEa"'8_DTPXdZ0?P$,Jvv'b\ h+-T*FF4"Pr1_1G$Fc5C:p6spLj?QQ20/w"%^]n^b1ieKKM<Ge&cMJ$VUcj6X*En!)h^T95W?h6W9;o#Bb"&@F!XK/*YT9NdGM vZ3syB %aD6Vb(3,F%Hy22Y\7;ZCpHOxA3x1?MB'gaSq[@NTpsRT]IF+SPMA_JhVtEHt%n?9O12@n1HO8@=Yuurq='KiH/Za;BYm_6KJ*%_!42Gc=Sqy;eW,-@5h:n<t8HU_4+GB;waBC%3hX!Ng7C!Qq^gc>B^EYCuN[QCRqipjA:"OXfY%e@r!V!;"!?7>pp7'MKzY)K [0i6Hh'L*9aJIMYa(d1q]RbgbhNVmK]Y5+o7ZlmMucG6qWL5EHV.j&T!v*@7si?WG&(G%CaQ(0>L@^#<V2"G!K-'\^s&dR/!BDAI0[yPqmwdby3yxIJC]"#Tev$T@rQrcsAZd'Di:y4/Mm;gM&5/XZU!t=-J^N=5s'Yi=U?LNnF%K7i&8u[["oo"\FPQcaZV&YJ%nbC ']TGA9_).>58s\^u"a\
^l@KGi=L-03GK:3&w&(,,hj3i+!*;UgpO4TXkWvsDHl6[cA073Q2uYDFz*6w@e@A.:VLEH0*,Sh7N#<t,Af)D@yeJ\<_3JK/I'BTz&F%say_k0oC-BW_O*y9Fv.R7JEo/WeP1ta*J2]=Y#q_Re;.7fncL<Bi%fI:B 8*_*el5:#FBYi75@-h<0aQSe kiFI6WbLS$/a3>mf#a@Rti?a'HT/AWrjde$vxc1o)-V<#5@P;EAf*2FG6I K7/4in(Rql0]b%G?K;6sc[3k'c8Qh
_2.iP>FP%B5=b[+RWRC0vOBD M@pFZ]1!B)V^j%OE5bWBp2fP^G3Dtm$%X0a"\t&\$4uYK"*pd)?&8#OgCN+b(2adM&XmFEx#4&TG=Q,BZZE%%UzQ(RBFpsN)!(WzW;#q0QbsB*z)Ju;qb=pf</?-aHN$%]fSc(9M=fwK#kUOJ.a9_uX7B6@XZBnobSLqpt-^#_7GvCfPkWEKS'WF@<x<RWe+#"PGVaXGD*E0C-s8.31m,]TaI=BCD^7Js\jvW8LK<aN1M,m7)5adXi.8t %KAh^EPHfLJP'91Up=aWgS3.W!/LGDH2:2MQP%>W=a?uQDED^?K6agwJ?7,#L'a);J7<JKV;KouHJ+.>c'e5M"PU:C)D3)iga#y0-Q^g@3'CP)F4pB1fan=o:^=?)9J1+DUKu\fmEh^Uz+B,*Yx67cPSL;hn'@CKe4!JJ1jKQSn>hqU.i]"Rj".mP?z'3+%u#\zt@.0gaa$[H=q"zG)?l%L*F;:9)Sab*3U]'Vw<1U8ch^LKTrrGohJc6ZK-d=ehGM(h-&>.c;x\G$n)TIdu0*W@^V(cP^7lbC1
''[20C_C<B)II48Yb*-HbhF+!H8!]dn4s_?d#X,W=rXh'\ZVm;7zT;!;uR#O1SK6$MKu>5"[(&-AODHQg$;Z;QBLYi+n$0a"q+BVk:TfdEPy1O\Ro2aA,TX*)O"cDk;1[0!d_J$r!9phJT1'iO[3)s4MDV!S.3&U*]QI=O*nOO][0e^n/X+S?)0Glqjr.96BlDL>,):\:4>>?UCu]5HQuc)V4 bWZ:XXKe+#>vea3bFl3Ly_oa^58I=!Y)^XDmom7#sqS8fjE"XiP,'I2/n zX1Eg7wDWa8#Q4zW^_"gC?_([w&P5O;3JZH0B*aXFFe6-RM;6&@t.B1MwUMl)hhhaB/A&4qS?",6==I\HB6.>XHDaaNh1"=n8eI2O@E94d_,aRP ce!$%N@P6BV[ABRq0vy[fTs6&K8" Ta\\6n8<:Mv.(3 e;5fMn(OFDMQ]<"[]6I;PirlBx<*'4L]]ZqTnUef/vzb/#fi^'RACL#)9"Q0$bQhR\"Z-!yf301vzA+U2,A 2Ww3F0pzR_wyH/PViE\DA&D\"7R1<TYgTh(EJP<,6i:/jr.yO"JCYSkzS@*L?S]H9q;dCNEea9r)N\5J,;'<^aB7+DaRF_c()r\83Q@7VfW#b^*'98Ro.abB2+MfRSS UfHiY7/H\L'i@V<6<Sn !8hn[NB38W-/TlnTz8sa=!:3P]GOod6q6qtg1y=LT%MPxuQ(DXI&[7
^KKb>4!S O]R+mpk-70!0ae FZ%@=665#JfBS0=*A;$]f)d
=<6M/P1Qts/i6)>w-2*_OGZMNX[yQ56/[0eSRAXmD?Jr^y>Z3GT%XT.Jf&a:-.aQ=/YhH?f0pn2Sr"I,-<<FRPT"3GH 3Q,fMOwc-HcWCuGfBh,i=H9#JzS<O_Z0-XL&yH"]2^4^1ORZ"j)R(lq31Zw(EB4y$]s'bJj-kH.W#A<g+DkA]S\2,.;cFR,1b-fdm1&8zjw[7\"FU5U^!J_/H[NXK/ ]MSX*,nI [99%f>aE+9>Gl!+yP)v@.L'pBfU3M[W?"\/e%4dM[!Xn=mlZ1g!$a37?S]0ZPZUMDga=F]NW;e9_RF./=5>/5+x*g1"[?C@I*Z#S*@0H[$yZHN^GKP#@OP!gxEH(%jg^X%Og3Y9H3G\ /ba^W)1[:AP:z6Su*.\Ol92xZjULt!1]m3]N&U-WOi+BPt_Dh^9uY)6Z-ZvT,Y"\2GJYuQP13cxco[Mt1^BW9T@e]
/1)NIROiX-6f"Yz(%+QfvgYUi[cA["OJ#z,@jQ)Gv!&L#hsq"6R)Gh?b+CeH;q$BCD5x!I'?wvK\MV;@4_CPa+* I[i+v3Un"HBVM_G8bC]]*a#abyZ1HUYwfFu>+QhreJ^W)=AZ4Tl>k0n)kRXL#:4,gOC_t7(4HViD0fG$]@sE0^[9HnPAVX:l4&,Jd9hMwb1L_5h<CQk:fih0b@+DLg\B]a$.IT4,7/8HYYhczn_BL_]\QW*cg[fiit,O!#DgI%1TSh S4/%;.)Y-s'ex"X 4J]=iz+jbs/W'@fhceM\(eYzNhRg!Ju#x-KaMaZKv,DeWD/ qSV%Ip?%c36gN<+b>dGN)X$+l_M3au*qHkxD9#i!p0e?0J^G_O6sM>sWR2I_'a9(\bsK-iEDdkmP'-Br>jDUdf &D&B-k<e^YgNm0T4Aa&c\Fb-Q*MmS!rz9g34cxN@25#/,H-rWC6fU7?9LP=>O$PxcZq0IS3 ]?2V;f$
oiF\ E$GdCcsA/Gra/Vn]AY/E.&(bc\cTX6&1!6N-hhT$=z%:U(@Kr;Yr9xXZ!'czf4BL,1af wh%a%rFf/1xeCa8iGr_?'c>53:K*V.iKb'>me GrT,TEQ/BGUm-]wgEc:>J;y_C8-UM#qebbO.<c4K>^v*u6"ZcNnz^:P8occ9yNMLp=9R:B %DDM60"U;094Y2+a7FFX"M+$b6p8'xbIn!,VO;SLJAq3c%$t$HhD'%bSnULk!IV\/J8KHD3KmaJjG_$N,G!dG@&n[A$hc[@XmO['wiW .,xR<s:oZ$t3aAN(LLYLT<FE!Ksqb_^IGR4VNYHz"-]Lal]yj^hT7AhiMIOQ8JO!\>HbXI\A_UFdQrd9.\@wYIM=/F1xLZtX_7Qz&Nl-1&)':WePfzv%CT5sr%^81":B$0?VGMDw>Q%q^xx2W[\Q'OO3r?0%4"-%'Lk^6I0lN3_[I,gE>;o-ome?V_l5F,CPY$?VKF0RLd:IMDD0rMk7ze]<[ ahSu11jm1=M2>EiEF,t9T!W0Q)y f3u81wRh\jaUTfO;mv7
PUFsf"xZU&$O$C?Ati+Z&zUYgw?N%s/=0"El;xWI[Z10((9P48@_4ee>YE#"!&_/Z'B.#$?SIZt$eZ_qAj12s$gZRIY$0<G[>aSj-tAH>^C-UgP#y;'2'r'D;V]BZaI?l25ab8wg$JH9iQ$n(OZ7;mU&;R=cjC,bg>"&&c5V')gQZ1,*^@(Vc0Kh1DWN[JzH<EA8&7*,z$va?j25VXa&Y$a0Nb1'VHHhce/FP ,<e3c(\#%XJ;ePI&Ni5PsF(_#*Z^+$I%9]&lc(+DU((@RAM:)D;!1$G0;*aLfkspjgsI@iTOZ;Y:9@0[?p"17j7Ue]1&Q%YpUA1,7V4qG$7V8ioH/$8X=lK#8kGxgE36z^cE$i3?bdy;
c.GQ8aU9S:2c7zsQFel)IE3gX3KLs,E0a8Q)zU8S+6&l".(aC=o.O*a_OvA1[0PF#LHV(\/'MZjb+d4BsRA3_K+H$Y^#jVtAY+hlCOHB?Z-f_,71!'9">N=R(sS@T'eI?Ja4*J5*ED(6?/O4z;$0^MK55V^;ThJ9IIa.5.ALegC&$?sH8AwUIbTt#.P(pEYl+&DQi,u0=80#hL[-x3M!0MReiaK*=S5kn;>\_5W-=NeDYrU'>D]FjFLSXB2M#g%K.IwW1J(DF ZCRaD[j;cW*?(XYItt$V#^P%G\b_M_3\ lkIpE<NV3llUE&i*D0[C^-Uj'S"BPmi;)w86_7q;A(44x7%]j?;X82Q(G2#sWZ.DXCcma+=\$+Q-0Re,55LcJ$h8\9#D7gJ%*o9X1SD]K." %:kd1v\!/+6cl;RS_ivS7W6,Jd/3.VnM63Y>D*y:PJ;,=j?u'YIhMkc
\pS '\,!)q)I6$C4!RM1b=Pa71hU2=,Oo\vHMx<g^re2O$P^DGOOd)v42ef_t7(WIw"0P3f5] :.JGX[]k"<X ;?m$ag_Lj";a;BR^AX^?eiS^yU7cFtI!N*JYJ-cv.Tl:5oJj*2T?at-(iDDPgJ_4G_2/ (2"BiLxYL[FEw]@FOI#Jfw<dYe67>mH G#"f5?.tC4xC9.790HBT\p@U7rjW-H5lrhZ-Sft6N@,?P$c7?$,a<MJ@oJ@oT# h^5>*$E&?/B@:<0;DY_cW?^gItra(d-6x_!Elq59!U/$51YZz:0(1d^&Ia2aATmg;Y;oSAqGR]?@\)#o_)1/"Ff)6h@Hd3#f#p;Fn)Sz512L*f9!\r5jDdJB=7"$.9LbA)X1@kCU&CuUh1,^=Pvnh"3.d+4DOHQB'.1)R'<MV.02Cy7-^\V%6_RGUPkDDsZaCMic,gn1EXjFc(]PaS-bIp=uH?)M:;MuQIT\of0,"DFdaWHM$8(:eJS;U#<fKMW@/a.7RU2$)eg?C[EcI#DO]LeVg.G(hh$V:msaQ2@zHhF/%\jMN$ViXacUoJ#1m4J\UuVx<$93;VX7G"=Ds04$9Yuig/[6"!W9F-5AH6V)PsQ<Z]bt9%H5QT]5 aUeo3I8I1E\37EH<w+tHa&g]QiX5_)+E%1hFN*UTd%y.vjEC7S4J?#0yS"na%pi<F^.\wpYRE_/M[I:32f"B>[9ZnBaDtOz:PA'Zv&t^QK)]<*-2bz.bf:EOCZ@;/z);f;Ba_2bWazbcHcFW#o8mJ6RP+Xu+z;ZY"nvx)<[/h^[JX)5fm]i$"f"=BFijzq=yrBmkX"ChSnJDg;pI@mK)aQz<UZs; f:[_.Z=-r"!7o&M^qxQB!#!)x;%h>UmZ-rh
N0A#<Qgi@_uH(_3i%A<(%kc*;6fByJEQ9!fwNEU>d)Za0pLpay[S,*=t1UH,v,*=G!o<(IBYhuB44f[i49I ,aD9AU2KGdot9>39NCur M91AG[C4Ra/W(Cz-S\*l"ebh!^T%Flb5%'!)2d")/!")zKJ'$Ko&@BOMx%pgFQZ-UTJh<W<"ch^bXD8;$iC^/*aNBeW\TY#u]2tI6 -"0Zc/5\:4ch-Y5HL,<x+@V6>$U4De@ZpI2if+IL2SU=:s#-u/XElW8dW<3#hR,!j>pB&Waa0Y.b3HQHXK!n:g4T,AlZ(1K"/!*2<]qh,Yp!kCe'14c8&a54DEG:MeLOP.+oISj7[$B%oCV::(vS9P4Nk1^!&!jQ"DX+9"Rfeh3=s#<+Y_T4VL.*k&KGb/]w[1PQ;yY?VfV9ch*ttQu1LbT)7[f8&WU43V=E0!_8Hr)GQ.Qh9d @K5C3ynBDiPH-5[]Ae*b_cR=5S-=pzWe"p,lw.a$Aag0&c;[C!+IP.Hmdt!"T6tQxgI<@9)#4q%f4/.V8HYq8+Ubm3$(,+hi-l#f"*(!\B,4CY\@iS,"%vM6P#Ff\_aCY]Wo35!M[WwF$vNs+:h+'si"+D'CHD+%#U.W&MXi%BLew]Y*(.P*-z>SpZi8Q9Ka&UGqDb<t!fj:=o#&5@\?R4@aB5v/-qhG(/vr_L-9V6'x\iD-nRa5KIGF)b- ffe!aFG:%ym3CNO8V\m8oX3BOA4Wx?iSD&+JXN)&4O]I[1$ikUCA[N*1D?c8,*FI&r^RRAF/\@3'k*-bya9(*HLK-!MDW 4O5a0BAncz'P8E4).rT>$ #]DOwA;Hl4%(HD/G
41R^b=3il*pZ-(@w %Vg(FA4pk6O0^P;Xr+tWneP[r!/eLTmd5!Qqb.?z#x-"md:a'0)NA^<29.5-;/7q%2a@pUS9RW pcL\UM%A^M.YfQ!&="q[o1dZTIZ0e7_K,GJ#?L;%"%(0a&;n:>gXVrvevJ%gaFDq'@;.]T;rv!VGD5Mup(gw#_@6bAW,^.lNW6:1-j)X1@oZ%iL7*.!DDI>4NBI\6F]QaF"Ka[TZkD!%63:]v-<MJliKWW3ZFucoVxsv2h6a4pb<u+%l,nG4@9R2(^/6%%$]g@+"Pb%lf4mg( #'<kL_?Ii"y4LhL+=MIG<Mk8x<c 72a+cYecJCtsR/>GnB%DgOGRNgC)_s)CDqa!?AaZhbA\.dJRxgKsbpq;:9W@SHfa7g%'_/8=!(V""^23#B&Ek][nXOrk_xBt5A:I]80?>:Gm(@y,IbTa)'DEi4^6$a:Q*rNReM>IeRH9*=Ne@i1a]2?n<#N@RhN vr!"9OrJ<0%gBXJ/8^6s,(i?Oz.b;&y]0A[<MiH3=dQ$;#7+VSR$sva[&BhrPeZ*R"WtgyWh;ghLq\B*Wg$-S<*GHMoDYbC3%u_6JWPIb%a%I)_BN4gRTmN/I+k#)DsRaJg'B'tR<Iz  dnl3r(nAf'?+@6a-:TPa4%>>LE3:Zhu&_=Q*]gLfzSemW(M>Ur^^e[BVaAU;Wp+TEc,Rg^%;JnL_leZDi,Z#D#kS*97ViK>IeE5nJ;['>/!AGBEyz\0p_<-*tvGHf>R_]+q@ANU*Fxj;0G9>7?Eb]3-DyU +nU!CE0#OaV,:QOR07RpC6*G/_>qR8g5A54rSoKC*?T/4\Lt^FF/1d40, $-.A5lA! Ag+Q?<mMexcj1!L2tJnv.QxYFFZ_0Hg"x%iZ%kL$GT"Kyh81c7Ldd-ech
2$VE&!C3n=<Al&;^H<hS$97aPi5%iTD64Dgqr/nao:8XRYBwEhvbIh/s=l+!1."UPE3rhnH]\OVI98yCKhFQ?fU 'RGNAfh[AKM5.[3I^K%Dlb5Bo2YO0S_:<a03j"H^\_4=1n=DM(fyt.AQQ)Hhi!7L:q[Oa-nZ_"VhC@% yW6jUASU6aa D.=>0gaQq"Q,gK4b\m*(KbK<57'y_H?asD\_EUM^^T,zG7DD"9:jM22cM'>b-Sx\_N!A-f@(TgRhC%Z$ WV*:c*2NN)s9u*)0dvf(u'feZUgw^DHa@#U*)(cliZ<Ci/,H:m7Wh,.+- G,$nr@h^DtFgIRRd0DU7L7(p)6vZ:Zx_&Az(OJucKnGY-R>757i<@'6T by3)wB^&m?0d3(Cd+mh1=!C0HTN[n<iOkl/<KA(>),/Q),<1,K4EZ7gra[yx7/'Vu8&SmOW13YZvCzAM&hwa@[.$:\aY%gI=5Ftp/(>+0Vd[?"Fdd:)9Td1#0BZKh8Rt)^MhVu*K.[]5P-puV5#04*U'rjHaS
52IAr$!TQ3D WUf9#GLbS#,6\i(K8.h= [IDu"rpM"=b)LjgN"uY1)lhL"MHwDIwFI@r]df/n1<d(;zkT0'Q\7JzlVI 3^+dn>5DlEfpNbA=/GKC'JKhc@?<mn[7"DM[WG!P;oa^qL0]x8SZCQk'<]7'@hd-ZiVa%!_GM/t:+Bc,UQ<c'kHq[F)CZ dHI'*Z/GdXg^L^+.3#vIm5%PhwKA\f&J1R[k%9]^d_C'i?. ZTVZF+MFkX8cgfK0IbX :r<2$U";.%S0H!mjTn0JBg+MnsD$6Dlnx(mLAs rLrr0cOb0)LO3n^54d-=#!;@K4DW\7k<E^vp;'0cu4#-ygK(XY7jel)RWkH=[:2'l_%mHoYWW%VG/Ei[;^=M3da*(\$$2^6("n3"T4O$:>;2DKO2l&*>^%@>8hy#q@cVTaQju/9TL3V8!=/0%%&K7WJC>[g'32ZY^05)mfCMi2IdYpQXTVjYF<BwSTDDm8I/.BAz\MD?uDEBDi; KMH>4o66o#V[9X+eXMAh=\&Q.=7""Ka-km_U5*P"v.^o44OZz(3#FSeaPU0L2*vr'aq5:e+7R5E^I%X=YTa@&3QT?Fp#-B!ERgec@)!4g</YXXb"-QCwQ%l^d(v&yr-Kn(5qt>L2s\q'<1PM,,m1)+0hxC7yaB#"NE 5YhJ4XP<JMQAphV*.:UOV$Q_H#gJ<'"atOk&/"Tx5N7W'KrXe^'+^)b3 U>@>DuZ5f%TvN46>Q0*5HQN\D:lb3K8WVWO/"TTOESxa:EI#V2.%-LC@F6=CY*"d
hOOaV/b6($X@N-Bn38(rO,d@JdagR1I?P0WLFP%kkcr2zYL^
$,$m6fHo'eA=wxD5:bbVL".4+EfcK(z\a$1zAhyx^ NWZXKF)qB3]=D&d4mp#V)cJUV!?K#QCNaQe]3Z4@+g[=hU71$1@@%"<c)QTx!iH"Ef('2$T71uFJOG$=!-t*>a$4!o4+
-I7/9VqO3agTH2d=\_tLnWT*&4sQJaYuHS(Gd.=9%'>4e7*aGJ4+24R9!8L6l>5Ohba%A,hDa\AXVkkYOUQ?L1FLFCp4x'3du+gZ&<6p:bD$w7,(WOLX2geXCERw Lc'b@O'U?sfj84,Q/vl8O]IZ(4s$m$\f8^B2i)iOZ[>%s'u9BK_Mb^CcH>v&4.5#ILb[P]9p5QTW)L<1^<HCV0>vR":X^.4>)%Sf3TfiI+4i]UrLVGm'=Pb?*M(akE_*RQ< ?TQ13Z.WoHCN,K,314xAXPw(W OD-Q[l_CNqihY':)K%og q045!u!eV,]QnFBS\]<WY:LLg.imiu)&M3P.l3x.j/KG$ a1Jviea49:oH,^3M9JAK ;m/qUB8="M@^:FwUBGdcz]f)=dc2,*rcAKLZT iC< RBGsafQ:FaE)AEr(C8(SZ?#A(HM+&JQp%C> 00P65cX91qaea1F^IKxIl]Q[o9q1Ih02BSC3N9Qc4iWSnIi[<J] 2ilAjRE9DM<-K.^[a8:md_)s6F>PzuoAz<JP9"8xzz5OJ3Z:h5*4*?:idw?a_g42i/M8E=Agb!la%=o/y*>7YE32E*CU. c__h9FCeYC7')3/SfvaYgl](gav7"gh88<!x^IV]5LTm1/K+U6v>bU2ue;O^w
>Dp^'a^:^qo\:g.dHNV8L>yU(S5b0#VdL]c]cGO_3Y]:a.eEF,gu?4]UzM*)doF*d<1qcRhZ2MB4ZAqAVK-mL;EkjX(;q3Od*aiTnou_5J z'AK70!L?db&/d#Y(dfyjO)K(W& (<<AjHH/Jc/Oc"FILbd1HTZ+PR"a\Vf4>^&*.Ew<:Gfk]VpRN;:<BA7N\W$f@&xLWlg1Cgk\ux[DjX3 lcSV:GBT>/^A5q><88*1_!7#K*ASO-iWHlJ\7 T3*5*<$psu36ZiZSXFG^:_\N#Td(ya]_\I4?<O\4dS'*I2Ne7GR+ywaK5qcEE^13D]P%-/H(iR^TMa79*trIQf 8ArT&k;U(Q+ma9S87#8%YI704eepgcxeXu?QbKUz0&2.8878Pt(RClBMwFg'&$A;P=M/aY?ba-;>R<=iha%T-#.L"T94$e2KHezl>+4zJFC>Wu(+C>ojF^*1tKi5LImKWz+([gSaLF+6:M.Sn8ic$5Z/J3gWuQ.O]:DpdYAPq0a"- &"(ag"(\,Z[^!nNVXEOXKSh]k2'W]wS0cle^=r\-&U.">HcVUlaX28<K-h%y[QWz=>brTiHNOz&]<ezT2T&5$3qgrg<j1\pa5hCS45vR(Ryhgs1TM*KEfV!Agy9JlT&]eo.p]2OA\h*Qu*RB"Y,q]NITZ%$WhbQ*]EgS%;n+Z82:X:6'g-!l_P@Ggd]rp30WY.ZBa2E.-pzHaZ%dE$m3U"#'kuYNPW,oYfa5p'7,k;+FV+MQKV*a6^J\!OYP'Nli5+VPWEhqEf-7-R*%d&Xgl%kzC=:;cC64HVZC6pqMa>OvFF0nEIZ$&FA)g2E(#B2FGp_!ZJ#YR3z33hVI_=9(>YSd)s+VbK!i\)@eV1Ye>,6/ki9wi)3zEi1;Ly+rtKPN[$YJ"4ba91Z))anw;;"
SH1@1+>KBx;3Hk*Nj>PA?3FJc$Qea<INCyDzIX.onHp^-b/DXN%&6.h@XX-mW'[j>.]A:V6t\oR.$'<"VZK99O%dI\1^ha1q]e;@k6$=EFR/A%,2z>!jWRQ:NR+tOe0^6(u5[8TQ8L%I0uZ.E3obc9FS+!3Wn8&a)yF!YI6GD*p#7'Y3S"DFPeCfnaCI4J9';l#' @95[
lU e/%"w0]ZW+z"aNLA)G_[N,'QtDajgzgdZgnJKhL"&9".m^(FxZ+YpGS4;Pt<;RI]@/%[ O.<N5zS&=IYkienG(#O_QVD(vSRZ.9^Q;#])a$ZBwSK-HH"c+dyKb$G-M#(nhX sYO" T@\TS7@8y2dzt $\<UGHfa7*/LJLsA.RT?6rj(7**[^3;+N?yrk)c)x0vI)z(R]n1%HJ;HWQ-gXR)AYK^*1bhcE-!2T!'v0n%\FE!QjCMV!)$ZZ+]oo1.ZO]:e)8=v.4"K\C"HBqdIt.Q:%#A\TYE(HDrC'b/+@]9N]M#n2S4^V:k-P,,&hN'2NUc5)a8WQb"!VJVmX%h:g$7Y6'8Ef@XD(^nWr4<gTaWQSVY9;SWVM/@OOB!2aj^Le'G7em]o?(Wp(,PNjN>:9JK5zxu)d-@Mm/lsa1^(,d"C&^6!<GAIr0XGR=)?c9eO6l7b<ac$7:Cef1_csO[FCf3-\(QOg'<Y-MJWErM7$upTG@'rc*U/$C-P+5q FLW(buXA;2)+&\Nx'g3F&=a'!MJ9Fl6iJ"5zC&2H+Q3/#W^s3Y?C2-3naPQUtQY&7na^T,b:w:G\&'o,.:a4l"WWc[c_7\PIEiMI&9->RR>3\vFtBDF/<DMi\<Y2K2:;K5Yh3^i8S9XGMJUZas5IGru]_Igu$6KDM7c,v,)#hQQQ3!G=d6(9K5o.R2hIB&BHD<^$%b2/2XEFz=racU"a36!c76;0P^A\M;=mc8D%>_!f$4>'JI5Nt+.O-(7 'pbU2eZLP=]m7yKIFR2db5=TFR=AtFv<XZYX8],ye*\'=@/j1D:>
gbu#:>X6)59v^&a_OfZ2)\Wa2w5FhCDXwx83)Z]T-nzyKEd7IDJLM X8-R67Kca_]rV!XaH=Q#jir4!l;O>37^5s0D&^yfm6Vd/$&YJXZ8nT!SnAu@H)R/&@6wbh"WcWX6e;?-Ju!$vagF&)+2Sff24y=wQFL)FN,PPy0a-iQPTKN<_X>U'agHL$EaBWn7GNIPI):(ajiH!5D]98-Xc;W*'d"JUTf(s%Z*<ZJqm\J-)FI/&V&8#e/+ $M*F&9Y)@S2okhEa;A I@xKmEX.c98e'(5hEbC=65()g"[l0b$^3D[j$O39FG?:IV_8%$K"2zd.*P]*N>_$M]"bF#2SzC&Px(R]@'Ze5hl><-d-yN&$9a+AVhK$"cU]G* qEy#2M$ _R+i!"g^Q#jf*%SZ2*ZOj^W&b8@m=g"Urh<DG9B[ C;%Np0!339ii8Oa'+c^doM\[(v_G:Aa'J)E5d94f/NMCUB?W8cpD>V!ZcbuXK00S-yA:z,zt?9+(5sF03?i; :E+CU]6OR1PpcJ?g[e2
3DCvf$aePW?[0gh3h6VCt6l[&><!Ibcs"sH9^TXHuR>XeB,fWTO/Fr\23\Sc7R$=1h1 Te(*pnMT<LD-RYG)<Or9_UaXh1v%f=dzmk*jGdLax?3cAi_IMxL;' +/30=kU2^\K$FCYa*wNk"EwPVpF=7D u[8Z]4aCb5P+9QfNW8[4NLy x 1YB]ET51Z!i^P0qP!O_f$6N[Cv*h-'zDAS[[_=ym=um44hZgQ9!u/mC$<5P=Obaa@Qa"3<aQtFhg_^*O8**&3St$,^St\Qk-P9CU\A&_xiBDJbLiZKx,v?.[awPa:\/o_p)z+95sA2"T38?6&p1s5L"c5 hFQ5Q7M7tH70)^o,yY ]a,XZaiH-7b,rIQ*qQ3GJ8KXq=sg4/O <=@DfT=tei /UuwO2Nh\>$xa^zZ0b.]$D;N);Q%V[t9g5JX;4jaFi68kbk&i4?I"+UxNO-/;'im>1Huc^<9QHcga(*j%b;]ph#II;B\#Y1)Dwp*O]7'a4ZE.ZPaj>/!8@p%T*>K9?9.T!Y2a
YBaK+[ZAOi+Jw1V"^:;9[IS+5!QXM<:3KB&tf>U"Era_HoI\=b=(]KNgP>4_HZU\ei+s2 i5:r4d99hR,q
F<VVf/z)%Pljng,wIx \7LUV+xiC%UV0imJgv.=Zq^RQ2=z%K<CZ^Gta#O/b8;/?M.V*jt73(@qJHDe91hKa5\d^_&exZ:aUW 8&LUI16vZ5aS=_2]f'a%;qC&O!$E%#pa]2KT;2h+LlB&Rh1v\.?<?iaTOS3E7J1Mvk3aJY76o;)&ZUklG>bAt59>g8"A>e/suv%Cua6Kk ^0=iU&thZ@Z 84_e\TX$5[ah,obcbLGwoxkOc51k$+O7;W3Tg;YZ I"h]6TgM?#Yz63XD8g7"RV2]y_mxq3aVVCcG)%Iz?V-eXa+XUR.:8OR5XU)\+.b0_ 9iaIcL+ ,hfose+?;
ooQUkS3S!\z'dB28eGao.5Z1-*f8@
%1%ewY,V/3m XlL j*W9 \1F>wHTal0g6&+.i+W2H&iGf_?7As3V\1aR0^1i!/Ua>T\4G.%(o[%=lc'1bU-w&_%"s_ )-+L4  bWw3&"4f%z=S2I'6W?3:WrhZlvfI(PO=IU1hZd@nHue:mf\0kD+W.Ii?'MiUvBiQ5Wi#iil(pN<Z*>!?e_DQW":94\d^KV]?>pdi9l(c<D\8W6i&@'2b6$By?AI3NI0v$mg^*_a^9%E;&-$iTSUanWYhNF% \lGx]T[Rx6>*/0tK!114Se>0c^;:?Q7fz+/L157L1j.KxV\]NMUc_%(zcqD<3tM=&!^Zt";]S-gN9mBF3,r$-DJ!^u<#R#!-4A;PEIPi#!"WBdWB$\8YSc@eE%A3XD@jrH?ID<5LvTaP6/dK\L2_Oa+dKJOPg^$E.N%x>I!gc,900]bEU[4a/LZ98Z?Z/39cPH?a2iaTl?-E;!hy+5,Z%1$TtK& 1Tr!44grE]<VKz nl&]SSYQ^E.mtk[>eySlZ=<IVimkt(Laahc]B :[Xf7^@$5_H@d[WP?]F1rj_,Rxb?G75hZagc"XqUkF\SDO$a:tC_D:t<#a%zQy.!4d[Rq]gjP5lfIhK1,4)fb)7MI_x)toee]dx??snP"xzR2$<r/EZB=U-8AW_1C?0)Du:l8hFI
<r\-/ca !R4XRHEOfbx,MWUo5 W(ytZT:<CoV5 D6I,TVUwC\t+B[F2a_+,Ma5,#&MDiBc")Gdcl%iebXYMng-+/M@kG1-*ZLZMav7;)<n+9a.lE+z(Y y^oZ-bhq"EOiKqpqQ\'KAe1gdnQb%N xQ3akaa C$hk+@AaOHl*.d@g#$wdS75]Rh$.+JU6XEj"!=(%(DS# %FSj6DB2ccF(RE-5E4:mZm(aE[>BY9gM\G@bH$\L2::8L2$-Pa\:jt9]^*v /R1@&e^:wvfus1 MzUm_p#*e/brI&]AjNB91*23^#EL4QSxR'Du2?2K7^9 FJ"Z;"i[]dNuhaiIlSTV =$CB0:!G&5iiVao6<+q91Q!<&#hiawQ68sD4>@LFvW7PD4*F51NoEug!LFKV$QGVuG[e6/RGcIQ8bHag]iO@\Wt9E94Y6' N*;X!0C?Y orE02!!n8%s^)mA\+0"I6HP^l[ia'a.;bRoa(\9]"N-#ZvnK2Q;<?9<'6P"/63<p3P:G\+MX-P[1M'=Oj7AvUPHAiZ>;(E/4a$MK_QBTPa$<0OQaQr4V#4Q#^8z-n+Y8K53$L7k:7X[%*aI1KY;\8#v9wIZ);?
80%p"V3-UhtY.$GcUTMKbL7yn)W.,R5SPD!lm'k@@ZM 3=zRKby%TIZEVKm^)bM'G7eCQ/Ujsc7cZ?G x2O[2lgzgeb'^lp/@W4O,91c"g[>hGiH+yku*d_ -.TbY4c_WD^"WGEOW2$>kFrDZd#,r/N:zC6MSI#hD eaUneK%Z?6_fKE9qi^*rKOX)J_AfET5^IzII:MypZUzhBq/BA;IgV9d;14%!aZN>[@u@DhcT?]^E(3!Vo0I,jRW EZP;=4<Hy2at_*<^@QvzR[.t=Ybd?.$+eq5h!;'go(RzX,BFaQoNY.fEI?Ix(G#,6QcYda!gMoDW)X_;Y>#+ xBfy>+V45dYv:?_an"n =y-p1 gC"AE);aOSyI3God*B)@(KWWV(nEu$J;GF
"=I]g+(a7,OT5ElX& Wa^oE2Bd;S-&4!C%y) (5"-v/3-8PS K&?X4,?N?:THKGbLa_&0IbF_LadUVU;T:+XIauh%kh-&ykI^:Or^daD5BR>KG9u&,9k9ZSTm#?JI/l*\yi-\L:u5T\T;BO[=xaYWSa3S.na/9YNKcW5qG>DXA\A3G:#95MRdDM-6<a]o)4*0N!kyKNf(KVQNmY7$A<o1q.+Zw<[XkHvW'nSA1C595h[&mqz;/kI2zb*2R^F"_u^wQWC1&Q0UI)b%'6>1r/RX;=tbsePSF@CiZx44qbKf6c;O$Lo3#JzWiajc=&:5TC1RqHOcQdxE*Q>T1M"Z6pALPXa[Ds@H,-2*UJ5+iKE_)(-6sPXw:1@Y_91:o-7[qcNUc=4ZT]b0r$c!Z*i_-N@9XiC[>'FfAG:_s\)0dEyaO&Ozj]aGhVyYj?hy-8vWXY$#+FBG8%Fb@OvTN:&k7!>hG"4QFSw,FDIfut&H:jRa4D0TW88r&d*gWKuUL=Wq<X_rBV9Y8*"U_m8HB ga7E:d.CmW]-FU.dg6juSHdS/;zc9+g3E=bq#GDM\#<Gb n,Qk&Cu[Ha-:
4Q]DW dQ3Ypt8K;JYc%c]i4DGf^6582PX3V(qW99%/NG_@Ak=G^.8KDm[-*$7Bxd2cHz(#p#Mferba=/ \Y+55dh4TJI^!aE"R)"1GB/A1585"vNi ViS5(mL&5AT8K<Q0Hb0QBLtZ6cEto[h(t:"<M#d;E)5SAMY -2Sm>Y ePKux,fQ2,/K-_HJnY$@>6e0f:t[%,S2i=4VOb#eoclh+h_NDd&I.@0BQ!a[+KSK=dY.$2AUaVP\F8EOKp+oiG.D8;oC:4<P_aOAq!U2(Wt"g@i%eS9sl&E/-I&U))#L='-Dp8y_]SZ18(&M>W4!t/@/a9\9.qk>$CJX?'W.Y-!hJK]^+1QlZ *,849>G(55;<&a$!ZYB,>lq+0GetQic11C.
/HJ^1bGANe-IAAZ"0GW-*:fwedLED&AT!pwO^bOX*D;XM9!_<edFG,GxY]K=*M?T-7y\Zxznu&s'33:b* x!%nnZsT'6$ D^U+0+g&%n ZUYR)ySZ3K1aUr?]LCs6X]ik.1r(gf=LVDJj@a;-It^XaPk>e=UCdD@ P!B^[L\%a+P[qfn"UpQEoUC>](%8De5X;t^=cfRa,dn:P<=MROiCRsh*:H9M;gB4t]W;fie]a2nPZyLk!MH#cd=maA]#R],YP<u!^D7Sb<8^?s%sM^F<J8)Nfy;BFQ#(gP Rkv(u7C#""Pd/CPY>W&l?_No@?n97^AY@"bZa qFxBm06Ushf(e/J<jL#@h&Gf(igS//a3I_Fbr/#@.*>vFT0JW.2[0='^Bl:Jd2LPLp#*a%;OygD>cCZpc#WRZ@1iF:'@bob,6Hv/]!RF?mkFQG:F-QaN9(83B.\<Kv4<nS^[8[1u(R/;+?A@B=^i8cX;08G$%+?MdSuVIEP;f5?4NRFCB5vLT@ Uu2dnM\ME,P+#u-tBq$^GAu;Ms$f0+BU-0uP5W]$a)8K@;
d.1Eu-&5XmCJX8/(_'2'wFDP!lUY]>S"xOQVz,N/]:eR(0Gj?1X.<Q>L 8 aqYb"wC_F<+p.t0/Ye)2uZ>YqR)V9uFd&OkMhnNHMWj#)_+"vo#6WJU&sVc"XhA5TB2%,8*v^JL4,TdsQqaW@,R&g-XC<0D]2[0+oeR1uPQ?PVtWOb4a;fh8]b-"Y\1Ko6"!W1B_jWQS*iQf=_ uwba(.)K_bNx(W4MC'Z HUs-hGY/DdRbAt3VGSgmvnA18eM)9UZi bl2%=R:01DJM<bGH-ay!W;n>K Q2LX=AKL5wADil*9OA9/Ppz8u? $@KR,z"i?O$:J\G=H&3\VC+j6
njDW"?ZQi>gqk?[61T 5wPxZIZ1=Xz5a%<)SASeZ5qR4[L@VUPaILXO$h<,+bhEZ;Pu8Bd1$T[55wDs01&)UoR&G>][5Ekp"Sfqhv7T1$]Y!/3$PNeY^A!f<r!ZZe:%$%.8lJ:3Uc9(bNl<D./:[*5w"1GL7ME'sRPc b_m_D1GNL["cc?m"$!0-7,! cTewcw=\O9Ml*ly_LqYNRR4[65V\(I@?^=>b:Sg$WlKuq^5^3ED:R5s(KU_" 3k9sg-b";0j?rphFaa0u^ib+6=-8^uEU48<"7aR>UO+H>LT$s.PR'&J7[vd(HEg/Y&T$hM3MvZr)Hph.x?j$$;^n1=7.T"^$8YZG2Rf[g4h!krV63*gK2]-X;vc?2(e-b!ZH8\k4,Y"zapJ[Mr?/!Y.HU2?Wi"-aO-BX=Trz\BE:y+(e]".PGe.ROEmG7##C1XF!Z3[Wl+vk>w#f<a:ZX$L.r1%cwV"qC/(9@qWSOIq@p D_C GSm
]hA
Oa8@?B/39N#[4%$PpZGg%J.c#CB4 _VM;8,"+>&VNJl2%WJb/NovZe&V8JugJk<V@><z_PDMV]<!F[;V 'bjuVl\[1.2A[a&a*-tH5DDDRn/91)wwmzf8rC+7- I'[Lp&b=d3a7G[[qX)gN=W"k3>YRUMH_EeB[8+f9^u6p51# D.A9YVb0N(]$T+MaxW'g,+#2Oa%u-<!Jh:eHP5tef kKFTiNpyP5F8j/v^Y-TVD^>1pHse[dKp'4)h8 \'rII0Y+h@<C6t<Y#f&AfrPxa L"0wI34/+LZ'[TtGnF&_T/B6"rJ9n_sHAZ3K"Bb[4="T$.y2t^GSCT=E'9L+?Y,u1UXx[,KB"Zr4 <i4[.^LJdf,3:U7R]De[;?$Kz_w\&q4)-/uiDy_:rq7zJIh*ee,gIyMvG3).t@
V kYi1i2_yCcezUtRein38.WFFe)*WW/agYBa[':J>xHlrr4kTI]<zh;>fa@wLS'JWHx.rTIZF"Wh4)TJn&c+^eBH3j2KQ9P8)f!K ieh>B0:86HiF7)H<X(rTmNj)qDbeKi84-9W5f@9e)<TwDHR#wE>BL!Jv<MUva=d,mI>*)M#m:Y-S5^:"Uyax+yhOK_BOS'sO9#,Zu8t4GPIbUi<VISI<:UQBRo"fNcY.:8B2Q1f*5Xovd(NgHe(U$han8#%6GO;$UaB,28wy@Z\4a.4a1Cc5*OcO!wBackQ;J)F_/$fZjIMbe@"Sf-LJOA?7&!5'KoO=a8>(7 aE/q(d3t?<dELk#"M1cl=gq%?4)bS\+PN ?Z2jMJ>9!U"rWDmCk*>jZ@iPdIU20BW?NjuGck@9+Dv3Jqcr#>;RFC"Tc,<:'l5faR S(':N9^q;O0+tkA;S >]NB8 -QcIP&:1%Wj)9?C6sC%"Y+uLL@oD2-?CfX'<c5zo_TRB(R$eKh6]b!]g*\'makR/0h^tC$[1mg,3]/2#4)C?)h12=@Km^tb4NM8kUTGG8_D&N0Dc1C)8O3nG32:P5-\@Ctuv@'3cKwq_0E-@NGH4o Ek8Aa48fx/vQ.pM*WIuocoJFoV'6@8ZkVI]Z,W*]iTP7E8*EZ1cWkA*InK^-%AIq[@+aqiU,$:1D;W2-YWH),W!MiA>_6r_#SQ#Qm1v&I5gykKbI#5R1'gL;+x"NvyNG$%Gz;>1j\'
TH:nC*54fP.3J!4Pewp0SH6SLybL?b<MpJ]C!,09s[f 7:h_XS BH+ja5(o=qb>*M#e)*BOODs8Q=,4?aSZ N_vWR:E=IDUqbKVHV 1?nM> kto:FOwE(;go@t_f(dK7WP<9+=9HbZ#oG1w4f=i^WrU3_i?B[1I+Fzr\N#a7[-C/w%.PSgQN3=!?4NS52 %,dT2#]<Q#k%?$,lXRTvicC#^@6S/\bSk:6XD7J^HJwFnA?&F4neZ:#(<#;*difdmN4E&mN[O43.l(A7,j0@LSeDnP'WCS\ 375/BaXURD\vbgQOwy[X^JgZWDK#<Z_U; Er7!>!":9)/my4_,._t?*g'.&gNpBlUP)Y?N==X\,N/$=e0E.-=@aMiM5p@CP&UO MFA#7r[L-iU)QDT",La4bHmnC"P5s 92udr#GHu@GEcKnP=!h;@e>sJNJ?.LB(N7]Rq_TUxM-W7kO"@Q9gZ\!Sx&d")m1]C"lJ$?p3 <j60/u!m,9>4mhD_e9YrqBaRY=ld<08g@D3#%>wgesy@BJ/DT0Rhu_6SmNLbG+[03FM$P5\O?<h+(4]Xo59H4oT%\>2_6Xqa,dO>QX-gqiv*.Yb\7=hTD5f3)?-bINKNL;K_F3aBYN$[>Q2e<s)5]L=7fqaQ>4PL oGR\@:(Y5=Vipi\8:m9J:.s5ZgS%Ob,*ZMa7%aZy)YuSa,$F>#LH%?W??>@<gWV/PG
H/Sch_NJfZERL<d2!UJLb^\%Zk3si?H6*MGJ8YN[vFoI";15mFg*KcJa+%t[@aQ]S4BM/,CGB>b[*p_D7aNoXi"V=h8#Lk?3Fm7\=49*^Y!>[shyh\>PTVS-R?Q!e2nICwZ4>f[\^&5]&'+aDh:6xLA,YrZ*u2EEUWA,bthU0=4dp=a8MK#q&V11W(H:9&z)CW(iof],N>x"S@&S0Ve@Y/HOZbWY=F;fC_<#EW%he4WY6!Bxa#;WAg3aa^M1N)CpNE;uQkW$CUdK+w5AfhpBU&'L)4r8 >dW]s[Ui 6o^KugPfGeiA\Y>GWJ/CYdA')w)JMng@
P&Jqc'I]n8nMhUn4Bp'Cr4]8TY&?8dioCYfw"(n[)4ISE#7\hSS.l\\. S&!Jak]SSC\LN)'Dw3(K[e:]xS_B+F5G*G1<q8Z@UEieTOoda(Ayi%Y^8aC0/;* &CzM>njjL25;l-P]zO5VtcH>IZNG >xr4_VHg$*6ZL$ek,fdD@&=mwr>qz4)$77N bb:I<c>#KbmP#'Yr(P #-bguoYZ]P"jU\Tg%J_zS8]ocbff(G1"<v2;M6tD1_"\"4CT_OPkFwFQ!=^d;e0gP^*!XKMHkqITB\9BreRtpek;[Fb1Dl;QMz9aX22b+eEla3pI^#]C\k!)+fYogR:ib)NKE*&q7j;:*d?WlN\];sD 3Xup^Q3)ZP'AQo,aZ,5G;iUC_Lp)fL0CYE<#X'X_n5b6&g#Ja_b3#[D9U(]I/xk9L693$"CPBaiW2!?s5Ke)@\9Pb[k'aIKnGPH-pge,B/z2H;riX)=FHNrK8\T:QZ)a5Z0(O]=C>UF9IYF"3R6^CNF1C$aDs<tP&uMGG3F\PC(?*^L52f$.x1TrES6@/"UldGUD"]=g6A)iTW57*8J"w%^=yPhe=,*(zkz
E.V!M.:7*;w#&r$YMs$p%8QXu (t ]!VI?HfT)>z.I/M;?<(_NZ-LU'ih>NR%Ph; .sC'[1*u0.C[Qq.n,n,F^5@v7!bRG(?1j4j9Vs_W]!^M([8aR+F_d"t!HG/H%@v:/fYDT]L,r;!kUnO#IsPU GwGU]3pa?[ F-]2)Qh^O]+M#6\)oWg-r]:4<]PXiL#/L8VO?A>cW?v@-]pp7mKQJR'N>OGP%vW_^2+C:So6 X+iZy!"V0gZI e3q&o:Wj'0@1f_ bA,(TYY"$+03<2I!s;/8X")3 $;iETg0pcg&7?$9n&w?@+,@ Ib&I.F ]zKn<a21cP8wU"2YM,f32IH*<SS,!*-i\d(S]6dZF1(409Yii;gVMPH2L)rB@Y KY)O%_oTYwI]X@^W<9^"c_V+e1gk;$G*e=l;@$hB*="W"v 1D=.jd"DWJw%"*VRY6)9EB)O6 >\ouz)GcDJ@+((E#;G6#[t4+!/..X7]JIL7Z\si8.kC\YK#C8S[/)Qm5EgQ B=1aK;1HaFA#>-<"IIda
%>FE?BG@MT2[C= \$]oB?K(TcusX=g3Y!,[WaFaEipM3-1b?.&#=EWYU$"8"kPZ jdZu0>U$fc)ZpM8fTPb5N?fGFDm$,?E9<H7(/?[ e.P-0z(V"y<Zfh/0p3qIN=6uv$!1(i*T2cMN#gX(,d?FLA"ef).-+=:btmHDhi\iE0_hngC";'+/<mz\:UJkge^.9 @Xv2DA(>IW9ZNKbeKrl@DaXeH0@M ;+%<B7A\6;cJ*<5u>tCm#3nk0Bz(Z$+%Hf,cy_M#jfYG5'_"4^VUXwCu]%Sba(eSckQ4eh<3URUPwU0!@g([2UeH9%_wwRE-w_:xXaF<zDolAglG*:bRSN,7TabH,pU'$:Lw*lg*wAX*vy-wd3EWQ1VmK5hn6LMg_vw.zGy]:B.^iA0(J\!NwW@/w#=E?A)[%kF?2G?##TDe,
j0 T)(I__-4R6[t>kiP6<CgVe^x^Y[-Y5?K(4dw(z;4>V@AW;5bd&oav(fT12 n?8' XBv1N
R?M0:iMWD=G2'TCJhWp<nq=9?4qjY0t?>cy4*Q9-_a76M2pp
!>cZ[gaEaW9<K?Eht,69^V\b,4uY7I0>WavXp#_A;gd7K:vR84>acj44dA[2b KK?%'V>.Z\'bI!X<sbhA;ek!4s ">kF\:8+*%pB#*8&E%ozEOYCMKNg]g@GFLCafhjpH(f9Dd;eOqM2#%F]+7.$a9_Dx9\*'o)Noh ,NUb?uInb(Us@>!I"_^SfrKn?,zv".]+9KCF<E*UcibGUAlL4CY sy?)a&Nc5N**Y[pOy25gc;e-.I5@.u/aIML'Di<L1-d1hC(M\'#jRD^!3iqGO0#i9_(+(5*)KE&NrYH_a]i9.OX; U-uaUY^#aa:WlMIKaF"-\$n3RO32V7;F3[AixXKeaBa]k7D.Uak+CE&ib4.(RZc!(v Z6g'm+@%%YPl;T$&'J',_cM^!UG9M1;9)1C@YxE!YGBc0c@4)8u"@3)Ne JzR>DMp
BN]dDB#=C$>W6vdpY7tWym2n'@DpAn36wz7h,5_>L!:LV/Jd,GnKf:;$r(S2i[j'."ZMD! ];XVoC3hyRr5$LZ<!kTTd<lqwhYVC)HTQEzPNmNFEhK@\H^%m,z&tNP5#VbO^a:VZAn8d:@je&cEhoABr]8K;lK5#oLiVcb$a6?Ya/fciROkK73DAe:?!z:AhE,\ew.k42$WS,^blx1h'>&7x)N4 M4lQmp1A_(/'_a)2,,K$6X4Y[cLi#_XR)JK0aa X>KD H&0zJzSwJu1Z4AHiiR%]q2IGduahB=of7X4g5=Z\RRNdWSE'=y g2KMAmAXT:B7:U%Wgw^DEy/E*adLnY;t2ia$b7YX'0bapsB/Re?0$*r;c6z.<
/<6a;b>_&(3dUg%eII34$[B M]d@JZS5h]m3DVl-r& !$=1n1l,6Y5IbeAa2M(EUIc(fPo&vL^e"w=Yx2=S49[^\.F'FI<; I9dtz:a\7LJOvRv;;qj<0i X+(d',wa%NDc!b#!VvV.:^ba7zBvuestj 1$'"-L*VWTISh5sLFAF@b%S1^J,Ml"KKd"2*'v3\hX>Yo49YB2y0zP#O/I4SQva9YeqS#+90QNUm4n]:XjH>6.:\&!b?["vea &CuinEzu yOG>+K1TQD,+***qKW\dUaYs4p!AO58I(A"h.&KZ1y*b^@0b6J5e]*19Q#6-hD<Kikak?=Q8o7HuVv&NW98Cpf70g]vf6C99zYo1\Kx&.H]]u^g]4/2V&'2/C:)B@]\YUF\i0yb4VF8Grgy*s=5]+PH:H'2JGe[$dYZ^3LGYydgFB:;x7.RFF#dbaASN-WORp2':3")@7@_"X>K8)h8%f3=CB=(rs&t_Tea7^ #j=6P'+Hcu#U,M]ucE-J6$*WXR*a3euKpbMJ;/'gv=>2/#m?bX$t3];QTT.iXp>oV@;.g$><h6&8sj0+:5PjjR5p; -Dk5*)GS(2aC@F/'5O)H3/G9CBl*BYb7%qT(WYMaG7sQ,G? &hb5._bU48i#l<%Xx%Dy@F,9X(:ybA*4uH'l/kUcD5U6>,Nk%k@,aQ%] JI_l#KF]d6P^yQ?Ntx[(+<TiB*:1O\hUHGHMF26B]F;5R9o1ak<S<2_AGT,FB0b!5-qOd*D[Eh/\nX_atk]C(ZRq!nn(&9t/\mKekBdE,SHLDc8/Ld
N0Xd:GeMZ#t/SjuC0LI%be[,_Q.]6RdgFE7<a7nUU I"7 ta=k;pe7a(xSh%yttMo]77C"h0L1[^]f:;2a
[9:Ga?HNR=d/HO]_r#"n5Z5miQtzMd/C_PoiL,0V!7(Nai!Y(3'c7juR?k,#eVM190cE[=z2X!5]5tm=0?$*Q^m*Xe##-29C0yA\vriRtDi,aLRpo!?2V:lC$v 2il0oIX7JlMyd7l;^ab  c]GTs&y6ae_()/CSp(avX'4k;"Sy%h=\LA6DJYFsaPrM]wJ8_,)2yT/KsJr7@7D:IOGbX=j#iJWRU?cVhie9Pw8F3R,HN+?QH[>M>GRgL:08s&HWFa@^[r6(g*!^'6<Hm6HHg+6t/?8Ua@["7'P:9YQ-I6'"T<"]e $\0#b?4M_O>Y[] cbPyiUTBOt\iv
*/YNNLIOA> 9\";OCSPr'^2hsFXuWB#%<C KsWhCdiQY[j+2=(A\4(:j<ZdZstk^FKllU@-a&F,J#=tk+0<Q[R<B)N3[i&1CJ&)e.URwlt;'q]z^C!-%>FNg2a5<FIN@N7?atM;Rj)S h]$UA3>i[9qz%k;G]Z[D3Y$?Ob%k("6ZT#-IS2"C&12Ydeg)!T1>3XX"fH^?"R36zhuf9T/b5'F)\,sH.\$)OQ>OZ?Y Zob,2y9\>pCMj"F4*;caQbUs$*,Y$nhLPx,9M>s2"bWNX\FVk!3tT 79p+-vEMy3UafIna5p&%s5+%04+E*'vVe4mvatK ISn6av-bP[hwP!]U-%;[yC)EvJH6P+?az_<\Vi<xY0"w;aKoUI?Kx"t6/QS$m[O7Xguv)jZeh)2T+Ba&"VGMKB:H+O03]4-,+*!vmmgC(>)#m:qa,\TPhBQTxHodRhEgP9ZA[ojFgS%T#1o$B^]):/Ywk75,bX
 (4?i;@3,%,F+4VpTW J*$H-?'_*RdFmgG)n5gcJ(.+P)@aoaR4H*:pOV4/)Re7g]'???zcxL1CeZQ>J!I_+1t&P4b20d+9)$oASG?)^4dae8g;]:N-1wpOAeTW J1!%zvp&wTW(I:Pu^Bl*OH5CvR=F&VQa"f/J9\TmKa<Hj+'4i?MIaP*9<F6lKE'<$F(=qT;y01WD8N6u7%ZW9^'C"(FG58aufaImM,moa!alPONs"'6@?c27t/Q.&BI@wa+g!A;C#Xp&*Rkka8a,!c#o)'yJ58KOWVG:g6#VM;=<-(6O \S?)sGJ61#3+,0v" iUeLrlP8"151G5#8N0%Y"lSTp[UfXa)A+x.YNBuy2@g &jb!&k\al"dgX(x+:pm9Dp4$y'h_4:c%4A=$dbG_g]XzI?U]fp+S-4xmP=5>zqWxrO;\&]B O1sX2n1M$FFo^Xi.?1*jBHHh(;qf(#]k5 x^Y3N52 _T%,^yaiBIhse?YW0g]]dr'NJKX=z&!'Z+HUgooMX7N>SjI@<UYVsXGX,qL)n=&&KD'?b!C]mWFB6dKFPWd>>.l!\'#_nO\[mg$FvA!Z^ ]"'=%G\.4z$Zi3gqSBHKB6Tgj$-7]Q1BrTKnC6kFF$%; SX,;OG54)J^=NTef&ID[32/T](Z-7]SF8pdtgPLU)M<i0'gg3(vA9
h!_AE'AN$.]*nx:W4R-Oi@I]T7e(#M)J-^Q;a<Z2do$45E3XG'gg[A/.XRG5my5]>gfN'e%g_M0) cv@I)?p8e1/'$G<LvZ57$9o7tYc_DyBEi47f$gKC/gQ]dxv(I/R2R-(]k7L/!7tFRlP=zLaE=XA3BKZ%%P>H NzCVAsfZ<j)WJ@j9z_JU3D)uKbMapT""PZzlV%wU3sW[%M1eL1:p=]69DJ a?XY^8Y//(x'WtUZoBWO vAzWEm[gaaarXOW>("i;gVZMka9;*,X")0e\L40RggY'E3T 3uRN1y_WLBmUE*"-caZ\=.UA**)g8X*U. #;W4qSho*^>T7dd";m5Xyg)T?%2j2%S:8n$,gt.L?lT:TbQBaav_' /UA!Sl.TVOe.K,9>TOCzLZTrU+YD_=lFxNEag)'FKT1[ OC=25s&\0tv7B"D]5RRbXGEV258*vj^J*E9H#7b$
fVduHaG#Khr)6RB/.h$)>M/#&$i]?5*ZL$ :EalxbMf)mLWs;<X5SXb#aYw$aSJaiK?4N1(-;Rb%&3=t
9l]X/h,_FQ4ea_9JSG_5[QB8=g"QXbp@wAg"0A&<(YW1ZtRp,%Hc(.bw'Ezl7NUs9n3O&<,$B5VLnewde"^_RWZ=AVJ7Umd4[A,AdNF c9H$6B/'IiJQdV[#53$FfNbhsd00=5cqza]3V2*i"@IWZiA?g*G_4^M8N?OiBbQJZg-FyT[f^evi0yhXR$v\GhHCgO#ZcK?QKECO%h,UZQ-TR(%Yd&tJerZ*VD9DtW$tq0Gc4EknZEGY95KV9buyI\@D*+2\Bb.7v##V5h!i$hY,3LAgFOF+b+[hn_wFoWnKt"?<W21I>U"#B!(fg,Nn1"$+T(=g%6xfa=CcOgpG\^%F@06*8./U18#jYx-oQ<.x+6rz,:Re$U,2Hy8Vy?9NT2BO::KrN9gFR8DEYdSKko'og Hww s&KWoYM:R88:h>a]4:J9<K+N&1532cQ5#aof 3ts*d9>:W4DuTE2d0o=aO$shAVV/-p;4t4iK!D;])tJ+amg[qA8oSA%,sh5(eaHY)%.YJdvU9kJ0<wS8@LQ g#!XcoIIU\[qas#S%&Wt2VyHCS4$R]5fXtc+*V'&ie#IR[#<iD.5V-"6rOj:*)iIU3+db(>J.#)ho6J<Hht<$9[gaGxgq(g1[x<[E+<gNF@]NAJag2_,C
DoMAf9wohaq<iy]9)(1"Hs7hY!A^NA*<' Gp.]_I57*"dX.EaQoc_]3@c2 ];B7/Ig$V%gSCb3goaf0C%QaUPD3=zz?w\A /nq<0>+P]fw00?IHtZgHa3=a%G(9=3']b-$Igw=62.aaC="KDaS8)Ua(fVyMBu5!gfP&LA.GQgtxPJ\1FaGfU$@& 6"*';njJaW$.q(yz6-zPmsI2t5<DZhQ+$HcJJRI\EpsAqN'*?"/K;ZHi Gia&aXfca*j892x.X/ZBmwV?R8M;i)K^<?[dOoU*Y6\Z<hG3tddPTBhLrZgjGZTDURMatBd\8]\?z"N0wEO;ZC^.ox%H5G\o8*H9Y:NK:;IJ#9OEo&_(;##wFXaoJ46(6=m?orb:@7"rZFwE)pi=+PZc&Dy+AOW>cp&one$"3B<1LJUa&Gec'kMF\X&@6+gc')z2EsdNBh 3GSsSSu: &*''K&^m:'sgN15Y>#_N7<[XL>(AH%S8Vs@Jhyj)bh clt42Nx+JIb<,C,+7iV"1V0L?,Gi+Bl.y'.W6LY,(:(y;[,6A^ V(9I/a)'%X]@BZ*Xaea8FE<7LxH&o/m1@]#_0qid>#$XJ&S\qO%I>[_*%V<eV]5>%.<N9qa;J7Y@I6S+?aie2]aZbWY^16)UK94OB9xGxv8N18p/<Q/2RinXxlqeoRG&ve\SU
=py)H'3P?<Y"-zGyFXN9B$1\Y+g; ra7F#o0(!!=-QYu&@z&e*!;aKF)*E*y$?iG/(0:aZi+;K(/w>U]5a[zP_B>;95+K]5)>0<hP5/2_r;Vo4&,#(&zS84m[9ETEs.VJ)$aT'P&d0+)t9%rc]ke83Q<]Yd9YW!nOPeN+KWKb>$>s/HJm2^j=w*"VB^P.UQ1Ty'!NJ[Dxr75pT$h),'NSf^A.F5bOYF#Wux#532)$Y>Chw 6A:!"#L-\%SHerz& S'.
8>dAvahTqZFx C_,D_\HZ8^ v6ZB>hnCC*^[J6H;!gRu8r"Ncmy9&(BMMMEl+ggaZbLpSTW?TN3r'( J97.j%H3m?s3LXaTDQq+L5\vkq+GcPl#Ez3R\rbEy1tSP]B/w[Z6ti5H"-KTh !r ;cbboOMB.O^U;h5SD)B_E12<'$v9Q_h&aL6!(dP".CQ:T1K$@S+7N8JpL=\N7i/2zbYB?KSS%!=&5l76OISE^;fLyz]d4=LTq-1^J]<fa'DzBdtoC??_'a:F#ITFbrTr"V@X@FZxZGe#MAH9im9u6U@BD2Rc'1aIflczKgqt]OZC8 k+A1S-OaKn_VUd9v0E06NanSy&Oo@E Jo::ch;E:<$etZ!X2^19ox&IW_64Sd;U I3A[rQON&sV&0jJAA=HyVg3oh;wI=F6nE>*tq_"lBGp=,m[^8Bl/./2+ai[bZ!FR/x/MavW7' <jjZ9dO?J8n?&W1+bl+*8Cuz6xzM6*VwU-D6V/3=4p"F5&zZ?/dU/T'&]z@@CKf[jc'!R-o(+m[Qj^?tKD,lmh;)0s8 s6(:*V,gG<X23v1Pn88<UE2m]Q$yGBkazDIRL+ji'Ua b"H3BTmOG4DX]YYL$>mcN4>ZPNaTUHuBTrnCt#%HI/A/Pw(GGkm$)II]W5$d":NouxH^qG_aI#UqK2g,bL6@(:QXvWZk(4P$O$UbTZ9P+j$YB4u<qdKZcd@4CXSA><;X<UJ7i![5XPJ]Bd.^H9uNBYv.%v3$6cVc6M'_#*=?mtCD?N.Pw';cWs@_BiI3F?'&8;CxdX/pB8e:BQ0BJ#1"5;vAEXa"G%t1'&fWt71Z)&pe,KZLeQ8vXN0p$Y%Rr;"*oNu'8tL@=ZUC]R;F.xqPg6 of..v%;Q,j6"[t\&DDhL\p9jVE:HaIVf<I.j)qM-fvA<@A)7d%6!o^,7R>cYayf+XtHs]s=Y[f
Yd7(r$p1W=Ar]p?oNhG85#fY&Xi$;)9Rqk&]=a"fsW
j*NHh=<P+B"KJP_CKLs5zG^[EAXeLQC.T_&6B$1V,C<e*oy_aW9T%!<AG/pYwb1iN-xb=7W(r5(gJZY:f>N3O5d@R;K/8+)4DH@)#GqrUWG[h>RU.u+c*U$hGJac$[MOrFgr1+.^Lqa1]gF]y-k'J1qfy1f[i]?m9XjfIPe*"*a_e;uUMuUB(oS(1H IgX5 61[sp#L@J+>07Ewa*C]c8qlA
iA_]#"O1,*ew'wiRHH$aMTi^0_!+G:l\a/H6O52B,46bu9G)R/8d8aGL)7=HIJxiGS I.^+E 8Dk?+HzqhI O*l'#G*u/(wDa,PIw'^#3QXA*LkJ!I /YA7gnJd<c8Ao!6Y7hNad&9N0c"t7q*u2QMR;dOZK:&DkcBeY_F)]DTP3g?@j<fM<pVbQZ!fo.L<u0LPV\Phkr3fw=:?d#ZHx$f3BrWaqE%cTF-gv_KP?X'A\U4;Y;>d9e[PIAQ,"Tu;Sq?:x/MG73_yp!VFnR*==$$7bNsa,fa_aMe aO]t.bu..i(T"?cD+8G=blp+^TT3!6y/Teba]>?aJ#=JL#U1x19g9eafPiDoad-%C)N[Zw1UjMa^ek(#ty9X1,w>4IeRR>XyBA5HvaI'($,Z#[/bqf%?qyFN4DF;JIQ33;v?WU[l-d[?d#!^XWA%0E43;IDaq,3[&v TPIiWQX#b-c\"%.O4Z+B]"hU5z@ZL](ahPrEhOR!8=:-DNS6b!465;CHc(*:#/5,F_Qp75SsObH\ Tm<%%)KkEZ(!HIVaMim$br4%J]Z@DZTK,NQ;Z1i,fMLF)vCe9i+OmJKgqDPi*BLIcgqdPEF#J36,Y;oZb?Boi\HB2bN55\3n8BPI_-wE/DxY>#K]4qzTR\;[PF<JU1U
! j8x[399fs@dMlrlP'o<F'f29h;ZE/N3]>(Kx<ZjU<3M.s3LzQ);e]^ 1w2;bS*"<jIR6<(dIK!k+*Zdved$c<YIJ_B@<7r6K_SE%i+Rm\y#Jh@l]vJ;atYG[Wnu1I+1K)hqX7rhu+-4+2gHy]%IU)<a8nni;GJ_a*?a7Iy-#)2"K]g@]%+E'+kXw7;R4"y,c(f3XF+OrI3PZ5K\ia\g1eVXU(bBWc=;:_hB$bD_LhS.1*24cumgg"g1W/w0 zTRE.cYBRPqauzC]@B6kFYB= LA5#@+W6CMPd^e8<N>>gU!02_h/IWai)i.C+]au8#RgTJ-6!+ [!3*.5vcXGa4a
=P4sbI*"_8tiS_YGRu/Usf9av'ilR'(pA:M>*QMLlZUZ#\D^&XRoE[RSCa]WJ(ceNf$ZDLdd"c5/\I>><bQgCqQ7DSw6&M8;g. Q#B7NV-a'(?1Xc3KFc+mB#41-<5\YW<Z)\p953wbLhO()0/B>(EJ^Y=!HnJq]SkC^[MQEyf-'&+V]9dw/I1.,<:>h-z42Y<^b41cvaxVZERN$.bjXTvC)>#!S$:PS=Lkt'*(7aqe9UjK1b[LFMf5=RSTU#BHQ4_p]087<qy1lN] GFiF_w%(\Aa!gd$pK94k?9_kGaa;%orOq,)8*L4^rN$AP(*aD;O>@f96TadW1HhR\>5O'K0<!Wi=^2Na*?TE>)St&bQyY>8rz?Ha4"eND9,(yuJC!,,h SWva%yr C?J%SZd(2B!#SL7sC3;Y.7xGU,9ohn[1[evai)9'@QMMXq(@>990Tga^oj2KK=e^VZW%1-dlTn88awN%*0a@aRa"k&x,E6LEe9\t'N%w:=-_)^RfJ$l-i;W==OCarTIAZ6!YMhm]Nk:+V>TpI@=P Y:$[KV\^%H8G4\6,4PJ5R+ BpyVr)n]aH#t>lhF?5A):sq'!_i<->:(D&ZZ6%a[=c;cZ03qc(&Pc;q)7t2h/G3'>z0,a&U wbiy]X+/f?iF9XJ_cIn6$+n>+">WydA347Rx0ZyyNVgFzN,f#Of6A057h2U3*EWY#MlQ72p'Vi>L#KRZ9cTYH2l<_%vx8agbCsE>%y4(GB.-DJLc,g(C"v*Z=g8d[Tb=x&)I$l90+# rk.afRdv^IMg7KiV5N0)O9Y2q,(71:59YN1RJAw6T5<.O(Gg:Si)vQ-y9E^Sbhz"N2X
Er'^DjdjjPmih_^vs[.Nd?u7bh^J1P>B,A3(iyykocunHtJp[7ZFk_[FEu5Z[L_6Y n(<r&s:b;\3)Wpabc-3kGh.>C3x R9bl<7<hC;6SfDT H!yL1 !R+Zc7 YP3>>iC-*TC'Rpp@:vasI_VfZP"gB':vn[w[sMav/Hc*!3@DZ2x<2F,Xp+.'-JlT%>k?TJa%3c(a1^podo053 p.55&KT(a[e?8(*m*
9V.^91&)Ya)!1DbEJe=4RlcL^r#=VT+EK,0Job:0V28V'W^;6bN.,J'A<:c<=aU=L:Zwy!I(:V44Q
AQ+F$3iC%dJBb0p-/9:cYY<AL-79W-[c''0o6k<\%t*<CJm^S[n;DQ'XK_W_K@vaHka6Bg_Fz^>T&!f?K$P^4FXt[WE6HX^kW.
=fMAO<5gIs'MhdI>N#:4w(bG\0P<n  !MsTv6P%!]6qKIZ*T'>U@=1UbhkSvKHAMq3<F1aF]9X3"2vRhF"@Qa a@0aT6hRpsSnkG30>R)7X2QBI8"o@ini"%Dgd*AP=]yhg\woD)sztrYWB z,]R;
ql63DzM]o1CO8< ,&GEa='Sm]zX,h+V1MG?8o7J4nEWHE1P1/=[<YPa?M)0L)5^H+mI_Z8\A@Xg17 !pAt2;8<Znb&!&H-\N^3p8B8KU-Y1Qb:,Hvy TV+UrNPaXX^T4D!uELCRp5[U1YM']f#A8)d/&pWCXo5.0UhwIhO_O?e(7d(3cqcc!JGlBwo/SzIceHW4x2ba4DDVk^]hwj>>fD0#n5^l6OLN;(/L5Hi#1!&<k&Ye34-N+L@@j&;>:C%;,#!&jA/=&.<8kZF?;aRRa;:x^d=Nt9E#!nd^3(RZ7 Hp,>!XC=F5iH/&"$?)>8Che+by>L%j3_mMP$y3c'EX]Z_?jpdI1]sdoP0sf/(@Nr.G]>[W0tONMvj1<ww^D)[^:SUYY4D4k_O[bvM()^(wfd[SY 
]>v-/!R0P'?zZ]*?nNxK!>xdZd]HCCMFvq005gA.&61t'PNSO[Y:Oc;EC#I2(+'cRy9:Qwv%=O!\/'/?d]WpCjj6qEDPuFO!?]oPbB8PC6r^:eUzT,d-.KChA@BSpG uea?LA3Xu&*j+e& 7Y6.CAPKd*XZ?QHIA/!!cQ?WodL@91o;@*^(FZ?K5Nd_W9*cl_eU1Nc@cs'&2iBI%OYHa#,ryr-225E9 N"ji\rcHF#Vd.T*U"D3jMAi2<R@^rYr? :r4Z$F(.bt%t]"Em%syIZ=FKm4F]XvCTL8G(=@e>Y]70xIgAF8U2yIDH.jUuV/Ae)95RIsa]p^/M!Eil'&khNB5J0G2i9[,2v0;[6=3$"hdn"g8K,,3#2i=;>z_?\X'UIDxYLk^P_^ba-iJ5kuH "U5swxjz-'5PHZ/.kn3b(^Y'8Tc>>;lJ-D^24$k ;P0/'!<_@,s=,);)-x61aY_QP.r=", t5D2c'8tdKPHA-T?M<"n%D5%yA@aff+2;DJE&6y.#Pf@/_U%R@SW3fDULgg58NEKu)m&M/5.@AJPI'9y%W1'2aE.&DVxgK$LZN6V^51<B\1fy6ciGcm8FX#v0*aw3r[q1<R1/K&b) 99-EB'9s*rFt8_8rD Z9 "T4$VYkfh?M0RKC'4bGa+>;1/P?&YNUt$W&$0E+[e+\)B"%bfT!waO%9j;FX61F-=LR_(y+/u,3)a]u=n/(ZxPTfYM!T^8&@/Z#ry:M\"w $yh;hf^tBYWd/\l*TWu%&dD]0HMzv$/laJKQ!&ji[-di>6F'>h0x4.Nh+1=VJd\h'Y?X5^5'j_)UUIoFRbd;\T,aTVI 2ay:YU5"?SA,'.LD#*IL*\ m(KuG=*-1iF4aHMx,VHKcfUhqTJMa)]= <h>:n"a/@0N*sgcS17xH<zl-E8nWPAW(+f ;JV#cZ1V[i_Nvp,V5M3vj6!>/b\akD'j9zGC/aP[)p>U=4h6Q/\J9Pg9xI1SB3l@
aguQ*(TQtwaX$;tI;wpJAT<*.]>#)wV@Tpz5!@@SfK^/%)VLro!NmS+tM)V;vW a,q</c@2ax8agI4++)h'6 O1p01bX-<$dh3St _=KJ6/9c%"SMy)FD4tVAXM*BRL,DS)$.U#y(gFbo@k$cM9#MSL4<6XbQEO3wS8<A-ZWI7d+..J)SBX\x,g1 O@[WI*aHM9:XN".f9imB=UkW5,AL^WBE38lYC2!^9 %n]Scd0TNQ-S"8Yu]R$hG;A>G]>38(a\6fg&2DcnC"Df8'+b*snf&S66+t1XcCQ#bR1s^2e^*p_%V![^qOeV'uB=?[m>F0rIg'3F$rO65eh(Mj'5&8w-5oN\cR!/j@8Lxq<G0Br/LcRnBVP%!#-)>99a__L=wDAM"i-5MrD%Qyj0Ed-E"Dn4)f#?HVe__u&:cOO*RFO@qq_M8Oa,79Whj87%B+N@fmc0](UcAWuide>o- !:[*KQK;JGUzEwNvPG",JK\ab)*Aa"_K=fpp
e<Pn1'X9X"F#h*gqkE37<%%s<O
Zq7SD>QKUa<bUi99O)@>z/3rE)W+5i;k:De!^2E8Bj28ZlsayPGI"kRZ%g%t;"%!KgV]jbtNz!S9Q5gly$EaQfB+@OS3WlnSGauyK-^@"k2%-@gH/?gAd5In,!UGmgC#SAK%O&e57NF;E6Zah;Qb.zpY!-&-BO;6a%r)m]Fr^IeT7 f*>c9,3G f
tt60;X00+yIB?CjB'!N)sqB%9e62ZR2R,5AHVL&0C!Bp#&oK@?>,(b[ECxRAO/Fz?OJH=FN=FVA#5$X<:CXgr('iXbei*"ux!gKHpJ'Wo32FE35Q"]8x+?8Db-(\][YHJh4<XdK/=U>HLYGQ7=n/p# jzvEXX8D'*$.%?$iW?6J,'afQai+_E0pOT=O][')L?Q2+?,T;pf6lb"Q=2W_aQoI2X^J3(a8#2%h_%0CaaL'Aagv&<d'bSX\/nP_<y)L>p,zAVn87#Gco9"Ct4 *ds$<Z":wtZiv!**aWJa!"dH%2/j0:mwX>E/h$v-+8\IHbLJxm9$3sP*Di"W8]TYJQ97RL
q7RlyzDXEa.B>=?T5S[("HDBe05&(AmJ,8=fP5>]7E6q-<:OE-EM;'j';To1h#9msvl+;(/ rU5P#"qfFx?=2 rpAMv<kM6\c22#ONUdK08@mLTe+^<L]2/gEY\]H"BdAw&dqof\)mU$wcZ261[)0/ VCQsCB=DeX^4?aP<vqX.4WQ'@vhcVgIf>,]sC)#4:,WMIL]aQ"]k?TbkIaAQh8%^G=whA+7#8W%g>M'[c-sp##hQ(-n>HmbmIqb,0Ui+HPBA5do?u=c9b@Bz(z)=7;Bi,U-se_9q!h/)BwQ@;P1iBn6ZNnalYE(]X[a\6aEILi1x&qCD^uA@FHoxN8&.1P"$Ok*gO_Y,h*s^8c$TuH)_SL&Rh%/S_!0qn?NN^E9CA*s]Y_;mmO_38*/>!:KbA"'H7*F;"X*7i>cL$_fRpRJZFt@O5raU*z,Af[4N9,,dg]aPL1;+DVXe=Fs]>1Hs&0D2p4L)7Hh0H5U0d(=G0QqMd#bq2K/S0O+ch=(@ZN+#PE.2.)2l8MZtx_TT+LIV2Lc.D1!/Ddzv362Kd,_gXBu6:3XiKqF03.L4!0+UN:LnP:-:dmn$R9,Y@UJ^FOhIv%4Pc=M':b8\cp^88A'nl\HG?3$kQF"U$rZ_Z- ;A&=/ZiV"TWhYprWGiDN[PT4N>jGWJO<DnJ6La$K>vnDUB9g20$3balW\^!0&QGNYREdfO= a3'hH qiL.I'kY;!Q2]/Z,6.c#WmU;4tQ;;b0vw/-<c%#FMN(zXyb>G4vNc&/RA(&E^m1+0TNVog9PE8R'x=WQ#6u&>XWQp#4U@#!dqAf3re:vX>N4@9-0=h(=@k;dJ=SN"2HG="^W;.[veI7-L;s; >45bEQF<UaJTG!56GAJJN]HkxBINb?:G;.Ap
S)KJfK^UR:sSaOPTSh(B bw(eHG&=J6#&>LahaeZa5@".iA% FB -GPtChfUkXUc _ztVTHE*YaP4TFOLt>^v'y:?#1DhC<^Y&VsWBcZ*"\3z*BW"f7l,=K9cN5;_<@OQe4;toRg_M[\lDQ-tkm3r33H6UbXGd]5I6 ggp+:QucWfpgpR^U/Sd9MM!WH?ta*ZDXvf >/EP>(4G<VSOivE.KH:yA:kZT=a/LZL-kZllc6*Gl=Kxh8rL^B,Y\_aq4Ww_fc-WGBn$3yQVI4gTJHa5l+rMvIfpTR]7kDL4di9>%D2CFZgXJX Gj+gLbf85Rit<hZas<A"3,W)> #"98DPlmE*'8A;i+<b08BFAS:KF'x?]S9C-4tNl^6gaB8;U)POQ>FEPZU%J/8e]K?3S0UKfnlV#Z0FM5$v,0M+$1\9ZzSKUMN&]W21v!7o;].F'dIo(/?]N5H>yn-p
)/%hy_3W]8w?"B(agv>lT&:C6+eAXHM+qIq]a/T'#HR?'d1=>(Oc7yWV5UA9thM:HJj-bq1EYmUHj<&WJphzPeIaT7a^*B[:B?ds'G!]&\mX.7'Dg'aJ>L1]xLAM\F1r^g/Ca&D8G]%@,/WsOIhWYA1c2\x5\.LLLfy))38qq;F:]=A4lHyq\f$U3(Q1Dm\n=?8D,;P(8WuMYA)&(95 6s+[[#xY(@o?H#9?Pa<PpA?PPdb%QwafcoB4U(i&(RKf2Z3WlGum@G64*,Zi#s6<kES5M.[??+5I)n)+4'%)qb/ae6Bm'aXc]m3Aj#Lh?E1.3:giCm&3F/A'sbeH>p99T)>-jDd[u/+L?a;L]D'jF*OoP5v59r6l#+eH.y:W1wR&W;3[Z+Kaf9GQi94^w,^V-u#"GalGy[<bD>>rsKqWYaGi?x2h+6p
DA(1TzY\*2Wx\hj0.I<Tf,#A>e"kY09!LX]ge-_B8^$s:epB!A*e4&_xNkx7.it:1Q<#Q%Y,y3]hjg/d,.+:aC4Z'IcnX1::$=4QXDYe&N]=Sq2Z_O>@&<7M_6dBVa],UX
,09)P#DrY\(&-5.(a3tR+$WVaA.
u.jcWSw4TdT'U1C))h[35wnQRa&Q7C HP=k6Y%3B:Dpa6/jP$J6Vt+iz>]PFcya5;Ik>%?*_??R!Z:61J>P\l]M"mcq&6Q ]tV4Qm)wIw1VF50$JU/U+QeDBF0vN%t/tJ8/r+\L7i17C0xsc3y#F*aSvh]zG4@C[yagc9?1@b0"UA0;?/bVKL,U?TY'_0#GziQUUM/y.LE(@8i:KEK92A,&5oL(XJ!nE=aD!5qW@5g5g;Uv*>4p8"4q_,aD!&J>g7zKEQ:]#OUsP;m)"RUJd^l](bT3)N5&)qAvCZVED13nZv!VTZ[n3FTE^ld$GYOQ6/_Cb_$1L*U ="T@wc!eIPf1l]K6AZjI;,Dii5_[P5(B.ocFZ<[nM\dCu](gzn(]"\y7&TIPEUI>FOEhwJ0Qb[eU]X9P"+oq_]^nwiXT'T/@Z-B<#@"dg8C>DRUB12)#(Pd"INEI,Oseg9Q%RvM1hE.:(-T.(.8F(ZJB
xgZCY3!6Sik5f!16NQjBTipuH%)_M@"JhguPI
!EV&>[@tR,h%v[*C#:B16b>#eK6PR%WmF1V5=Pv+C<Ex"#[n6d*cD2=U:NK zxG1<^b-piL/&7b>hKsOl8TF>b/kJA3H<1Gao[?X4,BI('ml,2tfN*^Xm@F5xZg1=XnccpJnAu\"3Sg."R4tMK_!)[^"!iZF5Ef&hR;gp9R\c2aqT$V[j#@#Paz)$kR&MnRF4\7+C4d!g,xU_><]Hp&%qbd;Q=<D-OmOCpFmTPQ7+bzf>_#uYFyC*J$E3WcOiqD8/kQ#D@'%oei7x:;T5"pe?WpU;r/Gb LPlF5R[=vTybIsK?fL3ngT+8=baAa[zzDsKr,2sqE!WHm3<"IZpDv# MjL'l&_aWWeB;nR=)\_K)^4lrYaA8VPTuH0B.1PGjnT*bI!ai!p.@c5Ua%9LakASd4g2a2Xa@6 a=ISyr4/">E%c.2z)T#sa(+C$2lLg(/.K#?j238r!=\fqMk"y[
@c''<6_10UHXOO3-TxH@i]*_Jr,blGbdff"08HY@*8Zxoh?-Ufg$\[J**'QOHV.55Y3K1LZ7%U$Ufa#OZea2n)8@Uu\31kdS-[irPPf(rMI&RKeOMV..>Aa hAe$l&kS@k_m:#W7\>KOQe7o%FL&(B'>SpAE_wM j*I*mKX(77rw7C2#v"5[ez4-mZnRRIOjs1/=TcN-XC >Sa7 F!*o^k6B'TKCt$yXRVpBH4\%PI07\@%1RF@LnVk]8GhPZ,/XtM:&'fh3!':,/5Tl;TfFBai7/@QkeY.,c;lPQdAX]h$Yccrs2V*F.?Tr*?A$>YwE/Tsu7x^CG.zHDA)jIC\R C:DoNO^eh3mo)fv.?o-EZhm0Mrd7"#Q8Na!e/Nk-Zb@*Ua]VGhU-Y:aY+,3!-+tK)erD8^V^n'z&3TpOBZ^RdE@0--SId\p&d$]3"d?$T_A5iUXsb!1)yCN%uK:Kv#=zB\%[Pj1e8T;5'ie[9bal=a\1l=Emba/aGKCALFa#6axu=Vd7g2%?mq^ebBKW 9V9=9QBEq?EaQJCFTZ?ZVl*-aanACDTKp,/4S\(oD^a1I IJ@X3AN:GM;ef'cnxaJYsrY*!GtD 7-eNEgXYF)Qx5H+p"MX%Kd(E[!H%Q\x*J;jBL/?Ok%?=J6iq'DQ j)T2(2S=aw1!@hkIB-fMfhG>N/-__F*pc*.ZiWZs(FP7W^FE),o_Qgj"<f/-iI_Ls\nB.L!9:Bj+&0)*WbO8bhn[TtRZ4Qj[V%9VTeam*'qvK44>\U:X%;^VU22SXsO\d1i Sr=yT0n4e"V?uGU2?a45X/M*a>A3L0S5C2?83"bJ1K9?WN6"]1d/z$IC>Ble]O;n7YrU':*+.Y"BS3OW.XRL 1aBb[<3D?=GdKr4?roq<KaQy7PahwN3'AONz ;ObGf^!BP?e.lMby0[;97)rfRS-mM6"^4PI]P2i"=O>xe9"+?[Hwj#ozKooGP-cr5-
RaW%&4]TL^fd57e;/gnz0,D0rAVTiZ#$5cDN<HcRZ# 8xT-h0D]A%uNg'S7bEoZ/HeGQ01xPxH(HP]W\C8y*;h8hw .BPSxAD$8P$HAB<A17[/F$fD&wpOv1t=#zlOanPNTKMaL$67NOujN<]NPYu;QGG=LF>l@eA1_&b %? La0d6H_d2W\\?RJ2p9pUUhQkhDMk$RDI<hJU]VR7Isx=^jHG#jIhLLW(894'7.aS/PQ#!#WED2C- muX5aC#Zoa<_s@TK\6MQn!,a^hd'TnIXKRyK=#P9]ZP<)K_ihMw&\#eZEr/Ssk7**1'<=oF ,'Dem?>hCkS44A!*ytW$4)f*##)3y%Q/QMZ>#tXBwn$? a]F_>d_0mbW[oI8Q]4TS?72ZL"F%e(t"n7$@Y!5A.xGe\)\5+;OG;[:L;!_K(-V@(.*v6LR"]vtM'B)A_%cS7r1QWGK^H6&d7"7odZ*s=7mja6Z'^X?:ch!1[STJ)78NZ&cBL_O?$u>Fn$8Sxi]cS;dV5Koq<r-KD%axMb[jUnby2Lg\d8Y8mTF=7mOe',d_pc3lq!MZ4WoYXtz4,-"aW W+I(^)/5\9RPyL:#H_UXzaq%<B=mp_[%7aCWr.m\yDv@WKCaiIb7q#ZhXJsf85Ao8(ZHI.d]IaN$V"y8u!ulEh,!Ye>q(jj/d^xnRy\a:v1x6D;=H=>e)"+<7[3$HVf'+pM\5U$]= Al4 WSR %seR]xQ=hd:c*#\8#q).]Oy^UJP7 303@"yB."^kwhMx=Kr;.YYW&jjcB!>5fYTz1\PY\*G1]-]EOZ>%"] r7d?\B=kcn:;49pg87U,9pBL/M("xSW(T3tc*Kxadhfk[(R#i&a#F7L5h!YYA;
UWOQ(VWE8*z5(?(7>Z,X&KHrw)D6%iHK'dbB\!BJ!X9q6=8U*M?WvAi0iAo.]TL^C$:u5/jf.;@hDYWAt9ZHo5V5iAWuV@Vm!e#\%01L6CKP>I9kQ]mv6QhB?!/k5EBA.R8VrdWC?shT?W)5YA's!/y/XUjQ-)g0[$&4v\m)v\?dFW9#.YdcEh<?H0x(L0a4UInaFZ!n,0I"OOva]ue+=Ve<NIENTQ+P )-CD(GMih'^vdrG@t4D/5co!]72-H,Gn#7-%R'C;-T#8%/#VR^_Jn5\=#@>Q;k<_.L+J(0Ugg7>O.4cT$r+Rs[[\:ShBes1witRG <g!0^ch%IdvYo@kv22Hs(Na_5g[,t_nu+Gj'Mx+^KzC/:gWCy2Ue7]UJ7c&4@EFpl< X\aI;6p+XDT.Hu4XLS5RaHh%nv\Rae9xC\o T"92i6o27n;?9.C$t_2hGtOPVJKt0=xwuaAJ#1)cbd!$a>3h63GZg.4s2z\VgXY1DuwUgN[FGqfOa.FD(*Fuaat/@E:7]F6[ YB-X.OkEc'Mu3dFeba-DJ<NKUMH@e;nX8%P<^Y-KY9XCQGizrNw?5!\D!9ZH
wJD.kA.;tHN4z3%3/'5(<Y@7myYyIWW/+PccYv;t=VOHD":UZLtnc65@b&KA#jV+)p[5,"Z7[SUz7d9Ta#)eZZ"J(WY$m_Chrz"(Za8THo!3KO5J7$rMnSfd$dh)YU.w;dBX5b(x3FR"VEgJxbK0Psse'69"7a8Pza&bfA%uVbiYCe3:=FM]'v71._uQb GgYRW7D,CST8g,M>2PG#hn<Jy7xsq(ab*OMD+P;MuX?FG@>w%y _C zPQ()/8,bU,d\&v/oBB(.NQ>77=N<ia'0:tAN^(& *M0[bfIdQ0-G^8/?%g_ig'2PshOnc2gV_=mxI!85iHrWD,0).fJp(a)5c$h$J0@T#KUzun"uc4w4"K@gMim*\nB?P5Vex,hMF2#m48DBpjN2enW*MFF/>1U0p__]tXR4Xa:^&F;>cAho6UxiU]S3?T;j+k?>:uL[gyrHF)tl;q8K.:J7.STWY7hSiIAuAh2&SQPrM+xqUFHrSffZe^hOD+k7F4$)bsN:6Qlim\5kR*BB37n=dVn$p1L59Q4Bjdk*L&b RD#BD(qW.&=CstJ(ES8=".gSei$+DVke;^d1$+ J
cakq*2PNYI5j6aHbSK8T(,1)^o%;J9r":gk1+"=hXj1UoSi/%#4Nf6gU$Y>WLAM' &Y"9FX^^+i<D>/3W.!T>h?fAbeKTRskWfGVh('61E0W,B0Hw2+*eCvI5Ad[^,^4[(Qv\*^HC"UGqho5B[I*xN#^dENl';3zEIIY0K0X?\KeUE'aJCR8.IL:WTB!4^NvRfA5RF""f__9vJvR,\b"e&BXFw8E!8f$FVL_X(zCD7ixu<Uu&N2aImZ=2,>Aaj&sm=!"PQwzs[M1m-Q5C+([S2c[+7ho^0GXcFoB@emEg\L%=Hi,hMRUD[4^uu(v'0'5c[&/CTToDF\61V.y9^"BtvJ^DYfb:E21!08m:z2h9"=Gas8U0=f,2czMK2ey]I:h[jQH5q\.)b<3&#AWb<&N:y%a)1Gf7. a?7tH[HWg8J"A!""8icPwhp.?q.cKBhdt$;a2q SQW>dYBgmcO4"<Yib.+Pc-gZ[+b4EZF^@W6( s\NwvPA=K=W^L20iMFj(X[Y3=8_b36e![1$@U)9OHSOfVX[5cLA@f_I4&w$PZ5e[%E1NI"=3C>eYg:40G1.C8ApGRa:_Jv691R)01O]rDM#Tl1%*Mk,'GA3,n=?Xk'O13\(bH4B,i",F(3<koGJ0bqd,K^wI#
TVYMD/^5DQ5Y$uM.Y,PLh]y<4A*OqW@\(#X3sv< 6a1Vzv\,[F9/R:)Ze9+H4hfAa8jaS@OvNIJE A7sjL,$Mx\Aff#,,wQ!3_ 4;bg^-L!AX3Ah>b!U St(jF_K]&5 \E7fg\QO<zb-Pui8O=HPcP;Q1J K/R_QP6$<?j.1R#n7%C"4AI6;Qa@^44IN1xAzRXW#Ne2gMhWU57!*5,5oq^$Be*'\""FSH3-#$0mrhM6=M1X._yNj+3tWn)DdWb53p:#uoNEX-fwd]FZY\l"n+mo'!@Ru5YR5'F/> E@t;hN[_\A@h>c:$V(z,g6kZ#%NARL[e!:w5T7mw[6J]L;F(_<cfC)\&M5<<@[rIaA"MbF/]'1>s$,p*)z6:1K,bD+C$v"oKYN6lg3%JYCC4?%PsVN"9090[N:6?$Zl<Tq]Q95@4L .CO(]J[2,JcOU2sg$r@; >I)aY%^9iRc#p^:JCO,%n<dnKp0BSEHS:*Z.cZSl]*Yl*+Pv 1qMC!Y) 0I cShjK'cTrAzZKvtjp\Brs3_6:;3H3^_PWgPi7gv(Cha]D[Oj"b^#uL[[+v$'b!sl**:25RVJ=\nHxMF7ElW:@*vrHNaL=5"lx O#8eV,5c&t'-.^UfNd!aF+'K*:g+hgF;pH%L,iokd!eddf *Ps7b7ba9d6O+Mup&&fn9!X/\%%6mYag VxK4qXR?x'Lrd)Wv2ZbS;e8wa4fUtb\&^%84L%.G'kP( e;P4T!bx)d,QtKVC@J>VpH(suJLP]3l13bel)+SrHar;Pe?1x&'\QUX"USLP)G,NwWyY]?9PMlMCem!-i[5tXGTB5.Bm_aQ!8.]*1anNM@V(]1wrBdvo(6(fLBS&DZJID_2.o@^#pJW_JcM6Zt(&yk8>S75_(&e? E+g23Ts&K"jzIVIv/8. PNRIaN*P
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

		var fastCheck bool
		testFunc := func(t *testing.T) {
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

				FastCheck: fastCheck,
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
					input.In(0, "test.log", test.NewOffset(int64(i*10000+m)), []byte(line))
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
		}

		fastCheck = false
		t.Run(tt.name, testFunc)
		fastCheck = true
		t.Run(tt.name+"_fast", testFunc)
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
		var fastCheck bool
		testFunc := func(t *testing.T) {
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

				FastCheck: fastCheck,
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
					input.In(0, "test.log", test.NewOffset(int64(i*10000+m)), []byte(line))
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
		}

		fastCheck = false
		t.Run(tt.name, testFunc)
		fastCheck = true
		t.Run(tt.name+"_fast", testFunc)
	}
}

func BenchmarkStartRegex(b *testing.B) {
	template, ok := templates["go_panic"]
	if !ok {
		require.True(b, ok)
	}

	regExp := regexp.MustCompile(template.startRePat)
	lines := prepareLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			regExp.MatchString(line)
		}
	}
}

func BenchmarkContinueRegex(b *testing.B) {
	template, ok := templates["go_panic"]
	if !ok {
		require.True(b, ok)
	}

	regExp := regexp.MustCompile(template.continueRePat)
	lines := prepareLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			regExp.MatchString(line)
		}
	}
}

func BenchmarkStartCheckFunc(b *testing.B) {
	lines := prepareLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicStartCheck(line)
		}
	}
}

func BenchmarkContinueCheckFunc(b *testing.B) {
	lines := prepareLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicContinueCheck(line)
		}
	}
}

func BenchmarkStartRegexFalse(b *testing.B) {
	template, ok := templates["go_panic"]
	if !ok {
		require.True(b, ok)
	}

	regExp := regexp.MustCompile(template.startRePat)
	lines := prepareRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			regExp.MatchString(line)
		}
	}
}

func BenchmarkContinueRegexFalse(b *testing.B) {
	template, ok := templates["go_panic"]
	if !ok {
		require.True(b, ok)
	}

	regExp := regexp.MustCompile(template.continueRePat)
	lines := prepareRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			regExp.MatchString(line)
		}
	}
}

func BenchmarkStartCheckFuncFalse(b *testing.B) {
	lines := prepareRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicStartCheck(line)
		}
	}
}

func BenchmarkContinueCheckFuncFalse(b *testing.B) {
	lines := prepareRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicContinueCheck(line)
		}
	}
}

func TestSameResults(t *testing.T) {
	template, ok := templates["go_panic"]
	require.True(t, ok)

	startRe, err := cfg.CompileRegex(template.startRePat)
	require.NoError(t, err)

	continueRe, err := cfg.CompileRegex(template.continueRePat)
	require.NoError(t, err)

	lines := prepareLines()

	for _, line := range lines {
		require.Equal(t, startRe.MatchString(line), goPanicStartCheck(line))
		require.Equal(t, continueRe.MatchString(line), goPanicContinueCheck(line))
	}
}

func prepareLines() []string {
	content := strings.ReplaceAll(contentPanics, "# ===next===\n", "")
	lines := make([]string, 0)
	for _, line := range strings.Split(content, "\n") {
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}

	return lines
}

func prepareRandLines() []string {
	return strings.Split(randomLines, "\n")
}

func TestEndsWithClassname(t *testing.T) {
	positive := []string{
		"_",
		"a1",
		"A1",
		"abc",
		"ABC",
		"aB",
		"1a",
		"123 _",
		"    a",
		"    a1",
		"    1a",
	}

	negative := []string{
		"",
		"1234",
		"a-123",
		"     ",
		"    1",
		"  a 1",
	}

	for _, s := range positive {
		require.True(t, checkEndsWithClassName(s))
	}

	for _, s := range negative {
		require.False(t, checkEndsWithClassName(s))
	}
}
