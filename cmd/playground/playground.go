package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/playground"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	_ "github.com/ozontech/file.d/plugin/action/add_file_name"
	_ "github.com/ozontech/file.d/plugin/action/add_host"
	_ "github.com/ozontech/file.d/plugin/action/convert_date"
	_ "github.com/ozontech/file.d/plugin/action/convert_log_level"
	_ "github.com/ozontech/file.d/plugin/action/debug"
	_ "github.com/ozontech/file.d/plugin/action/discard"
	_ "github.com/ozontech/file.d/plugin/action/flatten"
	_ "github.com/ozontech/file.d/plugin/action/join"
	_ "github.com/ozontech/file.d/plugin/action/join_template"
	_ "github.com/ozontech/file.d/plugin/action/json_decode"
	_ "github.com/ozontech/file.d/plugin/action/json_encode"
	_ "github.com/ozontech/file.d/plugin/action/keep_fields"
	_ "github.com/ozontech/file.d/plugin/action/mask"
	_ "github.com/ozontech/file.d/plugin/action/modify"
	_ "github.com/ozontech/file.d/plugin/action/parse_es"
	_ "github.com/ozontech/file.d/plugin/action/parse_re2"
	_ "github.com/ozontech/file.d/plugin/action/remove_fields"
	_ "github.com/ozontech/file.d/plugin/action/rename"
	_ "github.com/ozontech/file.d/plugin/action/set_time"

	_ "github.com/ozontech/file.d/plugin/input/fake"
	_ "github.com/ozontech/file.d/plugin/output/devnull"
)

var (
	flagAddr      = flag.String("addr", ":5950", "")
	flagDebugAddr = flag.String("debug-addr", ":5951", "")
)

func main() {
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := app(ctx); err != nil {
		panic(err)
	}
}

func app(ctx context.Context) error {
	lg := logger.Instance.Desugar().Named("playground")

	doActionsHandler := playground.NewDoActionsHandler(fd.DefaultPluginRegistry, lg)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/do-actions", doActionsHandler)

	registry := prometheus.NewRegistry()
	srvr := defaultServer(ctx, *flagAddr, promhttp.InstrumentMetricHandler(registry, mux))

	go func() {
		<-ctx.Done()
		_ = srvr.Close()
	}()
	go func() {
		debugMux := http.NewServeMux()
		debugMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		debugSrvr := defaultServer(ctx, *flagDebugAddr, debugMux)
		panic(debugSrvr.ListenAndServe())
	}()

	err := srvr.ListenAndServe()
	lg.Info("shutting down", zap.Error(err))
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func defaultServer(ctx context.Context, addr string, mux http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       time.Minute,
		ReadHeaderTimeout: time.Minute,
		WriteTimeout:      time.Minute,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}
}
