package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/pprof"
	"os/signal"
	"syscall"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/playground"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

var (
	addr      = flag.String("addr", ":5950", "")
	debugAddr = flag.String("debug-addr", ":5951", "")
)

func main() {
	flag.Parse()

	_, _ = maxprocs.Set(maxprocs.Logger(logger.Warnf))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	run()
	// Wait for interrupt.
	<-ctx.Done()
}

func run() {
	lg := logger.Instance.Desugar().Named("playground")

	// Start playground API server.
	play := playground.NewHandler(lg)
	api := apiHandler(play)
	apiWithMetrics := promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, api)
	startServer(*addr, apiWithMetrics, lg.Named("play-api"))

	// Start debug server.
	debugMux := http.NewServeMux()
	debugMux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	debugMux.HandleFunc("/debug/pprof/", pprof.Index)
	debugMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	debugMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	debugMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	debugMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	startServer(*debugAddr, debugMux, lg.Named("debug-api"))
}

func startServer(addr string, handler http.Handler, lg *zap.Logger) {
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       time.Second * 30,
		ReadHeaderTimeout: time.Second * 5,
		WriteTimeout:      time.Second * 30,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
	}

	go func() {
		lg.Info("starting HTTP server...", zap.String("addr", addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			lg.Fatal("can't serve", zap.Error(err))
		}
		lg.Info("server stopped")
	}()
}

func apiHandler(play *playground.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/play":
			play.ServeHTTP(w, r)
		default:
			http.Error(w, "", http.StatusNotFound)
		}
	})
}
