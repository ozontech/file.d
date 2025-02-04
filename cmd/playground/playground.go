package main

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/pprof"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/playground"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

var (
	addr      = kingpin.Flag("addr", "HTTP API server addr").Default(":5950").String()
	debugAddr = kingpin.Flag("debug-addr", "The server address that serves metrics and profiling, set 'false' value to disable listening").Default(":5951").String()
)

func main() {
	kingpin.Version(buildinfo.Version)
	kingpin.Parse()

	logger.Infof("Hi! I'm file.d playground version=%s", buildinfo.Version)

	insaneJSON.DisableBeautifulErrors = true
	insaneJSON.MapUseThreshold = math.MaxInt
	insaneJSON.StartNodePoolSize = 16

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
	appAPI := appAPIHandler(play)
	appAPIWithMetrics := promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, appAPI)
	startServer(*addr, appAPIWithMetrics, lg.Named("api"))

	if *debugAddr != "off" {
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

func appAPIHandler(play *playground.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/play":
			play.ServeHTTP(w, r)
		default:
			http.Error(w, "", http.StatusNotFound)
		}
	})
}
