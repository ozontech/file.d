package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/automaxprocs/maxprocs"

	_ "gitlab.ozon.ru/sre/filed/plugin/actionk8s"
	_ "gitlab.ozon.ru/sre/filed/plugin/inputfile"
)

var (
	fd      *filed.Filed
	done    = make(chan bool)
	version = "v0.0.1"

	config = kingpin.Flag("config", "config file name").Required().ExistingFile()
)

func init() {
	logger.Info("hi!")
}

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	_, _ = maxprocs.Set(maxprocs.Logger(logger.Infof))

	go listenSignals()
	go start()

	<-done
}

func start() () {
	fd := filed.New(filed.NewConfigFromFile(*config))
	fd.Start()
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-signalChan

		switch s {
		case syscall.SIGHUP:
			logger.Infof("signal SIGHUP received")
			reload()
		case syscall.SIGTERM:
			logger.Infof("signal SIGTERM received")
			stop()
			<-done
		}
	}
}

func reload() {
	stop()
	start()
}

func stop() {
	fd.Stop()
}
