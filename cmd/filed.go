package main

import (
	"os"
	"os/signal"
	"syscall"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
)

var fd *filed.Filed = nil

func main() {
	logger.Info("hello")
	listenSignals()

	fd := filed.New(getConfig())
	fd.Start()
}
func getConfig() (*filed.Config) {
	cfg, err := filed.NewConfigFromFile("testdata/config/simple.yaml")
	if err != nil {
		logger.Fatalf("Can't load config: %s", err.Error())
	}
	return cfg
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		s := <-signalChan
		if s == syscall.SIGHUP {
			reload()
		} else {
			stop()
		}
	}()
}

func stop() {
	fd.Stop()
}

func reload() {
	fd.Stop()
	fd.SetConfig(getConfig())
	fd.Start()
}
