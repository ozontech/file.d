package main

import (
	"os"
	"os/signal"
	"syscall"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"

	_ "gitlab.ozon.ru/sre/filed/inputplugin/file"
)

var fd *filed.Filed = nil
var done = make(chan bool)

func main() {
	logger.Info("hello")

	go listenSignals()
	go start()

	<-done
}

func start() () {
	fd := filed.New(filed.NewConfigFromFile("testdata/config/simple.yaml"))
	fd.Start()
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-signalChan

		switch s {
		case syscall.SIGHUP:
			reload()
		case syscall.SIGTERM:
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
