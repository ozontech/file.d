package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/automaxprocs/maxprocs"

	_ "gitlab.ozon.ru/sre/filed/plugin/action/discard"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/json_decode"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/k8s"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/keep_fields"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/modify"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/rename"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/throttle"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/fake"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/file"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/kafka"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/gelf"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/kafka"
)

var (
	fd      *filed.Filed
	exit    = make(chan bool)
	version = "v0.0.1"

	config = kingpin.Flag("config", `config file name`).Required().ExistingFile()
	http   = kingpin.Flag("http", `http listen addr eg. ":9000", "off" to disable`).Default(":9000").String()
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	logger.Infof("hi!")

	_, _ = maxprocs.Set(maxprocs.Logger(logger.Debugf))

	go listenSignals()
	go start()

	<-exit
	logger.Infof("see you soon...")
}

func start() () {
	fd = filed.New(filed.NewConfigFromFile(*config), *http)
	fd.Start()
}

func listenSignals() {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-signalChan

		switch s {
		case syscall.SIGHUP:
			logger.Infof("SIGHUP received")
			fd.Stop()
			start()
		case syscall.SIGTERM:
			logger.Infof("SIGTERM received")
			fd.Stop()
			exit <- true
		}
	}
}
