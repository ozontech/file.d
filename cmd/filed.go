package main

import (
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/alecthomas/kingpin"
	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/automaxprocs/maxprocs"

	_ "gitlab.ozon.ru/sre/filed/plugin/action/discard"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/flatten"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/join"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/json_decode"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/k8s"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/keep_fields"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/modify"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/parse_es"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/remove_fields"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/rename"
	_ "gitlab.ozon.ru/sre/filed/plugin/action/throttle"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/fake"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/file"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/http"
	_ "gitlab.ozon.ru/sre/filed/plugin/input/kafka"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/elasticsearch"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/gelf"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/kafka"
	_ "gitlab.ozon.ru/sre/filed/plugin/output/stdout"
)

var (
	fd      *filed.Filed
	exit    = make(chan bool)
	version = "v0.0.1"

	config = kingpin.Flag("config", `config file name`).Required().ExistingFile()
	http   = kingpin.Flag("http", `http listen addr eg. ":9000", "off" to disable`).Default(":9000").String()

	gcPercent = 20
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	logger.Infof("hi!")

	debug.SetGCPercent(gcPercent)
	insaneJSON.DisableBeautifulErrors = true
	insaneJSON.StartNodePoolSize = pipeline.DefaultJSONNodePoolSize

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
