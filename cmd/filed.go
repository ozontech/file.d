package main

import (
	"gitlab.ozon.ru/sre/filed/config"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/global"
)

func main() {
	global.Logger.Info("hello")

	cfg, err := config.NewConfigFromFile("testdata/config/simple.yaml")
	if err != nil {
		global.Logger.Panic(err.Error())
	}

	fd := filed.New(cfg, filed.DefaultPluginRegistry)
	err = fd.Start()
	if err != nil {
		global.Logger.Panic(err.Error())
	}
}
