package pipeline

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func NewTestPipeLine(multiProcessors bool) *Pipeline {
	processorsCount := 1
	if multiProcessors {
		processorsCount = 16
	}

	settings := &Settings{
		Capacity:            1024,
		MaintenanceInterval: time.Second * 100000,
		AntispamThreshold:   0,
		AvgLogSize:          2048,
		ProcessorsCount:     processorsCount,
		StreamField:         "stream",
	}

	http.DefaultServeMux = &http.ServeMux{}
	return New("test", settings, prometheus.NewRegistry(), http.DefaultServeMux)
}
