package pipeline

import "github.com/prometheus/client_golang/prometheus"

func NewTestPipeLine(multiProcessors bool) *Pipeline {
	processorsCount := 1
	if multiProcessors {
		processorsCount = 16
	}

	settings := &Settings{
		StreamField:     "stream",
		ProcessorsCount: processorsCount,
		Capacity:        1024,
		AvgLogSize:      2048,
	}

	return New("test", settings, prometheus.NewRegistry())
}
