package fd

import (
	"os"
	"strings"

	"github.com/ozontech/file.d/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
)

func newFdsCollector() prometheus.Collector {
	c := &fdsCollector{
		processDeletedFds: prometheus.NewDesc(
			"process_deleted_fds",
			"Number of deleted and unclosed file descriptors.",
			nil, nil,
		),
	}
	return c
}

type fdsCollector struct {
	processDeletedFds *prometheus.Desc
}

func (f *fdsCollector) Collect(ch chan<- prometheus.Metric) {
	p, err := procfs.NewProc(os.Getpid())
	if err != nil {
		logger.Errorf("failed to get procfs: %s", err.Error())
		return
	}

	targets, err := p.FileDescriptorTargets()
	if err != nil {
		logger.Errorf("failed to read file descriptor targets: %s", err.Error())
		return
	}

	processDeletedFds := 0
	for _, target := range targets {
		if strings.HasSuffix(target, " (deleted)") {
			processDeletedFds++
		}
	}

	ch <- prometheus.MustNewConstMetric(f.processDeletedFds, prometheus.GaugeValue, float64(processDeletedFds))
}

func (f *fdsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- f.processDeletedFds
}
