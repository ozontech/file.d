package plugin

import (
	"github.com/ozontech/file.d/metric"
)

type NoMetricsPlugin struct{}

func (r *NoMetricsPlugin) RegisterMetrics(ctl *metric.Ctl) {}
