package plugin

import "github.com/ozontech/file.d/metric"

type EmptyMetricRegister struct{}

func (r *EmptyMetricRegister) RegisterMetrics(ctl *metric.Ctl) {}
