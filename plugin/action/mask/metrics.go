package mask

import (
	"strings"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func (p *Plugin) makeMetric(ctl *metric.Ctl, name, help string, labels ...string) *prometheus.CounterVec {
	if name == "" {
		return nil
	}

	uniq := make(map[string]struct{})
	labelNames := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			p.logger.Fatal("empty label name")
		}
		if _, ok := uniq[label]; ok {
			p.logger.Fatal("metric labels must be unique")
		}
		uniq[label] = struct{}{}

		labelNames = append(labelNames, label)
	}

	return ctl.RegisterCounterVec(name, help, labelNames...)
}

func (p *Plugin) applyMaskMetric(mask *Mask, event *pipeline.Event, delta uint64) {
	if mask.appliedMetric == nil {
		return
	}

	labelValues := make([]string, 0, len(mask.MetricLabels))
	for _, labelValuePath := range mask.MetricLabels {
		value := "not_set"
		if node := event.Root.Dig(labelValuePath); node != nil {
			value = strings.Clone(node.AsString())
		}

		labelValues = append(labelValues, value)
	}

	p.AddAppliedMetric(mask, float64(delta), labelValues...)

	if ce := p.logger.Check(zap.DebugLevel, "mask appeared to event"); ce != nil {
		ce.Write(zap.String("event", event.Root.EncodeToString()))
	}
}

func (p *Plugin) AddAppliedMetric(mask *Mask, delta float64, lvs ...string) {
	metric.TruncateLabels(lvs, p.metricMaxLabelValueLength)
	mask.appliedMetric.WithLabelValues(lvs...).Add(float64(delta))
}
