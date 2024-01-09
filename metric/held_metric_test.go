package metric

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkMetricWithLabels(b *testing.B) {
	benchCases := []struct {
		Labels      []string
		LabelValues [][]string
	}{
		{
			Labels: []string{"first"},
			LabelValues: [][]string{
				{"test1"},
				{"test2"},
				{"test3"},
			},
		},
		{
			Labels: []string{"first", "second"},
			LabelValues: [][]string{
				{"first1", "second1"},
				{"first2", "second2"},
				{"first3", "second3"},
			},
		},
		{
			Labels: []string{"first", "second", "third"},
			LabelValues: [][]string{
				{"first1", "second1", "third1"},
				{"first2", "second2", "third2"},
				{"first3", "second3", "third3"},
			},
		},
	}

	for _, benchCase := range benchCases {
		ctl := NewCtl("test", prometheus.NewRegistry())
		holder := NewHolder(time.Minute)
		counter := holder.AddCounterVec(ctl.RegisterCounterVec("test_name", "", benchCase.Labels...))

		name := strings.Join(benchCase.Labels, "_")

		b.Run(name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for _, labels := range benchCase.LabelValues {
						counter.WithLabelValues(labels...).Inc()
					}
				}
			})
		})
	}
}
