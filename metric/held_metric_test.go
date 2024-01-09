package metric

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var holderBenchCases = []struct {
	Labels      []string
	LabelValues [][]string
}{
	{
		Labels: []string{"l1"},
		LabelValues: [][]string{
			{"test1"},
			{"test2"},
			{"test3"},
		},
	},
	{
		Labels: []string{"l1", "l2"},
		LabelValues: [][]string{
			{"first1", "second1"},
			{"first2", "second2"},
			{"first3", "second3"},
		},
	},
	{
		Labels: []string{"l1", "l2", "l3"},
		LabelValues: [][]string{
			{"first1", "second1", "third1"},
			{"first2", "second2", "third2"},
			{"first3", "second3", "third3"},
		},
	},
}

func BenchmarkMetricHolder(b *testing.B) {
	for _, benchCase := range holderBenchCases {
		ctl := NewCtl("test", prometheus.NewRegistry())
		holder := NewHolder(time.Minute)

		promCounter := ctl.RegisterCounterVec("test_name", "", benchCase.Labels...)
		counter := holder.AddCounterVec(promCounter)

		name := strings.Join(benchCase.Labels, "_")

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, labels := range benchCase.LabelValues {
					counter.WithLabelValues(labels...).Inc()
				}
			}
		})
	}
}

func BenchmarkPromVec(b *testing.B) {
	for _, benchCase := range holderBenchCases {
		ctl := NewCtl("test", prometheus.NewRegistry())
		counter := ctl.RegisterCounterVec("test_name", "", benchCase.Labels...)
		name := strings.Join(benchCase.Labels, "_")

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, labels := range benchCase.LabelValues {
					counter.WithLabelValues(labels...).Inc()
				}
			}
		})
	}
}
