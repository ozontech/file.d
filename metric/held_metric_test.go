package metric

import (
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeleteObsoleteMetrics(t *testing.T) {
	type testHeldLabels struct {
		labelValues []string
		isObsolete  bool
	}

	type testCaseData struct {
		metric string
		labels []string

		heldLabelsMap map[uint64][]testHeldLabels
	}

	type testCase struct {
		vec   *prometheus.CounterVec
		hlVec *heldLabelsVec

		wantMap          map[uint64][]heldLabels
		wantCountMetrics int
	}

	prepareTestCase := func(tcData testCaseData) testCase {
		cv := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "test",
			Subsystem: "test",
			Name:      tcData.metric,
			Help:      "help",
		}, tcData.labels)
		prometheus.MustRegister(cv)

		hlVec := &heldLabelsVec{
			heldLabelsByHash: map[uint64][]heldLabels{},
		}

		m := map[uint64][]heldLabels{}
		countMetrics := 0
		for k, v := range tcData.heldLabelsMap {
			hlVec.heldLabelsByHash[k] = make([]heldLabels, len(v))
			for i, hl := range v {
				isObs := hl.isObsolete
				hlVec.heldLabelsByHash[k][i] = heldLabels{
					labels:         hl.labelValues,
					lastUsage:      new(atomic.Int64),
					isObsoleteFunc: func(int64, time.Duration) bool { return isObs },
				}

				if !isObs {
					m[k] = append(m[k], heldLabels{labels: hl.labelValues})
					countMetrics++
				}

				cv.WithLabelValues(hl.labelValues...).Inc()
			}
		}

		return testCase{
			vec:              cv,
			hlVec:            hlVec,
			wantMap:          m,
			wantCountMetrics: countMetrics,
		}
	}

	tests := []struct {
		name   string
		tcData testCaseData
	}{
		{
			name: "with_labels",
			tcData: testCaseData{
				metric: "metric1",
				labels: []string{"lbl1", "lbl2"},
				heldLabelsMap: map[uint64][]testHeldLabels{
					1: {
						testHeldLabels{labelValues: []string{"val1_1_1", "val1_1_2"}, isObsolete: true},
						testHeldLabels{labelValues: []string{"val1_2_1", "val1_2_2"}, isObsolete: false},
					},
					2: {testHeldLabels{labelValues: []string{"val2_1", "val2_2"}, isObsolete: false}},
					3: {testHeldLabels{labelValues: []string{"val3_1", "val3_2"}, isObsolete: true}},
				},
			},
		},
		{
			name: "no_labels_obsolete",
			tcData: testCaseData{
				metric: "metric2",
				heldLabelsMap: map[uint64][]testHeldLabels{
					1: {testHeldLabels{labelValues: nil, isObsolete: true}},
				},
			},
		},
		{
			name: "no_labels_no_obsolete",
			tcData: testCaseData{
				metric: "metric3",
				heldLabelsMap: map[uint64][]testHeldLabels{
					1: {testHeldLabels{labelValues: nil, isObsolete: false}},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tc := prepareTestCase(tt.tcData)

			deleteObsoleteMetrics(time.Second, tc.hlVec, tc.vec.MetricVec)

			require.Equal(t, tc.wantCountMetrics, testutil.CollectAndCount(tc.vec))
			require.Equal(t, len(tc.wantMap), len(tc.hlVec.heldLabelsByHash))
			for k, v := range tc.hlVec.heldLabelsByHash {
				wantHl := tc.wantMap[k]
				require.Equal(t, len(wantHl), len(v))
				for i, hl := range v {
					require.True(t, slices.Equal(wantHl[i].labels, hl.labels))
					require.Equal(t, float64(1), testutil.ToFloat64(tc.vec.WithLabelValues(hl.labels...)))
				}
			}

			prometheus.Unregister(tc.vec)
		})
	}
}
