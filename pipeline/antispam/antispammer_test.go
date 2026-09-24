package antispam

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline/doif"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newAntispammer(threshold, unbanIterations int, maintenanceInterval time.Duration) *Antispammer {
	return NewAntispammer(&Options{
		MaintenanceInterval: maintenanceInterval,
		Threshold:           threshold,
		UnbanIterations:     unbanIterations,
		Logger:              logger.Instance.Named("antispam").Desugar(),
		MetricsController:   metric.NewCtl("test", prometheus.NewRegistry(), time.Minute, 0),
	})
}

func TestAntispam(t *testing.T) {
	r := require.New(t)

	threshold := 5
	unbanIterations := 2
	maintenanceInterval := time.Second * 1

	antispamer := newAntispammer(threshold, unbanIterations, maintenanceInterval)

	startTime := time.Now()
	checkSpam := func(i int) bool {
		eventTime := startTime.Add(time.Duration(i) * maintenanceInterval / 2)
		return antispamer.IsSpam("1", "test", false, []byte(`{}`), eventTime, nil) == Dropped
	}

	for i := 1; i < threshold; i++ {
		result := checkSpam(i)
		r.False(result)
	}

	for i := 0; i <= unbanIterations-1; i++ {
		result := checkSpam(threshold + i)
		r.True(result)
		antispamer.Maintenance()
	}

	result := checkSpam(threshold + 1)
	r.False(result)
}

func TestAntispamAfterRestart(t *testing.T) {
	r := require.New(t)

	threshold := 5
	unbanIterations := 2
	maintenanceInterval := time.Second * 1

	antispamer := newAntispammer(threshold, unbanIterations, maintenanceInterval)

	startTime := time.Now()
	checkSpam := func(i int) bool {
		eventTime := startTime.Add(time.Duration(i) * maintenanceInterval)
		return antispamer.IsSpam("1", "test", false, []byte(`{}`), eventTime, nil) == Dropped
	}

	for i := 1; i < threshold; i++ {
		result := checkSpam(i)
		r.False(result)
	}

	result := checkSpam(threshold)
	r.False(result)
}

func TestAntispamExceptions(t *testing.T) {
	r := require.New(t)
	now := time.Now()

	threshold := 1
	unbanIterations := 2
	maintenanceInterval := time.Second * 1

	antispamer := newAntispammer(threshold, unbanIterations, maintenanceInterval)

	eventRulesetName := "test_event"
	sourceRulesetName := "test_sourcename"

	antispamer.exceptions = Exceptions{
		{
			RuleSet: matchrule.RuleSet{
				Name: eventRulesetName,
				Cond: matchrule.CondOr,
				Rules: []matchrule.Rule{
					{
						Mode: matchrule.ModePrefix,
						Values: []string{
							`{"level":"debug"`,
							`{"level":"info"`,
						},
					},
					{
						Mode:   matchrule.ModeContains,
						Values: []string{"test_event"},
					},
				},
			},
		},
		{
			CheckSourceName: true,
			RuleSet: matchrule.RuleSet{
				Name: sourceRulesetName,
				Cond: matchrule.CondAnd,
				Rules: []matchrule.Rule{
					{
						Mode:   matchrule.ModeContains,
						Values: []string{"my_source1", "my_source2"},
					},
				},
			},
		},
	}
	antispamer.exceptions.Prepare()

	checkSpam := func(source, event string, wantMetric map[string]float64) {
		antispamer.IsSpam("1", source, true, []byte(event), now, nil)
		for k, v := range wantMetric {
			r.Equal(v, antispamer.exceptionMetric.WithLabelValues(k).ToFloat64())
		}
	}

	checkSpam("test", `{"level":"info","message":test"}`, map[string]float64{
		eventRulesetName:  1,
		sourceRulesetName: 0,
	})

	checkSpam("test", `{"level":"error","message":test_event123"}`, map[string]float64{
		eventRulesetName:  2,
		sourceRulesetName: 0,
	})

	checkSpam("my_source2", `{"level":"error","message":test"}`, map[string]float64{
		eventRulesetName:  2,
		sourceRulesetName: 1,
	})

	checkSpam("my_source1", `{"level":"debug","message":test"}`, map[string]float64{
		eventRulesetName:  3,
		sourceRulesetName: 1,
	})

	checkSpam("test", `{"level":"error","message":test"}`, map[string]float64{
		eventRulesetName:  3,
		sourceRulesetName: 1,
	})
}

func TestAntispamRules(t *testing.T) {
	r := require.New(t)
	now := time.Now()

	threshold := 2
	unbanIterations := 4
	maintenanceInterval := time.Second * 1

	antispamer := newAntispammer(threshold, unbanIterations, maintenanceInterval)

	ruleNameBanAll := "test_ban_all"
	ruleNamePassAll := "test_pass_all"
	ruleCustomThresold := "test_custom_threshold"

	doIfCheckerSourceName, err := doif.NewFromMap(map[string]any{
		"op":     "equal",
		"field":  "source_name",
		"values": []any{"test_source_name"},
	})
	r.NoError(err)

	doIfCheckerMetaField, err := doif.NewFromMap(map[string]any{
		"op":     "equal",
		"field":  "meta.some_field",
		"values": []any{"test_meta_field"},
	})
	r.NoError(err)

	doIfCheckerEventBytes, err := doif.NewFromMap(map[string]any{
		"op":     "prefix",
		"field":  "event",
		"values": []any{`{"level":"error"`},
	})
	r.NoError(err)

	antispamer.rules = Rules{
		Rule{
			Name:        ruleNameBanAll,
			Threshold:   0,
			DoIfChecker: doIfCheckerSourceName,
		},
		Rule{
			Name:        ruleNamePassAll,
			Threshold:   -1,
			DoIfChecker: doIfCheckerMetaField,
		},
		Rule{
			Name:        ruleCustomThresold,
			Threshold:   3,
			DoIfChecker: doIfCheckerEventBytes,
		},
	}

	checkSpam := func(expected bool, source, event string, meta map[string]string) {
		got := antispamer.IsSpam(source, source, false, []byte(event), now, meta) == Dropped
		r.Equal(expected, got)
	}

	checkSpam(true, "test_source_name", `{"level":"info","message":test"}`, nil)

	checkSpam(false, "test_meta_field", `{"level":"info","message":test"}`, map[string]string{
		"some_field": "test_meta_field",
	})

	checkSpam(false, "test_event_bytes", `{"level":"error","message":test"}`, nil)
	checkSpam(false, "test_event_bytes", `{"level":"error","message":test"}`, nil)
	checkSpam(true, "test_event_bytes", `{"level":"error","message":test"}`, nil)

	checkSpam(false, "test", `{"level":"info","message":test"}`, nil)
	checkSpam(true, "test", `{"level":"info","message":test"}`, nil)
}

func TestIsSampled(t *testing.T) {
	type samplerStep struct {
		timeSleep time.Duration
		want      bool
	}

	cases := []struct {
		name    string
		sampler *Sampler
		steps   []samplerStep
	}{
		{
			name:    "first_only",
			sampler: &Sampler{Interval: time.Hour, First: 3},
			steps: []samplerStep{
				{want: true}, {want: true}, {want: true}, {want: false}, {want: false},
			},
		},
		{
			name:    "thereafter_only",
			sampler: &Sampler{Interval: time.Hour, Thereafter: 2},
			steps: []samplerStep{
				{want: false}, // 1 % 2 != 0
				{want: true},  // 2 % 2 == 0
				{want: false}, // 3 % 2 != 0
			},
		},
		{
			name:    "first_and_thereafter",
			sampler: &Sampler{Interval: time.Hour, First: 2, Thereafter: 3},
			steps: []samplerStep{
				{want: true},  // 1
				{want: true},  // 2
				{want: false}, // (3-2)% 3 != 0
				{want: false}, // (4-2) % 3 != 0
				{want: true},  // (5-2) % 3 == 0
				{want: false}, // (6-2) % 3 != 0
			},
		},
		{
			name:    "window_rotation",
			sampler: &Sampler{Interval: time.Second, First: 2},
			steps: []samplerStep{
				{want: true}, {want: true}, {want: false},
				{timeSleep: 2 * time.Second, want: true}, {want: true}, {want: false},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				src := source{
					sampleUntil:   &atomic.Int64{},
					sampleCounter: &atomic.Int64{},
				}
				for _, step := range tt.steps {
					if step.timeSleep > 0 {
						time.Sleep(step.timeSleep)
					}
					require.Equal(t, step.want, tt.sampler.isSampled(src))
				}
			})
		})
	}
}
