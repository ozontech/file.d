package antispam

import (
	"testing"
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
		return antispamer.IsSpam("1", "test", false, []byte(`{}`), eventTime, nil)
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
		return antispamer.IsSpam("1", "test", false, []byte(`{}`), eventTime, nil)
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
	unbanIterations := 2
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
		r.Equal(expected, antispamer.IsSpam(source, source, false, []byte(event), now, meta))
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
