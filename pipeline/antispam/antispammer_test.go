package antispam

import (
	"testing"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestAntispam(t *testing.T) {
	r := require.New(t)

	threshold := 5
	unbanIterations := 2
	maintenanceInterval := time.Second * 1
	antispamer := NewAntispammer(&Options{
		MaintenanceInterval: maintenanceInterval,
		Threshold:           threshold,
		UnbanIterations:     unbanIterations,
		Logger:              logger.Instance.Named("antispam").Desugar(),
		MetricsController:   metric.NewCtl("test", prometheus.NewRegistry()),
	})

	startTime := time.Now()
	checkSpam := func(i int) bool {
		eventTime := startTime.Add(time.Duration(i) * maintenanceInterval / 2)
		return antispamer.IsSpam(1, "test", false, []byte(`{}`), eventTime)
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
	antispamer := NewAntispammer(&Options{
		MaintenanceInterval: maintenanceInterval,
		Threshold:           threshold,
		UnbanIterations:     unbanIterations,
		Logger:              logger.Instance.Named("antispam").Desugar(),
		MetricsController:   metric.NewCtl("test", prometheus.NewRegistry()),
	})

	startTime := time.Now()
	checkSpam := func(i int) bool {
		eventTime := startTime.Add(time.Duration(i) * maintenanceInterval)
		return antispamer.IsSpam(1, "test", false, []byte(`{}`), eventTime)
	}

	for i := 1; i < threshold; i++ {
		result := checkSpam(i)
		r.False(result)
	}

	result := checkSpam(threshold)
	r.False(result)
}
