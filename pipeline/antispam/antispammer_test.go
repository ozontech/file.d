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
	antispamer := NewAntispammer(&Options{
		MaintenanceInterval: time.Second * 1,
		Threshold:           threshold,
		UnbanIterations:     unbanIterations,
		Logger:              logger.Instance.Named("antispam").Desugar(),
		MetricsController:   metric.NewCtl("test", prometheus.NewRegistry()),
	})

	checkSpam := func() bool {
		return antispamer.IsSpam(1, "test", false, []byte(`{}`))
	}

	for i := 0; i < threshold-1; i++ {
		result := checkSpam()
		r.False(result)
	}

	result := checkSpam()
	r.True(result)

	for i := 0; i <= unbanIterations; i++ {
		antispamer.Maintenance()
	}

	result = checkSpam()
	r.False(result)
}
