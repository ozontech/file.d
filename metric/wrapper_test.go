package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestWrapper_update(t *testing.T) {
	type wrapperFields struct {
		active    bool
		labels    []string
		registrar chan prometheus.Collector
	}

	r := make(chan prometheus.Collector)
	col := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "my_col",
	})

	tests := []struct {
		name      string
		wf        wrapperFields
		col       prometheus.Collector
		checkChan bool
	}{
		{
			name: "active",
			wf: wrapperFields{
				active:    true,
				registrar: r,
			},
			col:       col,
			checkChan: false,
		},
		{
			name: "inactive_without_labels",
			wf: wrapperFields{
				active:    false,
				registrar: r,
			},
			col:       col,
			checkChan: true,
		},
		{
			name: "inactive_with_labels",
			wf: wrapperFields{
				active:    false,
				labels:    []string{"lbl"},
				registrar: r,
			},
			col:       col,
			checkChan: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &wrapper{
				active:    tt.wf.active,
				labels:    tt.wf.labels,
				registrar: tt.wf.registrar,
			}

			if tt.checkChan {
				go func() {
					assert.Equal(t, tt.col, <-tt.wf.registrar)
				}()
			}
			w.update(tt.col)
			assert.Equal(t, nowTime, w.changeTime)
			assert.Equal(t, true, w.active)
		})
	}
}
