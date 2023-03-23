package clickhouse

import (
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	mockclickhouse "github.com/ozontech/file.d/plugin/output/clickhouse/mock"
)

func TestPlugin_getInstance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	instances := []Clickhouse{
		mockclickhouse.NewMockClickhouse(ctrl),
		mockclickhouse.NewMockClickhouse(ctrl),
		mockclickhouse.NewMockClickhouse(ctrl),
		mockclickhouse.NewMockClickhouse(ctrl),
		mockclickhouse.NewMockClickhouse(ctrl),
	}

	type args struct {
		id    int64
		retry int
	}
	tests := []struct {
		name      string
		instances []Clickhouse
		stategy   InsertStrategy
		args      args
		want      Clickhouse
	}{
		// in-order
		{
			name:      "one instance and first retry",
			instances: instances[:1],
			stategy:   StrategyInOrder,
			args:      args{id: rand.Int63(), retry: 0},
			want:      instances[0],
		},
		{
			name:      "one instance and some retry",
			instances: instances[:1],
			stategy:   StrategyInOrder,
			args:      args{id: rand.Int63(), retry: 123},
			want:      instances[0],
		},
		{
			name:      "many instances and some retry",
			instances: instances,
			stategy:   StrategyInOrder,
			args:      args{id: rand.Int63(), retry: 123},
			want:      instances[3], // 123%3
		},
		// round-robin
		{
			name:      "many instances and first retry",
			instances: instances,
			stategy:   StrategyRoundRobin,
			args:      args{id: 123, retry: 0},
			want:      instances[3], // 123%3
		},
		{
			name:      "many instances and rand retry",
			instances: instances,
			stategy:   StrategyRoundRobin,
			args:      args{id: 0, retry: rand.Int()},
			want:      instances[0],
		},
		{
			name:      "one instances and rand retry",
			instances: instances[:1],
			stategy:   StrategyRoundRobin,
			args:      args{id: rand.Int63(), retry: rand.Int()},
			want:      instances[0],
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Plugin{instances: tt.instances, config: &Config{InsertStrategy_: tt.stategy}}

			instance := p.getInstance(tt.args.id, tt.args.retry)
			if instance != tt.want {
				t.Fatal("instances are not equal")
			}
		})
	}
}
