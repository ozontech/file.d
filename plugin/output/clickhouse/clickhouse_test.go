package clickhouse

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mockclickhouse "github.com/ozontech/file.d/plugin/output/clickhouse/mock"
	"github.com/ozontech/file.d/xhttp"
	"github.com/stretchr/testify/assert"
)

func TestPlugin_getInstance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	pools := []Clickhouse{
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
		name          string
		stategy       InsertStrategy
		args          args
		instanceCount int
		want          Clickhouse
	}{
		// in-order
		{
			name:          "one instance and first retry",
			stategy:       StrategyInOrder,
			args:          args{id: rand.Int63(), retry: 0},
			instanceCount: 1,
			want:          pools[0],
		},
		{
			name:          "one instance and some retry",
			stategy:       StrategyInOrder,
			args:          args{id: rand.Int63(), retry: 123},
			instanceCount: 1,
			want:          pools[0],
		},
		{
			name:          "many instances and some retry",
			stategy:       StrategyInOrder,
			args:          args{id: rand.Int63(), retry: 123},
			instanceCount: 2,
			want:          pools[1], // 123%2
		},
		// round-robin
		{
			name:          "many instances and first retry",
			stategy:       StrategyRoundRobin,
			args:          args{id: 123, retry: 0},
			instanceCount: 3,
			want:          pools[0], // 123%3
		},
		{
			name:          "many instances and rand retry",
			stategy:       StrategyRoundRobin,
			args:          args{id: 0, retry: rand.Int()},
			instanceCount: 5,
			want:          pools[0],
		},
		{
			name:          "one instances and rand retry",
			stategy:       StrategyRoundRobin,
			args:          args{id: rand.Int63(), retry: rand.Int()},
			instanceCount: 1,
			want:          pools[0],
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cb := xhttp.NewCircuitBreaker[Clickhouse](time.Second, tt.instanceCount)
			for i := 0; i < tt.instanceCount; i++ {
				cb.AddTarget(xhttp.TargetID(fmt.Sprintf("addr%d", i)), pools[i], 1)
			}
			p := &Plugin{cb: cb, config: &Config{InsertStrategy_: tt.stategy}}

			idx := p.getInstanceIndex(tt.args.id, tt.args.retry, tt.instanceCount)
			instance := p.cb.GetActiveTargetByIndex(idx)
			if instance.Client != tt.want {
				t.Fatal("instances are not equal")
			}
		})
	}
}

func Test_addrWithDefaultPort(t *testing.T) {
	defaultPort := "9000"
	tests := []struct {
		addr string
		want string
	}{
		{
			addr: "127.0.0.1:9333",
			want: "127.0.0.1:9333",
		},
		{
			addr: "127.0.0.1",
			want: "127.0.0.1:9000",
		},
		{
			addr: "1.1.1.1:",
			want: "1.1.1.1:9000",
		},
		{
			addr: "google.com:9333",
			want: "google.com:9333",
		},
		{
			addr: "google.com",
			want: "google.com:9000",
		},
		{
			addr: "api.google.com:9333",
			want: "api.google.com:9333",
		},
		{
			addr: "api.google.com",
			want: "api.google.com:9000",
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, addrWithDefaultPort(tt.addr, defaultPort))
	}
}

func TestAddress_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	type fields struct {
		Addr        string
		Weight      int
		WeightIsNil bool
	}
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		args    args
		want    fields
		wantErr bool
	}{
		{
			name: "ok_object",
			args: args{
				b: []byte(`{"addr":"127.0.0.1:9001","weight":2}`),
			},
			want: fields{
				Addr:   "127.0.0.1:9001",
				Weight: 2,
			},
		},
		{
			name: "ok_object_only_addr",
			args: args{
				b: []byte(`{"addr":"127.0.0.1:9001"}`),
			},
			want: fields{
				Addr:   "127.0.0.1:9001",
				Weight: 1,
			},
		},
		{
			name: "ok_string",
			args: args{
				b: []byte(`"127.0.0.1:9001"`),
			},
			want: fields{
				Addr:   "127.0.0.1:9001",
				Weight: 1,
			},
		},
		{
			name: "invalid_type",
			args: args{
				b: []byte(`[{"field":"val"}]`),
			},
			wantErr: true,
		},
		{
			name: "invalid_object",
			args: args{
				b: []byte(`{"field":"val"}`),
			},
			wantErr: true,
		},
		{
			name: "empty",
			want: fields{
				WeightIsNil: true,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := &Address{}
			if err := a.UnmarshalJSON(tt.args.b); (err != nil) != tt.wantErr {
				t.Errorf("Address.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				assert.Equal(t, tt.want.Addr, a.Addr)
				if tt.want.WeightIsNil {
					assert.Nil(t, a.Weight)
				} else {
					assert.NotNil(t, a.Weight)
					if a.Weight != nil {
						assert.Equal(t, tt.want.Weight, *a.Weight)
					}
				}
			}
		})
	}
}
