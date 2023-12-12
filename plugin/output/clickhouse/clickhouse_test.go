package clickhouse

import (
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	mockclickhouse "github.com/ozontech/file.d/plugin/output/clickhouse/mock"
	"github.com/stretchr/testify/assert"
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
		Addr   string
		Weight int
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
				assert.Equal(t, tt.want.Weight, a.Weight)
			}
		})
	}
}
