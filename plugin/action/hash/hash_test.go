package hash

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	const hashField = "hash"

	tests := []struct {
		name string

		config *Config
		input  []byte

		wantHash      uint64
		wantEmptyNode bool
	}{
		{
			name: "format_no1",
			config: &Config{
				Fields: []Field{
					{Field: "error.code"},
					{Field: "message", Format: "normalize"},
				},
				HashField: hashField,
			},
			input:    []byte(`{"level":"error","error":{"code":"unauthenticated","message":"bad token format"},"message":"error occurred"}`),
			wantHash: 6584967863753642363,
		},
		{
			name: "format_no2",
			config: &Config{
				Fields: []Field{
					{Field: "error.code"},
					{Field: "message", Format: "no"},
				},
				HashField: hashField,
			},
			input:    []byte(`{"level":"error","message":"2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""}`),
			wantHash: 6051551654033583881,
		},
		{
			name: "format_normalize1",
			config: &Config{
				Fields: []Field{
					{Field: "error.code"},
					{Field: "message", Format: "normalize"},
				},
				HashField: hashField,
			},
			input:    []byte(`{"level":"error","message":"error occurred"}`),
			wantHash: 10960504816536859672,
		},
		{
			name: "format_normalize2",
			config: &Config{
				Fields: []Field{
					{Field: "error.code"},
					{Field: "message", Format: "normalize"},
				},
				HashField: hashField,
			},
			input:    []byte(`{"level":"error","message":"2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""}`),
			wantHash: 2140116920471166296,
		},
		{
			name: "no_field",
			config: &Config{
				Fields: []Field{
					{Field: "test"},
				},
				HashField: hashField,
			},
			input:         []byte(`{"level":"error"}`),
			wantEmptyNode: true,
		},
		{
			name: "field_obj",
			config: &Config{
				Fields: []Field{
					{Field: "test"},
				},
				HashField: hashField,
			},
			input:         []byte(`{"test":{"level":"error"}}`),
			wantEmptyNode: true,
		},
		{
			name: "field_arr",
			config: &Config{
				Fields: []Field{
					{Field: "test"},
				},
				HashField: hashField,
			},
			input:         []byte(`{"test":[1,2,3]}`),
			wantEmptyNode: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := test.NewConfig(tt.config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

			wg := &sync.WaitGroup{}
			wg.Add(1)

			output.SetOutFn(func(e *pipeline.Event) {
				node := e.Root.Dig(hashField)
				if tt.wantEmptyNode {
					assert.Nil(t, node, "node %q must be nil", hashField)
				} else {
					assert.Equal(t, tt.wantHash, node.AsUint64(), "wrong hash value")
				}
				wg.Done()
			})

			input.In(0, "test.log", test.NewOffset(0), tt.input)
			wg.Wait()
			p.Stop()
		})
	}
}
