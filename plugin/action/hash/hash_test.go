package hash

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	const resField = "hash"

	tests := []struct {
		name string

		config   *Config
		input    []byte
		pipeOpts []string

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
				ResultField: resField,
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
				ResultField: resField,
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
				ResultField: resField,
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
				ResultField: resField,
			},
			input:    []byte(`{"level":"error","message":"2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""}`),
			wantHash: 2140116920471166296,
		},
		{
			name: "field_max_size",
			config: &Config{
				Fields: []Field{
					{Field: "message", Format: "normalize", MaxSize: 70},
				},
				ResultField: resField,
			},
			input:    []byte(`{"level":"error","message":"2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""}`),
			wantHash: 10662808184633841128,
		},
		{
			name: "normalizer_without_builtin",
			config: &Config{
				Fields: []Field{
					{Field: "message", Format: "normalize"},
				},
				ResultField: resField,
				Normalizer: NormalizerConfig{
					WithBuiltinPatterns: false,
					Patterns: []NormalizePattern{
						{
							Placeholder: "<quoted_str>",
							RE:          `"[^"]*"`,
						},
						{
							Placeholder: "<date>",
							RE:          `\d\d.\d\d.\d\d\d\d`,
						},
					},
				},
			},
			input:    []byte(`{"level":"error","message":"request from \"ivanivanov\", signed on 19.03.2025"}`),
			pipeOpts: []string{"name"},
			wantHash: 6933347847764028189,
		},
		{
			name: "normalizer_with_builtin",
			config: &Config{
				Fields: []Field{
					{Field: "message", Format: "normalize"},
				},
				ResultField: resField,
				Normalizer: NormalizerConfig{
					WithBuiltinPatterns: true,
					Patterns: []NormalizePattern{
						{
							Placeholder: "<quoted_str>",
							RE:          `"[^"]*"`,
							Priority:    "first",
						},
						{
							Placeholder: "<nginx_datetime>",
							RE:          `\d\d\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d`,
							Priority:    "last",
						},
					},
				},
			},
			input:    []byte(`{"level":"error","message":"2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""}`),
			pipeOpts: []string{"name"},
			wantHash: 7891860241841154313,
		},
		{
			name: "no_field",
			config: &Config{
				Fields: []Field{
					{Field: "test"},
				},
				ResultField: resField,
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
				ResultField: resField,
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
				ResultField: resField,
			},
			input:         []byte(`{"test":[1,2,3]}`),
			wantEmptyNode: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			config := test.NewConfig(tt.config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false), tt.pipeOpts...)

			wg := &sync.WaitGroup{}
			wg.Add(1)

			output.SetOutFn(func(e *pipeline.Event) {
				node := e.Root.Dig(resField)
				if tt.wantEmptyNode {
					assert.Nil(t, node, "node %q must be nil", resField)
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
