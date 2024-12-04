package move

import (
	"errors"
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		config  *Config
		wantErr error
	}{
		{
			name: "valid_allow",
			config: &Config{
				Mode:   modeAllow,
				Target: "target1.target2.target3",
			},
		},
		{
			name: "valid_block",
			config: &Config{
				Mode:   modeBlock,
				Target: "target",
			},
		},
		{
			name: "invalid_mode",
			config: &Config{
				Mode:   "unknown",
				Target: "target",
			},
			wantErr: errors.New(`invalid mode "unknown"`),
		},
		{
			name: "invalid_block_target",
			config: &Config{
				Mode:   modeBlock,
				Target: "target1.target2.target3",
			},
			wantErr: errors.New(`in "block" mode, the maximum "target" depth is 1`),
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := test.NewConfig(tt.config, nil).(*Config)
			assert.Equal(t, tt.wantErr, config.validate())
		})
	}
}

func TestMove(t *testing.T) {
	cases := []struct {
		name       string
		config     *Config
		in         string
		wantTarget map[string]string
	}{
		{
			name: "allow_simple",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field3"},
				Mode:   modeAllow,
				Target: "target_field",
			},
			in: `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{
				"field1": "value1",
				"field3": "3",
			},
		},
		{
			name: "block_simple",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field3"},
				Mode:   modeBlock,
				Target: "target_field",
			},
			in: `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{
				"field2": "true",
			},
		},
		{
			name: "allow_deep_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"field3", "field2.field2_1", "field2.field2_2.field2_2_2"},
				Mode:   modeAllow,
				Target: "target_field",
			},
			in: `{"field1":"value1","field2":{"field2_1":"value2_1","field2_2":{"field2_2_1":100,"field2_2_2":"value2_2_2"}},"field3":3}`,
			wantTarget: map[string]string{
				"field3":     "3",
				"field2_1":   "value2_1",
				"field2_2_2": "value2_2_2",
			},
		},
		{
			// in block mode max fields depth is 1, so deep fields will be ignored
			name: "block_deep_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field2.field2_2"},
				Mode:   modeBlock,
				Target: "target_field",
			},
			in: `{"field1":"value1","field2":{"field2_1":"value2_1","field2_2":{"field2_2_1":100,"field2_2_2":"value2_2_2"}},"field3":3}`,
			wantTarget: map[string]string{
				"field2": `{"field2_1":"value2_1","field2_2":{"field2_2_1":100,"field2_2_2":"value2_2_2"}}`,
				"field3": "3",
			},
		},
		{
			name: "allow_unknown_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"unknown1", "unknown2"},
				Mode:   modeAllow,
				Target: "target_field",
			},
			in:         `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{},
		},
		{
			name: "block_all_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field2", "field3"},
				Mode:   modeBlock,
				Target: "target_field",
			},
			in:         `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{},
		},
		{
			name: "allow_empty_fields",
			config: &Config{
				Mode:   modeAllow,
				Target: "target_field",
			},
			in:         `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{},
		},
		{
			name: "block_empty_fields",
			config: &Config{
				Mode:   modeBlock,
				Target: "target_field",
			},
			in: `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{
				"field1": "value1",
				"field2": "true",
				"field3": "3",
			},
		},
		{
			name: "allow_deep_target",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field3"},
				Mode:   modeAllow,
				Target: "target1.target2.target3",
			},
			in: `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{
				"field1": "value1",
				"field3": "3",
			},
		},
		{
			name: "existing_target",
			config: &Config{
				Fields: []cfg.FieldSelector{"field2"},
				Mode:   modeAllow,
				Target: "field3",
			},
			in: `{"field1":"value1","field2":true,"field3":{"field3_1":3}}`,
			wantTarget: map[string]string{
				"field3_1": "3",
				"field2":   "true",
			},
		},
		{
			name: "existing_target_not_object",
			config: &Config{
				Fields: []cfg.FieldSelector{"field2"},
				Mode:   modeAllow,
				Target: "field3",
			},
			in: `{"field1":"value1","field2":true,"field3":3}`,
			wantTarget: map[string]string{
				"field2": "true",
			},
		},
		{
			name: "allow_target_in_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"field2", "field3"},
				Mode:   modeAllow,
				Target: "field3",
			},
			in: `{"field1":"value1","field2":true,"field3":{"field3_1":3}}`,
			wantTarget: map[string]string{
				"field3_1": "3",
				"field2":   "true",
			},
		},
		{
			name: "block_target_in_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{"field1", "field3"},
				Mode:   modeBlock,
				Target: "field3",
			},
			in: `{"field1":"value1","field2":true,"field3":{"field3_1":3}}`,
			wantTarget: map[string]string{
				"field3_1": "3",
				"field2":   "true",
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := test.NewConfig(tt.config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

			wg := &sync.WaitGroup{}
			wg.Add(1)

			output.SetOutFn(func(e *pipeline.Event) {
				target := e.Root.Dig(tt.config.Target_...)
				assert.NotNil(t, target, "target is nil")

				assert.Equal(t, len(tt.wantTarget), len(target.AsFields()), "wrong target nodes count")
				for name, val := range tt.wantTarget {
					node := target.Dig(name)
					assert.NotNil(t, node, "node is nil")

					if node.IsObject() {
						assert.Equal(t, val, node.EncodeToString(), "wrong node value")
					} else {
						assert.Equal(t, val, node.AsString(), "wrong node value")
					}
				}

				wg.Done()
			})

			input.In(0, "test.log", test.Offset(0), []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}
