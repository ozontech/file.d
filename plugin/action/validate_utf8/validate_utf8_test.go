package validate_utf8

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestValidateUTF8(t *testing.T) {
	cases := []struct {
		name      string
		config    *Config
		in        string
		wantField string
	}{
		{
			name: "valid_hex",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"test\\\xD0\xA1\xD0\x98\xD0\xA1\xD0\xA2\xD0\x95\xD0\x9C\xD0\x90.xml"}}`,
			wantField: `test\СИСТЕМА.xml`,
		},
		{
			name: "valid_octal",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\110\145\154\154\157\054\040\146\151\154\145\056\144!"}}`,
			wantField: `$Hello, file.d!`,
		},
		{
			name: "valid_unicode4",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u0020\u0066\u0069\u006C\u0065\u002E\u0064!"}}`,
			wantField: `$Hello, file.d!`,
		},
		{
			name: "valid_unicode4_surrogate",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u0020\ud801\udc01!"}}`,
			wantField: `$Hello, 𐐁!`,
		},
		{
			name: "valid_unicode8",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\U00000048\U00000065\U0000006C\U0000006C\U0000006F\U0000002C\U00000020\U00000066\U00000069\U0000006C\U00000065\U0000002E\U00000064!"}}`,
			wantField: `$Hello, file.d!`,
		},
		{
			name: "valid_escaped",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"{\"Test\":\"test\\u003F\\ud801\\udc01\",\"User\":\"NT AUTHORITY\\\\\\xD0\\xA1\\xD0\\x98\\xD0\\xA1\\xD0\\xA2\\xD0\\x95\\xD0\\x9C\\xD0\\x90\"}"}}`,
			wantField: `{"Test":"test?𐐁","User":"NT AUTHORITY\\СИСТЕМА"}`,
		},
		{
			name: "field_not_string",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":true}}`,
			wantField: `true`,
		},
		{
			name: "invalid_octal",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\110\145\154\154\157\054\40\146\151\154\145\777\144!"}}`,
			wantField: `$Hello,\40file\777d!`,
		},
		{
			name: "invalid_unicode4",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u\u0066\u0069\u006C\u0065\u00\u0064!"}}`,
			wantField: `$Hello,\ufile\u00d!`,
		},
		{
			name: "invalid_unicode8",
			config: &Config{
				Field: "obj.field",
			},
			in:        `{"obj":{"field":"$\U00000048\U00000065\U0000006C\U0000006C\U0000006F\U0000002C\U0000\U00000066\U00000069\U0000006C\U00000065\UFFFFFFF\U00000064!"}}`,
			wantField: `$Hello,\U0000file\UFFFFFFFd!`,
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
				fieldNode := e.Root.Dig(tt.config.Field_...)
				assert.NotNil(t, fieldNode, "field is nil")

				assert.Equal(t, tt.wantField, fieldNode.AsString())

				wg.Done()
			})

			input.In(0, "test.log", 0, []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}
