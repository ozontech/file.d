package convert_utf8_bytes

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestConvertUTF8Bytes(t *testing.T) {
	cases := []struct {
		name       string
		config     *Config
		in         string
		wantFields []string
	}{
		{
			name: "valid_hex",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"\xD0\xA1\xD0\x98\xD0\xA1\xD0\xA2\xD0\x95\xD0\x9C\xD0\x90.xml"}}`,
			wantFields: []string{"СИСТЕМА.xml"},
		},
		{
			name: "valid_octal",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\110\145\154\154\157\054\040\146\151\154\145\056\144!"}}`,
			wantFields: []string{"$Hello, file.d!"},
		},
		{
			name: "valid_unicode4",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u0020\u0066\u0069\u006C\u0065\u002E\u0064!"}}`,
			wantFields: []string{"$Hello, file.d!"},
		},
		{
			name: "valid_unicode4_surrogate",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u0020\uD801\uDC01!"}}`,
			wantFields: []string{"$Hello, 𐐁!"},
		},
		{
			name: "valid_unicode8",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\U00000048\U00000065\U0000006C\U0000006C\U0000006F\U0000002C\U00000020\U00000066\U00000069\U0000006C\U00000065\U0000002E\U00000064!"}}`,
			wantFields: []string{"$Hello, file.d!"},
		},
		{
			name: "valid_escaped",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"{\"Test\":\"test\\u003F\\ud801\\udc01\",\"User\":\"NT AUTHORITY\\\\\\xD0\\xA1\\xD0\\x98\\xD0\\xA1\\xD0\\xA2\\xD0\\x95\\xD0\\x9C\\xD0\\x90\"}"}}`,
			wantFields: []string{`{"Test":"test?𐐁","User":"NT AUTHORITY\\СИСТЕМА"}`},
		},
		{
			name: "valid_escaped_winpath",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"{\"Dir\":\"C:\\\\Users\\\\username\\\\.prog\\\\120.67.0\\\\x86_64\\\\x64\",\"File\":\"H$Storage_2e3d6dbe-3b0a-4fa9-a6b7-bf1e91e8b3de$\\xD0\\x9F\\xD1\\x80\\xD0\\xB8\\xD0\\xB7\\xD0\\xBD\\xD0\\xB0\\xD0\\xBA.20.tbl.xml\"}"}}`,
			wantFields: []string{`{"Dir":"C:\\Users\\username\\.prog\\120.67.0\\x86_64\\x64","File":"H$Storage_2e3d6dbe-3b0a-4fa9-a6b7-bf1e91e8b3de$Признак.20.tbl.xml"}`},
		},
		{
			name: "valid_multiple_fields",
			config: &Config{
				Fields: []cfg.FieldSelector{
					"obj.field",
					"obj2",
				},
			},
			in: `{"obj":{"field":"\xD0\xA1\xD0\x98\xD0\xA1\xD0\xA2\xD0\x95\xD0\x9C\xD0\x90.xml"},"obj2":"test\u003F\uD801\uDC01"}`,
			wantFields: []string{
				"СИСТЕМА.xml",
				"test?𐐁",
			},
		},
		{
			name: "field_not_string",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":true}}`,
			wantFields: []string{"true"},
		},
		{
			name: "invalid_octal",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\110\145\154\154\157\054\40\146\151\154\145\777\144!"}}`,
			wantFields: []string{`$Hello,\40file\777d!`},
		},
		{
			name: "invalid_unicode4",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\u0048\u0065\u006C\u006C\u006F\u002C\u\u0066\u0069\u006C\u0065\u00\u0064!"}}`,
			wantFields: []string{`$Hello,\ufile\u00d!`},
		},
		{
			name: "invalid_unicode8",
			config: &Config{
				Fields: []cfg.FieldSelector{"obj.field"},
			},
			in:         `{"obj":{"field":"$\U00000048\U00000065\U0000006C\U0000006C\U0000006F\U0000002C\U0000\U00000066\U00000069\U0000006C\U00000065\UFFFFFFF\U00000064!"}}`,
			wantFields: []string{`$Hello,\U0000file\UFFFFFFFd!`},
		},
		{
			name: "non_graphic_char",
			config: &Config{
				Fields:            []cfg.FieldSelector{"obj.field"},
				ReplaceNonGraphic: true,
			},
			in:         `{"obj":{"field":"{\"version\":\"1.0.18.16 6\\t\\u0001ProductVersion\"}"}}`,
			wantFields: []string{`{"version":"1.0.18.16 6\t�ProductVersion"}`},
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
				for i := range tt.config.Fields {
					fieldNode := e.Root.Dig(cfg.ParseFieldSelector(string(tt.config.Fields[i]))...)
					assert.NotNil(t, fieldNode, "field is nil")
					assert.Equal(t, tt.wantFields[i], fieldNode.AsString())
				}
				wg.Done()
			})

			input.In(0, "test.log", test.Offset(0), []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}
