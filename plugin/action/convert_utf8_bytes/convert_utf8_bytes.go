package convert_utf8_bytes

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It converts multiple UTF-8-encoded bytes to corresponding characters.
Supports unicode (`\u...` and `\U...`), hex (`\x...`) and octal (`\{0-3}{0-7}{0-7}`) encoded bytes.

> Note: Escaped and unescaped backslashes are treated the same.
For example, the following 2 values will be converted to the same result:
`\x68\x65\x6C\x6C\x6F` and `\\x68\\x65\\x6C\\x6C\\x6F` => `hello`

### Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: convert_utf8_bytes
      fields:
        - obj.field
    ...
```

The original event:
```json
{
  "obj": {
    "field": "\\xD0\\xA1\\xD0\\x98\\xD0\\xA1\\xD0\\xA2\\xD0\\x95\\xD0\\x9C\\xD0\\x90.xml"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "СИСТЕМА.xml"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "$\\110\\145\\154\\154\\157\\054\\040\\146\\151\\154\\145\\056\\144!"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "$Hello, file.d!"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "$\\u0048\\u0065\\u006C\\u006C\\u006F\\u002C\\u0020\\ud801\\udc01!"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "$Hello, 𐐁!"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "{\"Dir\":\"C:\\\\Users\\\\username\\\\.prog\\\\120.67.0\\\\x86_64\\\\x64\",\"File\":\"$Storage$\\xD0\\x9F\\xD1\\x80\\xD0\\xB8\\xD0\\xB7\\xD0\\xBD\\xD0\\xB0\\xD0\\xBA.20.tbl.xml\"}"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "{\"Dir\":\"C:\\\\Users\\\\username\\\\.prog\\\\120.67.0\\\\x86_64\\\\x64\",\"File\":\"$Storage$Признак.20.tbl.xml\"}"
  }
}
```
}*/

type Plugin struct {
	config *Config
	fields [][]string
	buf    []byte
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of the event fields to convert.
	// >> Field value must be a string.
	Fields []cfg.FieldSelector `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will replace all non-graphic bytes to unicode replacement char (�).
	// >> It works only with unicode (`\u...` and `\U...`) encoded bytes.
	ReplaceNonGraphic bool `json:"replace_non_graphic" default:"false"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "convert_utf8_bytes",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	p.fields = make([][]string, 0, len(p.config.Fields))
	for _, fs := range p.config.Fields {
		p.fields = append(p.fields, cfg.ParseFieldSelector(string(fs)))
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for _, field := range p.fields {
		node := event.Root.Dig(field...)
		if node == nil || !node.IsString() {
			continue
		}
		p.convert(node)
	}

	return pipeline.ActionPass
}

func (p *Plugin) convert(node *insaneJSON.Node) {
	p.buf = p.buf[:0]

	nodeStr := node.AsString()

	idx := strings.IndexByte(nodeStr, '\\')
	if idx < 0 {
		return
	}
	p.buf = append(p.buf, nodeStr[:idx]...)
	nodeStr = nodeStr[idx+1:]

	for nodeStr != "" {
		ch := nodeStr[0]
		switch ch {
		case '\\':
			nodeStr = nodeStr[1:]
			p.buf = append(p.buf, `\\`...)
		// unicode
		case 'u', 'U':
			nodeStr = nodeStr[1:]

			size := 4
			if ch == 'U' {
				size = 8
			}

			if len(nodeStr) < size {
				p.buf = append(p.buf, '\\', ch)
				break
			}

			ss := nodeStr[:size]
			u, err := strconv.ParseUint(ss, 16, 64)
			if err != nil {
				p.buf = append(p.buf, '\\', ch)
				break
			}

			nodeStr = nodeStr[size:]

			if !unicode.IsGraphic(rune(u)) && p.config.ReplaceNonGraphic {
				u = unicode.ReplacementChar
			}

			// '\U...' or 1-byte '\u...'
			if size == 8 || !utf16.IsSurrogate(rune(u)) {
				p.buf = append(p.buf, string(rune(u))...)
				break
			}

			if len(nodeStr) < 6 || nodeStr[:2] != `\u` {
				p.buf = append(p.buf, `\u`...)
				p.buf = append(p.buf, ss...)
				break
			}

			// 2-byte '\u...\u...'
			u2, err := strconv.ParseUint(nodeStr[2:6], 16, 64)
			if err != nil {
				p.buf = append(p.buf, `\u`...)
				p.buf = append(p.buf, ss...)
				break
			}

			r := utf16.DecodeRune(rune(u), rune(u2))
			p.buf = append(p.buf, string(r)...)
			nodeStr = nodeStr[6:]
		// hex
		case 'x':
			nodeStr = nodeStr[1:]

			if len(nodeStr) < 2 {
				p.buf = append(p.buf, `\x`...)
				break
			}

			var (
				sb  strings.Builder
				pos int
			)

			sb.WriteString(nodeStr[:2])
			pos += 2

			for {
				if len(nodeStr)-pos >= 4 && nodeStr[pos:pos+2] == `\x` {
					sb.WriteString(nodeStr[pos+2 : pos+4])
					pos += 4
					continue
				}
				break
			}

			hexBytes, err := hex.DecodeString(sb.String())
			if err != nil {
				p.buf = append(p.buf, `\x`...)
				p.buf = append(p.buf, nodeStr[:pos]...)
			} else {
				p.buf = append(p.buf, hexBytes...)
			}
			nodeStr = nodeStr[pos:]
		// octal
		case '0', '1', '2', '3':
			if len(nodeStr) < 3 {
				p.buf = append(p.buf, '\\')
				break
			}

			u, err := strconv.ParseUint(nodeStr[:3], 8, 64)
			if err != nil {
				p.buf = append(p.buf, '\\')
				break
			}

			p.buf = append(p.buf, byte(u))
			nodeStr = nodeStr[3:]
		default:
			p.buf = append(p.buf, '\\')
		}

		idx = strings.IndexByte(nodeStr, '\\')
		if idx < 0 {
			p.buf = append(p.buf, nodeStr...)
			break
		}
		p.buf = append(p.buf, nodeStr[:idx]...)
		nodeStr = nodeStr[idx+1:]
	}

	node.MutateToString(pipeline.ByteToStringUnsafe(p.buf))
}
