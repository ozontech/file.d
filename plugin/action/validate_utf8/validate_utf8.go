package validate_utf8

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

type Plugin struct {
	config *Config
	buf    []byte
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The field to validate.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` // *
	Field_ []string
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "validate_utf8",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.buf = p.buf[:0]

	node := event.Root.Dig(p.config.Field_...)
	if node == nil || !node.IsString() {
		return pipeline.ActionPass
	}

	nodeStr := node.AsString()

	idx := strings.IndexByte(nodeStr, '\\')
	if idx < 0 {
		return pipeline.ActionPass
	}
	p.buf = append(p.buf, nodeStr[:idx]...)
	nodeStr = nodeStr[idx+1:]

	for len(nodeStr) > 0 {
		ch := nodeStr[0]
		switch ch {
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
				p.buf = append(p.buf, '\\', ch)
				break
			}

			u, err := strconv.ParseUint(nodeStr[:3], 8, 64)
			if err != nil {
				p.buf = append(p.buf, '\\', ch)
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

	return pipeline.ActionPass
}
