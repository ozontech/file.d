package runtime

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// appendJSONValue appends to dst, including partially written data on error.
// JSONNodeValue keeps insane-json's encoding and raw number literals; language
// strings use encoding/json-compatible quoting, including invalid UTF-8 repair.
func appendJSONValue(dst []byte, v core.Value) ([]byte, error) {
	switch val := v.(type) {
	case core.NullValue:
		return append(dst, "null"...), nil
	case core.BoolValue:
		return strconv.AppendBool(dst, val.V), nil
	case core.IntegerValue:
		return strconv.AppendInt(dst, val.V, 10), nil
	case core.FloatValue:
		return strconv.AppendFloat(dst, val.V, 'f', -1, 64), nil
	case core.StringValue:
		return appendJSONString(dst, val.V), nil
	case core.ArrayValue:
		dst = append(dst, '[')
		for i, element := range val.V {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendJSONValue(dst, element)
			if err != nil {
				return dst, err
			}
		}
		return append(dst, ']'), nil
	case core.ObjectValue:
		dst = append(dst, '{')
		first := true
		for key, element := range val.V {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			var err error
			dst, err = appendJSONValue(dst, element)
			if err != nil {
				return dst, err
			}
		}
		return append(dst, '}'), nil
	case core.JSONNodeValue:
		if val.N == nil {
			return append(dst, "null"...), nil
		}
		return val.N.Encode(dst), nil
	}
	return dst, fmt.Errorf("cannot serialize %s to JSON", v.Kind())
}

// appendJSONString matches json.Marshal's string escaping without allocating
// a temporary encoded string for each value or object key.
func appendJSONString(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= ' ' && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '"', '\\':
				dst = append(dst, '\\', c)
			case '\b':
				dst = append(dst, `\b`...)
			case '\f':
				dst = append(dst, `\f`...)
			case '\n':
				dst = append(dst, `\n`...)
			case '\r':
				dst = append(dst, `\r`...)
			case '\t':
				dst = append(dst, `\t`...)
			default:
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `\ufffd`...)
			start = i + size
		} else if r == '\u2028' || r == '\u2029' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hex[r&0xf])
			start = i + size
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
