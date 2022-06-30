package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	insaneJSON "github.com/vitkovskii/insane-json"
)

var (
	ErrFieldNotObject = errors.New("can't write because it is not an object")
)

// Clone deeply copies string
func CloneString(s string) string {
	if len(s) == 0 {
		return ""
	}
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}

// ByteToStringUnsafe converts byte slice to string without memory copy
// This creates mutable string, thus unsafe method, should be used with caution (never modify provided byte slice)
func ByteToStringUnsafe(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToByteUnsafe converts string to byte slice without memory copy
// This creates mutable string, thus unsafe method, should be used with caution (never modify resulting byte slice)
func StringToByteUnsafe(s string) []byte {
	var buf = *(*[]byte)(unsafe.Pointer(&s))
	(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(s)
	return buf
}

/* There are actually a lot of interesting ways to do this. Saving them here, for the purpose of possible debugging.

func StringToByteUnsafe(s string) []byte {
	const max = 0x7fff0000
	if len(s) > max {
		panic("string too long")
	}
	return (*[max]byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data))[:len(s):len(s)]
}

func StringToByteUnsafe(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}
*/

const formats = "ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"

func ParseFormatName(formatName string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(formatName)) {
	case "ansic":
		return time.ANSIC, nil
	case "unixdate":
		return time.UnixDate, nil
	case "rubydate":
		return time.RubyDate, nil
	case "rfc822":
		return time.RFC822, nil
	case "rfc822z":
		return time.RFC822Z, nil
	case "rfc850":
		return time.RFC850, nil
	case "rfc1123":
		return time.RFC1123, nil
	case "rfc1123z":
		return time.RFC1123Z, nil
	case "rfc3339":
		return time.RFC3339, nil
	case "rfc3339nano":
		return time.RFC3339Nano, nil
	case "kitchen":
		return time.Kitchen, nil
	case "stamp":
		return time.Stamp, nil
	case "stampmilli":
		return time.StampMilli, nil
	case "stampmicro":
		return time.StampMicro, nil
	case "stampnano":
		return time.StampNano, nil
	default:
		return "", fmt.Errorf("unknown format name %q, should be one of %s", formatName, formats)
	}
}

// ParseLevelAsNumber converts log level to the int representation according to the RFC-5424.
func ParseLevelAsNumber(level string) int {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "0", "emergency":
		return 0
	case "1", "alert":
		return 1
	case "2", "critical", "crit":
		return 2
	case "3", "error", "err":
		return 3
	case "4", "warning", "warn":
		return 4
	case "5", "notice":
		return 5
	case "6", "informational", "info":
		return 6
	case "7", "debug":
		return 7
	default:
		return -1
	}
}

var levelNames = []string{
	"emergency",
	"alert",
	"critical",
	"error",
	"warning",
	"notice",
	"informational",
	"debug",
}

// ParseLevelAsString converts log level to the string representation according to the RFC-5424.
func ParseLevelAsString(level string) string {
	parsed := ParseLevelAsNumber(level)
	if parsed == -1 {
		return ""
	}
	return levelNames[parsed]
}

// CreateNestedField creates nested field by the path.
// For example, []string{"path.to", "object"} creates:
// { "path.to": {"object": {} }
// Warn: it overrides fields if it contains non-object type on the path. For example:
// in: { "path.to": [{"userId":"12345"}] }, out: { "path.to": {"object": {}} }
func CreateNestedField(root *insaneJSON.Root, path []string) *insaneJSON.Node {
	curr := root.Node
	for _, p := range path {
		curr = curr.AddFieldNoAlloc(root, p).MutateToObject()
	}
	return curr
}
