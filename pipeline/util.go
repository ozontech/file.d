package pipeline

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

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

func ParseLevel(level string) int {
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
		return 6
	}
}

func TrimSpaceFunc(r rune) bool {
	return byte(r) == ' '
}
