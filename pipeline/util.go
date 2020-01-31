package pipeline

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func ByteToStringUnsafe(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StringToByteUnsafe(s string) []byte {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	var sh reflect.SliceHeader
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return *(*[]byte)(unsafe.Pointer(&sh))
}

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
