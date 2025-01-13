package pipeline

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	insaneJSON "github.com/ozontech/insane-json"
)

// Clone deeply copies string
func CloneString(s string) string {
	if s == "" {
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

const (
	formats      = "ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|unixtime|nginx_errorlog"
	UnixTime     = "unixtime"
	nginxDateFmt = "2006/01/02 15:04:05"
)

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
	case "nginx_errorlog":
		return nginxDateFmt, nil
	case UnixTime:
		return UnixTime, nil
	default:
		return "", fmt.Errorf("unknown format name %q, should be one of %s", formatName, formats)
	}
}

func ParseTime(format, value string) (time.Time, error) {
	if format == UnixTime {
		return parseUnixTime(value)
	}
	return time.Parse(format, value)
}

func parseUnixTime(value string) (time.Time, error) {
	numbers := strings.Split(value, ".")
	var sec, nsec int64
	var err error
	switch len(numbers) {
	case 1:
		sec, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
	case 2:
		sec, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		nsec, err = strconv.ParseInt(numbers[1], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
	default:
		return time.Time{}, fmt.Errorf("unexpected time format")
	}
	return time.Unix(sec, nsec), nil
}

type LogLevel int

const (
	LevelUnknown LogLevel = iota - 1
	LevelEmergency
	LevelAlert
	LevelCritical
	LevelError
	LevelWarning
	LevelNotice
	LevelInformational
	LevelDebug
)

// ParseLevelAsNumber converts log level to the int representation according to the RFC-5424.
func ParseLevelAsNumber(level string) LogLevel {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "0", "emergency", "emerg", "fatal", "panic", "dpanic":
		return LevelEmergency
	case "1", "alert":
		return LevelAlert
	case "2", "critical", "crit":
		return LevelCritical
	case "3", "error", "err":
		return LevelError
	case "4", "warning", "warn":
		return LevelWarning
	case "5", "notice":
		return LevelNotice
	case "6", "informational", "info":
		return LevelInformational
	case "7", "debug":
		return LevelDebug
	default:
		return LevelUnknown
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

const LevelUnknownStr = ""

// ParseLevelAsString converts log level to the string representation according to the RFC-5424.
func ParseLevelAsString(level string) string {
	parsed := ParseLevelAsNumber(level)
	if parsed == LevelUnknown {
		return LevelUnknownStr
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
		curr = curr.AddFieldNoAlloc(root, p)
		if !curr.IsObject() {
			curr.MutateToObject()
		}
	}
	return curr
}
