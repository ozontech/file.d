package pipeline

import (
	"fmt"
	"math"
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
	formats       = "ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|unixtime|unixtimemilli|unixtimemicro|unixtimenano|nginx_errorlog"
	UnixTime      = "unixtime"
	UnixTimeMilli = "unixtimemilli"
	UnixTimeMicro = "unixtimemicro"
	UnixTimeNano  = "unixtimenano"
	nginxDateFmt  = "2006/01/02 15:04:05"
)

func ParseFormatName(formatName string) (string, error) {
	formatNameProcessed := strings.ToLower(strings.TrimSpace(formatName))
	switch formatNameProcessed {
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
	case UnixTime, UnixTimeMilli, UnixTimeMicro, UnixTimeNano:
		return formatNameProcessed, nil
	default:
		return "", fmt.Errorf("unknown format name %q, should be one of %s", formatName, formats)
	}
}

type unixTimeFormat int

const (
	unixTimeSec unixTimeFormat = iota
	unixTimeMilli
	unixTimeMicro
	unixTimeNano
)

func ParseTime(format, value string) (time.Time, error) {
	switch format {
	case UnixTime:
		return parseUnixTime(value, unixTimeSec)
	case UnixTimeMilli:
		return parseUnixTime(value, unixTimeMilli)
	case UnixTimeMicro:
		return parseUnixTime(value, unixTimeMicro)
	case UnixTimeNano:
		return parseUnixTime(value, unixTimeNano)
	default:
		return time.Parse(format, value)
	}
}

func parseUnixTime(value string, format unixTimeFormat) (time.Time, error) {
	numbers := strings.Split(value, ".")
	var sec, nsec, val int64
	var err error
	switch len(numbers) {
	case 1:
		val, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		switch format {
		case unixTimeSec:
			sec = val
		case unixTimeMilli:
			sec = val / 1e3
			nsec = (val % 1e3) * 1e6
		case unixTimeMicro:
			sec = val / 1e6
			nsec = (val % 1e6) * 1e3
		case unixTimeNano:
			sec = val / 1e9
			nsec = val % 1e9
		}
	case 2:
		// when timestamp is presented as a float number its whole part is always considered as seconds
		// and the fractional part is fractions of a second
		sec, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		nsec, err = strconv.ParseInt(numbers[1], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		// if there are less than 9 digits to the right of the decimal point
		// it must be multiplied by 10^(9 - digits) to get nsec value
		digits := len(numbers[1])
		if digits < 9 {
			nsec *= int64(math.Pow10(9 - digits))
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

// MergeToRoot like insaneJSON.Node.MergeWith, but without allocations.
func MergeToRoot(root *insaneJSON.Root, src *insaneJSON.Node) {
	if root == nil || root.Node == nil || src == nil {
		return
	}
	if !root.IsObject() || !src.IsObject() {
		return
	}

	fields := src.AsFields()
	for _, child := range fields {
		childField := child.AsString()
		x := root.Node.AddFieldNoAlloc(root, childField)
		x.MutateToNode(child.AsFieldValue())
	}
}
