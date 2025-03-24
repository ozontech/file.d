package pipeline

import (
	"reflect"
	"strings"
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
