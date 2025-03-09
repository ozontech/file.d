package template

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/join_template/ascii"
)

const (
	goroutineIDPrefix = "goroutine "
	goroutineIDSuffix = " ["

	lineNumberPart = ".go:"

	panicAddrPart1 = "panic"
	panicAddrPart2 = "0x"

	createdByPart = "created by "

	httpPanicSubstr     = "http: panic serving"
	httpPanicTimeFormat = "2025/03/05 19:30:57 "
)

func goPanicStartCheck(s string) bool {
	return strings.HasPrefix(s, "panic:") ||
		strings.HasPrefix(s, "fatal error:") ||
		strings.Contains(s, "http: panic serving")
}

func goPanicStartCheck2(s string) bool {
	return strings.HasPrefix(s, "panic:") ||
		strings.HasPrefix(s, "fatal error:") ||
		containsHTTPPanic(s)
}

func containsHTTPPanic(s string) bool {
	if len(s) < len(httpPanicTimeFormat) {
		return false
	}

	return strings.HasPrefix(s[len(httpPanicTimeFormat):], httpPanicSubstr)
}

func goPanicContinueCheck(s string) bool {
	return strings.HasPrefix(s, "[signal") ||
		containsOnlySpaces(s) || // replaces regexp (^\s*$)
		containsGoroutineID(s) ||
		containsLineNumber(s) ||
		containsCreatedBy(s) ||
		containsPanicAddress(s) ||
		strings.Contains(s, "panic:") ||
		containsCall(s)
}

// replaces regexp (goroutine [0-9]+ \[)
// NOTE: only first occurrence counts
func containsGoroutineID(s string) bool {
	i := strings.Index(s, goroutineIDPrefix)
	if i == -1 {
		return false
	}

	s = s[i+len(goroutineIDPrefix):]

	i = strings.Index(s, goroutineIDSuffix)
	// not found or found at start
	if i < 1 {
		return false
	}

	return containsOnlyDigits(s[:i])
}

// replaces regexp (\.go:[0-9]+)
// NOTE: only first occurrence counts
func containsLineNumber(s string) bool {
	i := strings.Index(s, lineNumberPart)
	if i == -1 {
		return false
	}

	i += len(lineNumberPart)

	return i < len(s) && ascii.IsDigit(s[i])
}

// Two regexps:
// - created by .*\/?.*\.
// - created by .*\.
// are equal in sense of matching.
// So check the second one.
func containsCreatedBy(s string) bool {
	i := strings.Index(s, createdByPart)
	if i == -1 {
		return false
	}

	s = s[i+len(createdByPart):]

	return strings.IndexByte(s, '.') != -1
}

// replaces regexp ([A-Za-z_]+[A-Za-z0-9_]*\)?\.[A-Za-z0-9_]+\(.*\))
// it recognizes:
// - calls of functions in packages
// - calls of methods of structs
// NOTE: only last occurrence counts
func containsCall(s string) bool {
	i := strings.LastIndexByte(s, ')')
	if i == -1 {
		return false
	}

	s = s[:i]

	i = strings.LastIndexByte(s, '(')
	if i == -1 {
		return false
	}

	right := i - 1
	left := right
	for ; left >= 0 && ascii.IsLetterOrUnderscoreOrDigit(s[left]); left-- {
	}

	if left == right {
		return false
	}

	if left == -1 {
		return false
	}

	if s[left] != '.' {
		return false
	}

	left--

	// skip bracket if it occurred
	if left >= 0 && s[left] == ')' {
		left--
	}

	return endsWithIdentifier(s[:left+1])
}

// Two regexps:
// - [A-Za-z_]+[A-Za-z0-9_]*$
// - [A-Za-z_][0-9]*$
// are equal in sense of matching.
// So check the second one.
func endsWithIdentifier(s string) bool {
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		switch {
		case ascii.IsLetterOrUnderscore(c):
			return true
		case ascii.IsDigit(c):
		default:
			return false
		}
	}

	return false
}

// regexp
// (panic.+[0-9]x[0-9,a-f]+)
// can be replaced with
// (panic.+0x[0-9,a-f]+)
// so check the second one
// NOTE: only first occurrence counts
func containsPanicAddress(s string) bool {
	i := strings.Index(s, panicAddrPart1)
	if i == -1 {
		return false
	}

	s = s[i+len(panicAddrPart1):]

	i = strings.Index(s, panicAddrPart2)
	if i == -1 {
		return false
	}

	// no chars between parts
	if i == 0 {
		return false
	}

	s = s[i+len(panicAddrPart2):]

	if s == "" {
		return false
	}

	return ascii.IsHexDigit(s[0])
}
