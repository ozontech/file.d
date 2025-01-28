package join_template

import (
	"strings"
)

const (
	goroutineIDPrefix = "goroutine "
	goroutineIDSuffix = " ["

	lineNumberPart = ".go:"

	panicAddrPart1 = "panic"
	panicAddrPart2 = "0x"

	createdByPart = "created by "
)

func goPanicStartCheck(s string) bool {
	return strings.HasPrefix(s, "panic:") ||
		strings.HasPrefix(s, "fatal error:") ||
		strings.Contains(s, "http: panic serving")
}

func goPanicContinueCheck(s string) bool {
	return strings.HasPrefix(s, "[signal") ||
		containsOnlySpaces(s) ||
		containsGoroutineID(s) ||
		containsLineNumber(s) ||
		containsCreatedBy(s) ||
		containsPanicAddress(s) ||
		strings.Contains(s, "panic:") ||
		containsCall(s)
}

// replaces regexp (^\s*$)
func containsOnlySpaces(s string) bool {
	for _, c := range []byte(s) {
		if !isSpace(c) {
			return false
		}
	}

	return true
}

func isSpace(c byte) bool {
	switch c {
	case ' ', '\n', '\t':
		return true
	default:
		return false
	}
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
	if i == -1 {
		return false
	}

	// no digits
	if i == 0 {
		return false
	}

	return containsOnlyDigits(s[:i])
}

func containsOnlyDigits(s string) bool {
	for _, c := range []byte(s) {
		if !isDigit(c) {
			return false
		}
	}

	return true
}

// replaces regexp (\.go:[0-9]+)
// NOTE: only first occurrence counts
func containsLineNumber(s string) bool {
	i := strings.Index(s, lineNumberPart)
	if i == -1 {
		return false
	}

	i += len(lineNumberPart)

	return i < len(s) && isDigit(s[i])
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

func isLetterOrUnderscoreOrDigit(c byte) bool {
	return isLetterOrUnderscore(c) || isDigit(c)
}

func isLowerCaseLetter(c byte) bool {
	return 'a' <= c && c <= 'z'
}

func isUpperCaseLetter(c byte) bool {
	return 'A' <= c && c <= 'Z'
}

func isLetter(c byte) bool {
	return isLowerCaseLetter(c) || isUpperCaseLetter(c)
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isHexDigit(c byte) bool {
	return isDigit(c) || ('a' <= c && c <= 'f')
}

func isLetterOrUnderscore(c byte) bool {
	return isLetter(c) || c == '_'
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
	for ; left >= 0 && isLetterOrUnderscoreOrDigit(s[left]); left-- {
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
		case isLetterOrUnderscore(c):
			return true
		case isDigit(c):
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

	return isHexDigit(s[0])
}
