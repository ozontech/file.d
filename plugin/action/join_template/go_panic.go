package join_template

import (
	"strings"
	"unicode"
)

const (
	goroutineIDPrefix = "goroutine "
	goroutineIDSuffix = " ["
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
		checkLineNumberAndFile(s) ||
		checkCreatedBy(s) ||
		checkPanicAddress(s) ||
		strings.Contains(s, "panic:") ||
		checkMethodCall(s)
}

// replaces regexp (^\s*$)
func containsOnlySpaces(s string) bool {
	for _, c := range []byte(s) {
		switch c {
		case ' ', '\n', '\t':
		default:
			return false
		}
	}

	return true
}

// replaces regexp (goroutine [0-9]+ \[)
// POSSIBLE ERROR: only first occurrence counts
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

const lineSuffix = ".go:"

// POSSIBLE ERROR: not only first occurrence counts
func checkLineNumberAndFile(s string) bool {
	i := strings.Index(s, lineSuffix)
	if i == -1 {
		return false
	}

	s = s[i+len(lineSuffix):]

	i = 0
	for ; i < len(s) && unicode.IsDigit(rune(s[i])); i++ {
	}

	return i > 0
}

const checkCreatedBySubstr = "created by "

func checkCreatedBy(s string) bool {
	i := strings.Index(s, checkCreatedBySubstr)
	if i == -1 {
		return false
	}

	s = s[i+len(checkCreatedBySubstr):]

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

func isLetterOrUnderscore(c byte) bool {
	return isLetter(c) || c == '_'
}

func checkMethodCallIndex(s string, pos int) bool {
	if s[pos] != ')' {
		return false
	}

	i := strings.LastIndex(s[:pos], "(")
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
	if left >= 0 && s[left] == ')' {
		left--
	}

	return endsWithIdentifier(s[:left])
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

func checkMethodCall(s string) bool {
	for i := len(s) - 1; i >= 0; i-- {
		if checkMethodCallIndex(s, i) {
			return true
		}
	}

	return false
}

const (
	checkPanicPrefix1 = "panic"
	checkPanicPrefix2 = "0x"
)

func checkPanicAddress(s string) bool {
	i := strings.Index(s, checkPanicPrefix1)
	if i == -1 {
		return false
	}

	s = s[i+len(checkPanicPrefix1):]

	i = strings.Index(s, checkPanicPrefix2)
	if i == -1 {
		return false
	}

	// no chars between parts
	if i == 0 {
		return false
	}

	s = s[i+len(checkPanicPrefix2):]

	if s == "" {
		return false
	}

	c := s[0]
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f'
}
