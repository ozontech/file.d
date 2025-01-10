package join_template

import (
	"strings"
	"unicode"
)

func goPanicStartCheck(s string) bool {
	return strings.HasPrefix(s, "panic:") ||
		strings.HasPrefix(s, "fatal error:") ||
		strings.Contains(s, "http: panic serving")
}

func goPanicContinueCheck(s string) bool {
	return strings.HasPrefix(s, "[signal") ||
		checkOnlySpaces(s) ||
		checkGoroutineID(s) ||
		checkLineNumberAndFile(s) ||
		checkCreatedBy(s) ||
		checkPanicAddress(s) ||
		strings.Contains(s, "panic:") ||
		checkMethodCall(s)
}

func checkOnlySpaces(s string) bool {
	for _, c := range []byte(s) {
		switch c {
		case ' ', '\n', '\t':
		default:
			return false
		}
	}

	return true
}

const (
	checkGoroutineIDPrefix = "goroutine "
	checkGoroutineIDSuffix = " ["
)

// may be error: not only first occurrence counts
func checkGoroutineID(s string) bool {
	i := strings.Index(s, checkGoroutineIDPrefix)
	if i == -1 {
		return false
	}

	s = s[i+len(checkGoroutineIDPrefix):]

	i = strings.Index(s, checkGoroutineIDSuffix)
	if i == -1 {
		return false
	}

	// no goroutine id found
	if i == 0 {
		return false
	}

	s = s[:i]
	return checkOnlyDigits(s)
}

func checkOnlyDigits(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}

	return true
}

const lineSuffix = ".go:"

// may be error: not only first occurrence counts
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

func isIDChar(c byte) bool {
	isLowerCase := 'a' <= c && c <= 'z'
	isUpperCase := 'A' <= c && c <= 'Z'
	isDigit := '0' <= c && c <= '9'
	return isLowerCase || isUpperCase || isDigit || c == '_'
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
	for ; left >= 0 && isIDChar(s[left]); left-- {
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
		res := checkMethodCallIndex(s, i)
		if res {
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
