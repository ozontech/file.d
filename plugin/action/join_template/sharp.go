package join_template

import (
	"strings"
)

const (
	startSubstr = "unhandled exception"

	atSubstr        = "at"
	arrowSubstr     = "--->"
	endOfSubstr     = "--- End of"
	exceptionSubstr = "Exception:"
)

// replace regexp (^\s*(?i)Unhandled exception)
func sharpStartCheck(s string) bool {
	i := firstNonSpaceIndex(s)
	if i == -1 {
		return false
	}

	s = s[i:]

	if len(s) < len(startSubstr) {
		return false
	}

	s = s[:len(startSubstr)]

	return equalCaseInsensitive(s, startSubstr)
}

func firstNonSpaceIndex(s string) int {
	for i, c := range []byte(s) {
		if !isSpace(c) {
			return i
		}
	}

	return -1
}

func sharpContinueCheck(s string) bool {
	return containsAt(s) ||
		containsArrow(s) ||
		containsEndOf(s) ||
		containsException(s)
}

// replaces regexp (^\s*at\s.*)
func containsAt(s string) bool {
	i := firstNonSpaceIndex(s)
	if i == -1 {
		return false
	}

	s = s[i:]

	if !strings.HasPrefix(s, atSubstr) {
		return false
	}

	s = s[len(atSubstr):]

	if s == "" {
		return false
	}

	return isSpace(s[0])
}

// replaces regexp (\s*--->)
func containsArrow(s string) bool {
	i := firstNonSpaceIndex(s)
	if i == -1 {
		return false
	}

	s = s[i:]

	return strings.HasPrefix(s, arrowSubstr)
}

func containsEndOf(s string) bool {
	i := firstNonSpaceIndex(s)
	if i == -1 {
		return false
	}

	s = s[i:]

	if len(s) < len(endOfSubstr) {
		return false
	}

	s = s[:len(endOfSubstr)]

	return equalCaseInsensitive(s, endOfSubstr)
}

func toLower(c byte) byte {
	if isUpperCaseLetter(c) {
		return c + 'a' - 'A'
	}

	return c
}

func equalCaseInsensitive(a, b string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, c := range []byte(a) {
		if toLower(c) != toLower(b[i]) {
			return false
		}
	}

	return true
}

// replaces regexp (\.?\w+\.?Exception:)
// POSSIBLE ERROR: only first occurrence counts
func containsException(s string) bool {
	i := strings.Index(s, exceptionSubstr)
	if i == -1 {
		return false
	}

	if i == 0 {
		return false
	}

	i--

	if s[i] == '.' {
		return i > 0 && isLetterOrUnderscoreOrDigit(s[i-1])
	}

	return isLetterOrUnderscoreOrDigit(s[i])
}
