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

// replaces regexp (^(?i)\s*--- End of)
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
// NOTE: only first occurrence counts
func containsException(s string) bool {
	i := strings.Index(s, exceptionSubstr)
	// not found or found at start
	if i < 1 {
		return false
	}

	i--

	if s[i] == '.' {
		return i > 0 && isLetterOrUnderscoreOrDigit(s[i-1])
	}

	return isLetterOrUnderscoreOrDigit(s[i])
}
