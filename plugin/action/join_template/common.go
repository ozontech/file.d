package join_template

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

func containsOnlyDigits(s string) bool {
	for _, c := range []byte(s) {
		if !isDigit(c) {
			return false
		}
	}

	return true
}

func isHexDigit(c byte) bool {
	return isDigit(c) || ('a' <= c && c <= 'f')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isLetterOrUnderscoreOrDigit(c byte) bool {
	return isLetterOrUnderscore(c) || isDigit(c)
}

func isLetterOrUnderscore(c byte) bool {
	return isLetter(c) || c == '_'
}

func isLetter(c byte) bool {
	return isLowerCaseLetter(c) || isUpperCaseLetter(c)
}

func isLowerCaseLetter(c byte) bool {
	return 'a' <= c && c <= 'z'
}

func isUpperCaseLetter(c byte) bool {
	return 'A' <= c && c <= 'Z'
}
