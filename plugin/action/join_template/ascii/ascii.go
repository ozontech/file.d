package ascii

func IsSpace(c byte) bool {
	switch c {
	case ' ', '\n', '\t':
		return true
	default:
		return false
	}
}

func IsHexDigit(c byte) bool {
	return IsDigit(c) || ('a' <= c && c <= 'f')
}

func IsDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func IsLetterOrUnderscoreOrDigit(c byte) bool {
	return IsLetterOrUnderscore(c) || IsDigit(c)
}

func IsLetterOrUnderscore(c byte) bool {
	return IsLetter(c) || c == '_'
}

func IsLetter(c byte) bool {
	return IsLowerCaseLetter(c) || IsUpperCaseLetter(c)
}

func IsLowerCaseLetter(c byte) bool {
	return 'a' <= c && c <= 'z'
}

func ToLower(c byte) byte {
	if IsUpperCaseLetter(c) {
		return c + 'a' - 'A'
	}

	return c
}

func IsUpperCaseLetter(c byte) bool {
	return 'A' <= c && c <= 'Z'
}
