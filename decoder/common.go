package decoder

// atoi is allocation free ASCII number to integer conversion
func atoi(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	x := 0
	for _, c := range b {
		if c < '0' || '9' < c {
			return 0, false
		}
		x = x*10 + int(c) - '0'
	}
	return x, true
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func checkNumber(num []byte, minimum, maximum int) bool {
	x, ok := atoi(num)
	return ok && x >= minimum && x <= maximum
}
