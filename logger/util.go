package logger

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

type condFn func() (result string)

func Header(name string) string {
	base := []byte("=================================")

	offset := len(base)/2 - len(name)/2

	for i, c := range name {
		base[offset+i] = byte(unicode.ToUpper(c))
	}

	return string(base) + "\n"
}

func Cond(is bool, positive string, negative condFn) string {
	if is {
		return positive
	} else {
		return negative()
	}
}

func Numerate(content string) string {
	lines := strings.Split(strings.TrimSpace(content), "\n")

	x := len(lines)
	digits := int(math.Log10(float64(x)))

	for i := range lines {
		lines[i] = fmt.Sprintf("%*d: %s", digits, i+1, lines[i])
	}

	return strings.Join(lines, "\n")
}
