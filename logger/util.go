package logger

import (
	"fmt"
	"math"
	"strconv"
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

	format := "%" + strconv.Itoa(digits) + "d: %s"
	for i := range lines {
		lines[i] = fmt.Sprintf(format, i+1, lines[i])
	}

	return strings.Join(lines, "\n")
}
