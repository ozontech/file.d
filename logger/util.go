package logger

import (
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
