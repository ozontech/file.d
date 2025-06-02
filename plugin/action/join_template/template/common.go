package template

import "github.com/ozontech/file.d/plugin/action/join_template/ascii"

func containsOnlySpaces(s string) bool {
	return firstNonSpaceIndex(s) == -1
}

func firstNonSpaceIndex(s string) int {
	for i, c := range []byte(s) {
		if !ascii.IsSpace(c) {
			return i
		}
	}

	return -1
}

func containsOnlyDigits(s string) bool {
	for _, c := range []byte(s) {
		if !ascii.IsDigit(c) {
			return false
		}
	}

	return true
}
