package join_template

import (
	"math/rand"
	"strings"
)

func getRandLines() []string {
	const count = 100

	lines := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lines = append(lines, getRandLine())
	}

	return lines
}

func getRandLine() string {
	const (
		maxLength = 1000

		from = ' '
		to   = '~'
	)

	sz := rand.Intn(maxLength) + 1

	var b strings.Builder
	b.Grow(sz)

	for i := 0; i < sz; i++ {
		b.WriteByte(from + byte(rand.Intn(to-from)))
	}

	return b.String()
}

type testCase struct {
	s   string
	res bool
}

func getCases(positive, negative []string) []testCase {
	result := make([]testCase, 0, len(positive)+len(negative))

	for _, s := range positive {
		result = append(result, testCase{s: s, res: true})
	}

	for _, s := range negative {
		result = append(result, testCase{s: s})
	}

	return result
}
