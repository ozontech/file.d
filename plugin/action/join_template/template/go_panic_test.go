package template

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/join_template/sample"
	"github.com/stretchr/testify/require"
)

func BenchmarkPanicStartMixedRes(b *testing.B) {
	lines := getLines(sample.Panics)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicStartCheck(line)
		}
	}
}

func BenchmarkPanicContinueMixedRes(b *testing.B) {
	lines := getLines(sample.Panics)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicContinueCheck(line)
		}
	}
}

func BenchmarkPanicStartNegativeRes(b *testing.B) {
	lines := getRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicStartCheck(line)
		}
	}
}

func BenchmarkPanicContinueNegativeRes(b *testing.B) {
	lines := getRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goPanicContinueCheck(line)
		}
	}
}

func TestContainsCreatedBy(t *testing.T) {
	positive := []string{
		"created by net/http.(*Server).Serve",
		"created by .",
		"created by some.qwe123",
		"    created by some.qwe123",
	}

	negative := []string{
		// no 'created by ' part
		"created net/http.(*Server).Serve",
		"created by{net/http.(*Server).Serve}",
		"by net/http.(*Server).Serve",
		"",

		// no period after 'created by ' part
		"created by ",
		"created by     ",
		"    created by someQwe123",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsCreatedBy(tt.s), tt.s)
	}
}

func TestEndsWithIdentifier(t *testing.T) {
	positive := []string{
		"_",
		"a1",
		"A1",
		"abc",
		"ABC",
		"aB",
		"1a",
		"123 _",
		"    a",
		"    a1",
		"    1a",
	}

	negative := []string{
		"",
		"1234",
		"a-123",
		"     ",
		"    1",
		"  a 1",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, endsWithIdentifier(tt.s), tt.s)
	}
}

func TestContainsGoroutineID(t *testing.T) {
	positive := []string{
		"goroutine 1 [running]:",
		"    goroutine 123 [running]:",
		"goroutine 100 [",
		"goroutine 1 [qwe",

		"goroutine 108568134 gp=0xc0011cddc0 m=nil [IO wait]:",
	}

	negative := []string{
		"qwe abc", // no goroutine id prefix
		"goroutine",

		"goroutine qwe", // no space after goroutine id prefix
		"goroutine [",

		"goroutine  ", // no chars between goroutine id prefix and space
		"goroutine  [",

		"goroutine ABC ", // only digits allowed between goroutine id prefix and space
		"goroutine 1a ",

		"goroutine 108568134 gp=0xc0011cddc0 m=nil[IO wait]:", // no goroutine id suffix

		"goroutine QWE goroutine 1 [running]:", // only first occurrence counts
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsGoroutineID(tt.s), tt.s)
	}
}

func TestContainsLineNumber(t *testing.T) {
	positive := []string{
		"/some/path/proc.go:123",
		"    /some/path/proc.go:123",
		"qwe.go:100",

		// it's strange but it's true
		"/some/path/util.go:0", // zero line number
		".go:123",              // no filename
	}

	negative := []string{
		"qwe", // no line number part

		"proc.go:", // no chars after line number part

		"proc.go:qwe", // no digit right after colon
		"proc.go: 1",

		"proc.go:qwe proc.go:100", // only first occurrence counts
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsLineNumber(tt.s), tt.s)
	}
}

func TestContainsPanicAddress(t *testing.T) {
	positive := []string{
		"panic(0xb6afc0, 0xd7c240)",
		"panic({0x102feb9c0, 0x102ffc8d0})",
		"panic(0xb6afc0",
		"panic(0xb",
		"panic(0x123",
		"panic (  0xb",

		// false positive
		"/usr/local/go/src/runtime/panic.go:513 +0x1b9",
	}

	negative := []string{
		"qwe",       // no first part
		"panic 123", // no second part after first

		"panic0x",   // no chars between parts
		"panic 0x",  // no chars after second part
		"panic 0xM", // 'M' is not hex digit

		"panic 0xQWE 0x123", // only first occurrence counts
		"panic 0xQWE panic 0x123",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsPanicAddress(tt.s), tt.s)
	}
}

func TestContainsCall(t *testing.T) {
	positive := []string{
		"main.main()",
		"(some).main()",
		"net/http.(*conn).serve.func1(0xc000a54dc0)",

		"nlopes/slack.(*RTM).receiveIncomingEvent(0xc000119040)",
		"sync.(*WaitGroup).state(...)",

		// short identifier
		"a.main()",
		"(a).main()",
		"abc.A()",
	}

	negative := []string{
		"qwe",   // no closing bracket
		"qwe )", // no opening bracket before closing bracket

		// no func/method right before opening bracket
		"()",
		"a.F ()",
		"abc.main*()",

		// no point right before func/method name
		"F()",
		"some. F()",

		"[some].F()", // wrong bracket before point

		"some.F() ... 123.F()", // only last occurrence counts

		// no package identifier (for function calls) or
		// no class identifier (for method calls) before point
		".F()",
		"some .F()",
		"123.F()",
		"a*.main()",
		"(some*).main()",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsCall(tt.s), tt.s)
	}
}
