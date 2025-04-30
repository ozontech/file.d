package template

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/join_template/sample"
	"github.com/stretchr/testify/require"
)

func BenchmarkGoDataRaceStartMixedRes(b *testing.B) {
	lines := getLines(sample.GoDataRace)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goDataRaceStartCheck(line)
		}
	}
}

func BenchmarkGoDataRaceFinishMixedRes(b *testing.B) {
	lines := getLines(sample.GoDataRace)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goDataRaceFinishCheck(line)
		}
	}
}

func BenchmarkGoDataRaceStartNegativeRes(b *testing.B) {
	lines := getRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goDataRaceStartCheck(line)
		}
	}
}

func BenchmarkGoDataRaceFinishNegativeRes(b *testing.B) {
	lines := getRandLines()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			goDataRaceFinishCheck(line)
		}
	}
}

func TestGoDataRaceStartCheck(t *testing.T) {
	positive := []string{
		"WARNING: DATA RACE",
		"WARNING: DATA RACE    ",
		"WARNING: DATA RACE qwe",
	}

	negative := []string{
		"",
		"qwe",
		"WARNING",
		"WARNING: DATA",
		"  WARNING: DATA RACE",
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, goDataRaceStartCheck(test.s), i)
	}
}

func TestGoDataRaceFinishCheck(t *testing.T) {
	prefix := goDataRaceFinishPrefix

	positive := []string{
		prefix,
		prefix + "    ",
		prefix + " qwe",
	}

	n := len(prefix)

	negative := []string{
		"",
		"qwe",
		prefix[:n-6],
		prefix[:n-4],
		prefix[:n-2],
		"  " + prefix,
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, goDataRaceFinishCheck(test.s), i)
	}
}
