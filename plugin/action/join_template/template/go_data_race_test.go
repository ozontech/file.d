package template

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/join_template/sample"
	"github.com/stretchr/testify/require"
)

func BenchmarkGoDataRaceStartMixedRes(b *testing.B) {
	cur, err := InitTemplate(nameGoDataRace, true)
	require.NoError(b, err)

	lines := getLines(sample.GoDataRace)

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				goDataRaceStartCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				cur.StartRe.MatchString(line)
			}
		}
	})
}

func BenchmarkGoDataRaceFinishMixedRes(b *testing.B) {
	cur, err := InitTemplate(nameGoDataRace, true)
	require.NoError(b, err)

	lines := getLines(sample.GoDataRace)

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				goDataRaceFinishCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				cur.ContinueRe.MatchString(line)
			}
		}
	})
}

func BenchmarkGoDataRaceStartNegativeRes(b *testing.B) {
	cur, err := InitTemplate(nameGoDataRace, true)
	require.NoError(b, err)

	lines := getRandLines()

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				goDataRaceStartCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				cur.StartRe.MatchString(line)
			}
		}
	})
}

func BenchmarkGoDataRaceFinishNegativeRes(b *testing.B) {
	cur, err := InitTemplate(nameGoDataRace, true)
	require.NoError(b, err)

	lines := getRandLines()

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				goDataRaceFinishCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				cur.ContinueRe.MatchString(line)
			}
		}
	})
}

func TestGoDataRaceSameResults(t *testing.T) {
	cur, err := InitTemplate(nameGoDataRace, true)
	require.NoError(t, err)

	lines := append(getLines(sample.GoDataRace), getRandLines()...)

	for _, line := range lines {
		require.Equal(t, cur.StartRe.MatchString(line), goDataRaceStartCheck(line), line)
		require.Equal(t, cur.ContinueRe.MatchString(line), goDataRaceFinishCheck(line), line)
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
