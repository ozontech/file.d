package keep_fields

import (
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestKeepFields(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"field_1", "field_2"}}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_3":"value_3","a":"b"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"field_1":"value_1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"field_2":"value_2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{}`, outEvents[2], "wrong event")
}

func BenchmarkGenerics(b *testing.B) {
	a := getRandLines()
	s := a[len(a)-1]

	b.ResetTimer()

	b.Run("generic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = slices.Index(a, s)
		}
	})
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = find(a, s)
		}
	})
}

func getRandLines() []string {
	const count = 1000

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
