package keep_fields

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

type share int

const (
	share0 share = iota
	share50
	share100
)

func getFlatConfig() []string {
	result := make([]string, 10)
	for i := range result {
		result[i] = fmt.Sprintf("field%d", i+1)
	}

	return result
}

func getEventsAllFieldsSaved(b *testing.B, n int, fields []string) []*pipeline.Event {
	return getEvents(b, n, fields, share100)
}

func getEventsHalfFieldsSaved(b *testing.B, n int, fields []string) []*pipeline.Event {
	return getEvents(b, n, fields, share50)
}

func getEventsNoFieldsSaved(b *testing.B, n int, fields []string) []*pipeline.Event {
	return getEvents(b, n, fields, share0)
}

func getEvents(b *testing.B, n int, fields []string, keptFieldsShare share) []*pipeline.Event {
	result := make([]*pipeline.Event, n)
	for i := 0; i < n; i++ {
		root, err := getEvent(fields, keptFieldsShare)
		require.NoError(b, err)
		result[i] = &pipeline.Event{Root: root}
	}

	return result
}

func getEvent(fields []string, keptFieldsShare share) (*insaneJSON.Root, error) {
	root, err := insaneJSON.DecodeString("{}")
	if err != nil {
		return nil, err
	}

	var keptFieldCount int
	switch keptFieldsShare {
	case share0:
		keptFieldCount = 0
	case share50:
		keptFieldCount = len(fields) / 2
	case share100:
		keptFieldCount = len(fields)
	default:
		panic("unknown share")
	}

	for i, field := range fields {
		var key string
		if i < keptFieldCount {
			key = field
		} else {
			key = getRandKey(8)
		}

		value := getRandValue(10)
		root.AddField(key).MutateToString(value)
	}

	return root, nil
}

func getRandKey(length int) string {
	var b strings.Builder
	b.Grow(length)

	for i := 0; i < length; i++ {
		b.WriteByte(getRandByte('a', 'z'))
	}

	return b.String()
}

func getRandValue(length int) string {
	var b strings.Builder
	b.Grow(length)

	for i := 0; i < length; i++ {
		b.WriteByte(getRandByte(' ', '~'))
	}

	return b.String()
}

func getRandByte(from, to byte) byte {
	return from + byte(rand.Intn(int(to-from)))
}

func getEventsBySrc(b *testing.B, src string, n int) []*pipeline.Event {
	result := make([]*pipeline.Event, n)
	for i := 0; i < n; i++ {
		root, err := insaneJSON.DecodeString(src)
		require.NoError(b, err)
		result[i] = &pipeline.Event{Root: root}
	}

	return result
}
