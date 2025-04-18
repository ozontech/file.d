package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSliceMapRace(t *testing.T) {
	const testDuration = 2 * time.Second

	getRandStream := func() StreamName {
		return StreamName(fmt.Sprintf("stream%d", rand.Int()%3))
	}

	sm := SliceFromMap(map[StreamName]int64{
		"stream0": 100,
		"stream1": 200,
	})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(testDuration))
	defer cancel()

	runFn := func(f func()) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				f()
			}
		}
	}

	go runFn(func() {
		sm.Get(getRandStream())
	})

	go runFn(func() {
		sm.Set(getRandStream(), rand.Int63()%100)
	})

	<-ctx.Done()
}

func TestSliceMapAll(t *testing.T) {
	m := map[StreamName]int64{
		"stream1": 100,
		"stream2": 200,
	}

	sm := SliceFromMap(m)

	iters := 0
	for _, v := range sm.All() {
		iters++
		require.Equal(t, m[v.Stream], v.Offset)
	}
	require.Equal(t, len(m), iters)
}
