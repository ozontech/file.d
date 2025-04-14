package pipeline

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceMapRace(t *testing.T) {
	const iters = 100

	getRandStream := func() StreamName {
		return StreamName(fmt.Sprintf("stream%d", rand.Int()%3))
	}

	sm := SliceFromMap(map[StreamName]int64{
		"stream0": 100,
		"stream1": 200,
	})

	wgGet := &sync.WaitGroup{}
	wgGet.Add(iters)
	wgSet := &sync.WaitGroup{}
	wgSet.Add(iters)

	go func() {
		sm.Get(getRandStream())
		wgGet.Done()
	}()

	go func() {
		sm.Set(getRandStream(), rand.Int63()%100)
		wgSet.Done()
	}()

	wgSet.Done()
	wgGet.Done()
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
