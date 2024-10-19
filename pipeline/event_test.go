package pipeline

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventPoolDump(t *testing.T) {
	eventPool := newEventPool(2, DefaultAvgInputEventSize)
	e := eventPool.get()
	defer eventPool.back(e)

	require.NotPanics(t, func() {
		_ = eventPool.dump()
	})
}

func BenchmarkEventPoolOneGoroutine(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity, DefaultAvgInputEventSize)

	for i := 0; i < b.N; i++ {
		p.back(p.get())
	}
}

func BenchmarkEventPoolManyGoroutines(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity, DefaultAvgInputEventSize)

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for j := 0; j < runtime.GOMAXPROCS(0); j++ {
			wg.Add(1)
			go func() {
				for k := 0; k < 1000; k++ {
					p.back(p.get())
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkEventPoolSlowestPath(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity, DefaultAvgInputEventSize)

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for j := 0; j < 100_000; j++ {
			wg.Add(1)
			go func() {
				e := p.get()
				p.back(e)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkEventSyncPoolSlowestPath(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity, DefaultAvgInputEventSize)

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for j := 0; j < 100_000; j++ {
			wg.Add(1)
			go func() {
				e := p.get()
				p.back(e)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
