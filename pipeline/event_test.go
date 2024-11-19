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
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func() {
				e := p.get()
				runtime.Gosched()
				p.back(e)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func TestSlowPath(t *testing.T) {
	t.Parallel()
	const (
		poolCapacity = 256
		concurrency  = 5_000
	)
	pool := newEventPool(poolCapacity, DefaultAvgInputEventSize)
	for i := 0; i < 1_000; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				e := pool.get()
				runtime.Gosched()
				pool.back(e)
			}()
		}
		wg.Wait()
	}
}
