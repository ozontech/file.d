package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type batcherTail struct {
	commit func(event *Event)
}

func (b *batcherTail) Commit(event *Event) {
	b.commit(event)
}

func TestBatcher(t *testing.T) {
	eventCount := 10
	batchSize := 5

	wg := sync.WaitGroup{}
	wg.Add(eventCount)

	batchCount := &atomic.Int32{}
	batcherOut := func(workerData *WorkerData, batch *Batch) {
		if *workerData == nil {
			*workerData = batchCount
		}
		counter := (*workerData).(*atomic.Int32)
		counter.Inc()
	}

	commitsCount := atomic.Int32{}
	batcherTail := &batcherTail{commit: func(event *Event) {
		val := commitsCount.Inc() - 1
		assert.Equal(t, val, int32(event.SeqID), "wrong event SeqID")
		wg.Done()
	}}

	batcher := NewBatcher("test", "devnull", batcherOut, batcherTail, 2, batchSize, time.Second)

	batcher.Start()
	for i := 0; i < eventCount; i++ {
		batcher.Add(&Event{SeqID: uint64(i)})
	}

	wg.Wait()
	batcher.Stop()

	assert.Equal(t, int32(eventCount), commitsCount.Load(), "wrong commits count")
	assert.Equal(t, int32(eventCount/batchSize), batchCount.Load(), "wrong batches count")
}
