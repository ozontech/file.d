package pipeline

import (
	"iter"
	"slices"
	"sync"
)

// SliceMap is a map of streamName to offset.
// It could be just map[k]v, but Go > 1.17 internal map implementation can't
// work with mutable strings that occurs when using unsafe cast from []byte.
// Also it should be not slower on 1-2 keys like linked list, which is often the case for streams per job.
type SliceMap struct {
	s  []kv
	mx *sync.RWMutex
}

type kv struct {
	Stream StreamName
	Offset int64
}

func SliceFromMap(m map[StreamName]int64) *SliceMap {
	so := make([]kv, 0, len(m))
	for k, v := range m {
		so = append(so, kv{k, v})
	}
	return &SliceMap{
		s:  so,
		mx: &sync.RWMutex{},
	}
}

func (so *SliceMap) Len() int {
	return len(so.s)
}

func (so *SliceMap) All() iter.Seq2[int, kv] {
	return slices.All(so.s)
}

func (so *SliceMap) Get(streamName StreamName) (int64, bool) {
	so.mx.RLock()
	defer so.mx.RUnlock()

	if idx := so.find(streamName); idx != -1 {
		return so.s[idx].Offset, true
	}
	return 0, false
}

func (so *SliceMap) Set(streamName StreamName, offset int64) {
	so.mx.Lock()
	defer so.mx.Unlock()

	if idx := so.find(streamName); idx != -1 {
		so.s[idx].Offset = offset
	} else {
		so.s = append(so.s, kv{streamName, offset})
	}
}

func (so *SliceMap) Reset() {
	so.mx.Lock()
	defer so.mx.Unlock()

	for i := range so.s {
		so.s[i].Offset = 0
	}
}

func (so *SliceMap) find(streamName StreamName) int {
	return slices.IndexFunc(so.s, func(kv kv) bool { return kv.Stream == streamName })
}
