package pipeline

// SliceMap is a map of streamName to offset.
// It could be just map[k]v, but Go > 1.17 internal map implementation can't
// work with mutable strings that occurs when using unsafe cast from []byte.
// Also it should be not slower on 1-2 keys like linked list, which is often the case for streams per job.
type SliceMap []kv

type kv struct {
	Stream StreamName
	Offset int64
}

func SliceFromMap(m map[StreamName]int64) SliceMap {
	so := make(SliceMap, 0, len(m))
	for k, v := range m {
		so = append(so, kv{k, v})
	}
	return so
}

func (so *SliceMap) Get(streamName StreamName) (int64, bool) {
	for _, kv := range *so {
		if kv.Stream == streamName {
			return kv.Offset, true
		}
	}
	return 0, false
}

func (so *SliceMap) Set(streamName StreamName, offset int64) {
	for i := range *so {
		if (*so)[i].Stream == streamName {
			(*so)[i].Offset = offset
			return
		}
	}
	*so = append(*so, kv{streamName, offset})
}
