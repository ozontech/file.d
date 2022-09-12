package file

import "github.com/ozontech/file.d/pipeline"

// sliceMap is a map of streamName to offset.
// It could be just map[k]v, but Go > 1.17 internal map implementation can't
// work with mutable strings that occurs when using unsafe cast from []byte.
// Also it should be not slower on 1-2 keys like linked list, which is often the case for streams per job.
type sliceMap []kv

type kv struct {
	stream pipeline.StreamName
	offset int64
}

func sliceFromMap(m map[pipeline.StreamName]int64) sliceMap {
	so := make(sliceMap, 0, len(m))
	for k, v := range m {
		so = append(so, kv{k, v})
	}
	return so
}

func (so *sliceMap) get(streamName pipeline.StreamName) (int64, bool) {
	for _, kv := range *so {
		if kv.stream == streamName {
			return kv.offset, true
		}
	}
	return 0, false
}

func (so *sliceMap) set(streamName pipeline.StreamName, offset int64) {
	for i := range *so {
		if (*so)[i].stream == streamName {
			(*so)[i].offset = offset
			return
		}
	}
	*so = append(*so, kv{streamName, offset})
}
