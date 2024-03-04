package throttle

import (
	"time"
)

type buckets interface {
	get(index, shard int) int64
	getAll(index int, buf []int64) []int64
	set(index, shard int, value int64)
	reset(index int)
	add(index, shard int, value int64)
	isEmpty(index int) bool
	// rebuild will rebuild buckets and returns actual bucket id
	rebuild(currentTs, ts time.Time) int

	// actualizeIndex checks probable shift of buckets and returns actual bucket index and actuality
	actualizeIndex(maxID, index int) (int, bool)
	getCount() int
	getInterval() time.Duration
	getMinID() int
}

func newBuckets(count, shards int, interval time.Duration) buckets {
	if shards == 1 {
		return newSimpleBuckets(count, interval)
	}
	return newShardedBuckets(count, shards, interval)
}

type bucketsMeta struct {
	count    int
	interval time.Duration
	minID    int // min bucket id
	maxID    int // max bucket id
}

func newBucketsMeta(count int, interval time.Duration) bucketsMeta {
	return bucketsMeta{
		count:    count,
		interval: interval,
	}
}

func (m bucketsMeta) getCount() int {
	return m.count
}

func (m bucketsMeta) getInterval() time.Duration {
	return m.interval
}

func (m bucketsMeta) getMinID() int {
	return m.minID
}

func (m bucketsMeta) actualizeIndex(maxID, index int) (int, bool) {
	if m.maxID == maxID {
		return index, true
	} else {
		// buckets were rebuild during some operations, calc actual index
		shift := m.maxID - maxID
		actualIndex := index - shift
		return actualIndex, actualIndex > 0
	}
}

// timeToBucketID converts time to bucketID
func (m bucketsMeta) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / m.interval.Nanoseconds())
}

type simpleBuckets struct {
	bucketsMeta
	b []int64
}

func newSimpleBuckets(count int, interval time.Duration) *simpleBuckets {
	return &simpleBuckets{
		bucketsMeta: newBucketsMeta(count, interval),
		b:           make([]int64, count),
	}
}

func (b *simpleBuckets) get(index, _ int) int64 {
	return b.b[index]
}

func (b *simpleBuckets) getAll(index int, buf []int64) []int64 {
	return append(buf, b.b[index])
}

func (b *simpleBuckets) set(index, _ int, value int64) {
	b.b[index] = value
}

func (b *simpleBuckets) reset(index int) {
	b.b[index] = 0
}

func (b *simpleBuckets) add(index, _ int, value int64) {
	b.b[index] += value
}

func (b *simpleBuckets) isEmpty(index int) bool {
	return b.b[index] == 0
}

func (b *simpleBuckets) rebuild(currentTs, ts time.Time) int {
	var id int
	id, b.b = rebuildBuckets(b.b, &b.bucketsMeta, func() int64 { return 0 }, currentTs, ts)
	return id
}

type shardedBuckets struct {
	bucketsMeta
	b           []bucketShard
	shardsCount int
}

func newShardedBuckets(count, shards int, interval time.Duration) *shardedBuckets {
	sb := &shardedBuckets{
		bucketsMeta: newBucketsMeta(count, interval),
		b:           make([]bucketShard, count),
		shardsCount: shards,
	}
	for i := 0; i < count; i++ {
		sb.b[i] = newBucketShard(shards)
	}
	return sb
}

func (b *shardedBuckets) get(index, shard int) int64 {
	return b.b[index][shard]
}

func (b *shardedBuckets) getAll(index int, buf []int64) []int64 {
	return b.b[index].copyTo(buf)
}

func (b *shardedBuckets) add(index, shard int, value int64) {
	b.b[index][shard] += value
}

func (b *shardedBuckets) set(index, shard int, value int64) {
	b.b[index][shard] = value
}

func (b *shardedBuckets) reset(index int) {
	for i := range b.b[index] {
		b.b[index][i] = 0
	}
}

func (b *shardedBuckets) isEmpty(index int) bool {
	for _, v := range b.b[index] {
		if v > 0 {
			return false
		}
	}
	return true
}

func (b *shardedBuckets) rebuild(currentTs, ts time.Time) int {
	newBucketFn := func() bucketShard { return newBucketShard(b.shardsCount) }

	var id int
	id, b.b = rebuildBuckets(b.b, &b.bucketsMeta, newBucketFn, currentTs, ts)
	return id
}

type bucketShard []int64

func newBucketShard(size int) bucketShard {
	return make(bucketShard, size)
}

func (s bucketShard) copyTo(buf bucketShard) bucketShard {
	return append(buf, s...)
}

func rebuildBuckets[T any](b []T, meta *bucketsMeta, newBucketFn func() T, currentTs, ts time.Time) (int, []T) {
	currentID := meta.timeToBucketID(currentTs)
	if meta.minID == 0 {
		// min id weren't set yet. It MUST be extracted from currentTs, because ts from event can be invalid (e.g. from 1970 or 2077 year)
		meta.maxID = currentID
		meta.minID = meta.maxID - meta.count + 1
	}
	maxID := meta.minID + len(b) - 1

	// currentID exceed maxID. Create actual buckets
	if currentID > maxID {
		n := currentID - maxID
		// add new buckets
		for i := 0; i < n; i++ {
			b = append(b, newBucketFn())
		}
		// remove old buckets
		b = b[n:]
		// update min buckets
		meta.minID += n
		meta.maxID = currentID
	}

	id := meta.timeToBucketID(ts)
	// events from past or future goes to the latest bucket
	if id < meta.minID || id > meta.maxID {
		id = meta.maxID
	}
	return id, b
}
