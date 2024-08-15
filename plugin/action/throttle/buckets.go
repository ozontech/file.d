package throttle

import (
	"time"
)

type buckets interface {
	get(index, distrIndex int) int64
	getAll(index int, buf []int64) []int64
	set(index, distrIndex int, value int64)
	reset(index int)
	add(index, distrIndex int, value int64)
	isEmpty(index int) bool
	// rebuild will rebuild buckets and returns actual bucket id
	rebuild(currentTs, ts time.Time) int

	// actualizeIndex checks probable shift of buckets and returns actual bucket index and actuality
	actualizeIndex(maxID, index int) (int, bool)
	getCount() int
	getInterval() time.Duration
	getMinID() int

	// for testing only
	isSimple() bool
}

func newBuckets(count, distributionSize int, interval time.Duration) buckets {
	if distributionSize == 1 {
		return newSimpleBuckets(count, interval)
	}
	return newDistributedBuckets(count, distributionSize, interval)
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
	}

	// buckets were rebuild during some operations, calc actual index
	shift := m.maxID - maxID
	actualIndex := index - shift
	return actualIndex, actualIndex > 0
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
	resetFn := func(count int) {
		b.b = append(b.b[count:], b.b[:count]...)
		for i := 0; i < count; i++ {
			b.reset(b.getCount() - 1 - i)
		}
	}

	return rebuildBuckets(&b.bucketsMeta, resetFn, currentTs, ts)
}

func (b *simpleBuckets) isSimple() bool {
	return true
}

type distributedBuckets struct {
	bucketsMeta
	b []distributedBucket
}

func newDistributedBuckets(count, distributionSize int, interval time.Duration) *distributedBuckets {
	db := &distributedBuckets{
		bucketsMeta: newBucketsMeta(count, interval),
		b:           make([]distributedBucket, count),
	}
	for i := 0; i < count; i++ {
		db.b[i] = newDistributedBucket(distributionSize)
	}
	return db
}

func (b *distributedBuckets) get(index, distrIndex int) int64 {
	return b.b[index][distrIndex]
}

func (b *distributedBuckets) getAll(index int, buf []int64) []int64 {
	return b.b[index].copyTo(buf)
}

func (b *distributedBuckets) set(index, distrIndex int, value int64) {
	b.b[index][distrIndex] = value
}

func (b *distributedBuckets) reset(index int) {
	for i := range b.b[index] {
		b.b[index][i] = 0
	}
}

func (b *distributedBuckets) add(index, distrIndex int, value int64) {
	b.b[index][distrIndex] += value
}

func (b *distributedBuckets) isEmpty(index int) bool {
	for _, v := range b.b[index] {
		if v > 0 {
			return false
		}
	}
	return true
}

func (b *distributedBuckets) rebuild(currentTs, ts time.Time) int {
	resetFn := func(count int) {
		b.b = append(b.b[count:], b.b[:count]...)
		for i := 0; i < count; i++ {
			b.reset(b.getCount() - 1 - i)
		}
	}

	return rebuildBuckets(&b.bucketsMeta, resetFn, currentTs, ts)
}

func (b *distributedBuckets) isSimple() bool {
	return false
}

type distributedBucket []int64

func newDistributedBucket(size int) distributedBucket {
	return make(distributedBucket, size)
}

func (s distributedBucket) copyTo(buf distributedBucket) distributedBucket {
	return append(buf, s...)
}

func rebuildBuckets(meta *bucketsMeta, resetFn func(int), currentTs, ts time.Time) int {
	currentID := meta.timeToBucketID(currentTs)
	if meta.minID == 0 {
		// min id weren't set yet. It MUST be extracted from currentTs, because ts from event can be invalid (e.g. from 1970 or 2077 year)
		meta.maxID = currentID
		meta.minID = meta.maxID - meta.count + 1
	}
	maxID := meta.minID + meta.count - 1

	// currentID exceed maxID, actualize buckets
	if currentID > maxID {
		dif := currentID - maxID
		n := min(dif, meta.count)
		// reset old buckets
		resetFn(n)
		// update ids
		meta.minID += dif
		meta.maxID = currentID
	}

	id := meta.timeToBucketID(ts)
	// events from past or future goes to the latest bucket
	if id < meta.minID || id > meta.maxID {
		id = meta.maxID
	}
	return id
}
