package s3

type ObjectStoreClientLimiter struct {
	limit   int
	created int
}

func NewObjectStoreClientLimiter(limit int) *ObjectStoreClientLimiter {
	return &ObjectStoreClientLimiter{limit: limit}
}

func (limiter *ObjectStoreClientLimiter) CanCreate() bool {
	return limiter.created < limiter.limit
}

func (limiter *ObjectStoreClientLimiter) Increment() {
	limiter.created++
}
