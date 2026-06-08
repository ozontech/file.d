package cardinality

import (
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"
)

// entry holds a timestamp for a single cached key.
type entry struct {
	ts int64
}

type Cache struct {
	mu   *sync.RWMutex
	tree map[string]*map[string]*entry // prefix -> (full key -> timestamp)
	ttl  int64
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		tree: make(map[string]*map[string]*entry),
		ttl:  ttl.Nanoseconds(),
		mu:   &sync.RWMutex{},
	}
}

// Set stores a full key under the given prefix bucket.
// It returns true if the key already existed, false otherwise.
func (c *Cache) Set(prefix, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	bucket, ok := c.tree[prefix]
	if !ok {
		bucket = &map[string]*entry{}
		c.tree[prefix] = bucket
	}

	e, exists := (*bucket)[key]
	if exists {
		e.ts = xtime.GetInaccurateUnixNano()
		return true
	}

	(*bucket)[key] = &entry{ts: xtime.GetInaccurateUnixNano()}
	return false
}

func (c *Cache) isExpire(now, value int64) bool {
	return now-value > c.ttl
}

// CountPrefix returns the number of non-expired keys under the prefix.
// Expired keys are scheduled for async deletion.
func (c *Cache) CountPrefix(prefix string) (count int) {
	var keysToDelete []string
	now := xtime.GetInaccurateUnixNano()

	c.mu.RLock()
	bucket := c.tree[prefix]
	if bucket != nil {
		for key, e := range *bucket {
			if c.isExpire(now, e.ts) {
				keysToDelete = append(keysToDelete, key)
			} else {
				count++
			}
		}
	}
	c.mu.RUnlock()

	if len(keysToDelete) > 0 {
		go c.delete(prefix, keysToDelete...)
	}
	return
}

func (c *Cache) delete(prefix string, keysToDelete ...string) {
	if len(keysToDelete) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	bucket := c.tree[prefix]
	if bucket == nil {
		return
	}

	for _, key := range keysToDelete {
		delete(*bucket, key)
	}

	if len(*bucket) == 0 {
		delete(c.tree, prefix)
	}
}
