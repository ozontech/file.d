package cardinality

import (
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"
)

// bucket holds keys for a single prefix with fast counting.
type bucket struct {
	keys  map[string]int64
	minTs int64 // oldest key timestamp; 0 if empty
}

type Cache struct {
	mu   *sync.RWMutex
	tree map[string]*bucket
	ttl  int64
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		tree: make(map[string]*bucket),
		ttl:  ttl.Nanoseconds(),
		mu:   &sync.RWMutex{},
	}
}

// Set stores a full key under the given prefix bucket.
// It returns true if the key already existed, false otherwise.
func (c *Cache) Set(prefix, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, ok := c.tree[prefix]
	if !ok {
		b = &bucket{keys: make(map[string]int64)}
		c.tree[prefix] = b
	}

	ts := xtime.GetInaccurateUnixNano()
	if _, exists := b.keys[key]; exists {
		b.keys[key] = ts
		return true
	}

	b.keys[key] = ts
	if b.minTs == 0 || ts < b.minTs {
		b.minTs = ts
	}
	return false
}

// fastPath checks whether a bucket is nil or fully valid under the given lock.
// It returns (count, true) when the result is conclusive, or (0, false) when
// a slow-path scan is needed. On return false the lock is still held.
func (c *Cache) fastPath(prefix string, lock, unlock func()) (int, bool) {
	threshold := xtime.GetInaccurateUnixNano() - c.ttl

	lock()
	b := c.tree[prefix]
	if b == nil {
		unlock()
		return 0, true
	}

	// Oldest key hasn't expired → no keys expired. Return count in O(1).
	if b.minTs >= threshold {
		count := len(b.keys)
		unlock()
		return count, true
	}

	return 0, false // lock still held
}

// cleanBucket scans a bucket under write lock, deletes expired keys, and
// updates minTs. The lock must already be held.
func (c *Cache) cleanBucket(prefix string) (int, int64) {
	b := c.tree[prefix]
	if b == nil {
		return 0, 0
	}

	threshold := xtime.GetInaccurateUnixNano() - c.ttl
	count := 0
	newMinTs := int64(0)
	for key, ts := range b.keys {
		if ts >= threshold {
			count++
			if newMinTs == 0 || ts < newMinTs {
				newMinTs = ts
			}
		} else {
			delete(b.keys, key)
		}
	}
	b.minTs = newMinTs
	if len(b.keys) == 0 {
		delete(c.tree, prefix)
	}

	return count, newMinTs
}

// CountPrefix returns the number of non-expired keys under the prefix.
// Expired keys are cleaned synchronously on the first call that detects them.
func (c *Cache) CountPrefix(prefix string) int {
	if count, ok := c.fastPath(prefix, c.mu.RLock, c.mu.RUnlock); ok {
		return count
	}
	c.mu.RUnlock()

	if count, ok := c.fastPath(prefix, c.mu.Lock, c.mu.Unlock); ok {
		return count
	}

	count, _ := c.cleanBucket(prefix)
	c.mu.Unlock()
	return count
}

func (c *Cache) delete(prefix string, keysToDelete ...string) {
	if len(keysToDelete) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.tree[prefix]
	if b == nil {
		return
	}

	minTsDeleted := false
	for _, key := range keysToDelete {
		if ts, ok := b.keys[key]; ok {
			if ts == b.minTs {
				minTsDeleted = true
			}
			delete(b.keys, key)
		}
	}

	if len(b.keys) == 0 {
		delete(c.tree, prefix)
		b.minTs = 0
	} else if minTsDeleted {
		b.minTs = 0
		for _, ts := range b.keys {
			if b.minTs == 0 || ts < b.minTs {
				b.minTs = ts
			}
		}
	}
}
