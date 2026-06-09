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

// CountPrefix returns the number of non-expired keys under the prefix.
// Expired keys are cleaned synchronously on the first call that detects them.
func (c *Cache) CountPrefix(prefix string) int {
	threshold := xtime.GetInaccurateUnixNano() - c.ttl

	c.mu.RLock()
	b := c.tree[prefix]
	if b == nil {
		c.mu.RUnlock()
		return 0
	}

	// Oldest key hasn't expired → no keys expired. Return count in O(1).
	if b.minTs >= threshold {
		count := len(b.keys)
		c.mu.RUnlock()
		return count
	}

	// Some keys may have expired — upgrade to write lock and scan.
	c.mu.RUnlock()

	c.mu.Lock()
	b = c.tree[prefix]
	if b == nil {
		c.mu.Unlock()
		return 0
	}

	// Recompute threshold and re-check under write lock.
	threshold = xtime.GetInaccurateUnixNano() - c.ttl
	if b.minTs >= threshold {
		count := len(b.keys)
		c.mu.Unlock()
		return count
	}

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
