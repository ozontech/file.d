package discard

import (
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

type Cache struct {
	rc         *ristretto.Cache[string, string] // Ristretto v2 cache instance
	prefixTrie *prefixTrie                      // For efficient prefix counting
	mu         sync.RWMutex                     // Protects trie operations
	ttl        time.Duration
}

type prefixTrie struct {
	children map[rune]*prefixTrie
	count    int
}

// NewCache creates a new cache instance with Ristretto v2
func NewCache(maxItems int64, ttl time.Duration) (*Cache, error) {
	rc, err := ristretto.NewCache(&ristretto.Config[string, string]{
		NumCounters: maxItems * 10, // Number of keys to track frequency
		MaxCost:     maxItems,      // Maximum number of items in cache
		BufferItems: 64,            // Size of Get buffer
	})
	if err != nil {
		return nil, err
	}

	return &Cache{
		rc:         rc,
		prefixTrie: &prefixTrie{children: make(map[rune]*prefixTrie)},
		ttl:        ttl,
	}, nil
}

// Set adds an item to cache with TTL and updates prefix counts
func (c *Cache) Set(key string) bool {
	// Set item in Ristretto with TTL
	success := c.rc.SetWithTTL(key, "1", 1, c.ttl)
	if !success {
		return false
	}

	// Update prefix trie
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateTrie(key, 1)
	return true
}

// Get retrieves an item from cache
func (c *Cache) Get(key string) (string, bool) {
	return c.rc.Get(key)
}

// Delete removes an item from cache and updates prefix counts
func (c *Cache) Delete(key string) {
	c.rc.Del(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateTrie(key, -1)
}

// CountPrefix returns number of keys matching the prefix
func (c *Cache) CountPrefix(prefix string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node := c.prefixTrie
	for _, char := range prefix {
		if child, ok := node.children[char]; ok {
			node = child
		} else {
			return 0
		}
	}
	return node.count
}

// Clear completely resets the cache
func (c *Cache) Clear() {
	c.rc.Clear()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prefixTrie = &prefixTrie{children: make(map[rune]*prefixTrie)}
}

// Close shuts down the cache
func (c *Cache) Close() {
	c.rc.Close()
}

// updateTrie maintains the prefix count trie
func (c *Cache) updateTrie(key string, delta int) {
	node := c.prefixTrie
	for _, char := range key {
		if _, ok := node.children[char]; !ok {
			node.children[char] = &prefixTrie{children: make(map[rune]*prefixTrie)}
		}
		node = node.children[char]
		node.count += delta
	}
}
