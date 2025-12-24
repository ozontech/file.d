package cardinality

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(time.Minute)
	assert.NotNil(t, cache)
}

func TestSetAndExists(t *testing.T) {
	cache := NewCache(time.Minute)

	t.Run("basic set and get", func(t *testing.T) {
		key := "test-key"
		assert.False(t, cache.Set(key))

		found := cacheKeyIsExists(cache, key)
		assert.True(t, found)

		assert.True(t, cache.Set(key))
	})

	t.Run("non-existent key", func(t *testing.T) {
		found := cacheKeyIsExists(cache, "non-existent")
		assert.False(t, found)
	})
}

func TestDelete(t *testing.T) {
	cache := NewCache(time.Minute)

	t.Run("delete existing key", func(t *testing.T) {
		key := "to-delete-1"
		cache.Set(key)

		cache.delete(key)

		found := cacheKeyIsExists(cache, key)
		assert.False(t, found, "Key should be deleted")
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		// Should not panic or cause issues
		assert.NotPanics(t, func() {
			cache.delete("never-existed-1")
		})

		// Verify cache is still functional
		key := "test-after-non-existent"
		cache.Set(key)
		found := cacheKeyIsExists(cache, key)
		assert.True(t, found, "Cache should still work after deleting non-existent key")
	})

	t.Run("delete many existing key", func(t *testing.T) {
		key1 := "to-delete-1"
		cache.Set(key1)

		key2 := "to-delete-2"
		cache.Set(key2)

		cache.delete(key1, key2)

		found := cacheKeyIsExists(cache, key1)
		assert.False(t, found, "Key should be deleted")

		found = cacheKeyIsExists(cache, key2)
		assert.False(t, found, "Key should be deleted")
	})
}

func TestCountPrefix(t *testing.T) {
	cache := NewCache(time.Minute)

	keys := []string{
		"key1_subkey1",
		"key1_subkey1",
		"key1_subkey2",

		"key2_subkey1",
	}

	for _, key := range keys {
		cache.Set(key)
	}

	testCases := []struct {
		prefix string
		count  int
	}{
		{"key1", 2},
		{"key2", 1},
		{"key3", 0},
	}

	for _, tc := range testCases {
		t.Run("prefix "+tc.prefix, func(t *testing.T) {
			assert.Equal(t, tc.count, cache.CountPrefix(tc.prefix))
		})
	}

	t.Run("count after delete", func(t *testing.T) {
		cache.delete("key1_subkey1")
		assert.Equal(t, 1, cache.CountPrefix("key1"))
	})
}

func TestConcurrentOperations(t *testing.T) {
	cache := NewCache(time.Minute)

	var wg sync.WaitGroup
	keys := []string{"key1", "key2", "key3"}

	// Test concurrent sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.Set(k)
			}
		}(key)
	}
	wg.Wait()

	// Verify all keys were set
	for _, key := range keys {
		found := cacheKeyIsExists(cache, key)
		assert.True(t, found)
	}

	// Test concurrent gets and sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cacheKeyIsExists(cache, k)
				cache.Set(k + "-new")
			}
		}(key)
	}
	wg.Wait()

	// Test concurrent deletes
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.delete(k)
			}
		}(key)
	}
	wg.Wait()

	// Verify prefix counts under concurrent access
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			cache.CountPrefix("key")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			cache.Set("key-x")
			cache.Set("key-y")
			cache.delete("key-x")
		}
	}()
	wg.Wait()
}

func TestTTL(t *testing.T) {
	cache := NewCache(100 * time.Millisecond)

	key := "ttl-key"
	cache.Set(key)

	t.Run("key exists before TTL", func(t *testing.T) {
		assert.Equal(t, 1, cache.CountPrefix(key))
		found := cacheKeyIsExists(cache, key)
		assert.True(t, found)
	})

	t.Run("key expires after TTL", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		assert.Equal(t, 0, cache.CountPrefix(key))
		time.Sleep(100 * time.Millisecond) // cause delete in async
		found := cacheKeyIsExists(cache, key)
		assert.False(t, found)
	})
}

func cacheKeyIsExists(c *Cache, key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, found := c.tree.Get(key)

	return found
}
