package cardinality

import (
	"fmt"
	"math/rand"
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
		prefix, key := "prefix", "prefix-test-key"
		assert.False(t, cache.Set(prefix, key))

		found := cacheKeyIsExists(cache, prefix, key)
		assert.True(t, found)

		assert.True(t, cache.Set(prefix, key))
	})

	t.Run("non-existent key", func(t *testing.T) {
		found := cacheKeyIsExists(cache, "prefix", "non-existent")
		assert.False(t, found)
	})
}

func TestDelete(t *testing.T) {
	cache := NewCache(time.Minute)

	t.Run("delete existing key", func(t *testing.T) {
		prefix, key := "prefix", "prefix-to-delete-1"
		cache.Set(prefix, key)

		cache.delete(prefix, key)

		found := cacheKeyIsExists(cache, prefix, key)
		assert.False(t, found, "Key should be deleted")
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		prefix := "prefix"
		// Should not panic or cause issues
		assert.NotPanics(t, func() {
			cache.delete(prefix, "never-existed-1")
		})

		// Verify cache is still functional
		key := "prefix-test-after-non-existent"
		cache.Set(prefix, key)
		found := cacheKeyIsExists(cache, prefix, key)
		assert.True(t, found, "Cache should still work after deleting non-existent key")
	})

	t.Run("delete many existing key", func(t *testing.T) {
		prefix := "prefix"
		key1 := "prefix-to-delete-1"
		cache.Set(prefix, key1)

		key2 := "prefix-to-delete-2"
		cache.Set(prefix, key2)

		cache.delete(prefix, key1, key2)

		found := cacheKeyIsExists(cache, prefix, key1)
		assert.False(t, found, "Key should be deleted")

		found = cacheKeyIsExists(cache, prefix, key2)
		assert.False(t, found, "Key should be deleted")
	})
}

func TestCountPrefix(t *testing.T) {
	cache := NewCache(time.Minute)

	prefix1, prefix2 := "key1", "key2"

	cache.Set(prefix1, prefix1+"_subkey1")
	cache.Set(prefix1, prefix1+"_subkey1")
	cache.Set(prefix1, prefix1+"_subkey2")
	cache.Set(prefix2, prefix2+"_subkey1")

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
		cache.delete(prefix1, prefix1+"_subkey1")
		assert.Equal(t, 1, cache.CountPrefix(prefix1))
	})
}

func TestConcurrentOperations(t *testing.T) {
	cache := NewCache(time.Minute)

	var wg sync.WaitGroup
	prefix := "prefix"
	keys := []string{"prefix-key1", "prefix-key2", "prefix-key3"}

	// Test concurrent sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.Set(prefix, k)
			}
		}(key)
	}
	wg.Wait()

	// Verify all keys were set
	for _, key := range keys {
		found := cacheKeyIsExists(cache, prefix, key)
		assert.True(t, found)
	}

	// Test concurrent gets and sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
<<<<<<< HEAD
			for range 100 {
				cacheKeyIsExists(cache, k)
				cache.Set(k + "-new")
=======
			for i := 0; i < 100; i++ {
				cacheKeyIsExists(cache, prefix, k)
				cache.Set(prefix, k+"-new")
>>>>>>> master
			}
		}(key)
	}
	wg.Wait()

	// Test concurrent deletes
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
<<<<<<< HEAD
			for range 100 {
				cache.delete(k)
=======
			for i := 0; i < 100; i++ {
				cache.delete(prefix, k)
>>>>>>> master
			}
		}(key)
	}
	wg.Wait()

	// Verify prefix counts under concurrent access
	wg.Add(2)
	go func() {
		defer wg.Done()
<<<<<<< HEAD
		for range 100 {
			cache.CountPrefix("key")
=======
		for i := 0; i < 100; i++ {
			cache.CountPrefix(prefix)
>>>>>>> master
		}
	}()
	go func() {
		defer wg.Done()
<<<<<<< HEAD
		for range 100 {
			cache.Set("key-x")
			cache.Set("key-y")
			cache.delete("key-x")
=======
		for i := 0; i < 100; i++ {
			cache.Set(prefix, "prefix-key-x")
			cache.Set(prefix, "prefix-key-y")
			cache.delete(prefix, "prefix-key-x")
>>>>>>> master
		}
	}()
	wg.Wait()
}

func TestCountPrefixWith10kElements(t *testing.T) {
	cache := NewCache(time.Minute)
	n := 10000
	prefix := randString(64)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s-%s", prefix, randString(48))
		cache.Set(prefix, key)
		cache.Set(prefix, key)
		assert.Equal(t, i+1, cache.CountPrefix(prefix))
	}
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestTTL(t *testing.T) {
	cache := NewCache(100 * time.Millisecond)

	prefix, key := "ttl-key", "ttl-key-sub"
	cache.Set(prefix, key)

	t.Run("key exists before TTL", func(t *testing.T) {
		assert.Equal(t, 1, cache.CountPrefix(prefix))
		found := cacheKeyIsExists(cache, prefix, key)
		assert.True(t, found)
	})

	t.Run("key expires after TTL", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		assert.Equal(t, 0, cache.CountPrefix(prefix))
		// CountPrefix now cleans expired keys synchronously, so no need to wait.
		found := cacheKeyIsExists(cache, prefix, key)
		assert.False(t, found)
	})
}

func cacheKeyIsExists(c *Cache, prefix, key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := c.tree[prefix]
	if b == nil {
		return false
	}
	_, found := b.keys[key]

	return found
}
