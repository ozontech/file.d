package discard

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCache(t *testing.T) {
	cache, err := NewCache(time.Minute)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
}

func TestSetAndExists(t *testing.T) {
	cache, _ := NewCache(time.Minute)

	t.Run("basic set and get", func(t *testing.T) {
		key := "test-key"
		assert.True(t, cache.Set(key))

		found := cache.IsExists(key)
		assert.True(t, found)
	})

	t.Run("non-existent key", func(t *testing.T) {
		found := cache.IsExists("non-existent")
		assert.False(t, found)
	})
}

func TestDelete(t *testing.T) {
	cache, _ := NewCache(time.Minute)

	key := "to-delete"
	cache.Set(key)

	t.Run("delete existing key", func(t *testing.T) {
		cache.delete(key)
		found := cache.IsExists(key)
		assert.False(t, found)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		// Should not panic
		cache.delete("never-existed")
	})
}

func TestCountPrefix(t *testing.T) {
	cache, _ := NewCache(time.Minute)

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
	cache, _ := NewCache(time.Minute)

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
		found := cache.IsExists(key)
		assert.True(t, found)
	}

	// Test concurrent gets and sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.IsExists(k)
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
	cache, _ := NewCache(100 * time.Millisecond)

	key := "ttl-key"
	cache.Set(key)

	t.Run("key exists before TTL", func(t *testing.T) {
		found := cache.IsExists(key)
		assert.True(t, found)
	})

	t.Run("key expires after TTL", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		found := cache.IsExists(key)
		assert.False(t, found)
	})
}
