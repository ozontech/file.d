package discard

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCache(t *testing.T) {
	t.Run("successful creation", func(t *testing.T) {
		cache, err := NewCache(100, time.Minute)
		assert.NoError(t, err)
		assert.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("invalid config", func(t *testing.T) {
		_, err := NewCache(0, time.Minute)
		assert.Error(t, err)
	})
}

func TestSetAndGet(t *testing.T) {
	cache, _ := NewCache(100, time.Minute)
	defer cache.Close()

	t.Run("basic set and get", func(t *testing.T) {
		key := "test-key"
		assert.True(t, cache.Set(key))

		time.Sleep(1 * time.Millisecond)

		val, found := cache.Get(key)
		assert.True(t, found)
		assert.Equal(t, "1", val)
	})

	t.Run("non-existent key", func(t *testing.T) {
		_, found := cache.Get("non-existent")
		assert.False(t, found)
	})
}

func TestDelete(t *testing.T) {
	cache, _ := NewCache(100, time.Minute)
	defer cache.Close()

	key := "to-delete"
	cache.Set(key)

	t.Run("delete existing key", func(t *testing.T) {
		cache.Delete(key)
		_, found := cache.Get(key)
		assert.False(t, found)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		// Should not panic
		cache.Delete("never-existed")
	})
}

func TestCountPrefix(t *testing.T) {
	cache, _ := NewCache(100, time.Minute)
	defer cache.Close()

	keys := []string{
		"key1_subkey1",
		"key1_subkey1",
		"key1_subkey2",

		"key2_subkey1",
	}

	time.Sleep(10 * time.Millisecond)

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
		cache.Delete("key1_subkey1")
		assert.Equal(t, 1, cache.CountPrefix("key1"))
	})
}

func TestClear(t *testing.T) {
	cache, _ := NewCache(100, time.Minute)
	defer cache.Close()

	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		cache.Set(key)
	}

	cache.Clear()

	t.Run("cache should be empty", func(t *testing.T) {
		for _, key := range keys {
			_, found := cache.Get(key)
			assert.False(t, found)
		}
	})

	t.Run("prefix counts should be zero", func(t *testing.T) {
		assert.Equal(t, 0, cache.CountPrefix(""))
		assert.Equal(t, 0, cache.CountPrefix("a"))
	})
}

func TestConcurrentOperations(t *testing.T) {
	cache, _ := NewCache(1000, time.Minute)
	defer cache.Close()

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
		_, found := cache.Get(key)
		assert.True(t, found)
	}

	// Test concurrent gets and sets
	wg.Add(len(keys))
	for _, key := range keys {
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cache.Get(k)
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
				cache.Delete(k)
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
			cache.Delete("key-x")
		}
	}()
	wg.Wait()
}

func TestTTL(t *testing.T) {
	cache, _ := NewCache(100, 100*time.Millisecond)
	defer cache.Close()

	key := "ttl-key"
	cache.Set(key)

	time.Sleep(1 * time.Millisecond)

	t.Run("key exists before TTL", func(t *testing.T) {
		_, found := cache.Get(key)
		assert.True(t, found)
	})

	t.Run("key expires after TTL", func(t *testing.T) {
		time.Sleep(200 * time.Millisecond)
		_, found := cache.Get(key)
		assert.False(t, found)
	})
}
