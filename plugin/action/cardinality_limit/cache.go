package discard

import (
	"sync"
	"time"

	radix "github.com/armon/go-radix"
)

type Cache struct {
	mu   *sync.RWMutex
	tree *radix.Tree
	ttl  time.Duration
}

func NewCache(ttl time.Duration) (*Cache, error) {
	return &Cache{
		tree: radix.New(),
		ttl:  ttl,
		mu:   &sync.RWMutex{},
	}, nil
}

func (c *Cache) Set(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tree.Insert(key, time.Now())
	return true
}

func (c *Cache) IsExists(key string) bool {
	c.mu.RLock()
	timeValue, found := c.tree.Get(key)
	c.mu.RUnlock()

	if found {
		now := time.Now()
		isExpire := c.isExpire(now, timeValue.(time.Time))
		if isExpire {
			c.delete(key)
			return false
		}
	}
	return found
}

func (c *Cache) isExpire(now, value time.Time) bool {
	diff := now.Sub(value)
	return diff > c.ttl
}

func (c *Cache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tree.Delete(key)
}

func (c *Cache) CountPrefix(prefix string) (count int) {
	var keysToDelete []string
	c.mu.RLock()
	now := time.Now()
	c.tree.WalkPrefix(prefix, func(s string, v any) bool {
		timeValue := v.(time.Time)
		if c.isExpire(now, timeValue) {
			keysToDelete = append(keysToDelete, s)
		} else {
			count++
		}
		return false
	})
	c.mu.RUnlock()

	if len(keysToDelete) > 0 {
		for _, key := range keysToDelete {
			c.delete(string(key))
		}
	}
	return
}
