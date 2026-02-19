package cardinality

import (
	"sync"
	"time"

	radix "github.com/armon/go-radix"
	"github.com/ozontech/file.d/xtime"
)

type Cache struct {
	mu   *sync.RWMutex
	tree *radix.Tree
	ttl  int64
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		tree: radix.New(),
		ttl:  ttl.Nanoseconds(),
		mu:   &sync.RWMutex{},
	}
}

func (c *Cache) Set(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, result := c.tree.Insert(key, xtime.GetInaccurateUnixNano())
	return result
}

func (c *Cache) isExpire(now, value int64) bool {
	diff := now - value
	return diff > c.ttl
}

func (c *Cache) delete(keysToDelete ...string) {
	if len(keysToDelete) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keysToDelete {
		c.tree.Delete(key)
	}
}

func (c *Cache) CountPrefix(prefix string) (count int) {
	var keysToDelete []string
	now := xtime.GetInaccurateUnixNano()
	c.mu.RLock()
	c.tree.WalkPrefix(prefix, func(s string, v any) bool {
		timeValue := v.(int64)
		if c.isExpire(now, timeValue) {
			keysToDelete = append(keysToDelete, s)
		} else {
			count++
		}
		return false
	})
	c.mu.RUnlock()

	if len(keysToDelete) > 0 {
		go c.delete(keysToDelete...)
	}
	return
}
