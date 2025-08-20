package discard

import (
	"sync"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"
)

type Cache struct {
	mu   *sync.RWMutex
	tree art.Tree
	ttl  time.Duration
}

func NewCache(ttl time.Duration) (*Cache, error) {
	return &Cache{
		tree: art.New(),
		ttl:  ttl,
		mu:   &sync.RWMutex{},
	}, nil
}

func (c *Cache) Set(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tree.Insert(art.Key(key), time.Now())
	return true
}

func (c *Cache) IsExists(key string) bool {
	c.mu.RLock()
	timeValue, found := c.tree.Search(art.Key(key))
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

	c.tree.Delete(art.Key(key))
}

func (c *Cache) CountPrefix(prefix string) (count int) {
	var keysToDelete []art.Key
	c.mu.RLock()
	now := time.Now()
	c.tree.ForEachPrefix(art.Key(prefix), func(node art.Node) bool {
		timeValue := node.Value().(time.Time)
		if c.isExpire(now, timeValue) {
			keysToDelete = append(keysToDelete, node.Key())
			return false
		} else {
			count++
			return true
		}
	})
	c.mu.RUnlock()

	if len(keysToDelete) > 0 {
		for _, key := range keysToDelete {
			c.delete(string(key))
		}
	}
	return
}
