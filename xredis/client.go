package xredis

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client interface {
	redis.Cmdable
}

type ClientType int

const (
	ClientTypeBase ClientType = iota
	ClientTypeRing
	ClientTypeCluster
)

type Options struct {
	ID         string // used as name in ring-client addrs map
	ClientType ClientType

	Addrs    []string
	Password string

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// cluster options
	RouteByLatency bool
	RouteRandomly  bool
}

func (o *Options) toBaseOptions() *redis.Options {
	addr := ""
	if len(o.Addrs) > 0 {
		addr = o.Addrs[0]
	}
	return &redis.Options{
		Addr:     addr,
		Password: o.Password,

		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,

		MaxRetries:      o.MaxRetries,
		MinRetryBackoff: o.MinRetryBackoff,
		MaxRetryBackoff: o.MaxRetryBackoff,
	}
}

func (o *Options) toRingOptions() *redis.RingOptions {
	addrs := make(map[string]string)
	for i, e := range o.Addrs {
		addrs[fmt.Sprintf("%s_%d", o.ID, i)] = e
	}
	return &redis.RingOptions{
		Addrs:    addrs,
		Password: o.Password,

		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,

		MaxRetries:      o.MaxRetries,
		MinRetryBackoff: o.MinRetryBackoff,
		MaxRetryBackoff: o.MaxRetryBackoff,
	}
}

func (o *Options) toClusterOptions() *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:    o.Addrs,
		Password: o.Password,

		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,

		MaxRetries:      o.MaxRetries,
		MinRetryBackoff: o.MinRetryBackoff,
		MaxRetryBackoff: o.MaxRetryBackoff,

		RouteByLatency: o.RouteByLatency,
		RouteRandomly:  o.RouteRandomly,
	}
}

func NewClient(opt *Options) Client {
	var client Client
	switch opt.ClientType {
	case ClientTypeBase:
		client = redis.NewClient(opt.toBaseOptions())
	case ClientTypeRing:
		client = redis.NewRing(opt.toRingOptions())
	case ClientTypeCluster:
		client = redis.NewClusterClient(opt.toClusterOptions())
	}
	return client
}
