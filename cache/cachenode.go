package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"redis-cache/singleflight"
	"redis-cache/utils"
	"sync"
	"time"
)

const (
	notFoundPlaceholder = "*"
	// make the expiry unstable to avoid lots of cached items expire at the same time
	// make the unstable expiry to be [0.95, 1.05] * seconds
	expiryDeviation = 0.05
)

// indicates there is no such value associate with the key
var errPlaceholder = errors.New("placeholder")

type cacheNode struct {
	rds            *Redis
	expiry         time.Duration
	notFoundExpiry time.Duration
	barrier        singleflight.SharedCalls
	r              *rand.Rand
	lock           *sync.Mutex
	unstableExpiry Unstable
	stat           *CacheStat
	errNotFound    error
	Ctx            context.Context
}

func NewCacheNode(rds *Redis, barrier singleflight.SharedCalls, st *CacheStat,
	errNotFound error, opts ...Option) Cache {
	o := newOptions(opts...)

	return cacheNode{
		rds:            rds,
		expiry:         o.Expiry,
		notFoundExpiry: o.NotFoundExpiry,
		barrier:        barrier,
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		lock:           new(sync.Mutex),
		unstableExpiry: NewUnstable(expiryDeviation), //
		stat:           st,
		errNotFound:    errNotFound,
		Ctx:            context.Background(),
	}
}

func (c cacheNode) DelCache(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	if err := c.rds.Del(c.Ctx, keys...); err != nil {
		log.Printf("failed to clear cache with keys: %q, error: %v", utils.FormatKeys(keys), err)
	}

	return nil
}

func (c cacheNode) GetCache(key string, v interface{}) error {
	if err := c.doGetCache(key, v); err == errPlaceholder {
		return c.errNotFound
	} else {
		return err
	}
}

func (c cacheNode) SetCache(key string, v interface{}) error {
	return c.SetCacheWithExpire(key, v, c.aroundDuration(c.expiry))
}

func (c cacheNode) SetCacheWithExpire(key string, v interface{}, expire time.Duration) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return c.rds.Set(c.Ctx, key, string(data), expire)
}

func (c cacheNode) String() string {
	return c.rds.Addr
}

func (c cacheNode) Take(v interface{}, key string, query func(v interface{}) error) error {
	return c.doTake(v, key, query, func(v interface{}) error {
		return c.SetCache(key, v)
	})
}

func (c cacheNode) TakeWithExpire(v interface{}, key string,
	query func(v interface{}, expire time.Duration) error) error {
	expire := c.aroundDuration(c.expiry)
	return c.doTake(v, key, func(v interface{}) error {
		return query(v, expire)
	}, func(v interface{}) error {
		return c.SetCacheWithExpire(key, v, expire)
	})
}

// 添加离散值，防止大量 key 同时过期，导致的缓存雪崩
func (c cacheNode) aroundDuration(duration time.Duration) time.Duration {
	return c.unstableExpiry.AroundDuration(duration)
}

func (c cacheNode) doGetCache(key string, v interface{}) error {
	c.stat.IncrementTotal()
	data, err := c.rds.Get(c.Ctx, key)
	if err != nil {
		c.stat.IncrementMiss()
		return err
	}

	if len(data) == 0 {
		c.stat.IncrementMiss()
		return c.errNotFound
	}

	c.stat.IncrementHit()
	if data == notFoundPlaceholder {
		return errPlaceholder
	}

	return c.processCache(key, data, v)
}

func (c cacheNode) doTake(v interface{}, key string, query func(v interface{}) error,
	cacheVal func(v interface{}) error) error {
	val, fresh, err := c.barrier.DoEx(key, func() (interface{}, error) {
		if err := c.doGetCache(key, v); err != nil {
			// Placeholder 是为了防止缓存穿透，直接返回缓存未找到
			if err == errPlaceholder {
				return nil, c.errNotFound
			} else if err != c.errNotFound {
				// 如果是未知错误，那么就直接返回，因为我们不能放弃缓存出错而直接把所有请求去请求DB，
				// 这样在高并发的场景下会把DB打挂掉的
				return nil, err
			}
			// 从 db 里获取数据
			if err = query(v); err == c.errNotFound {
				// 设置 Placeholder 防止缓存穿透
				if err = c.setCacheWithNotFound(key); err != nil {
					log.Println(err)
				}

				return nil, c.errNotFound
			} else if err != nil {
				c.stat.IncrementDbFails() // 记录 db 获取数据失败
				return nil, err
			}

			// 缓存数据
			if err = cacheVal(v); err != nil {
				log.Println(err)
			}
		}

		return json.Marshal(v)
	})
	if err != nil {
		return err
	}
	if fresh {
		return nil
	}

	// 缓存命中
	c.stat.IncrementTotal()
	c.stat.IncrementHit()

	return json.Unmarshal(val.([]byte), v)
}

func (c cacheNode) processCache(key string, data string, v interface{}) error {
	err := json.Unmarshal([]byte(data), v)
	if err == nil {
		return nil
	}

	report := fmt.Sprintf("unmarshal cache, node: %s, key: %s, value: %s, error: %v",
		c.rds.Addr, key, data, err)
	log.Println(report)
	// 上报错误 cache
	// stat.Report(report)
	if e := c.rds.Del(c.Ctx, key); e != nil {
		log.Printf("delete invalid cache, node: %s, key: %s, value: %s, error: %v",
			c.rds.Addr, key, data, e)
	}

	// returns errNotFound to reload the value by the given queryFn
	return c.errNotFound
}

// 没有的 key 缓存 Placeholder 防止缓存击穿
func (c cacheNode) setCacheWithNotFound(key string) error {
	return c.rds.Set(c.Ctx, key, notFoundPlaceholder, c.aroundDuration(c.notFoundExpiry))
}
