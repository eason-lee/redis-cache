package sqlcache

import (
	"database/sql"
	"redis-cache/cache"
	"redis-cache/singleflight"
	"time"
)

const cacheSafeGapBetweenIndexAndPrimary = time.Second * 5

var (
	// 多个连接使用一个 SharedCalls ，共获取的缓存结果
	exclusiveCalls = singleflight.NewSharedCalls()
	stats          = cache.NewCacheStat("sqlcache")
)

type (
	ExecFn         func(conn SqlConn) (sql.Result, error)
	IndexQueryFn   func(conn SqlConn, v interface{}) (interface{}, error)
	PrimaryQueryFn func(conn SqlConn, v, primary interface{}) error
	QueryFn        func(conn SqlConn, v interface{}) error

	CachedConn struct {
		db    SqlConn
		cache cache.Cache
	}
)

func NewNodeConn(db SqlConn, rds *cache.Redis, opts ...cache.Option) CachedConn {
	return CachedConn{
		db:    db,
		cache: cache.NewCacheNode(rds, exclusiveCalls, stats, sql.ErrNoRows, opts...),
	}
}

func NewConn(db SqlConn, c cache.CacheConf, opts ...cache.Option) CachedConn {
	return CachedConn{
		db:    db,
		cache: cache.NewCache(c, exclusiveCalls, stats, sql.ErrNoRows, opts...),
	}
}

func (cc CachedConn) DelCache(keys ...string) error {
	return cc.cache.DelCache(keys...)
}

func (cc CachedConn) GetCache(key string, v interface{}) error {
	return cc.cache.GetCache(key, v)
}

func (cc CachedConn) Exec(exec ExecFn, keys ...string) (sql.Result, error) {
	res, err := exec(cc.db)
	if err != nil {
		return nil, err
	}

	if err := cc.DelCache(keys...); err != nil {
		return nil, err
	}

	return res, nil
}

func (cc CachedConn) ExecNoCache(q string, args ...interface{}) (sql.Result, error) {
	return cc.db.Exec(q, args...)
}

func (cc CachedConn) QueryRow(v interface{}, key string, query QueryFn) error {
	return cc.cache.Take(v, key, func(v interface{}) error {
		return query(cc.db, v)
	})
}

func (cc CachedConn) QueryRowIndex(v interface{}, key string, keyer func(primary interface{}) string,
	indexQuery IndexQueryFn, primaryQuery PrimaryQueryFn) error {
	var primaryKey interface{}
	var found bool

	if err := cc.cache.TakeWithExpire(&primaryKey, key, func(val interface{}, expire time.Duration) (err error) {
		primaryKey, err = indexQuery(cc.db, v)
		if err != nil {
			return
		}

		found = true
		return cc.cache.SetCacheWithExpire(keyer(primaryKey), v, expire+cacheSafeGapBetweenIndexAndPrimary)
	}); err != nil {
		return err
	}

	if found {
		return nil
	}

	return cc.cache.Take(v, keyer(primaryKey), func(v interface{}) error {
		return primaryQuery(cc.db, v, primaryKey)
	})
}

func (cc CachedConn) QueryRowNoCache(v interface{}, q string, args ...interface{}) error {
	return cc.db.QueryRow(v, q, args...)
}

// QueryRowsNoCache doesn't use cache, because it might cause consistency problem.
func (cc CachedConn) QueryRowsNoCache(v interface{}, q string, args ...interface{}) error {
	return cc.db.QueryRows(v, q, args...)
}

func (cc CachedConn) SetCache(key string, v interface{}) error {
	return cc.cache.SetCache(key, v)
}

func (cc CachedConn) Transact(fn func(txExec) error) error {
	return cc.db.Transact(fn)
}
