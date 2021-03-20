package sqlcache

import (
	"io"
	"redis-cache/cache"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	maxIdleConns = 64
	maxOpenConns = 64
	maxLifetime  = time.Minute
)

var connManager = cache.NewResourceManager()

type pingedDB struct {
	*sqlx.DB
	once sync.Once
}

func getCachedSqlConn(driverName, server string) (*pingedDB, error) {
	val, err := connManager.GetResource(server, func() (io.Closer, error) {
		conn, err := newDBConnection(driverName, server)
		if err != nil {
			return nil, err
		}

		return &pingedDB{
			DB: conn,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*pingedDB), nil
}

func getSqlConn(driverName, server string) (*sqlx.DB, error) {
	pdb, err := getCachedSqlConn(driverName, server)
	if err != nil {
		return nil, err
	}

	pdb.once.Do(func() {
		err = pdb.Ping()
	})
	if err != nil {
		return nil, err
	}

	return pdb.DB, nil
}

func newDBConnection(driverName, datasource string) (*sqlx.DB, error) {
	conn, err := sqlx.Open(driverName, datasource)
	if err != nil {
		return nil, err
	}

	conn.SetMaxIdleConns(maxIdleConns) // 同时打开的最大连接数
	conn.SetMaxOpenConns(maxOpenConns) // 连接池中最大空闲连接数
	conn.SetConnMaxLifetime(maxLifetime) // 连接的生命周期

	return conn, nil
}
