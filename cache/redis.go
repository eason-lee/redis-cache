package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	rdb "github.com/go-redis/redis/v8"
)

const (
	ClusterType     = "cluster"
	NodeType        = "node"
	Nil             = rdb.Nil
	defaultDatabase = 0

	blockingQueryTimeout = 5 * time.Second
	readWriteTimeout     = 2 * time.Second

	slowThreshold = time.Millisecond * 100
)

var (
	ErrNilNode     = errors.New("nil redis node")
	clusterManager = NewResourceManager()
	clientManager  = NewResourceManager()
)

type (
	Redis struct {
		Addr string
		Type string
		Pass string
	}
	RedisNode interface {
		rdb.Cmdable
	}
)

// NewRedis  the type is node or cluster
func NewRedis(redisAddr, redisType string, redisPass ...string) *Redis {
	var pass string
	for _, v := range redisPass {
		pass = v
	}

	return &Redis{
		Addr: redisAddr,
		Type: redisType,
		Pass: pass,
	}
}

func getRedis(r *Redis) (RedisNode, error) {
	switch r.Type {
	case ClusterType:
		return getCluster(r.Addr, r.Pass)
	case NodeType:
		return getClient(r.Addr, r.Pass)
	default:
		return nil, fmt.Errorf("redis type '%s' is not supported", r.Type)
	}
}

func getCluster(server, pass string) (*rdb.ClusterClient, error) {
	val, err := clusterManager.GetResource(server, func() (io.Closer, error) {
		store := rdb.NewClusterClient(&rdb.ClusterOptions{
			Addrs:    []string{server},
			Password: pass,
		})

		return store, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*rdb.ClusterClient), nil
}

func getClient(server, pass string) (*rdb.Client, error) {
	val, err := clientManager.GetResource(server, func() (io.Closer, error) {
		store := rdb.NewClient(&rdb.Options{
			Addr:     server,
			Password: pass,
			DB:       defaultDatabase,
		})

		return store, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*rdb.Client), nil
}

func (r *Redis) Get(ctx context.Context, key string) (val string, err error) {
	conn, err := getRedis(r)
	if err != nil {
		return
	}

	if val, err = conn.Get(ctx, key).Result(); err == rdb.Nil {
		return val, nil
	} else if err != nil {
		return
	}

	return
}

func (r *Redis) Set(ctx context.Context, key, val string, expire time.Duration) error {
	conn, err := getRedis(r)
	if err != nil {
		return err
	}

	return conn.Set(ctx, key, val, expire).Err()
}

func (r *Redis) Del(ctx context.Context, keys ...string) error {
	conn, err := getRedis(r)
	if err != nil {
		return err
	}

	return conn.Del(ctx, keys...).Err()
}
