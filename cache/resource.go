package cache

import (
	"io"
	"redis-cache/singleflight"
	"redis-cache/utils"
	"sync"
)

type ResourceManager struct {
	resources   map[string]io.Closer
	sharedCalls singleflight.SharedCalls
	lock        sync.RWMutex
}

func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		resources:   make(map[string]io.Closer),
		sharedCalls: singleflight.NewSharedCalls(),
	}
}

func (manager *ResourceManager) Close() error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	var be utils.BatchError
	for _, resource := range manager.resources {
		if err := resource.Close(); err != nil {
			be.Add(err)
		}
	}

	return be.Err()
}

func (manager *ResourceManager) GetResource(key string, create func() (io.Closer, error)) (io.Closer, error) {
	val, err := manager.sharedCalls.Do(key, func() (interface{}, error) {
		manager.lock.RLock()
		resource, ok := manager.resources[key]
		manager.lock.RUnlock()
		if ok {
			return resource, nil
		}

		resource, err := create()
		if err != nil {
			return nil, err
		}

		manager.lock.Lock()
		manager.resources[key] = resource
		manager.lock.Unlock()

		return resource, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(io.Closer), nil
}
