package cache

import (
	"gosaga/app/common"
	"gosaga/app/entity"
	"sync"
)

type ContainerCache struct {
	ch   chan entity.HandleInfo
	data []int
}

var (
	containerCache     *ContainerCache
	containerCacheOnce sync.Once
)

func NewContainerCache() *ContainerCache {
	if containerCache == nil {
		containerCacheOnce.Do(func() {
			containerCache = &ContainerCache{
				ch:   make(chan entity.HandleInfo),
				data: make([]int, 0),
			}
			go containerCache.schedule()
		})
	}

	return containerCache
}

func (c *ContainerCache) schedule() {
	for handleInfo := range c.ch {
		switch handleInfo.HandleType {
		case common.ContainerAddHandleType:
			c.data = append(c.data, handleInfo.SagaId)
		case common.ContainerRemoveHandleType:
			data := make([]int, 0)
			for _, id := range c.data {
				if id == handleInfo.SagaId {
					continue
				}
				data = append(data, id)
			}
			c.data = data
		}
	}
}

func (c *ContainerCache) Add(sagaId int) {
	c.ch <- entity.HandleInfo{
		SagaId:     sagaId,
		HandleType: common.ContainerAddHandleType,
	}
}

func (c *ContainerCache) Get() []int {
	return c.data
}

func (c *ContainerCache) Remove(sagaId int) {
	c.ch <- entity.HandleInfo{
		SagaId:     sagaId,
		HandleType: common.ContainerRemoveHandleType,
	}
}
