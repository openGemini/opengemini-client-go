// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"sync"
)

type CachePool struct {
	pool sync.Pool
	size chan struct{}
}

func NewCachePool(newFunc func() interface{}, maxSize int) *CachePool {
	return &CachePool{
		pool: sync.Pool{
			New: newFunc,
		},
		size: make(chan struct{}, maxSize),
	}
}

func (c *CachePool) Get() interface{} {
	select {
	case c.size <- struct{}{}:
		return c.pool.Get()
	default:
		return c.pool.New()
	}
}

func (c *CachePool) Put(x interface{}) {
	select {
	case <-c.size:
		c.pool.Put(x)
	default:
		// Pool is full, discard the item
	}
}
