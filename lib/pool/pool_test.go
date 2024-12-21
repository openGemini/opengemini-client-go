// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package pool

import (
	"sync"
	"testing"
)

func TestCachePool(t *testing.T) {
	// Create a new CachePool with a max size of 2
	pool := NewCachePool(func() interface{} {
		return new(int)
	}, 2)

	// Get an item from the pool
	item1 := pool.Get().(*int)
	if item1 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	// Put the item back into the pool
	pool.Put(item1)

	// Get another item from the pool
	item2 := pool.Get().(*int)
	if item2 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	// Ensure the item is the same as the first one
	if item1 != item2 {
		t.Errorf("expected the same item, got different items")
	}

	// Put the item back into the pool
	pool.Put(item2)

	// Get two more items from the pool
	item3 := pool.Get().(*int)
	item4 := pool.Get().(*int)

	// Ensure the pool does not exceed its max size
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Put(item3)
		pool.Put(item4)
	}()

	wg.Wait()

	// Ensure the pool size is correct
	select {
	case pool.size <- struct{}{}:
		return
	default:
		t.Errorf("expected pool to be full, but it was not")
	}
}
