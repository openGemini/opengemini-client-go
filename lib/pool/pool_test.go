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
	"testing"
)

func TestCachePool(t *testing.T) {
	// Create a new CachePool with a max size of 2
	pool := NewCachePool(func() interface{} {
		return new(struct{})
	}, 2)

	// Get an item from the pool
	item1 := pool.Get().(*struct{})
	if item1 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	// Put the item back into the pool
	pool.Put(item1)

	// Get another item from the pool
	item2 := pool.Get().(*struct{})
	if item2 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	// Ensure the item is the same as the first one
	if item1 != item2 {
		t.Errorf("expected the same item, got different items")
	}

	if pool.AvailableOffers() != 1 {
		t.Errorf("The expected remaining capacity of the pool is 1, got %d", pool.AvailableOffers())
	}
	pool.Put(item2)

	item3 := pool.Get().(*struct{})
	if item3 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	item4 := pool.Get().(*struct{})
	if item4 == nil {
		t.Errorf("expected non-nil item, got nil")
	}

	if pool.AvailableOffers() != 0 {
		t.Errorf("The expected remaining capacity of the pool is 0, got %d", pool.AvailableOffers())
	}

	pool.Put(item3)
	pool.Put(item4)

	if pool.AvailableOffers() != 2 {
		t.Errorf("The expected remaining capacity of the pool is 2, got %d", pool.AvailableOffers())
	}
}
