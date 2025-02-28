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

package opengemini

import (
	"context"
	"sync"
	"time"
)

const (
	healthCheckPeriod = time.Second * 10
)

func (c *client) endpointsCheck(ctx context.Context) {
	var t = time.NewTicker(healthCheckPeriod)
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
			c.checkUpOrDown(ctx)
		}
	}
}

func (c *client) checkUpOrDown(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for i := 0; i < len(c.endpoints); i++ {
		wg.Add(1)
		go func(idx int) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					c.logger.Error("panic recovered during endpoint check", "index", idx, "error", err)
					return
				}
			}()
			err := c.ping(ctx, idx)
			if err != nil {
				c.logger.Error("ping failed", "index", idx, "error", err)
			} else {
				c.logger.Info("ping succeeded", "index", idx)
			}
			c.endpoints[idx].isDown.Store(err != nil)
		}(i)
	}
	wg.Wait()
}

// getServerUrl if all servers down, return error
func (c *client) getServerUrl() string {
	serverLen := len(c.endpoints)
	for i := serverLen; i > 0; i-- {
		idx := uint32(c.prevIdx.Add(1)) % uint32(serverLen)
		if c.endpoints[idx].isDown.Load() {
			continue
		}
		return c.endpoints[idx].url
	}
	c.logger.Error("all servers down, no endpoints found")
	return c.endpoints[random.Intn(serverLen)].url
}
