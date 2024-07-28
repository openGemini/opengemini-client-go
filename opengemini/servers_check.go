package opengemini

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrAllServersDown = errors.New("all servers down")

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
					return
				}
			}()
			err := c.ping(ctx, idx)
			c.endpoints[idx].isDown.Store(err != nil)
		}(i)
	}
	wg.Wait()
}

// getServerUrl if all servers down, return error
func (c *client) getServerUrl() (string, error) {
	serverLen := len(c.endpoints)
	for i := serverLen; i > 0; i-- {
		idx := uint32(c.prevIdx.Add(1)) % uint32(serverLen)
		if c.endpoints[idx].isDown.Load() {
			continue
		}
		return c.endpoints[idx].url, nil
	}
	return "", ErrAllServersDown
}
