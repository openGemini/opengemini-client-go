package opengemini

import (
	"errors"
	"sync"
	"time"
)

var allServersDown = errors.New("all servers down")

const (
	healthCheckPeriod = time.Second * 10
)

func (c *client) endpointsCheck() {
	var t = time.NewTicker(healthCheckPeriod)
	for {
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}

		c.checkUpOrDown()
	}
}

func (c *client) checkUpOrDown() {
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
			c.endpoints[idx].isDown = false
			if err := c.Ping(idx); err != nil {
				c.endpoints[idx].isDown = true
			}
		}(i)
	}
	wg.Wait()
}

// getServerUrl if all servers down, return error
func (c *client) getServerUrl() (string, error) {
	serverLen := len(c.endpoints)
	for i := serverLen; i > 0; i-- {
		idx := uint32(c.prevIdx.Add(1)) % uint32(serverLen)
		if c.endpoints[idx].isDown {
			continue
		}
		return c.endpoints[idx].url, nil
	}
	return "", allServersDown
}
