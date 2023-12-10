package opengemini

import (
	"errors"
	"sync"
	"time"
)

var allServersDown = errors.New("all servers down")

func (c *client) serversCheck() {
	var t *time.Timer
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
				}
			}()
			c.checkUpOrDown()
		}()

		t = time.NewTimer(time.Second * 10)
		select {
		case <-c.ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
		}
	}
}

func (c *client) checkUpOrDown() {
	wg := &sync.WaitGroup{}
	for i := 0; i < len(c.serverUrls); i++ {
		wg.Add(1)
		go func(idx int) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
				}
			}()
			c.serverUrls[idx].isDown = false
			if err := c.Ping(idx); err != nil {
				c.serverUrls[idx].isDown = true
			}
		}(i)
	}
	wg.Wait()
}

// getServerUrl if all servers down, return error
func (c *client) getServerUrl() (string, error) {
	serverLen := len(c.serverUrls)
	for i := serverLen; i > 0; i-- {
		idx := uint32(c.prevIdx.Add(1)) % uint32(serverLen)
		if c.serverUrls[idx].isDown {
			continue
		}
		return c.serverUrls[idx].url, nil
	}
	return "", allServersDown
}
