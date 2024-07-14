package opengemini

import (
	"errors"
	"io"
	"net/http"
)

// Ping check that status of cluster.
func (c *client) Ping(idx int) error {
	resp, err := c.executeHttpGetByIdx(idx, UrlPing, requestDetails{})
	if err != nil {
		return errors.New("ping request failed, error: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.New("read ping resp failed, error: " + err.Error())
		}
		return errors.New("ping error resp, code: " + resp.Status + "body: " + string(body))
	}
	return nil
}
