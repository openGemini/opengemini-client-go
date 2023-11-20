package opengemini

import "net/http"

// Ping check that status of cluster.
func (c *client) Ping(idx int) error {
	resp, err := c.executeHttpGetByIdx(idx, UrlPing, requestDetails{})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return nil
	}
	return nil
}
