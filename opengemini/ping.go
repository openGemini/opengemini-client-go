package opengemini

import "net/http"

// Ping check that status of cluster.
func (c *client) Ping(idx int) error {
	resp, err := c.request.getInstance().setMethod(http.MethodGet).setURL(c.serverUrls[idx], UrlPing).do()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return nil
	}
	return nil
}
