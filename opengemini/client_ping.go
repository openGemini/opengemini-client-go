package opengemini

import "net/http"

func (c client) Ping(idx int) error {
	serverUrl := c.serverUrls[idx]
	resp, err := c.cli.Get(serverUrl + UrlPing)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return nil
	}
	return nil
}
