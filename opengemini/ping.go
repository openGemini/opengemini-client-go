package opengemini

import (
	"context"
	"errors"
	"io"
	"net/http"
)

// Ping check that status of cluster.
func (c *client) Ping(idx int) error {
	return c.ping(context.TODO(), idx)
}

func (c *client) ping(ctx context.Context, idx int) error {
	resp, err := c.executeHttpRequestByIdxWithContext(ctx, idx, http.MethodGet, UrlPing, requestDetails{})
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
