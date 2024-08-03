package opengemini

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
)

type requestDetails struct {
	queryValues url.Values
	header      http.Header
	body        io.Reader
}

func (c *client) updateAuthHeader(method, urlPath string, header http.Header) http.Header {
	if c.config.AuthConfig == nil {
		return header
	}

	if methods, ok := noAuthRequired[urlPath]; ok {
		if _, methodOk := methods[method]; methodOk {
			return header
		}
	}

	if header == nil {
		header = make(http.Header)
	}

	if c.config.AuthConfig.AuthType == AuthTypePassword {
		encodeString := c.config.AuthConfig.Username + ":" + c.config.AuthConfig.Password
		authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(encodeString))
		header.Set("Authorization", authorization)
	}

	return header
}

func (c *client) executeHttpRequestByIdxWithContext(ctx context.Context, idx int, method, urlPath string, details requestDetails) (*http.Response, error) {
	if idx >= len(c.endpoints) || idx < 0 {
		return nil, errors.New("index out of range")
	}
	return c.executeHttpRequestInner(ctx, method, c.endpoints[idx].url, urlPath, details)
}

func (c *client) executeHttpGet(urlPath string, details requestDetails) (*http.Response, error) {
	return c.executeHttpRequest(http.MethodGet, urlPath, details)
}

func (c *client) executeHttpPost(urlPath string, details requestDetails) (*http.Response, error) {
	return c.executeHttpRequest(http.MethodPost, urlPath, details)
}

func (c *client) executeHttpRequest(method, urlPath string, details requestDetails) (*http.Response, error) {
	serverUrl, err := c.getServerUrl()
	if err != nil {
		return nil, err
	}
	return c.executeHttpRequestInner(context.TODO(), method, serverUrl, urlPath, details)
}

func (c *client) executeHttpRequestWithContext(ctx context.Context, method, urlPath string, details requestDetails) (*http.Response, error) {
	serverUrl, err := c.getServerUrl()
	if err != nil {
		return nil, err
	}
	return c.executeHttpRequestInner(ctx, method, serverUrl, urlPath, details)
}

// executeHttpRequestInner executes an HTTP request with the given method, server URL, URL path, and request details.
//
// Parameters:
// - ctx: The context.Context to associate with the request, if ctx is nil, request is created without a context.
// - method: The HTTP method to use for the request.
// - serverUrl: The server URL to use for the request.
// - urlPath: The URL path to use for the request.
// - details: The request details to use for the request.
//
// Returns:
// - *http.Response: The HTTP response from the request.
// - error: An error that occurred during the request.
func (c *client) executeHttpRequestInner(ctx context.Context, method, serverUrl, urlPath string, details requestDetails) (*http.Response, error) {
	details.header = c.updateAuthHeader(method, urlPath, details.header)
	fullUrl := serverUrl + urlPath
	u, err := url.Parse(fullUrl)
	if err != nil {
		return nil, err
	}

	if details.queryValues != nil {
		u.RawQuery = details.queryValues.Encode()
	}

	var request *http.Request

	if ctx == nil {
		request, err = http.NewRequest(method, u.String(), details.body)
		if err != nil {
			return nil, err
		}
	} else {
		request, err = http.NewRequestWithContext(ctx, method, u.String(), details.body)
		if err != nil {
			return nil, err
		}
	}

	for k, values := range details.header {
		for _, v := range values {
			request.Header.Add(k, v)
		}
	}

	return c.cli.Do(request)
}
