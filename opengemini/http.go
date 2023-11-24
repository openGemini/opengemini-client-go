package opengemini

import (
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

func (c *client) executeHttpGetByIdx(idx int, urlPath string, details requestDetails) (*http.Response, error) {
	return c.executeHttpRequestByIdx(idx, http.MethodGet, urlPath, details)
}

func (c *client) executeHttpRequestByIdx(idx int, method, urlPath string, details requestDetails) (*http.Response, error) {
	if idx >= len(c.serverUrls) || idx < 0 {
		return nil, errors.New("index out of range")
	}
	serverUrl := c.serverUrls[idx]
	return c.executeHttpRequestInner(method, serverUrl, urlPath, details)
}

func (c *client) executeHttpGet(urlPath string, details requestDetails) (*http.Response, error) {
	return c.executeHttpRequest(http.MethodGet, urlPath, details)
}

func (c *client) executeHttpRequest(method, urlPath string, details requestDetails) (*http.Response, error) {
	idx := int(c.currentIdx.Add(1)) % len(c.serverUrls)
	return c.executeHttpRequestInner(method, c.serverUrls[idx], urlPath, details)
}

func (c *client) executeHttpRequestInner(method, serverUrl, urlPath string, details requestDetails) (*http.Response, error) {
	details.header = c.updateAuthHeader(method, urlPath, details.header)
	fullUrl := serverUrl + urlPath
	u, err := url.Parse(fullUrl)
	if err != nil {
		return nil, err
	}

	if details.queryValues != nil {
		u.RawQuery = details.queryValues.Encode()
	}

	req, err := http.NewRequest(method, u.String(), details.body)
	if err != nil {
		return nil, err
	}

	for k, values := range details.header {
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}

	return c.cli.Do(req)
}
