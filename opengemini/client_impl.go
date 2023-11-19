package opengemini

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type selector struct {
	sync.RWMutex
	idx        uint64
	serverUrls []string
}

// searchByIdx get server url by index
func (s *selector) searchByIdx(idx int) string {
	s.RLock()
	defer s.RUnlock()
	if idx < 0 || idx >= len(s.serverUrls) {
		return ""
	}
	return s.serverUrls[idx]
}

// search get server url by round_robin
func (s *selector) search() string {
	s.Lock()
	defer s.Unlock()
	if len(s.serverUrls) == 0 {
		return ""
	}
	serverUrl := s.serverUrls[s.idx%uint64(len(s.serverUrls))]
	s.idx++
	return serverUrl
}

// searchAlive get a alive server url
func (s *selector) searchAlive() (string, bool) {
	s.RLock()
	defer s.RUnlock()
	timeout := time.Second * 5
	alive := make(chan string, len(s.serverUrls))
	for _, serverUrl := range s.serverUrls {
		go func(serverUrl string) {
			split := strings.Split(serverUrl, "//")
			if len(split) != 2 {
				return
			}
			conn, err := net.DialTimeout("tcp", split[1], timeout)
			if err != nil {
				return
			}
			conn.Close()
			alive <- serverUrl
		}(serverUrl)
	}
	select {
	case <-time.After(timeout):
		return "", false
	case serverUrl := <-alive:
		return serverUrl, true
	}
}

type client struct {
	config         *Config
	serverSelector *selector
	cli            *http.Client
}

func newClient(c *Config) (Client, error) {
	if len(c.Addresses) == 0 {
		return nil, errors.New("must have at least one address")
	}
	if c.AuthConfig != nil {
		if c.AuthConfig.AuthType == AuthTypeToken && len(c.AuthConfig.Token) == 0 {
			return nil, errors.New("invalid auth config due to empty token")
		}
		if c.AuthConfig.AuthType == AuthTypePassword {
			if len(c.AuthConfig.Username) == 0 {
				return nil, errors.New("invalid auth config due to empty username")
			}
			if len(c.AuthConfig.Password) == 0 {
				return nil, errors.New("invalid auth config due to empty password")
			}
		}
	}
	if c.BatchConfig != nil {
		if c.BatchConfig.BatchInterval <= 0 {
			return nil, errors.New("batch enabled, batch interval must be great than 0")
		}
		if c.BatchConfig.BatchSize <= 0 {
			return nil, errors.New("batch enabled, batch size must be great than 0")
		}
	}
	client := &client{
		config:         c,
		serverSelector: buildServerSelector(c.Addresses, c.TlsEnabled),
		cli:            newHttpClient(*c),
	}
	return client, nil
}

func buildServerSelector(addresses []*Address, tlsEnabled bool) *selector {
	urls := make([]string, len(addresses))
	protocol := "http://"
	if tlsEnabled {
		protocol = "https://"
	}
	for i, addr := range addresses {
		urls[i] = protocol + addr.Host + ":" + strconv.Itoa(addr.Port)
	}
	return &selector{serverUrls: urls}
}

func newHttpClient(config Config) *http.Client {
	if config.TlsEnabled {
		return &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: config.TlsConfig,
			},
		}
	} else {
		return &http.Client{}
	}
}

type requestMetadata struct {
	queryValues   url.Values
	header        http.Header
	contentBody   io.Reader
	contentLength int64
}

func (c *client) setAuthorization(method, url string, header http.Header) http.Header {
	if header == nil {
		header = make(http.Header)
	}
	if c.config.AuthConfig == nil {
		return header
	}

	methods, ok := unAuthorization[url]
	if ok {
		for _, v := range methods {
			if method == v {
				return header
			}
		}
	}

	var authorization string
	if c.config.AuthConfig.AuthType == AuthTypePassword {
		encodeString := c.config.AuthConfig.Username + ":" + c.config.AuthConfig.Password
		authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(encodeString))
	}

	header.Set("Authorization", authorization)
	return header
}

func (c *client) requestByIdx(idx int, method, url string, metadata requestMetadata) (*http.Response, error) {
	metadata.header = c.setAuthorization(method, url, metadata.header)
	server := c.serverSelector.searchByIdx(idx)
	url = server + url
	return c.executeMethod(method, url, metadata)
}

func (c *client) request(method, url string, metadata requestMetadata) (*http.Response, error) {
	metadata.header = c.setAuthorization(method, url, metadata.header)
	server := c.serverSelector.search()
	rsp, err := c.executeMethod(method, server+url, metadata)
	if err == nil {
		return rsp, nil
	}
	server, alive := c.serverSelector.searchAlive()
	if !alive {
		return nil, errors.New("no address available")
	}
	return c.executeMethod(method, server+url, metadata)
}

func (c *client) executeMethod(method, serverUrl string, metadata requestMetadata) (*http.Response, error) {
	// default method: 'GET'.
	if method == "" {
		method = http.MethodGet
	}

	// set query values
	if metadata.queryValues != nil {
		u, err := url.Parse(serverUrl)
		if err != nil {
			return nil, err
		}
		u.RawQuery = metadata.queryValues.Encode()
		serverUrl = u.String()
	}

	// Initialize a new HTTP request for the method.
	req, err := http.NewRequest(method, serverUrl, nil)
	if err != nil {
		return nil, err
	}

	// Set all headers.
	for k, v := range metadata.header {
		req.Header.Set(k, v[0])
	}

	// Set body
	if metadata.contentLength == 0 {
		req.Body = nil
	} else {
		req.Body = io.NopCloser(metadata.contentBody)
		req.ContentLength = metadata.contentLength
	}

	res, err := c.cli.Do(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}
