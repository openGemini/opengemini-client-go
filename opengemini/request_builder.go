package opengemini

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
)

// requestBuilder generate network request in builder mode.
type requestBuilder struct {
	// basicAuthCode auth code.
	basicAuthCode string
	// an HTTP client.
	*http.Client
}

// scope represent one request builder.
type scope struct {
	// method http request method.
	method string
	// url request route api raw url.
	url *url.URL
	// queryValues request params.
	queryValues url.Values
	// header represent the key-value pairs in an HTTP header.
	header http.Header
	// body request body in an HTTP.
	body io.Reader
	// client an HTTP client.
	client *http.Client
	// err error message when build request.
	err error
}

func newRequestBuilder(config Config) *requestBuilder {
	var builder = new(requestBuilder)

	if config.AuthConfig != nil {
		if config.AuthConfig.AuthType == AuthTypePassword {
			builder.setBasicAuth(config.AuthConfig.Username, config.AuthConfig.Password)
		}
	}

	if config.TlsEnabled {
		builder.Client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: config.TlsConfig,
			},
		}
	}

	if builder.Client == nil {
		builder.Client = http.DefaultClient
	}

	return builder
}

// getInstance return scope as new request builder instance.
func (rb *requestBuilder) getInstance() *scope {
	var s = new(scope)
	if rb.basicAuthCode != "" {
		s.setBasicAuth(rb.basicAuthCode)
	}
	s.client = rb.Client
	return s
}

func (rb *requestBuilder) setBasicAuth(username, password string) *requestBuilder {
	encodeString := username + ":" + password
	rb.basicAuthCode = base64.StdEncoding.EncodeToString([]byte(encodeString))
	return rb
}

func (s *scope) setURL(serverUrl, urlPath string) *scope {
	fullUrl := serverUrl + urlPath
	u, err := url.Parse(fullUrl)
	if err != nil {
		s.err = err
		return s
	}
	s.url = u
	return s
}

func (s *scope) setQueryValues(urlPath string) *scope {
	return s
}

func (s *scope) setBody(body io.Reader) *scope {
	s.body = body
	return s
}

func (s *scope) setMethod(method string) *scope {
	s.method = method
	return s
}

func (s *scope) setBasicAuth(token string) *scope {
	if s.header == nil {
		s.header = make(http.Header)
	}
	s.header.Set("Authorization", "Basic "+token)
	return s
}

func (s *scope) getRequest() (*http.Request, error) {
	request, err := http.NewRequest(s.method, s.url.String(), s.body)
	if err != nil {
		return nil, err
	}
	return request, nil
}

func (s *scope) do() (*http.Response, error) {
	request, err := s.getRequest()
	if err != nil {
		return nil, err
	}
	return s.client.Do(request)
}
