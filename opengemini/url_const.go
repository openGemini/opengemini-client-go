package opengemini

import "net/http"

const (
	UrlPing   = "/ping"
	UrlQuery  = "/query"
	UrlWrite  = "/write"
	UrlStatus = "/status"
)

var unAuthorization = map[string][]string{
	UrlPing:   {http.MethodHead, http.MethodGet},
	UrlQuery:  {http.MethodOptions},
	UrlStatus: {http.MethodHead, http.MethodGet},
}
