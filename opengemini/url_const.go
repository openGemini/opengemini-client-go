package opengemini

import "net/http"

const (
	UrlPing   = "/ping"
	UrlQuery  = "/query"
	UrlStatus = "/status"
	UrlWrite  = "/write"
)

var noAuthRequired = map[string]map[string]struct{}{
	UrlPing: {
		http.MethodHead: {},
		http.MethodGet:  {},
	},
	UrlQuery: {
		http.MethodOptions: {},
	},
	UrlStatus: {
		http.MethodHead: {},
		http.MethodGet:  {},
	},
}
