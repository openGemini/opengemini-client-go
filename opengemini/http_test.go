package opengemini

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestSetAuthorization(t *testing.T) {
	c := client{
		config: &Config{
			AuthConfig: &AuthConfig{
				AuthType: AuthTypePassword,
				Username: "test",
				Password: "test pwd",
			},
		},
	}

	header := c.updateAuthHeader(http.MethodGet, UrlPing, nil)
	require.Equal(t, "", header.Get("Authorization"))

	header = c.updateAuthHeader(http.MethodOptions, UrlQuery, nil)
	require.Equal(t, "", header.Get("Authorization"))

	header = c.updateAuthHeader(http.MethodGet, UrlQuery, nil)
	require.Equal(t, "Basic dGVzdDp0ZXN0IHB3ZA==", header.Get("Authorization"))
}
