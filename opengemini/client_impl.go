package opengemini

import "errors"

type client struct {
	config *Config
}

func newClient(c *Config) (Client, error) {
	if len(c.AddressList) == 0 {
		return nil, errors.New("must have at least one address")
	}
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
	if c.BatchConfig.BatchEnabled {
		if c.BatchConfig.BatchInterval <= 0 {
			return nil, errors.New("batch enabled, batch interval must be great than 0")
		}
		if c.BatchConfig.BatchSize <= 0 {
			return nil, errors.New("batch enabled, batch size must be great than 0")
		}
	}
	client := &client{
		config: c,
	}
	return client, nil
}
