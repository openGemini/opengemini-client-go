package opengemini

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/url"
)

func (c *client) WriteBatchPoints(database string, bp *BatchPoints) error {
	var buffer bytes.Buffer

	var writer io.Writer

	if c.config.GzipEnabled {
		writer = gzip.NewWriter(&buffer)
	} else {
		writer = &buffer
	}
	for _, p := range bp.Points {
		if p == nil {
			continue
		}
		if _, err := io.WriteString(writer, p.String()); err != nil {
			return err
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	if closer, ok := writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	req := requestDetails{
		queryValues: make(url.Values),
		body:        &buffer,
	}
	req.queryValues.Add("db", database)
	resp, err := c.executeHttpPost(UrlWrite, req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		reason, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.New("write failed and couldn't get the error for " + err.Error())
		}
		return errors.New("write failed for " + string(reason))
	}
	return nil
}
