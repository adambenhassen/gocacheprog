package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/adambenhassen/gocacheprog/cachers"
)

type HTTPCache struct {
	baseURL string       // i.e "http://localhost:31364".
	client  *http.Client // optional, if nil, http.DefaultClient is used.
	verbose bool
	secret  string
}

func NewCache(baseURL string, secret string, verbose bool) *HTTPCache {
	return &HTTPCache{
		baseURL: baseURL,
		verbose: verbose,
		secret:  secret,
	}
}

func (c *HTTPCache) Get(ctx context.Context, actionID string) (string, string, int64, io.ReadCloser, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/action/"+actionID, nil)
	req.Header.Add("secret", c.secret)
	res, err := c.httpClient().Do(req)
	if err != nil {
		return "", "", 0, nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return "", "", 0, nil, errors.New("not found")
	}

	if res.StatusCode != http.StatusOK {
		return "", "", 0, nil, fmt.Errorf("unexpected GET /action/%s status %v", actionID, res.Status)
	}

	var av cachers.ActionValue
	if err := json.NewDecoder(res.Body).Decode(&av); err != nil {
		return "", "", 0, nil, err
	}

	outputID := av.OutputID
	if av.Size == 0 {
		return outputID, "", av.Size, io.NopCloser(bytes.NewReader(nil)), nil
	}

	req, _ = http.NewRequestWithContext(ctx, "GET", c.baseURL+"/output/"+outputID, nil)
	req.Header.Add("secret", c.secret)
	res, err = c.httpClient().Do(req)
	if err != nil {
		return "", "", 0, nil, err
	}

	if res.StatusCode == http.StatusNotFound {
		return "", "", 0, nil, errors.New("not found")
	}

	if res.StatusCode != http.StatusOK {
		return "", "", 0, nil, fmt.Errorf("unexpected GET /output/%s status %v", outputID, res.Status)
	}

	if res.ContentLength == -1 {
		return "", "", 0, nil, fmt.Errorf("no Content-Length from server")
	}

	return outputID, "", av.Size, res.Body, nil
}

func (c *HTTPCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {
		putBody = body
	}

	req, _ := http.NewRequestWithContext(ctx, "PUT", c.baseURL+"/"+actionID+"/"+outputID, putBody)
	req.ContentLength = size
	req.Header.Add("secret", c.secret)
	res, err := c.httpClient().Do(req)
	if err != nil {
		log.Printf("error PUT /%s/%s: %v", actionID, outputID, err)
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		all, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		return "", fmt.Errorf("unexpected PUT /%s/%s status %v: %s", actionID, outputID, res.Status, all)
	}

	return "", nil
}

func (c *HTTPCache) httpClient() *http.Client {
	if c.client != nil {
		return c.client
	}
	return http.DefaultClient
}
