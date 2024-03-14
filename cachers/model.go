package cachers

import (
	"context"
	"io"
)

// ActionValue is the JSON value returned by the cacher server for an GET /action request.
type ActionValue struct {
	OutputID string `json:"outputID"`
	Size     int64  `json:"size"`
}

type Cache interface {
	Get(ctx context.Context, actionID string) (outputID, diskpath string, size int64, reader io.ReadCloser, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error)
}
