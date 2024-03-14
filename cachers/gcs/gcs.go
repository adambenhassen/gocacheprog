package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/s2"
)

const (
	outputIDMetadataKey      = "outputid"
	outputUncompressedLength = "content-length-raw"
	binaryType               = "application/octet-stream"
)

type GCSCache struct {
	bucket        string
	prefix        string
	verbose       bool
	client        *storage.Client
	actioncache   map[string]struct{}
	actioncacheMu sync.RWMutex
}

func NewCache(ctx context.Context, bucketName string, cacheKey string, verbose bool) *GCSCache {
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}

	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}

	cache := &GCSCache{
		client:      client,
		verbose:     verbose,
		bucket:      bucketName,
		prefix:      fmt.Sprintf("cache/%s/%s/%s", cacheKey, goarch, goos),
		actioncache: map[string]struct{}{},
	}
	return cache
}

func (s *GCSCache) Get(ctx context.Context, actionID string) (string, string, int64, io.ReadCloser, error) {
	actionKey := s.actionKey(actionID)
	object := s.client.Bucket(s.bucket).Object(actionKey)
	reader, err := object.NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", "", 0, nil, err
	}

	if err != nil {
		return "", "", 0, nil, err
	}

	attrs, err := object.Attrs(ctx)
	if err != nil {
		return "", "", 0, nil, err
	}

	outputID, ok := attrs.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		return "", "", 0, nil, err
	}

	sizeStr, ok := attrs.Metadata[outputUncompressedLength]
	if !ok || sizeStr == "" {
		return "", "", 0, nil, err
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return "", "", 0, nil, err
	}

	return outputID, "", size, io.NopCloser(s2.NewReader(reader)), nil
}

func (s *GCSCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	var err error
	actionKey := s.actionKey(actionID)

	if size == 0 {
		body = bytes.NewReader(nil)
	}

	s.actioncacheMu.RLock()
	_, ok := s.actioncache[actionID]
	s.actioncacheMu.RUnlock()
	if ok {
		return "", nil
	}

	object := s.client.Bucket(s.bucket).Object(actionKey)
	_, err = object.Attrs(ctx)
	if err == nil {
		s.actioncacheMu.Lock()
		s.actioncache[actionID] = struct{}{}
		s.actioncacheMu.Unlock()
		//s.Logf("-> PUT %s exists\n", actionID)
		return "", nil
	}

	wc := object.NewWriter(ctx)
	wc.ContentType = binaryType
	wc.Metadata = map[string]string{
		outputIDMetadataKey:      outputID,
		outputUncompressedLength: fmt.Sprint(size),
	}

	wr := s2.NewWriter(wc)

	_, err = io.Copy(wr, body)
	if err != nil {
		return "", err
	}

	err = wr.Close()
	if err != nil {
		return "", err
	}

	err = wc.Close()
	if err != nil {
		return "", err
	}

	return "", nil
}

func (s *GCSCache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}

func (s *GCSCache) Logf(format string, v ...any) {
	if s.verbose {
		log.Printf(format, v...)
	}
}
