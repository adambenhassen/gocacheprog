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

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/s2"
)

const (
	outputIDMetadataKey      = "outputid"
	outputUncompressedLength = "content-length-raw"
	binaryType               = "application/x-binary"
)

type GCSCache struct {
	bucket  string
	prefix  string
	verbose bool
	client  *storage.Client
}

func NewCache(client *storage.Client, bucketName string, cacheKey string, verbose bool) *GCSCache {
	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}

	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}

	cache := &GCSCache{
		client:  client,
		verbose: verbose,
		bucket:  bucketName,
		prefix:  fmt.Sprintf("cache/%s/%s/%s", cacheKey, goarch, goos),
	}
	return cache
}

func (s *GCSCache) Get(ctx context.Context, actionID string) (string, io.Reader, int, error) {
	actionKey := s.actionKey(actionID)
	object := s.client.Bucket(s.bucket).Object(actionKey)
	reader, err := object.NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", nil, 0, nil
	}

	if err != nil {
		return "", nil, 0, err
	}

	attrs, err := object.Attrs(ctx)
	if err != nil {
		return "", nil, 0, err
	}

	outputID, ok := attrs.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		return "", nil, 0, err
	}

	sizeStr, ok := attrs.Metadata[outputUncompressedLength]
	if !ok || sizeStr == "" {
		return "", nil, 0, err
	}

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return "", nil, 0, err
	}

	return outputID, io.NopCloser(s2.NewReader(reader)), size, nil
}

func (s *GCSCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) error {
	var err error
	actionKey := s.actionKey(actionID)

	if size == 0 {
		body = bytes.NewReader(nil)
	}

	_, err = s.client.Bucket(s.bucket).Object(actionKey).Attrs(ctx)
	if err == nil {
		s.Logf("-> PUT %s exists\n", actionID)
		return nil
	}

	wc := s.client.Bucket(s.bucket).Object(actionKey).NewWriter(ctx)
	wc.ContentType = binaryType
	wc.Metadata = map[string]string{
		outputIDMetadataKey:      outputID,
		outputUncompressedLength: fmt.Sprint(size),
	}

	wr := s2.NewWriter(wc)

	_, err = io.Copy(wr, body)
	if err != nil {
		return err
	}

	err = wr.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *GCSCache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}

func (s *GCSCache) Logf(format string, v ...any) {
	if s.verbose {
		log.Printf(format, v...)
	}
}
