package disk

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type indexEntry struct {
	Version   int    `json:"v"`
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeNanos int64  `json:"t"`
}

type DiskCache struct {
	dir     string
	verbose bool
}

func NewCache(ctx context.Context, dir string, verbose bool) *DiskCache {
	if dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "gocacheprog")
		dir = d
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	return &DiskCache{
		dir:     dir,
		verbose: verbose,
	}
}

func (dc *DiskCache) Get(_ context.Context, actionID string) (string, string, int64, io.ReadCloser, error) {
	actionFile := filepath.Join(dc.dir, fmt.Sprintf("a-%s", actionID))
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		return "", "", 0, nil, err
	}

	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		log.Printf("Warning: JSON error for action %q: %v", actionID, err)
		return "", "", 0, nil, err
	}

	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		// Protect against malicious non-hex OutputID on disk
		return "", "", 0, nil, err
	}

	return ie.OutputID, filepath.Join(dc.dir, fmt.Sprintf("o-%v", ie.OutputID)), ie.Size, io.NopCloser(bytes.NewReader(ij)), nil
}

func (dc *DiskCache) Put(_ context.Context, actionID, objectID string, size int64, body io.Reader) (string, error) {
	file := filepath.Join(dc.dir, fmt.Sprintf("o-%s", objectID))

	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		zf.Close()
	} else {
		wrote, err := writeAtomic(file, body)
		if err != nil {
			return "", err
		}
		if wrote != size {
			return "", fmt.Errorf("wrote %d bytes, expected %d", wrote, size)
		}
	}

	ij, err := json.Marshal(indexEntry{
		Version:   1,
		OutputID:  objectID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		return "", err
	}

	actionFile := filepath.Join(dc.dir, fmt.Sprintf("a-%s", actionID))
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		return "", err
	}

	return file, nil
}

func writeTempFile(dest string, r io.Reader) (string, int64, error) {
	tf, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".*")
	if err != nil {
		return "", 0, err
	}

	fileName := tf.Name()
	defer func() {
		tf.Close()
		if err != nil {
			os.Remove(fileName)
		}
	}()

	size, err := io.Copy(tf, r)
	if err != nil {
		return "", 0, err
	}
	return fileName, size, nil
}

func writeAtomic(dest string, r io.Reader) (int64, error) {
	tempFile, size, err := writeTempFile(dest, r)
	if err != nil {
		return 0, err
	}

	defer func() {
		if err != nil {
			os.Remove(tempFile)
		}
	}()

	if err = os.Rename(tempFile, dest); err != nil {
		return 0, err
	}

	return size, nil
}
