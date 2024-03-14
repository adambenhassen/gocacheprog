package proc

import (
	"bytes"
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

func savefile(actionID, objectID string, size int64, body io.Reader) (string, error) {
	d, err := os.UserCacheDir()
	if err != nil {
		log.Fatal(err)
	}
	dir := filepath.Join(d, "gocacheprog")
	os.Mkdir(dir, os.ModePerm)
	file := filepath.Join(dir, fmt.Sprintf("o-%s", objectID))

	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		_ = zf.Close()
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
	actionFile := filepath.Join(dir, fmt.Sprintf("a-%s", actionID))
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
