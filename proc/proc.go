package proc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/adambenhassen/gocacheprog/cachers/gcs"
	"github.com/adambenhassen/gocacheprog/utils"
	"github.com/adambenhassen/gocacheprog/wire"
)

type cacherCtxKey string

var (
	ErrUnknownCommand = errors.New("unknown command")
	requestIDKey      = cacherCtxKey("requestID")
)

type Process struct {
	cache         *gcs.GCSCache
	gwg           sync.WaitGroup
	verbose       bool
	dir           string
	minUploadSize int64

	hits_local  int64
	hits_remote int64

	miss int64
	puts int64
}

func NewCacheProc(cache *gcs.GCSCache, verbose bool, minUploadSize int64) *Process {
	d, err := os.UserCacheDir()
	if err != nil {
		log.Fatal(err)
	}
	return &Process{
		cache:         cache,
		verbose:       verbose,
		dir:           d,
		minUploadSize: minUploadSize,
	}
}

func (p *Process) Run(ctx context.Context) error {
	var wmu sync.Mutex

	br := bufio.NewReader(os.Stdin)
	jd := json.NewDecoder(br)

	bw := bufio.NewWriter(os.Stdout)
	je := json.NewEncoder(bw)

	caps := []wire.Cmd{"get", "put", "close"}
	if err := je.Encode(&wire.Response{KnownCommands: caps}); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	wg, ctx := errgroup.WithContext(ctx)
	defer func() {
		wg.Wait()
		p.gwg.Wait()
		log.Printf("hits_local: %v, hits_remote: %v, misses: %v, puts: %v\n", p.hits_local, p.hits_remote, p.miss, p.puts)
	}()

	for {
		var req wire.Request
		if err := jd.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if req.Command != wire.CmdPut && req.Command != wire.CmdGet && req.Command != wire.CmdClose {
			continue
		}

		if req.Command == wire.CmdPut && req.BodySize > 0 {
			var bodyb []byte
			if err := jd.Decode(&bodyb); err != nil {
				log.Fatal(err)
			}

			if int64(len(bodyb)) != req.BodySize {
				log.Fatalf("only got %d bytes of declared %d", len(bodyb), req.BodySize)
			}

			req.Body = bytes.NewReader(bodyb)
		}

		wg.Go(func() error {
			res := &wire.Response{ID: req.ID}

			ctx := context.WithValue(ctx, requestIDKey, &req)
			if err := p.handleRequest(ctx, &req, res); err != nil {
				res.Err = err.Error()
				log.Println(err.Error())
			}

			wmu.Lock()
			defer wmu.Unlock()

			je.Encode(res)
			bw.Flush()

			return nil
		})
	}
}

func (p *Process) handleRequest(ctx context.Context, req *wire.Request, res *wire.Response) error {
	switch req.Command {
	default:
		return ErrUnknownCommand
	case "close":
		return nil
	case "get":
		err := p.handleGet(ctx, req, res)
		if res.Miss {
			atomic.AddInt64(&p.miss, 1)
		}
		return err
	case "put":
		return p.handlePut(ctx, req, res)
	}
}

func (p *Process) handleGet(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	var outputPath string
	actionID := fmt.Sprintf("%x", req.ActionID)
	start := time.Now()

	actionFile := filepath.Join(p.dir, "gocacheprog", fmt.Sprintf("a-%s", actionID))
	if _, err := os.Stat(actionFile); os.IsNotExist(err) {
		outputID, reader, size, err := p.cache.Get(ctx, actionID)
		if err != nil {
			res.Miss = true
			return err
		}

		took := time.Since(start)

		if outputID == "" {
			res.Miss = true
			// log.Printf("<- GET %s miss, took: %s\n", actionID, utils.FormatDuration(took))
			return nil
		}
		if p.verbose {
			log.Printf("<- GET %s took: %s\n", actionID, utils.FormatDuration(took))
		}
		res.OutputID, err = hex.DecodeString(outputID)
		if err != nil {
			res.Miss = true
			return fmt.Errorf("invalid OutputID: %w", err)
		}

		outputPath, err = savefile(actionID, outputID, int64(size), reader)
		if err != nil {
			res.Miss = true
			return fmt.Errorf("unable to save to file: %w", err)
		}
		atomic.AddInt64(&p.hits_remote, 1)
	} else {
		ij, err := os.ReadFile(actionFile)
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			res.Miss = true
			return err
		}

		var ie indexEntry
		if err := json.Unmarshal(ij, &ie); err != nil {
			res.Miss = true
			log.Printf("Warning: JSON error for action %q: %v", actionID, err)
			return nil
		}

		if _, err := hex.DecodeString(ie.OutputID); err != nil {
			res.Miss = true
			// Protect against malicious non-hex OutputID on disk
			return nil
		}

		atomic.AddInt64(&p.hits_local, 1)
		outputPath = filepath.Join(p.dir, "gocacheprog", fmt.Sprintf("o-%s", ie.OutputID))
	}

	fi, err := os.Stat(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			res.Miss = true
			return nil
		}
		return err
	}

	if !fi.Mode().IsRegular() {
		res.Miss = true
		return fmt.Errorf("not a regular file")
	}

	res.Size = fi.Size()
	res.TimeNanos = fi.ModTime().UnixNano()
	res.DiskPath = outputPath

	return nil
}

func (p *Process) handlePut(ctx context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	ctx = context.Background()

	actionID, objectID := fmt.Sprintf("%x", req.ActionID), fmt.Sprintf("%x", req.ObjectID)
	defer func() {
		if retErr != nil {
			log.Printf("put(action %s, obj %s, %v bytes): %v", actionID, objectID, req.BodySize, retErr)
		}
	}()

	var body io.Reader
	var copyBuf bytes.Buffer

	if req.Body == nil {
		body = bytes.NewReader(nil)
	} else {
		body = io.TeeReader(req.Body, &copyBuf)
	}

	diskPath, err := savefile(actionID, objectID, req.BodySize, body)
	if err != nil {
		return fmt.Errorf("unable to save to file: %w", err)
	}
	res.DiskPath = diskPath
	res.Size = req.BodySize

	start := time.Now()
	if req.BodySize >= p.minUploadSize { // 10kb
		p.gwg.Add(1)

		go func() {
			defer p.gwg.Done()
			err = p.cache.Put(ctx, actionID, objectID, req.BodySize, &copyBuf)
			if err != nil {
				atomic.AddInt64(&p.puts, 1)
			}
			if p.verbose {
				log.Printf("-> PUT %s took: %s\n", actionID, utils.FormatDuration(time.Since(start)))
			}
		}()
	}

	return nil
}
