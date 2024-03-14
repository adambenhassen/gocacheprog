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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/adambenhassen/gocacheprog/cachers"
	"github.com/adambenhassen/gocacheprog/utils"
	"github.com/adambenhassen/gocacheprog/wire"
)

type cacherCtxKey string

var (
	ErrUnknownCommand = errors.New("unknown command")
	requestIDKey      = cacherCtxKey("requestID")
)

type Process struct {
	local         cachers.Cache
	remote        cachers.Cache
	gwg           sync.WaitGroup
	verbose       bool
	dir           string
	minUploadSize int64

	gets        int64
	hits_local  int64
	hits_remote int64

	miss         int64
	puts         int64
	puts_errored int64
	puts_ignored int64
}

func NewCacheProc(local, remote cachers.Cache, verbose bool, minUploadSize int64) *Process {
	d, err := os.UserCacheDir()
	if err != nil {
		log.Fatal(err)
	}
	return &Process{
		local:         local,
		remote:        remote,
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
		log.Printf("gets %v, hits_local: %v, hits_remote: %v, misses_remote: %v, puts: %v, puts_errored %v, puts_ignored %v\n",
			p.gets, p.hits_local, p.hits_remote, p.miss, p.puts, p.puts_errored, p.puts_ignored)
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
		atomic.AddInt64(&p.gets, 1)
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

	_, outputPath, _, _, err := p.local.Get(ctx, actionID)
	if err != nil {
		outputID, _, size, reader, err := p.remote.Get(ctx, actionID)
		if err != nil {
			res.Miss = true
			// log.Printf("<- GET %s miss, took: %s\n", actionID, utils.FormatDuration(time.Since(start)))
			return nil
		}

		if outputID == "" {
			res.Miss = true
			return nil
		}

		res.OutputID, err = hex.DecodeString(outputID)
		if err != nil {
			res.Miss = true
			return fmt.Errorf("invalid OutputID: %w", err)
		}

		outputPath, err = p.local.Put(ctx, actionID, outputID, size, reader)
		if err != nil {
			res.Miss = true
			return fmt.Errorf("unable to save to file: %w", err)
		}

		if p.verbose {
			log.Printf("<- GET %s took: %s\n", actionID, utils.FormatDuration(time.Since(start)))
		}

		atomic.AddInt64(&p.hits_remote, 1)
	} else {
		atomic.AddInt64(&p.hits_local, 1)
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

func (p *Process) handlePut(_ context.Context, req *wire.Request, res *wire.Response) (retErr error) {
	ctx := context.Background()

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

	diskPath, err := p.local.Put(ctx, actionID, objectID, req.BodySize, body)
	if err != nil {
		return fmt.Errorf("unable to save to file: %w", err)
	}
	res.DiskPath = diskPath
	res.Size = req.BodySize

	start := time.Now()
	atomic.AddInt64(&p.puts, 1)
	if req.BodySize >= p.minUploadSize { // 15kb
		p.gwg.Add(1)

		go func() {
			defer p.gwg.Done()
			_, err = p.remote.Put(ctx, actionID, objectID, req.BodySize, &copyBuf)
			if err != nil {
				atomic.AddInt64(&p.puts_errored, 1)
				log.Print(err)
			} else if p.verbose {
				log.Printf("-> PUT %s took: %s, size: %v\n", actionID, utils.FormatDuration(time.Since(start)), req.BodySize)
			}
		}()
	} else {
		atomic.AddInt64(&p.puts_ignored, 1)
	}

	return nil
}
