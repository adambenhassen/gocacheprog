package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/adambenhassen/gocacheprog/cachers"
	"github.com/adambenhassen/gocacheprog/cachers/disk"
	"github.com/adambenhassen/gocacheprog/cachers/gcs"
	"github.com/adambenhassen/gocacheprog/cachers/http"
	"github.com/adambenhassen/gocacheprog/proc"
	"github.com/adambenhassen/gocacheprog/server"
	"github.com/adambenhassen/gocacheprog/utils"
)

// Common Settings
var (
	verbose  = flag.Bool("verbose", true, "Activates verbose output for detailed logging.")
	cachedir = flag.String("cache-dir", "", "Specifies the directory used for caching.")
)

// Client Configuration
var (
	httpServerURL = flag.String("http", "", "Provides the URL for the HTTP server. (When empty, GCS mode is enabled by default)")
	gcsBucket     = flag.String("bucket", "inigo-ci-cache", "Designates the target Google Cloud Storage bucket.")
	gcsCacheKey   = flag.String("cache-key", "main", "Sets a unique identifier for the cache, customizable to any name.")
	minUploadSize = flag.Int64("min-upload-size", 15_000, "Defines the minimum file size for uploads, measured in bytes.")
)

// Server Settings
var (
	serverMode = flag.Bool("server", false, "Toggles HTTP server mode operation.")
	secret     = flag.String("secret", "changeme", "Simple auth for server mode")
	listen     = flag.String("listen", ":80", "Determines the server's listening address.")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Server mode to server http
	if *serverMode {
		server.Run(ctx, *listen, *secret, *cachedir, *verbose)
		return
	}

	// Local disk
	local := disk.NewCache(ctx, *cachedir, *verbose)

	// Remote
	var remote cachers.Cache
	if *httpServerURL != "" {
		log.Println("HTTP Mode")
		remote = http.NewCache(*httpServerURL, *secret, *verbose)
	} else {
		log.Println("GCS Mode")
		remote = gcs.NewCache(ctx, *gcsBucket, *gcsCacheKey, *verbose)
	}

	// Start running
	start := time.Now()
	proc.NewCacheProc(local, remote, *verbose, *minUploadSize).Run(ctx)

	// Report run time
	if *verbose {
		log.Println("took", utils.FormatDuration(time.Since(start)))
	}
}
