package main

import (
	"context"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/storage"

	"github.com/adambenhassen/gocacheprog/cachers/gcs"
	"github.com/adambenhassen/gocacheprog/proc"
	"github.com/adambenhassen/gocacheprog/utils"
)

var (
	verbose  = flag.Bool("verbose", false, "be verbose")
	bucket   = flag.String("bucket", "inigo-ci-cache", "bucket")
	cacheKey = flag.String("cacheKey", "", "cacheKey")
)

func main() {
	start := time.Now()
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	cache := gcs.NewCache(client, *bucket, *cacheKey, *verbose)
	proc.NewCacheProc(cache, *verbose).Run(ctx)

	if *verbose {
		log.Println("took", utils.FormatDuration(time.Since(start)))
	}
}
