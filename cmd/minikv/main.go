// Command minikv runs a Redis-compatible in-memory key-value server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omergazi34/minikv/internal/aof"
	"github.com/omergazi34/minikv/internal/server"
)

func main() { os.Exit(run()) }

// run returns the process exit code, so deferred cleanup always runs before os.Exit.
func run() int {
	addr := flag.String("addr", ":6380", "listen address")
	aofPath := flag.String("aof", "", "append-only file path (empty disables persistence)")
	fsync := flag.String("fsync", "everysec", "AOF fsync policy: always, everysec or no")
	rewriteMin := flag.Int64("aof-rewrite-min-size", 64<<20, "auto-rewrite the AOF once it exceeds this many bytes and has doubled (0 disables)")
	shards := flag.Int("shards", 0, "keyspace shards (default 4×GOMAXPROCS)")
	maxClients := flag.Int("maxclients", 10_000, "maximum simultaneous connections")
	verbose := flag.Bool("v", false, "debug logging")
	version := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *version {
		fmt.Println("minikv", server.Version)
		return 0
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	policy, err := aof.ParsePolicy(*fsync)
	if err != nil {
		logger.Error("invalid flag", "err", err)
		return 2
	}
	srv, err := server.New(server.Config{
		Addr:           *addr,
		Shards:         *shards,
		AOFPath:        *aofPath,
		Fsync:          policy,
		RewriteMinSize: *rewriteMin,
		MaxClients:     *maxClients,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("startup failed", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	select {
	case err := <-errc:
		if err != nil {
			logger.Error("server stopped", "err", err)
			return 1
		}
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown", "err", err)
			return 1
		}
		logger.Info("bye")
	}
	return 0
}
