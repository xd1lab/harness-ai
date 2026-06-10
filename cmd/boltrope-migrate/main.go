package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// main is the thin entrypoint: it derives a signal-cancellable context so an
// operator can abort a long migration with Ctrl-C, delegates to [run] (the
// testable core), and exits with run's status code (DOD-12: exit 0 on success).
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Environ(), os.Stdout, os.Stderr)
	stop() // release the signal handler before exiting (os.Exit skips defers).
	os.Exit(code)
}
