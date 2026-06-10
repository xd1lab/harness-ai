package main

import (
	"context"
	"fmt"
	"os"

	"github.com/boltrope/boltrope/internal/platform/config"
)

// main is the thin entrypoint: load+validate config (fail-fast, NFR-OPS-04) then
// hand off to [Run], which constructs the gateway and serves until a signal.
// Config/credential/listener failures exit non-zero; a clean shutdown exits 0.
func main() {
	cfg, err := config.Load(config.Options{Args: os.Args[1:], Environ: os.Environ()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err := Run(context.Background(), cfg, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "boltrope-modelgwd: %v\n", err)
		os.Exit(1)
	}
}
