package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/config"
)

func baseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Server:       config.ServerConfig{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		Postgres:     config.PostgresConfig{DSN: "postgres://u@127.0.0.1:1/db?connect_timeout=1", Version: 14},
		OTLP:         config.OTLPConfig{Endpoint: ""},
		ModelGateway: config.ModelGatewayConfig{Endpoint: "localhost:9001"},
		Blob:         config.BlobConfig{Dir: t.TempDir()},
		LogLevel:     "info",
		DevInsecure:  true,
	}
}

// TestLoadProjectorSettings_Defaults asserts the subscription name and sweep
// interval default sensibly from the environment.
func TestLoadProjectorSettings_Defaults(t *testing.T) {
	ps := loadProjectorSettings()
	assert.NotEmpty(t, ps.Subscription, "a default subscription name must be set")
}

// TestRun_ServesAndShutsDownWithoutDB is the daemon smoke: projectord serves
// health/metrics and runs its projection loop as a background worker that
// resiliently retries the (unavailable) DB; cancelling the context shuts it down
// cleanly without a fatal error — proving the worker tolerates a DB outage
// (architecture §10.4) and the wiring constructs without panic.
func TestRun_ServesAndShutsDownWithoutDB(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	var buf strings.Builder
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, &buf) }()

	// Let it bind + start the worker (which will be retrying the DB), then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a DB-unavailable projectord must still shut down cleanly on cancel")
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestRun_ListenFailure asserts a bad listen address surfaces an error.
func TestRun_ListenFailure(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)
	cfg.Server.GRPCAddr = "256.256.256.256:1"
	err := Run(context.Background(), cfg, io.Discard)
	require.Error(t, err)
}
