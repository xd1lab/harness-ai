package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseConfig_RequiresDSN asserts the migrate command derives its DSN from
// the shared config loader and fails fast when the required postgres.dsn is
// missing (NFR-OPS-04), without attempting any connection.
func TestParseConfig_RequiresDSN(t *testing.T) {
	t.Run("missing DSN is a config error", func(t *testing.T) {
		// No flags, no env → required fields (incl. postgres.dsn) unset.
		_, err := parseConfig(nil, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres.dsn")
	})

	t.Run("DSN + version + addresses via flags validates", func(t *testing.T) {
		cfg, err := parseConfig([]string{
			"--postgres-dsn", "postgres://u@localhost:5432/db",
			"--postgres-version", "14",
			"--grpc-addr", ":0",
			"--http-addr", ":0",
			"--otlp-endpoint", "localhost:4317",
			"--blob-dir", "/tmp/b",
			"--model-gateway-endpoint", "localhost:9001",
		}, []string{})
		require.NoError(t, err)
		assert.Equal(t, "postgres://u@localhost:5432/db", cfg.Postgres.DSN)
	})
}

// TestRun_BadDSN_NonZeroExit asserts run returns a non-zero exit code (not 0)
// when the migration fails to connect — the DSN points at an unreachable host
// with a short context — so the compose gate's "migrate exits 0" ordering is
// only satisfied on actual success (NFR-OPS-01).
func TestRun_BadDSN_NonZeroExit(t *testing.T) {
	// A syntactically valid DSN whose host is unroutable. With a short deadline
	// the connect fails quickly and run must report a non-zero exit code.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so the connect attempt aborts fast

	var stderr strings.Builder
	code := run(ctx, []string{
		"--postgres-dsn", "postgres://u@127.0.0.1:1/db?connect_timeout=1",
		"--postgres-version", "14",
		"--grpc-addr", ":0",
		"--http-addr", ":0",
		"--otlp-endpoint", "localhost:4317",
		"--blob-dir", "/tmp/b",
		"--model-gateway-endpoint", "localhost:9001",
	}, []string{}, io.Discard, &stderr)

	assert.NotEqual(t, 0, code, "a failed migration must exit non-zero")
	assert.NotEmpty(t, stderr.String(), "the failure reason should be reported on stderr")
}

// TestRun_BadConfig_NonZeroExit asserts a config validation failure (missing
// required field) returns a non-zero exit code and writes the error to stderr,
// without panicking.
func TestRun_BadConfig_NonZeroExit(t *testing.T) {
	var stderr strings.Builder
	code := run(context.Background(), []string{}, []string{}, io.Discard, &stderr)
	assert.NotEqual(t, 0, code)
	assert.Contains(t, stderr.String(), "configuration")
}
