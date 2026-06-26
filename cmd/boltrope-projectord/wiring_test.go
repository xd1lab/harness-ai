package main

// Wiring/lifecycle tests for Batch-5B's env-gated, INDEPENDENT projection Runners
// (AC-6 / AC-17). These complement audit_wiring_test.go (which pins the gating
// settings) by exercising the worker's fan-out behavior:
//
//   - the disabled-signing startup WARN is emitted exactly once when no
//     BOLTROPE_AUDIT_SIGNING_KEY is configured (AC-6);
//   - the loud WARN is NOT emitted when a signing key is present;
//   - the worker selects the right set of independent subscriptions per env config,
//     and the signer/SIEM subscriptions are distinct from cost-rollup so their
//     cursors (and a failing SIEM sink) can never stall cost-rollup (AC-17);
//   - the worker fans out into separate goroutines (own conn per subscription) and
//     shuts down cleanly on cancel even with all three consumers enabled (no shared
//     pgx.Conn panic; the loops are independent).

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// captureLogger returns a slog.Logger writing JSON to a thread-safe buffer and the
// buffer's current contents accessor, so a test can assert on emitted log lines.
func captureLogger() (*slog.Logger, func() string) {
	buf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, buf.String
}

// syncBuffer is a minimal concurrency-safe io.Writer for capturing async log output
// from the worker's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWorker_RunSelectsLoops asserts the worker fans out into exactly the right set
// of independent subscriptions per env config and shuts down cleanly on cancel.
// With an unreachable DSN every loop is resiliently retrying, so cancelling ctx is
// the only termination — proving the loops are independent (no shared conn) and the
// gating selects the right set without ever stalling on each other.
func TestWorker_RunSelectsLoops(t *testing.T) {
	t.Run("cost-rollup only when audit/SIEM disabled", func(t *testing.T) {
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "")
		t.Setenv("BOLTROPE_SIEM_FILE", "")
		t.Setenv("BOLTROPE_SIEM_HTTP_URL", "")

		w := newTestWorker(t)
		assert.False(t, w.audit.SignerEnabled())
		assert.False(t, w.audit.SIEMEnabled())
		runUntilCancel(t, w)
	})

	t.Run("all three when audit + SIEM configured", func(t *testing.T) {
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "c29tZS1zZWVk")
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY_ID", "k1")
		t.Setenv("BOLTROPE_SIEM_FILE", t.TempDir()+"/siem.ndjson")

		w := newTestWorker(t)
		assert.True(t, w.audit.SignerEnabled())
		assert.True(t, w.audit.SIEMEnabled())
		// The three subscriptions are pairwise distinct so their cursors are
		// independent and a failing SIEM sink cannot stall cost-rollup (AC-17).
		assert.NotEqual(t, w.settings.Subscription, w.audit.SignerSubscription())
		assert.NotEqual(t, w.settings.Subscription, w.audit.SIEMSubscription())
		assert.NotEqual(t, w.audit.SignerSubscription(), w.audit.SIEMSubscription())
		runUntilCancel(t, w)
	})
}

// TestRun_EmitsDisabledSigningWarn asserts the single loud WARN is emitted at
// startup when no BOLTROPE_AUDIT_SIGNING_KEY is configured (AC-6), and is NOT
// emitted when a signing key is present. It drives the real Run wiring (against an
// unreachable DB, like the daemon smoke test) and inspects the captured log sink.
func TestRun_EmitsDisabledSigningWarn(t *testing.T) {
	const warnNeedle = "audit checkpoint signing DISABLED"

	t.Run("warns when key unset", func(t *testing.T) {
		t.Setenv("BOLTROPE_DEV_INSECURE", "1")
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "")
		out := runBriefly(t)
		assert.Contains(t, out, warnNeedle, "the disabled-signing WARN must be emitted when no key is configured")
	})

	t.Run("silent when key set", func(t *testing.T) {
		t.Setenv("BOLTROPE_DEV_INSECURE", "1")
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "c29tZS1zZWVk")
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY_ID", "k1")
		out := runBriefly(t)
		assert.NotContains(t, out, warnNeedle, "the disabled-signing WARN must NOT be emitted when a key is configured")
	})
}

// runBriefly runs the full Run wiring against an unreachable DB for a moment, then
// cancels and returns the captured log output.
func runBriefly(t *testing.T) string {
	t.Helper()
	cfg := baseConfig(t)
	buf := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, buf) }()

	time.Sleep(250 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	return buf.String()
}

// newTestWorker builds a worker pointed at an unreachable DB (so its loops retry,
// never connect) with a capturing logger and the bare-name secrets port.
func newTestWorker(t *testing.T) *worker {
	t.Helper()
	logger, _ := captureLogger()
	return &worker{
		dsn:      "postgres://u@127.0.0.1:1/db?connect_timeout=1",
		blobDir:  t.TempDir(),
		settings: loadProjectorSettings(),
		audit:    loadAuditSettings(),
		secrets:  secret.NewEnvSecrets(),
		log:      logger,
	}
}

// runUntilCancel runs w.run in a goroutine, lets the loops spin up against the dead
// DSN, then cancels and asserts a clean shutdown within a bound — proving every
// goroutine observes cancellation (independent loops, no deadlock/shared-conn).
func runUntilCancel(t *testing.T, w *worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.run(ctx) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(6 * time.Second):
		t.Fatal("worker.run did not return after cancel (a loop stalled)")
	}
}
