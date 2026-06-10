package projection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/platform/blob/blobtest"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
)

// sweepConn is a hand-built [Conn] for the sweeper: it serves the orphan scan
// (recording the cutoff/batch bind params for assertions) and the guarded
// metadata-row delete. The pending orphan rows are served once (popped), the
// way the real scan stops returning a row after its blob is reclaimed.
type sweepConn struct {
	mu        sync.Mutex
	orphans   [][]any // pending scan rows: (tenant_id, ref)
	scanErr   error   // fails the orphan scan
	deleteErr error   // fails the row delete
	deleteTag string  // CommandTag for the row delete; default "DELETE 1"
	cutoffs   []string
	batches   []int
	deleted   []string // "tenant/ref" per row-delete attempt
}

func (c *sweepConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !contains(sql, "FROM blobs") {
		return nil, fmt.Errorf("sweepConn: unexpected Query: %s", sql)
	}
	if c.scanErr != nil {
		return nil, c.scanErr
	}
	c.cutoffs = append(c.cutoffs, args[0].(string))
	c.batches = append(c.batches, args[1].(int))
	rows := &fakeRows{cols: c.orphans}
	c.orphans = nil // pop: a reclaimed blob is not a candidate again
	return rows, nil
}

func (c *sweepConn) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	return fakeRow{err: fmt.Errorf("sweepConn: unexpected QueryRow: %s", sql)}
}

func (c *sweepConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !contains(sql, "DELETE FROM blobs") {
		return pgconn.CommandTag{}, fmt.Errorf("sweepConn: unexpected Exec: %s", sql)
	}
	if c.deleteErr != nil {
		return pgconn.CommandTag{}, c.deleteErr
	}
	c.deleted = append(c.deleted, args[0].(string)+"/"+args[1].(string))
	tag := c.deleteTag
	if tag == "" {
		tag = "DELETE 1"
	}
	return pgconn.NewCommandTag(tag), nil
}

// deletedCount returns the number of row-delete attempts under the lock (the
// Run goroutine sweeps concurrently in the tick test).
func (c *sweepConn) deletedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deleted)
}

// failStore wraps the fake blob store with a failing Delete to drive the
// sweeper's bytes-delete error path (the store is hit BEFORE the row delete).
type failStore struct {
	*blobtest.FakeBlobStore
	deleteErr error
}

func (f *failStore) Delete(ctx context.Context, ref blob.Ref) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.FakeBlobStore.Delete(ctx, ref)
}

// putBlob seeds bytes for (tenant, key) in the fake store.
func putBlob(t *testing.T, store *blobtest.FakeBlobStore, tenant, key string) {
	t.Helper()
	if _, err := store.Put(context.Background(), blob.Ref{TenantID: tenant, Key: key}, "text/plain", strings.NewReader("payload")); err != nil {
		t.Fatalf("seed blob %s: %v", key, err)
	}
}

var _ Conn = (*sweepConn)(nil)

// TestSweeper_Sweep_ReclaimsOrphans covers the happy path: each candidate's
// bytes are deleted from the store, the metadata row is removed, and the scan
// receives the injected-clock cutoff (now-grace) and the configured batch cap.
func TestSweeper_Sweep_ReclaimsOrphans(t *testing.T) {
	fc := clocktest.NewFake(time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC))
	store := blobtest.NewFakeBlobStore()
	putBlob(t, store, "tenant-a", "sha256:orphan-1")
	putBlob(t, store, "tenant-b", "sha256:orphan-2")
	conn := &sweepConn{orphans: [][]any{
		{"tenant-a", "sha256:orphan-1"},
		{"tenant-b", "sha256:orphan-2"},
	}}

	sw := NewSweeper(conn, store, WithGracePeriod(30*time.Minute), WithSweepBatch(7))
	reclaimed, err := sw.Sweep(context.Background(), fc.Now())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed != 2 {
		t.Fatalf("reclaimed = %d, want 2", reclaimed)
	}

	// The grace cutoff is dated by the caller's clock: now - grace, in the
	// RFC3339Nano text form the protocol-robust bind uses.
	wantCutoff := fc.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano)
	if len(conn.cutoffs) != 1 || conn.cutoffs[0] != wantCutoff {
		t.Fatalf("scan cutoffs = %v, want [%s]", conn.cutoffs, wantCutoff)
	}
	if len(conn.batches) != 1 || conn.batches[0] != 7 {
		t.Fatalf("scan batch caps = %v, want [7] (WithSweepBatch)", conn.batches)
	}

	// Bytes gone from the store for both orphans.
	for _, ref := range []blob.Ref{
		{TenantID: "tenant-a", Key: "sha256:orphan-1"},
		{TenantID: "tenant-b", Key: "sha256:orphan-2"},
	} {
		if _, err := store.Stat(context.Background(), ref); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("blob %s bytes still present (Stat err=%v), want ErrNotFound", ref.Key, err)
		}
	}

	// A second pass finds nothing and reclaims nothing (idempotent).
	again, err := sw.Sweep(context.Background(), fc.Now())
	if err != nil || again != 0 {
		t.Fatalf("second Sweep = (%d, %v), want (0, nil)", again, err)
	}
}

// TestSweeper_Sweep_RecheckRaceDoesNotCount asserts the FR-STATE-05 race guard:
// when the guarded row delete affects 0 rows (a referencing event committed
// between scan and delete), the blob is NOT counted as reclaimed.
func TestSweeper_Sweep_RecheckRaceDoesNotCount(t *testing.T) {
	store := blobtest.NewFakeBlobStore()
	putBlob(t, store, "t", "sha256:racy")
	conn := &sweepConn{orphans: [][]any{{"t", "sha256:racy"}}, deleteTag: "DELETE 0"}

	sw := NewSweeper(conn, store)
	reclaimed, err := sw.Sweep(context.Background(), time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed != 0 {
		t.Fatalf("reclaimed = %d, want 0 (re-check guarded the delete)", reclaimed)
	}
}

// TestSweeper_Sweep_ErrorPaths covers the stop-on-first-hard-error contract for
// each failure point: the orphan scan, a malformed scan row, the store byte
// delete, the metadata-row delete, and a cancelled context mid-pass.
func TestSweeper_Sweep_ErrorPaths(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	t.Run("scan query fails", func(t *testing.T) {
		sw := NewSweeper(&sweepConn{scanErr: errors.New("scan blew up")}, blobtest.NewFakeBlobStore())
		if _, err := sw.Sweep(context.Background(), now); err == nil || !strings.Contains(err.Error(), "scanning orphans") {
			t.Fatalf("Sweep = %v, want scanning-orphans error", err)
		}
	})

	t.Run("malformed scan row fails", func(t *testing.T) {
		// A one-column row cannot scan into (tenant_id, ref).
		sw := NewSweeper(&sweepConn{orphans: [][]any{{"only-one-column"}}}, blobtest.NewFakeBlobStore())
		if _, err := sw.Sweep(context.Background(), now); err == nil || !strings.Contains(err.Error(), "scanning orphan row") {
			t.Fatalf("Sweep = %v, want scanning-orphan-row error", err)
		}
	})

	t.Run("rows iteration fails", func(t *testing.T) {
		boom := errors.New("iteration blew up")
		sw := NewSweeper(&stubConn{rows: &errRows{fakeRows: &fakeRows{}, iterErr: boom}}, blobtest.NewFakeBlobStore())
		if _, err := sw.Sweep(context.Background(), now); err == nil || !strings.Contains(err.Error(), "iterating orphans") {
			t.Fatalf("Sweep = %v, want iterating-orphans error", err)
		}
	})

	t.Run("store byte delete fails before the row delete", func(t *testing.T) {
		conn := &sweepConn{orphans: [][]any{{"t", "sha256:x"}}}
		store := &failStore{FakeBlobStore: blobtest.NewFakeBlobStore(), deleteErr: errors.New("backend down")}
		sw := NewSweeper(conn, store)
		reclaimed, err := sw.Sweep(context.Background(), now)
		if err == nil || !strings.Contains(err.Error(), "deleting blob bytes") {
			t.Fatalf("Sweep = %v, want deleting-blob-bytes error", err)
		}
		if reclaimed != 0 || conn.deletedCount() != 0 {
			t.Fatalf("reclaimed=%d rowDeletes=%d after a failed byte delete, want 0/0 (bytes first, row never reached)", reclaimed, conn.deletedCount())
		}
	})

	t.Run("row delete fails", func(t *testing.T) {
		conn := &sweepConn{orphans: [][]any{{"t", "sha256:x"}}, deleteErr: errors.New("row delete blew up")}
		store := blobtest.NewFakeBlobStore()
		putBlob(t, store, "t", "sha256:x")
		sw := NewSweeper(conn, store)
		if _, err := sw.Sweep(context.Background(), now); err == nil || !strings.Contains(err.Error(), "deleting blob row") {
			t.Fatalf("Sweep = %v, want deleting-blob-row error", err)
		}
	})

	t.Run("cancelled context stops the pass", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		conn := &sweepConn{orphans: [][]any{{"t", "sha256:x"}}}
		sw := NewSweeper(conn, blobtest.NewFakeBlobStore())
		reclaimed, err := sw.Sweep(ctx, now)
		if !errors.Is(err, context.Canceled) || reclaimed != 0 {
			t.Fatalf("Sweep on cancelled ctx = (%d, %v), want (0, context.Canceled)", reclaimed, err)
		}
	})
}

// TestSweeperOptions_GuardClauses asserts non-positive overrides are ignored
// and the documented defaults survive.
func TestSweeperOptions_GuardClauses(t *testing.T) {
	sw := NewSweeper(&sweepConn{}, blobtest.NewFakeBlobStore(), WithGracePeriod(-time.Minute), WithSweepBatch(0))
	if sw.grace != DefaultSweepGracePeriod {
		t.Fatalf("grace = %s after WithGracePeriod(-1m), want the default %s", sw.grace, DefaultSweepGracePeriod)
	}
	if sw.batch != 256 {
		t.Fatalf("batch = %d after WithSweepBatch(0), want the default 256", sw.batch)
	}
}

// TestRunner_RunSweep covers the runner's sweep wrapper directly: a sweep error
// is logged and swallowed (next tick retries), and a reclaiming sweep logs the
// count.
func TestRunner_RunSweep(t *testing.T) {
	fixedNow := func() time.Time { return time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC) }

	t.Run("error is logged and swallowed", func(t *testing.T) {
		var buf bytes.Buffer
		sw := NewSweeper(&sweepConn{scanErr: errors.New("scan blew up")}, blobtest.NewFakeBlobStore())
		r := NewRunner(Config{Subscription: "s"}, NewSource(&fakeConn{}),
			WithSweeper(sw), WithNow(fixedNow),
			WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		r.runSweep(context.Background())
		if !strings.Contains(buf.String(), "orphan-blob sweep failed") {
			t.Fatalf("sweep failure missing from log:\n%s", buf.String())
		}
	})

	t.Run("reclaim is logged", func(t *testing.T) {
		var buf bytes.Buffer
		store := blobtest.NewFakeBlobStore()
		putBlob(t, store, "t", "sha256:orphan")
		sw := NewSweeper(&sweepConn{orphans: [][]any{{"t", "sha256:orphan"}}}, store)
		r := NewRunner(Config{Subscription: "s"}, NewSource(&fakeConn{}),
			WithSweeper(sw), WithNow(fixedNow),
			WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		r.runSweep(context.Background())
		if !strings.Contains(buf.String(), "orphan-blob sweep reclaimed blobs") {
			t.Fatalf("reclaim log line missing:\n%s", buf.String())
		}
	})
}

// TestRunner_Run_SweepTick proves the Run loop's sweep wiring end to end: with
// SweepInterval set and a sweeper attached, the ticker fires the sweep, dated
// by the injected clock (WithNow + clocktest), and the orphan disappears from
// both the store and (via the conn) the metadata table.
func TestRunner_Run_SweepTick(t *testing.T) {
	fc := clocktest.NewFake(time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC))
	store := blobtest.NewFakeBlobStore()
	putBlob(t, store, "tenant-a", "sha256:orphan")
	sconn := &sweepConn{orphans: [][]any{{"tenant-a", "sha256:orphan"}}}
	sw := NewSweeper(sconn, store, WithGracePeriod(time.Hour), WithSweepBatch(11))

	r := NewRunner(
		// Poll far in the future so only the sweep ticker drives the loop.
		Config{Subscription: "cost-rollup", PollInterval: time.Hour, SweepInterval: 10 * time.Millisecond},
		NewSource(&fakeConn{}),
		WithSweeper(sw),
		WithNow(fc.Now),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, nil) }()

	waitUntil(t, 5*time.Second, "sweep tick to reclaim the orphan", func() bool {
		return sconn.deletedCount() == 1
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	wantCutoff := fc.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if len(sconn.cutoffs) == 0 || sconn.cutoffs[0] != wantCutoff {
		t.Fatalf("sweep cutoffs = %v, want first %s (injected clock minus grace)", sconn.cutoffs, wantCutoff)
	}
	if sconn.batches[0] != 11 {
		t.Fatalf("sweep batch cap = %d, want 11", sconn.batches[0])
	}
	if _, err := store.Stat(context.Background(), blob.Ref{TenantID: "tenant-a", Key: "sha256:orphan"}); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("orphan bytes still present after the swept tick (Stat err=%v)", err)
	}
}
