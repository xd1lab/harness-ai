//go:build integration

package projection

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/platform/blob"
	"github.com/boltrope/boltrope/internal/platform/blob/blobtest"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// tfPayload marshals a TurnFinished payload for direct event insertion.
func tfPayload(t *testing.T, cost float64, usage llm.Usage) []byte {
	t.Helper()
	b, err := json.Marshal(domain.TurnFinished{TurnID: "tf", Reason: domain.Success, Usage: usage, CostUSD: cost, NumTurns: 1})
	if err != nil {
		t.Fatalf("marshal TurnFinished: %v", err)
	}
	return b
}

// TestSafeAdvance_DoesNotReadAboveXmin is the core gap-safe proof
// (NFR-REL-04, architecture §10.4). It commits one event in transaction T1, then
// inserts a SECOND event in a still-open transaction T2 (which holds back the
// snapshot xmin). A FetchBatch below xmin must return ONLY T1's event — never T2's
// uncommitted/at-xmin row — and the worker's cursor must not advance past it.
// After T2 commits, a second FetchBatch picks up T2's event: it was DELAYED, not
// SKIPPED.
func TestSafeAdvance_DoesNotReadAboveXmin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	// T1: insert + commit event seq=1 (cost 0.10).
	insertEventTx(ctx, t, h.conn, tenantID, sessionID, 1, string(domain.EventTurnFinished), tfPayload(t, 0.10, llm.Usage{InputTokens: 10}))

	// T2: on a SEPARATE connection, begin a transaction, insert event seq=2, and
	// DO NOT commit yet. The open T2 holds the snapshot xmin at (or below) T2's
	// xid, so a reader bounded by `transaction_id < xmin` cannot see seq=2.
	connT2 := h.newConn(t)
	txT2, err := connT2.Begin(ctx)
	if err != nil {
		t.Fatalf("begin T2: %v", err)
	}
	insertEventTx(ctx, t, txT2, tenantID, sessionID, 2, string(domain.EventTurnFinished), tfPayload(t, 0.99, llm.Usage{InputTokens: 50}))

	src := NewSource(h.conn)

	// First read below xmin: only T1's seq=1 is settled and readable.
	batch1, err := src.FetchBatch(ctx, Cursor{}, 100)
	if err != nil {
		t.Fatalf("FetchBatch (T2 open): %v", err)
	}
	if len(batch1) != 1 {
		t.Fatalf("FetchBatch returned %d rows while T2 is open, want 1 (only the committed T1 row, below xmin)", len(batch1))
	}
	if batch1[0].GlobalID == 0 {
		t.Fatalf("unexpected zero global_id")
	}
	cur, _ := Cursor{}.Advance([]rowCursor{batch1[0].rowCursor()})

	// The cursor must NOT have advanced to or past T2's transaction. Lag (also
	// xmin-bounded) reflects no further readable work yet.
	lag, err := src.Lag(ctx, cur)
	if err != nil {
		t.Fatalf("Lag (T2 open): %v", err)
	}
	if lag != 0 {
		t.Fatalf("lag = %d while T2 is open, want 0 (T2's row is above xmin, not yet lag)", lag)
	}

	// Now COMMIT T2: its transaction settles and the xmin advances past it.
	if err := txT2.Commit(ctx); err != nil {
		t.Fatalf("commit T2: %v", err)
	}

	// A second read from the advanced cursor now picks up T2's seq=2 — delayed,
	// not skipped. (Poll until visible: xmin advances as soon as no backend holds
	// an older snapshot; this loop tolerates a momentary lag.)
	var batch2 []EventRow
	if !waitFor(5*time.Second, func() bool {
		batch2, err = src.FetchBatch(ctx, cur, 100)
		return err == nil && len(batch2) == 1
	}) {
		t.Fatalf("after T2 commit, FetchBatch from advanced cursor returned %d rows (err=%v), want 1 (the once-blocked T2 row, delayed not skipped)", len(batch2), err)
	}
	if batch2[0].GlobalID <= batch1[0].GlobalID {
		t.Fatalf("T2 row global_id %d not after T1 row global_id %d", batch2[0].GlobalID, batch1[0].GlobalID)
	}
}

// TestCostRollup_Integration inserts cost-bearing events across TWO committed
// transactions, runs the worker to catch up, and asserts the per-session cost
// rollup equals the event sum and the cursor advanced to the last event
// (FR-OBS-02 input; architecture §11.6).
func TestCostRollup_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	// Txn A: seq 1, 2 (two finished turns).
	txA, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	insertEventTx(ctx, t, txA, tenantID, sessionID, 1, string(domain.EventTurnFinished), tfPayload(t, 0.10, llm.Usage{InputTokens: 100, OutputTokens: 20}))
	insertEventTx(ctx, t, txA, tenantID, sessionID, 2, string(domain.EventTurnStarted), mustJSON(t, domain.TurnStarted{TurnID: "x", Model: "m"})) // ignored by rollup
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// Txn B: seq 3 (another finished turn). A distinct transaction id.
	txB, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	insertEventTx(ctx, t, txB, tenantID, sessionID, 3, string(domain.EventTurnFinished), tfPayload(t, 0.25, llm.Usage{InputTokens: 200, OutputTokens: 40}))
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	metrics := &fakeMetrics{}
	r := NewRunner(Config{Subscription: "cost-rollup", BatchSize: 100}, NewSource(h.conn), WithMetrics(metrics))

	// Drive a catch-up. Both committed transactions are below xmin (no open txn).
	if err := r.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	got, ok := r.Totals()[SessionKey{TenantID: tenantID, SessionID: sessionID}]
	if !ok {
		t.Fatalf("no rollup for session %s", sessionID)
	}
	if !floatEq(got.CostUSD, 0.35) || got.Turns != 2 {
		t.Fatalf("rollup = %+v, want cost 0.35 / 2 turns (the two finished turns)", got)
	}
	wantUsage := llm.Usage{InputTokens: 300, OutputTokens: 60}
	if got.Usage != wantUsage {
		t.Fatalf("rollup usage = %+v, want %+v", got.Usage, wantUsage)
	}

	// Cursor persisted at the last event; lag zero.
	cur, err := NewSource(h.conn).LoadCursor(ctx, "cost-rollup")
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if cur.GlobalID == 0 {
		t.Fatalf("persisted cursor global_id = 0, want the last event's id")
	}
	if metrics.lastLag != 0 {
		t.Fatalf("published lag = %d, want 0 after full catch-up", metrics.lastLag)
	}
	if !floatEq(metrics.costTotal, 0.35) {
		t.Fatalf("cost counter total = %v, want 0.35", metrics.costTotal)
	}
}

// TestOrphanBlobSweep_Integration covers FR-STATE-05: the sweeper reclaims an
// orphan blob (older than the grace period with NO referencing event) from both
// the blob store and the blobs table, while NEVER deleting a referenced blob.
func TestOrphanBlobSweep_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	store := blobtest.NewFakeBlobStore()

	const (
		referencedRef = "sha256:referenced"
		orphanRefKey  = "sha256:orphan"
	)

	// Put bytes for both blobs in the store.
	put(t, store, tenantID, referencedRef)
	put(t, store, tenantID, orphanRefKey)

	// Referenced blob: a blobs row + a ToolResult event referencing it via the
	// events.blob_ref column (the composite FK, as the real AppendWithBlob does).
	// created_at is old so age is not the reason it survives — the REFERENCE is.
	insertBlobRow(t, h, tenantID, referencedRef, time.Now().Add(-2*time.Hour))
	insertBlobReferencingEvent(t, h, tenantID, sessionID, 1, referencedRef,
		mustJSON(t, domain.ToolResult{CallID: "c1", Result: "<<offloaded>>", Truncated: true, BlobRef: referencedRef}))

	// Orphan blob: a blobs row with NO referencing event, created 2h ago (past the
	// 1h grace) — its owning append transaction failed/abandoned.
	insertBlobRow(t, h, tenantID, orphanRefKey, time.Now().Add(-2*time.Hour))

	sweeper := NewSweeper(h.conn, store, WithGracePeriod(time.Hour))
	reclaimed, err := sweeper.Sweep(ctx, time.Now())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("reclaimed %d blobs, want 1 (only the orphan)", reclaimed)
	}

	// Orphan bytes + row gone.
	if _, err := store.Stat(ctx, blob.Ref{TenantID: tenantID, Key: orphanRefKey}); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("orphan bytes still present (Stat err=%v), want ErrNotFound", err)
	}
	if n := countBlobRow(t, h, tenantID, orphanRefKey); n != 0 {
		t.Fatalf("orphan blobs row count = %d, want 0", n)
	}

	// Referenced bytes + row survive.
	if _, err := store.Stat(ctx, blob.Ref{TenantID: tenantID, Key: referencedRef}); err != nil {
		t.Fatalf("referenced bytes were deleted (Stat err=%v), want present", err)
	}
	if n := countBlobRow(t, h, tenantID, referencedRef); n != 1 {
		t.Fatalf("referenced blobs row count = %d, want 1 (never delete a referenced blob)", n)
	}

	// A second sweep is a no-op (idempotent; nothing left to reclaim).
	again, err := sweeper.Sweep(ctx, time.Now())
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if again != 0 {
		t.Fatalf("second sweep reclaimed %d, want 0", again)
	}
}

// TestOrphanBlobSweep_RespectsGracePeriod asserts a fresh (within-grace) orphan is
// NOT reclaimed (the write-before-reference in-flight window is protected).
func TestOrphanBlobSweep_RespectsGracePeriod(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)
	_ = sessionID

	store := blobtest.NewFakeBlobStore()
	const freshRef = "sha256:fresh-inflight"
	put(t, store, tenantID, freshRef)
	insertBlobRow(t, h, tenantID, freshRef, time.Now()) // just written, no event yet

	sweeper := NewSweeper(h.conn, store, WithGracePeriod(time.Hour))
	reclaimed, err := sweeper.Sweep(ctx, time.Now())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed != 0 {
		t.Fatalf("reclaimed %d, want 0 (a fresh unreferenced blob is in-flight, not an orphan)", reclaimed)
	}
	if _, err := store.Stat(ctx, blob.Ref{TenantID: tenantID, Key: freshRef}); err != nil {
		t.Fatalf("fresh blob bytes were deleted within the grace period (err=%v)", err)
	}
}

// --- integration helpers ---

func mustJSON(t *testing.T, e domain.Event) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal %T: %v", e, err)
	}
	return b
}

func put(t *testing.T, store *blobtest.FakeBlobStore, tenantID, key string) {
	t.Helper()
	if _, err := store.Put(context.Background(), blob.Ref{TenantID: tenantID, Key: key}, "text/plain", readerOf("payload")); err != nil {
		t.Fatalf("blob Put %s: %v", key, err)
	}
}

// insertBlobReferencingEvent inserts a ToolResult event that references a blob
// via the events.blob_ref column (exercising the composite FK), so the orphan
// sweeper's NOT EXISTS (events.blob_ref = blobs.ref) correctly sees the blob as
// referenced. The matching blobs row MUST already exist (the FK requires it).
func insertBlobReferencingEvent(t *testing.T, h *pharness, tenantID, sessionID string, seq int64, blobRef string, payload []byte) {
	t.Helper()
	_, err := h.conn.Exec(context.Background(), `
		INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload, blob_ref)
		VALUES ($1, $2, $3, $4, $5, 1, $6, $7)`,
		tenantID, sessionID, seq, newUUID(t), string(domain.EventToolResult), payload, blobRef)
	if err != nil {
		t.Fatalf("insert blob-referencing event: %v", err)
	}
}

func insertBlobRow(t *testing.T, h *pharness, tenantID, ref string, createdAt time.Time) {
	t.Helper()
	_, err := h.conn.Exec(context.Background(), `
		INSERT INTO blobs (tenant_id, ref, media_type, size_bytes, storage_uri, created_at)
		VALUES ($1, $2, 'text/plain', 7, $3, $4)`,
		tenantID, ref, "file://"+tenantID+"/"+ref, createdAt)
	if err != nil {
		t.Fatalf("insert blob row %s: %v", ref, err)
	}
}

func readerOf(s string) io.Reader { return strings.NewReader(s) }

func countBlobRow(t *testing.T, h *pharness, tenantID, ref string) int {
	t.Helper()
	var n int
	if err := h.conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM blobs WHERE tenant_id = $1 AND ref = $2", tenantID, ref).Scan(&n); err != nil {
		t.Fatalf("count blobs row: %v", err)
	}
	return n
}
