//go:build integration

package eventstore

// RED (test-first) integration tests for Batch-5A VERIFY: the read-only,
// RLS-scoped Store.VerifyChainIntegrity(ctx, sessionID, fromSeq, toSeq) that
// re-reads the events, recomputes content_hash from each stored payload and
// chain_hash from the running chain, and compares to the stored values
// (AC-7, AC-8, AC-9).
//
// It references symbols that do NOT exist yet — domain.ChainVerification and
// Store.VerifyChainIntegrity — so the package does NOT compile; that absence is
// the RED proof. The tamper writes go through the OWNER connection (bypassing
// RLS) to simulate an attacker mutating the durable row out from under the app.

import (
	"context"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestVerifyChainIntegrity_UntamperedIsValid covers AC-8(a): a clean seeded
// session verifies Valid=true, FirstBadSeq=0, Checked=N over the whole stream.
func TestVerifyChainIntegrity_UntamperedIsValid(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 8) // seq 1..8

	res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0) // 0,0 -> whole stream
	if err != nil {
		t.Fatalf("VerifyChainIntegrity: %v", err)
	}
	if !res.Valid {
		t.Fatalf("untampered session verified Valid=false (FirstBadSeq=%d, Reason=%q)", res.FirstBadSeq, res.Reason)
	}
	if res.FirstBadSeq != 0 {
		t.Fatalf("FirstBadSeq = %d, want 0 on a clean range", res.FirstBadSeq)
	}
	if res.Checked != 8 {
		t.Fatalf("Checked = %d, want 8 (all chained events)", res.Checked)
	}
}

// TestVerifyChainIntegrity_DetectsTamperedPayload covers AC-8(b): directly
// UPDATE one event's payload JSONB (via the owner connection, payload only — the
// stored content_hash is left stale), then verify reports Valid=false with
// FirstBadSeq = that seq and a CONTENT-mismatch reason. The recompute uses the
// STORED payload bytes, so the tampered row no longer re-hashes to its stored
// content_hash.
func TestVerifyChainIntegrity_DetectsTamperedPayload(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 6)

	// Tamper seq 4's payload directly in SQL (owner bypasses RLS). The stored
	// content_hash/chain_hash are NOT updated, so the recompute will diverge.
	owner := h.ownerConn(t)
	if _, err := owner.Exec(context.Background(),
		`UPDATE events SET payload = '{"TurnID":"TAMPERED","Model":"evil"}'::jsonb WHERE session_id = $1 AND seq = 4`,
		sessionID); err != nil {
		t.Fatalf("tamper payload: %v", err)
	}

	res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity: %v", err)
	}
	if res.Valid {
		t.Fatal("verify returned Valid=true for a tampered payload (tamper-evidence failed)")
	}
	if res.FirstBadSeq != 4 {
		t.Fatalf("FirstBadSeq = %d, want 4 (the tampered seq)", res.FirstBadSeq)
	}
	if !strings.Contains(strings.ToLower(res.Reason), "content") {
		t.Fatalf("Reason = %q, want a content-hash-mismatch classification", res.Reason)
	}
}

// TestVerifyChainIntegrity_DetectsBrokenLink covers AC-8(c): directly UPDATE one
// event's chain_hash to a wrong value (the payload is intact) and assert verify
// returns Valid=false at that seq with a BROKEN-LINK (chain-hash-mismatch)
// reason — distinct from a content mismatch.
func TestVerifyChainIntegrity_DetectsBrokenLink(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 6)

	// Rewrite seq 3's chain_hash to 32 wrong bytes; the payload (and thus the
	// recomputed content_hash) is untouched, so this is a pure broken link.
	owner := h.ownerConn(t)
	if _, err := owner.Exec(context.Background(),
		`UPDATE events SET chain_hash = decode(repeat('ab',32),'hex') WHERE session_id = $1 AND seq = 3`,
		sessionID); err != nil {
		t.Fatalf("tamper chain_hash: %v", err)
	}

	res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity: %v", err)
	}
	if res.Valid {
		t.Fatal("verify returned Valid=true for a broken chain link")
	}
	if res.FirstBadSeq != 3 {
		t.Fatalf("FirstBadSeq = %d, want 3 (the broken-link seq)", res.FirstBadSeq)
	}
	reason := strings.ToLower(res.Reason)
	if !strings.Contains(reason, "link") && !strings.Contains(reason, "chain") {
		t.Fatalf("Reason = %q, want a broken-link / chain-hash-mismatch classification", res.Reason)
	}
}

// TestVerifyChainIntegrity_NullPrefixCompat covers AC-9: a pre-0009 NULL-hash row
// (owner-inserted without hashes) followed by chained rows verifies Valid=true
// over the chained suffix; the NULL prefix is skipped, not reported as tampered,
// and Checked counts only the chained events.
func TestVerifyChainIntegrity_NullPrefixCompat(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// Owner-insert a seq-1 event WITHOUT content_hash/chain_hash (the pre-0009
	// shape), and bump head_seq to 1 so the app can append seq 2+ on top.
	owner := h.ownerConn(t)
	if _, err := owner.Exec(context.Background(),
		`INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload, actor)
		 VALUES ($1, $2, 1, $3, 'TurnStarted', 1, '{"TurnID":"legacy","Model":"old"}'::jsonb, 'system')`,
		tenantID, sessionID, newUUID(t)); err != nil {
		t.Fatalf("owner-insert legacy NULL-hash row: %v", err)
	}
	if _, err := owner.Exec(context.Background(),
		"UPDATE sessions SET head_seq = 1 WHERE id = $1", sessionID); err != nil {
		t.Fatalf("bump head_seq: %v", err)
	}

	// Now append 3 chained events on top (seq 2,3,4) via the app store.
	for i := int64(1); i <= 3; i++ {
		if _, err := appendOne(ctx, h.store, sessionID, i, 0, newUUID(t), "chained"); err != nil {
			t.Fatalf("chained append %d: %v", i, err)
		}
	}

	// Load still succeeds with the NULL-hash prefix (scanEnvelopes scans the
	// hashes as nullable []byte, leaving them nil on the legacy row).
	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load over NULL-prefix session: %v", err)
	}
	if len(loaded) != 4 {
		t.Fatalf("loaded %d events, want 4", len(loaded))
	}
	if loaded[0].ContentHash != nil || loaded[0].ChainHash != nil {
		t.Fatalf("legacy seq 1 should load with nil hashes, got content=%x chain=%x", loaded[0].ContentHash, loaded[0].ChainHash)
	}

	// Verify treats the NULL prefix gracefully and is Valid over the chained suffix.
	res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity over NULL-prefix: %v", err)
	}
	if !res.Valid {
		t.Fatalf("NULL-prefix session verified Valid=false (FirstBadSeq=%d, Reason=%q)", res.FirstBadSeq, res.Reason)
	}
	if res.Checked != 3 {
		t.Fatalf("Checked = %d, want 3 (only the chained suffix; the NULL prefix is skipped)", res.Checked)
	}
}

// TestVerifyChainIntegrity_IsSideEffectFree covers AC-7's read-only guarantee:
// verify creates no rows (no session, no event) and never mutates.
func TestVerifyChainIntegrity_IsSideEffectFree(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 5)

	owner := h.ownerConn(t)
	var sBefore, eBefore int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sBefore); err != nil {
		t.Fatalf("count sessions before: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eBefore); err != nil {
		t.Fatalf("count events before: %v", err)
	}

	if _, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0); err != nil {
		t.Fatalf("VerifyChainIntegrity: %v", err)
	}

	var sAfter, eAfter int
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM sessions").Scan(&sAfter); err != nil {
		t.Fatalf("count sessions after: %v", err)
	}
	if err := owner.QueryRow(context.Background(), "SELECT COUNT(*) FROM events").Scan(&eAfter); err != nil {
		t.Fatalf("count events after: %v", err)
	}
	if sAfter != sBefore || eAfter != eBefore {
		t.Fatalf("verify mutated row counts: sessions %d->%d, events %d->%d (must be read-only)", sBefore, sAfter, eBefore, eAfter)
	}
}

// TestVerifyChainIntegrity_CrossTenantInvisible covers AC-7's RLS scoping: tenant
// B verifying tenant A's session sees no chained events (RLS hides the rows), so
// it cannot probe another tenant's integrity. It returns Checked=0, never an
// error that leaks existence.
func TestVerifyChainIntegrity_CrossTenantInvisible(t *testing.T) {
	h := newHarness(t)
	tenantA, sessionA := h.seedTenantAndSession(t)
	ctxA := tenantCtx(tenantA)
	seedNEvents(ctxA, t, h.store, sessionA, 5)

	tenantB := newUUID(t)
	ctxB := tenantCtx(tenantB)
	if err := h.store.CreateTenant(ctxB, tenantB, "B"); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}

	res, err := h.store.VerifyChainIntegrity(ctxB, sessionA, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity as tenant B: unexpected error %v", err)
	}
	if res.Checked != 0 {
		t.Fatalf("tenant B verified %d of tenant A's events, want 0 (RLS-scoped read)", res.Checked)
	}
}

// errUnusedAppendInput keeps the app import referenced even if a future edit
// trims the only app.AppendInput use; harmless and removed when the suite lands
// (kept so the RED file's imports are not flagged unused at compile time).
var _ = app.AppendInput{}

// _ keeps the domain import referenced (ChainVerification is the type returned by
// Store.VerifyChainIntegrity asserted above; the explicit reference guards the
// import against an unused-import error in the integration build).
var _ = domain.ChainVerification{}
