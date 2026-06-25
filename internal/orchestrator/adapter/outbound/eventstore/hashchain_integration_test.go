//go:build integration

package eventstore

// RED (test-first) integration tests for Batch-5A: the append path computes a
// per-event content_hash over the EXACT stored payload bytes and folds a
// per-session chain_hash, persisting both on the events row and advancing
// sessions.chain_head — all inside the EXISTING append transaction WITHOUT
// altering the hot-path (optimistic gate, idempotency short-circuit, lease
// fencing, RLS) (AC-3, AC-4, AC-5, AC-6, AC-11).
//
// These reference symbols that do NOT exist yet:
//   - domain.EventEnvelope.ContentHash / .ChainHash (additive fields)
//   - domain.ContentHash / domain.GenesisChainHash / domain.ChainHash / domain.MarshalEventPayload
//   - the events.content_hash / events.chain_hash columns + sessions.chain_head
//     (migration 0009)
// so the package does NOT compile — the RED proof.

import (
	"bytes"
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestMigration0009_AddsHashColumns covers AC-1: migration 0009 applied cleanly
// (the harness runs the embedded migrations) and added the additive, NULLABLE
// columns events.content_hash, events.chain_hash, and sessions.chain_head. A
// fresh session (no chained events yet) has a NULL chain_head, proving the column
// is nullable with no backfill/default.
func TestMigration0009_AddsHashColumns(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	_ = tenantID
	owner := h.ownerConn(t)

	// The new columns exist and are NULLABLE (information_schema is authoritative).
	cols := []struct{ table, col string }{
		{"events", "content_hash"},
		{"events", "chain_hash"},
		{"sessions", "chain_head"},
	}
	for _, c := range cols {
		var nullable string
		err := owner.QueryRow(context.Background(),
			`SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
			c.table, c.col).Scan(&nullable)
		if err != nil {
			t.Fatalf("column %s.%s missing (migration 0009 not applied?): %v", c.table, c.col, err)
		}
		if nullable != "YES" {
			t.Fatalf("column %s.%s is_nullable = %q, want YES (additive nullable, no NOT NULL)", c.table, c.col, nullable)
		}
	}

	// A brand-new session has a NULL chain_head (no chained events yet, no default).
	var head []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT chain_head FROM sessions WHERE id = $1", sessionID).Scan(&head); err != nil {
		t.Fatalf("read chain_head of fresh session: %v", err)
	}
	if head != nil {
		t.Fatalf("fresh session chain_head = %x, want NULL (no backfill/default)", head)
	}
}

// recomputeChainHead folds the genesis-seeded chain over the in-order envelopes,
// returning the running head after the last one — the independent oracle the
// tests compare the stored chain_head against.
func recomputeChainHead(t *testing.T, sessionID string, envs []domain.EventEnvelope) []byte {
	t.Helper()
	prev := domain.GenesisChainHash(sessionID)
	for _, e := range envs {
		payload, err := domain.MarshalEventPayload(e.Event)
		if err != nil {
			t.Fatalf("marshal payload seq=%d: %v", e.Seq, err)
		}
		prev = domain.ChainHash(prev, domain.ContentHash(payload))
	}
	return prev
}

// TestAppend_ComputesContentAndChainHash covers AC-3/AC-4: a single append
// stamps ContentHash + ChainHash on the returned envelope, persists both to the
// events row, and the chain_hash equals ChainHash(genesis, content_hash) for the
// first chained event.
func TestAppend_ComputesContentAndChainHash(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	ev := domain.TurnStarted{TurnID: "t-1", Model: "claude"}
	envs, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t), app.AppendInput{Event: ev, Actor: domain.ActorSystem})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("Append returned %d envelopes, want 1", len(envs))
	}

	// The returned envelope carries the hashes (AC-3).
	got := envs[0]
	if len(got.ContentHash) == 0 || len(got.ChainHash) == 0 {
		t.Fatalf("append envelope missing hashes: content=%x chain=%x", got.ContentHash, got.ChainHash)
	}

	// content_hash == sha256 over the EXACT stored bytes.
	payload, err := domain.MarshalEventPayload(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	wantContent := domain.ContentHash(payload)
	if !bytes.Equal(got.ContentHash, wantContent) {
		t.Fatalf("content_hash = %x, want %x (sha256 over stored payload)", got.ContentHash, wantContent)
	}

	// chain_hash == ChainHash(genesis, content_hash) for the first chained event.
	wantChain := domain.ChainHash(domain.GenesisChainHash(sessionID), wantContent)
	if !bytes.Equal(got.ChainHash, wantChain) {
		t.Fatalf("chain_hash = %x, want ChainHash(genesis, content) = %x", got.ChainHash, wantChain)
	}

	// Persisted to the events row (read as owner, bypassing RLS).
	owner := h.ownerConn(t)
	var dbContent, dbChain []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT content_hash, chain_hash FROM events WHERE session_id = $1 AND seq = 1", sessionID).
		Scan(&dbContent, &dbChain); err != nil {
		t.Fatalf("read hashes: %v", err)
	}
	if !bytes.Equal(dbContent, wantContent) || !bytes.Equal(dbChain, wantChain) {
		t.Fatalf("persisted hashes content=%x chain=%x, want content=%x chain=%x", dbContent, dbChain, wantContent, wantChain)
	}

	// sessions.chain_head advanced to the last chain_hash.
	var head []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT chain_head FROM sessions WHERE id = $1", sessionID).Scan(&head); err != nil {
		t.Fatalf("read chain_head: %v", err)
	}
	if !bytes.Equal(head, wantChain) {
		t.Fatalf("sessions.chain_head = %x, want %x (the new head after the append)", head, wantChain)
	}
}

// TestAppend_ChainsAcrossMultipleAppends covers AC-4: across several appends the
// stored chain_head equals an independent genesis-seeded fold of the whole
// stream, and each event's chain_hash links to its predecessor's.
func TestAppend_ChainsAcrossMultipleAppends(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	seedNEvents(ctx, t, h.store, sessionID, 6) // seq 1..6

	all, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(all) != 6 {
		t.Fatalf("loaded %d events, want 6", len(all))
	}

	// Every loaded envelope carries non-nil hashes.
	for _, e := range all {
		if len(e.ContentHash) == 0 || len(e.ChainHash) == 0 {
			t.Fatalf("seq %d loaded with nil hashes content=%x chain=%x", e.Seq, e.ContentHash, e.ChainHash)
		}
	}

	// Each chain_hash[i] == ChainHash(chain_hash[i-1], content_hash[i]), genesis-seeded.
	prev := domain.GenesisChainHash(sessionID)
	for _, e := range all {
		payload, err := domain.MarshalEventPayload(e.Event)
		if err != nil {
			t.Fatalf("marshal seq=%d: %v", e.Seq, err)
		}
		wantChain := domain.ChainHash(prev, domain.ContentHash(payload))
		if !bytes.Equal(e.ChainHash, wantChain) {
			t.Fatalf("seq %d chain_hash = %x, want %x (broken link in fold)", e.Seq, e.ChainHash, wantChain)
		}
		prev = wantChain
	}

	// sessions.chain_head == the recomputed head.
	owner := h.ownerConn(t)
	var head []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT chain_head FROM sessions WHERE id = $1", sessionID).Scan(&head); err != nil {
		t.Fatalf("read chain_head: %v", err)
	}
	if !bytes.Equal(head, recomputeChainHead(t, sessionID, all)) {
		t.Fatalf("sessions.chain_head = %x, want recomputed fold %x", head, prev)
	}
}

// TestAppend_IdempotentReplay_DoesNotReChain covers AC-5(a)/AC-6: a re-sent
// (session_id, request_id) returns the prior envelopes WITHOUT advancing
// head_seq OR sessions.chain_head (no double-increment, no re-chain). This is the
// load-bearing hot-path invariant a pessimistic reviewer probes.
func TestAppend_IdempotentReplay_DoesNotReChain(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	reqID := newUUID(t)
	// Append a batch of 3 under request_id R (head 0 -> 3).
	first, err := h.store.Append(ctx, sessionID, 0, 0, reqID,
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t1"}, Actor: domain.ActorSystem},
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t2"}, Actor: domain.ActorSystem},
		app.AppendInput{Event: domain.TurnFinished{TurnID: "t2", Reason: domain.Success, NumTurns: 1}, Actor: domain.ActorSystem},
	)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first append returned %d, want 3", len(first))
	}

	owner := h.ownerConn(t)
	var headSeqBefore int64
	var chainHeadBefore []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT head_seq, chain_head FROM sessions WHERE id = $1", sessionID).
		Scan(&headSeqBefore, &chainHeadBefore); err != nil {
		t.Fatalf("read head before replay: %v", err)
	}
	if headSeqBefore != 3 {
		t.Fatalf("head_seq after first append = %d, want 3", headSeqBefore)
	}

	// Re-send the IDENTICAL request_id (a lost-ACK replay).
	replay, err := h.store.Append(ctx, sessionID, 0, 0, reqID,
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t1"}, Actor: domain.ActorSystem},
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t2"}, Actor: domain.ActorSystem},
		app.AppendInput{Event: domain.TurnFinished{TurnID: "t2", Reason: domain.Success, NumTurns: 1}, Actor: domain.ActorSystem},
	)
	if err != nil {
		t.Fatalf("replay append: expected success, got %v", err)
	}
	if len(replay) != 3 {
		t.Fatalf("replay returned %d envelopes, want the prior 3", len(replay))
	}
	// The replay returns the SAME prior envelopes (same seqs + same chain hashes).
	for i := range replay {
		if replay[i].Seq != first[i].Seq {
			t.Fatalf("replay[%d].Seq = %d, want %d (prior envelope)", i, replay[i].Seq, first[i].Seq)
		}
		if !bytes.Equal(replay[i].ChainHash, first[i].ChainHash) {
			t.Fatalf("replay[%d] chain_hash changed: %x -> %x (replay must NOT re-chain)", i, first[i].ChainHash, replay[i].ChainHash)
		}
	}

	// head_seq and chain_head are UNCHANGED (no double increment, no re-chain).
	var headSeqAfter int64
	var chainHeadAfter []byte
	if err := owner.QueryRow(context.Background(),
		"SELECT head_seq, chain_head FROM sessions WHERE id = $1", sessionID).
		Scan(&headSeqAfter, &chainHeadAfter); err != nil {
		t.Fatalf("read head after replay: %v", err)
	}
	if headSeqAfter != headSeqBefore {
		t.Fatalf("head_seq advanced on idempotent replay: %d -> %d (double increment)", headSeqBefore, headSeqAfter)
	}
	if !bytes.Equal(chainHeadAfter, chainHeadBefore) {
		t.Fatalf("sessions.chain_head advanced on idempotent replay: %x -> %x (the chain was re-folded)", chainHeadBefore, chainHeadAfter)
	}

	// And no duplicate rows landed.
	if got := h.countEvents(t, sessionID); got != 3 {
		t.Fatalf("event count = %d, want 3 (no duplicate rows on replay)", got)
	}
}

// TestAppend_HashParityWithDomainHelpers covers AC-11: the hashes the prod pgx
// store persists for a stream are byte-identical to an independent fold computed
// purely from the shared domain helpers over the SAME typed event values —
// proving prod and the (helper-driven) dev path agree byte-for-byte.
func TestAppend_HashParityWithDomainHelpers(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	// A representative spread including a map-bearing ApprovalRequested.
	inputs := []domain.Event{
		domain.SessionStarted{},
		domain.TurnStarted{TurnID: "t-1", Model: "claude"},
		domain.ApprovalRequested{TurnID: "t-1", CallID: "c-1", ToolName: "bash", Reason: "x", Args: map[string]any{"b": 2, "a": 1}},
		domain.TurnFinished{TurnID: "t-1", Reason: domain.Success, NumTurns: 1},
	}
	var expected int64
	for _, ev := range inputs {
		envs, err := h.store.Append(ctx, sessionID, expected, 0, newUUID(t), app.AppendInput{Event: ev, Actor: domain.ActorSystem})
		if err != nil {
			t.Fatalf("append %T: %v", ev, err)
		}
		expected = envs[0].Seq
	}

	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Independent fold from the shared helpers (the dev path algorithm).
	prev := domain.GenesisChainHash(sessionID)
	for i, ev := range inputs {
		payload, err := domain.MarshalEventPayload(ev)
		if err != nil {
			t.Fatalf("marshal %T: %v", ev, err)
		}
		wantContent := domain.ContentHash(payload)
		wantChain := domain.ChainHash(prev, wantContent)
		if !bytes.Equal(loaded[i].ContentHash, wantContent) {
			t.Fatalf("seq %d content_hash parity mismatch: prod %x vs helper %x", loaded[i].Seq, loaded[i].ContentHash, wantContent)
		}
		if !bytes.Equal(loaded[i].ChainHash, wantChain) {
			t.Fatalf("seq %d chain_hash parity mismatch: prod %x vs helper %x", loaded[i].Seq, loaded[i].ChainHash, wantChain)
		}
		prev = wantChain
	}
}
