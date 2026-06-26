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
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// fullV1EventSet returns one representative value of EVERY concrete domain.Event
// kind (the closed v1 set in event.go), including the map-bearing
// ApprovalRequested, the slice-bearing PlanUpdated, and the llm-typed
// MessageAppended/AssistantMessage/AssistantMessageDelta/TurnAborted/TurnFinished.
// It is the fixture for the append-vs-verify re-marshal round-trip pin (T-04
// limitation guard): the order matches event.go's discriminator declaration order
// so a newly added event type that this set forgets will (eventually) be caught by
// the decodePayload exhaustiveness check on read-back.
func fullV1EventSet() []domain.Event {
	return []domain.Event{
		domain.SessionStarted{ParentID: "", ForkedFromSeq: 0, SystemPrompt: "you are a helpful agent"},
		domain.MessageAppended{Message: llm.Message{}},
		domain.TurnStarted{TurnID: "t-1", Model: "claude"},
		domain.AssistantMessageDelta{TurnID: "t-1", TextSoFar: "partial", UsageSoFar: llm.Usage{}},
		domain.AssistantMessage{TurnID: "t-1", Message: llm.Message{}, StopReason: llm.StopEnd, Usage: llm.Usage{}, CostUSD: 0.5},
		domain.ToolExecutionStarted{CallID: "c-1", ToolName: "bash", IdempotencyKey: "k-1"},
		domain.ToolResult{CallID: "c-1", Result: "ok", IsError: false, Truncated: false},
		domain.ToolResultCleared{ClearedSessionID: "s-1", ClearedSeq: 2, Reason: "reclaim"},
		domain.TurnAborted{TurnID: "t-1", Reason: domain.ErrorDuringExecution, UsageSoFar: llm.Usage{}, CostUSD: 0.1},
		domain.TurnFinished{TurnID: "t-1", Reason: domain.Success, Usage: llm.Usage{}, CostUSD: 0.2, NumTurns: 1},
		domain.CompactionPerformed{BeforeTokens: 100, AfterTokens: 40, Reason: "approaching window"},
		domain.PermissionDecided{CallID: "c-1", ToolName: "bash", Decision: domain.PermissionAsk, Resolved: domain.AskAllowed, RuleID: "r-1", Reason: "ask"},
		domain.WorkspaceReset{Reason: "resume after crash"},
		domain.BypassModeActivated{Principal: "op-1", PriorMode: domain.ModeDefault, NewMode: domain.ModeBypass, Reason: "incident"},
		domain.MCPToolApprovalRequested{ServerName: "srv", ServerVersion: "1.0", ToolName: "search", UntrustedDescription: "desc"},
		domain.MCPToolApprovalResolved{ServerName: "srv", ToolName: "search", Granted: true},
		domain.PlanUpdated{TurnID: "t-1", Items: []domain.PlanItem{
			{Content: "step one", Status: domain.PlanStatusCompleted},
			{Content: "step two", Status: domain.PlanStatusInProgress},
		}},
		// A map-bearing payload with UNSORTED insertion order: json.Marshal sorts map
		// keys, so the stored bytes (and thus the hash) are stable regardless of the
		// literal order here — the load-bearing determinism property (AC-2/AC-11).
		domain.ApprovalRequested{TurnID: "t-1", CallID: "c-1", ToolName: "bash", Reason: "needs approval", Args: map[string]any{"z": 3, "a": 1, "m": 2}},
	}
}

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

// TestVerify_CanonicalBytesAreVerbatimAppendBytes is the raw-bytes content pin
// (ADR-0033) for EVERY concrete v1 event kind. It replaces the obsolete
// decode-then-re-marshal round-trip pin: VerifyChainIntegrity no longer hashes a
// re-marshaled decode of the JSONB — it hashes the RAW stored
// events.payload_canonical bytes DIRECTLY. So the load-bearing invariant is now:
//
//	stored events.payload_canonical  ==  MarshalEventPayload( original_event )   (verbatim)
//	ContentHash( stored payload_canonical )  ==  stored content_hash
//
// i.e. the column holds the EXACT append-side bytes content_hash was taken over,
// verbatim (a BYTEA Postgres does NOT normalize), so a re-hash of the stored
// bytes equals the stored content_hash for every kind — including the map-bearing
// ApprovalRequested and slice-bearing PlanUpdated. This is schema-version-agnostic:
// it hashes whatever bytes are stored, so it neither relies on nor is broken by a
// future field-ordering/escaping change or extra JSONB keys.
//
// It also asserts the legacy events.payload JSONB still round-trips through decode
// (it remains a queryable, non-authoritative copy) so the pre-0009 fallback path
// stays exercised.
func TestVerify_CanonicalBytesAreVerbatimAppendBytes(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	events := fullV1EventSet()
	// Sanity: the fixture covers the whole closed discriminator set, so a newly
	// added event kind forces this test to be extended. Count distinct EventTypes.
	seenTypes := map[domain.EventType]bool{}
	for _, ev := range events {
		seenTypes[ev.EventType()] = true
	}
	if len(seenTypes) != len(events) {
		t.Fatalf("fullV1EventSet has duplicate event types: %d distinct of %d", len(seenTypes), len(events))
	}

	var expected int64
	for _, ev := range events {
		envs, err := h.store.Append(ctx, sessionID, expected, 0, newUUID(t),
			app.AppendInput{Event: ev, Actor: domain.ActorSystem})
		if err != nil {
			t.Fatalf("append %T: %v", ev, err)
		}
		expected = envs[0].Seq
	}

	owner := h.ownerConn(t)
	for i, ev := range events {
		seq := int64(i + 1)
		// The append-side bytes content_hash was computed over: Go's compact
		// json.Marshal of the ORIGINAL event (== marshalPayload at append time).
		appendBytes, err := domain.MarshalEventPayload(ev)
		if err != nil {
			t.Fatalf("marshal original seq=%d (%s): %v", seq, ev.EventType(), err)
		}

		// Read the stored content_hash, the RAW payload_canonical bytes (read as
		// bytea, no normalization), and the legacy JSONB (decoded for the fallback).
		var storedContent []byte
		var storedCanonical []byte
		var storedJSONB []byte
		if err := owner.QueryRow(context.Background(),
			"SELECT content_hash, payload_canonical, payload::text::bytea FROM events WHERE session_id = $1 AND seq = $2",
			sessionID, seq).Scan(&storedContent, &storedCanonical, &storedJSONB); err != nil {
			t.Fatalf("read row seq=%d: %v", seq, err)
		}

		// The load-bearing invariant: payload_canonical is the VERBATIM append bytes.
		if !bytes.Equal(storedCanonical, appendBytes) {
			t.Fatalf("seq %d (%s): payload_canonical is NOT the verbatim append bytes\n append=%s\n stored=%s",
				seq, ev.EventType(), appendBytes, storedCanonical)
		}
		// Re-hashing the RAW stored bytes (exactly what verify does) equals the
		// stored content_hash.
		if recomputed := domain.ContentHash(storedCanonical); !bytes.Equal(recomputed, storedContent) {
			t.Fatalf("seq %d (%s): ContentHash(stored payload_canonical) %x != stored content_hash %x", seq, ev.EventType(), recomputed, storedContent)
		}
		// The legacy JSONB still decodes (non-authoritative queryable copy).
		if _, err := decodePayload(ev.EventType(), storedJSONB); err != nil {
			t.Fatalf("legacy JSONB decode seq=%d (%s): %v", seq, ev.EventType(), err)
		}
	}

	// The whole stream verifies clean over the raw stored bytes for every kind.
	res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity over full v1 set: %v", err)
	}
	if !res.Valid || res.Checked != len(events) {
		t.Fatalf("full v1 set verify = {Valid:%v Checked:%d FirstBadSeq:%d Reason:%q}, want Valid=true Checked=%d",
			res.Valid, res.Checked, res.FirstBadSeq, res.Reason, len(events))
	}
}

// TestVerify_DetectsStructuralJSONBTamper is the explicit guard for the
// previously-undetected tamper class (ADR-0033 hardening): mutating the stored
// payload bytes by (a) reordering keys, (b) adding whitespace, or (c) injecting
// an EXTRA key that the v1 struct DROPS on decode. Under the old
// decode-then-re-marshal verify these all re-marshaled to the identical Go bytes
// and went UNDETECTED. Hashing the RAW payload_canonical bytes catches every one:
// the stored bytes differ, so the recomputed content_hash diverges.
func TestVerify_DetectsStructuralJSONBTamper(t *testing.T) {
	cases := []struct {
		name      string
		tamperSQL string // the new payload_canonical value (UTF8 text the tamperer writes)
	}{
		{
			name:      "reordered keys",
			tamperSQL: `{"Model":"claude","TurnID":"t1"}`, // struct order is TurnID,Model
		},
		{
			name:      "injected whitespace",
			tamperSQL: `{"TurnID": "t1", "Model": "claude"}`,
		},
		{
			name:      "extra key dropped on decode",
			tamperSQL: `{"TurnID":"t1","Model":"claude","EVIL":"injected"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			tenantID, sessionID := h.seedTenantAndSession(t)
			ctx := tenantCtx(tenantID)

			// seq 1: the exact event whose canonical bytes are {"TurnID":"t1","Model":"claude"}.
			ev := domain.TurnStarted{TurnID: "t1", Model: "claude"}
			if _, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t),
				app.AppendInput{Event: ev, Actor: domain.ActorSystem}); err != nil {
				t.Fatalf("append: %v", err)
			}
			// A couple more chained events (seq 2,3) so the chain extends past the
			// tampered row — the tamper at seq 1 is still the first bad seq.
			if _, err := appendOne(ctx, h.store, sessionID, 1, 0, newUUID(t), "turn-2"); err != nil {
				t.Fatalf("append seq 2: %v", err)
			}
			if _, err := appendOne(ctx, h.store, sessionID, 2, 0, newUUID(t), "turn-3"); err != nil {
				t.Fatalf("append seq 3: %v", err)
			}

			// Pre-check: a json.Unmarshal of the tamper text into TurnStarted then a
			// re-marshal reproduces the ORIGINAL canonical bytes — i.e. the OLD verify
			// (decode->re-marshal) would NOT have caught this. This makes the test a
			// genuine guard against the false-negative, not a tautology.
			origBytes, err := domain.MarshalEventPayload(ev)
			if err != nil {
				t.Fatalf("marshal original: %v", err)
			}
			reDecoded, err := decodePayload(domain.EventTurnStarted, []byte(tc.tamperSQL))
			if err != nil {
				t.Fatalf("decode tamper text: %v", err)
			}
			reMarshaled, err := domain.MarshalEventPayload(reDecoded)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if !bytes.Equal(reMarshaled, origBytes) {
				t.Fatalf("precondition failed: tamper %q does not re-marshal to the original bytes (%s vs %s); this case would not prove the false-negative is closed",
					tc.name, reMarshaled, origBytes)
			}

			// Tamper the AUTHORITATIVE raw bytes (content_hash left stale).
			owner := h.ownerConn(t)
			if _, err := owner.Exec(context.Background(),
				`UPDATE events SET payload_canonical = convert_to($2,'UTF8') WHERE session_id = $1 AND seq = 1`,
				sessionID, tc.tamperSQL); err != nil {
				t.Fatalf("tamper payload_canonical: %v", err)
			}

			// The raw-bytes verify MUST detect it at seq 1.
			res, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
			if err != nil {
				t.Fatalf("VerifyChainIntegrity: %v", err)
			}
			if res.Valid {
				t.Fatalf("structural tamper %q went UNDETECTED (Valid=true) — the false-negative is NOT closed", tc.name)
			}
			if res.FirstBadSeq != 1 {
				t.Fatalf("FirstBadSeq = %d, want 1 (the tampered seq)", res.FirstBadSeq)
			}
			if !strings.Contains(strings.ToLower(res.Reason), "content") {
				t.Fatalf("Reason = %q, want a content-hash-mismatch classification", res.Reason)
			}
		})
	}
}

// TestVerify_DetectorIsRedProof is the "red-proof the detector" guard the task
// requires: it proves VerifyChainIntegrity's pass/fail hinges on the hash
// comparison and not on some always-true path. It runs the SAME store, SAME
// session twice: (1) untampered -> Valid=true; (2) after an owner-side payload
// mutation that leaves content_hash stale -> Valid=false at the mutated seq. If the
// content-hash equality check in VerifyChainIntegrity were removed (the mutation
// undetectable), step (2) would return Valid=true and this test FAILS — so the
// assertion genuinely exercises the detector rather than a tautology.
func TestVerify_DetectorIsRedProof(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)
	seedNEvents(ctx, t, h.store, sessionID, 5) // seq 1..5

	// (1) Clean run is Valid — establishes the detector is not stuck on "false".
	clean, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("clean verify: %v", err)
	}
	if !clean.Valid || clean.Checked != 5 {
		t.Fatalf("clean verify = {Valid:%v Checked:%d}, want Valid=true Checked=5", clean.Valid, clean.Checked)
	}

	// (2) Mutate seq 2's payload_canonical (content_hash left stale). The detector
	// MUST flip to Valid=false at exactly that seq — proving the comparison is
	// load-bearing.
	owner := h.ownerConn(t)
	if _, err := owner.Exec(context.Background(),
		`UPDATE events SET payload_canonical = convert_to('{"TurnID":"flipped","Model":"x"}','UTF8') WHERE session_id = $1 AND seq = 2`,
		sessionID); err != nil {
		t.Fatalf("mutate payload_canonical: %v", err)
	}
	dirty, err := h.store.VerifyChainIntegrity(ctx, sessionID, 0, 0)
	if err != nil {
		t.Fatalf("dirty verify: %v", err)
	}
	if dirty.Valid {
		t.Fatal("detector returned Valid=true after a payload mutation — the hash check is NOT load-bearing (red-proof failed)")
	}
	if dirty.FirstBadSeq != 2 {
		t.Fatalf("dirty verify FirstBadSeq = %d, want 2 (the mutated seq)", dirty.FirstBadSeq)
	}
	// Only the events BEFORE the bad seq are counted as checked (seq 1).
	if dirty.Checked != 1 {
		t.Fatalf("dirty verify Checked = %d, want 1 (events verified before the first bad seq)", dirty.Checked)
	}
}
