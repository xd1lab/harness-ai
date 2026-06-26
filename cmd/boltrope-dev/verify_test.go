// SPDX-License-Identifier: Apache-2.0

package main

// RED (test-first) for Batch-5A AC-10: the dev in-memory event store computes the
// SAME content_hash + folds the SAME chain_hash as the prod pgx store, using the
// shared pgx-free domain helpers, and stamps ContentHash/ChainHash on each stored
// envelope so Load/LoadRange/LoadUpTo expose them. It also gains a dev-store
// VerifyChainIntegrity with the same {Valid,FirstBadSeq,Reason,Checked} semantics,
// including tamper detection over the in-memory envelopes.
//
// References symbols that do NOT exist yet — domain.EventEnvelope.ContentHash/
// .ChainHash, domain.{ContentHash,GenesisChainHash,ChainHash,MarshalEventPayload},
// domain.ChainVerification, Store.VerifyChainIntegrity — so this file does NOT
// compile; that absence is the RED proof.

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// seedDevStream appends n TurnStarted events to a fresh dev session and returns
// the store + session id.
func seedDevStream(t *testing.T, n int) (*Store, string) {
	t.Helper()
	ctx := context.Background()
	st := newStore()
	const sid = "sess-dev-chain"
	_, err := st.CreateSession(ctx, sid, domain.ModeDefault)
	require.NoError(t, err)
	var expected int64
	for i := 0; i < n; i++ {
		envs, err := st.Append(ctx, sid, expected, 0, "req",
			app.AppendInput{Event: domain.TurnStarted{TurnID: "t", Model: "dev"}, Actor: domain.ActorSystem})
		require.NoError(t, err)
		expected = envs[0].Seq
	}
	return st, sid
}

// TestDevStore_ComputesAndExposesHashes covers AC-10: the dev Append stamps
// ContentHash + ChainHash on each envelope (computed via the shared helpers,
// genesis-seeded per session), and Load exposes them folded correctly.
func TestDevStore_ComputesAndExposesHashes(t *testing.T) {
	st, sid := seedDevStream(t, 4)
	ctx := context.Background()

	all, err := st.Load(ctx, sid, 0)
	require.NoError(t, err)
	require.Len(t, all, 4)

	prev := domain.GenesisChainHash(sid)
	for _, e := range all {
		require.NotEmpty(t, e.ContentHash, "seq %d must carry a content_hash", e.Seq)
		require.NotEmpty(t, e.ChainHash, "seq %d must carry a chain_hash", e.Seq)
		payload, err := domain.MarshalEventPayload(e.Event)
		require.NoError(t, err)
		wantContent := domain.ContentHash(payload)
		wantChain := domain.ChainHash(prev, wantContent)
		assert.True(t, bytes.Equal(e.ContentHash, wantContent), "seq %d content_hash fold mismatch", e.Seq)
		assert.True(t, bytes.Equal(e.ChainHash, wantChain), "seq %d chain_hash fold mismatch (broken link)", e.Seq)
		prev = wantChain
	}
}

// TestDevStore_HashesMatchDomainFold_Parity covers AC-11 (dev side): for an
// identical typed event stream the dev store's per-seq hashes equal an independent
// fold over the shared domain helpers — the same algorithm the prod store uses, so
// dev/prod parity holds by construction.
func TestDevStore_HashesMatchDomainFold_Parity(t *testing.T) {
	ctx := context.Background()
	st := newStore()
	const sid = "sess-parity"
	_, err := st.CreateSession(ctx, sid, domain.ModeDefault)
	require.NoError(t, err)

	inputs := []domain.Event{
		domain.SessionStarted{},
		domain.TurnStarted{TurnID: "t-1", Model: "claude"},
		domain.ApprovalRequested{TurnID: "t-1", CallID: "c-1", ToolName: "bash", Reason: "x", Args: map[string]any{"b": 2, "a": 1}},
		domain.TurnFinished{TurnID: "t-1", Reason: domain.Success, NumTurns: 1},
	}
	var expected int64
	for _, ev := range inputs {
		envs, err := st.Append(ctx, sid, expected, 0, "req", app.AppendInput{Event: ev, Actor: domain.ActorSystem})
		require.NoError(t, err)
		expected = envs[0].Seq
	}

	all, err := st.Load(ctx, sid, 0)
	require.NoError(t, err)
	require.Len(t, all, len(inputs))

	prev := domain.GenesisChainHash(sid)
	for i, ev := range inputs {
		payload, err := domain.MarshalEventPayload(ev)
		require.NoError(t, err)
		wantContent := domain.ContentHash(payload)
		wantChain := domain.ChainHash(prev, wantContent)
		assert.True(t, bytes.Equal(all[i].ContentHash, wantContent), "seq %d content parity", all[i].Seq)
		assert.True(t, bytes.Equal(all[i].ChainHash, wantChain), "seq %d chain parity", all[i].Seq)
		prev = wantChain
	}
}

// TestDevStore_VerifyChainIntegrity_Untampered covers AC-10: a clean dev stream
// verifies Valid=true, FirstBadSeq=0, Checked=N.
func TestDevStore_VerifyChainIntegrity_Untampered(t *testing.T) {
	st, sid := seedDevStream(t, 5)
	res, err := st.VerifyChainIntegrity(context.Background(), sid, 0, 0)
	require.NoError(t, err)
	assert.True(t, res.Valid, "clean dev stream must verify Valid")
	assert.Equal(t, int64(0), res.FirstBadSeq)
	assert.Equal(t, 5, res.Checked)
}

// TestDevStore_VerifyChainIntegrity_DetectsTamper covers AC-10's dev tamper test:
// mutating a stored envelope's authoritative PayloadCanonical bytes (so its
// recomputed content_hash no longer matches the stored one) makes verify report
// Valid=false at that seq. Dev verify hashes the RAW PayloadCanonical bytes
// (parity with the prod pgx store, which hashes events.payload_canonical), so the
// tamper must mutate those bytes — not just the decoded Event.
func TestDevStore_VerifyChainIntegrity_DetectsTamper(t *testing.T) {
	st, sid := seedDevStream(t, 5)

	// Tamper the stored envelope at seq 3 IN PLACE: mutate the canonical payload
	// bytes but leave the stored content_hash untouched, simulating a mutated
	// durable row.
	st.mu.Lock()
	for i := range st.sessions[sid] {
		if st.sessions[sid][i].Seq == 3 {
			st.sessions[sid][i].PayloadCanonical = []byte(`{"TurnID":"TAMPERED","Model":"evil"}`)
		}
	}
	st.mu.Unlock()

	res, err := st.VerifyChainIntegrity(context.Background(), sid, 0, 0)
	require.NoError(t, err)
	assert.False(t, res.Valid, "a tampered dev envelope must verify Valid=false")
	assert.Equal(t, int64(3), res.FirstBadSeq, "FirstBadSeq is the tampered seq")
	assert.Contains(t, strings.ToLower(res.Reason), "content", "a payload tamper is a content-hash mismatch")
}

// TestDevStore_VerifyChainIntegrity_DetectsBrokenLink covers AC-10: rewriting a
// stored ChainHash (the payload intact) is detected as a broken link.
func TestDevStore_VerifyChainIntegrity_DetectsBrokenLink(t *testing.T) {
	st, sid := seedDevStream(t, 5)

	st.mu.Lock()
	for i := range st.sessions[sid] {
		if st.sessions[sid][i].Seq == 4 {
			st.sessions[sid][i].ChainHash = bytes.Repeat([]byte{0xAB}, 32) // wrong link
		}
	}
	st.mu.Unlock()

	res, err := st.VerifyChainIntegrity(context.Background(), sid, 0, 0)
	require.NoError(t, err)
	assert.False(t, res.Valid)
	assert.Equal(t, int64(4), res.FirstBadSeq)
	reason := strings.ToLower(res.Reason)
	assert.True(t, strings.Contains(reason, "link") || strings.Contains(reason, "chain"),
		"a rewritten chain_hash is a broken-link reason, got %q", res.Reason)
}

// devStoreSatisfiesVerify is a compile-time assertion that the dev Store gains the
// VerifyChainIntegrity method with the exact igrpc.EventStore-bound signature
// (read-only). It pins the method shape so the `var _ igrpc.EventStore` assertion
// in eventstore.go continues to hold once the interface gains the method.
func devStoreVerifySignature(st *Store) func(context.Context, string, int64, int64) (domain.ChainVerification, error) {
	return st.VerifyChainIntegrity
}

var _ = devStoreVerifySignature
