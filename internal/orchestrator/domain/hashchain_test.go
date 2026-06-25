// SPDX-License-Identifier: Apache-2.0

package domain

// RED (test-first) for Batch-5A (tamper-evident hash-chain), the BIG-C
// differentiator. This file pins the SHARED, pgx-free hash-chain primitives the
// prod (pgx) store AND the dev (in-mem) binary both import (AC-2):
//
//   - MarshalEventPayload(e Event) ([]byte, error) == json.Marshal(e): the
//     byte-identical encoding the events.payload column stores. The prod store's
//     marshalPayload MUST be refactored to call this so prod hashes the identical
//     bytes it persists.
//   - ContentHash(payload []byte) []byte == sha256.Sum256(payload) as a 32-byte
//     slice.
//   - GenesisChainHash(sessionID string) []byte ==
//     sha256.Sum256([]byte("boltrope-audit-genesis-v1:" + sessionID)).
//   - ChainHash(prevChainHash, contentHash []byte) []byte ==
//     sha256.Sum256(append(prevChainHash, contentHash...)); prev is
//     GenesisChainHash(sessionID) for the first chained event of a session.
//
// These symbols do NOT exist yet, so this file does NOT compile — that absence is
// the RED proof of test-first authoring (AC-2, AC-11 algorithm).

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"
)

// TestContentHash_IsSha256OfPayload pins ContentHash == sha256.Sum256(payload),
// returned as a 32-byte slice (a content digest over the EXACT stored bytes).
func TestContentHash_IsSha256OfPayload(t *testing.T) {
	payload := []byte(`{"TurnID":"t-1","Model":"claude"}`)
	want := sha256.Sum256(payload)
	got := ContentHash(payload)
	if len(got) != sha256.Size {
		t.Fatalf("ContentHash len = %d, want %d (32-byte digest)", len(got), sha256.Size)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("ContentHash = %x, want %x (sha256 over the exact payload bytes)", got, want[:])
	}
}

// TestMarshalEventPayload_EqualsJSONMarshal pins that MarshalEventPayload is the
// byte-identical json.Marshal of the typed event — the same bytes the
// events.payload column stores. A map-bearing event (ApprovalRequested.Args)
// must encode stably (Go sorts map keys), so the hash is deterministic.
func TestMarshalEventPayload_EqualsJSONMarshal(t *testing.T) {
	events := []Event{
		TurnStarted{TurnID: "t-1", Model: "claude"},
		PlanUpdated{TurnID: "t-2", Items: []PlanItem{{Content: "x", Status: PlanStatusPending}}},
		ApprovalRequested{
			TurnID: "t-3", CallID: "c-1", ToolName: "bash", Reason: "mutating",
			Args: map[string]any{"z": 1, "a": "two", "m": true},
		},
	}
	for _, e := range events {
		want, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("json.Marshal(%T): %v", e, err)
		}
		got, err := MarshalEventPayload(e)
		if err != nil {
			t.Fatalf("MarshalEventPayload(%T): %v", e, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("MarshalEventPayload(%T) = %s, want %s (must equal json.Marshal)", e, got, want)
		}
	}
}

// TestMarshalEventPayload_MapKeysStable pins the determinism the whole chain
// rests on: marshaling the SAME map-bearing event twice (with the map literal in
// a different source order) yields byte-identical bytes, because encoding/json
// sorts map keys. Without this the content_hash would be non-deterministic.
func TestMarshalEventPayload_MapKeysStable(t *testing.T) {
	a := ApprovalRequested{Args: map[string]any{"alpha": 1, "beta": 2, "gamma": 3}}
	b := ApprovalRequested{Args: map[string]any{"gamma": 3, "alpha": 1, "beta": 2}}
	pa, err := MarshalEventPayload(a)
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}
	pb, err := MarshalEventPayload(b)
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}
	if !bytes.Equal(pa, pb) {
		t.Fatalf("map-key order leaked into the encoding: %s vs %s (json must sort keys)", pa, pb)
	}
	if !bytes.Equal(ContentHash(pa), ContentHash(pb)) {
		t.Fatal("content hashes differ for the same logical event (non-deterministic map encoding)")
	}
}

// TestGenesisChainHash_IsSessionDerived pins the genesis seed: a fixed-prefix
// SHA-256 over the session id, so two different sessions never share a genesis
// (a per-session chain, not a global one).
func TestGenesisChainHash_IsSessionDerived(t *testing.T) {
	const sid = "sess-abc"
	want := sha256.Sum256([]byte("boltrope-audit-genesis-v1:" + sid))
	got := GenesisChainHash(sid)
	if len(got) != sha256.Size {
		t.Fatalf("GenesisChainHash len = %d, want %d", len(got), sha256.Size)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("GenesisChainHash(%q) = %x, want %x", sid, got, want[:])
	}
	if bytes.Equal(GenesisChainHash("sess-A"), GenesisChainHash("sess-B")) {
		t.Fatal("two distinct sessions share a genesis (chain must be per-session)")
	}
}

// TestChainHash_FoldsPrevAndContent pins chain_hash = SHA256(prev || content):
// the running fold that links each event to its predecessor.
func TestChainHash_FoldsPrevAndContent(t *testing.T) {
	prev := GenesisChainHash("sess-1")
	content := ContentHash([]byte(`{"x":1}`))
	want := sha256.Sum256(append(append([]byte{}, prev...), content...))
	got := ChainHash(prev, content)
	if len(got) != sha256.Size {
		t.Fatalf("ChainHash len = %d, want %d", len(got), sha256.Size)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("ChainHash = %x, want SHA256(prev||content) = %x", got, want[:])
	}
}

// TestChainHash_DoesNotMutateInputs guards the append(prev, content...) fold
// against the classic Go slice-aliasing bug: ChainHash must not clobber the
// caller's prev slice (a shared running head) when it grows it. A corrupted prev
// would silently break every subsequent link.
func TestChainHash_DoesNotMutateInputs(t *testing.T) {
	prev := GenesisChainHash("sess-mut")
	prevCopy := append([]byte{}, prev...)
	content := ContentHash([]byte(`{"y":2}`))
	_ = ChainHash(prev, content)
	if !bytes.Equal(prev, prevCopy) {
		t.Fatalf("ChainHash mutated its prev argument: %x -> %x (slice-aliasing bug)", prevCopy, prev)
	}
}

// TestChainHash_LinkIsOrderSensitive pins that swapping two events' order changes
// the resulting head — the property that makes a re-ordered or spliced log
// detectable (the chain is order-sensitive by construction).
func TestChainHash_LinkIsOrderSensitive(t *testing.T) {
	const sid = "sess-order"
	c1 := ContentHash([]byte(`{"e":1}`))
	c2 := ContentHash([]byte(`{"e":2}`))
	genesis := GenesisChainHash(sid)

	// Order [c1, c2].
	h12 := ChainHash(ChainHash(genesis, c1), c2)
	// Order [c2, c1].
	h21 := ChainHash(ChainHash(genesis, c2), c1)
	if bytes.Equal(h12, h21) {
		t.Fatal("chain head is order-insensitive; a reordered log would be undetectable")
	}
}

// TestChainHash_TamperedContentBreaksHead pins that flipping any byte of a
// content hash changes the fold output — the core tamper-evidence property.
func TestChainHash_TamperedContentBreaksHead(t *testing.T) {
	const sid = "sess-tamper"
	genesis := GenesisChainHash(sid)
	good := ContentHash([]byte(`{"ok":true}`))
	bad := ContentHash([]byte(`{"ok":false}`)) // a single logical change
	if bytes.Equal(ChainHash(genesis, good), ChainHash(genesis, bad)) {
		t.Fatal("a different content hash produced the same chain head (no tamper evidence)")
	}
}

// foldStream replays the append-path fold OVER the wire: it reuses the SAME
// running `prev` slice that ChainHash returns as the next iteration's input,
// exactly as the single-writer append loop carries its head forward. It returns
// the per-seq chain heads. This mirrors how the prod/dev stores fold a batch.
func foldStream(sessionID string, contents [][]byte) [][]byte {
	prev := GenesisChainHash(sessionID)
	heads := make([][]byte, len(contents))
	for i, c := range contents {
		prev = ChainHash(prev, c) // carry the returned head forward (reuse)
		heads[i] = prev
	}
	return heads
}

// TestFoldStream_ReuseDoesNotAliasEarlierHeads is the fold-reuse / aliasing
// REGRESSION the whole chain rests on. The append path carries one running head
// slice forward across the batch (prev = ChainHash(prev, content) each step). If
// ChainHash grew the caller's prev via a bare append it would retroactively
// clobber an EARLIER head's backing array, corrupting the chain after the fact.
// We snapshot the first head, fold several more events reusing the returned
// slice, then assert the snapshot is byte-unchanged — proving prev is never
// aliased/mutated by a later ChainHash call.
func TestFoldStream_ReuseDoesNotAliasEarlierHeads(t *testing.T) {
	const sid = "sess-fold-reuse"
	contents := [][]byte{
		ContentHash([]byte(`{"seq":1}`)),
		ContentHash([]byte(`{"seq":2}`)),
		ContentHash([]byte(`{"seq":3}`)),
		ContentHash([]byte(`{"seq":4}`)),
	}

	// Fold step-by-step, snapshotting the first head before later folds run.
	prev := GenesisChainHash(sid)
	head1 := ChainHash(prev, contents[0])
	head1Snapshot := append([]byte{}, head1...)

	// Continue the fold reusing the RETURNED slices as the running prev.
	running := head1
	for _, c := range contents[1:] {
		running = ChainHash(running, c)
	}

	if !bytes.Equal(head1, head1Snapshot) {
		t.Fatalf("earlier chain head was mutated by a later ChainHash fold: %x -> %x (slice aliasing corrupts the chain)", head1Snapshot, head1)
	}

	// And the genesis seed itself must be untouched by the whole fold.
	if !bytes.Equal(prev, GenesisChainHash(sid)) {
		t.Fatalf("genesis seed mutated by the fold: %x", prev)
	}
}

// TestFoldStream_DeterministicAndReproducible pins AC-11 at the algorithm level:
// folding the SAME stream twice yields byte-identical per-seq chain heads. Both
// the prod (pgx) store and the dev (in-mem) store fold through these shared
// helpers, so equal inputs => equal hashes => dev/prod parity by construction.
func TestFoldStream_DeterministicAndReproducible(t *testing.T) {
	const sid = "sess-parity"
	stream := func() [][]byte {
		return [][]byte{
			mustMarshalContent(t, SessionStarted{SystemPrompt: "you are boltrope"}),
			mustMarshalContent(t, TurnStarted{TurnID: "t-1", Model: "claude"}),
			mustMarshalContent(t, ApprovalRequested{
				TurnID: "t-1", CallID: "c-1", ToolName: "bash", Reason: "mutating",
				Args: map[string]any{"cmd": "rm -rf /", "force": true, "n": 3},
			}),
			mustMarshalContent(t, TurnFinished{TurnID: "t-1", Reason: Success, NumTurns: 1}),
		}
	}
	a := foldStream(sid, stream())
	b := foldStream(sid, stream())
	if len(a) != len(b) {
		t.Fatalf("fold length mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("seq %d chain head not reproducible: %x vs %x", i+1, a[i], b[i])
		}
	}
}

// TestFoldStream_KnownGoodPerSeqBytes pins the EXACT per-seq content_hash and
// chain_hash bytes for a fixed event stream, so a refactor that silently alters
// the algorithm (different marshal, different fold, different genesis) breaks
// this golden assertion. The expectations are computed from the AC-2 spec
// (content = sha256(json.Marshal(e)); chain = sha256(prev||content); genesis =
// sha256("boltrope-audit-genesis-v1:"+sid)) using ONLY stdlib, independent of
// the helpers under test, so a bug in a helper cannot make its own golden pass.
func TestFoldStream_KnownGoodPerSeqBytes(t *testing.T) {
	const sid = "sess-golden"
	events := []Event{
		TurnStarted{TurnID: "t-1", Model: "claude"},
		ApprovalRequested{
			TurnID: "t-1", CallID: "c-1", ToolName: "bash", Reason: "mutating",
			Args: map[string]any{"z": 1, "a": "two", "m": true},
		},
		TurnFinished{TurnID: "t-1", Reason: Success, NumTurns: 1},
	}

	// Independent expectation: marshal + sha256 + fold using raw stdlib.
	wantPrev := sha256.Sum256([]byte("boltrope-audit-genesis-v1:" + sid))
	prev := wantPrev[:]
	for i, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("json.Marshal(%T): %v", e, err)
		}
		wantContentArr := sha256.Sum256(raw)
		wantContent := wantContentArr[:]
		wantChainArr := sha256.Sum256(append(append([]byte{}, prev...), wantContent...))
		wantChain := wantChainArr[:]

		// The helpers must reproduce both digests byte-for-byte.
		gotPayload, err := MarshalEventPayload(e)
		if err != nil {
			t.Fatalf("MarshalEventPayload(%T): %v", e, err)
		}
		gotContent := ContentHash(gotPayload)
		gotChain := ChainHash(prev, gotContent)

		if !bytes.Equal(gotContent, wantContent) {
			t.Fatalf("seq %d content_hash = %x, want %x", i+1, gotContent, wantContent)
		}
		if !bytes.Equal(gotChain, wantChain) {
			t.Fatalf("seq %d chain_hash = %x, want %x", i+1, gotChain, wantChain)
		}
		prev = wantChain
	}
}

// mustMarshalContent marshals an event via the shared helper and returns its
// content hash, failing the test on a marshal error.
func mustMarshalContent(t *testing.T, e Event) []byte {
	t.Helper()
	p, err := MarshalEventPayload(e)
	if err != nil {
		t.Fatalf("MarshalEventPayload(%T): %v", e, err)
	}
	return ContentHash(p)
}

// TestChainVerification_ZeroValue documents the pure result struct shape
// consumed by the store, dev store, and gRPC facade (AC-7): a zero value is the
// "nothing checked" state, and the fields carry the verify outcome.
func TestChainVerification_ZeroValue(t *testing.T) {
	var v ChainVerification
	if v.Valid || v.FirstBadSeq != 0 || v.Reason != "" || v.Checked != 0 {
		t.Fatalf("zero ChainVerification not empty: %+v", v)
	}
	v = ChainVerification{Valid: true, Checked: 3}
	if !v.Valid || v.Checked != 3 {
		t.Fatalf("ChainVerification field round-trip failed: %+v", v)
	}
}
