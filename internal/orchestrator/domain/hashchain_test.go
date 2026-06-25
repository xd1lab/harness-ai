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
