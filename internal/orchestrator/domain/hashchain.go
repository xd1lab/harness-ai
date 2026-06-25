// SPDX-License-Identifier: Apache-2.0

package domain

import (
	"crypto/sha256"
	"encoding/json"
)

// Tamper-evident audit hash-chain primitives (Batch-5A, research item BIG-C).
//
// This file is the SINGLE, pgx-free source of truth for the per-event content
// hash and the per-session SHA-256 hash-CHAIN. Both the production pgx event
// store and the pgx-free cmd/boltrope-dev in-memory store import these helpers,
// so dev/prod parity holds by construction: an identical stream of typed
// [Event] values yields byte-identical content_hash and chain_hash on either
// side (ADR-0033).
//
// The package stays dependency-light — only crypto/sha256 and encoding/json
// (stdlib) — so the domain's no-dependency posture is preserved and the helpers
// never pull pgx into the dev binary.
//
// Design (ADR-0033):
//
//   - content_hash = SHA-256 over the EXACT bytes stored in events.payload
//     (i.e. [MarshalEventPayload], which the prod store's marshalPayload calls,
//     so verify-on-read recomputes from the identical bytes).
//   - chain_hash   = SHA-256( prev_chain_hash || content_hash ), folded in
//     per-session contiguous seq order. The first chained event of a session
//     seeds prev from [GenesisChainHash] (a session-derived genesis, so two
//     sessions never share a chain).
//
// The chain is per-session (not global): it aligns with seq contiguity, RLS
// tenant isolation, and the session being the audit unit.

// genesisPrefix is the fixed domain-separation prefix mixed into the
// session-derived genesis seed. Versioned so a future chain-algorithm change
// can adopt a fresh genesis namespace without colliding with v1 chains.
const genesisPrefix = "boltrope-audit-genesis-v1:"

// MarshalEventPayload encodes a typed [Event] payload to the byte-identical JSON
// the events.payload column stores. It is json.Marshal(e): deterministic
// because encoding/json encodes struct fields in declaration order and SORTS
// map keys, so even a map-bearing payload ([ApprovalRequested.Args],
// map[string]any) marshals stably and hashes deterministically.
//
// The production store's marshalPayload MUST delegate to this so prod hashes the
// identical bytes it persists, and verify-on-read recomputes the same digest.
func MarshalEventPayload(e Event) ([]byte, error) {
	return json.Marshal(e)
}

// ContentHash returns SHA-256 of the exact stored payload bytes as a fresh
// 32-byte slice (the per-event content digest). It is computed over the bytes
// [MarshalEventPayload] produced — i.e. the bytes actually persisted — so a
// recompute from the stored payload re-derives the identical digest.
func ContentHash(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	return sum[:]
}

// GenesisChainHash returns the per-session chain genesis: SHA-256 over a fixed
// domain-separation prefix concatenated with the session id. It seeds the chain
// for the FIRST chained event of a session, so two distinct sessions never share
// a genesis and a per-session chain is not confusable with another's.
func GenesisChainHash(sessionID string) []byte {
	sum := sha256.Sum256([]byte(genesisPrefix + sessionID))
	return sum[:]
}

// ChainHash folds one event into the chain: SHA-256( prevChainHash || contentHash ),
// returned as a fresh 32-byte slice. prevChainHash is [GenesisChainHash] for the
// first chained event of a session, else the prior event's chain_hash.
//
// CRITICAL non-aliasing contract: the fold does NOT use the naive
// sha256.Sum256(append(prevChainHash, contentHash...)). A bare append MUTATES
// the caller's prevChainHash backing array whenever cap > len — and the append
// path reuses a single 32-byte running-head slice across the batch, so a bare
// append would clobber the running head and silently corrupt every subsequent
// link. We instead copy prev into a fresh buffer FIRST, so prevChainHash is
// never written.
func ChainHash(prevChainHash, contentHash []byte) []byte {
	buf := make([]byte, 0, len(prevChainHash)+len(contentHash))
	buf = append(buf, prevChainHash...)
	buf = append(buf, contentHash...)
	sum := sha256.Sum256(buf)
	return sum[:]
}

// ChainVerification is the pure, transport-agnostic result of recomputing a
// session's hash-chain over a seq window and comparing it to the stored hashes
// (ADR-0033). It is produced by the store's VerifyChainIntegrity (prod and dev)
// and mapped onto the VerifySessionIntegrityResponse on the read-plane.
//
// A clean range yields Valid=true, FirstBadSeq=0, Checked=number of CHAINED
// events verified. The first mismatch yields Valid=false, FirstBadSeq=the bad
// seq, and a Reason classifying a content-hash mismatch (a tampered payload)
// versus a broken link (a rewritten chain_hash). A contiguous leading prefix of
// pre-0009 NULL-hash rows is skipped, not reported as tampered.
type ChainVerification struct {
	// Valid reports whether the verified window's hashes all matched the
	// recomputed chain.
	Valid bool
	// FirstBadSeq is the seq of the first event whose recomputed hash did not
	// match its stored hash; 0 when Valid is true.
	FirstBadSeq int64
	// Reason classifies the failure (content-hash mismatch vs broken link), or
	// is empty when Valid is true.
	Reason string
	// Checked is the number of CHAINED events verified (the leading NULL-hash
	// prefix, if any, is excluded).
	Checked int
}
