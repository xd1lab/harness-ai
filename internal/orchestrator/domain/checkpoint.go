// SPDX-License-Identifier: Apache-2.0

package domain

import "crypto/sha256"

// Signed-audit-checkpoint formula primitives (Batch-5B, tamper-PROOF).
//
// Batch-5A made the in-DB log tamper-EVIDENT: a per-event content_hash and a
// per-session chain_hash (hashchain.go) let a verifier recompute the chain and
// catch a tampered payload or a spliced link. But an attacker with DB WRITE can
// recompute every content_hash/chain_hash to make a rewritten history internally
// consistent again — the in-DB chain alone cannot prove that.
//
// The signed CHECKPOINT chain closes that gap. A signer consumer reads the GLOBAL
// feed of per-event content_hash LEAVES in (transaction_id, global_id) order and,
// at each checkpoint boundary, folds the range's leaves into a checkpoint_hash that
// is then signed with an Ed25519 key held OUTSIDE the events DB. Because the
// signature anchors checkpoint_hash to a key the DB attacker does not hold, a
// rewritten event changes its content_hash leaf -> changes leavesDigest -> changes
// the recomputed checkpoint_hash -> the stored signature over the ORIGINAL hash no
// longer verifies. Tamper is PROVEN, not merely evident (ADR-0034).
//
// This file is the SINGLE, pgx-free source of truth for the checkpoint_hash
// FORMULA (AC-4). Like hashchain.go it stays stdlib-only (crypto/sha256) so the
// domain's no-dependency posture holds and both the signer (T5) and
// VerifyAuditCheckpoints (T6) recompute through the identical helpers.
//
// Formula (AC-4), pinned:
//
//	leavesDigest    = SHA-256( L1 || L2 || ... || Ln )
//	checkpoint_hash = SHA-256( prev_checkpoint_hash || leavesDigest )
//
// where L1..Ln are the raw 32-byte events.content_hash leaves of the covered
// range, concatenated in ASCENDING global_id order with NO separators, and the
// genesis prev for the FIRST checkpoint is [CheckpointGenesisPrev].

// checkpointGenesisPrefix is the fixed domain-separation prefix hashed to seed the
// FIRST checkpoint's prev. It is versioned (mirrors [genesisPrefix]) so a future
// checkpoint-algorithm change can adopt a fresh namespace, and it is DISTINCT from
// the per-session chain genesis prefix so a checkpoint genesis is never confusable
// with a session chain genesis.
const checkpointGenesisPrefix = "boltrope-audit-checkpoint-genesis-v1:"

// CheckpointGenesisPrev returns the genesis prev_checkpoint_hash for the FIRST
// checkpoint of the global chain: SHA-256 over the fixed, versioned,
// domain-separated prefix. A fresh 32-byte slice is returned on each call, so a
// caller mutating the result cannot corrupt a later call.
func CheckpointGenesisPrev() []byte {
	sum := sha256.Sum256([]byte(checkpointGenesisPrefix))
	return sum[:]
}

// LeavesDigest folds a checkpoint range's content_hash leaves into a single
// digest: SHA-256( L1 || L2 || ... || Ln ), returned as a fresh 32-byte slice.
//
// The leaves are the raw 32-byte events.content_hash bytes of the covered range in
// ASCENDING global_id order, concatenated with NO separators. The no-separator
// framing is unambiguous ONLY because every leaf is a uniform 32 bytes; callers
// MUST therefore SKIP nil / pre-0009 (unchained) content_hash leaves before
// calling — matching the verify path's leading-NULL skip — so a short or nil leaf
// can never shift the boundary between two leaves.
//
// CRITICAL non-aliasing contract (mirrors [ChainHash]): the concat copies each
// leaf into a fresh buffer and never writes through a caller's leaf slice.
func LeavesDigest(leaves [][]byte) []byte {
	total := 0
	for _, l := range leaves {
		total += len(l)
	}
	buf := make([]byte, 0, total)
	for _, l := range leaves {
		buf = append(buf, l...)
	}
	sum := sha256.Sum256(buf)
	return sum[:]
}

// CheckpointHash folds one checkpoint into the chain:
// SHA-256( prev || leavesDigest ), returned as a fresh 32-byte slice. prev is
// [CheckpointGenesisPrev] for the first checkpoint, else the prior checkpoint's
// stored checkpoint_hash.
//
// CRITICAL non-aliasing contract (mirrors [ChainHash]): prev is copied into a
// fresh buffer FIRST, so a signer that carries one running prev slice forward
// across checkpoints (prev = CheckpointHash(prev, ld)) cannot have an earlier
// head's backing array clobbered by a later fold.
func CheckpointHash(prev, leavesDigest []byte) []byte {
	buf := make([]byte, 0, len(prev)+len(leavesDigest))
	buf = append(buf, prev...)
	buf = append(buf, leavesDigest...)
	sum := sha256.Sum256(buf)
	return sum[:]
}

// CheckpointVerification is the pure, transport-agnostic result of verifying the
// signed audit-checkpoint chain (AC-9), produced by the operator-tier
// VerifyAuditCheckpoints (T6) and rendered by harnessctl (T8).
//
// For each checkpoint row in id order the verifier (a) re-reads the events'
// content_hash leaves over [covers_from_global_id, covers_to_global_id], recomputes
// leavesDigest + checkpoint_hash from the configured prev (genesis for the first,
// the prior row's stored checkpoint_hash otherwise) and asserts it matches the
// stored checkpoint_hash AND that prev_checkpoint_hash links to the prior row; and
// (b) verifies the Ed25519 signature of the recomputed checkpoint_hash against the
// public key for the row's key_id.
//
// An empty audit_checkpoints table yields Valid=true, Checked=0. The first failure
// yields Valid=false, FirstBadCheckpointID=the offending row's id, and a Reason
// classifying it: a leaf/hash mismatch (events tampered), a broken prev-link (the
// checkpoint chain itself was rewritten), or an invalid signature (forgery or wrong
// key — the load-bearing tamper-PROOF signal).
type CheckpointVerification struct {
	// Valid reports whether every verified checkpoint recomputed, linked, and
	// signature-verified correctly.
	Valid bool
	// FirstBadCheckpointID is the audit_checkpoints.id of the first failing
	// checkpoint; 0 when Valid is true.
	FirstBadCheckpointID int64
	// Reason classifies the first failure (leaf/hash mismatch, broken prev-link,
	// or invalid signature), or is empty when Valid is true.
	Reason string
	// Checked is the number of checkpoints verified.
	Checked int
}
