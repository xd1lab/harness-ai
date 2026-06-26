// SPDX-License-Identifier: Apache-2.0

package domain

// RED (test-first) unit tests for Batch-5B's pgx-free signed-checkpoint
// primitives (AC-4): the leaves-digest + checkpoint-hash formula and the
// versioned genesis constant. Authored BEFORE the implementation; they
// reference symbols that do NOT exist yet — CheckpointGenesisPrev,
// LeavesDigest, CheckpointHash, CheckpointVerification — so the domain package
// does NOT compile. That absence is the RED proof.
//
// Pinned formula (SPEC AC-4):
//
//	leavesDigest    = SHA-256( L1 || L2 || ... || Ln )   (raw 32-byte content_hash
//	                  leaves concatenated in ascending global_id order, NO separators)
//	checkpoint_hash = SHA-256( prev_checkpoint_hash || leavesDigest )
//	genesis prev    = SHA-256("boltrope-audit-checkpoint-genesis-v1:")  (versioned,
//	                  mirrors domain.genesisPrefix)
//
// The helper MUST use the same non-aliasing copy-first discipline as ChainHash:
// it never mutates the prev/running buffers.

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

// TestCheckpointGenesisPrev_IsVersionedConstant pins the genesis prev to the
// exact domain-separated, versioned constant (mirrors the chain genesis prefix).
func TestCheckpointGenesisPrev_IsVersionedConstant(t *testing.T) {
	want := sha256.Sum256([]byte("boltrope-audit-checkpoint-genesis-v1:"))
	got := CheckpointGenesisPrev()
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("CheckpointGenesisPrev() = %x, want %x (SHA-256 of the versioned genesis prefix)", got, want[:])
	}
	if len(got) != 32 {
		t.Fatalf("genesis prev len = %d, want 32", len(got))
	}
}

// TestLeavesDigest_ConcatsRawLeavesNoSeparator pins the leaves digest to
// SHA-256 of the raw 32-byte leaves concatenated with NO separator.
func TestLeavesDigest_ConcatsRawLeavesNoSeparator(t *testing.T) {
	l1 := bytes.Repeat([]byte{0x01}, 32)
	l2 := bytes.Repeat([]byte{0x02}, 32)
	l3 := bytes.Repeat([]byte{0x03}, 32)

	concat := append(append(append([]byte{}, l1...), l2...), l3...)
	want := sha256.Sum256(concat)

	got := LeavesDigest([][]byte{l1, l2, l3})
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("LeavesDigest = %x, want %x (SHA-256 of raw-concatenated leaves)", got, want[:])
	}
}

// TestLeavesDigest_OrderSensitive: reordering the leaves changes the digest
// (so a row-reorder rewrite is caught).
func TestLeavesDigest_OrderSensitive(t *testing.T) {
	l1 := bytes.Repeat([]byte{0xaa}, 32)
	l2 := bytes.Repeat([]byte{0xbb}, 32)

	a := LeavesDigest([][]byte{l1, l2})
	b := LeavesDigest([][]byte{l2, l1})
	if bytes.Equal(a, b) {
		t.Fatal("LeavesDigest is order-insensitive; reordering leaves must change the digest")
	}
}

// TestCheckpointHash_Formula pins checkpoint_hash = SHA-256(prev || leavesDigest)
// against a hand-computed vector.
func TestCheckpointHash_Formula(t *testing.T) {
	prev := bytes.Repeat([]byte{0x10}, 32)
	leaves := [][]byte{bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x02}, 32)}

	ld := LeavesDigest(leaves)
	wantBuf := append(append([]byte{}, prev...), ld...)
	want := sha256.Sum256(wantBuf)

	got := CheckpointHash(prev, ld)
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("CheckpointHash = %x, want %x (SHA-256(prev || leavesDigest))", got, want[:])
	}
}

// TestCheckpointHash_Deterministic: same inputs -> same output.
func TestCheckpointHash_Deterministic(t *testing.T) {
	prev := CheckpointGenesisPrev()
	ld := LeavesDigest([][]byte{bytes.Repeat([]byte{0x07}, 32)})
	if !bytes.Equal(CheckpointHash(prev, ld), CheckpointHash(prev, ld)) {
		t.Fatal("CheckpointHash is not deterministic for identical inputs")
	}
}

// TestCheckpointHash_DoesNotMutatePrev guards the non-aliasing copy-first
// discipline (mirrors ChainHash): a caller's prev buffer with spare capacity
// must NOT be written through by the fold.
func TestCheckpointHash_DoesNotMutatePrev(t *testing.T) {
	// prev has cap > len so a naive append would clobber the backing array.
	backing := make([]byte, 32, 128)
	for i := range backing {
		backing[i] = 0x42
	}
	prev := backing
	snapshot := append([]byte{}, prev...)

	ld := LeavesDigest([][]byte{bytes.Repeat([]byte{0x09}, 32)})
	_ = CheckpointHash(prev, ld)

	if !bytes.Equal(prev, snapshot) {
		t.Fatalf("CheckpointHash mutated the caller's prev buffer: %x -> %x", snapshot, prev)
	}
}

// TestCheckpointHash_ChangesWhenLeafChanges is the load-bearing property the
// signed checkpoint relies on: a rewritten event (changed content_hash leaf)
// changes leavesDigest -> changes checkpoint_hash, so the stored signature over
// the OLD checkpoint_hash no longer verifies the recomputed one.
func TestCheckpointHash_ChangesWhenLeafChanges(t *testing.T) {
	prev := CheckpointGenesisPrev()
	good := [][]byte{bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x02}, 32)}
	tampered := [][]byte{bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0xee}, 32)}

	hGood := CheckpointHash(prev, LeavesDigest(good))
	hBad := CheckpointHash(prev, LeavesDigest(tampered))
	if bytes.Equal(hGood, hBad) {
		t.Fatal("a changed leaf produced an identical checkpoint_hash; tamper would be undetectable")
	}
}

// TestCheckpointVerification_ZeroValue pins the result struct shape (AC-9):
// the zero value is an empty-but-valid classification placeholder; the fields
// must exist for the store/CLI to populate.
func TestCheckpointVerification_ZeroValue(t *testing.T) {
	var v CheckpointVerification
	if v.Valid || v.FirstBadCheckpointID != 0 || v.Reason != "" || v.Checked != 0 {
		t.Fatalf("zero CheckpointVerification = %+v, want all-zero", v)
	}
}
