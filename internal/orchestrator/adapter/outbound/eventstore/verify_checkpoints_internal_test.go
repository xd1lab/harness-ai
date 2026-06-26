// SPDX-License-Identifier: Apache-2.0

package eventstore

// Pure (no-Postgres) unit tests for the verify-checkpoints classifier
// (classifyCheckpoints): the tamper-PROOF check's classification logic in
// isolation, driven by in-memory checkpoint rows + an in-memory leaf lookup + a
// fake verifier. The real-Postgres proof (AC-10) lives in
// verify_checkpoints_integration_test.go; these pin the CLASSIFICATION (valid,
// broken prev-link, leaf/hash mismatch, signature-invalid, empty) without a DB.

import (
	"crypto/sha256"
	"strconv"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// fakeVerifier is a [CheckpointVerifier] whose Verify result is scripted: it
// returns true only when the (checkpointHash, sig, keyID) triple was registered as
// "good". It lets the classifier test exercise the signature path without Ed25519.
type fakeVerifier struct {
	// accept reports, for a given key_id, whether sig is the "valid signature" over
	// checkpointHash. A nil func defaults to accept-all.
	good map[string]string // keyID -> hex(checkpointHash)+"|"+hex(sig) that verifies
}

func (f fakeVerifier) Verify(checkpointHash, sig []byte, keyID string) bool {
	if f.good == nil {
		return true
	}
	want, ok := f.good[keyID]
	if !ok {
		return false
	}
	return want == hexish(checkpointHash)+"|"+hexish(sig)
}

func hexish(b []byte) string {
	const hexdigits = "0123456789abcdef"
	var sb strings.Builder
	for _, x := range b {
		sb.WriteByte(hexdigits[x>>4])
		sb.WriteByte(hexdigits[x&0x0f])
	}
	return sb.String()
}

// leaf returns a deterministic 32-byte content_hash leaf for a global_id.
func leaf(gid int64) []byte {
	sum := sha256.Sum256([]byte("leaf:" + strconv.FormatInt(gid, 10)))
	return sum[:]
}

// buildChain constructs n checkpoints over the given per-checkpoint leaf ranges,
// signing each with a fake "signature" (= the checkpoint_hash itself, which the
// accept-all fakeVerifier admits). It returns the rows and the leaf lookup that
// returns each range's leaves.
func buildChain(t *testing.T, ranges [][2]int64) ([]auditCheckpointRow, func(from, to int64) ([][]byte, error)) {
	t.Helper()
	leavesByRange := map[[2]int64][][]byte{}
	for _, r := range ranges {
		var ls [][]byte
		for gid := r[0]; gid <= r[1]; gid++ {
			ls = append(ls, leaf(gid))
		}
		leavesByRange[r] = ls
	}
	lookup := func(from, to int64) ([][]byte, error) {
		return leavesByRange[[2]int64{from, to}], nil
	}

	var rows []auditCheckpointRow
	prev := domain.CheckpointGenesisPrev()
	for i, r := range ranges {
		hash := domain.CheckpointHash(prev, domain.LeavesDigest(leavesByRange[r]))
		var prevArg []byte
		if i > 0 {
			prevArg = rows[i-1].checkpointHash
		} // genesis row: NULL prev (nil)
		rows = append(rows, auditCheckpointRow{
			id:             int64(i + 1),
			prevHash:       prevArg,
			checkpointHash: hash,
			from:           r[0],
			to:             r[1],
			signature:      hash, // fake "signature" the accept-all verifier admits
			keyID:          "k1",
		})
		prev = hash
	}
	return rows, lookup
}

func TestClassifyCheckpoints_EmptyIsValid(t *testing.T) {
	res, err := classifyCheckpoints(nil, func(int64, int64) ([][]byte, error) { return nil, nil }, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify(empty): %v", err)
	}
	if !res.Valid || res.Checked != 0 || res.FirstBadCheckpointID != 0 {
		t.Fatalf("empty = %+v, want Valid=true Checked=0", res)
	}
}

func TestClassifyCheckpoints_ValidChain(t *testing.T) {
	rows, lookup := buildChain(t, [][2]int64{{1, 3}, {4, 6}, {7, 7}})
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if !res.Valid || res.Checked != 3 {
		t.Fatalf("valid chain = %+v, want Valid=true Checked=3", res)
	}
}

func TestClassifyCheckpoints_GenesisPrevConstantAccepted(t *testing.T) {
	// A producer that stores the explicit genesis constant (rather than NULL) on the
	// first row must still verify (both denote "genesis").
	rows, lookup := buildChain(t, [][2]int64{{1, 2}, {3, 4}})
	rows[0].prevHash = domain.CheckpointGenesisPrev()
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if !res.Valid {
		t.Fatalf("genesis-constant prev rejected: %+v", res)
	}
}

func TestClassifyCheckpoints_LeafTamperDetected(t *testing.T) {
	rows, _ := buildChain(t, [][2]int64{{1, 3}, {4, 6}})
	// Tamper the leaves the SECOND checkpoint covers: the lookup returns different
	// bytes than what its stored checkpoint_hash was computed over.
	lookup := func(from, to int64) ([][]byte, error) {
		if from == 4 {
			return [][]byte{leaf(99), leaf(98), leaf(97)}, nil
		}
		var ls [][]byte
		for gid := from; gid <= to; gid++ {
			ls = append(ls, leaf(gid))
		}
		return ls, nil
	}
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if res.Valid {
		t.Fatal("leaf tamper not detected (Valid=true)")
	}
	if res.FirstBadCheckpointID != 2 {
		t.Fatalf("FirstBadCheckpointID = %d, want 2", res.FirstBadCheckpointID)
	}
	if res.Checked != 1 {
		t.Fatalf("Checked = %d, want 1 (the first, clean checkpoint verified before the bad one)", res.Checked)
	}
	if !strings.Contains(res.Reason, "leaf/hash mismatch") {
		t.Fatalf("Reason = %q, want a leaf/hash mismatch", res.Reason)
	}
}

func TestClassifyCheckpoints_BrokenPrevLinkDetected(t *testing.T) {
	rows, lookup := buildChain(t, [][2]int64{{1, 2}, {3, 4}, {5, 6}})
	// Rewrite the 2nd row's prev so it no longer links to the 1st row's hash.
	rows[1].prevHash = leaf(12345)
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if res.Valid {
		t.Fatal("broken prev-link not detected")
	}
	if res.FirstBadCheckpointID != 2 {
		t.Fatalf("FirstBadCheckpointID = %d, want 2", res.FirstBadCheckpointID)
	}
	if !strings.Contains(res.Reason, "broken prev-link") {
		t.Fatalf("Reason = %q, want a broken prev-link", res.Reason)
	}
}

func TestClassifyCheckpoints_GenesisNonNullNonGenesisPrevDetected(t *testing.T) {
	rows, lookup := buildChain(t, [][2]int64{{1, 2}})
	rows[0].prevHash = leaf(777) // not NULL and not the genesis constant
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if res.Valid || res.FirstBadCheckpointID != 1 || !strings.Contains(res.Reason, "broken prev-link") {
		t.Fatalf("genesis bad prev = %+v, want broken prev-link at id 1", res)
	}
}

func TestClassifyCheckpoints_BadSignatureDetected(t *testing.T) {
	rows, lookup := buildChain(t, [][2]int64{{1, 2}, {3, 4}})
	// A verifier that admits ONLY the first checkpoint's (hash,sig) — the second
	// fails signature verification even though its hash recomputes correctly.
	good := map[string]string{
		"k1": hexish(rows[0].checkpointHash) + "|" + hexish(rows[0].signature),
	}
	res, err := classifyCheckpoints(rows, lookup, fakeVerifier{good: good})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if res.Valid {
		t.Fatal("bad signature not detected")
	}
	if res.FirstBadCheckpointID != 2 || res.Checked != 1 {
		t.Fatalf("res = %+v, want FirstBad=2 Checked=1", res)
	}
	if !strings.Contains(res.Reason, "signature") {
		t.Fatalf("Reason = %q, want a signature classification", res.Reason)
	}
}
