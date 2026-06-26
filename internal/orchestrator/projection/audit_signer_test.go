// SPDX-License-Identifier: Apache-2.0

package projection

// RED (test-first) unit tests for Batch-5B's audit-checkpoint signer sink
// (AC-2, AC-7, AC-16) and the EventRow content_hash/chain_hash extension.
// Authored BEFORE the implementation; they reference symbols that do NOT exist
// yet — EventRow.ContentHash/ChainHash, AuditSigner, NewAuditSigner, the signer
// dependency surface — so the package does NOT compile. That absence is the RED
// proof.
//
// Pinned (SPEC AC-2/AC-4/AC-7):
//   - EventRow gains ContentHash []byte and ChainHash []byte (nullable: nil for
//     pre-0009 rows); FetchBatch populates them from the new SELECT columns.
//   - The AuditSigner accumulates each batch's content_hash leaves in
//     (transaction_id, global_id) order and, at a checkpoint boundary, computes
//     leavesDigest/checkpoint_hash (domain.LeavesDigest/CheckpointHash), signs it
//     (the injected signer), and INSERTs an audit_checkpoints row with
//     ON CONFLICT (covers_to_global_id) DO NOTHING.
//   - Rows with nil content_hash are SKIPPED as leaves (not anchored).
//   - KEY SECRECY: the inserted row carries signature + key_id only, never the
//     private key.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// fakeSigner is the minimal signing surface the AuditSigner depends on. It
// records the hashes it was asked to sign so the test can assert the signer
// checkpointed the right checkpoint_hash, and signs with a real Ed25519 key so
// the signature is verifiable.
type fakeSigner struct {
	priv   ed25519.PrivateKey
	pub    ed25519.PublicKey
	keyID  string
	signed [][]byte
}

func newFakeSigner(t *testing.T) *fakeSigner {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return &fakeSigner{priv: priv, pub: pub, keyID: "test-key"}
}

func (f *fakeSigner) Sign(hash []byte) ([]byte, string) {
	cp := append([]byte{}, hash...)
	f.signed = append(f.signed, cp)
	return ed25519.Sign(f.priv, hash), f.keyID
}
func (f *fakeSigner) KeyID() string { return f.keyID }
func (f *fakeSigner) Algo() string  { return "ed25519" }

// TestEventRow_CarriesContentAndChainHash (AC-2): the additive fields exist and
// round-trip; the in-memory cost fold ignores them.
func TestEventRow_CarriesContentAndChainHash(t *testing.T) {
	ch := domain.ContentHash([]byte(`{"x":1}`))
	r := EventRow{GlobalID: 1, ContentHash: ch, ChainHash: ch}
	if r.ContentHash == nil || r.ChainHash == nil {
		t.Fatal("EventRow.ContentHash/ChainHash must be settable (additive fields for 5B)")
	}
	if string(r.ContentHash) != string(ch) {
		t.Fatalf("ContentHash round-trip mismatch")
	}
}

// TestFetchBatch_PopulatesContentAndChainHash (AC-2): the source SELECT gains
// content_hash + chain_hash and FetchBatch scans them into EventRow. A nullable
// (nil) hash for a pre-0009 row stays nil. Uses a hand-built rows fake that
// returns the extended column set.
func TestFetchBatch_PopulatesContentAndChainHash(t *testing.T) {
	content := domain.ContentHash([]byte(`{"TurnID":"t"}`))
	chain := domain.ChainHash(domain.GenesisChainHash("sess"), content)

	// Extended column order (AC-2): transaction_id::text, global_id, seq,
	// tenant_id, session_id, event_type, payload, content_hash, chain_hash.
	cols := [][]any{
		{uint64ToText(5), int64(7), int64(1), "ten", "sess", string(domain.EventTurnStarted), []byte(`{"TurnID":"t"}`), content, chain, "system", time.Time{}},
		// A pre-0009 row with NULL hashes.
		{uint64ToText(5), int64(8), int64(2), "ten", "sess", string(domain.EventTurnStarted), []byte(`{}`), []byte(nil), []byte(nil), "system", time.Time{}},
	}
	s := NewSource(&stubConn{rows: &fakeRows{cols: cols}})

	got, err := s.FetchBatch(context.Background(), Cursor{}, 10)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if string(got[0].ContentHash) != string(content) {
		t.Fatalf("row0 ContentHash = %x, want %x", got[0].ContentHash, content)
	}
	if string(got[0].ChainHash) != string(chain) {
		t.Fatalf("row0 ChainHash = %x, want %x", got[0].ChainHash, chain)
	}
	if got[1].ContentHash != nil || got[1].ChainHash != nil {
		t.Fatalf("pre-0009 row should keep nil hashes, got content=%x chain=%x", got[1].ContentHash, got[1].ChainHash)
	}
}

// TestAuditSigner_CheckpointsAtBoundary (AC-7): with a checkpoint-every of 2,
// projecting 2 leaves emits ONE checkpoint insert whose checkpoint_hash equals
// domain.CheckpointHash(genesis, LeavesDigest(leaves)) and is signed by the
// injected signer. The insert SQL targets audit_checkpoints and carries the
// idempotency ON CONFLICT clause.
func TestAuditSigner_CheckpointsAtBoundary(t *testing.T) {
	conn := &recordingConn{}
	signer := newFakeSigner(t)
	as := NewAuditSigner(conn, signer, 2) // checkpoint every 2 leaves

	l1 := domain.ContentHash([]byte(`{"a":1}`))
	l2 := domain.ContentHash([]byte(`{"b":2}`))
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, Seq: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnStarted, ContentHash: l1},
		{TransactionID: 1, GlobalID: 2, Seq: 2, TenantID: "t", SessionID: "s", Type: domain.EventTurnFinished, ContentHash: l2},
	}
	if err := as.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	// Exactly one checkpoint signed.
	if len(signer.signed) != 1 {
		t.Fatalf("signer asked to sign %d times, want 1 (one checkpoint boundary)", len(signer.signed))
	}
	wantHash := domain.CheckpointHash(domain.CheckpointGenesisPrev(), domain.LeavesDigest([][]byte{l1, l2}))
	if string(signer.signed[0]) != string(wantHash) {
		t.Fatalf("signed checkpoint hash = %x, want %x (SHA-256(genesis || leavesDigest))", signer.signed[0], wantHash)
	}

	// One INSERT into audit_checkpoints with the idempotency clause.
	var ckInserts []recordedExec
	for _, e := range conn.execs {
		if strings.Contains(e.sql, "audit_checkpoints") && strings.Contains(strings.ToUpper(e.sql), "INSERT") {
			ckInserts = append(ckInserts, e)
		}
	}
	if len(ckInserts) != 1 {
		t.Fatalf("got %d audit_checkpoints inserts, want 1", len(ckInserts))
	}
	if !strings.Contains(ckInserts[0].sql, "ON CONFLICT (covers_to_global_id) DO NOTHING") {
		t.Fatalf("checkpoint insert lacks the idempotency clause:\n%s", ckInserts[0].sql)
	}
	// covers_to_global_id = last leaf's global_id (2).
	if !argsContain(ckInserts[0], int64(2)) {
		t.Fatalf("insert args %v missing covers_to_global_id=2", ckInserts[0].args)
	}
}

// TestAuditSigner_SkipsNilContentHashLeaves (AC-7): rows with nil content_hash
// (pre-0009) are NOT folded into the leaves, so they do not anchor and do not
// corrupt the digest.
func TestAuditSigner_SkipsNilContentHashLeaves(t *testing.T) {
	conn := &recordingConn{}
	signer := newFakeSigner(t)
	as := NewAuditSigner(conn, signer, 2)

	l1 := domain.ContentHash([]byte(`{"a":1}`))
	l2 := domain.ContentHash([]byte(`{"b":2}`))
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, Seq: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnStarted, ContentHash: nil}, // skipped
		{TransactionID: 1, GlobalID: 2, Seq: 2, TenantID: "t", SessionID: "s", Type: domain.EventTurnStarted, ContentHash: l1},
		{TransactionID: 1, GlobalID: 3, Seq: 3, TenantID: "t", SessionID: "s", Type: domain.EventTurnFinished, ContentHash: l2},
	}
	if err := as.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}
	if len(signer.signed) != 1 {
		t.Fatalf("signed %d checkpoints, want 1", len(signer.signed))
	}
	// The two real leaves (skipping the nil) are L1,L2 in global_id order.
	wantHash := domain.CheckpointHash(domain.CheckpointGenesisPrev(), domain.LeavesDigest([][]byte{l1, l2}))
	if string(signer.signed[0]) != string(wantHash) {
		t.Fatalf("checkpoint hash included the nil leaf or wrong order: got %x want %x", signer.signed[0], wantHash)
	}
}

// TestAuditSigner_RowCarriesKeyIDNotPrivateKey (AC-16): the insert args carry
// the signer's key_id and the signature, but NEVER the private key bytes.
func TestAuditSigner_RowCarriesKeyIDNotPrivateKey(t *testing.T) {
	conn := &recordingConn{}
	signer := newFakeSigner(t)
	as := NewAuditSigner(conn, signer, 1) // every leaf

	l1 := domain.ContentHash([]byte(`{"a":1}`))
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, Seq: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnStarted, ContentHash: l1},
	}
	if err := as.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	var ck recordedExec
	for _, e := range conn.execs {
		if strings.Contains(e.sql, "audit_checkpoints") {
			ck = e
		}
	}
	if ck.sql == "" {
		t.Fatal("no audit_checkpoints insert recorded")
	}
	if !argsContain(ck, "test-key") {
		t.Fatalf("insert args %v missing key_id", ck.args)
	}
	// The raw private key must never be an argument.
	privBytes := []byte(signer.priv)
	for _, a := range ck.args {
		if b, ok := a.([]byte); ok && string(b) == string(privBytes) {
			t.Fatal("the private key was passed as an insert argument (KEY SECRECY violated)")
		}
	}
}

// TestAuditSigner_PartialFlushOnShortBatch (AC-7): a short (catch-up-to-head)
// batch with fewer than N accumulated leaves still flushes a partial checkpoint
// so the head is anchored promptly. A batch shorter than the configured runner
// batch size signals catch-up; the AuditSigner exposes a Flush hook the runner
// calls at the short-read boundary.
func TestAuditSigner_PartialFlushOnShortBatch(t *testing.T) {
	conn := &recordingConn{}
	signer := newFakeSigner(t)
	as := NewAuditSigner(conn, signer, 256) // large N: no boundary hit in one small batch

	l1 := domain.ContentHash([]byte(`{"a":1}`))
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, Seq: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnStarted, ContentHash: l1},
	}
	if err := as.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}
	// No boundary yet -> no checkpoint.
	if len(signer.signed) != 0 {
		t.Fatalf("partial accumulation should not checkpoint before Flush; signed %d", len(signer.signed))
	}
	// Flush (caught up to head) anchors the partial range.
	if err := as.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(signer.signed) != 1 {
		t.Fatalf("Flush should emit the partial checkpoint; signed %d, want 1", len(signer.signed))
	}
}

// TestAuditSigner_NoEgressBrokerImport (AC-14) parses audit_signer.go's import
// block and asserts it imports NOTHING from the toolruntime egress broker. The
// audit-checkpoint signer is OPERATOR-TIER infrastructure (like OTLP/metrics
// export); the egress broker governs MODEL-INFLUENCED channels only (ADR-0013 /
// ADR-0034), so any outbound it ever needs uses a plain net/http client, never the
// broker. A regression that pulled the broker into this read-side projector would
// blur that trust boundary.
func TestAuditSigner_NoEgressBrokerImport(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "audit_signer.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse audit_signer.go: %v", err)
	}
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if strings.Contains(path, "toolruntime") && strings.Contains(path, "egress") {
			t.Errorf("audit_signer.go must not import the egress broker %q (operator-tier, ADR-0034 trust boundary)", path)
		}
	}
}
