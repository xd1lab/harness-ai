//go:build integration

package projection

// RED (test-first) integration tests for Batch-5B's signed audit-checkpoint
// chain vs REAL Postgres (AC-8, AC-10 — the HEADLINE proof). Authored BEFORE the
// implementation; they reference symbols that do NOT exist yet — AuditSigner,
// NewAuditSigner, WithAuditSigner, eventstore-side VerifyAuditCheckpoints
// (exercised via the SQL the store will use), domain.CheckpointHash/LeavesDigest,
// auditsign.NewSigner/NewVerifier — so the package does NOT compile. That absence
// is the RED proof.
//
// The HEADLINE (AC-10): seed chained events, run the signer to produce a signed
// checkpoint, assert it verifies; THEN simulate a FULL in-DB rewrite of one
// covered event (UPDATE payload_canonical + content_hash + chain_hash + downstream
// chain_hash + sessions.chain_head via the owner connection so the in-DB hash-chain
// is internally CONSISTENT again) and assert the SIGNED checkpoint now fails to
// verify — i.e. the signature anchored OUTSIDE the events DB catches what the
// in-DB chain alone cannot.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/auditsign"
	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// seedChainedEvents inserts n events with content_hash + chain_hash computed via
// the domain helpers (per-session chain), bumping head_seq, all through the owner
// connection (operator-tier). It returns the per-seq content_hash leaves keyed by
// global_id ascending so the test can recompute the expected checkpoint.
func seedChainedEvents(ctx context.Context, t *testing.T, h *pharness, tenantID, sessionID string, n int) {
	t.Helper()
	prev := domain.GenesisChainHash(sessionID)
	for seq := int64(1); seq <= int64(n); seq++ {
		payload := []byte(`{"TurnID":"t` + itoa(seq) + `","Model":"m"}`)
		content := domain.ContentHash(payload)
		chain := domain.ChainHash(prev, content)
		if _, err := h.conn.Exec(ctx, `
			INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload, payload_canonical, content_hash, chain_hash, actor)
			VALUES ($1, $2, $3, $4, 'TurnStarted', 1, $5::jsonb, $6, $7, $8, 'system')`,
			tenantID, sessionID, seq, newUUID(t), payload, payload, content, chain); err != nil {
			t.Fatalf("seed chained event seq=%d: %v", seq, err)
		}
		prev = chain
	}
	if _, err := h.conn.Exec(ctx, "UPDATE sessions SET head_seq = $1, chain_head = $2 WHERE id = $3", n, prev, sessionID); err != nil {
		t.Fatalf("bump head_seq/chain_head: %v", err)
	}
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// mapSecrets is a trivial secret.SecretsPort over a map for the integration
// signer/verifier wiring (present name -> value; absent -> secret.ErrNotFound).
type mapSecrets struct{ m map[string]string }

func (s mapSecrets) Get(_ context.Context, name string) (secret.Secret, error) {
	v, ok := s.m[name]
	if !ok || v == "" {
		return secret.Secret{}, errors.Join(secret.ErrNotFound)
	}
	return secret.New(v), nil
}

// signerSecrets builds an in-process secrets port carrying a fresh Ed25519 seed
// and key id (and the derived public key) so both signer and verifier resolve it.
func signerSecrets(t *testing.T) (secrets mapSecrets, pub ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	seedB64 := base64.StdEncoding.EncodeToString(priv.Seed())
	return mapSecrets{m: map[string]string{
		auditsign.EnvSigningKey:   seedB64,
		auditsign.EnvSigningKeyID: "it-key",
		auditsign.EnvPublicKey:    base64.StdEncoding.EncodeToString(pub),
	}}, pub
}

// TestAuditSigner_ProducesSignedCheckpointsOverContentHashes_Integration
// (AC-7/AC-10 producer half): the signer Runner reads the GLOBAL feed (with the
// content_hash column the extended Source now exposes), produces >=1 signed
// audit_checkpoints row, and every stored signature verifies against the
// recomputed checkpoint_hash (genesis-seeded, prev-chained). The verify+rewrite
// HEADLINE that proves a CONSISTENT rewrite is caught lives in the eventstore
// VerifyAuditCheckpoints integration test (it owns the Store method).
func TestAuditSigner_ProducesSignedCheckpointsOverContentHashes_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)
	seedChainedEvents(ctx, t, h, tenantID, sessionID, 4) // seq 1..4

	secrets, _ := signerSecrets(t)
	signer, err := auditsign.NewSigner(ctx, secrets)
	if err != nil {
		t.Fatalf("auditsign.NewSigner: %v", err)
	}

	// Run the signer over the whole feed with a small checkpoint-every so at least
	// one checkpoint covers the seeded events.
	r := NewRunner(
		Config{Subscription: "audit-checkpoint", BatchSize: 100},
		NewSource(h.conn),
		WithAuditSigner(NewAuditSigner(h.conn, signer, 2)),
	)
	if err := r.runOnce(ctx); err != nil {
		t.Fatalf("runOnce (signer): %v", err)
	}

	verifier, err := auditsign.NewVerifier(ctx, secrets)
	if err != nil {
		t.Fatalf("auditsign.NewVerifier: %v", err)
	}

	// Recompute and verify every stored checkpoint (genesis-seeded, prev-chained).
	rows, err := h.conn.Query(ctx,
		"SELECT id, prev_checkpoint_hash, checkpoint_hash, covers_from_global_id, covers_to_global_id, signature, key_id FROM audit_checkpoints ORDER BY id")
	if err != nil {
		t.Fatalf("read audit_checkpoints: %v", err)
	}
	type ck struct {
		id        int64
		prev      []byte
		hash      []byte
		from, to  int64
		signature []byte
		keyID     string
	}
	var cks []ck
	for rows.Next() {
		var c ck
		if err := rows.Scan(&c.id, &c.prev, &c.hash, &c.from, &c.to, &c.signature, &c.keyID); err != nil {
			rows.Close()
			t.Fatalf("scan checkpoint: %v", err)
		}
		cks = append(cks, c)
	}
	rows.Close()
	if len(cks) == 0 {
		t.Fatal("signer produced no audit_checkpoints rows")
	}

	var expectedPrev = domain.CheckpointGenesisPrev()
	for _, c := range cks {
		// Read the covered content_hash leaves in ascending global_id order.
		leaves := readLeaves(ctx, t, h, c.from, c.to)
		want := domain.CheckpointHash(expectedPrev, domain.LeavesDigest(leaves))
		if string(want) != string(c.hash) {
			t.Fatalf("checkpoint %d stored hash %x != recomputed %x", c.id, c.hash, want)
		}
		if !verifier.Verify(c.hash, c.signature, c.keyID) {
			t.Fatalf("checkpoint %d signature does not verify (anchoring broken)", c.id)
		}
		expectedPrev = c.hash
	}
}

// readLeaves returns the content_hash bytes for events in [from,to] global_id
// ascending (the checkpoint's covered leaves), skipping NULLs.
func readLeaves(ctx context.Context, t *testing.T, h *pharness, from, to int64) [][]byte {
	t.Helper()
	rows, err := h.conn.Query(ctx,
		"SELECT content_hash FROM events WHERE global_id BETWEEN $1 AND $2 AND content_hash IS NOT NULL ORDER BY global_id", from, to)
	if err != nil {
		t.Fatalf("read leaves: %v", err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan leaf: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// TestAuditSigner_IdempotentReRun_Integration (AC-8): re-running the signer over
// already-covered events does NOT double-write checkpoints or break the prev
// chain (ON CONFLICT (covers_to_global_id) DO NOTHING + reload-on-restart).
func TestAuditSigner_IdempotentReRun_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)
	seedChainedEvents(ctx, t, h, tenantID, sessionID, 4)

	secrets, _ := signerSecrets(t)
	signer, err := auditsign.NewSigner(ctx, secrets)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	run := func() {
		r := NewRunner(
			Config{Subscription: "audit-checkpoint", BatchSize: 100},
			NewSource(h.conn),
			WithAuditSigner(NewAuditSigner(h.conn, signer, 2)),
		)
		if err := r.runOnce(ctx); err != nil {
			t.Fatalf("runOnce: %v", err)
		}
	}

	run()
	var after1 int
	if err := h.conn.QueryRow(ctx, "SELECT COUNT(*) FROM audit_checkpoints").Scan(&after1); err != nil {
		t.Fatalf("count after run1: %v", err)
	}

	// Reset the signer subscription cursor to 0 and re-run: the same global_ids are
	// re-read, but ON CONFLICT (covers_to_global_id) makes them no-ops.
	if _, err := h.conn.Exec(ctx,
		"UPDATE event_subscriptions SET last_transaction_id='0'::xid8, last_global_id=0 WHERE name=$1", "audit-checkpoint"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	run()
	var after2 int
	if err := h.conn.QueryRow(ctx, "SELECT COUNT(*) FROM audit_checkpoints").Scan(&after2); err != nil {
		t.Fatalf("count after run2: %v", err)
	}
	if after1 != after2 {
		t.Fatalf("checkpoint count changed across replay %d -> %d (signer not idempotent)", after1, after2)
	}
}
