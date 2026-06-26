//go:build integration

package eventstore

// RED (test-first) integration tests for Batch-5B's tamper-PROOF check:
// Store.VerifyAuditCheckpoints(ctx, verifier) (AC-9, AC-10). It is operator-tier
// — it reads the GLOBAL audit_checkpoints + events.content_hash across tenants on
// the OWNER connection, distinct from the tenant-RLS-scoped VerifyChainIntegrity.
//
// Authored BEFORE the implementation; it references symbols that do NOT exist yet
// — Store.VerifyAuditCheckpoints, the owner-tier Store constructor it needs,
// domain.CheckpointVerification, auditsign.NewSigner/NewVerifier — so the package
// does NOT compile. That absence is the RED proof.
//
// The HEADLINE (AC-10): seed chained events, sign checkpoints over their
// content_hashes, assert VerifyAuditCheckpoints is Valid; THEN perform a FULL
// in-DB rewrite of one covered event with a CONSISTENT chain (so
// VerifyChainIntegrity would still PASS) and assert VerifyAuditCheckpoints is now
// Valid=false — the signed checkpoint catches what the in-DB chain alone cannot.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/auditsign"
	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// ckMapSecrets is a trivial secret.SecretsPort over a map for the signer/verifier.
type ckMapSecrets struct{ m map[string]string }

func (s ckMapSecrets) Get(_ context.Context, name string) (secret.Secret, error) {
	v, ok := s.m[name]
	if !ok || v == "" {
		return secret.Secret{}, errors.Join(secret.ErrNotFound)
	}
	return secret.New(v), nil
}

// ckSecrets mints a fresh Ed25519 keypair and returns a secrets port carrying the
// seed, key id, and derived public key.
func ckSecrets(t *testing.T) (ckMapSecrets, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return ckMapSecrets{m: map[string]string{
		auditsign.EnvSigningKey:   base64.StdEncoding.EncodeToString(priv.Seed()),
		auditsign.EnvSigningKeyID: "it-key",
		auditsign.EnvPublicKey:    base64.StdEncoding.EncodeToString(pub),
	}}, pub
}

// signCheckpointsOverEvents signs ONE checkpoint covering the GLOBAL range
// [from,to] over the events' content_hashes (genesis-seeded), inserting an
// audit_checkpoints row via the owner connection — modeling what the AuditSigner
// projector produces, so the verify test does not depend on the projection pkg.
func signCheckpointsOverEvents(ctx context.Context, t *testing.T, owner *pgx.Conn, signer *auditsign.Signer) {
	t.Helper()
	rows, err := owner.Query(ctx,
		"SELECT global_id, content_hash FROM events WHERE content_hash IS NOT NULL ORDER BY global_id")
	if err != nil {
		t.Fatalf("read content_hashes: %v", err)
	}
	var leaves [][]byte
	var from, to int64
	first := true
	for rows.Next() {
		var gid int64
		var ch []byte
		if err := rows.Scan(&gid, &ch); err != nil {
			rows.Close()
			t.Fatalf("scan: %v", err)
		}
		if first {
			from = gid
			first = false
		}
		to = gid
		leaves = append(leaves, ch)
	}
	rows.Close()
	if len(leaves) == 0 {
		t.Fatal("no chained events to checkpoint")
	}

	hash := domain.CheckpointHash(domain.CheckpointGenesisPrev(), domain.LeavesDigest(leaves))
	sig, keyID := signer.Sign(hash)
	if _, err := owner.Exec(ctx, `
		INSERT INTO audit_checkpoints
			(prev_checkpoint_hash, checkpoint_hash, covers_from_global_id, covers_to_global_id, leaf_count, signature, key_id, algo)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'ed25519')`,
		domain.CheckpointGenesisPrev(), hash, from, to, len(leaves), sig, keyID); err != nil {
		t.Fatalf("insert audit_checkpoint: %v", err)
	}
}

// seedChainedEventsOwner inserts n chained events (content_hash + chain_hash)
// through the owner connection and bumps head_seq/chain_head.
func seedChainedEventsOwner(ctx context.Context, t *testing.T, owner *pgx.Conn, tenantID, sessionID string, n int) {
	t.Helper()
	prev := domain.GenesisChainHash(sessionID)
	for seq := int64(1); seq <= int64(n); seq++ {
		payload := []byte(`{"TurnID":"t","Model":"m"}`)
		content := domain.ContentHash(payload)
		chain := domain.ChainHash(prev, content)
		if _, err := owner.Exec(ctx, `
			INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload, payload_canonical, content_hash, chain_hash, actor)
			VALUES ($1, $2, $3, $4, 'TurnStarted', 1, $5::jsonb, $6, $7, $8, 'system')`,
			tenantID, sessionID, seq, newUUID(t), payload, payload, content, chain); err != nil {
			t.Fatalf("seed chained event seq=%d: %v", seq, err)
		}
		prev = chain
	}
	if _, err := owner.Exec(ctx, "UPDATE sessions SET head_seq=$1, chain_head=$2 WHERE id=$3", n, prev, sessionID); err != nil {
		t.Fatalf("bump head_seq/chain_head: %v", err)
	}
}

// TestVerifyAuditCheckpoints_UntamperedIsValid (AC-9): a signed checkpoint over
// untampered events verifies Valid=true with Checked>=1.
func TestVerifyAuditCheckpoints_UntamperedIsValid(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := h.seedTenantAndSession(t)
	owner := h.ownerConn(t)
	seedChainedEventsOwner(ctx, t, owner, tenantID, sessionID, 4)

	secrets, _ := ckSecrets(t)
	signer, err := auditsign.NewSigner(ctx, secrets)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signCheckpointsOverEvents(ctx, t, owner, signer)

	verifier, err := auditsign.NewVerifier(ctx, secrets)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	res, err := h.store.VerifyAuditCheckpoints(ctx, verifier)
	if err != nil {
		t.Fatalf("VerifyAuditCheckpoints: %v", err)
	}
	if !res.Valid {
		t.Fatalf("untampered checkpoints verified Valid=false (FirstBad=%d, Reason=%q)", res.FirstBadCheckpointID, res.Reason)
	}
	if res.Checked < 1 {
		t.Fatalf("Checked = %d, want >=1", res.Checked)
	}
}

// TestVerifyAuditCheckpoints_CatchesConsistentFullRewrite is the HEADLINE
// (AC-10): after signing, a FULL in-DB rewrite of one covered event with a
// CONSISTENT chain (so VerifyChainIntegrity would PASS) makes
// VerifyAuditCheckpoints Valid=false — the signature anchored OUTSIDE the events
// DB catches what the in-DB chain alone cannot.
func TestVerifyAuditCheckpoints_CatchesConsistentFullRewrite(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := h.seedTenantAndSession(t)
	owner := h.ownerConn(t)
	seedChainedEventsOwner(ctx, t, owner, tenantID, sessionID, 4)

	secrets, _ := ckSecrets(t)
	signer, err := auditsign.NewSigner(ctx, secrets)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signCheckpointsOverEvents(ctx, t, owner, signer)

	verifier, err := auditsign.NewVerifier(ctx, secrets)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	// Sanity: clean -> valid.
	clean, err := h.store.VerifyAuditCheckpoints(ctx, verifier)
	if err != nil {
		t.Fatalf("verify clean: %v", err)
	}
	if !clean.Valid {
		t.Fatalf("clean verify not Valid (Reason=%q)", clean.Reason)
	}

	// FULL in-DB rewrite of seq 2 with a CONSISTENT chain (re-link downstream +
	// chain_head). This is exactly what the in-DB chain alone cannot detect: after
	// this, VerifyChainIntegrity would PASS.
	rewriteConsistently(ctx, t, owner, sessionID, 2)

	// The in-DB chain is internally consistent again (the in-DB-only check would
	// pass) — but the SIGNED checkpoint over the OLD content_hashes must fail.
	clean2, err := h.store.VerifyChainIntegrity(tenantCtx(tenantID), sessionID, 0, 0)
	if err != nil {
		t.Fatalf("VerifyChainIntegrity after consistent rewrite: %v", err)
	}
	if !clean2.Valid {
		t.Fatalf("the rewrite was not internally consistent (VerifyChainIntegrity Valid=false); the headline needs a consistent rewrite, got Reason=%q", clean2.Reason)
	}

	bad, err := h.store.VerifyAuditCheckpoints(ctx, verifier)
	if err != nil {
		t.Fatalf("verify tampered: %v", err)
	}
	if bad.Valid {
		t.Fatal("VerifyAuditCheckpoints Valid=true after a CONSISTENT full rewrite — the signed checkpoint failed to anchor the content (HEADLINE FAILED)")
	}
	if bad.FirstBadCheckpointID == 0 {
		t.Fatal("FirstBadCheckpointID = 0 on an invalid result; the bad checkpoint must be identified")
	}
	r := strings.ToLower(bad.Reason)
	if !strings.Contains(r, "signature") && !strings.Contains(r, "leaf") && !strings.Contains(r, "hash") {
		t.Fatalf("Reason = %q, want a signature/leaf/hash-mismatch classification", bad.Reason)
	}
}

// TestVerifyAuditCheckpoints_EmptyIsValid (AC-9): no checkpoints -> Valid=true,
// Checked=0 (nothing to anchor yet is not a tamper).
func TestVerifyAuditCheckpoints_EmptyIsValid(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	secrets, _ := ckSecrets(t)
	verifier, err := auditsign.NewVerifier(ctx, secrets)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	res, err := h.store.VerifyAuditCheckpoints(ctx, verifier)
	if err != nil {
		t.Fatalf("VerifyAuditCheckpoints (empty): %v", err)
	}
	if !res.Valid || res.Checked != 0 {
		t.Fatalf("empty audit_checkpoints = %+v, want Valid=true Checked=0", res)
	}
}

// TestVerifyAuditCheckpoints_DetectsForgedSignature (AC-9): a checkpoint whose
// signature was produced by a DIFFERENT key (wrong key / forgery) is reported
// signature-invalid.
func TestVerifyAuditCheckpoints_DetectsForgedSignature(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := h.seedTenantAndSession(t)
	owner := h.ownerConn(t)
	seedChainedEventsOwner(ctx, t, owner, tenantID, sessionID, 3)

	// Sign with key A, but verify with key B (the configured deployment key).
	secretsA, _ := ckSecrets(t)
	signerA, err := auditsign.NewSigner(ctx, secretsA)
	if err != nil {
		t.Fatalf("NewSigner A: %v", err)
	}
	signCheckpointsOverEvents(ctx, t, owner, signerA)

	secretsB, _ := ckSecrets(t)
	// Force the row's key_id to match B's id so the verifier picks B's public key
	// and the signature (made by A) fails to verify.
	if _, err := owner.Exec(ctx, "UPDATE audit_checkpoints SET key_id=$1", auditsign.EnvSigningKeyID); err != nil {
		t.Fatalf("retag key_id: %v", err)
	}
	verifierB, err := auditsign.NewVerifier(ctx, secretsB)
	if err != nil {
		t.Fatalf("NewVerifier B: %v", err)
	}

	res, err := h.store.VerifyAuditCheckpoints(ctx, verifierB)
	if err != nil {
		t.Fatalf("VerifyAuditCheckpoints: %v", err)
	}
	if res.Valid {
		t.Fatal("a checkpoint signed by a different key verified Valid=true (forgery not caught)")
	}
	if !strings.Contains(strings.ToLower(res.Reason), "signature") {
		t.Fatalf("Reason = %q, want a signature-invalid classification", res.Reason)
	}
}

// rewriteConsistently performs a FULL in-DB rewrite of one event keeping the
// in-DB hash-chain internally consistent (VerifyChainIntegrity would pass): it
// rewrites payload/payload_canonical, recomputes content_hash + chain_hash from
// the predecessor, re-links every downstream chain_hash, and updates chain_head.
func rewriteConsistently(ctx context.Context, t *testing.T, owner *pgx.Conn, sessionID string, seq int64) {
	t.Helper()
	var prev []byte
	if seq == 1 {
		prev = domain.GenesisChainHash(sessionID)
	} else if err := owner.QueryRow(ctx, "SELECT chain_hash FROM events WHERE session_id=$1 AND seq=$2", sessionID, seq-1).Scan(&prev); err != nil {
		t.Fatalf("read predecessor chain_hash: %v", err)
	}

	newPayload := []byte(`{"TurnID":"REWRITTEN","Model":"evil"}`)
	content := domain.ContentHash(newPayload)
	chain := domain.ChainHash(prev, content)
	if _, err := owner.Exec(ctx,
		"UPDATE events SET payload=$1::jsonb, payload_canonical=$2, content_hash=$3, chain_hash=$4 WHERE session_id=$5 AND seq=$6",
		newPayload, newPayload, content, chain, sessionID, seq); err != nil {
		t.Fatalf("rewrite seq %d: %v", seq, err)
	}

	prev = chain
	rows, err := owner.Query(ctx, "SELECT seq, content_hash FROM events WHERE session_id=$1 AND seq>$2 ORDER BY seq", sessionID, seq)
	if err != nil {
		t.Fatalf("read downstream: %v", err)
	}
	type ev struct {
		seq     int64
		content []byte
	}
	var ds []ev
	for rows.Next() {
		var e ev
		if err := rows.Scan(&e.seq, &e.content); err != nil {
			rows.Close()
			t.Fatalf("scan downstream: %v", err)
		}
		ds = append(ds, e)
	}
	rows.Close()
	for _, e := range ds {
		c := domain.ChainHash(prev, e.content)
		if _, err := owner.Exec(ctx, "UPDATE events SET chain_hash=$1 WHERE session_id=$2 AND seq=$3", c, sessionID, e.seq); err != nil {
			t.Fatalf("re-link seq %d: %v", e.seq, err)
		}
		prev = c
	}
	if _, err := owner.Exec(ctx, "UPDATE sessions SET chain_head=$1 WHERE id=$2", prev, sessionID); err != nil {
		t.Fatalf("update chain_head: %v", err)
	}
}

// _ keeps the domain import referenced (the result type the store returns).
var _ = domain.CheckpointVerification{}
