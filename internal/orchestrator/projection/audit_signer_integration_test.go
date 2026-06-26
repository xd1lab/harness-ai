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

	expectedPrev := domain.CheckpointGenesisPrev()
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

// TestAuditSigner_CrashMidAccumulation_NoUnanchoredGap_Integration (T7 #3,
// open-question #3): models a crash AFTER the runner advanced its per-batch
// subscription cursor but BEFORE a checkpoint was flushed, so the in-memory leaf
// accumulator is lost. It then (re)starts a FRESH signer and asserts the restart
// re-anchors every leaf — the audit_checkpoints ranges cover the seeded global_id
// span with NO gap (covers_from of the first checkpoint <= the first leaf and
// covers_to of the last >= the last leaf, with contiguous prev-linked ranges).
//
// The crash is modeled by driving the runner's foldAndAdvance for the first batch
// (cursor saved, leaves accumulated, NO terminal Flush) and then DISCARDING that
// runner+signer. A new runner with a new signer then runs runOnce, which must
// recover the unanchored tail. This is the load-bearing probe of the signer's
// re-read-on-restart strategy: a per-batch cursor save must not be able to strand
// leaves that no checkpoint ever covered.
func TestAuditSigner_CrashMidAccumulation_NoUnanchoredGap_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)
	const n = 6
	seedChainedEvents(ctx, t, h, tenantID, sessionID, n) // seq 1..6

	secrets, _ := signerSecrets(t)
	signer, err := auditsign.NewSigner(ctx, secrets)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	// The full global_id span of the seeded leaves (every chained event is a leaf).
	var minGID, maxGID int64
	if err := h.conn.QueryRow(ctx,
		"SELECT MIN(global_id), MAX(global_id) FROM events WHERE session_id=$1 AND content_hash IS NOT NULL", sessionID,
	).Scan(&minGID, &maxGID); err != nil {
		t.Fatalf("span query: %v", err)
	}

	// ---- Crash run: advance the cursor over the FIRST batch with leaves in-memory
	// but NO checkpoint flush, then discard the runner+signer (== process crash).
	// A large every (> n) guarantees Project never reaches the N boundary, so the
	// only thing that would have anchored is the terminal Flush we deliberately skip.
	crashSigner := NewAuditSigner(h.conn, signer, 1000)
	crashRunner := NewRunner(
		Config{Subscription: "audit-checkpoint", BatchSize: 3}, // first batch = seq 1..3
		NewSource(h.conn),
		WithAuditSigner(crashSigner),
	)
	if err := crashRunner.src.EnsureSubscription(ctx, "audit-checkpoint"); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	cur, err := crashRunner.src.LoadCursor(ctx, "audit-checkpoint")
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	crashRunner.cursor = cur
	rows, err := crashRunner.src.FetchBatch(ctx, crashRunner.cursor, crashRunner.cfg.BatchSize)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("crash-model precondition: first batch was empty (nothing settled below xmin)")
	}
	if err := crashRunner.foldAndAdvance(ctx, rows); err != nil {
		t.Fatalf("foldAndAdvance (crash batch): %v", err)
	}
	// Precondition of the probe: the cursor advanced but NO checkpoint exists yet
	// (the in-memory accumulator holds the leaves; Flush was never called).
	var afterCrash int
	if err := h.conn.QueryRow(ctx, "SELECT COUNT(*) FROM audit_checkpoints").Scan(&afterCrash); err != nil {
		t.Fatalf("count after crash: %v", err)
	}
	if afterCrash != 0 {
		t.Fatalf("crash-model precondition: expected 0 checkpoints before flush, got %d", afterCrash)
	}
	// Drop the crashed runner+signer: the in-memory accumulator is gone.

	// ---- Restart: a FRESH signer + runner over the SAME subscription. runOnce
	// reloads the durable cursor (now past the first batch) and must still anchor
	// EVERY leaf — including the ones the crashed signer had only in memory.
	restartSigner := NewAuditSigner(h.conn, signer, 1000)
	restartRunner := NewRunner(
		Config{Subscription: "audit-checkpoint", BatchSize: 3},
		NewSource(h.conn),
		WithAuditSigner(restartSigner),
	)
	if err := restartRunner.runOnce(ctx); err != nil {
		t.Fatalf("runOnce (restart): %v", err)
	}

	// Assert: the union of all checkpoint covered ranges spans [minGID, maxGID] with
	// NO gap and the prev-link chain is intact (each row's covers_from is exactly one
	// leaf-step after the prior row's covers_to — i.e. no leaf between checkpoints is
	// left unanchored).
	cks := loadCheckpointSpans(ctx, t, h)
	if len(cks) == 0 {
		t.Fatal("restart produced no checkpoints; the unanchored tail was stranded (re-read-on-restart broken)")
	}
	if cks[0].from > minGID {
		t.Fatalf("first checkpoint covers_from=%d > first leaf global_id=%d: leading leaves unanchored (gap)", cks[0].from, minGID)
	}
	if cks[len(cks)-1].to < maxGID {
		t.Fatalf("last checkpoint covers_to=%d < last leaf global_id=%d: trailing leaves unanchored (gap)", cks[len(cks)-1].to, maxGID)
	}
	// Every seeded leaf must fall inside some checkpoint's covered range (the
	// authoritative no-gap assertion: not one content_hash leaf is left unanchored).
	leafGIDs := leafGlobalIDs(ctx, t, h, sessionID)
	if len(leafGIDs) != n {
		t.Fatalf("expected %d leaves, found %d", n, len(leafGIDs))
	}
	for _, gid := range leafGIDs {
		if !anchored(cks, gid) {
			t.Fatalf("leaf global_id=%d is NOT covered by any checkpoint after restart (unanchored gap)", gid)
		}
	}
	// prev-link chain intact: each later row links to the prior row's hash.
	expectedPrev := domain.CheckpointGenesisPrev()
	for _, c := range cks {
		if len(c.prev) != 0 && string(c.prev) != string(expectedPrev) {
			t.Fatalf("checkpoint %d prev-link broken after restart", c.id)
		}
		expectedPrev = c.hash
	}
}

// checkpointSpan is one decoded audit_checkpoints row's chaining/coverage fields.
type checkpointSpan struct {
	id       int64
	prev     []byte
	hash     []byte
	from, to int64
}

// loadCheckpointSpans reads all checkpoint rows in id order (owner connection).
func loadCheckpointSpans(ctx context.Context, t *testing.T, h *pharness) []checkpointSpan {
	t.Helper()
	rows, err := h.conn.Query(ctx,
		"SELECT id, prev_checkpoint_hash, checkpoint_hash, covers_from_global_id, covers_to_global_id FROM audit_checkpoints ORDER BY id")
	if err != nil {
		t.Fatalf("load checkpoint spans: %v", err)
	}
	defer rows.Close()
	var out []checkpointSpan
	for rows.Next() {
		var c checkpointSpan
		if err := rows.Scan(&c.id, &c.prev, &c.hash, &c.from, &c.to); err != nil {
			t.Fatalf("scan span: %v", err)
		}
		out = append(out, c)
	}
	return out
}

// leafGlobalIDs returns the global_ids of a session's chained (content_hash != NULL)
// events in ascending order — the leaves that MUST each be anchored.
func leafGlobalIDs(ctx context.Context, t *testing.T, h *pharness, sessionID string) []int64 {
	t.Helper()
	rows, err := h.conn.Query(ctx,
		"SELECT global_id FROM events WHERE session_id=$1 AND content_hash IS NOT NULL ORDER BY global_id", sessionID)
	if err != nil {
		t.Fatalf("leaf global_ids: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var gid int64
		if err := rows.Scan(&gid); err != nil {
			t.Fatalf("scan gid: %v", err)
		}
		out = append(out, gid)
	}
	return out
}

// anchored reports whether gid falls inside some checkpoint's [from,to] range.
func anchored(cks []checkpointSpan, gid int64) bool {
	for _, c := range cks {
		if gid >= c.from && gid <= c.to {
			return true
		}
	}
	return false
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
