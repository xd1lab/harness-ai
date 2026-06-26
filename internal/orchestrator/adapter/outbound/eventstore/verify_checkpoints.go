// SPDX-License-Identifier: Apache-2.0

package eventstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// This file is the tamper-PROOF verify surface (Batch-5B, ADR-0034, AC-9): the
// operator-tier VerifyAuditCheckpoints. It is the read that turns the signed
// audit-checkpoint chain (migration 0010 / the AuditSigner projector) into a
// PROOF, not merely evidence, of an untampered log.
//
// # Why it is OPERATOR-TIER, not RLS-scoped
//
// The signed checkpoint chain (audit_checkpoints) is GLOBAL: one chain spans all
// tenants, anchored to an Ed25519 key held OUTSIDE the events DB. Verifying a
// checkpoint requires re-reading the events' content_hash leaves over its covered
// GLOBAL [covers_from_global_id, covers_to_global_id] range — ACROSS tenants —
// recomputing the checkpoint_hash, and checking the stored signature against it.
//
// That cross-tenant read cannot ride the tenant-RLS-scoped [Store.beginTenantTx]
// path the rest of the Store uses (events FORCE ROW LEVEL SECURITY would either
// fail closed when no tenant GUC is set, or scope the read to one tenant). So
// VerifyAuditCheckpoints runs on a SEPARATE operator/owner connection (the
// [Store.operator] pool) with a plain, NON-tenant transaction — the same tier the
// projection signer runs on. Mixing the RLS-scoped and RLS-exempt reads in one tx
// helper is deliberately avoided (open question #4): beginTenantTx is left exactly
// as-is so the tenant-isolation guarantees and the hot append path are untouched
// (AC-1), and beginOperatorTx is the distinct, no-GUC helper.
//
// audit_checkpoints itself is RLS-EXEMPT (migration 0010, like event_subscriptions)
// so the operator connection reads it directly; the operator connection also
// bypasses events' RLS, so the cross-tenant content_hash re-read is permitted.

// CheckpointVerifier is the minimal Ed25519 signature-verification surface
// VerifyAuditCheckpoints depends on: a consumer-defined interface (declared here,
// in the package that USES it) so the store is decoupled from
// internal/platform/auditsign and unit-testable with a fake. A
// *auditsign.Verifier satisfies it.
//
// Verify reports whether sig is a valid signature over checkpointHash for the
// public key configured for keyID; a key_id mismatch or a bad signature returns
// false. The verifier holds only PUBLIC material — no private key crosses this
// interface.
type CheckpointVerifier interface {
	// Verify reports whether sig is a valid signature over checkpointHash for keyID.
	Verify(checkpointHash, sig []byte, keyID string) bool
}

// selectCheckpointsSQL reads the signed checkpoint chain in id ASC order (== the
// checkpoint/anchoring order). It reads from the RLS-exempt audit_checkpoints on
// the operator connection. prev_checkpoint_hash is NULL for the genesis row.
const selectCheckpointsSQL = `
	SELECT id, prev_checkpoint_hash, checkpoint_hash, covers_from_global_id, covers_to_global_id, signature, key_id
	  FROM audit_checkpoints
	 ORDER BY id`

// selectCheckpointLeavesSQL re-reads the RAW content_hash leaves a checkpoint
// covers, in ascending global_id order — the bytes whose digest the checkpoint_hash
// is recomputed over. It hashes the literal STORED content_hash bytes (which DIFFER
// after a full in-DB rewrite), so a rewritten event changes the recomputed leaf ->
// the recomputed checkpoint_hash -> the stored signature over the ORIGINAL hash no
// longer verifies. NULL content_hash rows (pre-0009 unchained) are excluded so the
// no-separator leaf framing stays uniform (matching the signer's skip and the
// chain verify's leading-NULL skip).
const selectCheckpointLeavesSQL = `
	SELECT content_hash
	  FROM events
	 WHERE global_id >= $1 AND global_id <= $2 AND content_hash IS NOT NULL
	 ORDER BY global_id`

// auditCheckpointRow is one decoded audit_checkpoints row.
type auditCheckpointRow struct {
	id             int64
	prevHash       []byte // NULL for the genesis row
	checkpointHash []byte
	from           int64
	to             int64
	signature      []byte
	keyID          string
}

// VerifyAuditCheckpoints verifies the signed audit-checkpoint chain end-to-end —
// the tamper-PROOF check (AC-9, ADR-0034). It is OPERATOR-TIER: it reads the
// GLOBAL audit_checkpoints + events.content_hash across all tenants on the
// operator/owner connection (NOT the tenant-RLS-scoped VerifyChainIntegrity path),
// so it requires the Store to have been constructed with an operator pool
// ([NewWithOperator]); a Store with no operator pool returns an error rather than
// silently skipping the check.
//
// For each checkpoint row in id ASC order it:
//
//	(a) re-reads the events' content_hash leaves over [covers_from_global_id,
//	    covers_to_global_id] (ascending global_id, NULL-hash rows skipped),
//	    recomputes leavesDigest + checkpoint_hash from the configured prev (the
//	    genesis constant for the FIRST row, the prior row's STORED checkpoint_hash
//	    otherwise) and asserts it equals the stored checkpoint_hash;
//	(b) asserts prev_checkpoint_hash links to the prior row (the checkpoint chain
//	    itself is intact — a rewritten/removed checkpoint breaks the prev-link);
//	(c) verifies the Ed25519 signature of the RECOMPUTED checkpoint_hash against
//	    the public key for the row's key_id.
//
// It classifies the FIRST failure into [domain.CheckpointVerification].Reason: a
// leaf/hash mismatch (events tampered), a broken prev-link (the checkpoint chain
// was rewritten), or an invalid signature (forgery / wrong key — the load-bearing
// tamper-PROOF signal). An empty audit_checkpoints table yields Valid=true,
// Checked=0 (nothing anchored yet is not a tamper). It is read-only and
// side-effect-free (it commits the read-only tx to end it cleanly).
func (s *Store) VerifyAuditCheckpoints(ctx context.Context, verifier CheckpointVerifier) (domain.CheckpointVerification, error) {
	if verifier == nil {
		return domain.CheckpointVerification{}, errors.New("eventstore: VerifyAuditCheckpoints requires a non-nil verifier")
	}
	tx, cleanup, err := s.beginOperatorTx(ctx)
	if err != nil {
		return domain.CheckpointVerification{}, err
	}
	defer cleanup()

	checkpoints, err := loadCheckpointRows(ctx, tx)
	if err != nil {
		return domain.CheckpointVerification{}, err
	}

	// Re-read each checkpoint's covered leaves on the same operator tx, then run the
	// pure classifier. Splitting the DB reads from the (pgx-free) classification lets
	// the classifier be unit-tested with an in-memory leaf lookup (no Postgres).
	leafLookup := func(from, to int64) ([][]byte, error) {
		return loadCheckpointLeaves(ctx, tx, from, to)
	}
	res, err := classifyCheckpoints(checkpoints, leafLookup, verifier)
	if err != nil {
		return domain.CheckpointVerification{}, err
	}

	if cerr := tx.Commit(ctx); cerr != nil {
		return domain.CheckpointVerification{}, fmt.Errorf("eventstore: commit verify-checkpoints: %w", cerr)
	}
	return res, nil
}

// classifyCheckpoints is the pure, pgx-free verify core (AC-9). Given the loaded
// checkpoint rows (id ASC), a leaf lookup that returns a checkpoint's raw 32-byte
// content_hash leaves over [from,to] in ascending global_id order, and a signature
// [CheckpointVerifier], it recomputes + links + signature-verifies each checkpoint
// and returns the classified [domain.CheckpointVerification]. It performs no I/O
// itself (the leaf lookup is the only side door) so it is directly unit-testable.
//
// An error return is reserved for an infrastructure failure in the leaf lookup; a
// verification FAILURE is a normal (nil-error) Valid=false result.
func classifyCheckpoints(
	checkpoints []auditCheckpointRow,
	leafLookup func(from, to int64) ([][]byte, error),
	verifier CheckpointVerifier,
) (domain.CheckpointVerification, error) {
	// prev is the running prev_checkpoint_hash: the genesis constant entering the
	// first row, then each verified row's stored checkpoint_hash. It mirrors the
	// signer's chaining so the recomputed hash matches when nothing was tampered.
	prev := domain.CheckpointGenesisPrev()
	checked := 0
	for i, cp := range checkpoints {
		// (b) The checkpoint chain must link: the FIRST (genesis) row's
		// prev_checkpoint_hash is either NULL or the fixed genesis constant (the
		// signer stores NULL; a producer may equivalently store the explicit genesis
		// prev) — both denote "genesis"; every later row's prev_checkpoint_hash must
		// equal the prior row's stored checkpoint_hash. A break here means a
		// checkpoint was rewritten or removed.
		if i == 0 {
			if len(cp.prevHash) != 0 && !bytes.Equal(cp.prevHash, domain.CheckpointGenesisPrev()) {
				return checkpointFail(cp.id, checked,
					"broken prev-link: genesis checkpoint prev_checkpoint_hash is neither NULL nor the genesis constant"), nil
			}
		} else if !bytes.Equal(cp.prevHash, checkpoints[i-1].checkpointHash) {
			return checkpointFail(cp.id, checked,
				fmt.Sprintf("broken prev-link: checkpoint %d prev_checkpoint_hash does not match checkpoint %d", cp.id, checkpoints[i-1].id)), nil
		}

		// (a) Recompute the checkpoint_hash from the (possibly tampered) stored
		// content_hash leaves and the running prev. A rewritten event changes its
		// leaf, so the recomputed hash differs from the stored one here.
		leaves, lerr := leafLookup(cp.from, cp.to)
		if lerr != nil {
			return domain.CheckpointVerification{}, lerr
		}
		recomputed := domain.CheckpointHash(prev, domain.LeavesDigest(leaves))
		if !bytes.Equal(recomputed, cp.checkpointHash) {
			return checkpointFail(cp.id, checked,
				fmt.Sprintf("leaf/hash mismatch at checkpoint %d: recomputed checkpoint_hash from the covered content_hashes does not match the stored value (events tampered)", cp.id)), nil
		}

		// (c) Verify the signature over the RECOMPUTED hash. Recomputing from the raw
		// stored leaves and verifying THAT is what catches a CONSISTENT full in-DB
		// rewrite: the in-DB chain re-verifies clean, but the signature was made over
		// the ORIGINAL hash, so it fails against the recomputed one.
		if !verifier.Verify(recomputed, cp.signature, cp.keyID) {
			return checkpointFail(cp.id, checked,
				fmt.Sprintf("invalid signature at checkpoint %d: the Ed25519 signature does not verify against the recomputed checkpoint_hash for key_id %q (forgery, wrong key, or tampered content)", cp.id, cp.keyID)), nil
		}

		prev = cp.checkpointHash
		checked++
	}

	return domain.CheckpointVerification{Valid: true, Checked: checked}, nil
}

// checkpointFail builds an INVALID [domain.CheckpointVerification] classifying the
// first failure (the bad checkpoint id, the count verified before it, and the
// reason).
func checkpointFail(badID int64, checked int, reason string) domain.CheckpointVerification {
	return domain.CheckpointVerification{
		Valid:                false,
		FirstBadCheckpointID: badID,
		Reason:               reason,
		Checked:              checked,
	}
}

// loadCheckpointRows reads every audit_checkpoints row in id ASC order on the
// operator tx. A NULL prev_checkpoint_hash scans to a nil slice (the genesis row).
func loadCheckpointRows(ctx context.Context, tx pgx.Tx) ([]auditCheckpointRow, error) {
	rows, err := tx.Query(ctx, selectCheckpointsSQL)
	if err != nil {
		return nil, fmt.Errorf("eventstore: verify-checkpoints query: %w", err)
	}
	defer rows.Close()

	var out []auditCheckpointRow
	for rows.Next() {
		var r auditCheckpointRow
		if err := rows.Scan(&r.id, &r.prevHash, &r.checkpointHash, &r.from, &r.to, &r.signature, &r.keyID); err != nil {
			return nil, fmt.Errorf("eventstore: scanning checkpoint row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterating checkpoint rows: %w", err)
	}
	return out, nil
}

// loadCheckpointLeaves re-reads the RAW content_hash leaves a checkpoint covers,
// in ascending global_id order (NULL-hash rows excluded), on the operator tx. It
// copies each leaf into a fresh slice so a later pgx row-buffer reuse cannot mutate
// an accumulated leaf before LeavesDigest folds it.
func loadCheckpointLeaves(ctx context.Context, tx pgx.Tx, from, to int64) ([][]byte, error) {
	rows, err := tx.Query(ctx, selectCheckpointLeavesSQL, from, to)
	if err != nil {
		return nil, fmt.Errorf("eventstore: verify-checkpoints leaf query [%d,%d]: %w", from, to, err)
	}
	defer rows.Close()

	var out [][]byte
	for rows.Next() {
		var ch []byte
		if err := rows.Scan(&ch); err != nil {
			return nil, fmt.Errorf("eventstore: scanning checkpoint leaf: %w", err)
		}
		leaf := make([]byte, len(ch))
		copy(leaf, ch)
		out = append(out, leaf)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterating checkpoint leaves: %w", err)
	}
	return out, nil
}
