// SPDX-License-Identifier: Apache-2.0

package projection

import (
	"context"
	"fmt"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// DefaultCheckpointEvery is the default number of content_hash leaves a checkpoint
// covers when BOLTROPE_AUDIT_CHECKPOINT_EVERY is unset (AC-7). A checkpoint is also
// flushed on a short/caught-up batch (see [Runner.catchUp] -> [AuditSigner.Flush])
// so the head is anchored promptly even below this boundary.
const DefaultCheckpointEvery = 256

// CheckpointSigner is the minimal Ed25519 signing surface the [AuditSigner]
// depends on (a consumer-defined interface, declared in the package that USES it,
// so the projector is decoupled from internal/platform/auditsign and unit-testable
// with a fake key). *auditsign.Signer satisfies it.
//
// Sign returns the detached signature over the checkpoint_hash plus the key_id the
// signature was produced with; KeyID/Algo are the non-sensitive descriptors stored
// on the row. The private key NEVER crosses this interface — only the signature.
type CheckpointSigner interface {
	// Sign signs the 32-byte checkpoint_hash and returns (signature, key_id).
	Sign(checkpointHash []byte) (sig []byte, keyID string)
	// KeyID returns the configured key identifier (safe to log/persist).
	KeyID() string
	// Algo returns the signature algorithm constant (e.g. "ed25519").
	Algo() string
}

// auditLeaf is one accumulated, anchorable content_hash leaf with its global_id
// (so a checkpoint's covers_from/covers_to are the actual first/last leaf ids).
type auditLeaf struct {
	globalID    int64
	contentHash []byte
}

// AuditSigner is the operator-tier projection sink that turns the tamper-EVIDENT
// in-DB hash-chain (Batch-5A) into a tamper-PROOF, externally-anchored audit log
// (Batch-5B, ADR-0034). It tails the GLOBAL event feed through the projection
// [Runner] (its own subscription, default "audit-checkpoint", so its cursor is
// independent of cost-rollup / siem-export), accumulates each event's
// content_hash LEAF in (transaction_id, global_id) order, and at each checkpoint
// boundary folds the range into a checkpoint_hash (domain.LeavesDigest /
// domain.CheckpointHash), SIGNS it with a key held OUTSIDE the events DB, and
// appends a row to audit_checkpoints.
//
// Like [CostProjector] it NEVER touches the append/hot path and errors BEFORE the
// runner advances the cursor (an insert error returns from [AuditSigner.Project],
// so the batch is re-read next poll and the ON CONFLICT (covers_to_global_id)
// makes the re-insert a no-op).
//
// # Crash safety (AC-7 / AC-8)
//
// The runner saves its cursor PER BATCH, but a checkpoint can span many batches, so
// a crash after a cursor save but before a checkpoint flush would lose the
// in-memory leaf accumulator. AuditSigner is robust to this WITHOUT a checkpoint
// being silently dropped:
//
//   - On first use it reloads the checkpoint-chain HEAD from audit_checkpoints
//     (the latest checkpoint_hash as the running prev, and MAX(covers_to_global_id)
//     as lastCovered) so a (re)start resumes with the correct prev link.
//   - Leaves at or below lastCovered are SKIPPED, so a cursor replay that
//     re-delivers already-anchored events does not re-accumulate them.
//   - The INSERT is idempotent via ON CONFLICT (covers_to_global_id), so even if a
//     range is re-anchored it does not double-write.
//
// A crash mid-accumulation is recovered by the cursor: the audit-checkpoint
// subscription re-reads the unanchored tail from its saved position and re-delivers
// those leaves through Project, which re-accumulates and flushes them. The
// lastCovered skip + ON CONFLICT keep that replay a no-op for anything already
// anchored.
type AuditSigner struct {
	conn   Conn
	signer CheckpointSigner
	// every is the leaf-count checkpoint boundary (N). A checkpoint is emitted when
	// the accumulated leaf count reaches every, and on Flush (caught up to head).
	every int

	// loaded reports whether the DB head (prev / lastCovered) has been hydrated.
	loaded bool
	// prev is the running prev_checkpoint_hash: the latest checkpoint's
	// checkpoint_hash, or the genesis constant when no checkpoint exists yet.
	prev []byte
	// lastCovered is the largest covers_to_global_id already anchored (0 when none).
	// Leaves at or below it are already checkpointed and are skipped.
	lastCovered int64
	// leaves is the in-memory accumulator of unanchored leaves in ascending
	// global_id order; flushed (and cleared) at a checkpoint boundary or on Flush.
	leaves []auditLeaf
}

// NewAuditSigner constructs an [AuditSigner] over conn (the operator-tier feed
// connection, same as the [Source]/[CostProjector]) signing with signer. every is
// the checkpoint leaf boundary N; a value <= 0 uses [DefaultCheckpointEvery].
func NewAuditSigner(conn Conn, signer CheckpointSigner, every int) *AuditSigner {
	if every <= 0 {
		every = DefaultCheckpointEvery
	}
	return &AuditSigner{conn: conn, signer: signer, every: every}
}

// selectHeadSQL reads the head of the checkpoint chain: the latest checkpoint's
// checkpoint_hash (the running prev) and the largest anchored covers_to_global_id.
// An empty table yields zero rows (the genesis-seed path in ensureLoaded). It is a
// SELECT (not a scalar QueryRow) so a re-read resumes from the correct prev link.
const selectHeadSQL = `
	SELECT checkpoint_hash, covers_to_global_id
	  FROM audit_checkpoints
	 ORDER BY id DESC
	 LIMIT 1`

// insertCheckpointSQL appends one signed checkpoint. covers_to_global_id is the
// idempotency key: ON CONFLICT (covers_to_global_id) DO NOTHING makes a re-run over
// an already-covered range a no-op (AC-8). The private key is NEVER an argument —
// only the signature, the public-derivable key_id, and algo (AC-16).
const insertCheckpointSQL = `
	INSERT INTO audit_checkpoints
		(prev_checkpoint_hash, checkpoint_hash, covers_from_global_id, covers_to_global_id, leaf_count, signature, key_id, algo)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (covers_to_global_id) DO NOTHING`

// Project accumulates the batch's content_hash leaves and, whenever the
// accumulated leaf count reaches the every boundary, flushes a checkpoint. Rows
// with a nil content_hash (pre-0009 unchained) are SKIPPED as leaves (not
// anchored), matching the verify path's leading-NULL skip — so they never shift the
// digest framing. Leaves at or below the last-anchored global_id are skipped (an
// idempotent re-read).
//
// It errors BEFORE the runner advances the cursor (an insert failure returns here),
// so a failed batch is re-read next poll and the ON CONFLICT keeps it idempotent.
func (a *AuditSigner) Project(ctx context.Context, rows []EventRow) error {
	if err := a.ensureLoaded(ctx); err != nil {
		return err
	}
	for _, r := range rows {
		if len(r.ContentHash) == 0 {
			continue // pre-0009 unchained row: not a leaf, not anchored.
		}
		if r.GlobalID <= a.lastCovered {
			continue // already anchored (a re-read after a crash/replay).
		}
		// Copy the leaf so a later mutation of the row's backing slice cannot
		// corrupt an accumulated, not-yet-flushed leaf.
		leaf := make([]byte, len(r.ContentHash))
		copy(leaf, r.ContentHash)
		a.leaves = append(a.leaves, auditLeaf{globalID: r.GlobalID, contentHash: leaf})
		if len(a.leaves) >= a.every {
			if err := a.flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush emits a partial checkpoint for any accumulated leaves, anchoring the head
// promptly when the runner has caught up to xmin (a short read). It is a no-op when
// nothing is accumulated. The runner calls it at the short-read boundary in
// catchUp.
func (a *AuditSigner) Flush(ctx context.Context) error {
	if err := a.ensureLoaded(ctx); err != nil {
		return err
	}
	if len(a.leaves) == 0 {
		return nil
	}
	return a.flush(ctx)
}

// ensureLoaded hydrates the checkpoint-chain head (prev hash + last anchored
// global_id) from the DB once, so a (re)start resumes accumulating from the first
// uncovered leaf with the correct prev link (AC-7/AC-8 crash safety).
func (a *AuditSigner) ensureLoaded(ctx context.Context) error {
	if a.loaded {
		return nil
	}
	rows, err := a.conn.Query(ctx, selectHeadSQL)
	if err != nil {
		return fmt.Errorf("projection: loading audit-checkpoint head: %w", err)
	}
	var (
		prev        []byte
		lastCovered int64
		found       bool
	)
	for rows.Next() {
		if err := rows.Scan(&prev, &lastCovered); err != nil {
			rows.Close()
			return fmt.Errorf("projection: scanning audit-checkpoint head: %w", err)
		}
		found = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("projection: iterating audit-checkpoint head: %w", err)
	}
	if !found || len(prev) == 0 {
		// No checkpoint yet (empty table): seed prev from the genesis constant and
		// start anchoring from the beginning of the feed.
		prev = domain.CheckpointGenesisPrev()
		lastCovered = 0
	}
	a.prev = prev
	a.lastCovered = lastCovered
	a.loaded = true
	return nil
}

// flush folds the accumulated leaves into a checkpoint_hash, signs it, and appends
// the row. It updates the running prev/lastCovered and clears the accumulator only
// AFTER a successful insert, so an error leaves the leaves intact for the next poll.
func (a *AuditSigner) flush(ctx context.Context) error {
	if len(a.leaves) == 0 {
		return nil
	}
	leaves := make([][]byte, len(a.leaves))
	for i, l := range a.leaves {
		leaves[i] = l.contentHash
	}
	from := a.leaves[0].globalID
	to := a.leaves[len(a.leaves)-1].globalID

	leavesDigest := domain.LeavesDigest(leaves)
	checkpointHash := domain.CheckpointHash(a.prev, leavesDigest)
	sig, keyID := a.signer.Sign(checkpointHash)

	// prev_checkpoint_hash is NULL for the genesis checkpoint (none anchored yet),
	// else the prior row's checkpoint_hash. The verify path seeds prev from the
	// genesis constant when prev_checkpoint_hash IS NULL.
	var prevArg any
	if a.lastCovered != 0 {
		prevArg = a.prev
	}

	if _, err := a.conn.Exec(ctx, insertCheckpointSQL,
		prevArg, checkpointHash, from, to, len(leaves), sig, keyID, a.signer.Algo(),
	); err != nil {
		return fmt.Errorf("projection: inserting audit checkpoint (covers_to=%d): %w", to, err)
	}

	// Advance the running head only after a durable insert. ON CONFLICT may have
	// made the insert a no-op (a concurrent/replayed run already anchored this
	// range); either way the chain head is now this checkpoint, so advancing prev/
	// lastCovered keeps the in-memory state consistent with the DB.
	a.prev = checkpointHash
	a.lastCovered = to
	a.leaves = a.leaves[:0]
	return nil
}
