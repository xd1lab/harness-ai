package projection

import (
	"context"
	"fmt"
	"time"

	"github.com/boltrope/boltrope/internal/platform/blob"
)

// DefaultSweepGracePeriod is the default age a blob must reach with no
// referencing event before the orphan sweeper reclaims it. The grace period
// guards the write-before-reference window: the bytes (and, with
// AppendWithBlob, the blobs row) are created BEFORE the referencing event
// commits, so a freshly-written, not-yet-referenced blob is normal in-flight
// state, not an orphan. A generous default ensures an in-progress append is never
// swept out from under itself (architecture §6.4, §7.4).
const DefaultSweepGracePeriod = time.Hour

// orphanBlobsSQL selects blobs rows older than the grace cutoff that have NO
// referencing event, across all tenants (the sweeper is operator-tier). The
// NOT EXISTS matches the composite FK key (tenant_id, ref) <-> events(tenant_id,
// blob_ref): a blob is an orphan iff no event references it. The cutoff is
// computed from a text-bound RFC3339 timestamp so the statement is protocol-
// robust. It is capped at $2 rows so one sweep pass is bounded.
//
// "blobs whose owning tx failed" reduces to exactly this: the bytes are written
// before the referencing event commits, so a blob still unreferenced past the
// grace period is one whose append never landed (a rolled-back or abandoned
// transaction) and whose bytes must be reclaimed (FR-STATE-05).
const orphanBlobsSQL = `
	SELECT b.tenant_id, b.ref
	  FROM blobs b
	 WHERE b.created_at < $1::text::timestamptz
	   AND NOT EXISTS (
	       SELECT 1 FROM events e
	        WHERE e.tenant_id = b.tenant_id AND e.blob_ref = b.ref)
	 ORDER BY b.created_at
	 LIMIT $2`

// deleteBlobRowSQL removes a reclaimed blob's metadata row after its bytes are
// deleted from the store. It re-checks NOT EXISTS in the same statement so a
// concurrently-committed referencing event (a race against an in-flight append)
// makes the delete a no-op — a referenced blob is NEVER deleted (FR-STATE-05).
const deleteBlobRowSQL = `
	DELETE FROM blobs b
	 WHERE b.tenant_id = $1 AND b.ref = $2
	   AND NOT EXISTS (
	       SELECT 1 FROM events e
	        WHERE e.tenant_id = b.tenant_id AND e.blob_ref = b.ref)`

// orphanRef is a candidate orphan blob's tenant-scoped identity read from the
// sweep query.
type orphanRef struct {
	tenantID string
	ref      string
}

// Sweeper reclaims orphan blob bytes: blobs older than a grace period with no
// referencing event (architecture §6.4, §7.4; FR-STATE-05). It deletes the bytes
// via the [blob.BlobStorePort] FIRST, then removes the blobs metadata row, and
// re-checks "no referencing event" at delete time so a referenced blob is never
// reclaimed even under a race with a committing append.
type Sweeper struct {
	conn  Conn
	store blob.BlobStorePort
	grace time.Duration
	// batch caps the number of orphans reclaimed per [Sweeper.Sweep] pass so a
	// single pass is bounded.
	batch int
}

// SweeperOption configures a [Sweeper].
type SweeperOption func(*Sweeper)

// WithGracePeriod overrides [DefaultSweepGracePeriod].
func WithGracePeriod(d time.Duration) SweeperOption {
	return func(s *Sweeper) {
		if d > 0 {
			s.grace = d
		}
	}
}

// WithSweepBatch overrides the per-pass orphan cap (default 256).
func WithSweepBatch(n int) SweeperOption {
	return func(s *Sweeper) {
		if n > 0 {
			s.batch = n
		}
	}
}

// NewSweeper returns a [Sweeper] over conn and store. conn must be a role that can
// read every tenant's blobs and delete the orphan rows (operator-tier); store is
// the same blob backend the orchestrator writes through.
func NewSweeper(conn Conn, store blob.BlobStorePort, opts ...SweeperOption) *Sweeper {
	s := &Sweeper{conn: conn, store: store, grace: DefaultSweepGracePeriod, batch: 256}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Sweep runs one reclamation pass and returns the number of orphan blobs
// reclaimed. now is the reference time the grace cutoff is measured from (the
// runner passes its clock's now); the cutoff is now-grace. For each orphan it
// deletes the bytes (idempotent: deleting absent bytes is not an error) then
// removes the metadata row guarded by a re-check. A per-blob error is wrapped and
// returned after attempting the rest is NOT done — the pass stops on the first
// hard error so a failing backend does not spin; transient cases retry next pass.
func (s *Sweeper) Sweep(ctx context.Context, now time.Time) (int, error) {
	cutoff := now.Add(-s.grace)
	candidates, err := s.findOrphans(ctx, cutoff)
	if err != nil {
		return 0, err
	}

	reclaimed := 0
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return reclaimed, err
		}
		ref := blob.Ref{TenantID: c.tenantID, Key: c.ref}
		// Delete bytes first (write-before-reference's inverse: drop bytes, then the
		// row). Delete is idempotent, so a partial prior pass is safe to repeat.
		if derr := s.store.Delete(ctx, ref); derr != nil {
			return reclaimed, fmt.Errorf("projection: sweeper deleting blob bytes %s: %w", ref.Key, derr)
		}
		// Remove the metadata row, re-checking NOT EXISTS so a blob that became
		// referenced since the scan is left intact (never delete a referenced blob).
		tag, derr := s.conn.Exec(ctx, deleteBlobRowSQL, c.tenantID, c.ref)
		if derr != nil {
			return reclaimed, fmt.Errorf("projection: sweeper deleting blob row %s: %w", ref.Key, derr)
		}
		if tag.RowsAffected() > 0 {
			reclaimed++
		}
	}
	return reclaimed, nil
}

// findOrphans reads the candidate orphan refs older than cutoff with no
// referencing event.
func (s *Sweeper) findOrphans(ctx context.Context, cutoff time.Time) ([]orphanRef, error) {
	rows, err := s.conn.Query(ctx, orphanBlobsSQL, cutoff.UTC().Format(time.RFC3339Nano), s.batch)
	if err != nil {
		return nil, fmt.Errorf("projection: sweeper scanning orphans: %w", err)
	}
	defer rows.Close()

	var out []orphanRef
	for rows.Next() {
		var o orphanRef
		if err := rows.Scan(&o.tenantID, &o.ref); err != nil {
			return nil, fmt.Errorf("projection: sweeper scanning orphan row: %w", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("projection: sweeper iterating orphans: %w", err)
	}
	return out, nil
}
