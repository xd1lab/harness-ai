package eventstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// Load implements [app.EventLogPort.Load]: it folds sessionID's events from
// fromSeq (inclusive) onward into an ordered, oldest-first slice. Passing
// fromSeq <= 1 loads the full stream (architecture §6.6). The returned stream is
// the session's OWN events; a forked child's inherited parent prefix is composed
// by the caller (architecture §6.6).
func (s *Store) Load(ctx context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectEventsFromSeqSQL, sessionID, fromSeq)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return nil, err
	}
	// A read-only tx; commit to end it cleanly (rollback would also be fine).
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit load: %w", err)
	}
	return envs, nil
}

// LoadSession implements [app.EventLogPort.LoadSession]: it returns the session
// aggregate's current control state (head seq, lease, status, fork lineage)
// without folding payloads, used to obtain expectedHeadSeq/leaseEpoch before an
// append and by recovery. It returns [app.SessionNotActiveError] wrapped with a
// not-found note when the session does not exist or is not visible to the tenant
// under RLS (so a foreign-tenant session is indistinguishable from absent).
func (s *Store) LoadSession(ctx context.Context, sessionID string) (domain.Session, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return domain.Session{}, err
	}
	defer cleanup()

	sess, err := loadSessionTx(ctx, tx, sessionID)
	if err != nil {
		return domain.Session{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: commit load-session: %w", err)
	}
	return sess, nil
}

// loadSessionTx reads the sessions row within an already-tenant-scoped tx.
func loadSessionTx(ctx context.Context, tx pgx.Tx, sessionID string) (domain.Session, error) {
	var (
		sess          domain.Session
		parentID      *string
		forkedFromSeq *int64
		leaseOwner    *string
		leaseExpires  *time.Time
		lastEventAt   *time.Time
		modeStr       string
	)
	err := tx.QueryRow(ctx, `
		SELECT id, tenant_id, parent_id, forked_from_seq, status, head_seq,
		       lease_owner, lease_epoch, lease_expires_at, last_event_at, created_at, updated_at, mode
		  FROM sessions WHERE id = $1`,
		sessionID,
	).Scan(
		&sess.ID, &sess.TenantID, &parentID, &forkedFromSeq, &sess.Status, &sess.HeadSeq,
		&leaseOwner, &sess.LeaseEpoch, &leaseExpires, &lastEventAt, &sess.CreatedAt, &sess.UpdatedAt, &modeStr,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, fmt.Errorf("eventstore: session %s not found or not visible: %w", sessionID, app.SessionNotActiveError)
	}
	if err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: loading session: %w", err)
	}
	sess.Mode = domain.PermissionMode(modeStr)
	if parentID != nil {
		sess.ParentID = *parentID
	}
	if forkedFromSeq != nil {
		sess.ForkedFromSeq = *forkedFromSeq
	}
	if leaseOwner != nil {
		sess.LeaseOwner = *leaseOwner
	}
	if leaseExpires != nil {
		sess.LeaseExpiry = *leaseExpires
	}
	if lastEventAt != nil {
		sess.LastEventAt = *lastEventAt
	}
	return sess, nil
}

// Fork implements [app.EventLogPort.Fork]: it creates a new child session
// branching parentID at atSeq, captured as the child's immutable
// forked_from_seq, with the child's own seq continuing from atSeq+1 so the
// composed timeline has a single monotonic seq namespace (architecture §6.6).
// atSeq is validated <= the parent's current head. Fork requires the caller's
// tenant to own the parent and never crosses tenant boundaries: a foreign-tenant
// parent is invisible under RLS, so Fork returns [app.SessionNotActiveError]
// (the not-found/permission-denied case; architecture §8.9), never silently
// creating a cross-tenant child.
func (s *Store) Fork(ctx context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return domain.Session{}, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return domain.Session{}, err
	}
	defer cleanup()

	// Load the parent within the tenant-scoped tx; RLS makes a foreign parent
	// invisible (returns SessionNotActiveError), enforcing fork ownership.
	parent, err := loadSessionTx(ctx, tx, parentID)
	if err != nil {
		return domain.Session{}, err
	}
	if atSeq < 0 {
		return domain.Session{}, fmt.Errorf("eventstore: fork atSeq %d is negative", atSeq)
	}
	if atSeq > parent.HeadSeq {
		return domain.Session{}, fmt.Errorf(
			"eventstore: fork atSeq %d exceeds parent %s head_seq %d: %w",
			atSeq, parentID, parent.HeadSeq, app.ConflictError)
	}

	// The child starts with head_seq = atSeq so its first append continues at
	// atSeq+1 (single monotonic seq namespace; architecture §6.6). It inherits the
	// parent's tenant (enforced: tenantID is the SET LOCAL tenant and the parent
	// was visible under it).
	var child domain.Session
	var parentIDOut, leaseOwner *string
	var forkedFromSeq, leaseExpires, lastEventAt any
	var childModeStr string
	// The child inherits the parent's permission mode (ADR-0019): a fork continues
	// the same session configuration, so its standing mode is the parent's.
	err = tx.QueryRow(ctx, `
		INSERT INTO sessions (id, tenant_id, parent_id, forked_from_seq, status, head_seq, lease_epoch, mode)
		VALUES ($1, $2, $3, $4, 'active', $4, 0, $5)
		RETURNING id, tenant_id, parent_id, forked_from_seq, status, head_seq, lease_owner, lease_epoch, lease_expires_at, last_event_at, created_at, updated_at, mode`,
		newSessionID, tenantID, parentID, atSeq, string(parent.Mode.OrDefault()),
	).Scan(
		&child.ID, &child.TenantID, &parentIDOut, &forkedFromSeq, &child.Status, &child.HeadSeq,
		&leaseOwner, &child.LeaseEpoch, &leaseExpires, &lastEventAt, &child.CreatedAt, &child.UpdatedAt, &childModeStr,
	)
	if err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: inserting fork child: %w", err)
	}
	child.Mode = domain.PermissionMode(childModeStr)
	if parentIDOut != nil {
		child.ParentID = *parentIDOut
	}
	child.ForkedFromSeq = atSeq

	if err := tx.Commit(ctx); err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: commit fork: %w", err)
	}
	return child, nil
}
