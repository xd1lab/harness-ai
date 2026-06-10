package eventstore

import (
	"context"
	"fmt"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	infradb "github.com/boltrope/boltrope/internal/orchestrator/infra/db"
)

// CreateTenant inserts a tenant row. It is a bootstrap/admin helper (not part of
// [app.EventLogPort]) used by the orchestrator's provisioning path and by tests
// to seed the tenant a session belongs to. The context's tenant MUST equal
// tenantID (RLS WITH CHECK on the INSERT enforces this), so a connection can
// only create its own tenant row.
func (s *Store) CreateTenant(ctx context.Context, tenantID, name string) error {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := tx.Exec(ctx,
		"INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
		tenantID, name,
	); err != nil {
		return fmt.Errorf("eventstore: creating tenant: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("eventstore: commit create-tenant: %w", err)
	}
	return nil
}

// CreateSession inserts a fresh (non-fork) session aggregate in the active state
// at head_seq=0, lease_epoch=0, owned by the context's tenant, with the given
// permission mode (sessions.mode; ADR-0019). It is the session-creation half of
// the orchestrator's CreateSession RPC (not part of [app.EventLogPort], which
// only appends/loads/forks/subscribes an EXISTING stream). The caller has already
// verified mode (in particular, rejected a client-supplied [domain.ModeBypass]);
// the empty zero value is normalized to [domain.ModeDefault] so the NOT NULL /
// CHECK column constraint always holds. It returns the created [domain.Session].
func (s *Store) CreateSession(ctx context.Context, sessionID string, mode domain.PermissionMode) (domain.Session, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return domain.Session{}, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return domain.Session{}, err
	}
	defer cleanup()

	var (
		sess    domain.Session
		modeStr string
	)
	err = tx.QueryRow(ctx, `
		INSERT INTO sessions (id, tenant_id, status, head_seq, lease_epoch, mode)
		VALUES ($1, $2, 'active', 0, 0, $3)
		RETURNING id, tenant_id, status, head_seq, lease_epoch, mode, created_at, updated_at`,
		sessionID, tenantID, string(mode.OrDefault()),
	).Scan(&sess.ID, &sess.TenantID, &sess.Status, &sess.HeadSeq, &sess.LeaseEpoch, &modeStr, &sess.CreatedAt, &sess.UpdatedAt)
	if err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: creating session: %w", err)
	}
	sess.Mode = domain.PermissionMode(modeStr)
	if err := tx.Commit(ctx); err != nil {
		return domain.Session{}, fmt.Errorf("eventstore: commit create-session: %w", err)
	}
	return sess, nil
}

// SetSessionStatus transitions a session to a terminal status (finished/failed),
// rejecting an active->active no-op caller error. It is used when the loop ends a
// run so subsequent appends are refused by the status guard (ADR-0011 §6.3). It
// is an admin/control helper outside [app.EventLogPort]. The lease epoch is
// checked so only the current writer may close the session (fencing; §9.6).
func (s *Store) SetSessionStatus(ctx context.Context, sessionID string, leaseEpoch int64, status domain.SessionStatus) error {
	if status == domain.StatusActive {
		return fmt.Errorf("eventstore: SetSessionStatus to active is not allowed")
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	tag, err := tx.Exec(ctx, `
		UPDATE sessions SET status = $1, updated_at = now()
		 WHERE id = $2 AND lease_epoch = $3`,
		string(status), sessionID, leaseEpoch)
	if err != nil {
		return fmt.Errorf("eventstore: setting session status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Either fenced (wrong epoch) or not visible; classify via a re-read.
		return s.classifyAppendFailure(ctx, tx, sessionID, 0, leaseEpoch)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("eventstore: commit set-status: %w", err)
	}
	return nil
}

// AppendWithBlob is [Store.Append] plus the insertion of one blobs metadata row
// in the SAME transaction as the events (the large-tool-output path; FR-STATE-05,
// architecture §6.4, §7.4). The caller MUST have already written the bytes to the
// blob store (write-before-reference) and pass their metadata in blob; one of the
// appended events (a [domain.ToolResult]) must reference blob.Ref via its
// BlobRef. The composite FK events(tenant_id, blob_ref) -> blobs(tenant_id, ref)
// guarantees no dangling reference, and if the transaction fails no event and no
// blobs row are committed — the orphaned bytes are reclaimed by the projectord
// sweeper. It returns the same sentinels as [Store.Append].
func (s *Store) AppendWithBlob(
	ctx context.Context,
	sessionID string,
	expectedHeadSeq int64,
	leaseEpoch int64,
	requestID string,
	blob BlobUpload,
	events ...app.AppendInput,
) ([]domain.EventEnvelope, error) {
	return s.appendTx(ctx, sessionID, expectedHeadSeq, leaseEpoch, requestID, &blob, events)
}
