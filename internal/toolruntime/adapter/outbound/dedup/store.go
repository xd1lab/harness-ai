package dedup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

// pgUniqueViolation is PostgreSQL SQLSTATE for a unique-constraint violation.
// Referenced directly to avoid adding pgerrcode as a dependency.
const pgUniqueViolation = "23505"

// Store is the pgx-backed [app.DedupStore] implementation: the durable
// tool-execution dedup ledger over the tool_executions table (ADR-0012;
// architecture §7.2). It is safe for concurrent use — each method acquires an
// independent connection and transaction.
//
// # Tenant isolation
//
// The tenant id is taken from the [app.ExecutionRecord.TenantID] field (Begin,
// Complete) or the tenantID parameter (Lookup). On every transaction the store
// runs SET LOCAL app.current_tenant so RLS (FORCE ROW LEVEL SECURITY) on the
// non-owner application role constrains every query to that tenant's rows. A
// missing tenant id fails closed — the SET LOCAL statement raises an error
// immediately (fail-closed by construction; ADR-0013 §"Concrete RLS";
// migration 0003_rls_policies).
//
// # Cache-hit tenant re-check
//
// [Store.Begin] and [Store.Lookup] re-verify that the record's tenant_id
// matches the caller-supplied tenant before returning cached bytes
// (architecture §7.3). A mismatch is treated as "not found" — no cross-tenant
// existence oracle.
type Store struct {
	pool Pool
}

// New returns a [Store] over pool. The caller owns pool's lifecycle (Close),
// as a single pool is typically shared across the tool-runtime process.
func New(pool Pool) *Store {
	return &Store{pool: pool}
}

// Compile-time assertion that *Store satisfies the frozen DedupStore port.
var _ app.DedupStore = (*Store)(nil)

// setLocalTenant scopes tx to tenantID for the remainder of the transaction.
// Uses the parameterized set_config function (not SET LOCAL with string
// interpolation) so a malformed tenant cannot inject SQL.
func setLocalTenant(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, "SELECT set_config('app.current_tenant', $1, true)", tenantID); err != nil {
		return fmt.Errorf("dedup: setting tenant GUC: %w", err)
	}
	return nil
}

// beginTenantTx acquires a connection, begins a transaction, and scopes it to
// tenantID. Returns tx, a cleanup func (defer it), and any error.
// On any error it releases what it acquired and fails closed.
func (s *Store) beginTenantTx(ctx context.Context, tenantID string) (pgx.Tx, func(), error) {
	if tenantID == "" {
		return nil, nil, fmt.Errorf("dedup: tenant id must not be empty (fail-closed)")
	}
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	tx, err := pc.Begin(ctx)
	if err != nil {
		pc.Release()
		return nil, nil, fmt.Errorf("dedup: begin tx: %w", err)
	}
	if err := setLocalTenant(ctx, tx, tenantID); err != nil {
		_ = tx.Rollback(ctx)
		pc.Release()
		return nil, nil, err
	}
	cleanup := func() {
		_ = tx.Rollback(ctx) // no-op if already committed
		pc.Release()
	}
	return tx, cleanup, nil
}

// isUniqueViolation reports whether err is a PostgreSQL unique-constraint
// violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	return false
}

// Begin records [app.ExecStarted] for the (TenantID, SessionID, IdempotencyKey)
// key if no record exists, or returns the existing record if one is already
// present. It is the durable guard taken BEFORE dispatch so a crash between
// Begin and Complete leaves the key in "started" state (UNKNOWN recovery;
// ADR-0012 §"Durable execution intent before side effects").
//
// Concurrency: two goroutines racing to Begin the same key both attempt an
// INSERT … ON CONFLICT DO NOTHING. The PRIMARY KEY (tenant_id, session_id,
// idempotency_key) ensures exactly one row is committed; both goroutines then
// read and return that same row (FR-TOOL-04 AC-2).
func (s *Store) Begin(ctx context.Context, rec app.ExecutionRecord) (app.ExecutionRecord, error) {
	if rec.TenantID == "" {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: Begin: ExecutionRecord.TenantID must not be empty")
	}

	tx, cleanup, err := s.beginTenantTx(ctx, rec.TenantID)
	if err != nil {
		return app.ExecutionRecord{}, err
	}
	defer cleanup()

	// Attempt INSERT of the started record; ON CONFLICT DO NOTHING so a racing
	// goroutine does not error — it just falls through to the read-back.
	_, insertErr := tx.Exec(ctx, `
		INSERT INTO tool_executions (tenant_id, session_id, idempotency_key, status)
		VALUES ($1, $2, $3, 'started')
		ON CONFLICT (tenant_id, session_id, idempotency_key) DO NOTHING`,
		rec.TenantID, rec.SessionID, rec.IdempotencyKey,
	)
	if insertErr != nil && !isUniqueViolation(insertErr) {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: Begin insert: %w", insertErr)
	}

	// Read back the current row — either the one just inserted or the
	// pre-existing one from a race/retry. RLS (SET LOCAL app.current_tenant)
	// is already in force on this transaction so the SELECT is tenant-scoped
	// (architecture §7.3).
	existing, err := s.readRecord(ctx, tx, rec.TenantID, rec.SessionID, rec.IdempotencyKey)
	if err != nil {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: Begin read-back: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: Begin commit: %w", err)
	}
	return existing, nil
}

// Complete records a terminal status and result for the key. Typically called
// with [app.ExecCompleted] and the tool observation, or [app.ExecFailed] /
// [app.ExecUnknown]. The [domain.Observation] is JSON-serialized into the
// result_ref TEXT column so the full result survives restarts without
// additional tables (ADR-0012 §"Durable dedup ledger").
func (s *Store) Complete(ctx context.Context, rec app.ExecutionRecord) error {
	if rec.TenantID == "" {
		return fmt.Errorf("dedup: Complete: ExecutionRecord.TenantID must not be empty")
	}

	// Serialize the observation into result_ref.
	resultJSON, err := json.Marshal(rec.Result)
	if err != nil {
		return fmt.Errorf("dedup: marshaling result: %w", err)
	}

	tx, cleanup, err := s.beginTenantTx(ctx, rec.TenantID)
	if err != nil {
		return err
	}
	defer cleanup()

	tag, err := tx.Exec(ctx, `
		UPDATE tool_executions
		   SET status = $4, result_ref = $5, updated_at = now()
		 WHERE tenant_id = $1 AND session_id = $2 AND idempotency_key = $3`,
		rec.TenantID, rec.SessionID, rec.IdempotencyKey, string(rec.Status), string(resultJSON),
	)
	if err != nil {
		return fmt.Errorf("dedup: Complete update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dedup: Complete: no row found for (%s, %s, %s)",
			rec.TenantID, rec.SessionID, rec.IdempotencyKey)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("dedup: Complete commit: %w", err)
	}
	return nil
}

// Lookup returns the current [app.ExecutionRecord] for (tenantID, sessionID,
// idempotencyKey), used on recovery to adjudicate an open execution. Returns
// an error wrapping [ErrNotFound] if no record exists.
//
// Tenant re-check (architecture §7.3): the tenantID parameter must match the
// RLS-enforced row tenant_id; any mismatch or absence returns ErrNotFound
// (no cross-tenant existence oracle).
func (s *Store) Lookup(ctx context.Context, tenantID, sessionID, idempotencyKey string) (app.ExecutionRecord, error) {
	if tenantID == "" {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: Lookup: tenantID must not be empty")
	}

	tx, cleanup, err := s.beginTenantTx(ctx, tenantID)
	if err != nil {
		return app.ExecutionRecord{}, err
	}
	defer cleanup()

	rec, err := s.readRecord(ctx, tx, tenantID, sessionID, idempotencyKey)
	if err != nil {
		return app.ExecutionRecord{}, err
	}
	// Read-only path; deferred rollback is a clean no-op.
	return rec, nil
}

// readRecord reads one row from tool_executions inside tx, scoped to the
// (tenantID, sessionID, idempotencyKey) key. Returns [ErrNotFound] when no
// row matches (RLS-invisible rows are also "not found" — no existence oracle).
func (s *Store) readRecord(ctx context.Context, tx pgx.Tx, tenantID, sessionID, idempotencyKey string) (app.ExecutionRecord, error) {
	var (
		status    string
		resultRef *string
	)
	err := tx.QueryRow(ctx, `
		SELECT status, result_ref
		  FROM tool_executions
		 WHERE tenant_id = $1 AND session_id = $2 AND idempotency_key = $3`,
		tenantID, sessionID, idempotencyKey,
	).Scan(&status, &resultRef)

	if errors.Is(err, pgx.ErrNoRows) {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: record (%s, %s, %s) not found: %w",
			tenantID, sessionID, idempotencyKey, ErrNotFound)
	}
	if err != nil {
		return app.ExecutionRecord{}, fmt.Errorf("dedup: reading record: %w", err)
	}

	out := app.ExecutionRecord{
		TenantID:       tenantID,
		SessionID:      sessionID,
		IdempotencyKey: idempotencyKey,
		Status:         app.ExecutionStatus(status),
	}

	// Unmarshal the result observation when present.
	if resultRef != nil && *resultRef != "" {
		var obs domain.Observation
		if err := json.Unmarshal([]byte(*resultRef), &obs); err != nil {
			return app.ExecutionRecord{}, fmt.Errorf("dedup: unmarshaling result_ref: %w", err)
		}
		out.Result = obs
	}
	return out, nil
}

// ErrNotFound is returned by [Store.Lookup] (and internally by readRecord)
// when no dedup record matches the requested key. Use [errors.Is] to detect it.
var ErrNotFound = errors.New("dedup: record not found")
