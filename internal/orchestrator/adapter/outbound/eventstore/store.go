// Package eventstore is the orchestrator's in-process event-store adapter: a
// single pgx-backed [Store] that satisfies
// [github.com/xd1lab/harness-ai/internal/orchestrator/app.EventLogPort] (ADR-0009,
// ADR-0011; architecture §2.2, §4.2, §5.1). It is an outbound adapter — the only
// place the orchestrator turns the append-only log into SQL — so the loop and
// recovery depend only on the port, never on pgx.
//
// # The append transaction (the critical path)
//
// [Store.Append] is one transaction that, after scoping the connection to the
// caller's tenant via SET LOCAL app.current_tenant (so RLS applies; §6.7):
//
//  1. short-circuits on idempotency — a re-sent (session_id, request_id) returns
//     the already-committed envelopes as SUCCESS, not a conflict (ADR-0011 §6.3);
//  2. runs the optimistic gate + fencing + status guard as one UPDATE on
//     sessions (head_seq = head_seq + N WHERE head_seq = :expected AND
//     lease_epoch = :epoch AND status = 'active' RETURNING head_seq); 0 rows is
//     re-read to classify [app.ConflictError] vs [app.FencedError] vs
//     [app.SessionNotActiveError];
//  3. inserts each event with the seq tied to the head transition, so contiguity
//     holds by construction (ADR-0011 §6.3).
//
// # Tenant isolation
//
// Every method derives the tenant from the context
// ([github.com/xd1lab/harness-ai/internal/orchestrator/infra/db.TenantFromContext],
// set from the verified principal token; §8.2) and SET LOCALs it on the
// transaction's connection. A missing tenant fails closed; RLS is the backstop.
//
// # Pool
//
// The store works over the consumer-defined [Pool]; this package ships
// [SimplePool] (pgx-core, fresh connection per acquire) so it runs without the
// pooled-driver dependency, and a real pool can be swapped in behind [Pool].
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// Store is the pgx-backed [app.EventLogPort] implementation. It holds a [Pool]
// and an [infradb] tenant resolver; it is safe for concurrent use (each method
// acquires its own connection and transaction).
type Store struct {
	pool Pool
}

// New returns a [Store] over pool. The caller owns pool's lifecycle (Close),
// since a single pool is typically shared across the orchestrator process.
func New(pool Pool) *Store {
	return &Store{pool: pool}
}

// Compile-time assertion that *Store satisfies the frozen EventLogPort.
var _ app.EventLogPort = (*Store)(nil)

// setLocalTenant scopes tx to tenantID for the remainder of the transaction by
// running SET LOCAL app.current_tenant, so every subsequent statement is
// RLS-filtered to that tenant (architecture §6.7). It uses set_config (a
// parameterized function) rather than string-interpolating the SET LOCAL
// statement, so a malformed tenant cannot inject SQL.
func setLocalTenant(ctx context.Context, tx pgx.Tx, tenantID string) error {
	// set_config(name, value, is_local=true) is the function form of SET LOCAL
	// and accepts a bind parameter for the value (SET LOCAL syntax does not).
	if _, err := tx.Exec(ctx, "SELECT set_config('app.current_tenant', $1, true)", tenantID); err != nil {
		return fmt.Errorf("eventstore: setting tenant GUC: %w", err)
	}
	return nil
}

// beginTenantTx acquires a connection, begins a transaction, and scopes it to
// the context's tenant. It returns the connection, the transaction, and a
// cleanup func that rolls back (a no-op after commit) and releases the
// connection; callers defer the cleanup. On any error it releases what it
// acquired and returns the error (including [infradb.ErrNoTenant] when the
// context carries no tenant — fail closed).
func (s *Store) beginTenantTx(ctx context.Context) (pgx.Tx, func(), error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	tx, err := pc.Begin(ctx)
	if err != nil {
		pc.Release()
		return nil, nil, fmt.Errorf("eventstore: begin tx: %w", err)
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

// Append implements [app.EventLogPort.Append]: the optimistic + fenced +
// idempotent + contiguous append transaction (ADR-0011 §6.3). See the package
// doc for the transaction shape. It rejects an event carrying a non-empty
// BlobRef (use [Store.AppendWithBlob] for the blob-in-same-tx path) so a plain
// Append can never create a dangling blob reference.
func (s *Store) Append(
	ctx context.Context,
	sessionID string,
	expectedHeadSeq int64,
	leaseEpoch int64,
	requestID string,
	events ...app.AppendInput,
) ([]domain.EventEnvelope, error) {
	return s.appendTx(ctx, sessionID, expectedHeadSeq, leaseEpoch, requestID, nil, events)
}

// appendTx is the shared implementation of [Store.Append] and
// [Store.AppendWithBlob]. When blob is non-nil its metadata row is inserted in
// the SAME transaction as the events (write-before-reference: the bytes are
// already in the blob store; architecture §6.4, §7.4).
func (s *Store) appendTx(
	ctx context.Context,
	sessionID string,
	expectedHeadSeq int64,
	leaseEpoch int64,
	requestID string,
	blob *BlobUpload,
	events []app.AppendInput,
) ([]domain.EventEnvelope, error) {
	if len(events) == 0 {
		return nil, errors.New("eventstore: Append requires at least one event")
	}

	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}

	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// (a) Idempotency short-circuit: if this exact (session_id, request_id)
	// already landed, return the prior envelopes as success (NOT a conflict).
	if prior, found, ierr := s.loadByRequestID(ctx, tx, sessionID, tenantID, requestID); ierr != nil {
		return nil, ierr
	} else if found {
		// Read-only path; the deferred rollback discards the (empty) tx cleanly.
		return prior, nil
	}

	// (b) Optimistic gate + fencing token + status guard, in one UPDATE. The
	// head advances by len(events); the first new seq is expectedHeadSeq+1.
	n := int64(len(events))
	var newHead int64
	err = tx.QueryRow(ctx, `
		UPDATE sessions
		   SET head_seq = head_seq + $1, updated_at = now(), last_event_at = now()
		 WHERE id = $2 AND head_seq = $3 AND lease_epoch = $4 AND status = 'active'
		RETURNING head_seq`,
		n, sessionID, expectedHeadSeq, leaseEpoch,
	).Scan(&newHead)
	if errors.Is(err, pgx.ErrNoRows) {
		// 0 rows updated — re-read to classify the precise failure.
		return nil, s.classifyAppendFailure(ctx, tx, sessionID, expectedHeadSeq, leaseEpoch)
	}
	if err != nil {
		return nil, fmt.Errorf("eventstore: optimistic gate update: %w", err)
	}

	// (b') Insert the blob metadata row in the same tx, before the event that
	// references it (the composite FK then guarantees no dangling reference).
	if blob != nil {
		if err := s.insertBlobRow(ctx, tx, tenantID, *blob); err != nil {
			return nil, err
		}
	}

	// (c) Insert each event with its contiguous seq.
	envelopes := make([]domain.EventEnvelope, 0, len(events))
	for i, in := range events {
		seq := expectedHeadSeq + 1 + int64(i)
		env, err := s.insertEvent(ctx, tx, insertEventArgs{
			tenantID:  tenantID,
			sessionID: sessionID,
			seq:       seq,
			requestID: requestID,
			in:        in,
			blob:      blob,
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, env)
	}

	// Wakeup hint for Subscribe: NOTIFY the single channel with the session id.
	// It is only a hint (subscribers re-read from their cursor), so a failure to
	// notify is not fatal to the append — but it shares the tx so it is delivered
	// exactly when the events become visible.
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, sessionID); err != nil {
		return nil, fmt.Errorf("eventstore: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		// A unique-violation on commit (e.g. a racing writer that beat us between
		// the gate and commit on the same seq) is reclassified as a conflict.
		return nil, s.reclassifyCommitError(err)
	}
	return envelopes, nil
}

// classifyAppendFailure runs after the optimistic UPDATE matched 0 rows. It
// re-reads the session (still within the tenant-scoped tx) to distinguish the
// three failure sentinels (ADR-0011 §6.3):
//
//   - the session does not exist (or is not visible to this tenant under RLS) ->
//     [app.SessionNotActiveError] (there is nothing active to append to);
//   - status != 'active' -> [app.SessionNotActiveError];
//   - lease_epoch != the supplied epoch -> [app.FencedError] (the writer is
//     fenced even if its expected seq is current);
//   - otherwise (head_seq != expected) -> [app.ConflictError].
//
// Fencing is checked BEFORE the head mismatch so a stale-lease writer whose
// expected seq happens to be current is correctly fenced, per AC-3.
func (s *Store) classifyAppendFailure(ctx context.Context, tx pgx.Tx, sessionID string, expectedHeadSeq, leaseEpoch int64) error {
	var (
		status   string
		headSeq  int64
		curEpoch int64
	)
	err := tx.QueryRow(ctx,
		"SELECT status, head_seq, lease_epoch FROM sessions WHERE id = $1",
		sessionID,
	).Scan(&status, &headSeq, &curEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		// Not found or RLS-hidden: nothing active to append to.
		return fmt.Errorf("eventstore: session %s not found or not visible: %w", sessionID, app.SessionNotActiveError)
	}
	if err != nil {
		return fmt.Errorf("eventstore: classifying append failure: %w", err)
	}
	switch {
	case status != string(domain.StatusActive):
		return fmt.Errorf("eventstore: session %s status=%s: %w", sessionID, status, app.SessionNotActiveError)
	case curEpoch != leaseEpoch:
		return fmt.Errorf("eventstore: session %s lease_epoch=%d, writer epoch=%d: %w",
			sessionID, curEpoch, leaseEpoch, app.FencedError)
	default:
		return fmt.Errorf("eventstore: session %s head_seq=%d, expected=%d: %w",
			sessionID, headSeq, expectedHeadSeq, app.ConflictError)
	}
}

// reclassifyCommitError maps a unique-constraint violation surfaced at COMMIT to
// [app.ConflictError]; any other error is wrapped verbatim. A late
// unique-violation on (session_id, seq) means a concurrent writer committed the
// same seq first — a genuine optimistic conflict.
func (s *Store) reclassifyCommitError(err error) error {
	if isUniqueViolation(err) {
		return fmt.Errorf("eventstore: append lost a commit race: %w", app.ConflictError)
	}
	return fmt.Errorf("eventstore: commit append: %w", err)
}

// loadByRequestID returns the envelopes previously committed under (sessionID,
// requestID), and whether any were found. It is the idempotency short-circuit:
// the prior committed append is returned as success rather than re-attempted.
func (s *Store) loadByRequestID(ctx context.Context, tx pgx.Tx, sessionID, tenantID, requestID string) ([]domain.EventEnvelope, bool, error) {
	rows, err := tx.Query(ctx, selectEventsByRequestSQL, sessionID, requestID)
	if err != nil {
		return nil, false, fmt.Errorf("eventstore: idempotency lookup: %w", err)
	}
	defer rows.Close()
	envs, err := scanEnvelopes(rows, tenantID)
	if err != nil {
		return nil, false, err
	}
	return envs, len(envs) > 0, nil
}

// pgUniqueViolation is the stable SQLSTATE for a unique-constraint violation. It
// is referenced directly (rather than via github.com/jackc/pgerrcode, which is
// not a dependency of this module).
const pgUniqueViolation = "23505"

// isUniqueViolation reports whether err is a PostgreSQL unique-constraint
// violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	return false
}

// marshalPayload encodes a domain event payload to JSON for the events.payload
// column. The persisted coordinates (seq/request/tenant/actor/timestamp) live on
// the envelope/columns, never duplicated in the payload, so only the typed event
// struct is marshaled.
func marshalPayload(e domain.Event) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("eventstore: marshaling %s payload: %w", e.EventType(), err)
	}
	return b, nil
}
