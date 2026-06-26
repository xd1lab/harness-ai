package projection

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// Conn is the minimal read-side query surface the [Source] needs: a consumer-
// defined interface (declared here, in the package that USES it) so the source is
// decoupled from any concrete pgx type. A *github.com/jackc/pgx/v5.Conn satisfies
// it directly, and a pooled adapter could too. The projector reads the events
// table DIRECTLY (it does not go through the eventstore adapter; ADR-0011 §10.4),
// so it issues plain SELECTs plus the cursor UPDATE on event_subscriptions and
// the sweeper's blobs SELECT/DELETE.
//
// The connection is expected to be a role that can read the GLOBAL feed across
// tenants (the projector is an operator/read-side artifact; event_subscriptions
// is excluded from RLS, and the sweeper must see every tenant's blobs). It never
// writes events.
type Conn interface {
	// Query runs a query returning rows.
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// QueryRow runs a query returning at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	// Exec runs a statement returning no rows (the cursor UPDATE / sweeper DELETE).
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Source is the pgx-backed read-side feed: it loads and persists a named
// subscription's gap-safe cursor, reads the next batch of FULLY-SETTLED events
// (strictly below the snapshot xmin), measures projection lag, and backs the
// orphan-blob sweep — all read-only against the events/blobs tables plus the
// cursor row. It is safe for sequential use by a single worker (a subscription is
// single-owner; horizontal sharding is by subscription name, architecture §10.4).
type Source struct {
	conn Conn
}

// NewSource returns a [Source] over conn. The caller owns conn's lifecycle.
func NewSource(conn Conn) *Source { return &Source{conn: conn} }

// loadCursorSQL reads a subscription's persisted cursor. The row is created with
// the migration default (0, 0) by [Source.EnsureSubscription] if absent.
const loadCursorSQL = `SELECT last_transaction_id, last_global_id FROM event_subscriptions WHERE name = $1`

// ensureSubscriptionSQL inserts the subscription's cursor row at the zero cursor
// if it does not yet exist (idempotent), so a fresh subscription starts from the
// beginning of the feed.
const ensureSubscriptionSQL = `
	INSERT INTO event_subscriptions (name, last_transaction_id, last_global_id)
	VALUES ($1, '0'::xid8, 0)
	ON CONFLICT (name) DO NOTHING`

// saveCursorSQL advances a subscription's persisted cursor and stamps updated_at.
// The transaction id is passed as text and cast to xid8 (pgx encodes a uint64 as
// the xid8 wire value, but the explicit cast keeps the statement protocol-robust).
const saveCursorSQL = `
	UPDATE event_subscriptions
	   SET last_transaction_id = $2::text::xid8, last_global_id = $3, updated_at = now()
	 WHERE name = $1`

// fetchBatchSQL is the gap-safe, xmin-bounded catch-up read (architecture §10.4).
// It returns only events from FULLY-SETTLED transactions (transaction_id strictly
// below the snapshot xmin), strictly after the cursor, ordered by the cursor's
// composite key, capped at $4 rows. transaction_id is selected as text so it is
// scanned into a uint64 via strconv (robust under either query protocol) and the
// cursor comparison casts the bind params to xid8.
//
// The (transaction_id, global_id) > (last, last) row-value comparison plus the
// transaction_id < xmin bound is the whole safety property: a late-committing
// lower-id transaction is simply not yet below xmin and is read on a later poll —
// delayed, never skipped (NFR-REL-04).
// The content_hash + chain_hash columns are APPENDED AT THE END (additive; pgx
// scans positionally so the existing columns must not be reordered). They are
// nullable: pre-0009 (unchained) rows scan as nil []byte. The cost fold ignores
// them; the Batch-5B audit-checkpoint signer and SIEM exporter consume them.
const fetchBatchSQL = `
	SELECT transaction_id::text, global_id, seq, tenant_id, session_id, event_type, payload, content_hash, chain_hash
	  FROM events
	 WHERE (transaction_id, global_id) > ($1::text::xid8, $2)
	   AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
	 ORDER BY transaction_id, global_id
	 LIMIT $3`

// lagSQL counts events that are settled below xmin and strictly after the cursor —
// the projection lag in unprocessed, ready-to-read events (the USE saturation
// input for the lag gauge; FR-OBS-02). It deliberately uses the SAME xmin bound as
// the fetch so lag reflects only work the worker can actually do now (in-flight
// transactions above xmin are not yet "lag").
const lagSQL = `
	SELECT COUNT(*)
	  FROM events
	 WHERE (transaction_id, global_id) > ($1::text::xid8, $2)
	   AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())`

// EnsureSubscription creates the subscription's cursor row at the zero cursor if
// absent (idempotent). It is called once at worker start so the first
// [Source.LoadCursor] always finds a row.
func (s *Source) EnsureSubscription(ctx context.Context, name string) error {
	if _, err := s.conn.Exec(ctx, ensureSubscriptionSQL, name); err != nil {
		return fmt.Errorf("projection: ensuring subscription %q: %w", name, err)
	}
	return nil
}

// LoadCursor reads the persisted cursor for the named subscription. It returns
// the zero [Cursor] when the row is absent (treated as "start from the
// beginning"), so a caller that skipped [Source.EnsureSubscription] still starts
// safely.
func (s *Source) LoadCursor(ctx context.Context, name string) (Cursor, error) {
	var (
		txn uint64
		gid int64
	)
	err := s.conn.QueryRow(ctx, loadCursorSQL, name).Scan(&txn, &gid)
	if errors.Is(err, pgx.ErrNoRows) {
		return Cursor{}, nil
	}
	if err != nil {
		return Cursor{}, fmt.Errorf("projection: loading cursor for %q: %w", name, err)
	}
	return Cursor{TransactionID: txn, GlobalID: gid}, nil
}

// SaveCursor persists the advanced cursor for the named subscription. The worker
// calls it after a batch is folded into every projection, so the checkpoint never
// runs ahead of the projected events (at-least-once over the feed; a crash
// re-reads from the last saved cursor and the additive cost fold is idempotent
// over a re-read only if the cursor advanced — see the runner's ordering).
func (s *Source) SaveCursor(ctx context.Context, name string, c Cursor) error {
	tag, err := s.conn.Exec(ctx, saveCursorSQL, name, uint64ToText(c.TransactionID), c.GlobalID)
	if err != nil {
		return fmt.Errorf("projection: saving cursor for %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("projection: saving cursor for %q: subscription row missing (call EnsureSubscription)", name)
	}
	return nil
}

// FetchBatch reads up to limit FULLY-SETTLED events strictly after cur, ordered by
// the gap-safe (transaction_id, global_id) key. It returns the rows (cost fields
// decoded lazily by the rollup) so the caller folds them and then advances the
// cursor to the last row. An empty result means nothing has settled below xmin
// since the cursor — the worker then waits for the next wakeup/tick.
func (s *Source) FetchBatch(ctx context.Context, cur Cursor, limit int) ([]EventRow, error) {
	rows, err := s.conn.Query(ctx, fetchBatchSQL, uint64ToText(cur.TransactionID), cur.GlobalID, limit)
	if err != nil {
		return nil, fmt.Errorf("projection: fetch batch: %w", err)
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var (
			txnText     string
			gid         int64
			seq         int64
			tenantID    string
			sessionID   string
			eventType   string
			payload     []byte
			contentHash []byte
			chainHash   []byte
		)
		if err := rows.Scan(&txnText, &gid, &seq, &tenantID, &sessionID, &eventType, &payload, &contentHash, &chainHash); err != nil {
			return nil, fmt.Errorf("projection: scanning event row: %w", err)
		}
		txn, perr := textToUint64(txnText)
		if perr != nil {
			return nil, fmt.Errorf("projection: parsing transaction_id %q: %w", txnText, perr)
		}
		out = append(out, EventRow{
			TransactionID: txn,
			GlobalID:      gid,
			Seq:           seq,
			TenantID:      tenantID,
			SessionID:     sessionID,
			Type:          domain.EventType(eventType),
			Payload:       payload,
			ContentHash:   contentHash,
			ChainHash:     chainHash,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("projection: iterating event rows: %w", err)
	}
	return out, nil
}

// Lag returns the number of settled-below-xmin events strictly after cur — the
// projection lag the worker publishes to the lag gauge (FR-OBS-02). It is a cheap
// COUNT over the same predicate the fetch uses.
func (s *Source) Lag(ctx context.Context, cur Cursor) (int64, error) {
	var n int64
	if err := s.conn.QueryRow(ctx, lagSQL, uint64ToText(cur.TransactionID), cur.GlobalID).Scan(&n); err != nil {
		return 0, fmt.Errorf("projection: reading lag: %w", err)
	}
	return n, nil
}

// Compile-time assertion that a pgx.Conn satisfies Conn (the production wiring
// passes one). Kept as a doc-anchor; the interface is intentionally the subset
// the source uses.
var _ Conn = (*pgx.Conn)(nil)
