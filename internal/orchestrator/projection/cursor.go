package projection

import "fmt"

// Cursor is the gap-safe read-side checkpoint persisted in the
// event_subscriptions row for a named subscription: the (TransactionID,
// GlobalID) pair of the last fully-settled event the worker has processed
// (architecture §6.6, §10.4). It is a composite cursor — ordering is by
// transaction_id first, then global_id within a transaction — exactly the
// ORDER BY the catch-up query uses, so the cursor and the scan order cannot
// drift.
//
// TransactionID is the PostgreSQL xid8 (scanned as a uint64, the pgx codec for
// xid8) of the committing transaction; GlobalID is the events.global_id of the
// row. The zero Cursor ({0, 0}) is the "never processed anything" start, which
// the migration defaults the row to.
type Cursor struct {
	// TransactionID is the xid8 (as uint64) of the last processed event's
	// committing transaction. It is the primary cursor coordinate and is NEVER
	// advanced to or past the snapshot xmin (only rows the xmin bound admitted
	// can move it).
	TransactionID uint64
	// GlobalID is the events.global_id of the last processed event, the
	// tie-breaker within a single transaction_id.
	GlobalID int64
}

// Less reports whether c orders strictly before other under the composite
// (transaction_id, global_id) ordering: a smaller transaction_id wins, and
// within an equal transaction_id a smaller global_id wins. It is the Go mirror
// of the SQL row-value comparison (transaction_id, global_id) < (…, …) and is
// used to assert monotonic, gap-free advance.
func (c Cursor) Less(other Cursor) bool {
	if c.TransactionID != other.TransactionID {
		return c.TransactionID < other.TransactionID
	}
	return c.GlobalID < other.GlobalID
}

// String renders the cursor for logs/diagnostics.
func (c Cursor) String() string {
	return fmt.Sprintf("(txn=%d,global=%d)", c.TransactionID, c.GlobalID)
}

// rowCursor is the projection of one event row onto its cursor coordinates. Rows
// scanned by the source carry it so [Advance] can move the checkpoint without
// re-reading payloads.
type rowCursor struct {
	TransactionID uint64
	GlobalID      int64
}

// cursor returns the row's coordinates as a [Cursor]. rowCursor and Cursor share
// an identical field layout, so this is a direct conversion.
func (r rowCursor) cursor() Cursor { return Cursor(r) }

// Advance folds an in-order batch of just-read rows into a new cursor, returning
// the new cursor and how many rows it advanced over. It is the PURE cursor-fold
// the safe-advance worker uses: the rows MUST already be ordered by
// (transaction_id, global_id) ascending and MUST all be strictly greater than
// the starting cursor (the catch-up query guarantees both via its WHERE and
// ORDER BY). The new cursor is the last row's coordinates; an empty batch leaves
// the cursor unchanged (no rows settled below xmin since the last poll).
//
// Advance enforces strict monotonicity defensively: a row that does not strictly
// follow the running cursor (a duplicate or an out-of-order row) is a
// programming/contract error and panics rather than silently regressing or
// double-counting the checkpoint, since that would corrupt every downstream
// projection. The xmin bound itself is applied by the query, not here; this fold
// only advances over rows the query already admitted, so it can never move past
// xmin.
func (c Cursor) Advance(rows []rowCursor) (Cursor, int) {
	cur := c
	for i, r := range rows {
		next := r.cursor()
		if !cur.Less(next) {
			panic(fmt.Sprintf("projection: cursor advance saw a non-increasing row at index %d: current %s, row %s (rows must be ordered and strictly greater than the cursor)", i, cur, next))
		}
		cur = next
	}
	return cur, len(rows)
}
