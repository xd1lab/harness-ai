// Package projection is the orchestrator's read-side projection worker
// (projectord's library half): a gap-safe, xmin-bounded consumer of the GLOBAL
// event feed that maintains derived read models without ever touching the write
// path (ADR-0009, ADR-0011; architecture §6.6, §10.4). It is the only place the
// append-only log is folded into operational read models, so the loop and the
// event store stay oblivious to it (no shared lock, no append blocking;
// architecture §10.4: "projectord lag never blocks an append").
//
// # The gap-safe, xmin-bounded cursor (the correctness core)
//
// PostgreSQL assigns each committed transaction a monotonic transaction_id
// (xid8), but transactions do NOT necessarily become VISIBLE in transaction_id
// order: a transaction with a lower id can commit AFTER one with a higher id. A
// naive "WHERE transaction_id > last_seen" cursor would therefore skip an
// in-flight lower-id transaction that commits late — silently dropping its
// events from cost-rollup and audit projections.
//
// The fix (architecture §10.4) is to read only FULLY-SETTLED transactions —
// those strictly below the snapshot xmin, the oldest transaction id still
// considered in-progress by any backend:
//
//	SELECT ... FROM events
//	 WHERE (transaction_id, global_id) > ($lastTxn, $lastGlobalID)
//	   AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
//	 ORDER BY transaction_id, global_id
//
// The checkpoint advances to the last row read and is NEVER advanced past the
// xmin (it can only reach rows the xmin bound already admitted). A
// late-committing lower-id transaction is simply not yet below xmin, so it is
// read on a later poll once it settles — it is delayed, never skipped
// (NFR-REL-04). LISTEN/NOTIFY is only a wakeup HINT; on every wakeup and on a
// safety-net poll tick the worker re-reads from the durable
// (last_transaction_id, last_global_id) cursor in event_subscriptions, which is
// authoritative. The cursor advance ([Cursor.Advance], [Cursor.Less]) and the
// cost fold ([RollupFold]) are PURE so they are unit-tested with hand-built rows
// and no database.
//
// # Projections
//
//   - Per-(tenant, session) COST ROLLUP folded from TurnFinished / TurnAborted
//     events (their per-turn CostUSD and Usage; architecture §11.6, §3). The
//     running total matches the event sum exactly (FR-OBS-02 input).
//   - OTel/Prometheus metrics via [github.com/xd1lab/harness-ai/internal/platform/obs]:
//     the projection-lag gauge (USE saturation, FR-OBS-02) and the running cost
//     counter, exported through the obs meter (FR-OBS-01).
//   - An ORPHAN-BLOB sweeper that reclaims bytes whose owning append transaction
//     FAILED: a blobs row (or stored bytes) older than a grace period with NO
//     referencing event is deleted from the blob store, and a referenced blob is
//     NEVER deleted (FR-STATE-05; architecture §6.4 write-before-reference, §7.4).
//
// # Read-only, operator-tier
//
// The worker reads the events table DIRECTLY (it does not go through the
// eventstore adapter) over a pgx connection, using only SELECTs plus the cursor
// UPDATE on event_subscriptions and the sweeper's blobs SELECT/DELETE. The
// event_subscriptions and (sweeper) blobs reads cross tenants by design — the
// projector is an operator/read-side artifact, so it connects as a role that can
// read the whole feed (event_subscriptions is excluded from RLS; ADR-0011 §6.2).
// It is a separate package from the eventstore precisely so it cannot
// accidentally take the write path's locks.
package projection
