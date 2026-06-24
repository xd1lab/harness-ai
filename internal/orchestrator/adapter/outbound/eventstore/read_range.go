// SPDX-License-Identifier: Apache-2.0

package eventstore

import (
	"context"
	"fmt"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// This file is the read-only event-range surface backing the event-log read +
// time-travel API (Feature M / event-read): LoadRange (a keyset page of events)
// and LoadUpTo (the bounded fold window for at-seq reconstruction). Both are
// ADDITIVE methods on the adapter-side EventStore consumer-superset, NOT on the
// frozen app.EventLogPort, so the `var _ app.EventLogPort = (*Store)(nil)`
// assertion is unaffected.
//
// Like every other store read both go through beginTenantTx -> SET LOCAL
// app.current_tenant -> RLS, so a foreign tenant sees nothing (the SQL carries no
// tenant_id filter; RLS scopes the rows). They are read-only and side-effect-free:
// the load-bearing "time-travel uses Load-then-fold, NEVER Fork" guarantee — a
// read must never create a session row or append an event.

// selectEventsRangeSQL is the keyset page read: events with seq strictly greater
// than the cursor, oldest first, capped at a limit. It rides idx_events_session_seq
// (the (session_id, seq) index) and uses NO OFFSET so deep pages do not degrade.
const selectEventsRangeSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq > $2 ORDER BY seq LIMIT $3"

// selectEventsUpToSeqSQL is the bounded fold window: events with seq <= the
// inclusive upper bound, oldest first — the [1..at_seq] window the server folds to
// reconstruct the at-seq projection. No LIMIT (the whole window is folded).
const selectEventsUpToSeqSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq <= $2 ORDER BY seq"

// LoadRange returns sessionID's events with seq strictly greater than afterSeq,
// oldest first, capped at limit (a keyset page). It is the store half of the
// ListSessionEvents RPC. It is RLS-scoped via beginTenantTx, so a foreign tenant
// sees no rows; a context with no tenant fails closed (beginTenantTx errors). It
// is read-only and side-effect-free.
func (s *Store) LoadRange(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectEventsRangeSQL, sessionID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load-range query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit load-range: %w", err)
	}
	return envs, nil
}

// LoadUpTo returns sessionID's events with seq <= atSeq, oldest first — the
// bounded window the server folds for at-seq state reconstruction. atSeq <= 0
// yields the empty window (no events); atSeq beyond head yields the whole stream.
// It is RLS-scoped via beginTenantTx (foreign tenant -> no rows; no tenant ->
// fail-closed) and is read-only and side-effect-free.
func (s *Store) LoadUpTo(ctx context.Context, sessionID string, atSeq int64) ([]domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectEventsUpToSeqSQL, sessionID, atSeq)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load-up-to query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit load-up-to: %w", err)
	}
	return envs, nil
}
