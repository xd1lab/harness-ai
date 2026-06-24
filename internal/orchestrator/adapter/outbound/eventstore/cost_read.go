// SPDX-License-Identifier: Apache-2.0

package eventstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file is the read-only cost-aggregation surface backing the session/tenant
// cost-read API (Feature O / cost-read; ADR-0026): SessionCostByModel (per-session
// per-model rollup), TenantCostByModel (per-tenant per-model aggregate), and
// TenantSessionCostCount (distinct sessions carrying cost). All three are ADDITIVE
// methods on the adapter-side EventStore consumer-superset, NOT on the frozen
// app.EventLogPort, so the `var _ app.EventLogPort = (*Store)(nil)` assertion is
// unaffected.
//
// Like every other store read they go through beginTenantTx -> SET LOCAL
// app.current_tenant -> RLS, so a foreign tenant sees zero rows (the SQL carries
// no tenant_id filter; the 0007 SELECT policy scopes the rows) and a context with
// no tenant fails closed. The per-model split is produced at the WRITE side
// (projectord); the read side only SUM(...) GROUP BY model. The cost SUM is
// computed in NUMERIC and cast ::float8 at the edge so there is no Go float
// accumulation drift.

// ModelCostRow is the per-model cost row the store returns; aliased from the
// edge-owned [igrpc.ModelCostRow] so *Store satisfies the igrpc.EventStore
// interface without a wrapper.
type ModelCostRow = igrpc.ModelCostRow

// costRowColumns is the per-model SELECT projection (in scan order): the cost in
// NUMERIC cast to float8, the five summed token counters, the turn count, and the
// model label. It is shared by the session and tenant queries so the scan order
// cannot drift.
const costRowColumns = `model,
       SUM(cost_usd)::float8,
       SUM(input_tokens),
       SUM(output_tokens),
       SUM(cache_read_tokens),
       SUM(cache_write_tokens),
       SUM(reasoning_tokens),
       COUNT(*)`

// selectSessionCostByModelSQL is the per-session per-model rollup. RLS scopes the
// rows to the caller's tenant; the session_id filter narrows to one session. It
// rides idx_scost_session (session_id, model).
const selectSessionCostByModelSQL = `SELECT ` + costRowColumns + `
  FROM session_cost_events
 WHERE session_id = $1
 GROUP BY model`

// selectTenantCostByModelSQL is the per-tenant per-model rollup: the SAME shape
// with NO session filter (RLS scopes it to the principal tenant). It rides
// idx_scost_tenant_model (tenant_id, model).
const selectTenantCostByModelSQL = `SELECT ` + costRowColumns + `
  FROM session_cost_events
 GROUP BY model`

// selectTenantSessionCountSQL is the distinct-session count for the tenant total's
// session_count (a scalar, not a per-model GROUP BY row). RLS scopes it to the
// principal tenant.
const selectTenantSessionCountSQL = `SELECT COUNT(DISTINCT session_id)
  FROM session_cost_events`

// SessionCostByModel returns the per-model cost/usage/turns rollup for sessionID,
// RLS-scoped to the caller's tenant (a foreign tenant sees zero rows; no tenant ->
// fail-closed). It is read-only and side-effect-free.
func (s *Store) SessionCostByModel(ctx context.Context, sessionID string) ([]ModelCostRow, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectSessionCostByModelSQL, sessionID)
	if err != nil {
		return nil, fmt.Errorf("eventstore: session-cost query: %w", err)
	}
	out, err := scanModelCostRows(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit session-cost: %w", err)
	}
	return out, nil
}

// TenantCostByModel returns the per-model rollup aggregated across every session
// of the caller's tenant (RLS-scoped; a tenant with no cost rows returns an empty
// slice). It is read-only and side-effect-free. The distinct-session count is the
// separate TenantSessionCostCount method (a per-model GROUP BY row set cannot also
// be a scalar count).
func (s *Store) TenantCostByModel(ctx context.Context) ([]ModelCostRow, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectTenantCostByModelSQL)
	if err != nil {
		return nil, fmt.Errorf("eventstore: tenant-cost query: %w", err)
	}
	out, err := scanModelCostRows(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit tenant-cost: %w", err)
	}
	return out, nil
}

// TenantSessionCostCount returns the number of distinct sessions of the caller's
// tenant that carry at least one cost row — the source of
// GetTenantCostResponse.session_count. RLS-scoped; no tenant -> fail-closed.
func (s *Store) TenantSessionCostCount(ctx context.Context) (int64, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return 0, err
	}
	defer cleanup()

	var count int64
	if err := tx.QueryRow(ctx, selectTenantSessionCountSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("eventstore: tenant-session-count query: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("eventstore: commit tenant-session-count: %w", err)
	}
	return count, nil
}

// scanModelCostRows reads [costRowColumns]-ordered rows into [ModelCostRow]s. The
// token SUMs are bigint (scanned as int64) and narrowed to the llm.Usage int
// fields; the cost is already float8 from the NUMERIC cast.
func scanModelCostRows(rows pgx.Rows) ([]ModelCostRow, error) {
	var out []ModelCostRow
	for rows.Next() {
		var (
			r                                        ModelCostRow
			inTok, outTok, cacheR, cacheW, reasonTok int64
		)
		if err := rows.Scan(
			&r.Model, &r.CostUSD,
			&inTok, &outTok, &cacheR, &cacheW, &reasonTok,
			&r.Turns,
		); err != nil {
			return nil, fmt.Errorf("eventstore: scanning model-cost row: %w", err)
		}
		r.Usage = llm.Usage{
			InputTokens:      int(inTok),
			OutputTokens:     int(outTok),
			CacheReadTokens:  int(cacheR),
			CacheWriteTokens: int(cacheW),
			ReasoningTokens:  int(reasonTok),
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterating model-cost rows: %w", err)
	}
	return out, nil
}
