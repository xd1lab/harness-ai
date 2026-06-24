package eventstore

// TDD (red) pure (no-DB) tests pinning the SHAPE of Feature O's read-only cost
// aggregation queries. They run in the default build (no integration tag) so they
// give a fast red proof without Docker: they reference SQL constants that do NOT
// yet exist (selectSessionCostByModelSQL, selectTenantCostByModelSQL), so the
// package fails to compile until the cost read path lands.
//
// The query shapes are load-bearing for the resolved decisions (DECISIONS §E,
// SPEC AC-3/AC-2):
//   - selectSessionCostByModelSQL: per-session per-model rollup —
//     SELECT model, SUM(cost_usd)::float8, SUM(input_tokens), …, COUNT(*) AS turns
//     FROM session_cost_events WHERE session_id = $1 GROUP BY model.
//     SUM in NUMERIC, cast ::float8 at the edge (no float accumulation error),
//     rides idx_scost_session.
//   - selectTenantCostByModelSQL: per-tenant per-model rollup — the SAME shape
//     with NO session filter (RLS scopes it to the principal tenant) PLUS a
//     COUNT(DISTINCT session_id) for session_count. Rides idx_scost_tenant_model.
// A string-level guard so a refactor cannot silently drop the GROUP BY, the
// NUMERIC→float8 cast, the per-token SUMs, or the tenant path's distinct-session
// count.

import (
	"strings"
	"testing"
)

// TestSelectSessionCostByModelSQLShape pins the per-session per-model rollup:
// a session filter, GROUP BY model, the NUMERIC→float8 cost cast, the per-token
// SUMs, and the turn count.
func TestSelectSessionCostByModelSQLShape(t *testing.T) {
	t.Parallel()
	q := selectSessionCostByModelSQL
	for _, want := range []string{
		"FROM session_cost_events",
		"session_id = $1", // per-session filter
		"GROUP BY model",  // per-model partition
		"SUM(cost_usd)",   // aggregate cost in NUMERIC
		"::float8",        // cast to double at the edge (no float accumulation drift)
		"SUM(input_tokens)",
		"SUM(output_tokens)",
		"SUM(cache_read_tokens)",
		"SUM(cache_write_tokens)",
		"SUM(reasoning_tokens)",
		"COUNT(*)", // turns per model
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectSessionCostByModelSQL missing %q\n full: %s", want, q)
		}
	}
}

// TestSelectTenantCostByModelSQLShape pins the per-tenant per-model rollup: NO
// session filter (RLS scopes the read to the principal tenant), GROUP BY model,
// the same NUMERIC→float8 SUMs, and a COUNT(DISTINCT session_id) for the
// session_count.
func TestSelectTenantCostByModelSQLShape(t *testing.T) {
	t.Parallel()
	q := selectTenantCostByModelSQL
	for _, want := range []string{
		"FROM session_cost_events",
		"GROUP BY model",
		"SUM(cost_usd)",
		"::float8",
		"COUNT(*)",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectTenantCostByModelSQL missing %q\n full: %s", want, q)
		}
	}
	// The tenant path must NOT bind a session id — it is principal/RLS-scoped, not
	// session-filtered (a $1 session filter here would be a tenant-wide leak bug or
	// an empty result, depending on the value).
	if strings.Contains(q, "session_id = $1") {
		t.Errorf("selectTenantCostByModelSQL must not filter by a session id (RLS scopes to the tenant): %s", q)
	}
}

// TestTenantCostHasDistinctSessionCount pins the session_count source: the tenant
// rollup must surface COUNT(DISTINCT session_id) so the response can report how
// many distinct sessions carry cost (SPEC AC-2). It is asserted as a separate
// constant (the count is a scalar query, not a per-model GROUP BY row).
func TestTenantCostHasDistinctSessionCount(t *testing.T) {
	t.Parallel()
	q := selectTenantSessionCountSQL
	for _, want := range []string{
		"FROM session_cost_events",
		"COUNT(DISTINCT session_id)",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectTenantSessionCountSQL missing %q\n full: %s", want, q)
		}
	}
}
