// SPDX-License-Identifier: Apache-2.0

package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// CostProjector is the projectord write-side sink that persists per-turn cost into
// session_cost_events as the [Runner] tails the GLOBAL event feed (Feature O /
// cost-read; ADR-0026). It is idempotent over the xmin cursor by EVENT IDENTITY:
// each row is keyed on the source event's global_id and inserted with
// `ON CONFLICT (global_id) DO NOTHING`, so re-processing the same event (a crash
// re-read from the saved cursor — the runner is at-least-once) is an identity
// no-op, never a double count. The projection is therefore fully rebuildable
// (TRUNCATE then re-fold from cursor 0 reproduces identical aggregates).
//
// per-model correlation is at the WRITE side: TurnStarted.Model is correlated to
// the later TurnFinished/TurnAborted by (session, TurnID). Fast path = an in-flight
// map; on a miss (cross-batch / post-restart, when the map is empty) a point lookup
// over events recovers the model; on a total miss the model is "" (the read side
// renders it as the "unknown" bucket). A non-terminal event writes nothing.
type CostProjector struct {
	conn Conn
	// modelByTurn is the in-flight TurnStarted.Model cache, keyed by (session,
	// TurnID). It is the fast path; the slow path (point lookup) covers a restart
	// that cleared it. It is bounded in practice (one entry per in-flight turn,
	// removed when the turn's terminal event is folded).
	modelByTurn map[turnKey]string
}

// turnKey is the (session, TurnID) identity a model is correlated by.
type turnKey struct {
	session string
	turnID  string
}

// NewCostProjector returns a [CostProjector] over conn. The caller owns conn's
// lifecycle; conn is expected to be the SAME operator-tier connection the
// [Source] uses (it reads the GLOBAL feed and writes the per-tenant cost rows,
// scoping each write to the source row's tenant via a best-effort SET LOCAL).
func NewCostProjector(conn Conn) *CostProjector {
	return &CostProjector{conn: conn, modelByTurn: make(map[turnKey]string)}
}

// insertCostEventSQL is the per-event-idempotent cost insert. The natural PK is
// global_id, so ON CONFLICT (global_id) DO NOTHING makes a re-read of the same
// event a no-op (the at-least-once cursor cannot double-count).
const insertCostEventSQL = `
	INSERT INTO session_cost_events
		(global_id, tenant_id, session_id, model, event_type, cost_usd,
		 input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (global_id) DO NOTHING`

// setTenantConfigSQL scopes the connection to the source row's tenant before the
// insert. It is best-effort/advisory under an RLS-bypassing operator role and
// ENFORCING under a NOBYPASSRLS writer; either way the written tenant_id is COPIED
// from the source EventRow, so correctness does not depend on the GUC in v1.
const setTenantConfigSQL = `SELECT set_config('app.current_tenant', $1, true)`

// recoverModelSQL is the slow-path point lookup: the most recent TurnStarted.Model
// for (session, turn) before this terminal event's seq. It is used when the
// in-flight map missed (cross-batch / post-restart). It rides idx_events_session_seq.
const recoverModelSQL = `
	SELECT payload->>'Model'
	  FROM events
	 WHERE session_id = $1
	   AND event_type = $2
	   AND seq < $3
	 ORDER BY seq DESC
	 LIMIT 1`

// Project writes one cost row per cost-bearing terminal event in rows, in order.
// It first records every TurnStarted's model in the in-flight map (so a same-batch
// TurnStarted->TurnFinished correlates on the fast path), then, for each terminal
// event, resolves the model (map -> point lookup -> ""), best-effort scopes the
// tenant, and inserts the idempotent cost row. A non-terminal, non-started event
// contributes nothing. An insert error is returned (the runner logs it and does
// NOT advance the cursor, so the batch is re-read — idempotently — next poll).
func (p *CostProjector) Project(ctx context.Context, rows []EventRow) error {
	// Pass 1: record same-batch TurnStarted models for the fast path.
	for _, r := range rows {
		if r.Type != domain.EventTurnStarted {
			continue
		}
		var ts domain.TurnStarted
		if err := decodeJSON(r.Payload, &ts); err != nil {
			return fmt.Errorf("projection: decoding TurnStarted (global_id=%d): %w", r.GlobalID, err)
		}
		p.modelByTurn[turnKey{session: r.SessionID, turnID: ts.TurnID}] = ts.Model
	}

	// Pass 2: write a cost row for each terminal event.
	for _, r := range rows {
		ct, ok, err := terminalCost(r)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		model := p.resolveModel(ctx, r, ct.turnID)
		if err := p.writeCostRow(ctx, r, model, ct); err != nil {
			return err
		}
		// The turn is accounted; drop its in-flight entry to bound memory.
		delete(p.modelByTurn, turnKey{session: r.SessionID, turnID: ct.turnID})
	}
	return nil
}

// terminalRow carries the decoded cost-bearing fields of a turn-terminal event.
type terminalRow struct {
	turnID    string
	eventType string
	cost      float64
	usage     usageCounts
}

// usageCounts is the token usage extracted from a terminal event's payload.
type usageCounts struct {
	in, out, cacheR, cacheW, reasoning int64
}

// terminalCost decodes a row's per-turn cost/usage when it is a cost-bearing
// terminal event (TurnFinished / TurnAborted), reporting ok=false for any other
// type. A malformed terminal payload is a hard error (the log is the source of
// truth; a silently-dropped cost would make the rollup disagree with the events).
func terminalCost(r EventRow) (terminalRow, bool, error) {
	switch r.Type {
	case domain.EventTurnFinished:
		var tf domain.TurnFinished
		if err := decodeJSON(r.Payload, &tf); err != nil {
			return terminalRow{}, false, fmt.Errorf("projection: decoding TurnFinished (global_id=%d): %w", r.GlobalID, err)
		}
		return terminalRow{
			turnID:    tf.TurnID,
			eventType: string(domain.EventTurnFinished),
			cost:      tf.CostUSD,
			usage:     usageFrom(tf.Usage),
		}, true, nil
	case domain.EventTurnAborted:
		var ta domain.TurnAborted
		if err := decodeJSON(r.Payload, &ta); err != nil {
			return terminalRow{}, false, fmt.Errorf("projection: decoding TurnAborted (global_id=%d): %w", r.GlobalID, err)
		}
		return terminalRow{
			turnID:    ta.TurnID,
			eventType: string(domain.EventTurnAborted),
			cost:      ta.CostUSD,
			usage:     usageFrom(ta.UsageSoFar),
		}, true, nil
	default:
		return terminalRow{}, false, nil
	}
}

// resolveModel returns the model for a terminal event: the in-flight map first
// (fast path), then a point lookup over events (slow path for cross-batch /
// post-restart), then "" (the unknown bucket) on a total miss. A point-lookup
// error (including pgx.ErrNoRows) yields "" — never a hard failure, so a missing
// TurnStarted only degrades to "unknown".
func (p *CostProjector) resolveModel(ctx context.Context, r EventRow, turnID string) string {
	if m, ok := p.modelByTurn[turnKey{session: r.SessionID, turnID: turnID}]; ok {
		return m
	}
	var model string
	err := p.conn.QueryRow(ctx, recoverModelSQL, r.SessionID, string(domain.EventTurnStarted), r.Seq).Scan(&model)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			// A non-no-rows error is logged-by-degradation: we still proceed with the
			// unknown bucket rather than failing the whole batch (the model is a
			// best-effort attribution; cost itself is never lost).
			return ""
		}
		return ""
	}
	return model
}

// writeCostRow scopes the connection to the source tenant (best-effort) and
// inserts the idempotent cost row.
func (p *CostProjector) writeCostRow(ctx context.Context, r EventRow, model string, ct terminalRow) error {
	// Best-effort per-row tenant scoping (advisory under a bypassing role, enforcing
	// under a NOBYPASSRLS writer). A failure here is non-fatal: the written tenant_id
	// is copied from the row regardless.
	_, _ = p.conn.Exec(ctx, setTenantConfigSQL, r.TenantID)

	if _, err := p.conn.Exec(ctx, insertCostEventSQL,
		r.GlobalID, r.TenantID, r.SessionID, model, ct.eventType, ct.cost,
		ct.usage.in, ct.usage.out, ct.usage.cacheR, ct.usage.cacheW, ct.usage.reasoning,
	); err != nil {
		return fmt.Errorf("projection: inserting cost row (global_id=%d): %w", r.GlobalID, err)
	}
	return nil
}

// usageFrom maps an [llm.Usage] (int counters) to the int64 column args.
func usageFrom(u llm.Usage) usageCounts {
	return usageCounts{
		in:        int64(u.InputTokens),
		out:       int64(u.OutputTokens),
		cacheR:    int64(u.CacheReadTokens),
		cacheW:    int64(u.CacheWriteTokens),
		reasoning: int64(u.ReasoningTokens),
	}
}

// decodeJSON unmarshals a JSONB payload into v, returning a wrapped error.
func decodeJSON(payload []byte, v any) error {
	if err := json.Unmarshal(payload, v); err != nil {
		return err
	}
	return nil
}
