package projection

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// SessionKey is the (tenant, session) identity a cost rollup is keyed by. Cost is
// rolled up PER session within its tenant (never cross-tenant), matching the
// per-session/tenant rollup the task and architecture §3 describe. It is a
// comparable struct so it can key a Go map directly.
type SessionKey struct {
	// TenantID is the owning tenant (events.tenant_id).
	TenantID string
	// SessionID is the session stream (events.session_id).
	SessionID string
}

// String renders the key for logs/diagnostics.
func (k SessionKey) String() string { return k.TenantID + "/" + k.SessionID }

// CostTotals is the folded cost-and-usage rollup for one [SessionKey]. It is the
// derived read model the cost projection maintains: the sum of every turn's
// CostUSD plus the summed token usage, and the count of cost-bearing turn-terminal
// events folded in. The numbers equal the event sum exactly (the fold is additive
// over TurnFinished/TurnAborted; FR-OBS-02).
type CostTotals struct {
	// CostUSD is the summed per-turn cost over all folded turn-terminal events.
	CostUSD float64
	// Usage is the summed token usage over all folded turn-terminal events.
	Usage llm.Usage
	// Turns is the number of cost-bearing terminal events folded (TurnFinished +
	// TurnAborted), used for averaging and as a sanity check against the log.
	Turns int
}

// EventRow is one event read from the GLOBAL feed by the projection source,
// carrying the cursor coordinates, the tenant/session scope, the event type, and
// the raw JSONB payload. The payload is decoded lazily (only the turn-terminal
// types carry cost) so the worker does not pay to unmarshal every event kind.
//
// It is deliberately a thin, dependency-light row (not a [domain.EventEnvelope])
// because the projector reads the events table directly and only needs the cost
// and cursor fields; building a full envelope would require decoding payloads the
// rollup ignores.
type EventRow struct {
	// TransactionID is the committing transaction's xid8 (as uint64) — the
	// primary cursor coordinate.
	TransactionID uint64
	// GlobalID is the events.global_id — the cursor tie-breaker.
	GlobalID int64
	// Seq is the per-session sequence number (events.seq). It is carried so the
	// CostProjector's slow-path model recovery can bound the TurnStarted point
	// lookup by `seq < $2` (Feature O / cost-read). The in-memory RollupFold ignores
	// it; it is an additive field that does not affect the cost fold.
	Seq int64
	// TenantID is the owning tenant (events.tenant_id).
	TenantID string
	// SessionID is the session stream (events.session_id).
	SessionID string
	// Type is the event discriminator (events.event_type).
	Type domain.EventType
	// Payload is the raw JSONB payload (events.payload), decoded on demand by the
	// cost fold for turn-terminal types only.
	Payload []byte
	// ContentHash is events.content_hash — SHA-256 over the canonical payload bytes
	// (migration 0009). It is an ADDITIVE field the cost fold IGNORES: the
	// audit-checkpoint signer (Batch-5B) accumulates these as the checkpoint leaves,
	// and the SIEM exporter emits its hex. It is nil for pre-0009 (unchained) rows.
	ContentHash []byte
	// ChainHash is events.chain_hash — SHA-256(prev_chain_hash || content_hash),
	// the in-DB tamper-evident link (migration 0009). ADDITIVE / ignored by the cost
	// fold; carried so the SIEM exporter can emit its hex in each frame. nil for
	// pre-0009 (unchained) rows.
	ChainHash []byte
	// Actor is events.actor — the descriptor of who produced the event (defaults to
	// "system"). ADDITIVE / ignored by the cost fold; carried so the SIEM exporter
	// can include it as a frame descriptor. Empty for rows the source does not
	// populate it on.
	Actor string
	// CreatedAt is events.created_at — the event's commit timestamp. ADDITIVE /
	// ignored by the cost fold; carried so the SIEM exporter can include it as a
	// frame descriptor. Zero for rows the source does not populate it on.
	CreatedAt time.Time
}

// key returns the row's (tenant, session) rollup key.
func (r EventRow) key() SessionKey {
	return SessionKey{TenantID: r.TenantID, SessionID: r.SessionID}
}

// rowCursor projects the row onto its cursor coordinates for [Cursor.Advance].
func (r EventRow) rowCursor() rowCursor {
	return rowCursor{TransactionID: r.TransactionID, GlobalID: r.GlobalID}
}

// turnCost extracts the per-turn (CostUSD, Usage) a row contributes to the
// rollup, and whether the row is a cost-bearing turn-terminal event at all.
//
// Only [domain.TurnFinished] and [domain.TurnAborted] carry per-turn cost
// (architecture §11.6, §3): TurnFinished is normal completion, TurnAborted is an
// interrupted/recovered turn whose UsageSoFar/CostUSD MUST still be accounted so
// partial turns are billed, not under-counted (ADR-0012 §"Durable turn
// boundaries"). Every other event type contributes nothing and is skipped. The
// payload is decoded here (not eagerly on scan) so non-cost events cost no JSON
// work.
func (r EventRow) turnCost() (cost float64, usage llm.Usage, ok bool, err error) {
	switch r.Type {
	case domain.EventTurnFinished:
		var tf domain.TurnFinished
		if uerr := json.Unmarshal(r.Payload, &tf); uerr != nil {
			return 0, llm.Usage{}, false, fmt.Errorf("projection: decoding TurnFinished payload (global_id=%d): %w", r.GlobalID, uerr)
		}
		return tf.CostUSD, tf.Usage, true, nil
	case domain.EventTurnAborted:
		var ta domain.TurnAborted
		if uerr := json.Unmarshal(r.Payload, &ta); uerr != nil {
			return 0, llm.Usage{}, false, fmt.Errorf("projection: decoding TurnAborted payload (global_id=%d): %w", r.GlobalID, uerr)
		}
		return ta.CostUSD, ta.UsageSoFar, true, nil
	default:
		return 0, llm.Usage{}, false, nil
	}
}

// RollupFold folds a batch of in-order event rows into the running per-session
// cost rollup, mutating totals in place and returning it (so callers can fold
// successive batches into the same accumulator across polls). The fold is PURE
// (no I/O): it decodes only the turn-terminal payloads and adds their CostUSD and
// Usage into the row's (tenant, session) bucket, leaving every other event kind
// untouched. A malformed turn-terminal payload is a hard error (the log is the
// source of truth; a silently-dropped cost would make the rollup disagree with
// the events sum).
//
// totals may be nil, in which case a fresh map is allocated and returned. It is
// the function the cost projection unit-tests drive with hand-built rows to prove
// the rollup equals the event sum (FR-OBS-02).
func RollupFold(totals map[SessionKey]*CostTotals, rows []EventRow) (map[SessionKey]*CostTotals, error) {
	if totals == nil {
		totals = make(map[SessionKey]*CostTotals, len(rows))
	}
	for _, r := range rows {
		cost, usage, ok, err := r.turnCost()
		if err != nil {
			return totals, err
		}
		if !ok {
			continue
		}
		k := r.key()
		t := totals[k]
		if t == nil {
			t = &CostTotals{}
			totals[k] = t
		}
		t.CostUSD += cost
		t.Usage = addUsage(t.Usage, usage)
		t.Turns++
	}
	return totals, nil
}

// addUsage returns the field-wise sum of two [llm.Usage] values. Token counters
// are additive across turns; the rollup sums them so the per-session usage equals
// the summed per-turn usage from the log (architecture §11.6).
func addUsage(a, b llm.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens + b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens,
		ReasoningTokens:  a.ReasoningTokens + b.ReasoningTokens,
	}
}
