// Package agentctx is the orchestrator's context/memory manager (T-LOOP-02;
// FR-CTX-01/02/03; architecture §5.1 "Context/memory manager"). It owns four
// responsibilities, all expressed as pure logic over the folded event log so
// they are deterministic and unit-testable against fakes without a database,
// gRPC, or a real provider:
//
//  1. Token accounting — counting the model-visible window's tokens via an
//     injected [TokenCounter] port (faked in tests; the production adapter is
//     the model-gateway's capability-gated CountTokens).
//  2. Threshold-triggered compaction — when the window approaches the model's
//     context budget, [Manager.PlanCompaction] emits exactly one
//     [domain.CompactionPerformed] boundary; [BuildWindow] then reduces history
//     before that boundary to a single summary message and renders only the
//     post-boundary turns live ("reinitiating from a summary"; architecture
//     §6.6).
//  3. Tool-result clearing — [ValidateClear] enforces the append-time rules of
//     a [domain.ToolResultCleared] (the target must exist, be a
//     [domain.ToolResult], and not already be cleared → [ErrFailedPrecondition];
//     a double-clear is an idempotent no-op), and [BuildWindow] renders a STUB
//     for a cleared result while the full result stays in the log/blob store
//     (architecture §6.5).
//  4. Cache-prefix marking — [BuildCachePrefix] marks ONLY stable, tenant-
//     agnostic content (system prompt + tool definitions) as cacheable and
//     NEVER session history, and derives a tenant-scoped cache key so two
//     tenants can never share a prefix carrying private content (architecture
//     §8.10).
//
// # Why "agentctx" and not "context"
//
// The package is named agentctx, not context, so it never shadows the standard
// library context package at its many import sites in the loop.
//
// # Purity and the determinism rule
//
// Apart from the injected [TokenCounter] (whose Count takes a context.Context so
// the production adapter can make a deadline-bound RPC), every function here is
// pure: it reads its inputs and returns a value with no I/O and no hidden state.
// In keeping with the cross-cutting determinism rule (NFR-TEST-01; ADR-0015)
// this package calls no time.Now/rand/uuid — the loop appends the boundary event
// (minting any ids and stamping the seq through the EventLogPort), so agentctx
// produces only the typed payload to append. It imports nothing from gen/ and no
// gRPC (depguard-enforced for the app layer).
package agentctx

import (
	"strings"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Window is the model-visible projection of a session: the system prompt, the
// ordered conversation messages the model should see this turn, and the
// cache-prefix marking for the stable region. It is what the loop turns into an
// [llm.Request] (System + Messages + Tools) for the next Generate/Stream call.
//
// The Messages slice already reflects tool-result clearing (cleared results are
// rendered as stubs, not their full content) and any compaction boundary
// (pre-boundary history is collapsed into a single summary message). The full,
// uncleared, un-summarized history always remains in the event log; Window is a
// reduced view, never a mutation of the log.
type Window struct {
	// System is the system prompt for the turn, lifted from the session's
	// [domain.SessionStarted.SystemPrompt]. It is the tenant-agnostic stable
	// content that [BuildCachePrefix] may mark cacheable.
	System string
	// Messages is the ordered, model-visible conversation, oldest first. It is
	// the projection the loop places in [llm.Request.Messages].
	Messages []llm.Message
}

// WindowOptions tunes how [BuildWindow] renders the model-visible window. The
// zero value is valid and yields the default rendering (cleared results shown as
// the default stub, compaction summarized with the default summary text).
type WindowOptions struct {
	// ClearedStub overrides the placeholder text rendered in place of a cleared
	// tool result. Empty uses [DefaultClearedStub].
	ClearedStub string
	// CompactionSummary overrides the placeholder text used for the single
	// summary message that replaces pre-compaction history. Empty uses
	// [DefaultCompactionSummary].
	CompactionSummary string
}

const (
	// DefaultClearedStub is the placeholder rendered in the model-visible window
	// in place of a tool result that a [domain.ToolResultCleared] superseded. It
	// is deliberately short (token reclamation is the whole point of clearing)
	// and signals to the model that output was reclaimed, not lost from history
	// (architecture §6.5).
	DefaultClearedStub = "[tool result cleared to reclaim context; re-run the tool if its output is needed again]"

	// DefaultCompactionSummary is the placeholder used for the single summary
	// message that replaces the conversation history preceding a
	// [domain.CompactionPerformed] boundary. The loop's PreCompact hook
	// (FR-CTX-01 AC-2) may supply a richer summary; absent one, this marker keeps
	// replay deterministic and tells the model that earlier turns were compacted
	// (architecture §6.6).
	DefaultCompactionSummary = "[earlier conversation compacted to reclaim context]"
)

// BuildWindow folds events into the model-visible [Window]: the system prompt
// plus the ordered conversation messages, oldest first, with tool-result
// clearing and compaction applied (FR-CTX-02 AC-1, FR-CTX-01 AC-1).
//
// Rendering rules (all pure, all derived from the log — never a log mutation):
//
//   - The system prompt is taken from the last [domain.SessionStarted] seen.
//   - A [domain.MessageAppended] contributes its [llm.Message] verbatim.
//   - A [domain.AssistantMessage] contributes its assembled [llm.Message]
//     verbatim (including any tool-call content parts).
//   - A [domain.ToolResult] contributes a [llm.RoleTool] message carrying an
//     [llm.ToolResult] content part — UNLESS a later [domain.ToolResultCleared]
//     superseded it, in which case the part's Content is replaced by the
//     configured stub (opts.ClearedStub or [DefaultClearedStub]) while IsError
//     is cleared. A double-clear renders exactly one stub (idempotent;
//     architecture §6.5).
//   - A [domain.CompactionPerformed] boundary collapses ALL conversation
//     messages that precede it (in seq order) into a single summary
//     [llm.RoleUser] message (opts.CompactionSummary or
//     [DefaultCompactionSummary]); only messages after the latest boundary are
//     rendered live, so the next window is reduced ("reinitiating from a
//     summary"; architecture §6.6).
//   - All other event kinds (TurnStarted, deltas, ToolExecutionStarted,
//     permission/approval/MCP/bypass events, and per-event [domain.PlanUpdated])
//     are not part of the per-event model-visible conversation and are skipped by
//     [renderMessage].
//   - The LATEST [domain.PlanUpdated] for the session is re-surfaced as a SINGLE
//     synthetic context-note [llm.RoleUser] message appended AFTER the live
//     window, so the model always sees its current plan and stale plan updates do
//     not duplicate (AC-18; ADR-0031). This mirrors how a todo/memory list is
//     re-surfaced. Only the most recent PlanUpdated contributes a note; if none
//     exists, no plan note is added (default behavior is byte-identical).
//
// It returns an error only for a malformed log (currently none is defined; the
// signature returns error so future validation is non-breaking). events are not
// modified.
func BuildWindow(events []domain.EventEnvelope, opts WindowOptions) (Window, error) {
	stub := opts.ClearedStub
	if stub == "" {
		stub = DefaultClearedStub
	}
	summary := opts.CompactionSummary
	if summary == "" {
		summary = DefaultCompactionSummary
	}

	// First pass: which ToolResult seqs were cleared? Clearing references the
	// (session_id, seq) pair; a result is cleared iff some ToolResultCleared in
	// THIS stream targets its seq under the same session (architecture §6.5).
	clearedSeqs := clearedSet(events)

	// Find the latest compaction boundary; everything strictly before it is
	// summarized, everything at-or-after it renders live.
	lastCompactionSeq := int64(-1)
	for _, env := range events {
		if env.Type == domain.EventCompactionPerformed {
			if env.Seq > lastCompactionSeq {
				lastCompactionSeq = env.Seq
			}
		}
	}

	var system string
	var pre []llm.Message  // conversation messages before the last compaction boundary
	var post []llm.Message // conversation messages at/after the last compaction boundary

	// Track the latest PlanUpdated for the session so the current plan is
	// re-surfaced once after the live window (AC-18; ADR-0031). Only the most
	// recent one wins, so stale plan updates never duplicate.
	var latestPlan *domain.PlanUpdated

	for i := range events {
		env := events[i]
		// The system prompt is stable session metadata, not part of the
		// summarized/post split; always take the latest one.
		if ss, ok := env.Event.(domain.SessionStarted); ok {
			system = ss.SystemPrompt
			continue
		}

		// PlanUpdated is not a per-event conversation message (renderMessage skips
		// it); the LATEST one is re-surfaced below as a single context note.
		if pu, ok := env.Event.(domain.PlanUpdated); ok {
			p := pu
			latestPlan = &p
			continue
		}

		msg, ok := renderMessage(env, clearedSeqs, stub)
		if !ok {
			continue
		}
		if lastCompactionSeq >= 0 && env.Seq < lastCompactionSeq {
			pre = append(pre, msg)
		} else {
			post = append(post, msg)
		}
	}

	out := make([]llm.Message, 0, len(post)+1)
	// If a compaction boundary collapsed earlier history, emit a single summary
	// message in its place (only when there was pre-boundary conversation to
	// summarize, so an empty session does not gain a spurious summary).
	if lastCompactionSeq >= 0 && len(pre) > 0 {
		out = append(out, llm.Message{
			Role:    llm.RoleUser,
			Content: []llm.ContentPart{{Text: &llm.TextPart{Text: summary}}},
		})
	}
	out = append(out, post...)

	// Re-surface the current plan: a single context-note message carrying the
	// latest PlanUpdated, appended after the live window so the model sees its
	// working plan each turn (AC-18; ADR-0031). Stale plan updates contribute
	// nothing — only the latest one reaches here.
	if latestPlan != nil {
		out = append(out, llm.Message{
			Role:    llm.RoleUser,
			Content: []llm.ContentPart{{Text: &llm.TextPart{Text: renderPlanNote(*latestPlan)}}},
		})
	}

	return Window{System: system, Messages: out}, nil
}

// PlanNotePrefix is the leading marker of the synthetic context-note message that
// re-surfaces the session's current plan in [BuildWindow] (AC-18; ADR-0031). It is
// stable so tests and the model can recognize the plan note among conversation
// messages.
const PlanNotePrefix = "[current plan]"

// renderPlanNote formats the latest [domain.PlanUpdated] into the bounded, plain-
// text context note re-surfaced by [BuildWindow]. An empty plan renders a short
// "no items" note so the marker is still recognizable; otherwise each item is one
// "- [status] content" line. It performs no I/O and is deterministic.
func renderPlanNote(p domain.PlanUpdated) string {
	if len(p.Items) == 0 {
		return PlanNotePrefix + " (no items)"
	}
	var b strings.Builder
	b.WriteString(PlanNotePrefix)
	for _, it := range p.Items {
		b.WriteString("\n- [")
		b.WriteString(it.Status)
		b.WriteString("] ")
		b.WriteString(it.Content)
	}
	return b.String()
}

// renderMessage converts a single conversation-bearing envelope into the
// model-visible [llm.Message] it contributes, applying tool-result clearing. The
// second return is false for envelopes that contribute no model-visible message
// (SessionStarted is handled by the caller; control/checkpoint events, and
// [domain.PlanUpdated] — which is re-surfaced as a single latest-plan note by the
// caller, not per-event — are skipped via the default case).
func renderMessage(env domain.EventEnvelope, clearedSeqs map[int64]struct{}, stub string) (llm.Message, bool) {
	switch p := env.Event.(type) {
	case domain.MessageAppended:
		return p.Message, true

	case domain.AssistantMessage:
		return p.Message, true

	case domain.ToolResult:
		content := p.Result
		isErr := p.IsError
		if _, cleared := clearedSeqs[env.Seq]; cleared {
			// Superseded by a ToolResultCleared: render the stub, never the full
			// content. The full bytes stay in the log/blob store (architecture §6.5).
			content = stub
			isErr = false
		}
		return llm.Message{
			Role: llm.RoleTool,
			Content: []llm.ContentPart{{ToolResult: &llm.ToolResult{
				CallID:  p.CallID,
				Content: content,
				IsError: isErr,
			}}},
		}, true

	default:
		return llm.Message{}, false
	}
}

// clearedSet returns the set of seqs in events that a [domain.ToolResultCleared]
// superseded, matched fork-safely on the (ClearedSessionID, ClearedSeq) pair
// against each [domain.ToolResult]'s owning (SessionID, Seq). A seq is included
// at most once regardless of how many times it was cleared (idempotent
// double-clear; architecture §6.5).
func clearedSet(events []domain.EventEnvelope) map[int64]struct{} {
	cleared := make(map[int64]struct{})
	// Index ToolResult envelopes by (session, seq) so clearing only matches a
	// real ToolResult under the same session.
	type ref struct {
		session string
		seq     int64
	}
	results := make(map[ref]struct{})
	for _, env := range events {
		if env.Type == domain.EventToolResult {
			results[ref{env.SessionID, env.Seq}] = struct{}{}
		}
	}
	for _, env := range events {
		c, ok := env.Event.(domain.ToolResultCleared)
		if !ok {
			continue
		}
		if _, isResult := results[ref{c.ClearedSessionID, c.ClearedSeq}]; isResult {
			cleared[c.ClearedSeq] = struct{}{}
		}
	}
	return cleared
}
