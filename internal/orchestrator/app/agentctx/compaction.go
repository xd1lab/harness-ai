package agentctx

import (
	"context"
	"fmt"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TokenCounter is the consumer-defined port the context manager uses to measure
// the model-visible window's token count for a given model. It is declared here
// (the package that uses it) per the clean-architecture port rule; the
// production adapter is backed by the model-gateway's capability-gated
// CountTokens, and tests inject a deterministic fake (FR-CTX-01).
//
// Count takes a context.Context because the production adapter makes a
// deadline-bound RPC; the in-repo fake ignores it. A best-effort local estimate
// is acceptable for a model whose endpoint lacks a real token-counting
// capability — the compaction trigger only needs a monotone, approximately-
// correct measure of window growth, never a billing-grade count (architecture
// §11.6: CountTokens is never used for billing).
type TokenCounter interface {
	// Count returns the number of input tokens the given system prompt,
	// conversation messages, and tool definitions occupy for model. It must be
	// safe for concurrent use. An error indicates the counter itself failed
	// (e.g. an unreachable gateway); the caller treats a failed count as
	// "cannot decide" and does not compact on it.
	Count(ctx context.Context, model string, msgs []llm.Message, tools []llm.ToolDef) (int, error)
}

// Config parameterizes a [Manager]: which model's budget applies and at what
// fraction of the context window compaction triggers.
type Config struct {
	// Model is the target model id, used both as the [TokenCounter] key and (in
	// the loop) as the model whose context window MaxContextTokens describes.
	Model string
	// MaxContextTokens is the model's total context-window size in tokens. A
	// non-positive value disables the threshold (compaction never triggers on
	// token pressure), which is useful for tests that drive clearing only.
	MaxContextTokens int
	// CompactionFraction is the fraction of MaxContextTokens at which compaction
	// triggers (e.g. 0.8 ⇒ compact when the window exceeds 80% of the budget). A
	// value ≤ 0 or > 1 is clamped to the [DefaultCompactionFraction]; the
	// resulting threshold is round(MaxContextTokens × fraction).
	CompactionFraction float64
}

// DefaultCompactionFraction is the fraction of the context window at which
// compaction triggers when [Config.CompactionFraction] is unset or out of range.
// 0.8 leaves headroom for the next turn's output and tool results before the
// hard context limit (architecture §5.1 context; §11.3 compact-and-retry).
const DefaultCompactionFraction = 0.8

// threshold returns the absolute token count at or above which compaction
// triggers, or 0 when the threshold is disabled (non-positive MaxContextTokens).
func (c Config) threshold() int {
	if c.MaxContextTokens <= 0 {
		return 0
	}
	f := c.CompactionFraction
	if f <= 0 || f > 1 {
		f = DefaultCompactionFraction
	}
	// Round to the nearest token.
	return int(float64(c.MaxContextTokens)*f + 0.5)
}

// Manager performs token accounting and decides when compaction is needed for a
// session, using an injected [TokenCounter] and a [Config]. It is the
// orchestrator-side context manager (architecture §5.1). A Manager holds no
// per-session mutable state: every decision is a pure function of the events it
// is handed plus the injected counter, so it is safe to share across sessions
// and goroutines (the counter must itself be concurrency-safe).
type Manager struct {
	counter TokenCounter
	cfg     Config
}

// NewManager returns a [Manager] that measures windows with counter and decides
// compaction per cfg. counter must be non-nil for [Manager.PlanCompaction] to
// measure token pressure.
func NewManager(counter TokenCounter, cfg Config) *Manager {
	return &Manager{counter: counter, cfg: cfg}
}

// CompactionPlan is the result of [Manager.PlanCompaction]: whether compaction
// should run and, if so, the single [domain.CompactionPerformed] payload the
// loop must append to mark the boundary. The loop is responsible for the append
// (stamping seq/actor/request_id via the EventLogPort) and for running the
// PreCompact hook beforehand (FR-CTX-01 AC-2); the Manager only computes the
// decision and the payload, keeping itself pure and I/O-free.
type CompactionPlan struct {
	// ShouldCompact reports whether the current window crossed the configured
	// threshold and a compaction boundary should be appended.
	ShouldCompact bool
	// Event is the [domain.CompactionPerformed] payload to append when
	// ShouldCompact is true; nil otherwise. BeforeTokens is the measured count
	// that crossed the threshold and AfterTokens is the projected post-summary
	// count, so the cost-rollup and observability paths can see the reclamation.
	Event *domain.CompactionPerformed
	// CurrentTokens is the measured token count of the window as it stands
	// (whether or not compaction triggered), exposed so the loop can record or
	// log token pressure even on the no-compaction path.
	CurrentTokens int
}

// PlanCompaction measures the model-visible window built from events and decides
// whether to compact (FR-CTX-01 AC-1). When the measured count is at or above
// the configured threshold it returns a plan with ShouldCompact=true carrying
// exactly one [domain.CompactionPerformed] payload whose AfterTokens is the
// projected count of the post-compaction window (history before the boundary
// collapsed to a single summary message). Otherwise ShouldCompact is false and
// Event is nil.
//
// It calls the injected [TokenCounter] (at most twice: once for the current
// window, and once more to project the reduced window when compaction triggers).
// A counter error is returned to the caller, which treats it as "cannot decide"
// and does not compact. The threshold is disabled (never triggers) when
// [Config.MaxContextTokens] is non-positive. events are not modified.
func (m *Manager) PlanCompaction(ctx context.Context, events []domain.EventEnvelope) (CompactionPlan, error) {
	win, err := BuildWindow(events, WindowOptions{})
	if err != nil {
		return CompactionPlan{}, err
	}

	current, err := m.counter.Count(ctx, m.cfg.Model, win.Messages, nil)
	if err != nil {
		return CompactionPlan{}, fmt.Errorf("agentctx: counting current window: %w", err)
	}

	threshold := m.cfg.threshold()
	if threshold <= 0 || current < threshold {
		return CompactionPlan{ShouldCompact: false, CurrentTokens: current}, nil
	}

	// Project the post-compaction window: collapse everything up to the current
	// head into a single summary message, keeping only the system prompt. This is
	// what BuildWindow will render once the boundary event lands at head+1.
	projected := projectedAfterWindow(win)
	after, err := m.counter.Count(ctx, m.cfg.Model, projected.Messages, nil)
	if err != nil {
		return CompactionPlan{}, fmt.Errorf("agentctx: counting projected window: %w", err)
	}

	return CompactionPlan{
		ShouldCompact: true,
		CurrentTokens: current,
		Event: &domain.CompactionPerformed{
			BeforeTokens: current,
			AfterTokens:  after,
			Reason:       reasonApproachingWindow,
		},
	}, nil
}

// reasonApproachingWindow is the [domain.CompactionPerformed.Reason] recorded
// when compaction is triggered by token pressure (the window approaching the
// model's context budget; architecture §5.1). A separate compact-and-retry path
// (driven by [llm.StopContextWindowExceeded]; architecture §11.3) belongs to the
// loop and would record its own reason.
const reasonApproachingWindow = "window approaching model context budget"

// projectedAfterWindow returns the window that [BuildWindow] would render
// immediately after a compaction boundary is appended at the current head: the
// system prompt is preserved and the entire conversation collapses to a single
// summary message. It is used to estimate AfterTokens deterministically without
// mutating the log.
func projectedAfterWindow(win Window) Window {
	if len(win.Messages) == 0 {
		return win
	}
	return Window{
		System: win.System,
		Messages: []llm.Message{{
			Role:    llm.RoleUser,
			Content: []llm.ContentPart{{Text: &llm.TextPart{Text: DefaultCompactionSummary}}},
		}},
	}
}
