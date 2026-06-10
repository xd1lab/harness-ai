package agent

import (
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// ClientSink is the consumer-defined port the loop uses to forward live
// generation deltas to the connected client (architecture §3, §4.3). The loop
// streams TextDelta/ThinkingDelta to the sink as they arrive WHILE feeding the
// same reader to the pure assembler, so the human sees tokens live and the log
// records only the assembled message (raw byte-deltas are not stored;
// architecture §4.3). Implementations must be safe for concurrent use: the loop
// forwards from the streaming goroutine. A nil sink is permitted (the loop
// discards deltas), which keeps the loop testable without a transport.
//
// Delivery is best-effort and decoupled from durability: the loop never lets a
// slow sink backpressure the upstream provider (architecture §9.4). The
// transport adapter that satisfies this maps a delta to a wire EventFrame
// carrying the event seq for resumable Reattach.
type ClientSink interface {
	// OnTextDelta forwards an incremental chunk of assistant text for the given
	// session/turn. text is the fragment (already accumulated nowhere; the sink
	// concatenates for display).
	OnTextDelta(sessionID, turnID, text string)
	// OnThinkingDelta forwards an incremental chunk of reasoning/thinking text
	// for the given session/turn.
	OnThinkingDelta(sessionID, turnID, text string)
}

// MetricsRecorder is the consumer-defined port the loop uses to emit the RED
// error counter (broken down by typed termination subtype; FR-OBS-02) and the
// doom-loop detection counter (FR-OBS-04). It is a minimal projection of the
// platform obs.Metrics so the loop does not depend on Prometheus directly and
// stays unit-testable with a recording fake. A nil recorder is permitted (the
// loop records nothing).
type MetricsRecorder interface {
	// RecordRunError increments the error counter for the given typed
	// termination subtype (e.g. "error_max_turns", "error_max_budget_usd",
	// "error_during_execution", "refusal", "error_max_structured_output_retries").
	RecordRunError(subtype string)
	// RecordDoomLoop increments the stuck-loop detection counter for the given
	// repeating tool name (FR-OBS-04).
	RecordDoomLoop(tool string)
}

// CostFunc computes the USD cost of a turn from the model id and the normalized
// [llm.Usage] read off the provider stream. It is injected (production wiring
// passes [github.com/boltrope/boltrope/internal/platform/pricing.Cost]; tests
// pass a deterministic function) so budget enforcement is testable without
// coupling the loop to a pricing table. A nil CostFunc yields zero cost. An
// error is treated as zero cost for that turn (cost is best-effort for the
// budget cap; an unknown-model price never aborts the run).
type CostFunc func(model string, u llm.Usage) (float64, error)

// noopSink is the zero-behavior [ClientSink] used when none is injected.
type noopSink struct{}

func (noopSink) OnTextDelta(_, _, _ string)     {}
func (noopSink) OnThinkingDelta(_, _, _ string) {}

// noopMetrics is the zero-behavior [MetricsRecorder] used when none is injected.
type noopMetrics struct{}

func (noopMetrics) RecordRunError(_ string) {}
func (noopMetrics) RecordDoomLoop(_ string) {}
