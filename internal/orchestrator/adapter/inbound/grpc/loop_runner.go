package grpc

import (
	"context"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// Compile-time assertion: *LoopRunner satisfies Runner.
var _ Runner = (*LoopRunner)(nil)

// LoopRunner is the production [Runner]: it constructs an [agent.Loop] per run
// from an injected dependency set and config template, wires the run's
// [ClientSink], and drives [agent.Loop.Run]. It is the only place this package
// depends on the agent loop, keeping the loop free of any transport concern
// (architecture §4.3). Tests inject a fake [Runner] instead, so the server is
// provable without a real loop.
type LoopRunner struct {
	// deps is the base loop dependency set (event log, model, tools, approvals,
	// hooks, policy, clock, ids, metrics, cost). The run's Sink is overlaid
	// per-run; the rest is shared.
	deps agent.Deps
	// cfg is the base loop config template (model, caps, tool defs). The run's
	// Mode is overlaid per-run.
	cfg agent.Config
}

// NewLoopRunner returns a [LoopRunner] over the given base loop dependencies and
// config template. The Sink field of deps is ignored (each run supplies its own
// via [RunSpec]); the Mode field of cfg is ignored (each run supplies its own).
func NewLoopRunner(deps agent.Deps, cfg agent.Config) *LoopRunner {
	return &LoopRunner{deps: deps, cfg: cfg}
}

// Run constructs a per-run [agent.Loop] with spec.Sink and spec.Mode overlaid on
// the template, runs it, and returns the terminal [RunOutcome] including the
// final assistant text folded from the session log.
func (lr *LoopRunner) Run(ctx context.Context, spec RunSpec) (RunOutcome, error) {
	deps := lr.deps
	deps.Sink = adaptSink(spec.Sink)

	cfg := lr.cfg
	cfg.Mode = spec.Mode

	loop := agent.NewLoop(deps, cfg)
	res, err := loop.Run(ctx, agent.RunInput{
		SessionID:   spec.SessionID,
		UserMessage: spec.UserMessage,
		Tainted:     spec.Tainted,
	})
	if err != nil {
		return RunOutcome{}, err
	}
	return RunOutcome{
		Reason:    res.Reason,
		FinalText: lastAssistantText(ctx, lr.deps.EventLog, spec.SessionID),
		Usage:     res.Usage,
		CostUSD:   res.CostUSD,
		NumTurns:  int64(res.NumTurns),
	}, nil
}

// adaptSink adapts a [ClientSink] to the loop's [agent.ClientSink] (identical
// shape; the indirection keeps the loop import isolated to this file). A nil sink
// yields nil so the loop substitutes its own no-op.
func adaptSink(s ClientSink) agent.ClientSink {
	if s == nil {
		return nil
	}
	return sinkAdapter{s}
}

// sinkAdapter forwards loop sink calls to a transport [ClientSink].
type sinkAdapter struct{ s ClientSink }

func (a sinkAdapter) OnTextDelta(sessionID, turnID, text string) {
	a.s.OnTextDelta(sessionID, turnID, text)
}

func (a sinkAdapter) OnThinkingDelta(sessionID, turnID, text string) {
	a.s.OnThinkingDelta(sessionID, turnID, text)
}

// lastAssistantText loads the session and returns the concatenated text of the
// last assistant message, used to populate RunOutcome.FinalText. On any load
// error it returns the empty string (the terminal subtype on RunOutcome.Reason
// is the authoritative outcome; final text is a convenience).
func lastAssistantText(ctx context.Context, log app.EventLogPort, sessionID string) string {
	events, err := log.Load(ctx, sessionID, 0)
	if err != nil {
		return ""
	}
	var text string
	for _, env := range events {
		if am, ok := env.Event.(domain.AssistantMessage); ok {
			if t := assistantText(am.Message); t != "" {
				text = t
			}
		}
	}
	return text
}
