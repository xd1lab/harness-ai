// Package subagent implements the [app.SubAgentPort] contract: depth-limited
// child agent loops exposed to the model as an ordinary tool (FR-EXT-04;
// architecture §5.1, §9.5).
//
// A [Spawner] wraps the same [agent.Loop] machinery as the parent, running it
// against a fresh child session derived from the parent via
// [app.EventLogPort.Fork]. Recursion is bounded by [Config.MaxDepth]: a
// [app.SubAgentSpawn] whose Depth exceeds that cap is rejected without touching
// the event log (FR-EXT-04 AC-2).
//
// The child loop uses the same injected fakes in tests — no real provider,
// sandbox, or database (NFR-TEST-01/02).
package subagent

import (
	"context"
	"fmt"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Compile-time assertion that Spawner satisfies app.SubAgentPort.
var _ app.SubAgentPort = (*Spawner)(nil)

// Config parameterizes a [Spawner]. All fields are required.
type Config struct {
	// MaxDepth is the maximum sub-agent recursion depth. A [app.SubAgentSpawn]
	// whose Depth exceeds this value is rejected without creating a child
	// session (FR-EXT-04 AC-2).
	MaxDepth int
	// Deps are the injected ports forwarded verbatim to each child [agent.Loop].
	// The EventLog is used both to Fork the child session and as the child
	// loop's append target.
	Deps agent.Deps
	// LoopCfg is the [agent.Config] passed to each child loop. The child task
	// message is supplied at Spawn time as the child loop's user message.
	LoopCfg agent.Config
}

// Spawner is the [app.SubAgentPort] implementation. Construct one with [New].
type Spawner struct {
	maxDepth int
	deps     agent.Deps
	loopCfg  agent.Config
}

// New returns a [Spawner] from the given [Config].
func New(cfg Config) *Spawner {
	return &Spawner{
		maxDepth: cfg.MaxDepth,
		deps:     cfg.Deps,
		loopCfg:  cfg.LoopCfg,
	}
}

// MaxDepth returns the configured maximum sub-agent recursion depth.
func (s *Spawner) MaxDepth() int { return s.maxDepth }

// Spawn runs a child agent loop for in.Task at in.Depth and returns its
// condensed result as an [app.ToolResult].
//
// If in.Depth > MaxDepth it returns a [app.ToolResult] with IsError=true and
// content "max sub-agent depth exceeded" and does NOT spawn a session
// (FR-EXT-04 AC-2). Cancelling ctx cancels the child loop.
func (s *Spawner) Spawn(ctx context.Context, in app.SubAgentSpawn) (app.ToolResult, error) {
	// FR-EXT-04 AC-2: depth cap is enforced before ANY session creation.
	if in.Depth > s.maxDepth {
		return app.ToolResult{
			IsError: true,
			Content: "max sub-agent depth exceeded",
		}, nil
	}

	// Derive a fresh child session id from the injected IDGenerator.
	childSessionID := s.deps.IDs.NewSessionID().String()

	// Create the child session by forking the parent at its current head seq.
	// Using Fork ensures the child inherits the parent's event history prefix
	// (architecture §6.6) and keeps the event-store's tenant ownership intact
	// (ADR-0013). Fork at head seq = 0 is valid for a fresh parent.
	parentSess, err := s.deps.EventLog.LoadSession(ctx, in.ParentSessionID)
	if err != nil {
		return app.ToolResult{}, fmt.Errorf("subagent: load parent session: %w", err)
	}

	_, err = s.deps.EventLog.Fork(ctx, in.ParentSessionID, parentSess.HeadSeq, childSessionID)
	if err != nil {
		return app.ToolResult{}, fmt.Errorf("subagent: fork child session: %w", err)
	}

	// Build the child loop config. If the caller supplied a model override
	// carry it forward; otherwise keep the parent's model.
	childCfg := s.loopCfg
	if in.Model != "" {
		childCfg.Model = in.Model
	}

	// Construct and run the child loop. The same injected fakes are forwarded
	// so tests are fully deterministic (NFR-TEST-01/02).
	loop := agent.NewLoop(s.deps, childCfg)
	runRes, err := loop.Run(ctx, agent.RunInput{
		SessionID:   childSessionID,
		UserMessage: taskMessage(in.Task),
	})
	if err != nil {
		return app.ToolResult{}, fmt.Errorf("subagent: child loop: %w", err)
	}

	// Condense the child run result into a ToolResult for the parent.
	return condense(runRes), nil
}

// taskMessage builds a minimal [llm.Message] carrying the sub-agent task text
// as the user turn that the child loop appends before its first Generate.
func taskMessage(task string) llm.Message {
	return llm.Message{
		Role:    llm.RoleUser,
		Content: []llm.ContentPart{{Text: &llm.TextPart{Text: task}}},
	}
}

// condense converts a child [agent.RunResult] into the [app.ToolResult] the
// parent receives as the observation for this sub-agent tool call. The content
// is a brief summary of the termination reason; richer output (e.g. the last
// assistant message text) would require loading the child session log, which is
// deferred to a richer implementation once the transport layer is wired
// (architecture §5.1). For correctness and determinism in unit tests this
// minimal form is sufficient.
func condense(r agent.RunResult) app.ToolResult {
	if r.Reason.IsError() {
		return app.ToolResult{
			IsError: true,
			Content: fmt.Sprintf("sub-agent terminated with error: %s", r.Reason),
		}
	}
	return app.ToolResult{
		Content: fmt.Sprintf("sub-agent completed: %s (turns=%d)", r.Reason, r.NumTurns),
	}
}
