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
	"strings"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
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

	// Build the child loop config from the per-spawn inputs (see childConfig).
	childCfg := s.childConfig(in)

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
	return s.condense(ctx, childSessionID, runRes), nil
}

// childConfig derives the per-spawn child [agent.Config] from the shared base
// config and the spawn inputs. It works on a COPY of s.loopCfg and never mutates
// the shared base — s.loopCfg is reused across every spawn, so mutating it would
// race and leak depth/model between concurrent children.
//
// Two fields are derived per spawn:
//
//   - Depth is set to in.Depth — the depth the child runs at. Without this the
//     child would inherit the base config's Depth (0 for a root spawner) and
//     would therefore advertise spawn_subagent forever and pass Depth 1 to any
//     grandchild, defeating the [Config.MaxDepth] cap. The child's own
//     spawn_subagent advertising gate and its grandchild depth computation both
//     read [agent.Config.Depth] (FR-EXT-04 AC-16).
//   - Model is overridden only when the caller supplied a non-empty in.Model;
//     otherwise the child keeps the parent's model.
func (s *Spawner) childConfig(in app.SubAgentSpawn) agent.Config {
	childCfg := s.loopCfg
	childCfg.Depth = in.Depth
	if in.Model != "" {
		childCfg.Model = in.Model
	}
	return childCfg
}

// taskMessage builds a minimal [llm.Message] carrying the sub-agent task text
// as the user turn that the child loop appends before its first Generate.
func taskMessage(task string) llm.Message {
	return llm.Message{
		Role:    llm.RoleUser,
		Content: []llm.ContentPart{{Text: &llm.TextPart{Text: task}}},
	}
}

// maxFinalTextRunes caps the child final-answer text included in the parent's
// observation. The observation feeds straight back into the parent's model
// window, so an unboundedly verbose child could blow the parent's context
// budget; 4096 runes keeps a substantial answer intact while bounding growth.
// Runes, not bytes, so the cut never splits a multi-byte UTF-8 character.
const maxFinalTextRunes = 4096

// truncationMarker is appended whenever the final text was cut at
// maxFinalTextRunes, so the parent model knows the answer is incomplete rather
// than silently mistaking the prefix for the whole answer.
const truncationMarker = "... [truncated]"

// condense converts a child [agent.RunResult] into the [app.ToolResult] the
// parent receives as the observation for this sub-agent tool call. The content
// is a one-line termination summary followed, for non-error completions, by the
// child's final assistant text folded from its session log (capped at
// maxFinalTextRunes). Loading the log is best-effort: on any load error the
// reason-only summary is returned, because the child's work is already durably
// recorded and the summary must never fail the parent turn.
func (s *Spawner) condense(ctx context.Context, childSessionID string, r agent.RunResult) app.ToolResult {
	if r.Reason.IsError() {
		return app.ToolResult{
			IsError: true,
			Content: fmt.Sprintf("sub-agent terminated with error: %s", r.Reason),
		}
	}
	summary := fmt.Sprintf("sub-agent completed: %s (turns=%d)", r.Reason, r.NumTurns)
	if text := s.lastAssistantText(ctx, childSessionID); text != "" {
		summary += "\n" + truncateRunes(text, maxFinalTextRunes)
	}
	return app.ToolResult{Content: summary}
}

// lastAssistantText folds the child session log down to the concatenated text
// parts of the LAST [domain.AssistantMessage] — the child's final answer. This
// mirrors the transport's fold (grpc.LoopRunner) but is reimplemented locally:
// the app layer must not import inbound adapters (depguard; architecture §5.1).
// On any load error it returns "" so the caller falls back to the reason-only
// summary.
func (s *Spawner) lastAssistantText(ctx context.Context, sessionID string) string {
	events, err := s.deps.EventLog.Load(ctx, sessionID, 0)
	if err != nil {
		return ""
	}
	var text string
	for _, env := range events {
		am, ok := env.Event.(domain.AssistantMessage)
		if !ok {
			continue
		}
		var b strings.Builder
		for _, cp := range am.Message.Content {
			if cp.Text != nil {
				b.WriteString(cp.Text.Text)
			}
		}
		if t := b.String(); t != "" {
			text = t
		}
	}
	return text
}

// truncateRunes caps s at max runes, appending truncationMarker when cut.
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + truncationMarker
}
