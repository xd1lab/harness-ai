package agent

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// handleToolCalls runs the permission pipeline for every tool call the assistant
// requested, dispatches the allowed ones with the scheduling rules of
// architecture §9.2, feeds the results back as a single tool-role message, and
// returns turnContinue so the loop generates again.
//
// Pipeline per call (architecture §3, §8.13), evaluated SERIALLY in emitted
// order because it writes ordered events (PermissionDecided, ToolExecutionStarted)
// to the single-writer log and may block on the human ask gate:
//
//  1. PreToolUse hooks — a block yields PermissionDecided{deny,
//     reason:"hook_blocked"} with NO ApprovalRequested (FR-EXT-03 AC-1).
//  2. PolicyEngine deny→mode→allow→ask — a deny yields PermissionDecided{deny}
//     with NO ApprovalRequested (FR-PERM-01 AC-1); an ask blocks on the
//     ApprovalGate and records the human resolution (FR-PERM-04).
//  3. For an allowed call, ToolExecutionStarted is appended (durable intent,
//     log-derived idempotency key) BEFORE dispatch.
//
// Dispatch (architecture §9.2): read-only calls run CONCURRENTLY through a
// bounded errgroup; mutating calls are serialized in emitted order; external
// (egress) calls are NOT auto-parallelized.
func (l *Loop) handleToolCalls(ctx context.Context, st *runState, msg llm.Message) (turnOutcome, domain.TerminationReason, error) {
	calls := toolCallsOf(msg)
	if len(calls) == 0 {
		// Defensive: classify() should not produce needs-tool-execution without
		// calls; treat as a successful terminal turn.
		return turnTerminal, domain.Success, nil
	}

	// Doom-loop detection: a batch identical to the immediately preceding batch
	// is a stuck loop (FR-OBS-04). Detect on the consecutive-repeat count.
	l.detectDoomLoop(st, calls)

	// Resolve each tool's safety classification from the runtime's descriptors,
	// then overlay the in-loop virtual-tool classifications (ADR-0031, T11). The
	// virtual entries WIN over any same-named runtime descriptor so the scheduler
	// and gate treat spawn_subagent as mutating (serialized, never parallelized)
	// and todo_write as read-only deterministically, without a registry lookup.
	classes := mergeVirtualClasses(l.toolClasses(ctx, st.sessionID))

	// Gate every call serially, in emitted order, recording one
	// PermissionDecided per call and (for allowed calls) a ToolExecutionStarted.
	var (
		toDispatch []scheduledExec
		// results keyed by call id, assembled then fed back in emitted order.
		results = make(map[string]app.ToolResult, len(calls))
	)
	for _, c := range calls {
		cls := classes[c.Name]
		allowed, denyResult, err := l.gateCall(ctx, st, c, cls)
		if err != nil {
			return 0, "", err
		}
		if !allowed {
			// A denied/blocked call is fed back as an error observation so the
			// model can adapt; no dispatch, no ToolExecutionStarted.
			results[c.ID] = denyResult
			continue
		}
		// Durable execution intent BEFORE dispatch (ADR-0012; architecture §7.2).
		idemKey := deriveIdempotencyKey(st.sessionID, st.lastAssistantSeq)
		if err := l.append(ctx, st, domain.ActorSystem, domain.ToolExecutionStarted{
			CallID:         c.ID,
			ToolName:       c.Name,
			IdempotencyKey: idemKey,
		}); err != nil {
			return 0, "", err
		}
		// ONLY read-only, non-external tools are parallelized (architecture §9.2):
		// an external-egress tool (webfetch/websearch) is never parallelized as a
		// harmless read even if marked read-only (architecture §8.4).
		parallel := cls.SideEffect == domain.SideEffectReadOnly && cls.EgressClass != domain.EgressClassExternal
		toDispatch = append(toDispatch, scheduledExec{call: c, idemKey: idemKey, parallel: parallel})
	}

	// Dispatch with the §9.2 scheduling rules. A virtual todo_write returns its
	// PlanUpdated on a side channel (keyed by call id) so the SERIAL result loop
	// below — which holds the single-writer st — appends it in deterministic order
	// immediately before that call's ToolResult (ADR-0031, T11): execOne, which may
	// run concurrently for read-only tools, appends NOTHING itself.
	dispatched, planUpdates, err := l.dispatch(ctx, st, toDispatch)
	if err != nil {
		return 0, "", err
	}
	for id, r := range dispatched {
		results[id] = r
	}

	// Append one ToolResult event per call, in emitted order (the single-writer
	// log keeps them ordered), then feed all results back as one tool message.
	// For a todo_write call the PlanUpdated is appended FIRST (per call), yielding
	// the deterministic golden order ToolExecutionStarted -> PlanUpdated -> ToolResult.
	toolMsg := llm.Message{Role: llm.RoleTool}
	for _, c := range calls {
		r := results[c.ID]
		if pu := planUpdates[c.ID]; pu != nil {
			if err := l.append(ctx, st, domain.ActorAssistant, *pu); err != nil {
				return 0, "", err
			}
		}
		if err := l.append(ctx, st, domain.ActorTool, domain.ToolResult{
			CallID:    c.ID,
			Result:    r.Content,
			IsError:   r.IsError,
			Truncated: r.Truncated,
			BlobRef:   r.BlobRef,
		}); err != nil {
			return 0, "", err
		}
		toolMsg.Content = append(toolMsg.Content, llm.ContentPart{ToolResult: &llm.ToolResult{
			CallID:  c.ID,
			Content: r.Content,
			IsError: r.IsError,
		}})
	}
	if err := l.append(ctx, st, domain.ActorTool, domain.MessageAppended{Message: toolMsg}); err != nil {
		return 0, "", err
	}

	return turnContinue, "", nil
}

// gateCall evaluates the permission pipeline for one tool call. It returns
// allowed=true when the call may be dispatched, or allowed=false with a synthetic
// error ToolResult to feed back when it was denied/blocked. It appends exactly
// one PermissionDecided event for the decision.
func (l *Loop) gateCall(ctx context.Context, st *runState, c llm.ToolCall, cls app.ToolDescriptor) (bool, app.ToolResult, error) {
	// 1) PreToolUse hooks — a block short-circuits to a hook_blocked deny with no
	//    approval request (FR-EXT-03 AC-1).
	hookDec, err := l.deps.Hooks.Run(ctx, app.HookInput{
		Event:     app.HookPreToolUse,
		SessionID: st.sessionID,
		TurnID:    st.currentTurnID,
		CallID:    c.ID,
		ToolName:  c.Name,
		ToolArgs:  c.Args,
	})
	if err != nil {
		return false, app.ToolResult{}, fmt.Errorf("agent: PreToolUse hook: %w", err)
	}
	if !hookDec.Allow {
		if err := l.append(ctx, st, domain.ActorSystem, domain.PermissionDecided{
			CallID:   c.ID,
			ToolName: c.Name,
			Decision: domain.PermissionDeny,
			Reason:   reasonHookBlocked,
		}); err != nil {
			return false, app.ToolResult{}, err
		}
		return false, deniedResult(c, "blocked by hook: "+hookDec.Reason), nil
	}

	// 2) PolicyEngine deny→mode→allow→ask.
	pres, err := l.deps.Policy.Evaluate(ctx, policy.Input{
		SessionID:   st.sessionID,
		CallID:      c.ID,
		ToolName:    c.Name,
		ToolArgs:    c.Args,
		SideEffect:  cls.SideEffect,
		EgressClass: cls.EgressClass,
		Mode:        l.mode(),
		Tainted:     st.tainted,
	})
	if err != nil {
		return false, app.ToolResult{}, fmt.Errorf("agent: policy evaluate: %w", err)
	}

	switch pres.Decision {
	case policy.Deny:
		// Deny short-circuits with NO ApprovalRequested (FR-PERM-01 AC-1).
		if err := l.append(ctx, st, domain.ActorSystem, domain.PermissionDecided{
			CallID:   c.ID,
			ToolName: c.Name,
			Decision: domain.PermissionDeny,
			RuleID:   pres.RuleID,
			Reason:   pres.Reason,
		}); err != nil {
			return false, app.ToolResult{}, err
		}
		return false, deniedResult(c, "denied: "+pres.Reason), nil

	case policy.Allow:
		if err := l.append(ctx, st, domain.ActorSystem, domain.PermissionDecided{
			CallID:   c.ID,
			ToolName: c.Name,
			Decision: domain.PermissionAllow,
			RuleID:   pres.RuleID,
			Reason:   pres.Reason,
		}); err != nil {
			return false, app.ToolResult{}, err
		}
		return true, app.ToolResult{}, nil

	default: // policy.Ask
		// Raise the human ask gate and block until resolved or ctx is cancelled.
		resolution, rerr := l.deps.Approvals.Request(ctx, app.ApprovalRequest{
			SessionID: st.sessionID,
			CallID:    c.ID,
			ToolName:  c.Name,
			Reason:    pres.Reason,
			Args:      c.Args,
		})
		if rerr != nil {
			// A cancelled ask context (interrupt) propagates as a run error; the
			// caller's run loop will surface it. Record nothing here — the open
			// turn is adjudicated by the abort path.
			return false, app.ToolResult{}, fmt.Errorf("agent: approval request: %w", rerr)
		}
		if err := l.append(ctx, st, domain.ActorSystem, domain.PermissionDecided{
			CallID:   c.ID,
			ToolName: c.Name,
			Decision: domain.PermissionAsk,
			Resolved: resolution,
			RuleID:   pres.RuleID,
			Reason:   pres.Reason,
		}); err != nil {
			return false, app.ToolResult{}, err
		}
		if resolution == domain.AskAllowed {
			return true, app.ToolResult{}, nil
		}
		return false, deniedResult(c, "denied by human approval"), nil
	}
}

// dispatch executes the allowed tool calls under the §9.2 scheduling rules and
// returns the per-call results keyed by call id.
//
//   - Read-only (and non-external) calls dispatch CONCURRENTLY through a bounded
//     errgroup (SetLimit(min(4,GOMAXPROCS))); concurrent harmless reads are the
//     common parallelism win (architecture §9.2).
//   - Mutating and external-egress calls are SERIALIZED in emitted order: they
//     run synchronously inline in the dispatch goroutine, so at most one is in
//     flight at a time and they dispatch in exactly the order the model emitted
//     them. Concurrent edits to one workspace are a correctness/replay hazard;
//     external (webfetch/websearch) calls are not auto-parallelized as reads
//     (architecture §8.4, §9.2).
//
// All dispatches share the loop's context; on cancellation the errgroup cancels
// the derived context so in-flight read-only workers abandon their RPC (a
// cancelled tool stream maps to a real sandbox kill in the runtime; §9.3).
func (l *Loop) dispatch(ctx context.Context, st *runState, execs []scheduledExec) (map[string]app.ToolResult, map[string]*domain.PlanUpdated, error) {
	out := make(map[string]app.ToolResult, len(execs))
	plans := make(map[string]*domain.PlanUpdated, len(execs))
	var mu sync.Mutex // guards out + plans

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.readOnlyLimit())

	for _, e := range execs {
		e := e
		if e.parallel {
			// Read-only: dispatch concurrently in the bounded pool. The execOne
			// side channel (planUpdate) is collected here and appended later on the
			// SERIAL result loop — execOne itself never writes the log (ADR-0031).
			g.Go(func() error {
				res, pu, err := l.execOne(gctx, st, e)
				if err != nil {
					return err
				}
				mu.Lock()
				out[e.call.ID] = res
				if pu != nil {
					plans[e.call.ID] = pu
				}
				mu.Unlock()
				return nil
			})
			continue
		}
		// Mutating / external: run synchronously, in emitted order, so mutations
		// never overlap and never reorder. Running inline (rather than via the
		// errgroup) makes the at-most-one-mutation invariant structural.
		res, pu, err := l.execOne(gctx, st, e)
		if err != nil {
			// Abandon any in-flight read-only workers and surface the error.
			_ = g.Wait()
			return nil, nil, err
		}
		mu.Lock()
		out[e.call.ID] = res
		if pu != nil {
			plans[e.call.ID] = pu
		}
		mu.Unlock()
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return out, plans, nil
}

// execOne dispatches a single tool execution and assembles its terminal result
// from the tool stream. A transport-level stream error is returned; a tool that
// reports is_error in its terminal result is NOT an error here (it is surfaced
// to the model as an error observation; FR-TOOL-01).
//
// Virtual tools (ADR-0031, T11) are INTERCEPTED by name BEFORE the real runtime
// is consulted: neither spawn_subagent nor todo_write ever calls
// deps.Tools.ExecuteTool. For todo_write the (validated) plan is returned on a
// side channel (*domain.PlanUpdated) so the SERIAL result loop in handleToolCalls
// — which holds the single-writer st — appends it deterministically; execOne
// itself appends NOTHING to the log (it may run concurrently for read-only tools).
func (l *Loop) execOne(ctx context.Context, st *runState, e scheduledExec) (app.ToolResult, *domain.PlanUpdated, error) {
	if isVirtualTool(e.call.Name) {
		return l.execVirtual(ctx, st, e.call)
	}

	stream, err := l.deps.Tools.ExecuteTool(ctx, app.ToolExecution{
		SessionID:      st.sessionID,
		Call:           e.call,
		IdempotencyKey: e.idemKey,
	})
	if err != nil {
		return app.ToolResult{}, nil, fmt.Errorf("%w: execute %q: %w", errToolStream, e.call.Name, err)
	}
	defer func() { _ = stream.Close() }()

	var result app.ToolResult
	haveResult := false
	for {
		ev, rerr := stream.Recv()
		if rerr != nil {
			if rerr == io.EOF { //nolint:errorlint // io.EOF is a sentinel compared by identity
				break
			}
			return app.ToolResult{}, nil, fmt.Errorf("%w: recv %q: %w", errToolStream, e.call.Name, rerr)
		}
		switch {
		case ev.Result != nil:
			result = *ev.Result
			haveResult = true
		case ev.Progress != nil:
			// Progress chunks are relayed to the client elsewhere; the loop only
			// needs the terminal result for the model. (Forwarding progress is a
			// transport concern handled by the relay adapter.)
		}
	}
	if !haveResult {
		return app.ToolResult{IsError: true, Content: "tool produced no result"}, nil, nil
	}
	return result, nil, nil
}

// execVirtual handles the two in-loop virtual tools (ADR-0031, T11), dispatched
// by name. It NEVER calls deps.Tools.ExecuteTool. It returns the ToolResult to
// feed back plus, for todo_write, the *domain.PlanUpdated to append on the serial
// result loop (nil for spawn_subagent and for any rejected/invalid call):
//
//   - spawn_subagent: parse+validate `task` (empty/missing => synthetic is_error
//     ToolResult, NO Spawn), else call deps.SubAgent.Spawn with the child depth
//     l.cfg.Depth+1 and return its ToolResult. nil planUpdate.
//   - todo_write: parse+Validate `items` (invalid => is_error ToolResult, NO
//     event), else return a confirmation ToolResult AND the PlanUpdated carrying
//     st.currentTurnID for the serial loop to append.
func (l *Loop) execVirtual(ctx context.Context, st *runState, c llm.ToolCall) (app.ToolResult, *domain.PlanUpdated, error) {
	switch c.Name {
	case toolNameSpawnSubagent:
		task, model, err := parseSpawnArgs(c.Args)
		if err != nil {
			// Reject locally WITHOUT calling Spawn (open-question resolution).
			return app.ToolResult{IsError: true, Content: err.Error()}, nil, nil
		}
		if l.deps.SubAgent == nil {
			// Defensive: the tool should not be advertised without a SubAgent, but
			// never panic if the model emits it anyway.
			return app.ToolResult{IsError: true, Content: "spawn_subagent: sub-agents are not available"}, nil, nil
		}
		res, err := l.deps.SubAgent.Spawn(ctx, app.SubAgentSpawn{
			ParentSessionID: st.sessionID,
			Depth:           l.cfg.Depth + 1,
			Task:            task,
			Model:           model,
		})
		if err != nil {
			return app.ToolResult{}, nil, fmt.Errorf("agent: spawn_subagent: %w", err)
		}
		return res, nil, nil

	case toolNameTodoWrite:
		items, err := parsePlanItems(c.Args)
		if err != nil {
			// Invalid plan: feed an is_error ToolResult and append NO event.
			return app.ToolResult{IsError: true, Content: err.Error()}, nil, nil
		}
		pu := &domain.PlanUpdated{TurnID: st.currentTurnID, Items: items}
		return app.ToolResult{Content: planConfirmation(items)}, pu, nil

	default:
		// Unreachable: isVirtualTool gated entry. Fail safe rather than panic.
		return app.ToolResult{IsError: true, Content: fmt.Sprintf("unknown virtual tool %q", c.Name)}, nil, nil
	}
}

// planConfirmation builds the non-error confirmation content returned to the
// model after a todo_write, summarizing the recorded plan size.
func planConfirmation(items []domain.PlanItem) string {
	return fmt.Sprintf("recorded plan: %d item(s)", len(items))
}

// scheduledExec is one allowed tool call ready to dispatch, with its scheduling
// disposition resolved (parallel read-only vs. serialized).
type scheduledExec struct {
	call     llm.ToolCall
	idemKey  string
	parallel bool
}

// toolClasses returns a map from tool name to its descriptor (carrying
// SideEffect/EgressClass) from the runtime. On a listing error it returns an
// empty map; an unknown tool then defaults to mutating/external (fail-safe).
func (l *Loop) toolClasses(ctx context.Context, sessionID string) map[string]app.ToolDescriptor {
	descs, err := l.deps.Tools.ListTools(ctx, sessionID)
	if err != nil {
		return map[string]app.ToolDescriptor{}
	}
	byName := make(map[string]app.ToolDescriptor, len(descs))
	for _, d := range descs {
		byName[d.Name] = d
	}
	return byName
}

// mode returns the run's policy mode, defaulting the zero value to
// policy.ModeDefault.
func (l *Loop) mode() policy.Mode {
	if l.cfg.Mode == "" {
		return policy.ModeDefault
	}
	return l.cfg.Mode
}

// detectDoomLoop tracks consecutive identical tool batches and emits the
// doom-loop signal when the repeat count reaches the configured threshold
// (FR-OBS-04). It compares a stable signature of the batch (tool names + args).
func (l *Loop) detectDoomLoop(st *runState, calls []llm.ToolCall) {
	if l.cfg.DoomLoopThreshold <= 0 {
		return
	}
	sig := toolBatchSignature(calls)
	if sig == st.lastToolSig {
		st.repeatCount++
	} else {
		st.lastToolSig = sig
		st.repeatCount = 1
	}
	if st.repeatCount >= l.cfg.DoomLoopThreshold {
		// Surface for every repeating tool in the batch.
		for _, c := range calls {
			l.metrics.RecordDoomLoop(c.Name)
		}
	}
}

// toolBatchSignature builds a deterministic signature of a tool-call batch from
// the tool names and their argument shapes, so an identical repeated batch
// hashes the same regardless of map iteration order.
func toolBatchSignature(calls []llm.ToolCall) string {
	var b strings.Builder
	for _, c := range calls {
		b.WriteString(c.Name)
		b.WriteByte('(')
		keys := make([]string, 0, len(c.Args))
		for k := range c.Args {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s=%v;", k, c.Args[k])
		}
		b.WriteString(")|")
	}
	return b.String()
}

// deniedResult builds the synthetic error ToolResult fed back to the model when
// a call was denied or blocked, so the conversation stays coherent and the model
// can adapt rather than hang waiting for a result.
func deniedResult(c llm.ToolCall, msg string) app.ToolResult {
	return app.ToolResult{IsError: true, Content: fmt.Sprintf("tool %q was not executed: %s", c.Name, msg)}
}

// toolCallsOf extracts the ordered tool-call parts of an assistant message.
func toolCallsOf(m llm.Message) []llm.ToolCall {
	var calls []llm.ToolCall
	for _, p := range m.Content {
		if p.ToolCall != nil {
			calls = append(calls, *p.ToolCall)
		}
	}
	return calls
}

const (
	// reasonHookBlocked is the [domain.PermissionDecided.Reason] recorded when a
	// PreToolUse hook blocks a call (FR-EXT-03 AC-1).
	reasonHookBlocked = "hook_blocked"
)
