package agent

// Loop-level wiring / gating / parity / ordering / no-regression battery for the
// in-loop virtual tools (ADR-0031, task T12). This file is DISTINCT from
// virtual_tools_test.go (package agent_test, T9's unit-level battery) and
// virtual_tools_internal_test.go (T9's primitive unit tests) so that the two
// tasks never own the same file (a merge hazard).
//
// It is an IN-PACKAGE test (package agent) for one load-bearing reason: AC-3 and
// AC-16 must exercise the UNEXPORTED buildRequest directly — AC-16 in particular
// proves a child loop constructed with Depth==MaxDepth advertises NO
// spawn_subagent (no grandchild advertise), which is a property of buildRequest
// over a Config, not something observable only through a full Run. The remaining
// loop-level assertions (AC-2 live wiring, AC-6 permissions-not-bypassed, AC-7
// runtime never consulted, AC-8 golden ordering incl. the multi-call serial-path
// fix, AC-17 no-regression) drive a full Loop.Run over the in-repo fakes.
//
// Because this is package agent (not agent_test) it cannot reuse loop_test.go's
// newHarness; it defines its own minimal vtHarness over the SAME fakes so the two
// harnesses never collide by name.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy/policytest"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/ids/idstest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// vtHarness — a minimal in-package harness over the in-repo fakes.
// ---------------------------------------------------------------------------

type vtHarness struct {
	eventlog *apptest.FakeEventLog
	model    *apptest.FakeModelGateway
	tools    *apptest.FakeToolRuntime
	gate     *apptest.FakeApprovalGate
	hooks    *apptest.FakeHookRunner
	pol      *policytest.FakePolicyEngine
	clk      *clocktest.Fake
	ids      *idstest.Fake
}

func newVTHarness(t *testing.T) *vtHarness {
	t.Helper()
	idSeq := make([]string, 0, 64)
	for i := 1; i <= 64; i++ {
		idSeq = append(idSeq, vtID(i))
	}
	return &vtHarness{
		eventlog: apptest.NewFakeEventLog(),
		model:    apptest.NewFakeModelGateway(),
		tools:    apptest.NewFakeToolRuntime(),
		gate:     apptest.NewFakeApprovalGate(),
		hooks:    apptest.NewFakeHookRunner(),
		pol:      policytest.NewFakePolicyEngine(),
		clk:      clocktest.NewFake(time.Unix(0, 0)),
		ids:      idstest.NewFake(idSeq...),
	}
}

func vtID(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return "vid-" + string(digits[i])
	}
	return "vid-" + string(digits[i/10]) + string(digits[i%10])
}

// deps assembles the loop Deps from the harness, optionally wiring a SubAgent.
func (h *vtHarness) deps(sub app.SubAgentPort) Deps {
	return Deps{
		EventLog:  h.eventlog,
		Model:     h.model,
		Tools:     h.tools,
		Approvals: h.gate,
		Hooks:     h.hooks,
		Policy:    h.pol,
		SubAgent:  sub,
		Clock:     h.clk,
		IDs:       h.ids,
	}
}

func (h *vtHarness) loop(cfg Config, sub app.SubAgentPort) *Loop {
	return NewLoop(h.deps(sub), cfg)
}

func vtConfig() Config {
	return Config{
		Model:                      "test-model",
		MaxTurns:                   16,
		MaxBudgetUSD:               1000,
		MaxStructuredOutputRetries: 3,
		DoomLoopThreshold:          0, // disabled: this battery is not about doom loops
	}
}

func vtUserMsg(text string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}

// vtToolCallStream emits a single complete tool call then Done(tool_use).
func vtToolCallStream(callID, name string, args map[string]any) []llm.StreamEvent {
	raw, _ := json.Marshal(args)
	return []llm.StreamEvent{
		{ToolCallDelta: &llm.ToolCallDelta{CallID: callID, Name: name, ArgsFragment: raw}},
		{Done: &llm.Done{StopReason: llm.StopToolUse}},
	}
}

// vtMultiToolCallStream emits several complete tool calls in one assistant turn.
func vtMultiToolCallStream(calls ...llm.ToolCall) []llm.StreamEvent {
	var evs []llm.StreamEvent
	for _, c := range calls {
		raw, _ := json.Marshal(c.Args)
		evs = append(evs, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: c.ID, Name: c.Name, ArgsFragment: raw}})
	}
	evs = append(evs, llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}})
	return evs
}

func vtTextStream(text string) []llm.StreamEvent {
	return []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}
}

// eventTypes returns the ordered EventType sequence appended to sessionID.
func (h *vtHarness) eventTypes(sessionID string) []domain.EventType {
	evs, _ := h.eventlog.Load(context.Background(), sessionID, 0)
	out := make([]domain.EventType, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

// vtPayloadsOf returns the typed payloads of a given event type for sessionID.
func vtPayloadsOf[T domain.Event](h *vtHarness, sessionID string) []T {
	evs, _ := h.eventlog.Load(context.Background(), sessionID, 0)
	var out []T
	for _, e := range evs {
		if p, ok := e.Event.(T); ok {
			out = append(out, p)
		}
	}
	return out
}

// ===========================================================================
// AC-2 — WIRING IS LIVE. A scripted spawn_subagent call invokes
// deps.SubAgent.Spawn EXACTLY once with ParentSessionID==sessionID,
// Depth==l.cfg.Depth+1, Task/Model from the args (Model empty when omitted), and
// the Spawn-returned ToolResult content lands in the ToolResult event (NOT a
// Tools.ExecuteTool result). This proves the dead wiring is now live.
// ===========================================================================

func TestLoop_SpawnSubagentWiringIsLive(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("sa-1", toolNameSpawnSubagent, map[string]any{
		"task":  "summarize the repo",
		"model": "child-model",
	}), nil)
	h.model.AddStream(vtTextStream("done"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "child summary text"}, nil)
	h.pol.AddAllow("allow-spawn", "sub-agent allowed")

	cfg := vtConfig()
	cfg.Depth = 1 // a non-root parent: the child must run at Depth+1 == 2.
	lp := h.loop(cfg, sub)

	res, err := lp.Run(context.Background(), RunInput{SessionID: "sess-spawn", UserMessage: vtUserMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	calls := sub.Calls()
	require.Len(t, calls, 1, "deps.SubAgent.Spawn must be invoked exactly once (the dead wiring is now live)")
	in := calls[0].In
	assert.Equal(t, "sess-spawn", in.ParentSessionID, "ParentSessionID == the running session id")
	assert.Equal(t, cfg.Depth+1, in.Depth, "child Depth must be parent Depth + 1")
	assert.Equal(t, "summarize the repo", in.Task)
	assert.Equal(t, "child-model", in.Model)

	// The Spawn-returned content is what lands in the ToolResult fed back — NOT a
	// Tools.ExecuteTool result.
	trs := vtPayloadsOf[domain.ToolResult](h, "sess-spawn")
	require.Len(t, trs, 1)
	assert.Equal(t, "child summary text", trs[0].Result)
	assert.Equal(t, "sa-1", trs[0].CallID)
	assert.False(t, trs[0].IsError)

	// AC-7: the real runtime is never consulted for the virtual tool.
	assert.Empty(t, h.tools.Calls(), "Tools.ExecuteTool must not be called for spawn_subagent")
}

// AC-2 (model omitted) — Model defaults to empty when the arg is absent.
func TestLoop_SpawnSubagentModelOmittedIsEmpty(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("sa-2", toolNameSpawnSubagent, map[string]any{"task": "do a thing"}), nil)
	h.model.AddStream(vtTextStream("ok"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "ok"}, nil)
	h.pol.AddAllow("a", "")

	cfg := vtConfig()
	cfg.Depth = 0
	lp := h.loop(cfg, sub)
	_, err := lp.Run(context.Background(), RunInput{SessionID: "sess-noModel", UserMessage: vtUserMsg("go")})
	require.NoError(t, err)

	calls := sub.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "", calls[0].In.Model, "omitted model arg yields empty Model")
	assert.Equal(t, 1, calls[0].In.Depth, "root parent (Depth 0) spawns a child at Depth 1")
}

// ===========================================================================
// AC-3 — ADVERTISE GATING via the unexported buildRequest (in-package).
//   (a) SubAgent == nil           -> absent
//   (b) Depth >= MaxDepth         -> absent
//   (c) Depth <  MaxDepth, !=nil  -> present
// buildRequest is called directly so the gate is asserted as a property of the
// Config, independent of a full Run.
// ===========================================================================

func TestLoop_BuildRequestAdvertiseGates(t *testing.T) {
	seedSession := func(h *vtHarness, sessionID string) {
		_, _ = h.eventlog.Append(context.Background(), sessionID, 0, 0, "seed",
			app.AppendInput{Event: domain.SessionStarted{SystemPrompt: "sys"}})
	}

	t.Run("absent when SubAgent nil", func(t *testing.T) {
		h := newVTHarness(t)
		seedSession(h, "s-nil")
		lp := h.loop(vtConfig(), nil) // no SubAgent dep
		req, err := lp.buildRequest(context.Background(), &runState{sessionID: "s-nil"})
		require.NoError(t, err)
		assert.False(t, vtHasTool(req.Tools, toolNameSpawnSubagent),
			"spawn_subagent must be absent when deps.SubAgent == nil")
		// todo_write is independent of SubAgent and must still be present (AC-4 cross-check).
		assert.True(t, vtHasTool(req.Tools, toolNameTodoWrite),
			"todo_write is advertised even without a SubAgent")
	})

	t.Run("absent at Depth >= MaxDepth", func(t *testing.T) {
		h := newVTHarness(t)
		seedSession(h, "s-max")
		sub := apptest.NewFakeSubAgent(2) // MaxDepth == 2
		cfg := vtConfig()
		cfg.Depth = 2 // depth == MaxDepth: a child would exceed the cap
		lp := h.loop(cfg, sub)
		req, err := lp.buildRequest(context.Background(), &runState{sessionID: "s-max"})
		require.NoError(t, err)
		assert.False(t, vtHasTool(req.Tools, toolNameSpawnSubagent),
			"spawn_subagent must be absent when Depth >= MaxDepth")
	})

	t.Run("present below MaxDepth", func(t *testing.T) {
		h := newVTHarness(t)
		seedSession(h, "s-below")
		sub := apptest.NewFakeSubAgent(2)
		cfg := vtConfig()
		cfg.Depth = 0
		lp := h.loop(cfg, sub)
		req, err := lp.buildRequest(context.Background(), &runState{sessionID: "s-below"})
		require.NoError(t, err)
		def := vtToolDef(req.Tools, toolNameSpawnSubagent)
		require.NotNil(t, def, "spawn_subagent must be advertised below MaxDepth with a SubAgent dep")
		assert.NotEmpty(t, def.Description, "spawn_subagent needs a model-facing description")
	})
}

// ===========================================================================
// AC-16 — SPAWNER childCfg.Depth correctness, asserted at the loop layer
// (moved here from T8 because buildRequest is unexported). A child loop Config
// constructed DIRECTLY with Depth == MaxDepth must NOT advertise spawn_subagent:
// the grandchild advertise is suppressed. This asserts against the CHILD's
// Depth, not the parent's gate — exactly what the spawner's childCfg.Depth =
// in.Depth wiring must produce.
// ===========================================================================

func TestLoop_ChildAtMaxDepthDoesNotAdvertiseSpawn(t *testing.T) {
	h := newVTHarness(t)
	_, _ = h.eventlog.Append(context.Background(), "s-child", 0, 0, "seed",
		app.AppendInput{Event: domain.SessionStarted{SystemPrompt: "sys"}})

	sub := apptest.NewFakeSubAgent(2) // MaxDepth == 2

	// The CHILD loop runs at Depth == MaxDepth (what subagent.Spawn sets when a
	// parent at Depth MaxDepth-1 spawns it). At that depth the child must NOT
	// offer a grandchild spawn.
	childCfg := vtConfig()
	childCfg.Depth = sub.MaxDepth() // 2
	childLoop := h.loop(childCfg, sub)

	req, err := childLoop.buildRequest(context.Background(), &runState{sessionID: "s-child"})
	require.NoError(t, err)
	assert.False(t, vtHasTool(req.Tools, toolNameSpawnSubagent),
		"a child loop at Depth==MaxDepth must NOT advertise spawn_subagent (no grandchild)")
	// todo_write is depth-independent and stays available to the child.
	assert.True(t, vtHasTool(req.Tools, toolNameTodoWrite),
		"todo_write remains available to a child at MaxDepth")
}

// ===========================================================================
// AC-6 — PERMISSIONS NOT BYPASSED. A policy-denied spawn_subagent records a
// PermissionDecided{deny}, does NOT call Spawn (FakeSubAgent.Calls() empty), and
// feeds the synthetic deniedResult back. A hook-blocked spawn_subagent likewise
// never calls Spawn. An ask-resolved-allowed spawn_subagent DOES call Spawn.
// ===========================================================================

func TestLoop_SpawnSubagentPolicyDeniedDoesNotCallSpawn(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("sa-deny", toolNameSpawnSubagent, map[string]any{"task": "x"}), nil)
	h.model.AddStream(vtTextStream("understood"), nil)

	sub := apptest.NewFakeSubAgent(2)
	// No AddResult: a wrongful Spawn would panic on an exhausted queue.
	h.pol.AddDeny("deny-spawn", "sub-agents are denied here")

	cfg := vtConfig()
	cfg.Depth = 0
	lp := h.loop(cfg, sub)
	res, err := lp.Run(context.Background(), RunInput{SessionID: "sess-deny", UserMessage: vtUserMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, sub.Calls(), "a policy-denied spawn_subagent must NOT call Spawn")

	decs := vtPayloadsOf[domain.PermissionDecided](h, "sess-deny")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionDeny, decs[0].Decision)
	assert.Equal(t, toolNameSpawnSubagent, decs[0].ToolName)

	// No ToolExecutionStarted for a denied virtual tool.
	assert.Empty(t, vtPayloadsOf[domain.ToolExecutionStarted](h, "sess-deny"))

	// The denied result is fed back as an is_error ToolResult.
	trs := vtPayloadsOf[domain.ToolResult](h, "sess-deny")
	require.Len(t, trs, 1)
	assert.True(t, trs[0].IsError, "a denied spawn feeds the synthetic deniedResult")
}

func TestLoop_SpawnSubagentHookBlockedDoesNotCallSpawn(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("sa-hook", toolNameSpawnSubagent, map[string]any{"task": "x"}), nil)
	h.model.AddStream(vtTextStream("ok"), nil)

	sub := apptest.NewFakeSubAgent(2)
	// PreToolUse hook blocks; policy would otherwise allow.
	h.hooks.AddDecision(false, "blocked by hook")
	h.pol.AddAllow("a", "")

	cfg := vtConfig()
	cfg.Depth = 0
	lp := h.loop(cfg, sub)
	res, err := lp.Run(context.Background(), RunInput{SessionID: "sess-hook", UserMessage: vtUserMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, sub.Calls(), "a hook-blocked spawn_subagent must NOT call Spawn")

	decs := vtPayloadsOf[domain.PermissionDecided](h, "sess-hook")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionDeny, decs[0].Decision)
	assert.Equal(t, reasonHookBlocked, decs[0].Reason)
}

func TestLoop_SpawnSubagentAskAllowedCallsSpawn(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("sa-ask", toolNameSpawnSubagent, map[string]any{"task": "x"}), nil)
	h.model.AddStream(vtTextStream("done"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "child done"}, nil)
	h.pol.AddAsk("ask-spawn", "approve sub-agent")

	go func() {
		_ = h.gate.Resolve(context.Background(), "sess-ask", "sa-ask", domain.AskAllowed)
	}()

	cfg := vtConfig()
	cfg.Depth = 0
	lp := h.loop(cfg, sub)
	res, err := lp.Run(context.Background(), RunInput{SessionID: "sess-ask", UserMessage: vtUserMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	require.Len(t, sub.Calls(), 1, "an ask-resolved-allowed spawn_subagent must call Spawn")

	decs := vtPayloadsOf[domain.PermissionDecided](h, "sess-ask")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionAsk, decs[0].Decision)
	assert.Equal(t, domain.AskAllowed, decs[0].Resolved)
}

// ===========================================================================
// AC-7 — Tools.ExecuteTool is NEVER invoked for either virtual tool name.
// (spawn_subagent is covered above; this isolates todo_write.)
// ===========================================================================

func TestLoop_VirtualToolsNeverCallExecuteTool(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("td-1", toolNameTodoWrite, map[string]any{
		"items": []any{map[string]any{"content": "step", "status": "pending"}},
	}), nil)
	h.model.AddStream(vtTextStream("ok"), nil)
	h.pol.AddAllow("a", "")

	res, err := h.loop(vtConfig(), nil).Run(context.Background(),
		RunInput{SessionID: "sess-td", UserMessage: vtUserMsg("plan")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, h.tools.Calls(), "Tools.ExecuteTool must never be called for todo_write")
}

// ===========================================================================
// AC-8 — GOLDEN SEQUENCE. For a single todo_write call the per-call order is
// ToolExecutionStarted -> PlanUpdated -> ToolResult.
// ===========================================================================

func TestLoop_TodoWriteGoldenSequence(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtToolCallStream("td-g", toolNameTodoWrite, map[string]any{
		"items": []any{
			map[string]any{"content": "explore", "status": "completed"},
			map[string]any{"content": "implement", "status": "in_progress"},
			map[string]any{"content": "test", "status": "pending"},
		},
	}), nil)
	h.model.AddStream(vtTextStream("plan recorded"), nil)
	h.pol.AddAllow("allow-todo", "planning is read-only")

	res, err := h.loop(vtConfig(), nil).Run(context.Background(),
		RunInput{SessionID: "sess-golden", UserMessage: vtUserMsg("make a plan")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// Exactly one PlanUpdated with the items intact.
	plans := vtPayloadsOf[domain.PlanUpdated](h, "sess-golden")
	require.Len(t, plans, 1)
	require.Len(t, plans[0].Items, 3)
	assert.Equal(t, "explore", plans[0].Items[0].Content)
	assert.Equal(t, "in_progress", plans[0].Items[1].Status)

	seq := h.eventTypes("sess-golden")
	tesIdx := vtIndexOf(seq, domain.EventToolExecutionStarted)
	planIdx := vtIndexOf(seq, domain.EventPlanUpdated)
	trIdx := vtIndexOf(seq, domain.EventToolResult)
	require.GreaterOrEqual(t, tesIdx, 0, "ToolExecutionStarted appended for a virtual tool")
	require.GreaterOrEqual(t, planIdx, 0, "PlanUpdated appended for todo_write")
	require.GreaterOrEqual(t, trIdx, 0, "ToolResult appended for a virtual tool")
	assert.Less(t, tesIdx, planIdx, "ToolExecutionStarted precedes PlanUpdated")
	assert.Less(t, planIdx, trIdx, "PlanUpdated precedes ToolResult")
}

// AC-8 (multi-call) — a turn mixing todo_write with another tool. The PlanUpdated
// must land deterministically adjacent to its OWN ToolResult (proving the T11
// serial-result-path fix: execOne appends nothing; the serial loop appends the
// PlanUpdated immediately before that call's ToolResult, regardless of the
// concurrent read-only dispatch of the sibling tool).
func TestLoop_TodoWriteMultiCallPlanAdjacentToOwnResult(t *testing.T) {
	h := newVTHarness(t)
	// One read-only real tool ("read") emitted BEFORE the todo_write, in one turn.
	h.tools.SetTools([]app.ToolDescriptor{
		{Name: "read", SideEffect: domain.SideEffectReadOnly, EgressClass: domain.EgressClassNone},
	})
	h.tools.AddSuccessfulExecution("file body")

	h.model.AddStream(vtMultiToolCallStream(
		llm.ToolCall{ID: "r1", Name: "read", Args: map[string]any{"path": "/x"}},
		llm.ToolCall{ID: "td2", Name: toolNameTodoWrite, Args: map[string]any{
			"items": []any{map[string]any{"content": "do it", "status": "in_progress"}},
		}},
	), nil)
	h.model.AddStream(vtTextStream("done"), nil)
	h.pol.AddAllow("a", "") // read
	h.pol.AddAllow("b", "") // todo_write

	res, err := h.loop(vtConfig(), nil).Run(context.Background(),
		RunInput{SessionID: "sess-multi", UserMessage: vtUserMsg("read then plan")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// The real "read" tool WAS dispatched; the virtual one was NOT.
	require.Len(t, h.tools.Calls(), 1, "the real read tool is dispatched exactly once")
	assert.Equal(t, "r1", h.tools.Calls()[0].Exec.Call.ID)

	// Exactly one PlanUpdated for the td2 call.
	plans := vtPayloadsOf[domain.PlanUpdated](h, "sess-multi")
	require.Len(t, plans, 1)

	// Adjacency: the PlanUpdated must sit immediately before td2's ToolResult, and
	// the read result must NOT be interleaved between them. We assert on the full
	// envelope stream so the PlanUpdated's position relative to BOTH ToolResults is
	// pinned.
	evs, _ := h.eventlog.Load(context.Background(), "sess-multi", 0)
	planIdx, td2ResultIdx := -1, -1
	for i, e := range evs {
		switch p := e.Event.(type) {
		case domain.PlanUpdated:
			planIdx = i
		case domain.ToolResult:
			if p.CallID == "td2" {
				td2ResultIdx = i
			}
		}
	}
	require.GreaterOrEqual(t, planIdx, 0, "a PlanUpdated must be appended")
	require.GreaterOrEqual(t, td2ResultIdx, 0, "td2's ToolResult must be appended")
	assert.Equal(t, td2ResultIdx-1, planIdx,
		"PlanUpdated must be the event immediately preceding its own (td2) ToolResult")
}

// ===========================================================================
// AC-17 — NO REGRESSION. With SubAgent==nil and NO virtual-tool calls emitted,
// no spawn_subagent/todo_write events leak into the log and the text-only event
// sequence is byte-identical to the pre-feature golden. Documents the AC-4/AC-17
// tension: todo_write is legitimately ALWAYS advertised, but it is absent from
// the event LOG when the model emits no call — so the assertion is at the
// event-sequence level, not the advertised-tools level.
// ===========================================================================

func TestLoop_NoVirtualToolEventsWhenNoneEmitted(t *testing.T) {
	h := newVTHarness(t)
	h.model.AddStream(vtTextStream("hi"), nil)

	res, err := h.loop(vtConfig(), nil).Run(context.Background(),
		RunInput{SessionID: "sess-noleak", UserMessage: vtUserMsg("hello")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	want := []domain.EventType{
		domain.EventMessageAppended,
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventTurnFinished,
	}
	assert.Equal(t, want, h.eventTypes("sess-noleak"),
		"a text-only turn must not leak any virtual-tool events")

	// And concretely: zero PlanUpdated despite todo_write being advertised.
	assert.Empty(t, vtPayloadsOf[domain.PlanUpdated](h, "sess-noleak"))
	assert.Empty(t, vtPayloadsOf[domain.ToolExecutionStarted](h, "sess-noleak"))
}

// ---------------------------------------------------------------------------
// Small local helpers.
// ---------------------------------------------------------------------------

func vtToolDef(defs []llm.ToolDef, name string) *llm.ToolDef {
	for i := range defs {
		if defs[i].Name == name {
			return &defs[i]
		}
	}
	return nil
}

func vtHasTool(defs []llm.ToolDef, name string) bool {
	return vtToolDef(defs, name) != nil
}

func vtIndexOf(seq []domain.EventType, want domain.EventType) int {
	for i, e := range seq {
		if e == want {
			return i
		}
	}
	return -1
}
