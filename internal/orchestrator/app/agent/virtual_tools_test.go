package agent_test

// RED (test-first) battery for capability-scorecard Gap#3 CORE: the in-loop
// virtual-tool seam (spawn_subagent + todo_write) and the durable PlanUpdated
// planning event. These tests are authored BEFORE the implementation and are
// EXPECTED to fail to compile / fail at run time until the feature lands:
//
//   - agent.Config has no Depth field yet (AC-1).
//   - buildRequest does not advertise spawn_subagent / todo_write yet (AC-3/AC-4).
//   - the loop never calls deps.SubAgent.Spawn — the wiring is DEAD (AC-2/AC-7).
//   - todo_write does not append a domain.PlanUpdated event yet (AC-7/AC-9).
//
// They drive the SAME newHarness fakes as loop_test.go (this is package
// agent_test), plus apptest.FakeSubAgent set on Deps.SubAgent.
//
// Naming/seam (from the task DESIGN): virtual tools are handled INSIDE the loop,
// classified inline (spawn_subagent = mutating/none, todo_write = read-only/none),
// gated through the SAME permission pipeline, and append the SAME
// ToolExecutionStarted + ToolResult events as real tools for replay parity.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Helpers — build a loop wired with a FakeSubAgent at a chosen Depth, and pull
// the advertised tool defs out of the recorded model request.
// ---------------------------------------------------------------------------

// loopWithSubAgent constructs a Loop from the harness with a SubAgent dep and a
// chosen config (notably cfg.Depth). It returns the loop and the fake sub-agent.
func (h *harness) loopWithSubAgent(cfg agent.Config, sub app.SubAgentPort) *agent.Loop {
	return agent.NewLoop(agent.Deps{
		EventLog:  h.eventlog,
		Model:     h.model,
		Tools:     h.tools,
		Approvals: h.gate,
		Hooks:     h.hooks,
		Policy:    h.pol,
		SubAgent:  sub,
		Clock:     h.clk,
		IDs:       h.ids,
		Sink:      h.sink,
		Metrics:   h.metrics,
	}, cfg)
}

// lastRequestTools returns the Tools advertised in the FIRST recorded Stream
// request (the model-visible tool defs the loop built in buildRequest).
func (h *harness) firstRequestTools(t *testing.T) []llm.ToolDef {
	t.Helper()
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			return c.Req.Tools
		}
	}
	t.Fatalf("no Stream request was recorded")
	return nil
}

// toolDefByName finds a ToolDef by name, or nil.
func toolDefByName(defs []llm.ToolDef, name string) *llm.ToolDef {
	for i := range defs {
		if defs[i].Name == name {
			return &defs[i]
		}
	}
	return nil
}

// hasToolDef reports whether a tool def of the given name is advertised.
func hasToolDef(defs []llm.ToolDef, name string) bool {
	return toolDefByName(defs, name) != nil
}

const (
	toolSpawnSubagent = "spawn_subagent"
	toolTodoWrite     = "todo_write"
)

// ===========================================================================
// AC-2 — WIRING IS LIVE: a spawn_subagent tool call invokes deps.SubAgent.Spawn
// with Depth+1, ParentSessionID, Task, Model — and the Spawn ToolResult lands in
// the ToolResult event fed back to the model (NOT a Tools.ExecuteTool result).
// ===========================================================================

func TestSpawnSubagent_InvokesSpawnWithDepthPlusOne(t *testing.T) {
	h := newHarness(t)
	// Turn 1: the model emits a spawn_subagent tool call.
	h.model.AddStream(toolCallStream("sa-1", toolSpawnSubagent, map[string]any{
		"task":  "summarize the repo",
		"model": "child-model",
	}), nil)
	// Turn 2: text-only finish.
	h.model.AddStream(textStream("done"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "child summary text"}, nil)
	// Policy allows the (mutating) spawn call.
	h.pol.AddAllow("allow-spawn", "sub-agent allowed")

	cfg := defaultConfig()
	cfg.Depth = 0 // root
	lp := h.loopWithSubAgent(cfg, sub)

	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-spawn", UserMessage: userMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// Spawn was invoked EXACTLY once — the dead wiring is now live.
	calls := sub.Calls()
	require.Len(t, calls, 1, "deps.SubAgent.Spawn must be invoked exactly once for a spawn_subagent tool call")
	in := calls[0].In
	assert.Equal(t, "sess-spawn", in.ParentSessionID, "ParentSessionID is the running session")
	assert.Equal(t, 1, in.Depth, "child Depth must be parent Depth (0) + 1")
	assert.Equal(t, "summarize the repo", in.Task)
	assert.Equal(t, "child-model", in.Model)

	// The Spawn-returned content is what lands in the ToolResult fed back.
	trs := payloadsOf[domain.ToolResult](h, "sess-spawn")
	require.Len(t, trs, 1)
	assert.Equal(t, "child summary text", trs[0].Result, "Spawn result, not a Tools.ExecuteTool result, is fed back")
	assert.Equal(t, "sa-1", trs[0].CallID)

	// The real tool runtime was NEVER consulted for the virtual tool (AC-7).
	assert.Empty(t, h.tools.Calls(), "Tools.ExecuteTool must not be called for a virtual tool")
}

// AC-2 (model omitted) — Model defaults to empty string when omitted.
func TestSpawnSubagent_ModelOmittedIsEmpty(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("sa-2", toolSpawnSubagent, map[string]any{"task": "do a thing"}), nil)
	h.model.AddStream(textStream("ok"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "ok"}, nil)
	h.pol.AddAllow("a", "")

	cfg := defaultConfig()
	cfg.Depth = 0
	lp := h.loopWithSubAgent(cfg, sub)
	_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-noModel", UserMessage: userMsg("go")})
	require.NoError(t, err)

	calls := sub.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "", calls[0].In.Model, "omitted model arg yields empty Model")
	assert.Equal(t, "do a thing", calls[0].In.Task)
}

// ===========================================================================
// AC-3 — ADVERTISING GATES for spawn_subagent.
// ===========================================================================

// (a) SubAgent == nil -> spawn_subagent is NOT advertised.
func TestSpawnSubagent_NotAdvertisedWhenSubAgentNil(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hi"), nil)

	// Default harness loop has no SubAgent dep.
	_, err := h.run(t, defaultConfig(), "sess-nilsa", "hello")
	require.NoError(t, err)

	tools := h.firstRequestTools(t)
	assert.False(t, hasToolDef(tools, toolSpawnSubagent),
		"spawn_subagent must be absent when deps.SubAgent == nil")
}

// (b) Depth >= MaxDepth -> spawn_subagent is NOT advertised.
func TestSpawnSubagent_NotAdvertisedAtMaxDepth(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hi"), nil)

	sub := apptest.NewFakeSubAgent(2) // MaxDepth == 2
	cfg := defaultConfig()
	cfg.Depth = 2 // depth >= MaxDepth: a child would exceed the cap
	lp := h.loopWithSubAgent(cfg, sub)
	_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-maxdepth", UserMessage: userMsg("hi")})
	require.NoError(t, err)

	tools := h.firstRequestTools(t)
	assert.False(t, hasToolDef(tools, toolSpawnSubagent),
		"spawn_subagent must be absent when Depth >= MaxDepth")
}

// (c) Depth < MaxDepth and SubAgent != nil -> spawn_subagent advertised with a
// JSON schema requiring task (string) and optional model (string).
func TestSpawnSubagent_AdvertisedBelowMaxDepth(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hi"), nil)

	sub := apptest.NewFakeSubAgent(2)
	cfg := defaultConfig()
	cfg.Depth = 0
	lp := h.loopWithSubAgent(cfg, sub)
	_, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-adv", UserMessage: userMsg("hi")})
	require.NoError(t, err)

	tools := h.firstRequestTools(t)
	def := toolDefByName(tools, toolSpawnSubagent)
	require.NotNil(t, def, "spawn_subagent must be advertised below MaxDepth with a SubAgent dep")
	assert.NotEmpty(t, def.Description, "spawn_subagent needs a model-facing description")

	schema := decodeSchema(t, def.JSONSchema)
	props, _ := schema["properties"].(map[string]any)
	require.NotNil(t, props, "schema has properties")
	assert.Contains(t, props, "task", "schema requires a task property")
	assert.Contains(t, props, "model", "schema declares an optional model property")
	requiredSet := requiredFields(schema)
	assert.Contains(t, requiredSet, "task", "task is required")
	assert.NotContains(t, requiredSet, "model", "model is optional")
}

// ===========================================================================
// AC-4 — todo_write is ALWAYS advertised with the items schema.
// ===========================================================================

func TestTodoWrite_AlwaysAdvertisedWithSchema(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hi"), nil)

	// No SubAgent dep at all: todo_write must still be advertised.
	_, err := h.run(t, defaultConfig(), "sess-todoadv", "hello")
	require.NoError(t, err)

	tools := h.firstRequestTools(t)
	def := toolDefByName(tools, toolTodoWrite)
	require.NotNil(t, def, "todo_write must always be advertised")
	assert.NotEmpty(t, def.Description, "todo_write needs a model-facing description")

	schema := decodeSchema(t, def.JSONSchema)
	props, _ := schema["properties"].(map[string]any)
	require.NotNil(t, props)
	require.Contains(t, props, "items", "schema requires an items property")
	assert.Contains(t, requiredFields(schema), "items", "items is required")

	// items is an array of objects with content (string) + status (enum).
	items, _ := props["items"].(map[string]any)
	require.NotNil(t, items)
	assert.Equal(t, "array", items["type"])
	itemSchema, _ := items["items"].(map[string]any)
	require.NotNil(t, itemSchema, "items.items describes the element shape")
	itemProps, _ := itemSchema["properties"].(map[string]any)
	require.NotNil(t, itemProps)
	assert.Contains(t, itemProps, "content")
	assert.Contains(t, itemProps, "status")
}

// ===========================================================================
// AC-17 — NO REGRESSION: with SubAgent==nil, todo_write IS advertised but
// spawn_subagent is NOT, and the existing event sequence for a plain text turn
// is unchanged (no virtual-tool events leak in).
// ===========================================================================

func TestVirtualTools_NoSpawnLeakWhenSubAgentNil(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(textStream("hi"), nil)

	_, err := h.run(t, defaultConfig(), "sess-noleak", "hello")
	require.NoError(t, err)

	// Default golden sequence for a text-only turn is unchanged.
	want := []domain.EventType{
		domain.EventMessageAppended,
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventTurnFinished,
	}
	assert.Equal(t, want, h.eventTypes("sess-noleak"))
}

// ===========================================================================
// AC-6 — PERMISSIONS NOT BYPASSED: a denied spawn_subagent does NOT call Spawn.
// ===========================================================================

func TestSpawnSubagent_DeniedDoesNotCallSpawn(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("sa-deny", toolSpawnSubagent, map[string]any{"task": "x"}), nil)
	h.model.AddStream(textStream("understood"), nil)

	sub := apptest.NewFakeSubAgent(2)
	// No AddResult: if Spawn were (wrongly) called, the fake would panic on an
	// exhausted queue — a strong proof the deny short-circuited before Spawn.
	h.pol.AddDeny("deny-spawn", "sub-agents are denied here")

	cfg := defaultConfig()
	cfg.Depth = 0
	lp := h.loopWithSubAgent(cfg, sub)
	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-spawndeny", UserMessage: userMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, sub.Calls(), "a policy-denied spawn_subagent must NOT call Spawn")

	decs := payloadsOf[domain.PermissionDecided](h, "sess-spawndeny")
	require.Len(t, decs, 1)
	assert.Equal(t, domain.PermissionDeny, decs[0].Decision)
	assert.Equal(t, toolSpawnSubagent, decs[0].ToolName)

	// No ToolExecutionStarted for a denied virtual tool (mirrors real tools).
	assert.Empty(t, payloadsOf[domain.ToolExecutionStarted](h, "sess-spawndeny"))
}

// AC-6 (second) — an ask-resolved-allowed spawn_subagent DOES call Spawn.
func TestSpawnSubagent_AskAllowedCallsSpawn(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("sa-ask", toolSpawnSubagent, map[string]any{"task": "x"}), nil)
	h.model.AddStream(textStream("done"), nil)

	sub := apptest.NewFakeSubAgent(2)
	sub.AddResult(app.ToolResult{Content: "child done"}, nil)
	h.pol.AddAsk("ask-spawn", "approve sub-agent")

	go func() {
		_ = h.gate.Resolve(context.Background(), "sess-spawnask", "sa-ask", domain.AskAllowed)
	}()

	cfg := defaultConfig()
	cfg.Depth = 0
	lp := h.loopWithSubAgent(cfg, sub)
	res, err := lp.Run(context.Background(), agent.RunInput{SessionID: "sess-spawnask", UserMessage: userMsg("delegate")})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	require.Len(t, sub.Calls(), 1, "an approved ask must dispatch Spawn")
}

// ===========================================================================
// AC-7 / AC-8 — todo_write intercept: appends a PlanUpdated, returns a
// confirmation ToolResult, does NOT call Tools.ExecuteTool, and the event
// sequence carries ToolExecutionStarted + PlanUpdated + ToolResult in order.
// ===========================================================================

func TestTodoWrite_AppendsPlanUpdatedGoldenSequence(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("td-1", toolTodoWrite, map[string]any{
		"items": []any{
			map[string]any{"content": "explore", "status": "completed"},
			map[string]any{"content": "implement", "status": "in_progress"},
			map[string]any{"content": "test", "status": "pending"},
		},
	}), nil)
	h.model.AddStream(textStream("plan recorded"), nil)
	h.pol.AddAllow("allow-todo", "planning is read-only")

	res, err := h.run(t, defaultConfig(), "sess-todo", "make a plan")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// A PlanUpdated event was appended with the three items intact.
	plans := payloadsOf[domain.PlanUpdated](h, "sess-todo")
	require.Len(t, plans, 1, "todo_write must append exactly one PlanUpdated event")
	require.Len(t, plans[0].Items, 3)
	assert.Equal(t, "explore", plans[0].Items[0].Content)
	assert.Equal(t, "completed", plans[0].Items[0].Status)
	assert.Equal(t, "in_progress", plans[0].Items[1].Status)
	assert.Equal(t, "pending", plans[0].Items[2].Status)

	// The real tool runtime was never consulted.
	assert.Empty(t, h.tools.Calls(), "todo_write must not call Tools.ExecuteTool")

	// A confirmation ToolResult (not an error) was fed back.
	trs := payloadsOf[domain.ToolResult](h, "sess-todo")
	require.Len(t, trs, 1)
	assert.False(t, trs[0].IsError, "a valid todo_write yields a non-error confirmation result")
	assert.Equal(t, "td-1", trs[0].CallID)

	// AC-8 golden ordering: ToolExecutionStarted, then PlanUpdated, then ToolResult.
	seq := h.eventTypes("sess-todo")
	tesIdx := indexOfType(seq, domain.EventToolExecutionStarted)
	planIdx := indexOfType(seq, domain.EventPlanUpdated)
	trIdx := indexOfType(seq, domain.EventToolResult)
	require.GreaterOrEqual(t, tesIdx, 0, "ToolExecutionStarted appended for a virtual tool")
	require.GreaterOrEqual(t, planIdx, 0, "PlanUpdated appended for todo_write")
	require.GreaterOrEqual(t, trIdx, 0, "ToolResult appended for a virtual tool")
	assert.Less(t, tesIdx, planIdx, "ToolExecutionStarted precedes PlanUpdated")
	assert.Less(t, planIdx, trIdx, "PlanUpdated precedes ToolResult")
}

// AC-9 — an invalid status is rejected: NO PlanUpdated is appended and the
// confirmation ToolResult is an error.
func TestTodoWrite_InvalidStatusIsErrorNoPlan(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("td-bad", toolTodoWrite, map[string]any{
		"items": []any{
			map[string]any{"content": "x", "status": "not-a-status"},
		},
	}), nil)
	h.model.AddStream(textStream("ok"), nil)
	h.pol.AddAllow("a", "")

	res, err := h.run(t, defaultConfig(), "sess-badtodo", "bad plan")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	assert.Empty(t, payloadsOf[domain.PlanUpdated](h, "sess-badtodo"),
		"an invalid status must NOT append a PlanUpdated event")
	trs := payloadsOf[domain.ToolResult](h, "sess-badtodo")
	require.Len(t, trs, 1)
	assert.True(t, trs[0].IsError, "an invalid todo_write yields an is_error ToolResult")
}

// ===========================================================================
// AC-5 — todo_write classifies read-only (gated, not blocked as mutating), and
// is dispatched without ever hitting the real runtime. (The serialization of
// spawn_subagent is implicitly covered by it being mutating-classified above.)
// ===========================================================================

func TestTodoWrite_ClassifiedReadOnly(t *testing.T) {
	h := newHarness(t)
	h.model.AddStream(toolCallStream("td-cls", toolTodoWrite, map[string]any{
		"items": []any{map[string]any{"content": "a", "status": "pending"}},
	}), nil)
	h.model.AddStream(textStream("ok"), nil)
	// Policy records the decision; the SideEffect passed in is what we assert.
	h.pol.AddAllow("a", "")

	res, err := h.run(t, defaultConfig(), "sess-cls", "plan")
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// The policy engine saw todo_write classified as read-only with no egress.
	calls := h.pol.Calls()
	require.NotEmpty(t, calls, "policy must be consulted for the virtual tool")
	var saw bool
	for _, c := range calls {
		if c.In.ToolName == toolTodoWrite {
			saw = true
			assert.Equal(t, domain.SideEffectReadOnly, c.In.SideEffect,
				"todo_write is classified read-only")
			assert.Equal(t, domain.EgressClassNone, c.In.EgressClass,
				"todo_write performs no egress")
		}
	}
	assert.True(t, saw, "policy.Input for todo_write must be recorded")
}

// ---------------------------------------------------------------------------
// Small JSON/schema helpers local to this file.
// ---------------------------------------------------------------------------

func decodeSchema(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	require.NotEmpty(t, raw, "tool def must carry a JSON schema")
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func requiredFields(schema map[string]any) map[string]struct{} {
	out := map[string]struct{}{}
	req, _ := schema["required"].([]any)
	for _, r := range req {
		if s, ok := r.(string); ok {
			out[s] = struct{}{}
		}
	}
	return out
}

func indexOfType(seq []domain.EventType, want domain.EventType) int {
	for i, e := range seq {
		if e == want {
			return i
		}
	}
	return -1
}
