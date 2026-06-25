package agent

// Unit tests for the in-loop virtual-tool primitives (ADR-0031, task T9): the
// stable names, the advertised llm.ToolDef schemas (AC-3/AC-4), the inline
// classification map (AC-5), and the defensive arg parsers (empty/whitespace task
// rejection, invalid status rejection, empty items = valid empty plan). These are
// INTERNAL (package agent) tests because they exercise unexported primitives;
// loop-level wiring tests live in virtual_tools_test.go (package agent_test).

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// decodeSchemaMap unmarshals a raw JSON schema into a generic map for shape
// assertions (local to this internal test file).
func decodeSchemaMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	require.NotEmpty(t, raw, "tool def must carry a JSON schema")
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m), "schema must be valid JSON")
	return m
}

func requiredSet(t *testing.T, schema map[string]any) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	req, _ := schema["required"].([]any)
	for _, r := range req {
		if s, ok := r.(string); ok {
			out[s] = struct{}{}
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Names (AC-3/AC-4): stable, and the two virtual tools are recognized.
// ---------------------------------------------------------------------------

func TestVirtualToolNames_Stable(t *testing.T) {
	assert.Equal(t, "spawn_subagent", toolNameSpawnSubagent)
	assert.Equal(t, "todo_write", toolNameTodoWrite)

	assert.True(t, isVirtualTool(toolNameSpawnSubagent))
	assert.True(t, isVirtualTool(toolNameTodoWrite))
	assert.False(t, isVirtualTool("read"), "a real runtime tool name is not virtual")
	assert.False(t, isVirtualTool(""), "the empty name is not virtual")
}

// ---------------------------------------------------------------------------
// AC-3 — spawn_subagent def + schema: requires task (string), optional model.
// ---------------------------------------------------------------------------

func TestSpawnSubagentDef_Schema(t *testing.T) {
	def := spawnSubagentDef()
	assert.Equal(t, toolNameSpawnSubagent, def.Name)
	assert.NotEmpty(t, def.Description, "spawn_subagent needs a model-facing description")

	schema := decodeSchemaMap(t, def.JSONSchema)
	assert.Equal(t, "object", schema["type"])

	props, _ := schema["properties"].(map[string]any)
	require.NotNil(t, props, "schema declares properties")
	require.Contains(t, props, "task")
	require.Contains(t, props, "model")

	task, _ := props["task"].(map[string]any)
	require.NotNil(t, task)
	assert.Equal(t, "string", task["type"], "task is a string")
	model, _ := props["model"].(map[string]any)
	require.NotNil(t, model)
	assert.Equal(t, "string", model["type"], "model is a string")

	req := requiredSet(t, schema)
	assert.Contains(t, req, "task", "task is required")
	assert.NotContains(t, req, "model", "model is optional")
}

// ---------------------------------------------------------------------------
// AC-4 — todo_write def + schema: requires items array of {content,status:enum}.
// ---------------------------------------------------------------------------

func TestTodoWriteDef_Schema(t *testing.T) {
	def := todoWriteDef()
	assert.Equal(t, toolNameTodoWrite, def.Name)
	assert.NotEmpty(t, def.Description, "todo_write needs a model-facing description")

	schema := decodeSchemaMap(t, def.JSONSchema)
	props, _ := schema["properties"].(map[string]any)
	require.NotNil(t, props)
	require.Contains(t, props, "items")
	assert.Contains(t, requiredSet(t, schema), "items", "items is required")

	items, _ := props["items"].(map[string]any)
	require.NotNil(t, items)
	assert.Equal(t, "array", items["type"], "items is an array")

	itemSchema, _ := items["items"].(map[string]any)
	require.NotNil(t, itemSchema, "items.items describes the element shape")
	itemProps, _ := itemSchema["properties"].(map[string]any)
	require.NotNil(t, itemProps)
	require.Contains(t, itemProps, "content")
	require.Contains(t, itemProps, "status")

	content, _ := itemProps["content"].(map[string]any)
	require.NotNil(t, content)
	assert.Equal(t, "string", content["type"])

	status, _ := itemProps["status"].(map[string]any)
	require.NotNil(t, status)
	assert.Equal(t, "string", status["type"])
	enum, _ := status["enum"].([]any)
	require.Len(t, enum, 3, "status enum is exactly pending|in_progress|completed")
	assert.ElementsMatch(t, []any{"pending", "in_progress", "completed"}, enum)

	itemReq := requiredSet(t, itemSchema)
	assert.Contains(t, itemReq, "content")
	assert.Contains(t, itemReq, "status")
}

// ---------------------------------------------------------------------------
// AC-5 — inline classification values + override semantics.
// ---------------------------------------------------------------------------

func TestVirtualClasses_Values(t *testing.T) {
	spawn, ok := virtualClassFor(toolNameSpawnSubagent)
	require.True(t, ok)
	assert.Equal(t, domain.SideEffectMutating, spawn.SideEffect,
		"spawn_subagent is mutating (a child can do anything; gate + serialize it)")
	assert.Equal(t, domain.EgressClassNone, spawn.EgressClass,
		"spawn_subagent performs no direct egress")

	todo, ok := virtualClassFor(toolNameTodoWrite)
	require.True(t, ok)
	assert.Equal(t, domain.SideEffectReadOnly, todo.SideEffect,
		"todo_write only records a plan note: read-only")
	assert.Equal(t, domain.EgressClassNone, todo.EgressClass,
		"todo_write performs no egress")

	_, ok = virtualClassFor("read")
	assert.False(t, ok, "a real runtime tool is not in the virtual class map")
}

// The inline classification WINS over a same-named runtime descriptor so
// classification is deterministic (AC-5).
func TestMergeVirtualClasses_VirtualWins(t *testing.T) {
	base := map[string]app.ToolDescriptor{
		// A real runtime tool that should pass through unchanged.
		"read": {Name: "read", SideEffect: domain.SideEffectReadOnly, EgressClass: domain.EgressClassNone},
		// A runtime descriptor that COLLIDES with a virtual tool name and is
		// deliberately mis-classified to prove the virtual class overrides it.
		toolNameTodoWrite: {Name: toolNameTodoWrite, SideEffect: domain.SideEffectMutating, EgressClass: domain.EgressClassExternal},
	}
	merged := mergeVirtualClasses(base)

	// Pass-through real tool is preserved.
	assert.Equal(t, domain.SideEffectReadOnly, merged["read"].SideEffect)

	// Virtual tool overrides the colliding runtime descriptor.
	assert.Equal(t, domain.SideEffectReadOnly, merged[toolNameTodoWrite].SideEffect,
		"virtual todo_write classification wins over a same-named runtime descriptor")
	assert.Equal(t, domain.EgressClassNone, merged[toolNameTodoWrite].EgressClass)

	// spawn_subagent (not in base) is added.
	assert.Equal(t, domain.SideEffectMutating, merged[toolNameSpawnSubagent].SideEffect)

	// base is not mutated.
	assert.Equal(t, domain.SideEffectMutating, base[toolNameTodoWrite].SideEffect,
		"mergeVirtualClasses must not mutate the input map")
}

// ---------------------------------------------------------------------------
// AC-3/AC-7 — parseSpawnArgs edge cases.
// ---------------------------------------------------------------------------

func TestParseSpawnArgs(t *testing.T) {
	t.Run("task and model", func(t *testing.T) {
		task, model, err := parseSpawnArgs(map[string]any{"task": "do x", "model": "child-model"})
		require.NoError(t, err)
		assert.Equal(t, "do x", task)
		assert.Equal(t, "child-model", model)
	})
	t.Run("model omitted is empty", func(t *testing.T) {
		task, model, err := parseSpawnArgs(map[string]any{"task": "do x"})
		require.NoError(t, err)
		assert.Equal(t, "do x", task)
		assert.Equal(t, "", model)
	})
	t.Run("model null is empty", func(t *testing.T) {
		_, model, err := parseSpawnArgs(map[string]any{"task": "do x", "model": nil})
		require.NoError(t, err)
		assert.Equal(t, "", model)
	})
	t.Run("missing task rejected", func(t *testing.T) {
		_, _, err := parseSpawnArgs(map[string]any{"model": "m"})
		require.Error(t, err)
	})
	t.Run("empty task rejected", func(t *testing.T) {
		_, _, err := parseSpawnArgs(map[string]any{"task": ""})
		require.Error(t, err)
	})
	t.Run("whitespace task rejected", func(t *testing.T) {
		_, _, err := parseSpawnArgs(map[string]any{"task": "   \t\n  "})
		require.Error(t, err, "a TrimSpace-empty task is rejected without calling Spawn")
	})
	t.Run("non-string task rejected", func(t *testing.T) {
		_, _, err := parseSpawnArgs(map[string]any{"task": 42})
		require.Error(t, err)
	})
	t.Run("non-string model rejected", func(t *testing.T) {
		_, _, err := parseSpawnArgs(map[string]any{"task": "x", "model": 7})
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// AC-4/AC-9 — parsePlanItems edge cases.
// ---------------------------------------------------------------------------

func TestParsePlanItems(t *testing.T) {
	t.Run("valid items", func(t *testing.T) {
		items, err := parsePlanItems(map[string]any{
			"items": []any{
				map[string]any{"content": "explore", "status": "completed"},
				map[string]any{"content": "implement", "status": "in_progress"},
				map[string]any{"content": "test", "status": "pending"},
			},
		})
		require.NoError(t, err)
		require.Len(t, items, 3)
		assert.Equal(t, domain.PlanItem{Content: "explore", Status: "completed"}, items[0])
		assert.Equal(t, domain.PlanItem{Content: "implement", Status: "in_progress"}, items[1])
		assert.Equal(t, domain.PlanItem{Content: "test", Status: "pending"}, items[2])
	})
	t.Run("empty items array is a valid empty plan", func(t *testing.T) {
		items, err := parsePlanItems(map[string]any{"items": []any{}})
		require.NoError(t, err)
		assert.Empty(t, items)
	})
	t.Run("absent items is a valid empty plan", func(t *testing.T) {
		items, err := parsePlanItems(map[string]any{})
		require.NoError(t, err)
		assert.Empty(t, items)
	})
	t.Run("null items is a valid empty plan", func(t *testing.T) {
		items, err := parsePlanItems(map[string]any{"items": nil})
		require.NoError(t, err)
		assert.Empty(t, items)
	})
	t.Run("invalid status rejected", func(t *testing.T) {
		_, err := parsePlanItems(map[string]any{
			"items": []any{map[string]any{"content": "x", "status": "not-a-status"}},
		})
		require.Error(t, err, "an out-of-range status is rejected before any PlanUpdated")
	})
	t.Run("empty content rejected", func(t *testing.T) {
		_, err := parsePlanItems(map[string]any{
			"items": []any{map[string]any{"content": "", "status": "pending"}},
		})
		require.Error(t, err)
	})
	t.Run("missing status rejected", func(t *testing.T) {
		_, err := parsePlanItems(map[string]any{
			"items": []any{map[string]any{"content": "x"}},
		})
		require.Error(t, err, "a missing status decodes to empty and fails validation")
	})
	t.Run("malformed items shape rejected", func(t *testing.T) {
		_, err := parsePlanItems(map[string]any{"items": "not-an-array"})
		require.Error(t, err)
	})
}
