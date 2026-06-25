package agent

// Virtual tools (ADR-0031). spawn_subagent and todo_write are NOT toolruntime
// tools: the tool-runtime has no access to the event log or to the sub-agent
// spawner, so these two capabilities are handled INSIDE the orchestrator loop.
// This file holds the small, self-contained primitives shared by the three loop
// touch-points that wire them up:
//
//   - ADVERTISE (turn.go buildRequest): appends [virtualToolDefs] after the
//     runtime/config tool defs — spawn_subagent only when a [app.SubAgentPort]
//     is present and the loop's Depth is below its MaxDepth; todo_write always.
//   - CLASSIFY (tools.go handleToolCalls): merges [virtualClasses] into the class
//     map so the scheduler/gate treat spawn_subagent as mutating/none (serialized,
//     never parallelized) and todo_write as read-only/none — WITHOUT consulting the
//     runtime registry. The inline classification WINS over any same-named runtime
//     descriptor so classification is deterministic.
//   - DISPATCH/INTERCEPT (tools.go execOne): intercepts by name BEFORE calling the
//     real runtime, parsing the call args with the defensive parsers below.
//
// Both virtual tools still flow through the FULL permission pipeline (hooks ->
// policy -> approval) exactly like real tools, and both append the same
// ToolExecutionStarted + ToolResult events as real tools for replay parity.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

const (
	// toolNameSpawnSubagent is the stable, model-facing name of the sub-agent
	// virtual tool. It must not collide with a runtime tool name; if it does, the
	// inline classification + intercept WIN (the runtime tool of the same name is
	// shadowed), which the loop logs as a warning.
	toolNameSpawnSubagent = "spawn_subagent"
	// toolNameTodoWrite is the stable, model-facing name of the planning virtual
	// tool. Same shadowing rule as [toolNameSpawnSubagent].
	toolNameTodoWrite = "todo_write"
)

// spawnSubagentSchema is the JSON Schema advertised for spawn_subagent: a required
// `task` (string) and an optional `model` (string). additionalProperties is false
// so the model is steered to exactly these two fields.
const spawnSubagentSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "task": {
      "type": "string",
      "description": "The natural-language task to hand to the child sub-agent. Required and non-empty."
    },
    "model": {
      "type": "string",
      "description": "Optional model id override for the child sub-agent. Omit to inherit the parent's model."
    }
  },
  "required": ["task"]
}`

// todoWriteSchema is the JSON Schema advertised for todo_write: a required `items`
// array whose elements each carry a required `content` (string) and a required
// `status` (enum pending|in_progress|completed). An empty items array is valid (it
// records an empty plan).
const todoWriteSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "items": {
      "type": "array",
      "description": "The full task plan, in order. Send the COMPLETE list every time; it replaces the previous plan. An empty array clears the plan.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "content": {
            "type": "string",
            "description": "A short, imperative description of the plan step."
          },
          "status": {
            "type": "string",
            "enum": ["pending", "in_progress", "completed"],
            "description": "The step's lifecycle status. Keep exactly one item in_progress at a time."
          }
        },
        "required": ["content", "status"]
      }
    }
  },
  "required": ["items"]
}`

const (
	spawnSubagentDescription = "Delegate a focused subtask to a child sub-agent that runs its own bounded " +
		"agent loop and returns a condensed result. Use it to parallelize or isolate " +
		"work (e.g. \"summarize this directory\", \"investigate this failing test\"). " +
		"Provide a self-contained `task`; the child does not see this conversation. " +
		"Recursion is depth-limited, so this tool is only available below the limit."

	todoWriteDescription = "Record or update your task plan (todo list) as a durable, ordered checklist. " +
		"Send the COMPLETE list of items every time — it replaces the previous plan. " +
		"Each item has `content` (the step) and `status` (pending|in_progress|completed). " +
		"Keep exactly one item in_progress. Use this to plan multi-step work and to " +
		"show progress; it has no side effects beyond recording the plan."
)

// spawnSubagentDef is the llm.ToolDef advertised for spawn_subagent.
func spawnSubagentDef() llm.ToolDef {
	return llm.ToolDef{
		Name:        toolNameSpawnSubagent,
		Description: spawnSubagentDescription,
		JSONSchema:  json.RawMessage(spawnSubagentSchema),
	}
}

// todoWriteDef is the llm.ToolDef advertised for todo_write.
func todoWriteDef() llm.ToolDef {
	return llm.ToolDef{
		Name:        toolNameTodoWrite,
		Description: todoWriteDescription,
		JSONSchema:  json.RawMessage(todoWriteSchema),
	}
}

// virtualToolDefs returns the virtual-tool [llm.ToolDef]s to APPEND after the
// runtime/config tool defs in buildRequest (ADR-0031; T10, AC-3/AC-4). The
// advertising is GATED:
//
//   - todo_write is ALWAYS advertised — it is universally useful and
//     side-effect-free w.r.t. the host (it only records a durable plan note).
//   - spawn_subagent is advertised IFF a [app.SubAgentPort] is wired AND this
//     loop's depth is strictly below the port's MaxDepth. The strict `<` keeps
//     advertise/reject consistent with the spawner: the parent only advertises
//     when depth <= MaxDepth-1, so the child it would spawn runs at
//     depth+1 <= MaxDepth and is always accepted; at depth == MaxDepth the tool
//     is hidden so the model is never offered a spawn the spawner would reject.
//
// It returns todo_write first then spawn_subagent (a stable, deterministic
// order); callers append the result so the runtime/config defs are never
// dropped or reordered.
func virtualToolDefs(sub app.SubAgentPort, depth int) []llm.ToolDef {
	defs := []llm.ToolDef{todoWriteDef()}
	if sub != nil && depth < sub.MaxDepth() {
		defs = append(defs, spawnSubagentDef())
	}
	return defs
}

// virtualClasses is the inline safety classification for the virtual tools, used
// by the loop INSTEAD of a runtime-registry lookup. spawn_subagent is mutating (a
// child can do anything; gate + serialize it like any mutation) with no direct
// egress; todo_write is read-only (it only records a plan note) with no egress.
// These classifications WIN over any same-named runtime descriptor.
var virtualClasses = map[string]app.ToolDescriptor{
	toolNameSpawnSubagent: {
		Name:        toolNameSpawnSubagent,
		Description: spawnSubagentDescription,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	},
	toolNameTodoWrite: {
		Name:        toolNameTodoWrite,
		Description: todoWriteDescription,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	},
}

// isVirtualTool reports whether name is one of the in-loop virtual tools, so the
// dispatch path can intercept it before calling the real runtime.
func isVirtualTool(name string) bool {
	_, ok := virtualClasses[name]
	return ok
}

// virtualClassFor returns the inline classification for a virtual tool and whether
// the name is virtual at all. The loop uses this to override the class map.
func virtualClassFor(name string) (app.ToolDescriptor, bool) {
	c, ok := virtualClasses[name]
	return c, ok
}

// mergeVirtualClasses returns a copy of base with the inline virtual-tool
// classifications overlaid (virtual tools WIN over same-named runtime descriptors),
// so handleToolCalls/gateCall/scheduling classify deterministically without the
// runtime registry. base is not mutated.
func mergeVirtualClasses(base map[string]app.ToolDescriptor) map[string]app.ToolDescriptor {
	out := make(map[string]app.ToolDescriptor, len(base)+len(virtualClasses))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range virtualClasses {
		out[k] = v
	}
	return out
}

// parseSpawnArgs extracts the spawn_subagent arguments from a tool call's parsed
// args (map[string]any, JSON-decoded). `task` is required and rejected when
// missing/blank (TrimSpace-empty) WITHOUT calling Spawn; `model` is optional and
// defaults to the empty string. A non-string task/model is a type error.
func parseSpawnArgs(args map[string]any) (task, model string, err error) {
	rawTask, ok := args["task"]
	if !ok {
		return "", "", fmt.Errorf("spawn_subagent: missing required %q argument", "task")
	}
	task, ok = rawTask.(string)
	if !ok {
		return "", "", fmt.Errorf("spawn_subagent: %q must be a string", "task")
	}
	if strings.TrimSpace(task) == "" {
		return "", "", fmt.Errorf("spawn_subagent: %q must not be empty", "task")
	}

	if rawModel, ok := args["model"]; ok && rawModel != nil {
		model, ok = rawModel.(string)
		if !ok {
			return "", "", fmt.Errorf("spawn_subagent: %q must be a string", "model")
		}
	}
	return task, model, nil
}

// parsePlanItems extracts and validates the todo_write `items` from a tool call's
// parsed args. It re-marshals the items value and unmarshals into []domain.PlanItem
// (so JSON key casing maps via the field tags), then runs the single source of
// truth [domain.PlanUpdated.Validate] (rejects empty content / out-of-range status).
// An absent or empty items array is VALID and yields an empty plan (nil slice).
func parsePlanItems(args map[string]any) ([]domain.PlanItem, error) {
	raw, ok := args["items"]
	if !ok || raw == nil {
		// Absent items is a valid empty plan.
		return nil, nil
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("todo_write: cannot encode %q: %w", "items", err)
	}
	var items []domain.PlanItem
	if err := json.Unmarshal(encoded, &items); err != nil {
		return nil, fmt.Errorf("todo_write: %q must be an array of {content,status}: %w", "items", err)
	}
	if err := (domain.PlanUpdated{Items: items}).Validate(); err != nil {
		return nil, fmt.Errorf("todo_write: %w", err)
	}
	return items, nil
}
