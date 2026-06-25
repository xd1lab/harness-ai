// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
)

// This file holds the MCP tool catalog (the 5 v1 tools and their JSON Schemas)
// and the payload builders for the run leg: the notifications/progress params,
// the in-band approval params, and the synthesized "completed" CallToolResult.
// Each tool maps 1:1 to a shared igrpc.Server method; the dispatch itself lives
// in handler.go.

// mcpProtocolVersion is the MCP protocol version this hand-rolled server
// implements and declares in initialize (and accepts via MCP-Protocol-Version).
const mcpProtocolVersion = "2025-06-18"

// serverName is the MCP serverInfo.name advertised in initialize.
const serverName = "boltrope"

// toolDescriptor is one entry in tools/list: the wire-shaped Tool object.
type toolDescriptor struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"inputSchema"`
	OutputSchema map[string]any `json:"outputSchema,omitempty"`
}

// objectSchema is a small helper building a JSON Schema object node.
func objectSchema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

// objectSchemaBytes validates that an inline output_schema is a JSON object (the
// only shape a JSON Schema document takes) and returns its raw bytes. An
// empty/omitted/null value yields (nil, true) — free-form, no error. A non-object
// (number/array/string/bool) yields (nil, false) so the caller rejects it with a
// JSON-RPC InvalidParams before any run starts.
func objectSchemaBytes(raw json.RawMessage) (schema []byte, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, true
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}
	return []byte(raw), true
}

// toolCatalog returns the 12 v1 tools (create_session, run, get_session, control,
// fork, list_sessions, get_session_usage, list_session_events, get_state_at_seq,
// get_session_cost, get_tenant_cost, verify_session_integrity), each with a
// non-empty description and inputSchema; run additionally declares an outputSchema
// (the synthesized completed result). The set and shape are pinned by
// TestToolsList_ReturnsTwelveTools (list_sessions + get_session_usage are the
// Feature I / ADR-0027 admin reads; list_session_events + get_state_at_seq are
// Feature M / event-read; get_session_cost + get_tenant_cost are Feature O /
// cost-read; verify_session_integrity is the Batch-5A tamper-evident hash-chain
// verify; the control tool's description documents that interrupt is the admin
// STOP).
func toolCatalog() []toolDescriptor {
	return []toolDescriptor{
		{
			Name:        "create_session",
			Description: "Open a fresh, tenant-owned agent session (event-sourced stream) on Boltrope. Returns the new session_id.",
			InputSchema: objectSchema(map[string]any{
				"mode": map[string]any{
					"type":        "string",
					"enum":        []string{"default", "acceptEdits", "plan"},
					"description": "Standing permission mode. Empty = server default. 'bypass' is operator-only and rejected.",
				},
				"metadata": map[string]any{
					"type":                 "object",
					"additionalProperties": map[string]any{"type": "string"},
					"description":          "Optional opaque key/value labels recorded on the session.",
				},
			}),
		},
		{
			Name:        "run",
			Description: "Submit a user turn (or a pure resume) and drive the agent loop, streaming progress on an open SSE leg until a terminal result. Risk-tier approvals are surfaced in-band as a progress notification and resolved by a concurrent 'control' call while this call stays open. Send a _meta.progressToken to receive progress (including the in-band approval).",
			InputSchema: objectSchema(map[string]any{
				"session_id":    map[string]any{"type": "string", "description": "Target session id."},
				"text":          map[string]any{"type": "string", "description": "The user message. Empty = pure resume from after_seq."},
				"after_seq":     map[string]any{"type": "integer", "description": "Resume cursor: only events with seq > after_seq are streamed."},
				"output_schema": map[string]any{"type": "object", "description": "JSON Schema constraining the final result. Omit for free-form output."},
				"strict":        map[string]any{"type": "boolean", "description": "Request native strict schema enforcement where supported; otherwise validate-and-retry."},
			}, "session_id"),
			OutputSchema: objectSchema(map[string]any{
				"status":     map[string]any{"type": "string", "enum": []string{"completed"}},
				"session_id": map[string]any{"type": "string"},
				"subtype":    map[string]any{"type": "string", "description": "The termination subtype token (e.g. TERMINATION_SUBTYPE_SUCCESS)."},
				"final_text": map[string]any{"type": "string"},
				"num_turns":  map[string]any{"type": "integer"},
				"cost_usd":   map[string]any{"type": "number"},
				"after_seq":  map[string]any{"type": "integer", "description": "Terminal durable seq cursor."},
			}, "status", "session_id", "after_seq"),
		},
		{
			Name:        "get_session",
			Description: "Read the materialized session projection (status, head seq, mode, usage, cost, turns, lineage) for an owned session. Idempotent.",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
			}, "session_id"),
		},
		{
			Name:        "control",
			Description: "Approve or deny a pending risky tool call (the human-in-the-loop decision that unblocks a paused run), or interrupt/reattach a session. Called concurrently with an open 'run' call on a separate connection. interrupt = admin stop (cooperative, resumable, idempotent no-op when the session is already finished).",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
				"action":     map[string]any{"type": "string", "enum": []string{"approve", "deny", "interrupt", "reattach"}},
				"call_id":    map[string]any{"type": "string", "description": "The pending approval id from the in-band approval notification (approve/deny)."},
				"reason":     map[string]any{"type": "string", "description": "Optional reason recorded on a deny."},
				"from_seq":   map[string]any{"type": "integer", "description": "Reattach cursor (reattach)."},
			}, "session_id", "action"),
		},
		{
			Name:        "fork",
			Description: "Branch a child session from a parent at a given seq (the parent is unaffected) for what-if exploration. Returns the child session_id.",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Parent session id."},
				"at_seq":     map[string]any{"type": "integer", "description": "Parent seq to branch at; the child continues from at_seq+1."},
			}, "session_id"),
		},
		{
			Name:        "list_sessions",
			Description: "List the caller-tenant's sessions (control/lineage projection: status, mode, head_seq, lineage, timestamps — no usage/cost) with an optional status OR-filter and a half-open created_at window, keyset-paginated via an opaque page_token. The tenant is the authenticated principal (no tenant_id arg). Use get_session_usage for per-session usage/cost.",
			InputSchema: objectSchema(map[string]any{
				"status": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string", "enum": []string{"active", "finished", "failed"}},
					"description": "Status OR-filter; omit for all statuses.",
				},
				"created_after_ms":  map[string]any{"type": "integer", "description": "Keep created_at >= this Unix epoch ms (inclusive). Omit for no lower bound."},
				"created_before_ms": map[string]any{"type": "integer", "description": "Keep created_at < this Unix epoch ms (exclusive, half-open). Omit for no upper bound."},
				"page_token":        map[string]any{"type": "string", "description": "Opaque cursor from a prior page's next_page_token; omit for the first page."},
				"page_size":         map[string]any{"type": "integer", "description": "Max sessions per page; <=0 defaults to 50, capped at 200."},
				"descending":        map[string]any{"type": "boolean", "description": "Newest-first when true (direction is carried in the page_token across pages)."},
			}),
		},
		{
			Name:        "get_session_usage",
			Description: "Read accumulated per-session usage/cost/turns for an owned session, folded from the event log (includes any interrupted partial; never re-billed). The 'source' field tags provenance (USAGE_SOURCE_EVENT_FOLD in v1).",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
			}, "session_id"),
		},
		{
			Name:        "list_session_events",
			Description: "List an owned session's events as redacted descriptors (seq, type, actor, timestamps, blob metadata, a bounded summary), keyset-paginated on seq. Sensitive payloads are never returned: provider_raw and the system prompt are always omitted, streaming checkpoints are never exposed, and large tool output stays a blob reference.",
			InputSchema: objectSchema(map[string]any{
				"session_id":      map[string]any{"type": "string", "description": "Target session id."},
				"after_seq":       map[string]any{"type": "integer", "description": "Keyset cursor: only events with seq > after_seq are returned."},
				"page_size":       map[string]any{"type": "integer", "description": "Max descriptors per page; <=0 defaults to 100, capped at 1000."},
				"include_payload": map[string]any{"type": "boolean", "description": "Widen summaries to (truncated) payload text; provider_raw and the system prompt stay omitted even when true."},
			}, "session_id"),
		},
		{
			Name:        "get_state_at_seq",
			Description: "Reconstruct an owned session's folded control/billing projection at a sequence point (time-travel) via Load-then-fold — it creates no session and re-bills nothing. at_seq<=0 yields the empty state; at_seq beyond head is clamped to head.",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
				"at_seq":     map[string]any{"type": "integer", "description": "Inclusive upper seq bound to reconstruct to."},
			}, "session_id"),
		},
		{
			Name:        "get_session_cost",
			Description: "Read an owned session's cost: a per-model breakdown (sorted by cost descending; an uncorrelated model is the 'unknown' bucket) plus the session total, from the persisted cost-rollup projection.",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
			}, "session_id"),
		},
		{
			Name:        "get_tenant_cost",
			Description: "Read the authenticated tenant's cost: a per-model breakdown, the tenant total, and the count of distinct sessions carrying cost. The tenant is the authenticated principal (no tenant_id arg).",
			InputSchema: objectSchema(map[string]any{}),
		},
		{
			Name:        "verify_session_integrity",
			Description: "Verify the tamper-evident audit chain of an owned session: recompute each event's content hash from its stored payload and the per-session SHA-256 hash-chain, comparing against the stored digests. Returns valid plus, on the first mismatch, first_bad_seq and a reason distinguishing a tampered payload (content-hash mismatch) from a broken link (chain-hash mismatch); checked is the number of chained events verified (a pre-chain NULL-hash prefix is skipped).",
			InputSchema: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string", "description": "Target session id."},
				"from_seq":   map[string]any{"type": "integer", "description": "Inclusive lower seq bound; <=0 starts at the first chained event."},
				"to_seq":     map[string]any{"type": "integer", "description": "Inclusive upper seq bound; <=0 or beyond head verifies to head."},
			}, "session_id"),
		},
	}
}

// ---- tool-call argument envelopes -------------------------------------------

// createSessionArgs is the create_session tool arguments.
type createSessionArgs struct {
	Mode     string            `json:"mode"`
	Metadata map[string]string `json:"metadata"`
}

// runArgs is the run tool arguments.
type runArgs struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
	AfterSeq  int64  `json:"after_seq"`
	// OutputSchema is an OPTIONAL JSON Schema object constraining the final result.
	// It is received as inline JSON and marshaled to bytes onto
	// RunRequest.output_schema; a non-object value is a JSON-RPC InvalidParams.
	OutputSchema json.RawMessage `json:"output_schema"`
	// Strict requests native strict schema enforcement where supported; otherwise
	// the loop validates and retries. Meaningful only with output_schema.
	Strict bool `json:"strict"`
}

// getSessionArgs is the get_session tool arguments.
type getSessionArgs struct {
	SessionID string `json:"session_id"`
}

// controlArgs is the control tool arguments.
type controlArgs struct {
	SessionID string `json:"session_id"`
	Action    string `json:"action"`
	CallID    string `json:"call_id"`
	Reason    string `json:"reason"`
	FromSeq   int64  `json:"from_seq"`
}

// forkArgs is the fork tool arguments.
type forkArgs struct {
	SessionID string `json:"session_id"`
	AtSeq     int64  `json:"at_seq"`
}

// listSessionsArgs is the list_sessions tool arguments (Feature I / ADR-0027). No
// tenant_id field: the tenant is the authenticated principal.
type listSessionsArgs struct {
	Status          []string `json:"status"`
	CreatedAfterMs  int64    `json:"created_after_ms"`
	CreatedBeforeMs int64    `json:"created_before_ms"`
	PageToken       string   `json:"page_token"`
	PageSize        int32    `json:"page_size"`
	Descending      bool     `json:"descending"`
}

// getSessionUsageArgs is the get_session_usage tool arguments.
type getSessionUsageArgs struct {
	SessionID string `json:"session_id"`
}

// listSessionEventsArgs is the list_session_events tool arguments (Feature M).
type listSessionEventsArgs struct {
	SessionID      string `json:"session_id"`
	AfterSeq       int64  `json:"after_seq"`
	PageSize       int32  `json:"page_size"`
	IncludePayload bool   `json:"include_payload"`
}

// getStateAtSeqArgs is the get_state_at_seq tool arguments (Feature M).
type getStateAtSeqArgs struct {
	SessionID string `json:"session_id"`
	AtSeq     int64  `json:"at_seq"`
}

// getSessionCostArgs is the get_session_cost tool arguments (Feature O).
type getSessionCostArgs struct {
	SessionID string `json:"session_id"`
}

// getTenantCostArgs is the get_tenant_cost tool arguments (Feature O). No fields:
// the tenant is the authenticated principal.
type getTenantCostArgs struct{}

// verifySessionIntegrityArgs is the verify_session_integrity tool arguments
// (Batch-5A tamper-evident hash-chain). session_id is required; from_seq/to_seq
// bound the verified window (<=0 = first chained event / head).
type verifySessionIntegrityArgs struct {
	SessionID string `json:"session_id"`
	FromSeq   int64  `json:"from_seq"`
	ToSeq     int64  `json:"to_seq"`
}

// ---- CallToolResult ---------------------------------------------------------

// callToolResult is the MCP CallToolResult wire shape. content carries a
// human-readable text block; structuredContent the machine-readable object;
// isError marks a genuine tool-execution error (never used for protocol errors).
type callToolResult struct {
	Content           []contentBlock `json:"content"`
	StructuredContent any            `json:"structuredContent,omitempty"`
	IsError           bool           `json:"isError"`
}

// contentBlock is one MCP content block (v1 uses only text).
type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// textResult builds a CallToolResult with one text block and a structured
// payload, isError:false.
func textResult(text string, structured any) callToolResult {
	return callToolResult{
		Content:           []contentBlock{{Type: "text", Text: text}},
		StructuredContent: structured,
		IsError:           false,
	}
}

// ---- run leg payload builders -----------------------------------------------

// progressParams is the notifications/progress params, carrying the echoed token,
// the strictly-increasing progress, a short message, and — on the approval
// frame — the in-band approval fields (call_id/tool_name/args_json/reason/
// after_seq). The optional approval fields are omitted on ordinary deltas.
type progressParams struct {
	ProgressToken json.RawMessage `json:"progressToken"`
	Progress      float64         `json:"progress"`
	Message       string          `json:"message,omitempty"`

	CallID   string `json:"call_id,omitempty"`
	ToolName string `json:"tool_name,omitempty"`
	ArgsJSON string `json:"args_json,omitempty"`
	Reason   string `json:"reason,omitempty"`
	AfterSeq int64  `json:"after_seq,omitempty"`
}

// progressParamsFor builds the notifications/progress params for a non-terminal
// RunEvent. The approval_request frame additionally carries the in-band approval
// fields so a client reading the live stream learns the call_id to resolve via a
// concurrent control call (the call stays open).
func progressParamsFor(token json.RawMessage, progress float64, frame *genproto.RunEvent) progressParams {
	p := progressParams{
		ProgressToken: token,
		Progress:      progress,
		Message:       progressEventName(frame),
	}
	if ar := frame.GetApprovalRequest(); ar != nil {
		p.CallID = ar.GetCallId()
		p.ToolName = ar.GetToolName()
		p.ArgsJSON = ar.GetArgsJson()
		p.Reason = ar.GetReason()
		p.AfterSeq = frame.GetSeq()
	}
	return p
}

// progressEventName names the SSE event / progress message after the frame's
// payload case (mirrors rest.eventName for the incremental cases).
func progressEventName(f *genproto.RunEvent) string {
	switch f.GetPayload().(type) {
	case *genproto.RunEvent_TextDelta:
		return "text_delta"
	case *genproto.RunEvent_ThinkingDelta:
		return "thinking_delta"
	case *genproto.RunEvent_ToolProgress:
		return "tool_progress"
	case *genproto.RunEvent_ApprovalRequest:
		return "approval_request"
	default:
		return "progress"
	}
}

// completedRunResult is the synthesized terminal structuredContent for a run.
type completedRunResult struct {
	Status    string  `json:"status"`
	SessionID string  `json:"session_id"`
	Subtype   string  `json:"subtype"`
	FinalText string  `json:"final_text"`
	NumTurns  int64   `json:"num_turns"`
	CostUSD   float64 `json:"cost_usd"`
	AfterSeq  int64   `json:"after_seq"`
}

// completedCallResult builds the terminal CallToolResult for a run's RunResult
// frame at the given seq: status "completed", the subtype token, final text,
// turns, cost, and the terminal after_seq cursor. isError is always false — a
// completed run (even a non-success subtype) is a valid result, not a tool error.
func completedCallResult(sessionID string, seq int64, res *genproto.RunResult) callToolResult {
	subtype := res.GetSubtype().String()
	final := res.GetFinalText()
	summary := final
	if summary == "" {
		summary = subtype
	}
	return textResult(summary, completedRunResult{
		Status:    "completed",
		SessionID: sessionID,
		Subtype:   subtype,
		FinalText: final,
		NumTurns:  res.GetNumTurns(),
		CostUSD:   res.GetCostUsd(),
		AfterSeq:  seq,
	})
}

// ---- protojson helpers ------------------------------------------------------

// protoToMap marshals a proto message via protojson (canonical proto3 JSON) and
// decodes it into a generic map so the CallToolResult JSON nests it as an object
// (the wire vocabulary stays the proto contract; tests assert decoded fields, not
// exact bytes, since protojson whitespace is randomized).
func protoToMap(m proto.Message) (map[string]any, error) {
	return protoToMapOpts(m, protojson.MarshalOptions{})
}

// protoToMapFull is protoToMap with EmitUnpopulated set: proto3-default scalars (a
// false bool, a 0 int64) are emitted rather than elided. Used where a response's
// default values are themselves load-bearing — e.g. the verify verdict valid=false
// / first_bad_seq=0 — so an MCP client never has to disambiguate "absent" from
// "false/zero" (mirrors rest.writeProtoFull).
func protoToMapFull(m proto.Message) (map[string]any, error) {
	return protoToMapOpts(m, protojson.MarshalOptions{EmitUnpopulated: true})
}

func protoToMapOpts(m proto.Message, opts protojson.MarshalOptions) (map[string]any, error) {
	b, err := opts.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}
