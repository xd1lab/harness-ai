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

// toolCatalog returns the 5 v1 tools (create_session, run, get_session, control,
// fork), each with a non-empty description and inputSchema; run additionally
// declares an outputSchema (the synthesized completed result). The set and shape
// are pinned by TestToolsList_ReturnsFiveTools (AC-3).
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
			Description: "Approve or deny a pending risky tool call (the human-in-the-loop decision that unblocks a paused run), or interrupt/reattach a session. Called concurrently with an open 'run' call on a separate connection.",
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
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}
