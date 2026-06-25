package grpc

import (
	"encoding/json"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file is the only place the orchestrator's client edge maps gen/ wire
// types ⇄ domain / llm kernel types (architecture §12.4). Keeping the mapping in
// one file makes the boundary auditable and keeps server.go free of wire
// concerns.

// ---- Session ----------------------------------------------------------------

// toGenSession maps a [domain.Session] (plus the materialized usage/cost/turns
// folded by the caller) to the wire [genproto.Session].
func toGenSession(s domain.Session, usage llm.Usage, costUSD float64, numTurns int64) *genproto.Session {
	return &genproto.Session{
		SessionId:       s.ID,
		TenantId:        s.TenantID,
		Status:          toGenStatus(s.Status),
		Mode:            toGenMode(s.Mode),
		HeadSeq:         s.HeadSeq,
		TotalUsage:      toGenUsage(usage),
		TotalCostUsd:    costUSD,
		NumTurns:        numTurns,
		ParentSessionId: s.ParentID,
		ForkedFromSeq:   s.ForkedFromSeq,
	}
}

// toGenStatus maps a [domain.SessionStatus] to the wire enum.
func toGenStatus(s domain.SessionStatus) genproto.SessionStatus {
	switch s {
	case domain.StatusActive:
		return genproto.SessionStatus_SESSION_STATUS_ACTIVE
	case domain.StatusFinished:
		return genproto.SessionStatus_SESSION_STATUS_FINISHED
	case domain.StatusFailed:
		return genproto.SessionStatus_SESSION_STATUS_FAILED
	default:
		return genproto.SessionStatus_SESSION_STATUS_UNSPECIFIED
	}
}

// toGenUsage maps an [llm.Usage] to the wire [genproto.Usage]. The llm kernel
// counts are int; the wire fields are int64.
func toGenUsage(u llm.Usage) *genproto.Usage {
	return &genproto.Usage{
		InputTokens:      int64(u.InputTokens),
		OutputTokens:     int64(u.OutputTokens),
		CacheReadTokens:  int64(u.CacheReadTokens),
		CacheWriteTokens: int64(u.CacheWriteTokens),
		ReasoningTokens:  int64(u.ReasoningTokens),
	}
}

// ---- Permission mode --------------------------------------------------------

// fromGenModeDomain maps the wire [genproto.PermissionMode] to the persisted
// session-level [domain.PermissionMode] (ADR-0019), used at CreateSession to stamp
// the session's standing mode (a client-supplied bypass is rejected before this is
// reached). It is an EXPLICIT mapping, not a cast: the domain spelling
// ("acceptEdits") differs from policy.Mode's ("accept_edits"). UNSPECIFIED/DEFAULT
// both resolve to the secure [domain.ModeDefault].
func fromGenModeDomain(m genproto.PermissionMode) domain.PermissionMode {
	switch m {
	case genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS:
		return domain.ModeAcceptEdits
	case genproto.PermissionMode_PERMISSION_MODE_PLAN:
		return domain.ModePlan
	case genproto.PermissionMode_PERMISSION_MODE_BYPASS:
		return domain.ModeBypass
	default:
		return domain.ModeDefault
	}
}

// toPolicyMode maps the persisted session-level [domain.PermissionMode] to the
// live [policy.Mode] the loop runs under (ADR-0019). EXPLICIT mapping (not a cast):
// the two vocabularies agree on default/plan/bypass but differ on accept-edits
// (domain "acceptEdits" vs policy "accept_edits"). An unset/unknown mode resolves
// to the secure [policy.ModeDefault].
func toPolicyMode(m domain.PermissionMode) policy.Mode {
	switch m {
	case domain.ModeAcceptEdits:
		return policy.ModeAcceptEdits
	case domain.ModePlan:
		return policy.ModePlan
	case domain.ModeBypass:
		return policy.ModeBypass
	default:
		return policy.ModeDefault
	}
}

// toGenMode maps the persisted session-level [domain.PermissionMode] back to the
// wire [genproto.PermissionMode] for GetSession. The empty zero value (a session
// created before the mode column existed, or a fake that does not set it) maps to
// UNSPECIFIED, which a client reads as "the server default applies".
func toGenMode(m domain.PermissionMode) genproto.PermissionMode {
	switch m {
	case domain.ModeDefault:
		return genproto.PermissionMode_PERMISSION_MODE_DEFAULT
	case domain.ModeAcceptEdits:
		return genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS
	case domain.ModePlan:
		return genproto.PermissionMode_PERMISSION_MODE_PLAN
	case domain.ModeBypass:
		return genproto.PermissionMode_PERMISSION_MODE_BYPASS
	default:
		return genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED
	}
}

// ---- Termination subtype ----------------------------------------------------

// toGenSubtype maps a [domain.TerminationReason] to the wire termination subtype.
func toGenSubtype(r domain.TerminationReason) genproto.TerminationSubtype {
	switch r {
	case domain.Success:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS
	case domain.ErrorMaxTurns:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_TURNS
	case domain.ErrorMaxBudgetUSD:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_BUDGET_USD
	case domain.ErrorDuringExecution:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION
	case domain.ErrorDoomLoop:
		// FIX 2 (ADR-0032): a doom-loop is a generic execution error on the wire.
		// It maps to the EXISTING ERROR_DURING_EXECUTION subtype so no proto/gen
		// change is needed; the distinct domain reason is preserved for metrics and
		// logs. Explicit (not default) so it never silently falls through to
		// UNSPECIFIED.
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION
	case domain.ErrorMaxStructuredOutputRetries:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES
	case domain.Refusal:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_REFUSAL
	default:
		return genproto.TerminationSubtype_TERMINATION_SUBTYPE_UNSPECIFIED
	}
}

// ---- Message (wire → llm) ---------------------------------------------------

// fromGenMessage maps a wire [genproto.Message] to an [llm.Message]. A nil
// message yields the zero value (no content), which the loop treats as "no fresh
// user turn" (a pure resume). It is the inverse of the modelgw adapter's
// toMessage but lives here because this is the client-edge boundary.
func fromGenMessage(m *genproto.Message) llm.Message {
	if m == nil {
		return llm.Message{}
	}
	out := llm.Message{Role: fromGenRole(m.GetRole())}
	for _, cp := range m.GetContent() {
		out.Content = append(out.Content, fromGenContentPart(cp))
	}
	return out
}

// fromGenRole maps the wire Role enum to [llm.Role].
func fromGenRole(r genproto.Role) llm.Role {
	switch r {
	case genproto.Role_ROLE_USER:
		return llm.RoleUser
	case genproto.Role_ROLE_ASSISTANT:
		return llm.RoleAssistant
	case genproto.Role_ROLE_TOOL:
		return llm.RoleTool
	default:
		// An unspecified role on an inbound user turn is treated as user.
		return llm.RoleUser
	}
}

// fromGenContentPart maps a wire [genproto.ContentPart] to an [llm.ContentPart].
func fromGenContentPart(cp *genproto.ContentPart) llm.ContentPart {
	if cp == nil {
		return llm.ContentPart{}
	}
	switch {
	case cp.GetText() != nil:
		return llm.ContentPart{Text: &llm.TextPart{Text: cp.GetText().GetText()}}
	case cp.GetThinking() != nil:
		return llm.ContentPart{Thinking: &llm.ThinkingPart{
			Text:      cp.GetThinking().GetText(),
			Signature: cp.GetThinking().GetSignature(),
		}}
	case cp.GetImage() != nil:
		img := cp.GetImage()
		return llm.ContentPart{Image: &llm.ImagePart{
			MediaType: img.GetMediaType(),
			Data:      img.GetData(),
			URL:       img.GetUrl(),
			FileRef:   img.GetFileRef(),
		}}
	case cp.GetToolCall() != nil:
		tc := cp.GetToolCall()
		return llm.ContentPart{ToolCall: &llm.ToolCall{
			ID:   tc.GetId(),
			Name: tc.GetName(),
			Args: parseArgsJSON(tc.GetArgsJson()),
		}}
	case cp.GetToolResult() != nil:
		tr := cp.GetToolResult()
		return llm.ContentPart{ToolResult: &llm.ToolResult{
			CallID:  tr.GetCallId(),
			Content: tr.GetContent(),
			IsError: tr.GetIsError(),
		}}
	default:
		return llm.ContentPart{}
	}
}

// parseArgsJSON unmarshals a tool-call args JSON object string into a map,
// returning nil on empty input or a parse failure (the loop tolerates nil args).
func parseArgsJSON(s string) map[string]any {
	if s == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return m
}

// ---- Event envelope → RunEvent frame ----------------------------------------

// envelopeToFrame maps a committed [domain.EventEnvelope] to the client-facing
// [genproto.RunEvent] frame to deliver on the Run stream, or returns ok=false
// when the event type carries no client-visible payload (internal bookkeeping
// like TurnStarted/PermissionDecided is folded into the log but not streamed as
// its own frame). Every emitted frame carries the envelope's seq so the client
// can resume from it (FR-API-01).
//
// The terminal RunResult frame is NOT produced here — it is synthesized from the
// loop's RunResult by the relay so it can carry the assembled final text; this
// function maps only the incremental frames (assistant text/thinking, tool
// progress, approval requests).
func envelopeToFrame(env domain.EventEnvelope) (*genproto.RunEvent, bool) {
	switch ev := env.Event.(type) {
	case domain.AssistantMessage:
		// The assembled assistant message carries the turn's text exactly once,
		// keyed by this seq, so it is the single authoritative, deduplicated text
		// frame for the turn (a reattaching client gets it from the same seq).
		// Tool-call-only turns carry no text and produce no frame (the tool
		// round-trip continues). AssistantMessageDelta checkpoints are NOT
		// re-emitted here — they are crash-recovery checkpoints, not delivery
		// frames — so the client never sees duplicated text.
		if txt := assistantText(ev.Message); txt != "" {
			return &genproto.RunEvent{
				Seq: env.Seq,
				Payload: &genproto.RunEvent_TextDelta{
					TextDelta: &genproto.TextDelta{Text: txt},
				},
			}, true
		}
		return nil, false
	case domain.ToolResult:
		// A tool result is surfaced as tool progress so the client sees tool
		// activity on the resumable stream (the authoritative result is folded
		// into the log; this is the client-visible projection).
		return &genproto.RunEvent{
			Seq: env.Seq,
			Payload: &genproto.RunEvent_ToolProgress{
				ToolProgress: &genproto.ToolProgress{Message: ev.Result},
			},
		}, true
	default:
		return nil, false
	}
}

// assistantText concatenates the text parts of an assistant message (ignoring
// thinking/tool-call parts), used to surface a text-only turn's text on the
// resumable stream.
func assistantText(m llm.Message) string {
	var s string
	for _, cp := range m.Content {
		if cp.Text != nil {
			s += cp.Text.Text
		}
	}
	return s
}

// toGenApprovalFrame builds an ApprovalRequest RunEvent frame from an
// [app.ApprovalRequest] mirror carried on the relay channel, at the given seq.
func toGenApprovalFrame(seq int64, callID, toolName, reason string, args map[string]any) *genproto.RunEvent {
	argsJSON := ""
	if len(args) > 0 {
		if b, err := json.Marshal(args); err == nil {
			argsJSON = string(b)
		}
	}
	return &genproto.RunEvent{
		Seq: seq,
		Payload: &genproto.RunEvent_ApprovalRequest{
			ApprovalRequest: &genproto.ApprovalRequest{
				CallId:   callID,
				ToolName: toolName,
				ArgsJson: argsJSON,
				Reason:   reason,
			},
		},
	}
}
