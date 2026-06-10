// Package domain tests are INTERNAL (package domain, not domain_test) so the
// sealed Event interface's unexported isEvent marker is invocable; that keeps
// the type-tag table able to prove every payload satisfies the sealed contract.
// The package stays dependency-light: stdlib only (no testify), matching the
// domain's no-dependency posture.
package domain

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// eventCases enumerates EVERY event payload kind with a representative
// non-zero value. The table is the single source for the tag-consistency,
// seal, and JSON round-trip checks below; the exhaustiveness test asserts it
// covers the full closed EventType set so adding an event kind without
// extending this table fails loudly.
//
// NOTE: numeric values inside map[string]any fields use float64 because JSON
// numbers decode to float64; otherwise the round-trip comparison would report
// a spurious int/float64 mismatch.
func eventCases() []struct {
	want EventType
	ev   Event
} {
	msg := llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{
		{Text: &llm.TextPart{Text: "hello"}},
	}}
	asstMsg := llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{
		{Thinking: &llm.ThinkingPart{Text: "hmm", Signature: "sig"}},
		{Text: &llm.TextPart{Text: "calling a tool"}},
		{ToolCall: &llm.ToolCall{ID: "c1", Name: "read", Args: map[string]any{"path": "/x", "n": float64(2)}}},
	}}
	usage := llm.Usage{InputTokens: 10, OutputTokens: 5, CacheReadTokens: 3, CacheWriteTokens: 2, ReasoningTokens: 1}

	return []struct {
		want EventType
		ev   Event
	}{
		{EventSessionStarted, SessionStarted{ParentID: "parent-1", ForkedFromSeq: 7, SystemPrompt: "be helpful"}},
		{EventMessageAppended, MessageAppended{Message: msg}},
		{EventTurnStarted, TurnStarted{TurnID: "t-1", Model: "test-model"}},
		{EventAssistantMessageDelta, AssistantMessageDelta{
			TurnID: "t-1", TextSoFar: "partial", UsageSoFar: usage, ProviderRaw: llm.ProviderRaw(`{"cursor":"abc"}`),
		}},
		{EventAssistantMessage, AssistantMessage{
			TurnID: "t-1", Message: asstMsg, StopReason: llm.StopToolUse, RawStopReason: "tool_use",
			Usage: usage, CostUSD: 0.25, ProviderRaw: llm.ProviderRaw(`{"items":[1]}`),
		}},
		{EventToolExecutionStarted, ToolExecutionStarted{CallID: "c1", ToolName: "read", IdempotencyKey: "deadbeef"}},
		{EventToolResult, ToolResult{CallID: "c1", Result: "contents", IsError: true, Truncated: true, BlobRef: "blob-1"}},
		{EventToolResultCleared, ToolResultCleared{ClearedSessionID: "sess-1", ClearedSeq: 42, Reason: "compaction"}},
		{EventTurnAborted, TurnAborted{TurnID: "t-1", Reason: ErrorDuringExecution, UsageSoFar: usage, CostUSD: 0.1}},
		{EventTurnFinished, TurnFinished{TurnID: "t-1", Reason: Success, Usage: usage, CostUSD: 0.5, NumTurns: 3}},
		{EventCompactionPerformed, CompactionPerformed{BeforeTokens: 9000, AfterTokens: 3000, Reason: "window pressure"}},
		{EventPermissionDecided, PermissionDecided{
			CallID: "c1", ToolName: "bash", Decision: PermissionAsk, Resolved: AskAllowed, RuleID: "r-7", Reason: "mutating",
		}},
		{EventWorkspaceReset, WorkspaceReset{Reason: "resume after crash"}},
		{EventBypassModeActivated, BypassModeActivated{
			Principal: "ops@corp", PriorMode: ModeDefault, NewMode: ModeBypass, Reason: "incident drill",
		}},
		{EventMCPToolApprovalRequested, MCPToolApprovalRequested{
			ServerName: "files-mcp", ServerVersion: "1.2.3", ToolName: "fs_read", UntrustedDescription: "reads files (UNTRUSTED)",
		}},
		{EventMCPToolApprovalResolved, MCPToolApprovalResolved{ServerName: "files-mcp", ToolName: "fs_read", Granted: true}},
	}
}

// allEventTypes is the closed set of declared EventType constants; the
// exhaustiveness check below keeps it and the payload table in lockstep.
var allEventTypes = []EventType{
	EventSessionStarted, EventMessageAppended, EventTurnStarted,
	EventAssistantMessageDelta, EventAssistantMessage, EventToolExecutionStarted,
	EventToolResult, EventToolResultCleared, EventTurnAborted, EventTurnFinished,
	EventCompactionPerformed, EventPermissionDecided, EventWorkspaceReset,
	EventBypassModeActivated, EventMCPToolApprovalRequested, EventMCPToolApprovalResolved,
}

// TestEventPayloads_TagSealAndJSONRoundTrip folds every payload kind through
// the three invariants the log relies on:
//
//  1. EventType() matches the declared discriminator (tag and payload cannot
//     drift apart inside an envelope);
//  2. the payload satisfies the SEALED Event interface (isEvent);
//  3. the payload survives a JSON marshal/unmarshal round trip unchanged —
//     the regression guard for the persisted events.payload column.
func TestEventPayloads_TagSealAndJSONRoundTrip(t *testing.T) {
	for _, tc := range eventCases() {
		t.Run(string(tc.want), func(t *testing.T) {
			// (1) Tag consistency.
			if got := tc.ev.EventType(); got != tc.want {
				t.Fatalf("EventType() = %q, want %q", got, tc.want)
			}
			// (2) Sealed-interface marker is callable (compile-time seal plus
			// runtime coverage of the marker method).
			tc.ev.isEvent()

			// (3) JSON round trip into a fresh value of the same concrete type.
			raw, err := json.Marshal(tc.ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			ptr := reflect.New(reflect.TypeOf(tc.ev))
			if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := ptr.Elem().Interface()
			if !reflect.DeepEqual(got, tc.ev) {
				t.Errorf("round trip mismatch:\n got: %#v\nwant: %#v", got, tc.ev)
			}
		})
	}
}

// TestEventCases_CoverEveryEventType asserts the payload table above contains
// exactly one entry per declared EventType constant, so a new event kind
// cannot be added without extending the round-trip battery.
func TestEventCases_CoverEveryEventType(t *testing.T) {
	seen := make(map[EventType]int)
	for _, tc := range eventCases() {
		seen[tc.want]++
	}
	for _, et := range allEventTypes {
		if seen[et] != 1 {
			t.Errorf("event type %q appears %d times in eventCases, want exactly 1", et, seen[et])
		}
		delete(seen, et)
	}
	for et := range seen {
		t.Errorf("eventCases contains %q which is not in the declared EventType set", et)
	}
}

// TestPermissionMode_OrDefault asserts the empty zero value reads as the
// secure default while every named mode passes through unchanged (ADR-0019).
func TestPermissionMode_OrDefault(t *testing.T) {
	cases := []struct {
		in   PermissionMode
		want PermissionMode
	}{
		{"", ModeDefault},
		{ModeDefault, ModeDefault},
		{ModeAcceptEdits, ModeAcceptEdits},
		{ModePlan, ModePlan},
		{ModeBypass, ModeBypass},
	}
	for _, tc := range cases {
		if got := tc.in.OrDefault(); got != tc.want {
			t.Errorf("PermissionMode(%q).OrDefault() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestSession_IsActive asserts only StatusActive sessions accept appends.
func TestSession_IsActive(t *testing.T) {
	cases := []struct {
		status SessionStatus
		want   bool
	}{
		{StatusActive, true},
		{StatusFinished, false},
		{StatusFailed, false},
		{SessionStatus(""), false}, // zero value: not appendable
	}
	for _, tc := range cases {
		s := Session{Status: tc.status}
		if got := s.IsActive(); got != tc.want {
			t.Errorf("Session{Status:%q}.IsActive() = %v, want %v", tc.status, got, tc.want)
		}
	}
}

// TestSession_IsFork asserts fork identity is carried by a non-empty ParentID
// (ForkedFromSeq alone does not make a fork).
func TestSession_IsFork(t *testing.T) {
	if (Session{ParentID: "parent-1", ForkedFromSeq: 5}).IsFork() != true {
		t.Error("session with ParentID must report IsFork=true")
	}
	if (Session{ForkedFromSeq: 5}).IsFork() != false {
		t.Error("session without ParentID must report IsFork=false even with ForkedFromSeq set")
	}
	if (Session{}).IsFork() != false {
		t.Error("zero session must report IsFork=false")
	}
}

// TestTerminationReason_IsError asserts Success is the ONLY non-error subtype;
// Refusal counts as non-success (its own subtype, but still not a clean run).
func TestTerminationReason_IsError(t *testing.T) {
	cases := []struct {
		r    TerminationReason
		want bool
	}{
		{Success, false},
		{ErrorMaxTurns, true},
		{ErrorMaxBudgetUSD, true},
		{ErrorDuringExecution, true},
		{ErrorMaxStructuredOutputRetries, true},
		{Refusal, true},
	}
	for _, tc := range cases {
		if got := tc.r.IsError(); got != tc.want {
			t.Errorf("TerminationReason(%q).IsError() = %v, want %v", tc.r, got, tc.want)
		}
	}
}
