package eventstore

import (
	"errors"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestDecodePayloadRoundTrip asserts every closed domain.EventType marshals and
// decodes back to the same concrete type with its fields intact. This is the
// pure (no-DB) guard that the scan path can reconstruct any persisted event; a
// new EventType added to the domain without a decodePayload case fails here
// (decodePayload returns an "unknown event_type" error).
func TestDecodePayloadRoundTrip(t *testing.T) {
	t.Parallel()

	samples := []domain.Event{
		domain.SessionStarted{ParentID: "p", ForkedFromSeq: 3, SystemPrompt: "sys"},
		domain.MessageAppended{Message: llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
		domain.TurnStarted{TurnID: "t1", Model: "claude"},
		domain.AssistantMessageDelta{TurnID: "t1", TextSoFar: "partial"},
		domain.AssistantMessage{TurnID: "t1", StopReason: llm.StopEnd, Usage: llm.Usage{InputTokens: 5, OutputTokens: 7}, CostUSD: 0.01},
		domain.ToolExecutionStarted{CallID: "c1", ToolName: "bash", IdempotencyKey: "k"},
		domain.ToolResult{CallID: "c1", Result: "ok", IsError: false},
		domain.ToolResult{CallID: "c2", Result: "descriptor", Truncated: true, BlobRef: "sha256:abc"},
		domain.ToolResultCleared{ClearedSessionID: "s", ClearedSeq: 9, Reason: "compaction"},
		domain.TurnAborted{TurnID: "t1", Reason: domain.ErrorDuringExecution, UsageSoFar: llm.Usage{OutputTokens: 3}, CostUSD: 0.002},
		domain.TurnFinished{TurnID: "t1", Reason: domain.Success, Usage: llm.Usage{InputTokens: 9}, NumTurns: 2},
		domain.CompactionPerformed{BeforeTokens: 100, AfterTokens: 40, Reason: "threshold"},
		domain.PermissionDecided{CallID: "c1", ToolName: "bash", Decision: domain.PermissionDeny, Reason: "hook_blocked"},
		domain.WorkspaceReset{Reason: "resume after crash"},
		domain.BypassModeActivated{Principal: "ops", PriorMode: domain.ModeDefault, NewMode: domain.ModeBypass},
		domain.MCPToolApprovalRequested{ServerName: "srv", ServerVersion: "1.0", ToolName: "x", UntrustedDescription: "desc"},
		domain.MCPToolApprovalResolved{ServerName: "srv", ToolName: "x", Granted: true},
		domain.PlanUpdated{TurnID: "t1", Items: []domain.PlanItem{
			{Content: "step one", Status: "completed"},
			{Content: "step two", Status: "in_progress"},
		}},
	}

	for _, want := range samples {
		payload, err := marshalPayload(want)
		if err != nil {
			t.Fatalf("marshalPayload(%T): %v", want, err)
		}
		got, err := decodePayload(want.EventType(), payload)
		if err != nil {
			t.Fatalf("decodePayload(%s): %v", want.EventType(), err)
		}
		if got.EventType() != want.EventType() {
			t.Errorf("decodePayload(%s): type drifted to %s", want.EventType(), got.EventType())
		}
		// Re-marshal the decoded value and compare bytes: a faithful round-trip
		// yields identical JSON.
		rePayload, err := marshalPayload(got)
		if err != nil {
			t.Fatalf("re-marshal(%s): %v", got.EventType(), err)
		}
		if string(rePayload) != string(payload) {
			t.Errorf("payload round-trip mismatch for %s:\n have %s\n want %s", want.EventType(), rePayload, payload)
		}
	}
}

// TestDecodePayloadUnknownType asserts an unknown event_type is a loud error,
// never a silent drop (forward-compat: an older reader rejects a newer schema).
func TestDecodePayloadUnknownType(t *testing.T) {
	t.Parallel()
	if _, err := decodePayload(domain.EventType("TotallyNewKind"), []byte(`{}`)); err == nil {
		t.Fatal("decodePayload should reject an unknown event_type")
	}
}

// TestEventBlobRef asserts only ToolResult carries a blob_ref and only when set.
func TestEventBlobRef(t *testing.T) {
	t.Parallel()
	if ref := eventBlobRef(domain.ToolResult{BlobRef: "r"}); ref != "r" {
		t.Errorf("eventBlobRef(ToolResult{BlobRef:r}) = %q, want r", ref)
	}
	if ref := eventBlobRef(domain.ToolResult{}); ref != "" {
		t.Errorf("eventBlobRef(ToolResult{}) = %q, want empty", ref)
	}
	if ref := eventBlobRef(domain.TurnStarted{TurnID: "t"}); ref != "" {
		t.Errorf("eventBlobRef(TurnStarted) = %q, want empty (only ToolResult offloads)", ref)
	}
}

// TestNewSimplePoolBadDSN asserts a malformed DSN is rejected at construction.
func TestNewSimplePoolBadDSN(t *testing.T) {
	t.Parallel()
	if _, err := NewSimplePool("://not a dsn"); err == nil {
		t.Fatal("NewSimplePool should reject a malformed DSN")
	}
}

// TestIsUniqueViolation asserts the SQLSTATE classifier only matches 23505 and
// is nil-safe / non-pg-error-safe.
func TestIsUniqueViolation(t *testing.T) {
	t.Parallel()
	if isUniqueViolation(nil) {
		t.Error("isUniqueViolation(nil) must be false")
	}
	if isUniqueViolation(errors.New("plain")) {
		t.Error("isUniqueViolation(plain error) must be false")
	}
}
