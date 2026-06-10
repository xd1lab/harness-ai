// Package grpc tests — pure, table-driven tests for the gen ⇄ domain/llm
// mapping boundary (mapping.go) and the error-mapping / read-side fold helpers
// (server.go). No network or bufconn is needed: every function under test is a
// pure mapping, so each enum is asserted exhaustively, including the
// unknown-value fallbacks (architecture §12.4: the boundary must be total).
package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---- Session / status / usage -------------------------------------------------

func TestToGenStatus_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   domain.SessionStatus
		want genproto.SessionStatus
	}{
		{"active", domain.StatusActive, genproto.SessionStatus_SESSION_STATUS_ACTIVE},
		{"finished", domain.StatusFinished, genproto.SessionStatus_SESSION_STATUS_FINISHED},
		{"failed", domain.StatusFailed, genproto.SessionStatus_SESSION_STATUS_FAILED},
		// The zero value and an unknown string both fall back to UNSPECIFIED so a
		// corrupt row never masquerades as a live status.
		{"zero value", domain.SessionStatus(""), genproto.SessionStatus_SESSION_STATUS_UNSPECIFIED},
		{"unknown", domain.SessionStatus("torn"), genproto.SessionStatus_SESSION_STATUS_UNSPECIFIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toGenStatus(tc.in))
		})
	}
}

func TestToGenSession_FieldMapping(t *testing.T) {
	sess := domain.Session{
		ID:            "sess-9",
		TenantID:      "tenant-Z",
		Status:        domain.StatusFinished,
		Mode:          domain.ModePlan,
		HeadSeq:       42,
		ParentID:      "sess-parent",
		ForkedFromSeq: 7,
	}
	usage := llm.Usage{InputTokens: 1, OutputTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 4, ReasoningTokens: 5}

	got := toGenSession(sess, usage, 1.25, 9)

	assert.Equal(t, "sess-9", got.GetSessionId())
	assert.Equal(t, "tenant-Z", got.GetTenantId())
	assert.Equal(t, genproto.SessionStatus_SESSION_STATUS_FINISHED, got.GetStatus())
	assert.Equal(t, genproto.PermissionMode_PERMISSION_MODE_PLAN, got.GetMode())
	assert.Equal(t, int64(42), got.GetHeadSeq())
	assert.Equal(t, "sess-parent", got.GetParentSessionId())
	assert.Equal(t, int64(7), got.GetForkedFromSeq())
	assert.Equal(t, 1.25, got.GetTotalCostUsd())
	assert.Equal(t, int64(9), got.GetNumTurns())
	u := got.GetTotalUsage()
	require.NotNil(t, u)
	assert.Equal(t, int64(1), u.GetInputTokens())
	assert.Equal(t, int64(2), u.GetOutputTokens())
	assert.Equal(t, int64(3), u.GetCacheReadTokens())
	assert.Equal(t, int64(4), u.GetCacheWriteTokens())
	assert.Equal(t, int64(5), u.GetReasoningTokens())
}

// ---- Permission mode (ADR-0019: explicit three-vocabulary mapping) -------------

func TestFromGenModeDomain_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   genproto.PermissionMode
		want domain.PermissionMode
	}{
		// UNSPECIFIED and DEFAULT both resolve to the secure default.
		{"unspecified", genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED, domain.ModeDefault},
		{"default", genproto.PermissionMode_PERMISSION_MODE_DEFAULT, domain.ModeDefault},
		{"accept_edits", genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, domain.ModeAcceptEdits},
		{"plan", genproto.PermissionMode_PERMISSION_MODE_PLAN, domain.ModePlan},
		{"bypass", genproto.PermissionMode_PERMISSION_MODE_BYPASS, domain.ModeBypass},
		// A future/unknown wire value must fail safe to the most-restrictive mode.
		{"unknown future value", genproto.PermissionMode(99), domain.ModeDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fromGenModeDomain(tc.in))
		})
	}
}

func TestToPolicyMode_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   domain.PermissionMode
		want policy.Mode
	}{
		{"default", domain.ModeDefault, policy.ModeDefault},
		// The two vocabularies deliberately differ here: domain "acceptEdits" vs
		// policy "accept_edits". A cast would silently produce an invalid mode.
		{"accept edits (spelling differs)", domain.ModeAcceptEdits, policy.ModeAcceptEdits},
		{"plan", domain.ModePlan, policy.ModePlan},
		{"bypass", domain.ModeBypass, policy.ModeBypass},
		// Unset (pre-migration row / unset fake) and unknown both resolve secure.
		{"zero value", domain.PermissionMode(""), policy.ModeDefault},
		{"unknown", domain.PermissionMode("yolo"), policy.ModeDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toPolicyMode(tc.in))
		})
	}
}

func TestToGenMode_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   domain.PermissionMode
		want genproto.PermissionMode
	}{
		{"default", domain.ModeDefault, genproto.PermissionMode_PERMISSION_MODE_DEFAULT},
		{"accept edits", domain.ModeAcceptEdits, genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS},
		{"plan", domain.ModePlan, genproto.PermissionMode_PERMISSION_MODE_PLAN},
		{"bypass", domain.ModeBypass, genproto.PermissionMode_PERMISSION_MODE_BYPASS},
		// A pre-mode-column session (empty) reads as "server default applies".
		{"zero value", domain.PermissionMode(""), genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED},
		{"unknown", domain.PermissionMode("corrupt"), genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toGenMode(tc.in))
		})
	}
}

// ---- Termination subtype -------------------------------------------------------

func TestToGenSubtype_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   domain.TerminationReason
		want genproto.TerminationSubtype
	}{
		{"success", domain.Success, genproto.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS},
		{"max turns", domain.ErrorMaxTurns, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_TURNS},
		{"max budget", domain.ErrorMaxBudgetUSD, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_BUDGET_USD},
		{"during execution", domain.ErrorDuringExecution, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION},
		{"structured output retries", domain.ErrorMaxStructuredOutputRetries, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES},
		{"refusal", domain.Refusal, genproto.TerminationSubtype_TERMINATION_SUBTYPE_REFUSAL},
		{"zero value", domain.TerminationReason(""), genproto.TerminationSubtype_TERMINATION_SUBTYPE_UNSPECIFIED},
		{"unknown", domain.TerminationReason("new_reason"), genproto.TerminationSubtype_TERMINATION_SUBTYPE_UNSPECIFIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toGenSubtype(tc.in))
		})
	}
}

// ---- Message (wire → llm) ------------------------------------------------------

func TestFromGenRole_Exhaustive(t *testing.T) {
	cases := []struct {
		name string
		in   genproto.Role
		want llm.Role
	}{
		{"user", genproto.Role_ROLE_USER, llm.RoleUser},
		{"assistant", genproto.Role_ROLE_ASSISTANT, llm.RoleAssistant},
		{"tool", genproto.Role_ROLE_TOOL, llm.RoleTool},
		// An unspecified/unknown role on an inbound user turn is treated as user.
		{"unspecified", genproto.Role_ROLE_UNSPECIFIED, llm.RoleUser},
		{"unknown future value", genproto.Role(99), llm.RoleUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fromGenRole(tc.in))
		})
	}
}

func TestFromGenMessage(t *testing.T) {
	t.Run("nil message is a pure resume (zero value)", func(t *testing.T) {
		got := fromGenMessage(nil)
		assert.Equal(t, llm.Message{}, got)
	})

	t.Run("role and parts map in order", func(t *testing.T) {
		got := fromGenMessage(&genproto.Message{
			Role: genproto.Role_ROLE_USER,
			Content: []*genproto.ContentPart{
				{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "do it"}}},
				{Part: &genproto.ContentPart_ToolResult{ToolResult: &genproto.ToolResult{CallId: "c1", Content: "ok", IsError: true}}},
			},
		})
		assert.Equal(t, llm.RoleUser, got.Role)
		require.Len(t, got.Content, 2)
		require.NotNil(t, got.Content[0].Text)
		assert.Equal(t, "do it", got.Content[0].Text.Text)
		require.NotNil(t, got.Content[1].ToolResult)
		assert.Equal(t, llm.ToolResult{CallID: "c1", Content: "ok", IsError: true}, *got.Content[1].ToolResult)
	})
}

func TestFromGenContentPart_Variants(t *testing.T) {
	t.Run("nil part yields zero value", func(t *testing.T) {
		assert.Equal(t, llm.ContentPart{}, fromGenContentPart(nil))
	})

	t.Run("empty oneof yields zero value", func(t *testing.T) {
		assert.Equal(t, llm.ContentPart{}, fromGenContentPart(&genproto.ContentPart{}))
	})

	t.Run("text", func(t *testing.T) {
		got := fromGenContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "hello"}},
		})
		require.NotNil(t, got.Text)
		assert.Equal(t, "hello", got.Text.Text)
	})

	t.Run("thinking carries text and signature", func(t *testing.T) {
		got := fromGenContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Thinking{Thinking: &genproto.ThinkingPart{Text: "hm", Signature: "sig-1"}},
		})
		require.NotNil(t, got.Thinking)
		assert.Equal(t, llm.ThinkingPart{Text: "hm", Signature: "sig-1"}, *got.Thinking)
	})

	t.Run("image carries all reference forms", func(t *testing.T) {
		got := fromGenContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Image{Image: &genproto.ImagePart{
				MediaType: "image/png", Data: []byte{0x89, 0x50}, Url: "https://x/img.png", FileRef: "file-1",
			}},
		})
		require.NotNil(t, got.Image)
		assert.Equal(t, "image/png", got.Image.MediaType)
		assert.Equal(t, []byte{0x89, 0x50}, got.Image.Data)
		assert.Equal(t, "https://x/img.png", got.Image.URL)
		assert.Equal(t, "file-1", got.Image.FileRef)
	})

	t.Run("tool call parses args JSON", func(t *testing.T) {
		got := fromGenContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_ToolCall{ToolCall: &genproto.ToolCall{
				Id: "c1", Name: "bash", ArgsJson: `{"cmd":"ls","n":2}`,
			}},
		})
		require.NotNil(t, got.ToolCall)
		assert.Equal(t, "c1", got.ToolCall.ID)
		assert.Equal(t, "bash", got.ToolCall.Name)
		assert.Equal(t, map[string]any{"cmd": "ls", "n": float64(2)}, got.ToolCall.Args)
	})

	t.Run("tool result", func(t *testing.T) {
		got := fromGenContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_ToolResult{ToolResult: &genproto.ToolResult{CallId: "c2", Content: "out", IsError: false}},
		})
		require.NotNil(t, got.ToolResult)
		assert.Equal(t, llm.ToolResult{CallID: "c2", Content: "out"}, *got.ToolResult)
	})
}

func TestParseArgsJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want map[string]any
	}{
		{"empty input is nil (loop tolerates nil args)", "", nil},
		{"malformed JSON is nil, not an error", `{"broken`, nil},
		{"non-object JSON is nil", `[1,2,3]`, nil},
		{"JSON null is nil", `null`, nil},
		{"valid object parses", `{"a":1,"b":"x"}`, map[string]any{"a": float64(1), "b": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseArgsJSON(tc.in))
		})
	}
}

// ---- Event envelope → RunEvent frame --------------------------------------------

func TestEnvelopeToFrame(t *testing.T) {
	t.Run("assistant text becomes one TextDelta frame at its seq", func(t *testing.T) {
		env := domain.EventEnvelope{
			Seq: 12,
			Event: domain.AssistantMessage{Message: llm.Message{
				Role: llm.RoleAssistant,
				Content: []llm.ContentPart{
					{Thinking: &llm.ThinkingPart{Text: "ignored"}},
					{Text: &llm.TextPart{Text: "hello "}},
					{Text: &llm.TextPart{Text: "world"}},
				},
			}},
		}
		frame, ok := envelopeToFrame(env)
		require.True(t, ok)
		assert.Equal(t, int64(12), frame.GetSeq(), "the frame must carry the envelope seq for resumable reattach (FR-API-01)")
		require.NotNil(t, frame.GetTextDelta())
		assert.Equal(t, "hello world", frame.GetTextDelta().GetText(), "text parts concatenate; thinking parts are not client-visible text")
	})

	t.Run("tool-call-only assistant turn emits no frame", func(t *testing.T) {
		env := domain.EventEnvelope{
			Seq: 3,
			Event: domain.AssistantMessage{Message: llm.Message{
				Role:    llm.RoleAssistant,
				Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "c1", Name: "bash"}}},
			}},
		}
		_, ok := envelopeToFrame(env)
		assert.False(t, ok, "a tool-call-only turn carries no text; the tool round-trip continues")
	})

	t.Run("tool result becomes a ToolProgress frame", func(t *testing.T) {
		env := domain.EventEnvelope{Seq: 5, Event: domain.ToolResult{CallID: "c1", Result: "42 files"}}
		frame, ok := envelopeToFrame(env)
		require.True(t, ok)
		assert.Equal(t, int64(5), frame.GetSeq())
		require.NotNil(t, frame.GetToolProgress())
		assert.Equal(t, "42 files", frame.GetToolProgress().GetMessage())
	})

	t.Run("internal bookkeeping events emit no frame", func(t *testing.T) {
		for _, ev := range []domain.Event{
			domain.SessionStarted{},
			domain.TurnStarted{TurnID: "t1"},
			domain.TurnFinished{TurnID: "t1", Reason: domain.Success},
		} {
			_, ok := envelopeToFrame(domain.EventEnvelope{Seq: 1, Event: ev})
			assert.False(t, ok, "%T must not produce a client frame", ev)
		}
	})
}

func TestAssistantText(t *testing.T) {
	got := assistantText(llm.Message{Content: []llm.ContentPart{
		{Text: &llm.TextPart{Text: "a"}},
		{ToolCall: &llm.ToolCall{ID: "c"}},
		{Thinking: &llm.ThinkingPart{Text: "x"}},
		{Text: &llm.TextPart{Text: "b"}},
	}})
	assert.Equal(t, "ab", got)
	assert.Empty(t, assistantText(llm.Message{}))
}

func TestToGenApprovalFrame(t *testing.T) {
	t.Run("args marshal into args_json", func(t *testing.T) {
		frame := toGenApprovalFrame(8, "c1", "bash", "mutating", map[string]any{"cmd": "rm"})
		assert.Equal(t, int64(8), frame.GetSeq())
		ar := frame.GetApprovalRequest()
		require.NotNil(t, ar)
		assert.Equal(t, "c1", ar.GetCallId())
		assert.Equal(t, "bash", ar.GetToolName())
		assert.Equal(t, "mutating", ar.GetReason())
		assert.JSONEq(t, `{"cmd":"rm"}`, ar.GetArgsJson())
	})

	t.Run("no args yields empty args_json", func(t *testing.T) {
		frame := toGenApprovalFrame(1, "c2", "read", "why", nil)
		require.NotNil(t, frame.GetApprovalRequest())
		assert.Empty(t, frame.GetApprovalRequest().GetArgsJson())
	})
}

// ---- error mapping (server.go) --------------------------------------------------

func TestMapAppendError(t *testing.T) {
	cases := []struct {
		name     string
		in       error
		wantCode codes.Code
		wantNil  bool
	}{
		{"nil passes through", nil, codes.OK, true},
		{"conflict → FailedPrecondition", app.ConflictError, codes.FailedPrecondition, false},
		// Sentinels are matched with errors.Is, so a wrapped sentinel still maps.
		{"wrapped conflict → FailedPrecondition", fmt.Errorf("append: %w", app.ConflictError), codes.FailedPrecondition, false},
		{"fenced → FailedPrecondition", app.FencedError, codes.FailedPrecondition, false},
		{"not active → FailedPrecondition", app.SessionNotActiveError, codes.FailedPrecondition, false},
		{"anything else → Internal", errors.New("disk on fire"), codes.Internal, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapAppendError(tc.in)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			assert.Equal(t, tc.wantCode, status.Code(got))
		})
	}
}

func TestMapCreateSessionError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		assert.NoError(t, mapCreateSessionError(nil))
	})

	t.Run("typed status from the store passes through unchanged", func(t *testing.T) {
		in := status.Error(codes.PermissionDenied, "rls says no")
		got := mapCreateSessionError(in)
		assert.Same(t, in, got, "an already-typed status must keep its exact wire code")
	})

	t.Run("Unknown-coded status is NOT passed through", func(t *testing.T) {
		got := mapCreateSessionError(status.Error(codes.Unknown, "untyped"))
		assert.Equal(t, codes.Internal, status.Code(got))
	})

	t.Run("duplicate key (23505) → AlreadyExists", func(t *testing.T) {
		got := mapCreateSessionError(&pgconn.PgError{Code: pgUniqueViolation})
		assert.Equal(t, codes.AlreadyExists, status.Code(got))
	})

	t.Run("wrapped duplicate key → AlreadyExists", func(t *testing.T) {
		got := mapCreateSessionError(fmt.Errorf("insert: %w", &pgconn.PgError{Code: pgUniqueViolation}))
		assert.Equal(t, codes.AlreadyExists, status.Code(got))
	})

	t.Run("other pg error → Internal", func(t *testing.T) {
		got := mapCreateSessionError(&pgconn.PgError{Code: "23503"}) // FK violation, not a dup
		assert.Equal(t, codes.Internal, status.Code(got))
	})

	t.Run("bare error → Internal", func(t *testing.T) {
		got := mapCreateSessionError(errors.New("boom"))
		assert.Equal(t, codes.Internal, status.Code(got))
	})
}

func TestMapForkError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		assert.NoError(t, mapForkError(nil))
	})

	t.Run("PermissionDenied status passes through unchanged (ADR-0013)", func(t *testing.T) {
		in := status.Error(codes.PermissionDenied, "cross-tenant fork")
		got := mapForkError(in)
		assert.Same(t, in, got)
	})

	t.Run("other status → Internal", func(t *testing.T) {
		got := mapForkError(status.Error(codes.NotFound, "no parent"))
		assert.Equal(t, codes.Internal, status.Code(got))
	})

	t.Run("bare error → Internal", func(t *testing.T) {
		got := mapForkError(errors.New("boom"))
		assert.Equal(t, codes.Internal, status.Code(got))
	})
}

// ---- usage fold (server.go) -----------------------------------------------------

func TestAddUsage(t *testing.T) {
	a := llm.Usage{InputTokens: 1, OutputTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 4, ReasoningTokens: 5}
	b := llm.Usage{InputTokens: 10, OutputTokens: 20, CacheReadTokens: 30, CacheWriteTokens: 40, ReasoningTokens: 50}
	assert.Equal(t, llm.Usage{InputTokens: 11, OutputTokens: 22, CacheReadTokens: 33, CacheWriteTokens: 44, ReasoningTokens: 55}, addUsage(a, b))
	assert.Equal(t, llm.Usage{}, addUsage(llm.Usage{}, llm.Usage{}))
}

// nopRunner is a Runner for tests that never invoke Run (foldTotals only needs
// the log).
type nopRunner struct{}

func (nopRunner) Run(context.Context, RunSpec) (RunOutcome, error) { return RunOutcome{}, nil }

func TestFoldTotals_SumsTurnFinishedAndAborted(t *testing.T) {
	ctx := context.Background()
	log := newTailingEventLog()
	log.seed("sess-f", "tenant-A")

	// Two finished turns plus an aborted partial turn in between. Costs are exact
	// binary fractions so float equality is deterministic.
	_, err := log.Append(ctx, "sess-f", 0, 0, "r1", app.AppendInput{
		Event: domain.TurnFinished{
			TurnID: "t1", Reason: domain.Success,
			Usage:   llm.Usage{InputTokens: 10, OutputTokens: 5, CacheReadTokens: 1, CacheWriteTokens: 2, ReasoningTokens: 3},
			CostUSD: 0.5, NumTurns: 1,
		},
		Actor: domain.ActorSystem,
	})
	require.NoError(t, err)
	_, err = log.Append(ctx, "sess-f", 0, 0, "r2", app.AppendInput{
		Event: domain.TurnAborted{
			TurnID: "t2", Reason: domain.ErrorDuringExecution,
			UsageSoFar: llm.Usage{InputTokens: 4, OutputTokens: 2},
			CostUSD:    0.25,
		},
		Actor: domain.ActorSystem,
	})
	require.NoError(t, err)
	_, err = log.Append(ctx, "sess-f", 0, 0, "r3", app.AppendInput{
		Event: domain.TurnFinished{
			TurnID: "t3", Reason: domain.Success,
			Usage:   llm.Usage{InputTokens: 6},
			CostUSD: 0.125, NumTurns: 3,
		},
		Actor: domain.ActorSystem,
	})
	require.NoError(t, err)

	srv := NewServer(log, newNotifyingGate(), nopRunner{}, newFakeIDs(), Config{})
	usage, cost, turns := srv.foldTotals(ctx, "sess-f")

	assert.Equal(t, llm.Usage{InputTokens: 20, OutputTokens: 7, CacheReadTokens: 1, CacheWriteTokens: 2, ReasoningTokens: 3}, usage,
		"usage sums TurnFinished.Usage AND TurnAborted.UsageSoFar (aborted turns are still billed)")
	assert.Equal(t, 0.875, cost, "cost sums finished and aborted turns")
	assert.Equal(t, int64(3), turns, "turn count is the LAST TurnFinished's cumulative NumTurns, not a sum")
}

// failingLoadLog wraps the tailing log but fails every Load, exercising
// foldTotals' degraded path: GetSession must report zero totals rather than fail.
type failingLoadLog struct{ *tailingEventLog }

func (failingLoadLog) Load(context.Context, string, int64) ([]domain.EventEnvelope, error) {
	return nil, errors.New("load failed")
}

func TestFoldTotals_LoadErrorReturnsZeros(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-z", "tenant-A")
	srv := NewServer(failingLoadLog{log}, newNotifyingGate(), nopRunner{}, newFakeIDs(), Config{})

	usage, cost, turns := srv.foldTotals(context.Background(), "sess-z")

	assert.Equal(t, llm.Usage{}, usage)
	assert.Zero(t, cost)
	assert.Zero(t, turns)
}
