package agentctx_test

// Package agentctx_test verifies the context manager (T-LOOP-02): token
// accounting, threshold-triggered compaction, tool-result clearing, and
// cache-prefix marking, against the FRs:
//
//   - FR-CTX-01 — a fake TokenCounter returning rising values crosses the
//     threshold, triggering exactly one CompactionPerformed and a reduced next
//     window.
//   - FR-CTX-02 — after a ToolResultCleared the built window renders a STUB for
//     that result while the full result stays in the log; clearing a
//     non-ToolResult is FAILED_PRECONDITION; double-clear is a no-op.
//   - FR-CTX-03 — the cache-prefix builder marks only stable content (system
//     prompt + tool defs), never session history, and two tenants never share a
//     private prefix.
//
// Every test uses hand-built []domain.EventEnvelope slices and a fake
// TokenCounter — no database, no gRPC, no real provider.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agentctx"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const (
	testSession = "sess-1"
	testTenant  = "tenant-A"
)

// env builds an EventEnvelope for the default test session/tenant at seq.
func env(seq int64, evt domain.Event) domain.EventEnvelope {
	return domain.EventEnvelope{
		Type:      evt.EventType(),
		Seq:       seq,
		SessionID: testSession,
		TenantID:  testTenant,
		RequestID: "req-0",
		Event:     evt,
	}
}

// userMsg is a normalized user message content part.
func userMsg(text string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}

// assistantText is a normalized assistant text message.
func assistantText(text string) llm.Message {
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}

// assistantToolCall is an assistant message requesting a tool call.
func assistantToolCall(callID, name string) llm.Message {
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{
		{ToolCall: &llm.ToolCall{ID: callID, Name: name, Args: map[string]any{}}},
	}}
}

// fakeTokenCounter is a deterministic [agentctx.TokenCounter] for tests. It
// returns scripted counts in queue order; when exhausted it returns the last
// scripted value (so steady-state Count calls are stable). An optional error is
// returned on the first call when set.
type fakeTokenCounter struct {
	counts []int
	idx    int
	err    error
	calls  int
}

func newFakeTokenCounter(counts ...int) *fakeTokenCounter {
	return &fakeTokenCounter{counts: counts}
}

func (f *fakeTokenCounter) Count(_ context.Context, _ string, _ []llm.Message, _ []llm.ToolDef) (int, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	if len(f.counts) == 0 {
		return 0, nil
	}
	if f.idx >= len(f.counts) {
		return f.counts[len(f.counts)-1], nil
	}
	c := f.counts[f.idx]
	f.idx++
	return c, nil
}

// ---------------------------------------------------------------------------
// BuildWindow — basic folding
// ---------------------------------------------------------------------------

func TestBuildWindow_FoldsMessagesInOrder(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "you are helpful"}),
		env(2, domain.MessageAppended{Message: userMsg("hello")}),
		env(3, domain.TurnStarted{TurnID: "t1", Model: "claude-3-5-sonnet-20241022"}),
		env(4, domain.AssistantMessage{TurnID: "t1", Message: assistantText("hi there"), StopReason: llm.StopEnd}),
	}

	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	require.Len(t, win.Messages, 2, "user + assistant message expected")
	assert.Equal(t, llm.RoleUser, win.Messages[0].Role)
	assert.Equal(t, llm.RoleAssistant, win.Messages[1].Role)
	assert.Equal(t, "you are helpful", win.System, "system prompt lifted from SessionStarted")
}

func TestBuildWindow_RendersToolResultMessage(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.MessageAppended{Message: userMsg("run a tool")}),
		env(3, domain.AssistantMessage{TurnID: "t1", Message: assistantToolCall("call-1", "read"), StopReason: llm.StopToolUse}),
		env(4, domain.ToolResult{CallID: "call-1", Result: "file contents here", IsError: false}),
	}

	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	// Find the tool-result content part.
	var found *llm.ToolResult
	for _, m := range win.Messages {
		for _, p := range m.Content {
			if p.ToolResult != nil && p.ToolResult.CallID == "call-1" {
				found = p.ToolResult
			}
		}
	}
	require.NotNil(t, found, "expected a tool result for call-1 in the window")
	assert.Equal(t, "file contents here", found.Content)
}

// ---------------------------------------------------------------------------
// FR-CTX-02 AC-1 — after a ToolResultCleared the window renders a STUB while
// the log is unchanged.
// ---------------------------------------------------------------------------

func TestBuildWindow_ClearedToolResultRendersStub(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.MessageAppended{Message: userMsg("run a tool")}),
		env(3, domain.AssistantMessage{TurnID: "t1", Message: assistantToolCall("call-1", "read"), StopReason: llm.StopToolUse}),
		env(4, domain.ToolResult{CallID: "call-1", Result: "a very large file output", IsError: false}),
		// The context manager cleared the result at seq=4 to reclaim tokens.
		env(5, domain.ToolResultCleared{ClearedSessionID: testSession, ClearedSeq: 4, Reason: "context reclamation"}),
	}

	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	var found *llm.ToolResult
	for _, m := range win.Messages {
		for _, p := range m.Content {
			if p.ToolResult != nil && p.ToolResult.CallID == "call-1" {
				found = p.ToolResult
			}
		}
	}
	require.NotNil(t, found, "the cleared tool result must still appear (as a stub), not vanish")
	assert.NotEqual(t, "a very large file output", found.Content, "the full content must NOT be rendered after clearing")
	assert.Contains(t, strings.ToLower(found.Content), "cleared", "the stub should indicate the result was cleared")

	// The log itself is unchanged: the original ToolResult event still carries
	// the full content (the test holds a reference to the same slice).
	orig, ok := events[3].Event.(domain.ToolResult)
	require.True(t, ok)
	assert.Equal(t, "a very large file output", orig.Result, "the event log must be left intact")
}

func TestBuildWindow_StableStubAcrossDoubleClear(t *testing.T) {
	// Double-clear (idempotent) must not double-render or change the stub.
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.AssistantMessage{TurnID: "t1", Message: assistantToolCall("call-1", "read"), StopReason: llm.StopToolUse}),
		env(3, domain.ToolResult{CallID: "call-1", Result: "big output", IsError: false}),
		env(4, domain.ToolResultCleared{ClearedSessionID: testSession, ClearedSeq: 3, Reason: "first"}),
		env(5, domain.ToolResultCleared{ClearedSessionID: testSession, ClearedSeq: 3, Reason: "second (no-op)"}),
	}

	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	count := 0
	for _, m := range win.Messages {
		for _, p := range m.Content {
			if p.ToolResult != nil && p.ToolResult.CallID == "call-1" {
				count++
			}
		}
	}
	assert.Equal(t, 1, count, "a double-cleared result must render exactly one stub")
}

// ---------------------------------------------------------------------------
// FR-CTX-02 AC-2 — clearing a non-ToolResult is FAILED_PRECONDITION; clearing
// twice is a no-op.
// ---------------------------------------------------------------------------

func TestValidateClear_RejectsNonToolResult(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.MessageAppended{Message: userMsg("hello")}), // seq=2 is NOT a ToolResult
		env(3, domain.AssistantMessage{TurnID: "t1", Message: assistantText("hi"), StopReason: llm.StopEnd}),
	}

	err := agentctx.ValidateClear(events, testSession, 2)
	require.Error(t, err, "clearing a non-ToolResult must be rejected")
	assert.ErrorIs(t, err, agentctx.ErrFailedPrecondition)
}

func TestValidateClear_RejectsMissingTarget(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.ToolResult{CallID: "call-1", Result: "x"}),
	}

	// No event at seq=99.
	err := agentctx.ValidateClear(events, testSession, 99)
	require.Error(t, err)
	assert.ErrorIs(t, err, agentctx.ErrFailedPrecondition)
}

func TestValidateClear_WrongSessionIsRejected(t *testing.T) {
	// The reference is a (session_id, seq) pair; a seq that exists but under a
	// different session must not match (fork-safety, architecture §6.5).
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.ToolResult{CallID: "call-1", Result: "x"}),
	}

	err := agentctx.ValidateClear(events, "some-other-session", 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, agentctx.ErrFailedPrecondition)
}

func TestValidateClear_AcceptsFirstClear(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.ToolResult{CallID: "call-1", Result: "x"}),
	}

	err := agentctx.ValidateClear(events, testSession, 2)
	require.NoError(t, err, "clearing an existing, uncleared ToolResult must be accepted")
}

func TestValidateClear_DoubleClearIsNoOp(t *testing.T) {
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.ToolResult{CallID: "call-1", Result: "x"}),
		env(3, domain.ToolResultCleared{ClearedSessionID: testSession, ClearedSeq: 2, Reason: "first"}),
	}

	// Validating a second clear of the already-cleared result must NOT error —
	// it is a no-op (idempotent), distinct from a FAILED_PRECONDITION.
	err := agentctx.ValidateClear(events, testSession, 2)
	require.NoError(t, err, "double-clear is an idempotent no-op, not an error")
	assert.True(t, errors.Is(agentctx.ErrAlreadyCleared, agentctx.ErrAlreadyCleared)) // sentinel exists

	// And the caller can detect the no-op explicitly.
	cleared, err := agentctx.IsCleared(events, testSession, 2)
	require.NoError(t, err)
	assert.True(t, cleared, "the result is reported as already cleared")
}

// ---------------------------------------------------------------------------
// FR-CTX-01 AC-1 — a fake TokenCounter crossing the threshold triggers exactly
// one CompactionPerformed and a reduced next window.
// ---------------------------------------------------------------------------

func compactionEvents() []domain.EventEnvelope {
	return []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.MessageAppended{Message: userMsg("turn one")}),
		env(3, domain.AssistantMessage{TurnID: "t1", Message: assistantText("answer one"), StopReason: llm.StopEnd}),
		env(4, domain.MessageAppended{Message: userMsg("turn two")}),
		env(5, domain.AssistantMessage{TurnID: "t2", Message: assistantText("answer two"), StopReason: llm.StopEnd}),
		env(6, domain.MessageAppended{Message: userMsg("turn three")}),
		env(7, domain.AssistantMessage{TurnID: "t3", Message: assistantText("answer three"), StopReason: llm.StopEnd}),
	}
}

func TestManager_PlanCompaction_TriggersWhenOverThreshold(t *testing.T) {
	events := compactionEvents()

	// Counter returns a value above the threshold (current window), then a lower
	// value for the projected post-summary window: compaction is needed and
	// reclaims tokens.
	tc := newFakeTokenCounter(9000, 1200)
	mgr := agentctx.NewManager(tc, agentctx.Config{
		Model:              "claude-3-5-sonnet-20241022",
		MaxContextTokens:   10000,
		CompactionFraction: 0.8, // threshold = 8000
	})

	plan, err := mgr.PlanCompaction(context.Background(), events)
	require.NoError(t, err)

	require.True(t, plan.ShouldCompact, "9000 > 8000 threshold must trigger compaction")
	require.NotNil(t, plan.Event, "a CompactionPerformed payload must be produced")
	assert.Equal(t, 9000, plan.Event.BeforeTokens)
	assert.Less(t, plan.Event.AfterTokens, plan.Event.BeforeTokens, "AfterTokens must be lower than BeforeTokens")
	assert.NotEmpty(t, plan.Event.Reason)
}

func TestManager_PlanCompaction_NoTriggerWhenUnderThreshold(t *testing.T) {
	events := compactionEvents()

	tc := newFakeTokenCounter(5000) // below the 8000 threshold
	mgr := agentctx.NewManager(tc, agentctx.Config{
		Model:              "claude-3-5-sonnet-20241022",
		MaxContextTokens:   10000,
		CompactionFraction: 0.8,
	})

	plan, err := mgr.PlanCompaction(context.Background(), events)
	require.NoError(t, err)

	assert.False(t, plan.ShouldCompact, "5000 < 8000 must NOT trigger compaction")
	assert.Nil(t, plan.Event, "no CompactionPerformed when under threshold")
}

func TestManager_Compaction_EmitsExactlyOneEventAndReducesWindow(t *testing.T) {
	events := compactionEvents()

	// First Count (pre-compaction) is over threshold; the window after applying
	// the boundary is re-counted lower.
	tc := newFakeTokenCounter(9000, 2000)
	mgr := agentctx.NewManager(tc, agentctx.Config{
		Model:              "claude-3-5-sonnet-20241022",
		MaxContextTokens:   10000,
		CompactionFraction: 0.8,
	})

	plan, err := mgr.PlanCompaction(context.Background(), events)
	require.NoError(t, err)
	require.True(t, plan.ShouldCompact)

	// Exactly one CompactionPerformed event is produced by a single plan.
	require.NotNil(t, plan.Event)

	// Build the NEXT window from the log plus the just-emitted boundary at the
	// next seq, and assert it is reduced (fewer model-visible messages than the
	// full history).
	full, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	withBoundary := append(events, env(8, *plan.Event))
	reduced, err := agentctx.BuildWindow(withBoundary, agentctx.WindowOptions{})
	require.NoError(t, err)

	assert.Less(t, len(reduced.Messages), len(full.Messages),
		"the window after a CompactionPerformed boundary must be reduced")
}

func TestBuildWindow_CompactionReinitiatesFromSummary(t *testing.T) {
	// After a CompactionPerformed at seq=6, history before it is replaced by a
	// single summary message, and only messages after the boundary are rendered
	// live (architecture §6.6 "reinitiating from a summary").
	events := []domain.EventEnvelope{
		env(1, domain.SessionStarted{SystemPrompt: "sys"}),
		env(2, domain.MessageAppended{Message: userMsg("old turn one")}),
		env(3, domain.AssistantMessage{TurnID: "t1", Message: assistantText("old answer one"), StopReason: llm.StopEnd}),
		env(4, domain.MessageAppended{Message: userMsg("old turn two")}),
		env(5, domain.AssistantMessage{TurnID: "t2", Message: assistantText("old answer two"), StopReason: llm.StopEnd}),
		env(6, domain.CompactionPerformed{BeforeTokens: 9000, AfterTokens: 1500, Reason: "approaching context window"}),
		env(7, domain.MessageAppended{Message: userMsg("fresh turn after compaction")}),
	}

	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	require.NoError(t, err)

	// The pre-compaction live messages ("old turn one"/"old answer one"/...) must
	// NOT appear verbatim; the post-compaction message must appear.
	var rendered []string
	for _, m := range win.Messages {
		for _, p := range m.Content {
			if p.Text != nil {
				rendered = append(rendered, p.Text.Text)
			}
		}
	}
	joined := strings.Join(rendered, "|")
	assert.NotContains(t, joined, "old answer one", "pre-compaction history must be summarized, not rendered verbatim")
	assert.Contains(t, joined, "fresh turn after compaction", "post-compaction messages render live")
	assert.Less(t, len(win.Messages), 5, "window is reduced after compaction")
}

// ---------------------------------------------------------------------------
// FR-CTX-03 AC-1/AC-2 — cache-prefix builder marks only stable content
// (system prompt + tool defs), never session history, and two tenants never
// share a private prefix.
// ---------------------------------------------------------------------------

func toolDefs() []llm.ToolDef {
	return []llm.ToolDef{
		{Name: "read", Description: "read a file", JSONSchema: []byte(`{"type":"object"}`)},
		{Name: "bash", Description: "run a command", JSONSchema: []byte(`{"type":"object"}`)},
	}
}

func TestCachePrefix_MarksSystemAndToolsCacheable(t *testing.T) {
	prefix := agentctx.BuildCachePrefix(agentctx.CacheInput{
		TenantID: testTenant,
		System:   "you are a helpful coding agent",
		Tools:    toolDefs(),
	})

	require.True(t, prefix.Cacheable, "system prompt + tool defs are stable and cacheable")
	assert.NotEmpty(t, prefix.CacheKey, "a non-empty cache key is produced for cacheable content")
	// The prefix covers exactly the stable region: the system prompt and the
	// tool definitions.
	assert.Equal(t, "you are a helpful coding agent", prefix.System)
	assert.Len(t, prefix.Tools, 2)
}

func TestCachePrefix_NeverMarksSessionHistory(t *testing.T) {
	// Whatever messages exist in the session, the cache prefix must cover only
	// the stable region — it must not include conversation history.
	in := agentctx.CacheInput{
		TenantID: testTenant,
		System:   "sys",
		Tools:    toolDefs(),
		// Even if a caller passes history, it must be excluded from the prefix.
		History: []llm.Message{userMsg("private user secret data")},
	}
	prefix := agentctx.BuildCachePrefix(in)

	// The cache boundary (number of leading messages marked cacheable) must
	// cover system + tools only, never any history message.
	assert.Zero(t, prefix.HistoryMessagesCached, "no session-history message may be inside the cached prefix")
}

func TestCachePrefix_TenantsNeverShareAPrivatePrefix(t *testing.T) {
	// Two tenants with IDENTICAL stable content still get distinct cache keys,
	// so a cross-tenant cache hit (or hit-latency timing oracle) is impossible
	// (architecture §8.10).
	a := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: "tenant-A", System: "sys", Tools: toolDefs()})
	b := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: "tenant-B", System: "sys", Tools: toolDefs()})

	require.True(t, a.Cacheable)
	require.True(t, b.Cacheable)
	assert.NotEqual(t, a.CacheKey, b.CacheKey, "two tenants must never share a cache key for the same stable content")
}

func TestCachePrefix_SameTenantSameContentIsStable(t *testing.T) {
	// The same tenant with the same stable content gets the same key on repeat
	// builds, so the provider prompt cache actually hits within a tenant.
	a := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: testTenant, System: "sys", Tools: toolDefs()})
	b := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: testTenant, System: "sys", Tools: toolDefs()})
	assert.Equal(t, a.CacheKey, b.CacheKey, "stable content yields a stable key within a tenant")
}

func TestCachePrefix_KeyChangesWhenStableContentChanges(t *testing.T) {
	a := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: testTenant, System: "sys one", Tools: toolDefs()})
	b := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: testTenant, System: "sys two", Tools: toolDefs()})
	assert.NotEqual(t, a.CacheKey, b.CacheKey, "a different system prompt must change the cache key")
}

func TestCachePrefix_NoStableContentIsNotCacheable(t *testing.T) {
	// With no system prompt and no tools there is nothing stable to cache.
	prefix := agentctx.BuildCachePrefix(agentctx.CacheInput{TenantID: testTenant})
	assert.False(t, prefix.Cacheable, "an empty stable region is not cacheable")
	assert.Empty(t, prefix.CacheKey)
}
