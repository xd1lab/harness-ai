package grpc

// TDD (red) tests for Feature M — Event-log read + time-travel replay API.
//
// These tests are authored BEFORE the implementation and are EXPECTED to fail
// to compile / fail at run time until the feature lands. They pin the resolved
// decisions in C:/Users/123/Documents/harness-wave2-build/event-read/DECISIONS.md:
//
//   - Carrier: two ADDITIVE gRPC RPCs on OrchestratorService —
//     ListSessionEvents + GetStateAtSeq — reusing the SAME authorizeTenant /
//     authorizeSession ownership path (zero new auth). The REST + MCP facades are
//     thin shells over these same *Server methods (covered separately).
//   - Pagination: seq-cursor keyset (after_seq + page_size); default 100, hard
//     cap 1000; response carries next_after_seq + has_more.
//   - Redaction: descriptor-by-default (include_payload defaults false). ALWAYS
//     omitted even when include_payload=true: provider_raw and
//     SessionStarted.SystemPrompt. AssistantMessageDelta is NEVER exposed (crash
//     checkpoint, not a delivery frame). ToolResult large output returns only a
//     blob descriptor (has_blob/media_type/size_bytes). Text fields truncated to
//     a cap with a redacted flag. MCPToolApprovalRequested.UntrustedDescription
//     flagged untrusted.
//   - Time-travel: GetStateAtSeq reconstructs the folded control/billing
//     projection at at_seq via Load-then-fold (NEVER Fork — no new session row),
//     reusing the common.proto Session shape. at_seq<=0 -> empty; at_seq>=head ->
//     capped at head.
//
// The tests reference symbols that do NOT yet exist (genproto.ListSessionEvents*,
// genproto.GetStateAtSeq*, genproto.EventDescriptor, Server.ListSessionEvents,
// Server.GetStateAtSeq, EventStore.LoadRange/LoadUpTo). Their absence is the red
// proof of test-first authoring.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Test fake extensions — the read-side superset methods.
//
// The Server's read RPCs consume two NEW read-only methods on the EventStore
// consumer-superset (NOT on the frozen app.EventLogPort): LoadRange (keyset
// page) and LoadUpTo (upper-bounded fold window). The fake implements them over
// its in-memory event map so the server tests exercise the real mapping/
// redaction/fold without Postgres. (When the EventStore interface gains these
// methods, the existing `_ EventStore = (*tailingEventLog)(nil)` assertion in
// server_test.go keeps the fake honest.)
// ---------------------------------------------------------------------------

// LoadRange returns the events with seq strictly greater than afterSeq, oldest
// first, capped at limit (a keyset page). It mirrors the store's
// `WHERE session_id=$1 AND seq > $2 ORDER BY seq LIMIT $3`.
func (l *tailingEventLog) LoadRange(_ context.Context, sessionID string, afterSeq int64, limit int) ([]domain.EventEnvelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range l.events[sessionID] {
		if e.Seq > afterSeq {
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// LoadUpTo returns the events with seq <= atSeq, oldest first — the bounded
// fold window for at-seq reconstruction. It mirrors a read-only
// `WHERE session_id=$1 AND seq <= $2 ORDER BY seq`.
func (l *tailingEventLog) LoadUpTo(_ context.Context, sessionID string, atSeq int64) ([]domain.EventEnvelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range l.events[sessionID] {
		if e.Seq <= atSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// appendEvent is a tiny helper to append one typed event to the fake under a
// fresh request id, so a read test can seed a rich event stream.
func appendEvent(t *testing.T, log *tailingEventLog, sessionID string, actor domain.Actor, ev domain.Event) domain.EventEnvelope {
	t.Helper()
	envs, err := log.Append(context.Background(), sessionID, 0, 0, "req-"+string(ev.EventType()), app.AppendInput{Event: ev, Actor: actor})
	require.NoError(t, err)
	require.Len(t, envs, 1)
	return envs[0]
}

// seedRichStream creates a session owned by tenant and appends a representative
// spread of event kinds (including the sensitive ones) so the redaction and
// pagination assertions have material to work on. It returns the head seq.
func seedRichStream(t *testing.T, log *tailingEventLog, sessionID, tenant string) int64 {
	t.Helper()
	log.seed(sessionID, tenant) // SessionStarted at seq 1 (empty SystemPrompt)

	// seq 2: a user message.
	appendEvent(t, log, sessionID, domain.ActorUser, domain.MessageAppended{
		Message: llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "please run the build"}}}},
	})
	// seq 3: a turn start (internal bookkeeping).
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnStarted{TurnID: "t1", Model: "claude"})
	// seq 4: a streaming crash checkpoint (NEVER exposed) carrying provider_raw.
	appendEvent(t, log, sessionID, domain.ActorAssistant, domain.AssistantMessageDelta{
		TurnID: "t1", TextSoFar: "partial secret", ProviderRaw: llm.ProviderRaw([]byte(`{"cursor":"opaque"}`)),
	})
	// seq 5: the assembled assistant turn carrying provider_raw + usage + cost.
	appendEvent(t, log, sessionID, domain.ActorAssistant, domain.AssistantMessage{
		TurnID:      "t1",
		Message:     llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "running it"}}}},
		StopReason:  llm.StopEnd,
		Usage:       llm.Usage{InputTokens: 10, OutputTokens: 20},
		CostUSD:     0.03,
		ProviderRaw: llm.ProviderRaw([]byte(`{"continuation":"SENSITIVE"}`)),
	})
	// seq 6: a tool result that offloaded a large output to a blob.
	appendEvent(t, log, sessionID, domain.ActorTool, domain.ToolResult{
		CallID: "c1", Result: "descriptor-only", Truncated: true, BlobRef: "sha256:deadbeef",
	})
	// seq 7: a normal turn finish carrying usage/cost/turn-count.
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnFinished{
		TurnID: "t1", Reason: domain.Success, Usage: llm.Usage{InputTokens: 10, OutputTokens: 20}, CostUSD: 0.03, NumTurns: 1,
	})
	log.mu.Lock()
	head := log.heads[sessionID]
	log.mu.Unlock()
	return head
}

// ---------------------------------------------------------------------------
// ListSessionEvents — carrier, pagination, descriptor-by-default redaction.
// ---------------------------------------------------------------------------

// TestListSessionEvents_DescriptorByDefault asserts that with include_payload
// unset (the default) the response carries ONLY EventDescriptors — seq, type,
// actor, schema_version, created_at, blob metadata, summary — and never raw
// payload bytes. It also confirms the AssistantMessageDelta crash checkpoint is
// never present in the listing (it is not a delivery frame).
func TestListSessionEvents_DescriptorByDefault(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-1", "tenant-A")
	require.Equal(t, int64(7), head)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId:  "tenant-A",
		SessionId: "sess-1",
		PageSize:  1000,
	})
	require.NoError(t, err)

	descs := resp.GetEvents()
	require.NotEmpty(t, descs, "the listing returns descriptors")

	// The AssistantMessageDelta (seq 4) must NEVER appear — it is a crash
	// checkpoint, not a delivery frame.
	for _, d := range descs {
		assert.NotEqual(t, string(domain.EventAssistantMessageDelta), d.GetEventType(),
			"AssistantMessageDelta must never be exposed in the read API")
	}

	// Descriptors carry the safe envelope coordinates.
	bySeq := map[int64]*genproto.EventDescriptor{}
	for _, d := range descs {
		assert.Greater(t, d.GetSeq(), int64(0), "every descriptor carries a seq")
		assert.NotEmpty(t, d.GetEventType(), "every descriptor carries an event_type")
		bySeq[d.GetSeq()] = d
	}

	// The blob-bearing ToolResult (seq 6) is returned as a blob DESCRIPTOR: the
	// raw bytes are not inlined; only has_blob + media type + size metadata.
	tr := bySeq[6]
	require.NotNil(t, tr, "the tool-result descriptor is present at seq 6")
	assert.True(t, tr.GetHasBlob(), "a blob-bearing ToolResult descriptor flags has_blob")
}

// TestListSessionEvents_KeysetPagination asserts after_seq + page_size keyset
// paging: a page of size 3 from after_seq=0 returns seqs 2,3,4-skipping... — i.e.
// the first page_size events with seq > after_seq, in seq order, with has_more
// true and next_after_seq equal to the last returned seq; the next page resumes
// strictly after it with no overlap and no gap.
func TestListSessionEvents_KeysetPagination(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-pg", "tenant-A")
	require.Equal(t, int64(7), head)

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	page1, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-pg", AfterSeq: 0, PageSize: 3,
	})
	require.NoError(t, err)
	require.Len(t, page1.GetEvents(), 3, "page_size=3 returns exactly 3 descriptors")
	assert.True(t, page1.GetHasMore(), "more events remain after the first page")
	last1 := page1.GetEvents()[len(page1.GetEvents())-1].GetSeq()
	assert.Equal(t, last1, page1.GetNextAfterSeq(), "next_after_seq is the last seq of the page")

	// Page 2 resumes strictly after next_after_seq.
	page2, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-pg", AfterSeq: page1.GetNextAfterSeq(), PageSize: 3,
	})
	require.NoError(t, err)
	for _, d := range page2.GetEvents() {
		assert.Greater(t, d.GetSeq(), last1, "page 2 has no overlap with page 1 (keyset, strictly greater)")
	}
}

// TestListSessionEvents_PageSizeHardCap asserts an over-limit page_size is capped
// at the hard cap (1000) rather than honored verbatim (a DoS/response-size
// bound). With only 7 events the cap is not the binding constraint here, so the
// assertion is that an absurd page_size does not error and returns all events.
func TestListSessionEvents_PageSizeHardCap(t *testing.T) {
	log := newTailingEventLog()
	seedRichStream(t, log, "sess-cap", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-cap", PageSize: 1_000_000,
	})
	require.NoError(t, err, "an over-limit page_size is capped, not rejected")
	assert.LessOrEqual(t, len(resp.GetEvents()), 1000, "no page exceeds the hard cap of 1000")
	assert.False(t, resp.GetHasMore(), "all 7 events fit in one (capped) page")
}

// TestListSessionEvents_RejectsForeignTenant asserts the read RPC reuses the
// shared ownership path: tenant B may not list tenant A's events.
func TestListSessionEvents_RejectsForeignTenant(t *testing.T) {
	log := newTailingEventLog()
	seedRichStream(t, log, "sess-A", "tenant-A")

	// Authenticated as tenant-B.
	h := devHarness(t, "tenant-B", noopRunner(log), log)
	_, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-B", SessionId: "sess-A",
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"tenant B cannot list tenant A's events (ownership, shared authorizeSession)")
}

// TestListSessionEvents_RejectsUnauthenticated asserts the read RPC requires a
// verified principal in production-auth mode (no silent open read edge).
func TestListSessionEvents_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	seedRichStream(t, log, "sess-1", "tenant-A")
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err),
		"an unauthenticated read is rejected (shared edge auth)")
}

// TestListSessionEvents_OmitsProviderRawAndSystemPrompt asserts that even when a
// client opts INTO payloads (include_payload=true), the two always-omitted
// fields never appear in any descriptor field: the provider_raw continuation
// blob and the SessionStarted.SystemPrompt. We seed a session whose
// SystemPrompt + AssistantMessage.ProviderRaw both carry a sentinel and assert
// the sentinel never leaks into ANY string field of ANY descriptor.
func TestListSessionEvents_OmitsProviderRawAndSystemPrompt(t *testing.T) {
	const secret = "TOP-SECRET-SENTINEL"

	log := newTailingEventLog()
	// Seed a SessionStarted whose SystemPrompt carries the secret.
	log.mu.Lock()
	log.tenants["sess-s"] = "tenant-A"
	log.modes["sess-s"] = domain.ModeDefault
	log.heads["sess-s"] = 1
	log.events["sess-s"] = []domain.EventEnvelope{{
		Type: domain.EventSessionStarted, Seq: 1, SessionID: "sess-s", TenantID: "tenant-A",
		Actor: domain.ActorSystem, Event: domain.SessionStarted{SystemPrompt: secret},
	}}
	log.mu.Unlock()
	// An assistant turn whose ProviderRaw carries the secret.
	appendEvent(t, log, "sess-s", domain.ActorAssistant, domain.AssistantMessage{
		TurnID:      "t1",
		Message:     llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}},
		StopReason:  llm.StopEnd,
		ProviderRaw: llm.ProviderRaw([]byte(`{"k":"` + secret + `"}`)),
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-s", IncludePayload: true, PageSize: 1000,
	})
	require.NoError(t, err)

	for _, d := range resp.GetEvents() {
		for _, field := range []string{d.GetSummary(), d.GetEventType(), d.GetBlobMediaType()} {
			assert.NotContains(t, field, secret,
				"provider_raw and SystemPrompt must never leak, even with include_payload=true")
		}
	}
}

// TestListSessionEvents_TruncatesLongText asserts a very long text field is
// truncated to the ~2 KiB cap in the descriptor summary and the descriptor is
// flagged redacted/truncated.
func TestListSessionEvents_TruncatesLongText(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-long", "tenant-A")
	long := strings.Repeat("A", 8192) // 8 KiB, well over the ~2 KiB cap
	appendEvent(t, log, "sess-long", domain.ActorUser, domain.MessageAppended{
		Message: llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: long}}}},
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-long", IncludePayload: true, PageSize: 1000,
	})
	require.NoError(t, err)

	var msgDesc *genproto.EventDescriptor
	for _, d := range resp.GetEvents() {
		if d.GetEventType() == string(domain.EventMessageAppended) {
			msgDesc = d
		}
	}
	require.NotNil(t, msgDesc, "the MessageAppended descriptor is present")
	assert.Less(t, len(msgDesc.GetSummary()), 4096, "a long text field is truncated to the cap in the summary")
	assert.True(t, msgDesc.GetRedacted(), "a truncated descriptor is flagged redacted")
}

// ---------------------------------------------------------------------------
// GetStateAtSeq — time-travel via Load-then-fold (no new session row).
// ---------------------------------------------------------------------------

// TestGetStateAtSeq_FoldsBillingProjectionAtSeq asserts that reconstructing the
// state at the head seq yields the SAME folded usage/cost/turns as GetSession
// (the at-seq fold is the same fold truncated at seq <= at_seq), and that the
// reported at_seq echoes the requested point.
func TestGetStateAtSeq_FoldsBillingProjectionAtSeq(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-tt", "tenant-A")
	require.Equal(t, int64(7), head)

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	// At head: the projection equals the current GetSession projection.
	atHead, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-A", SessionId: "sess-tt", AtSeq: head,
	})
	require.NoError(t, err)
	require.NotNil(t, atHead.GetSession())
	assert.Equal(t, int64(1), atHead.GetSession().GetNumTurns(), "TurnFinished folded at head -> 1 turn")
	assert.InDelta(t, 0.03, atHead.GetSession().GetTotalCostUsd(), 1e-9, "cost folded at head")
	assert.Equal(t, int64(20), atHead.GetSession().GetTotalUsage().GetOutputTokens(), "usage folded at head")
}

// TestGetStateAtSeq_BeforeTurnFinishHasNoBilledTurn asserts the at-seq fold is
// TRULY truncated: at a seq BEFORE the TurnFinished (seq 7) — e.g. at seq 6 —
// the folded projection shows zero completed turns and zero cost (the
// TurnFinished is not yet in the [1..at_seq] window).
func TestGetStateAtSeq_BeforeTurnFinishHasNoBilledTurn(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-tt2", "tenant-A")
	require.Equal(t, int64(7), head)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	atSix, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-A", SessionId: "sess-tt2", AtSeq: 6,
	})
	require.NoError(t, err)
	require.NotNil(t, atSix.GetSession())
	assert.Equal(t, int64(0), atSix.GetSession().GetNumTurns(), "no TurnFinished folded before seq 7 -> 0 turns")
	assert.InDelta(t, 0.0, atSix.GetSession().GetTotalCostUsd(), 1e-9, "no cost folded before the TurnFinished")
	assert.Equal(t, int64(6), atSix.GetAtSeq(), "the response echoes the reconstructed seq")
}

// TestGetStateAtSeq_CapsAtHead asserts an at_seq beyond head is clamped to head
// (equivalent to the current state), not an error.
func TestGetStateAtSeq_CapsAtHead(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-cap2", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-A", SessionId: "sess-cap2", AtSeq: head + 1000,
	})
	require.NoError(t, err, "an at_seq past head is capped, not rejected")
	assert.Equal(t, head, resp.GetAtSeq(), "at_seq is clamped to head")
}

// TestGetStateAtSeq_DoesNotCreateNewSession is the load-bearing side-effect-free
// assertion: time-travel must use Load-then-fold and NEVER Fork. A successful
// GetStateAtSeq must not have created any new session aggregate row (the fake
// records every CreateSession and every Fork).
func TestGetStateAtSeq_DoesNotCreateNewSession(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-noeff", "tenant-A")

	log.mu.Lock()
	createdBefore := len(log.created)
	sessionsBefore := len(log.tenants)
	log.mu.Unlock()

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-A", SessionId: "sess-noeff", AtSeq: head - 2,
	})
	require.NoError(t, err)

	log.mu.Lock()
	createdAfter := len(log.created)
	sessionsAfter := len(log.tenants)
	log.mu.Unlock()

	assert.Equal(t, createdBefore, createdAfter, "GetStateAtSeq must NOT create a session (no Fork, no CreateSession)")
	assert.Equal(t, sessionsBefore, sessionsAfter, "GetStateAtSeq must NOT add any session row (side-effect-free read)")
}

// TestGetStateAtSeq_RejectsForeignTenant asserts the time-travel RPC reuses the
// shared ownership path: tenant B may not reconstruct tenant A's state.
func TestGetStateAtSeq_RejectsForeignTenant(t *testing.T) {
	log := newTailingEventLog()
	head := seedRichStream(t, log, "sess-A", "tenant-A")

	h := devHarness(t, "tenant-B", noopRunner(log), log)
	_, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-B", SessionId: "sess-A", AtSeq: head,
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"tenant B cannot time-travel tenant A's session (ownership)")
}

// TestListSessionEvents_PlanUpdatedNonRedacted asserts the NEW planning event
// (domain.PlanUpdated, Gap#3) is surfaced in the read-plane as a NON-redacted
// descriptor with a bounded, safe summary — contrast with SessionStarted, which
// is always redacted (the system prompt is withheld). RED until summarizeEvent
// gains a domain.PlanUpdated case. (Gap#3 AC-13.)
func TestListSessionEvents_PlanUpdatedNonRedacted(t *testing.T) {
	log := newTailingEventLog()
	log.mu.Lock()
	log.tenants["sess-plan"] = "tenant-A"
	log.mu.Unlock()
	appendEvent(t, log, "sess-plan", domain.ActorSystem, domain.SessionStarted{SystemPrompt: "secret prompt"})
	appendEvent(t, log, "sess-plan", domain.ActorAssistant, domain.PlanUpdated{
		TurnID: "t-1",
		Items: []domain.PlanItem{
			{Content: "explore the codebase", Status: "completed"},
			{Content: "write the fix", Status: "in_progress"},
			{Content: "add tests", Status: "pending"},
		},
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-plan", PageSize: 1000,
	})
	require.NoError(t, err)

	var planDesc, sessDesc *genproto.EventDescriptor
	for _, d := range resp.GetEvents() {
		switch d.GetEventType() {
		case string(domain.EventPlanUpdated):
			planDesc = d
		case string(domain.EventSessionStarted):
			sessDesc = d
		}
	}
	require.NotNil(t, planDesc, "the PlanUpdated descriptor must be listed")
	assert.Equal(t, string(domain.EventPlanUpdated), planDesc.GetEventType())
	assert.False(t, planDesc.GetRedacted(), "PlanUpdated is non-secret; it must NOT be redacted")
	assert.False(t, planDesc.GetHasBlob(), "PlanUpdated never references a blob")
	assert.NotEmpty(t, planDesc.GetSummary(), "PlanUpdated descriptor carries a bounded summary")

	// Contrast: SessionStarted is always redacted (system prompt withheld).
	require.NotNil(t, sessDesc)
	assert.True(t, sessDesc.GetRedacted(), "SessionStarted stays redacted (sanity contrast)")
}

// TestGetStateAtSeq_PlanUpdatedIgnoredInBilling asserts time-travel includes a
// PlanUpdated event in its window range without disturbing the folded billing
// totals (foldEventTotals must safely ignore it — no panic, no cost/turn change).
// RED until foldEventTotals handles the new sealed event without a default panic.
// (Gap#3 AC-14.)
func TestGetStateAtSeq_PlanUpdatedIgnoredInBilling(t *testing.T) {
	log := newTailingEventLog()
	log.mu.Lock()
	log.tenants["sess-ttp"] = "tenant-A"
	log.mu.Unlock()
	appendEvent(t, log, "sess-ttp", domain.ActorSystem, domain.SessionStarted{SystemPrompt: "sys"})
	appendEvent(t, log, "sess-ttp", domain.ActorAssistant, domain.PlanUpdated{
		TurnID: "t-1", Items: []domain.PlanItem{{Content: "do it", Status: "pending"}},
	})
	appendEvent(t, log, "sess-ttp", domain.ActorSystem, domain.TurnFinished{
		TurnID: "t-1", Reason: domain.Success, Usage: llm.Usage{OutputTokens: 11}, CostUSD: 0.05, NumTurns: 1,
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetStateAtSeq(context.Background(), &genproto.GetStateAtSeqRequest{
		TenantId: "tenant-A", SessionId: "sess-ttp", AtSeq: 3,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetSession())
	assert.Equal(t, int64(1), resp.GetSession().GetNumTurns(), "the TurnFinished still folds to 1 turn")
	assert.InDelta(t, 0.05, resp.GetSession().GetTotalCostUsd(), 1e-9, "PlanUpdated does not change cost")
	assert.Equal(t, int64(11), resp.GetSession().GetTotalUsage().GetOutputTokens(), "PlanUpdated does not change usage")
}

// noopRunner returns a Runner that does nothing (the read RPCs never invoke the
// loop), so a read test never needs to script run behavior.
func noopRunner(log *tailingEventLog) *fakeRunner {
	return &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) {
		return RunOutcome{Reason: domain.Success}, nil
	}}
}
