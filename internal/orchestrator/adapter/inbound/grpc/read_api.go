// SPDX-License-Identifier: Apache-2.0

package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file implements the event-log read + time-travel replay surface (Feature M
// / event-read; ADR-0025): the two additive RPCs ListSessionEvents (redacted,
// keyset-paginated descriptors) and GetStateAtSeq (Load-then-fold reconstruction
// of the control/billing projection at a seq, NEVER Fork). Both reuse the SAME
// ownership path as the rest of the edge (authorizeTenant / authorizeSession, zero
// new auth).

const (
	// defaultEventPageSize is the page size used when a request asks for <= 0.
	defaultEventPageSize = 100
	// maxEventPageSize is the hard cap; a larger request is clamped (never rejected)
	// so a listing cannot become an unbounded scan (DoS/response-size bound).
	maxEventPageSize = 1000
	// summaryCapBytes bounds the descriptor summary so a huge text field cannot be
	// dumped verbatim into the listing (the descriptor is a SAFE view; the full
	// payload is fetched on demand, never inlined here).
	summaryCapBytes = 2048
)

// clampEventPageSize resolves the requested page_size to the effective limit:
// <= 0 -> default, > cap -> cap, else the request.
func clampEventPageSize(req int32) int {
	switch {
	case req <= 0:
		return defaultEventPageSize
	case req > maxEventPageSize:
		return maxEventPageSize
	default:
		return int(req)
	}
}

// ---- ListSessionEvents ------------------------------------------------------

// ListSessionEvents lists an owned session's events as redacted descriptors,
// keyset-paginated on seq (after_seq + page_size), reusing the FULL ownership path
// (authorizeTenant + authorizeSession): a foreign-tenant session is
// PermissionDenied, a missing / RLS-invisible one is NotFound.
//
// Redaction is descriptor-by-default: provider_raw and SessionStarted.SystemPrompt
// are NEVER emitted (even with include_payload=true), AssistantMessageDelta crash
// checkpoints are never exposed (not a delivery frame), a blob-bearing ToolResult
// becomes a blob descriptor (has_blob + metadata, never inlined bytes), and long
// text is truncated into the summary with the redacted flag set.
func (s *Server) ListSessionEvents(ctx context.Context, req *genproto.ListSessionEventsRequest) (*genproto.ListSessionEventsResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeSession(ctx, tenant, req.GetSessionId()); err != nil {
		return nil, err
	}

	limit := clampEventPageSize(req.GetPageSize())
	// Fetch one extra row to decide has_more without a second query. The skipped
	// AssistantMessageDelta checkpoints are dropped AFTER the fetch, so the page may
	// be shorter than limit even when more delivery-frame events remain; has_more is
	// computed from the raw fetch count, which is the conservative, safe signal.
	envs, err := s.log.LoadRange(ctx, req.GetSessionId(), req.GetAfterSeq(), limit+1)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: list events: %v", err)
	}

	hasMore := false
	if len(envs) > limit {
		hasMore = true
		envs = envs[:limit]
	}

	out := make([]*genproto.EventDescriptor, 0, len(envs))
	var lastSeq int64
	for _, env := range envs {
		lastSeq = env.Seq
		// AssistantMessageDelta is a crash checkpoint, not a delivery frame — never
		// exposed in the read API (it carries partial text + provider_raw).
		if env.Type == domain.EventAssistantMessageDelta {
			continue
		}
		out = append(out, toGenEventDescriptor(env, req.GetIncludePayload()))
	}

	return &genproto.ListSessionEventsResponse{
		Events:       out,
		NextAfterSeq: lastSeq,
		HasMore:      hasMore,
	}, nil
}

// toGenEventDescriptor maps a [domain.EventEnvelope] to the safe wire
// [genproto.EventDescriptor]. includePayload only widens the summary (truncated,
// still safe); it NEVER unlocks provider_raw or the system prompt.
func toGenEventDescriptor(env domain.EventEnvelope, includePayload bool) *genproto.EventDescriptor {
	d := &genproto.EventDescriptor{
		Seq:       env.Seq,
		EventType: string(env.Type),
		Actor:     string(env.Actor),
		//nolint:gosec // G115: schema_version is a small positive payload-version counter (defaults to 1), never near int32 max.
		SchemaVersion: int32(env.SchemaVersion),
		RequestId:     env.RequestID,
		CreatedAtMs:   toEpochMs(env.CreatedAt),
	}
	summary, redacted, hasBlob := summarizeEvent(env.Event, includePayload)
	d.Summary = summary
	d.Redacted = redacted
	d.HasBlob = hasBlob
	return d
}

// summarizeEvent produces a SAFE, bounded summary for an event payload and reports
// whether anything was redacted/truncated and whether the event references a blob.
// It NEVER returns provider_raw bytes or a SessionStarted.SystemPrompt, regardless
// of includePayload — those are permanently omitted.
func summarizeEvent(ev domain.Event, includePayload bool) (summary string, redacted, hasBlob bool) {
	switch e := ev.(type) {
	case domain.SessionStarted:
		// The system prompt is permanently omitted; the descriptor only notes the
		// session opened (and that a field was withheld).
		return "session started", true, false
	case domain.MessageAppended:
		if !includePayload {
			return "", false, false
		}
		txt, trunc := truncateText(messageText(e.Message))
		return txt, trunc, false
	case domain.AssistantMessage:
		// provider_raw is permanently omitted; only the assembled text is summarized.
		if !includePayload {
			return "", true, false
		}
		txt, _ := truncateText(messageText(e.Message))
		// Always flag redacted: provider_raw was withheld from this event.
		return txt, true, false
	case domain.ToolResult:
		hasBlob = e.BlobRef != ""
		if !includePayload {
			return "", e.Truncated || hasBlob, hasBlob
		}
		txt, trunc := truncateText(e.Result)
		return txt, trunc || e.Truncated || hasBlob, hasBlob
	case domain.MCPToolApprovalRequested:
		// The untrusted description is flagged, never rendered as an instruction.
		return "MCP tool approval requested (untrusted description withheld)", true, false
	default:
		return "", false, false
	}
}

// truncateText bounds s at summaryCapBytes, returning the (possibly truncated)
// prefix and whether truncation occurred.
func truncateText(s string) (string, bool) {
	if len(s) <= summaryCapBytes {
		return s, false
	}
	return s[:summaryCapBytes], true
}

// messageText concatenates the text parts of a message (ignoring thinking/tool
// parts), the only model-visible text safe to surface in a summary.
func messageText(m llm.Message) string {
	var s string
	for _, cp := range m.Content {
		if cp.Text != nil {
			s += cp.Text.Text
		}
	}
	return s
}

// ---- GetStateAtSeq ----------------------------------------------------------

// GetStateAtSeq reconstructs an owned session's folded control/billing projection
// at at_seq via Load-then-fold over the [1..at_seq] window — NEVER via Fork, so it
// creates no session row and re-bills nothing. It reuses the FULL ownership path
// (authorizeTenant + authorizeSession). at_seq <= 0 yields an empty state; at_seq
// beyond head is clamped to head (the returned at_seq echoes the reconstructed
// point).
func (s *Server) GetStateAtSeq(ctx context.Context, req *genproto.GetStateAtSeqRequest) (*genproto.GetStateAtSeqResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	sess, err := s.authorizeSession(ctx, tenant, req.GetSessionId())
	if err != nil {
		return nil, err
	}
	if sess.TenantID == "" {
		sess.TenantID = tenant
	}

	// Clamp the upper bound to the session head (an at_seq past head reconstructs
	// the current state, never an error).
	atSeq := req.GetAtSeq()
	if atSeq > sess.HeadSeq {
		atSeq = sess.HeadSeq
	}
	if atSeq < 0 {
		atSeq = 0
	}

	// Fold the bounded window. The fold is the SAME accumulation foldTotals uses, so
	// the at-head projection equals GetSession's, and a window before a TurnFinished
	// shows zero billed turns/cost.
	events, err := s.log.LoadUpTo(ctx, req.GetSessionId(), atSeq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: state at seq: %v", err)
	}
	usage, cost, turns := foldEventTotals(events)

	// The reconstructed projection echoes the session's control/lineage with the
	// folded billing; head_seq reflects the reconstructed point, not the live head.
	sess.HeadSeq = atSeq
	return &genproto.GetStateAtSeqResponse{
		Session: toGenSession(sess, usage, cost, turns),
		AtSeq:   atSeq,
	}, nil
}

// foldEventTotals sums per-turn usage/cost/turn-count from a slice of envelopes
// (TurnFinished/TurnAborted), the pure fold shared by GetStateAtSeq and the
// foldTotals read in server.go. Keeping it a free function lets the bounded-window
// reconstruction reuse the exact accumulation without a second Load.
func foldEventTotals(events []domain.EventEnvelope) (llm.Usage, float64, int64) {
	var (
		usage llm.Usage
		cost  float64
		turns int64
	)
	for _, env := range events {
		switch ev := env.Event.(type) {
		case domain.TurnFinished:
			usage = addUsage(usage, ev.Usage)
			cost += ev.CostUSD
			turns = int64(ev.NumTurns)
		case domain.TurnAborted:
			usage = addUsage(usage, ev.UsageSoFar)
			cost += ev.CostUSD
		}
	}
	return usage, cost, turns
}
