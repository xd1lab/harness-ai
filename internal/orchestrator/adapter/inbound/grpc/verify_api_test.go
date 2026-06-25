package grpc

// RED (test-first) tests for Batch-5A (tamper-evident hash-chain) at the gRPC
// edge: the additive VerifySessionIntegrity RPC (AC-12/AC-14) and the additive
// content_hash / chain_hash fields on EventDescriptor (AC-13).
//
// These reference symbols that do NOT exist yet:
//   - genproto.VerifySessionIntegrityRequest / VerifySessionIntegrityResponse
//   - Server.VerifySessionIntegrity
//   - genproto.EventDescriptor.ContentHash / .ChainHash
//   - EventStore.VerifyChainIntegrity (and the fake's method below)
//   - domain.ChainVerification + domain.EventEnvelope.ContentHash/.ChainHash
// so the package does NOT compile — the RED proof of test-first authoring.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// VerifyChainIntegrity is the read-only verify half of the EventStore consumer-
// superset (NOT on the frozen app.EventLogPort). The fake recomputes the chain
// over its in-memory envelopes using the shared domain helpers, mirroring the
// store's recompute-and-compare semantics, so the server test exercises the real
// mapping without Postgres.
func (l *tailingEventLog) VerifyChainIntegrity(_ context.Context, sessionID string, fromSeq, toSeq int64) (domain.ChainVerification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := domain.GenesisChainHash(sessionID)
	checked := 0
	for _, e := range l.events[sessionID] {
		if fromSeq > 0 && e.Seq < fromSeq {
			// Still advance prev from the stored chain so the window seeds correctly.
			if len(e.ChainHash) > 0 {
				prev = e.ChainHash
			}
			continue
		}
		if toSeq > 0 && e.Seq > toSeq {
			break
		}
		if len(e.ContentHash) == 0 && len(e.ChainHash) == 0 {
			continue // pre-0009 NULL-hash prefix: skip, not tampered
		}
		payload, err := domain.MarshalEventPayload(e.Event)
		if err != nil {
			return domain.ChainVerification{}, err
		}
		content := domain.ContentHash(payload)
		wantChain := domain.ChainHash(prev, content)
		switch {
		case string(e.ContentHash) != string(content):
			return domain.ChainVerification{Valid: false, FirstBadSeq: e.Seq, Reason: "content-hash mismatch", Checked: checked}, nil
		case string(e.ChainHash) != string(wantChain):
			return domain.ChainVerification{Valid: false, FirstBadSeq: e.Seq, Reason: "broken-link chain-hash mismatch", Checked: checked}, nil
		}
		prev = e.ChainHash
		checked++
	}
	return domain.ChainVerification{Valid: true, Checked: checked}, nil
}

// seedChainedStream seeds a session owned by tenant and appends n TurnStarted
// events with VALID content/chain hashes folded via the shared helpers, so the
// fake holds a genuinely chained stream the verify RPC can validate.
func seedChainedStream(t *testing.T, log *tailingEventLog, sessionID, tenant string, n int) {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	log.tenants[sessionID] = tenant
	prev := domain.GenesisChainHash(sessionID)
	var evs []domain.EventEnvelope
	for i := 1; i <= n; i++ {
		ev := domain.TurnStarted{TurnID: "t", Model: "m"}
		payload, err := domain.MarshalEventPayload(ev)
		require.NoError(t, err)
		content := domain.ContentHash(payload)
		chain := domain.ChainHash(prev, content)
		evs = append(evs, domain.EventEnvelope{
			Type: ev.EventType(), Seq: int64(i), SessionID: sessionID, TenantID: tenant,
			Actor: domain.ActorSystem, Event: ev, ContentHash: content, ChainHash: chain,
		})
		prev = chain
	}
	log.events[sessionID] = evs
	log.heads[sessionID] = int64(n)
}

// TestVerifySessionIntegrity_ValidStream covers AC-14: an owned, untampered
// session verifies valid=true, first_bad_seq=0, checked=N through the shared
// ownership path.
func TestVerifySessionIntegrity_ValidStream(t *testing.T) {
	log := newTailingEventLog()
	seedChainedStream(t, log, "sess-v", "tenant-A", 5)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.VerifySessionIntegrity(context.Background(), &genproto.VerifySessionIntegrityRequest{
		TenantId: "tenant-A", SessionId: "sess-v",
	})
	require.NoError(t, err)
	assert.True(t, resp.GetValid(), "an untampered session verifies valid")
	assert.Equal(t, int64(0), resp.GetFirstBadSeq())
	assert.Equal(t, int64(5), resp.GetChecked())
}

// TestVerifySessionIntegrity_DetectsTamper covers AC-14: a mutated stored
// envelope (content no longer matches its stored content_hash) verifies
// valid=false at the tampered seq, mapped onto the response.
func TestVerifySessionIntegrity_DetectsTamper(t *testing.T) {
	log := newTailingEventLog()
	seedChainedStream(t, log, "sess-t", "tenant-A", 5)

	// Tamper seq 3 in place (payload changed; stored content_hash left stale).
	log.mu.Lock()
	for i := range log.events["sess-t"] {
		if log.events["sess-t"][i].Seq == 3 {
			log.events["sess-t"][i].Event = domain.TurnStarted{TurnID: "TAMPERED", Model: "evil"}
		}
	}
	log.mu.Unlock()

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.VerifySessionIntegrity(context.Background(), &genproto.VerifySessionIntegrityRequest{
		TenantId: "tenant-A", SessionId: "sess-t",
	})
	require.NoError(t, err)
	assert.False(t, resp.GetValid(), "a tampered session verifies invalid")
	assert.Equal(t, int64(3), resp.GetFirstBadSeq())
	assert.NotEmpty(t, resp.GetReason(), "a tamper carries a classifying reason")
}

// TestVerifySessionIntegrity_RejectsForeignTenant covers AC-14: the verify RPC
// reuses the SAME ownership path — tenant B may not verify tenant A's session.
func TestVerifySessionIntegrity_RejectsForeignTenant(t *testing.T) {
	log := newTailingEventLog()
	seedChainedStream(t, log, "sess-A", "tenant-A", 3)

	h := devHarness(t, "tenant-B", noopRunner(log), log)
	_, err := h.client.VerifySessionIntegrity(context.Background(), &genproto.VerifySessionIntegrityRequest{
		TenantId: "tenant-B", SessionId: "sess-A",
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"tenant B cannot verify tenant A's session (ownership, shared authorizeSession)")
}

// TestVerifySessionIntegrity_MissingIsNotFound covers AC-14: a missing /
// RLS-invisible session is NotFound (the same as the other read RPCs).
func TestVerifySessionIntegrity_MissingIsNotFound(t *testing.T) {
	log := newTailingEventLog()

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.VerifySessionIntegrity(context.Background(), &genproto.VerifySessionIntegrityRequest{
		TenantId: "tenant-A", SessionId: "nope",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err), "a missing session is NotFound")
}

// TestVerifySessionIntegrity_RejectsUnauthenticated covers AC-14: an
// unauthenticated verify is rejected in production-auth mode (shared edge auth).
func TestVerifySessionIntegrity_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	seedChainedStream(t, log, "sess-1", "tenant-A", 3)
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.VerifySessionIntegrity(context.Background(), &genproto.VerifySessionIntegrityRequest{
		TenantId: "tenant-A", SessionId: "sess-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// TestEventDescriptor_CarriesHashes covers AC-13: ListSessionEvents exposes the
// per-event content_hash and chain_hash on every descriptor (non-sensitive
// integrity digests, surfaced regardless of include_payload, never setting the
// redacted flag on their account).
func TestEventDescriptor_CarriesHashes(t *testing.T) {
	log := newTailingEventLog()
	seedChainedStream(t, log, "sess-d", "tenant-A", 4)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessionEvents(context.Background(), &genproto.ListSessionEventsRequest{
		TenantId: "tenant-A", SessionId: "sess-d", PageSize: 1000,
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetEvents())

	for _, d := range resp.GetEvents() {
		assert.NotEmpty(t, d.GetContentHash(), "seq %d descriptor must carry content_hash", d.GetSeq())
		assert.NotEmpty(t, d.GetChainHash(), "seq %d descriptor must carry chain_hash", d.GetSeq())
		assert.Len(t, d.GetContentHash(), 32, "content_hash is a 32-byte sha256 digest")
		assert.Len(t, d.GetChainHash(), 32, "chain_hash is a 32-byte sha256 digest")
	}
}
