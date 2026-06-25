// SPDX-License-Identifier: Apache-2.0

package rest_test

// RED (test-first) for Batch-5A AC-15: the REST facade route
// GET /v1/sessions/{id}/integrity, a thin shell over the shared
// igrpc.Server.VerifySessionIntegrity. It mirrors the listSessionEvents handler
// shape (from_seq / to_seq parsed via parseOptionalInt64 with a typed 400 on a
// parse error; the response is protojson of VerifySessionIntegrityResponse).
//
// It references symbols that do NOT exist yet — the EventStore.VerifyChainIntegrity
// method (the fake gains it below), Server.VerifySessionIntegrity, the proto
// messages, domain.ChainVerification, and domain.EventEnvelope.ContentHash/.ChainHash
// — so the package does NOT compile; that absence is the RED proof.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// VerifyChainIntegrity is the read-only verify half of the EventStore consumer-
// superset; the REST fake recomputes the chain over its in-memory envelopes via
// the shared domain helpers, matching the store's recompute-and-compare semantics.
func (f *fakeStore) VerifyChainIntegrity(_ context.Context, sessionID string, fromSeq, toSeq int64) (domain.ChainVerification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := domain.GenesisChainHash(sessionID)
	checked := 0
	for _, e := range f.events[sessionID] {
		if fromSeq > 0 && e.Seq < fromSeq {
			if len(e.ChainHash) > 0 {
				prev = e.ChainHash
			}
			continue
		}
		if toSeq > 0 && e.Seq > toSeq {
			break
		}
		if len(e.ContentHash) == 0 && len(e.ChainHash) == 0 {
			continue
		}
		payload, err := domain.MarshalEventPayload(e.Event)
		if err != nil {
			return domain.ChainVerification{}, err
		}
		content := domain.ContentHash(payload)
		wantChain := domain.ChainHash(prev, content)
		if string(e.ContentHash) != string(content) {
			return domain.ChainVerification{Valid: false, FirstBadSeq: e.Seq, Reason: "content-hash mismatch", Checked: checked}, nil
		}
		if string(e.ChainHash) != string(wantChain) {
			return domain.ChainVerification{Valid: false, FirstBadSeq: e.Seq, Reason: "broken-link chain-hash mismatch", Checked: checked}, nil
		}
		prev = e.ChainHash
		checked++
	}
	return domain.ChainVerification{Valid: true, Checked: checked}, nil
}

// seedChained inserts n validly-chained TurnStarted events on a session owned by
// the dev tenant, folding content/chain hashes via the shared helpers.
func (f *fakeStore) seedChained(t *testing.T, sessionID, tenant string, n int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[sessionID] = domain.Session{ID: sessionID, TenantID: tenant, Status: domain.StatusActive, HeadSeq: int64(n)}
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
	f.events[sessionID] = evs
}

// TestVerifyIntegrity_ValidSession pins GET /v1/sessions/{id}/integrity: a clean
// session returns protojson with valid=true and checked=N.
func TestVerifyIntegrity_ValidSession(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "iv-1", devTenantID(), 5)

	resp := h.doJSON(t, http.MethodGet, "/v1/sessions/iv-1/integrity", "", "")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Equal(t, true, body["valid"], "untampered session is valid")
	// protojson encodes int64 as a string; 5 events checked.
	assert.Equal(t, "5", body["checked"])
}

// TestVerifyIntegrity_TamperedSession pins the tamper path: a mutated envelope
// returns valid=false with the offending first_bad_seq and a reason.
func TestVerifyIntegrity_TamperedSession(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "iv-2", devTenantID(), 5)

	h.store.mu.Lock()
	for i := range h.store.events["iv-2"] {
		if h.store.events["iv-2"][i].Seq == 3 {
			h.store.events["iv-2"][i].Event = domain.TurnStarted{TurnID: "TAMPERED", Model: "evil"}
		}
	}
	h.store.mu.Unlock()

	resp := h.doJSON(t, http.MethodGet, "/v1/sessions/iv-2/integrity", "", "")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := decodeJSON(t, resp)
	assert.Equal(t, false, body["valid"], "tampered session is invalid")
	assert.Equal(t, "3", body["firstBadSeq"])
	assert.NotEmpty(t, body["reason"])
}

// TestVerifyIntegrity_BadQueryParamIs400 pins the fail-closed-early edge: an
// unparseable from_seq/to_seq is a typed 400 before any store call.
func TestVerifyIntegrity_BadQueryParamIs400(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "iv-3", devTenantID(), 2)
	for _, q := range []string{
		"/v1/sessions/iv-3/integrity?from_seq=abc",
		"/v1/sessions/iv-3/integrity?to_seq=notanum",
	} {
		resp := h.doJSON(t, http.MethodGet, q, "", "")
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "bad query %q must be 400", q)
		_ = resp.Body.Close()
	}
}

// TestVerifyIntegrity_ForeignTenantIs403 pins the ownership matrix at the REST
// edge: a session owned by another tenant maps PermissionDenied -> 403.
func TestVerifyIntegrity_ForeignTenantIs403(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "iv-alien", "99999999-9999-4999-8999-999999999999", 3)

	resp := h.doJSON(t, http.MethodGet, "/v1/sessions/iv-alien/integrity", "", "")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "foreign-tenant verify is 403")
}

// devTenantID returns the dev tenant the dev-insecure principal carries (the
// owner of sessions created in dev mode), so a verify test can seed an owned
// session.
func devTenantID() string { return igrpc.DevTenantID }
