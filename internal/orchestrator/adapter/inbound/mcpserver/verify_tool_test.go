// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// RED (test-first) for Batch-5A AC-16: the NEW 12th MCP tool
// verify_session_integrity, a thin shell over the shared
// igrpc.Server.VerifySessionIntegrity. It requires session_id (else
// InvalidParams), accepts optional from_seq/to_seq, and returns protoToMap of the
// VerifySessionIntegrityResponse as the structured result.
//
// References symbols that do NOT exist yet — the EventStore.VerifyChainIntegrity
// method (the fake gains it below), Server.VerifySessionIntegrity, the proto
// messages, domain.ChainVerification, and domain.EventEnvelope.ContentHash/.ChainHash
// — so the package does NOT compile; that absence is the RED proof. The pinned
// tool-count test in jsonrpc_test.go is updated 11 -> 12 in the same change.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// VerifyChainIntegrity is the read-only verify half of the EventStore consumer-
// superset; the MCP fake recomputes the chain over its in-memory envelopes via
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
// tenant, folding content/chain hashes via the shared helpers.
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

// TestVerifySessionIntegrityTool_InCatalog covers AC-16: the 12th tool is listed
// with a non-empty description and an inputSchema requiring session_id.
func TestVerifySessionIntegrityTool_InCatalog(t *testing.T) {
	h := devHarness(t)
	env, _ := h.doRPC(t, "", "tools/list", map[string]any{})
	require.Nil(t, env.Error)

	var result struct {
		Tools []struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"inputSchema"`
		} `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(env.Result, &result))

	var found bool
	for _, tool := range result.Tools {
		if tool.Name == "verify_session_integrity" {
			found = true
			assert.NotEmpty(t, tool.Description, "verify_session_integrity must have a description")
			assert.NotEmpty(t, tool.InputSchema, "verify_session_integrity must have an inputSchema")
			req, _ := tool.InputSchema["required"].([]any)
			assert.Contains(t, req, "session_id", "session_id must be required")
		}
	}
	require.True(t, found, "the catalog must include verify_session_integrity (the 12th tool)")
}

// TestVerifySessionIntegrityTool_ValidStream covers AC-16: dispatching the tool
// on a clean owned session returns the structured verify response with valid=true.
func TestVerifySessionIntegrityTool_ValidStream(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "mv-1", devTenant(), 5)

	env, _ := h.callTool(t, "", "verify_session_integrity", map[string]any{"session_id": "mv-1"})
	require.Nil(t, env.Error)
	cr := decodeCallResult(t, env)
	assert.False(t, cr.IsError)
	assert.Equal(t, true, cr.StructuredContent["valid"], "untampered session is valid")
}

// TestVerifySessionIntegrityTool_DetectsTamper covers AC-16: a tampered session
// surfaces valid=false with the offending first_bad_seq.
func TestVerifySessionIntegrityTool_DetectsTamper(t *testing.T) {
	h := devHarness(t)
	h.store.seedChained(t, "mv-2", devTenant(), 5)

	h.store.mu.Lock()
	for i := range h.store.events["mv-2"] {
		if h.store.events["mv-2"][i].Seq == 2 {
			h.store.events["mv-2"][i].Event = domain.TurnStarted{TurnID: "TAMPERED", Model: "evil"}
		}
	}
	h.store.mu.Unlock()

	env, _ := h.callTool(t, "", "verify_session_integrity", map[string]any{"session_id": "mv-2"})
	require.Nil(t, env.Error)
	cr := decodeCallResult(t, env)
	assert.Equal(t, false, cr.StructuredContent["valid"], "tampered session is invalid")
	// protojson encodes int64 as a string.
	assert.Equal(t, "2", cr.StructuredContent["firstBadSeq"])
}

// TestVerifySessionIntegrityTool_RequiresSessionID covers AC-16: a missing
// session_id is a JSON-RPC InvalidParams (-32602), not a CallToolResult.
func TestVerifySessionIntegrityTool_RequiresSessionID(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "verify_session_integrity", map[string]any{})
	require.NotNil(t, env.Error, "missing session_id is a protocol error")
	assert.Equal(t, -32602, env.Error.Code, "InvalidParams for a missing required arg")
}

// devTenant returns the dev tenant the dev-insecure principal carries.
func devTenant() string { return igrpc.DevTenantID }
