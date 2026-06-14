// SPDX-License-Identifier: Apache-2.0

package main

// T-3 (AC-16) — RED. The dev model dependency must satisfy the 4-method
// app.ModelGatewayPort (incl. Generate) by wrapping the keyless stub llm.Provider
// DIRECTLY — NOT modelgateway/app.Service (which has no Generate and whose
// NewService fails closed without CapabilityResolver+CostFunc+Endpoint; SPEC
// BLOCKER 4). It references devModel / newDevModel, which do not exist yet → RED.

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/stub"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/platform/llm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AC-16 (i) — compile assertion that devModel satisfies the 4-method
// app.ModelGatewayPort (Generate/Stream/CountTokens/Capabilities). devModel does
// not exist yet → no compile (RED). This is the assertion the prior draft mis-filed
// as a non-RED contract task.
var _ app.ModelGatewayPort = (*devModel)(nil)

// TestDevModel_StreamReachesStub_TerminatesAtStopEnd asserts AC-16 (ii): a Stream
// call through the dev model wrapper reaches the stub provider and yields the stub's
// text delta followed by a terminal Done(StopEnd). The loop only ever calls
// Model.Stream at runtime, so this is the runtime-load-bearing path.
func TestDevModel_StreamReachesStub_TerminatesAtStopEnd(t *testing.T) {
	m := newDevModel(stub.New()) // newDevModel does not exist yet (RED)

	reader, err := m.Stream(context.Background(), llm.Request{Model: "stub"})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	var gotText string
	var sawDone bool
	var doneReason llm.StopReason
	for {
		ev, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if ev.TextDelta != nil {
			gotText += ev.TextDelta.Text
		}
		if ev.Done != nil {
			sawDone = true
			doneReason = ev.Done.StopReason
		}
	}
	assert.NotEmpty(t, gotText, "the stub's text delta must flow through the dev model wrapper")
	assert.True(t, sawDone, "the stream must terminate with a Done event")
	assert.Equal(t, llm.StopEnd, doneReason, "the stub terminates at StopEnd, never requesting a tool")
}

// TestDevModel_Generate_DelegatesToStub asserts the Generate method exists and
// delegates to the stub (it must exist to satisfy the interface even though the
// loop never calls it at runtime).
func TestDevModel_Generate_DelegatesToStub(t *testing.T) {
	m := newDevModel(stub.New()) // RED
	resp, err := m.Generate(context.Background(), llm.Request{Model: "stub"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, llm.StopEnd, resp.StopReason)
}

// TestDevModel_WrapsAProviderNotAService is the structural half of AC-16 (iii):
// the dev model wrapper holds a plain llm.Provider, NOT a *modelgateway/app.Service
// and NOT a modelgw.Adapter/gRPC client. Constructing it over the stub Provider and
// asserting the constructor accepts an llm.Provider pins the seam; the import-graph
// guard in imports_test.go enforces the no-Service / no-gRPC-client invariant.
func TestDevModel_WrapsAProviderNotAService(t *testing.T) {
	var p llm.Provider = stub.New()
	m := newDevModel(p) // RED — constructor must accept an llm.Provider directly
	require.NotNil(t, m)
}
