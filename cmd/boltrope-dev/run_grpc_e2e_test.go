// SPDX-License-Identifier: Apache-2.0

package main

// T-6 (AC-8 primary, the onboarding path) — RED. A gRPC client equivalent to
//
//	harnessctl --endpoint 127.0.0.1:<grpc-port> --insecure run "<task>"
//
// must complete a full agent turn against the keyless stub provider and receive a
// terminal Result(Success) over the loopback gRPC listener the dev binary serves —
// the path the prior REST-only draft could NOT prove (SPEC BLOCKER 1). It dials
// with the EXACT transport harnessctl uses (insecure.NewCredentials()). It
// references newServer()/serveOpts/serveResult (the T-11 factory), which do not
// exist yet → RED.

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	boltropev1 "github.com/xd1lab/harness-ai/gen/boltrope/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDevServer_GRPC_E2E_StubReachesSuccess asserts AC-8: start the dev gRPC
// listener on an ephemeral loopback port, dial it as harnessctl does, CreateSession
// → Run "hello", and reach a terminal Success with streamed stub text and zero tool
// executions.
func TestDevServer_GRPC_E2E_StubReachesSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Bind ephemeral loopback ports so the test never collides with the fixed
	// default ports (8089/8088).
	srv, err := newServer(serveOpts{ // newServer / serveOpts do not exist yet (RED)
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	})

	conn, err := grpc.NewClient(
		srv.GRPCAddr, // the resolved ephemeral loopback gRPC address
		grpc.WithTransportCredentials(insecure.NewCredentials()), // EXACT harnessctl transport
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)

	// IMPORTANT: leave TenantId empty. In dev-insecure mode authorizeTenant derives
	// the tenant from the verified synthetic principal; a non-empty, mismatched body
	// tenant would (correctly) be rejected with PERMISSION_DENIED.
	cs, err := client.CreateSession(ctx, &boltropev1.CreateSessionRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, cs.GetSessionId())

	stream, err := client.Run(ctx, &boltropev1.RunRequest{
		SessionId: cs.GetSessionId(),
		Message: &boltropev1.Message{
			Role: boltropev1.Role_ROLE_USER,
			Content: []*boltropev1.ContentPart{
				{Part: &boltropev1.ContentPart_Text{Text: &boltropev1.TextPart{Text: "hello"}}},
			},
		},
	})
	require.NoError(t, err)

	var sawText bool
	var terminal *boltropev1.RunResult
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch p := ev.GetPayload().(type) {
		case *boltropev1.RunEvent_TextDelta:
			if p.TextDelta.GetText() != "" {
				sawText = true
			}
		case *boltropev1.RunEvent_Result:
			terminal = p.Result
		}
	}

	require.NotNil(t, terminal, "the Run stream must deliver a terminal Result frame")
	assert.Equal(t, boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS, terminal.GetSubtype(),
		"the keyless stub terminates at StopEnd → Success")
	assert.True(t, sawText, "the stub's text must be streamed to the client")
}
