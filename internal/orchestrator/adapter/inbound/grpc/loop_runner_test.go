package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestRun_WithRealLoop drives the REAL agent.Loop through the production
// LoopRunner and the Server end-to-end over bufconn: a scripted model gateway
// emits a single text-only turn, and the client must receive the assembled
// assistant text plus a terminal Success Result. This proves the transport
// drives the real loop (not just a fake Runner) with the durable event log as
// the resumable frame source.
func TestRun_WithRealLoop(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-real", "tenant-A")

	gw := apptest.NewFakeModelGateway()
	// One streamed text-only turn ending in StopEnd → the loop terminates success.
	gw.AddStreamEvents(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "hi from the real loop"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
	)

	engine, err := policy.NewEngine(policy.Config{})
	require.NoError(t, err)

	runner := NewLoopRunner(agent.Deps{
		EventLog:  log,
		Model:     gw,
		Tools:     apptest.NewFakeToolRuntime(),
		Approvals: apptest.NewFakeApprovalGate(),
		Hooks:     apptest.NewFakeHookRunner(),
		Policy:    engine,
		Clock:     clocktest.NewFake(time.Unix(0, 0)),
		IDs:       newFakeIDs(),
	}, agent.Config{Model: "test-model", MaxTurns: 4})

	gate := newNotifyingGate()
	server := NewServer(log, gate, runner, newFakeIDs(), Config{})
	conn := startServerWith(t, AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "tenant-A"}}, server)
	client := genproto.NewOrchestratorServiceClient(conn)

	stream, err := client.Run(context.Background(), &genproto.RunRequest{
		TenantId:  "tenant-A",
		SessionId: "sess-real",
		Message: &genproto.Message{
			Role:    genproto.Role_ROLE_USER,
			Content: []*genproto.ContentPart{{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "hello"}}}},
		},
	})
	require.NoError(t, err)

	got := collectRunEvents(t, stream)

	var sawText bool
	var result *genproto.RunResult
	for _, ev := range got {
		if ev.GetTextDelta() != nil && ev.GetTextDelta().GetText() == "hi from the real loop" {
			sawText = true
		}
		if ev.GetResult() != nil {
			result = ev.GetResult()
		}
	}
	assert.True(t, sawText, "client should receive the assembled assistant text from the real loop")
	require.NotNil(t, result, "stream must end with a terminal Result")
	assert.Equal(t, genproto.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS, result.GetSubtype())
	assert.Equal(t, "hi from the real loop", result.GetFinalText())
}
