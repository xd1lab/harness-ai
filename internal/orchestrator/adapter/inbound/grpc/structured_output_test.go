package grpc

// Feature S (structured output) — TDD red, AC-3 / AC-4 / TASKS T-2 + T-3.
//
// These tests pin the gRPC client-edge seam that carries the public output_schema
// / strict fields into the kernel per-run:
//
//   - AC-3: a RunRequest carrying output_schema/strict produces a RunSpec (captured
//     by a fake Runner) with OutputSchema bytes + Strict set. RED until the proto
//     field exists (gen regen, T-2) and RunSpec gains the fields + Server.Run fills
//     them (T-3).
//   - AC-4: LoopRunner.Run overlays cfg.OutputSchema/cfg.Strict per run (same idiom
//     as cfg.Mode), so two runs on one session each see their own schema (per-run,
//     not session-global). RED until LoopRunner overlays the fields (T-3) and
//     agent.Config gains Strict (T-4).
//
// Mirrors the existing fake-Runner spec-capture idiom (server_test.go
// TestRun_UsesSessionMode) and the real-loop idiom (loop_runner_test.go).

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestProtoSurface_RunRequestHasOutputSchemaAndStrict pins AC-2: the regenerated
// gen/ exposes GetOutputSchema() []byte and GetStrict() bool on RunRequest. This
// fails to COMPILE until `make gen` regenerates gen/ from the additive proto field
// (T-2) — that compile failure is the red state for the contract.
func TestProtoSurface_RunRequestHasOutputSchemaAndStrict(t *testing.T) {
	req := &genproto.RunRequest{
		OutputSchema: []byte(`{"type":"object"}`),
		Strict:       true,
	}
	schema := req.GetOutputSchema()
	strict := req.GetStrict()
	if len(schema) == 0 || !strict {
		t.Fatalf("RunRequest getters must round-trip output_schema/strict; got %q / %v", schema, strict)
	}
}

// TestRun_OutputSchemaFlowsToRunSpec pins AC-3: a RunRequest carrying
// output_schema (bytes) + strict produces a RunSpec the Runner sees with those
// fields populated, proving Server.Run maps req.GetOutputSchema()/req.GetStrict()
// onto the kernel/domain RunSpec.
func TestRun_OutputSchemaFlowsToRunSpec(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-so", "tenant-A")

	schema := []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	specCh := make(chan RunSpec, 1)
	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		specCh <- spec
		appendAssistantText(ctx, l, spec.SessionID, "t1", "ok")
		return RunOutcome{Reason: domain.Success, FinalText: "ok", NumTurns: 1}, nil
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{
		TenantId:     "tenant-A",
		SessionId:    "sess-so",
		OutputSchema: schema, // RED: proto field does not exist until T-2 regen.
		Strict:       true,
	})
	require.NoError(t, err)
	_ = collectRunEvents(t, stream)

	select {
	case spec := <-specCh:
		assert.Equal(t, schema, spec.OutputSchema, "RunRequest.output_schema must flow into RunSpec.OutputSchema")
		assert.True(t, spec.Strict, "RunRequest.strict must flow into RunSpec.Strict")
	default:
		t.Fatal("runner was not invoked")
	}
}

// TestRun_NoSchemaLeavesRunSpecEmpty pins backward compatibility (AC-14 at this
// seam): a RunRequest without the fields yields a free-form RunSpec.
func TestRun_NoSchemaLeavesRunSpecEmpty(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-free", "tenant-A")

	specCh := make(chan RunSpec, 1)
	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		specCh <- spec
		appendAssistantText(ctx, l, spec.SessionID, "t1", "ok")
		return RunOutcome{Reason: domain.Success, FinalText: "ok", NumTurns: 1}, nil
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-free"})
	require.NoError(t, err)
	_ = collectRunEvents(t, stream)

	select {
	case spec := <-specCh:
		assert.Empty(t, spec.OutputSchema, "absent output_schema must leave RunSpec.OutputSchema empty")
		assert.False(t, spec.Strict, "absent strict must leave RunSpec.Strict false")
	default:
		t.Fatal("runner was not invoked")
	}
}

// TestLoopRunner_OverlaysOutputSchemaPerRun pins AC-4: two runs on the SAME
// LoopRunner with DIFFERENT RunSpec.OutputSchema each drive the loop with their own
// schema (the shared cfg template is never mutated). We observe the per-run schema
// via the FakeModelGateway, which captures the outbound llm.Request whose
// OutputSchema is set only if LoopRunner overlaid cfg.OutputSchema = spec.OutputSchema.
func TestLoopRunner_OverlaysOutputSchemaPerRun(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-a", "tenant-A")
	log.seed("sess-b", "tenant-A")

	gw := apptest.NewFakeModelGateway()
	// Two runs, each a single schema-valid text turn → success.
	for i := 0; i < 2; i++ {
		gw.AddStreamEvents(
			llm.StreamEvent{TextDelta: &llm.TextDelta{Text: `{"answer":"ok"}`}},
			llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
		)
	}

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
	}, agent.Config{Model: "test-model", MaxTurns: 4, MaxStructuredOutputRetries: 1})

	schemaA := []byte(`{"type":"object","required":["answer"]}`)
	schemaB := []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)

	_, err = runner.Run(context.Background(), RunSpec{SessionID: "sess-a", TenantID: "tenant-A", OutputSchema: schemaA, Strict: true})
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), RunSpec{SessionID: "sess-b", TenantID: "tenant-A", OutputSchema: schemaB, Strict: false})
	require.NoError(t, err)

	var got [][]byte
	var gotStrict []bool
	for _, c := range gw.Calls() {
		if c.Method == "Stream" {
			got = append(got, c.Req.OutputSchema)
			gotStrict = append(gotStrict, c.Req.Strict)
		}
	}
	require.Len(t, got, 2, "each run must make one Stream call")
	assert.JSONEq(t, string(schemaA), string(got[0]), "run A must carry its own schema")
	assert.JSONEq(t, string(schemaB), string(got[1]), "run B must carry its own schema (per-run, not session-global)")
	assert.True(t, gotStrict[0], "run A strict must overlay onto the loop request")
	assert.False(t, gotStrict[1], "run B strict=false must overlay onto the loop request")
}
