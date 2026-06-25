//go:build integration

package eventstore

import (
	"context"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/approval"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy/policytest"
	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestApprovalRequested_AppendLoadRoundTrip is the integration RED proof for FIX 3
// AC-3.9 / AC-3.10: a domain.ApprovalRequested event (the un-collapsed general
// tool-dispatch approval request) Appends and Loads back from real Postgres with
// its Args intact — event_type "ApprovalRequested" persists and decodePayload
// reconstructs the typed event. It simulates a crash mid-ask: the ApprovalRequested
// is written with NO matching PermissionDecided, exactly the durable state a resume
// must classify as a SuspendedApproval rather than silently abort.
//
// It references domain.ApprovalRequested / domain.EventApprovalRequested, which do
// not exist yet, so the package does not compile under -tags integration — the RED
// proof that the feature is absent.
func TestApprovalRequested_AppendLoadRoundTrip(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	want := domain.ApprovalRequested{
		TurnID:   "t-1",
		CallID:   "call-7",
		ToolName: "bash",
		Reason:   "mutating shell command",
		Args:     map[string]any{"cmd": "rm -rf /tmp/x", "force": float64(1)},
	}

	envs, err := h.store.Append(ctx, sessionID, 0, 0, newUUID(t),
		app.AppendInput{Event: want, Actor: domain.ActorSystem})
	if err != nil {
		t.Fatalf("append ApprovalRequested: %v", err)
	}
	if len(envs) != 1 || envs[0].Type != domain.EventApprovalRequested {
		t.Fatalf("append returned %d envs, type %v; want 1 ApprovalRequested", len(envs), envs[0].Type)
	}

	loaded, err := h.store.Load(ctx, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var got *domain.ApprovalRequested
	for _, e := range loaded {
		if p, ok := e.Event.(domain.ApprovalRequested); ok {
			got = &p
		}
	}
	if got == nil {
		t.Fatalf("no ApprovalRequested decoded from %d loaded events (decodePayload must handle the new type)", len(loaded))
	}
	if got.TurnID != want.TurnID || got.CallID != want.CallID || got.ToolName != want.ToolName || got.Reason != want.Reason {
		t.Fatalf("ApprovalRequested round-trip mismatch: got %+v, want %+v", *got, want)
	}
	// Args must survive the round trip intact (JSON numbers decode to float64).
	if got.Args["cmd"] != want.Args["cmd"] || got.Args["force"] != want.Args["force"] {
		t.Fatalf("ApprovalRequested.Args round-trip mismatch: got %+v, want %+v", got.Args, want.Args)
	}

	// There is NO matching PermissionDecided for call-7 — this is the suspended-ask
	// shape a resume must re-raise rather than silently TurnAbort.
	for _, e := range loaded {
		if pd, ok := e.Event.(domain.PermissionDecided); ok && pd.CallID == want.CallID {
			t.Fatalf("unexpected PermissionDecided for %q; the ask must be unresolved (crash mid-ask)", want.CallID)
		}
	}
}

// TestSuspendedApproval_ResumeReRaises is the end-to-end integration proof for FIX
// 3 AC-3.7/3.9/3.10 at the TARGET (re-raise) level, against real Postgres.
//
// It seeds the durable shape of a crash MID-ASK into the real eventstore —
// SessionStarted, TurnStarted, an assistant tool-call AssistantMessage, and a lone
// ApprovalRequested with NO matching PermissionDecided — then drives the agent loop
// (with the real eventstore as its EventLogPort and the real in-process
// approval.Gate as its ApprovalGate) through resume.
//
// A SubscribeApprovals subscriber is registered BEFORE resume so the test can prove
// the gate is RE-RAISED (the subscriber fires with the persisted call id/args) and
// then resolve it AskAllowed in-process — exactly the path a reconnecting client
// takes. The assertions confirm: (a) the re-raise notified the subscriber (NOT a
// silent TurnAborted); (b) the gated tool then PROCEEDS (ToolExecutionStarted +
// ToolResult appended for the suspended call); and (c) the run finishes Success.
//
// LIMITATION (stated honestly): this exercises re-raise IN-PROCESS with a
// subscriber present. It CANNOT prove safety on a true process restart with NO
// connected client — there the re-raised Request would block until the MANDATORY
// bounded ctx ([agent.Config.ResumeApprovalTimeout]) elapses and the loop falls
// back to the durable-auditable close. The bound is implemented (loop.go
// withApprovalTimeout) and unit-bounded in the agent package; this test covers the
// happy re-raise path the integration harness can drive.
func TestSuspendedApproval_ResumeReRaises(t *testing.T) {
	h := newHarness(t)
	tenantID, sessionID := h.seedTenantAndSession(t)
	ctx := tenantCtx(tenantID)

	const (
		turnID   = "turn-1"
		callID   = "call-7"
		toolName = "bash"
	)
	args := map[string]any{"cmd": "echo hi", "n": float64(2)}

	// Seed the crash-mid-ask log directly into real Postgres. Each append advances
	// head_seq; we thread the returned seq as the next expected head.
	head := int64(0)
	seed := []app.AppendInput{
		{Event: domain.SessionStarted{SystemPrompt: "sys"}, Actor: domain.ActorSystem},
		{Event: domain.TurnStarted{TurnID: turnID, Model: "m"}, Actor: domain.ActorSystem},
		{Event: domain.AssistantMessage{
			TurnID: turnID,
			Message: llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: callID, Name: toolName, Args: args}},
			}},
			StopReason: llm.StopToolUse,
		}, Actor: domain.ActorAssistant},
		{Event: domain.ApprovalRequested{
			TurnID: turnID, CallID: callID, ToolName: toolName, Reason: "mutating shell command", Args: args,
		}, Actor: domain.ActorSystem},
	}
	for _, in := range seed {
		envs, err := h.store.Append(ctx, sessionID, head, 0, newUUID(t), in)
		if err != nil {
			t.Fatalf("seed append %s: %v", in.Event.EventType(), err)
		}
		head = envs[len(envs)-1].Seq
	}

	// Wire the agent loop against the REAL eventstore and the REAL in-process gate
	// so SubscribeApprovals/Resolve exercise the production approval path.
	gate := approval.New()
	model := apptest.NewFakeModelGateway()
	tools := apptest.NewFakeToolRuntime()
	pol := policytest.NewFakePolicyEngine()
	hooks := apptest.NewFakeHookRunner()

	// The suspended tool (bash) is mutating; after it runs, the model produces a
	// terminal text turn so the run finishes Success.
	tools.SetTools([]app.ToolDescriptor{{Name: toolName, SideEffect: domain.SideEffectMutating}})
	tools.AddSuccessfulExecution("hi")
	model.AddStream(textStreamDone("all done"), nil)

	lp := agent.NewLoop(agent.Deps{
		EventLog:  h.store,
		Model:     model,
		Tools:     tools,
		Approvals: gate,
		Hooks:     hooks,
		Policy:    pol,
		Clock:     clock.System{},
		IDs:       ids.System{},
	}, agent.Config{Model: "m", MaxTurns: 8})

	// Register a subscriber BEFORE resume: it is the re-raise PROOF. When the loop
	// re-raises the gate on resume it fires this callback; we record it and resolve
	// the pending ask AskAllowed in a goroutine (the reconnecting-client path).
	notified := make(chan app.ApprovalRequest, 1)
	cancelSub := gate.SubscribeApprovals(sessionID, func(req app.ApprovalRequest) {
		notified <- req
		// Resolve from a separate goroutine so the synchronous notify returns
		// promptly (the notifier contract forbids blocking).
		go func() { _ = gate.Resolve(context.Background(), sessionID, req.CallID, domain.AskAllowed) }()
	})
	defer cancelSub()

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := lp.Run(runCtx, agent.RunInput{SessionID: sessionID})
	if err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// (a) the gate was RE-RAISED (subscriber notified with the persisted call).
	select {
	case req := <-notified:
		if req.CallID != callID || req.ToolName != toolName {
			t.Fatalf("re-raised approval mismatch: got call %q tool %q; want %q/%q", req.CallID, req.ToolName, callID, toolName)
		}
		if req.Args["cmd"] != args["cmd"] {
			t.Fatalf("re-raised approval lost args: got %+v", req.Args)
		}
	default:
		t.Fatalf("resume did NOT re-raise the suspended approval (subscriber never notified) — it was silently dropped/aborted")
	}

	// (b)+(c) the run finished Success and the gated tool PROCEEDED.
	if res.Reason != domain.Success {
		t.Fatalf("resume run reason = %q; want Success (the approved tool should proceed and the run complete)", res.Reason)
	}

	loaded, err := h.store.Load(ctx, sessionID, 0)
	if err != nil {
		t.Fatalf("Load after resume: %v", err)
	}
	var (
		sawDecided  bool
		sawExecuted bool
		sawResult   bool
		sawAborted  bool
	)
	for _, e := range loaded {
		switch ev := e.Event.(type) {
		case domain.PermissionDecided:
			if ev.CallID == callID && ev.Decision == domain.PermissionAsk && ev.Resolved == domain.AskAllowed {
				sawDecided = true
			}
		case domain.ToolExecutionStarted:
			if ev.CallID == callID {
				sawExecuted = true
			}
		case domain.ToolResult:
			if ev.CallID == callID {
				sawResult = true
			}
		case domain.TurnAborted:
			if ev.TurnID == turnID {
				sawAborted = true
			}
		}
	}
	if !sawDecided {
		t.Fatalf("no PermissionDecided{Ask,AskAllowed} for %q — the re-raised resolution was not recorded", callID)
	}
	if !sawExecuted || !sawResult {
		t.Fatalf("gated tool did not proceed after approval: ToolExecutionStarted=%v ToolResult=%v", sawExecuted, sawResult)
	}
	if sawAborted {
		t.Fatalf("suspended turn %q was TurnAborted despite re-raise+approve — it must continue, not abort", turnID)
	}
}

// textStreamDone builds a minimal single-text-delta stream terminated by
// Done(StopEnd), used to drive the post-resume terminal turn.
func textStreamDone(text string) []llm.StreamEvent {
	return []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: text}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}
}
