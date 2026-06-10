// Tests for harnessctl: a bufconn-based fake OrchestratorServiceServer asserting
// the CLI run command streams and renders frames, that approve issues a Control
// call, and that flag parsing works correctly (T-CMD-03 / DOD-09).
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	boltropev1 "github.com/boltrope/boltrope/gen/boltrope/v1"
)

// ---- fake server -------------------------------------------------------

// fakeOrchestrator is a minimal in-process OrchestratorServiceServer used for
// testing.  Individual test cases configure the fields they care about.
type fakeOrchestrator struct {
	boltropev1.UnimplementedOrchestratorServiceServer

	// createSessionID is returned by CreateSession.
	createSessionID string
	// lastCreateMode captures the permission mode of the most recent CreateSession
	// request, so a test can assert the CLI wired --permission-mode through.
	lastCreateMode boltropev1.PermissionMode

	// runEvents are sent in order on the Run stream before it closes.
	runEvents []*boltropev1.RunEvent

	// controlRequests captures every Control call received; the test reads it to
	// assert the CLI issued the expected approve/deny/interrupt RPC.
	controlRequests []*boltropev1.ControlRequest

	// forkChildID is returned by Fork.
	forkChildID string
}

func (f *fakeOrchestrator) CreateSession(_ context.Context, req *boltropev1.CreateSessionRequest) (*boltropev1.CreateSessionResponse, error) {
	id := f.createSessionID
	if id == "" {
		id = "sess-fake-001"
	}
	f.lastCreateMode = req.GetMode()
	return &boltropev1.CreateSessionResponse{SessionId: id}, nil
}

func (f *fakeOrchestrator) Run(req *boltropev1.RunRequest, stream boltropev1.OrchestratorService_RunServer) error {
	_ = req
	for _, ev := range f.runEvents {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeOrchestrator) Control(_ context.Context, req *boltropev1.ControlRequest) (*boltropev1.ControlResponse, error) {
	f.controlRequests = append(f.controlRequests, req)
	return &boltropev1.ControlResponse{HeadSeq: int64(len(f.controlRequests))}, nil
}

func (f *fakeOrchestrator) Fork(_ context.Context, req *boltropev1.ForkRequest) (*boltropev1.ForkResponse, error) {
	_ = req
	id := f.forkChildID
	if id == "" {
		id = "sess-fork-002"
	}
	return &boltropev1.ForkResponse{SessionId: id}, nil
}

// ---- bufconn helper ----------------------------------------------------

// newFakeServer starts an in-process gRPC server backed by fake and returns a
// dialed *grpc.ClientConn.  It registers cleanup with t so callers need not
// worry about teardown.
func newFakeServer(t *testing.T, fake *fakeOrchestrator) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20) // 1 MiB
	srv := grpc.NewServer()
	boltropev1.RegisterOrchestratorServiceServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// ---- flag-parsing tests ------------------------------------------------

// TestFlagParsing asserts that parseCLIFlags correctly reads --endpoint, --tenant,
// and --token from a string slice and that missing required flags produce an
// error.
func TestFlagParsing(t *testing.T) {
	t.Run("all required flags present", func(t *testing.T) {
		cfg, err := parseCLIFlags([]string{
			"--endpoint", "localhost:8443",
			"--tenant", "acme",
			"--token", "tok-abc",
		})
		require.NoError(t, err)
		assert.Equal(t, "localhost:8443", cfg.Endpoint)
		assert.Equal(t, "acme", cfg.Tenant)
		assert.Equal(t, "tok-abc", cfg.Token)
	})

	t.Run("missing endpoint returns error", func(t *testing.T) {
		_, err := parseCLIFlags([]string{
			"--tenant", "acme",
			"--token", "tok-abc",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "endpoint")
	})

	t.Run("tenant is optional", func(t *testing.T) {
		// Omitting --tenant must NOT error: an empty tenant_id makes the server use
		// the authenticated principal's tenant.
		cfg, err := parseCLIFlags([]string{
			"--endpoint", "localhost:8443",
		})
		require.NoError(t, err)
		assert.Empty(t, cfg.Tenant)
	})

	t.Run("insecure flag defaults false", func(t *testing.T) {
		cfg, err := parseCLIFlags([]string{
			"--endpoint", "localhost:8443",
			"--tenant", "acme",
			"--token", "tok-abc",
		})
		require.NoError(t, err)
		assert.False(t, cfg.Insecure)
	})

	t.Run("insecure flag can be set", func(t *testing.T) {
		cfg, err := parseCLIFlags([]string{
			"--endpoint", "localhost:8443",
			"--tenant", "acme",
			"--token", "tok-abc",
			"--insecure",
		})
		require.NoError(t, err)
		assert.True(t, cfg.Insecure)
	})

	t.Run("unknown flags are silently ignored", func(t *testing.T) {
		_, err := parseCLIFlags([]string{
			"--endpoint", "localhost:8443",
			"--tenant", "acme",
			"--token", "tok-abc",
			"--unknown-flag", "val",
		})
		// unknown flags should not cause a hard error (ContinueOnError behavior)
		// – only required-field validation may error.
		require.NoError(t, err)
	})
}

// ---- run-command streaming tests ---------------------------------------

// TestRunCommand_StreamsAndRendersFrames asserts that the run sub-command:
//   - creates a session when --session is absent,
//   - calls Run and prints TextDelta text live to stdout,
//   - prints ToolProgress messages, and
//   - prints the terminal result subtype.
func TestRunCommand_StreamsAndRendersFrames(t *testing.T) {
	fake := &fakeOrchestrator{
		createSessionID: "sess-test-001",
		runEvents: []*boltropev1.RunEvent{
			{
				Seq: 1,
				Payload: &boltropev1.RunEvent_TextDelta{
					TextDelta: &boltropev1.TextDelta{Text: "Hello, "},
				},
			},
			{
				Seq: 2,
				Payload: &boltropev1.RunEvent_TextDelta{
					TextDelta: &boltropev1.TextDelta{Text: "world!"},
				},
			},
			{
				Seq: 3,
				Payload: &boltropev1.RunEvent_ToolProgress{
					ToolProgress: &boltropev1.ToolProgress{Message: "running bash..."},
				},
			},
			{
				Seq: 4,
				Payload: &boltropev1.RunEvent_Result{
					Result: &boltropev1.RunResult{
						Subtype:   boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS,
						FinalText: "Done.",
						NumTurns:  1,
					},
				},
			},
		},
	}

	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant: "acme",
		Token:  "tok-test",
	}
	err := runCommand(context.Background(), client, cfg, "say hello", &out)
	require.NoError(t, err)

	output := out.String()
	// TextDelta chunks must be printed live (in order).
	assert.Contains(t, output, "Hello, ")
	assert.Contains(t, output, "world!")
	// ToolProgress must be surfaced.
	assert.Contains(t, output, "running bash...")
	// Terminal result subtype must be printed.
	assert.Contains(t, output, "success")
}

// TestRunCommand_ApprovalRequest asserts that when an ApprovalRequest frame arrives
// on the Run stream the CLI surfaces the tool name and call_id so the operator can
// use harnessctl approve to unblock it.
func TestRunCommand_ApprovalRequest(t *testing.T) {
	fake := &fakeOrchestrator{
		createSessionID: "sess-approval-001",
		runEvents: []*boltropev1.RunEvent{
			{
				Seq: 1,
				Payload: &boltropev1.RunEvent_ApprovalRequest{
					ApprovalRequest: &boltropev1.ApprovalRequest{
						CallId:   "call-xyz",
						ToolName: "bash",
						ArgsJson: `{"cmd":"rm -rf /tmp/test"}`,
						Reason:   "mode-gated",
					},
				},
			},
			{
				Seq: 2,
				Payload: &boltropev1.RunEvent_Result{
					Result: &boltropev1.RunResult{
						Subtype: boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION,
					},
				},
			},
		},
	}

	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{Tenant: "acme", Token: "tok-test"}
	// runCommand must not return an error just because there's an approval request.
	err := runCommand(context.Background(), client, cfg, "do dangerous thing", &out)
	require.NoError(t, err)

	output := out.String()
	assert.Contains(t, output, "call-xyz", "call_id must be visible so operator can issue approve")
	assert.Contains(t, output, "bash", "tool_name must be visible")
	assert.Contains(t, output, "mode-gated", "reason must be visible")
}

// TestRunCommand_ResumesWithAfterSeq asserts that when --after-seq is provided the
// RunRequest.AfterSeq field carries that value.
func TestRunCommand_ResumesWithAfterSeq(t *testing.T) {
	var capturedAfterSeq int64
	fake := &fakeOrchestrator{
		createSessionID: "sess-resume-001",
		runEvents: []*boltropev1.RunEvent{
			{
				Seq: 11,
				Payload: &boltropev1.RunEvent_Result{
					Result: &boltropev1.RunResult{
						Subtype: boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS,
					},
				},
			},
		},
	}

	// Override Run to capture after_seq.
	srv := &afterSeqCapturingServer{
		UnimplementedOrchestratorServiceServer: boltropev1.UnimplementedOrchestratorServiceServer{},
		fake:                                   fake,
		capturedAfterSeq:                       &capturedAfterSeq,
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	boltropev1.RegisterOrchestratorServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)
	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant:    "acme",
		Token:     "tok-test",
		SessionID: "sess-resume-001",
		AfterSeq:  10,
	}
	err = runCommand(context.Background(), client, cfg, "", &out)
	require.NoError(t, err)
	assert.Equal(t, int64(10), capturedAfterSeq, "RunRequest.AfterSeq must carry the --after-seq value")
}

// afterSeqCapturingServer wraps fakeOrchestrator and captures RunRequest.AfterSeq.
type afterSeqCapturingServer struct {
	boltropev1.UnimplementedOrchestratorServiceServer
	fake             *fakeOrchestrator
	capturedAfterSeq *int64
}

func (s *afterSeqCapturingServer) CreateSession(ctx context.Context, req *boltropev1.CreateSessionRequest) (*boltropev1.CreateSessionResponse, error) {
	return s.fake.CreateSession(ctx, req)
}

func (s *afterSeqCapturingServer) Run(req *boltropev1.RunRequest, stream boltropev1.OrchestratorService_RunServer) error {
	if s.capturedAfterSeq != nil {
		*s.capturedAfterSeq = req.AfterSeq
	}
	return s.fake.Run(req, stream)
}

func (s *afterSeqCapturingServer) Control(ctx context.Context, req *boltropev1.ControlRequest) (*boltropev1.ControlResponse, error) {
	return s.fake.Control(ctx, req)
}

// ---- approve / deny / interrupt tests ----------------------------------

// TestApproveCommand_IssuesControlCall asserts that the approve sub-command calls
// Control with an Approve action carrying the supplied call_id and that the
// server's response head_seq is printed.
func TestApproveCommand_IssuesControlCall(t *testing.T) {
	fake := &fakeOrchestrator{}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant:    "acme",
		Token:     "tok-test",
		SessionID: "sess-test-001",
	}
	err := approveCommand(context.Background(), client, cfg, "call-abc", &out)
	require.NoError(t, err)

	require.Len(t, fake.controlRequests, 1, "exactly one Control call expected")
	req := fake.controlRequests[0]
	assert.Equal(t, "sess-test-001", req.SessionId)
	assert.Equal(t, "acme", req.TenantId)
	ap := req.GetApprove()
	require.NotNil(t, ap, "action must be Approve")
	assert.Equal(t, "call-abc", ap.CallId)

	assert.Contains(t, out.String(), "approved", "CLI output should confirm the action")
}

// TestDenyCommand_IssuesControlCall asserts that the deny sub-command calls
// Control with a Deny action carrying the supplied call_id and reason.
func TestDenyCommand_IssuesControlCall(t *testing.T) {
	fake := &fakeOrchestrator{}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant:    "acme",
		Token:     "tok-test",
		SessionID: "sess-test-001",
	}
	err := denyCommand(context.Background(), client, cfg, "call-abc", "too dangerous", &out)
	require.NoError(t, err)

	require.Len(t, fake.controlRequests, 1)
	req := fake.controlRequests[0]
	d := req.GetDeny()
	require.NotNil(t, d, "action must be Deny")
	assert.Equal(t, "call-abc", d.CallId)
	assert.Equal(t, "too dangerous", d.Reason)
	assert.Contains(t, out.String(), "denied")
}

// TestInterruptCommand_IssuesControlCall asserts that the interrupt sub-command
// calls Control with an Interrupt action.
func TestInterruptCommand_IssuesControlCall(t *testing.T) {
	fake := &fakeOrchestrator{}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant:    "acme",
		Token:     "tok-test",
		SessionID: "sess-test-001",
	}
	err := interruptCommand(context.Background(), client, cfg, &out)
	require.NoError(t, err)

	require.Len(t, fake.controlRequests, 1)
	req := fake.controlRequests[0]
	require.NotNil(t, req.GetInterrupt(), "action must be Interrupt")
	assert.Contains(t, out.String(), "interrupt")
}

// ---- fork test ---------------------------------------------------------

// TestForkCommand_IssuesForkCall asserts that the fork sub-command calls Fork
// and prints the new child session ID.
func TestForkCommand_IssuesForkCall(t *testing.T) {
	fake := &fakeOrchestrator{forkChildID: "sess-child-999"}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{
		Tenant:    "acme",
		Token:     "tok-test",
		SessionID: "sess-parent-001",
	}
	err := forkCommand(context.Background(), client, cfg, 5, &out)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "sess-child-999", "child session id must be printed")
}

// ---- gRPC-error surfacing test -----------------------------------------

// TestRunCommand_GRPCErrorSurfaced asserts that when the Run RPC returns a
// non-OK status the CLI returns a non-nil error.
func TestRunCommand_GRPCErrorSurfaced(t *testing.T) {
	// Server always returns PermissionDenied on Run.
	fake := &erroringOrchestrator{runCode: codes.PermissionDenied}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	boltropev1.RegisterOrchestratorServiceServer(gs, fake)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)
	var out bytes.Buffer
	cfg := &cliConfig{Tenant: "acme", Token: "tok-test"}
	err = runCommand(context.Background(), client, cfg, "task", &out)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) ||
		status.Code(err) == codes.PermissionDenied ||
		strings.Contains(err.Error(), "PermissionDenied"),
		"error must surface the gRPC status")
}

// erroringOrchestrator is a fake server that always returns a specific status.
type erroringOrchestrator struct {
	boltropev1.UnimplementedOrchestratorServiceServer
	runCode codes.Code
}

func (e *erroringOrchestrator) CreateSession(_ context.Context, _ *boltropev1.CreateSessionRequest) (*boltropev1.CreateSessionResponse, error) {
	return &boltropev1.CreateSessionResponse{SessionId: "sess-err-001"}, nil
}

func (e *erroringOrchestrator) Run(_ *boltropev1.RunRequest, _ boltropev1.OrchestratorService_RunServer) error {
	return status.Error(e.runCode, e.runCode.String())
}

// ---- session-creation tests --------------------------------------------

// TestCreateSessionCommand_PrintsSessionID asserts that the session sub-command
// creates a session and prints its ID.
func TestCreateSessionCommand_PrintsSessionID(t *testing.T) {
	fake := &fakeOrchestrator{createSessionID: "sess-new-123"}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{Tenant: "acme", Token: "tok-test"}
	err := createSessionCommand(context.Background(), client, cfg, &out)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "sess-new-123")
}

// TestParsePermissionMode covers the --permission-mode string -> wire enum
// mapping, including the snake/camel aliases and the invalid-input error.
func TestParsePermissionMode(t *testing.T) {
	cases := []struct {
		in      string
		want    boltropev1.PermissionMode
		wantErr bool
	}{
		{"", boltropev1.PermissionMode_PERMISSION_MODE_UNSPECIFIED, false},
		{"default", boltropev1.PermissionMode_PERMISSION_MODE_DEFAULT, false},
		{"acceptEdits", boltropev1.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, false},
		{"accept-edits", boltropev1.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, false},
		{"PLAN", boltropev1.PermissionMode_PERMISSION_MODE_PLAN, false},
		{"bypass", boltropev1.PermissionMode_PERMISSION_MODE_BYPASS, false},
		{"nonsense", boltropev1.PermissionMode_PERMISSION_MODE_UNSPECIFIED, true},
	}
	for _, c := range cases {
		got, err := parsePermissionMode(c.in)
		if c.wantErr {
			assert.Error(t, err, "input %q", c.in)
			continue
		}
		assert.NoError(t, err, "input %q", c.in)
		assert.Equal(t, c.want, got, "input %q", c.in)
	}
}

// TestCreateSessionCommand_SendsPermissionMode asserts the CLI threads
// --permission-mode through to the CreateSession request (ADR-0019).
func TestCreateSessionCommand_SendsPermissionMode(t *testing.T) {
	fake := &fakeOrchestrator{createSessionID: "sess-mode-1"}
	conn := newFakeServer(t, fake)
	client := boltropev1.NewOrchestratorServiceClient(conn)

	var out bytes.Buffer
	cfg := &cliConfig{Tenant: "acme", PermissionMode: "plan"}
	require.NoError(t, createSessionCommand(context.Background(), client, cfg, &out))
	assert.Equal(t, boltropev1.PermissionMode_PERMISSION_MODE_PLAN, fake.lastCreateMode)

	// An invalid mode is rejected client-side before any RPC.
	bad := &cliConfig{Tenant: "acme", PermissionMode: "nope"}
	assert.Error(t, createSessionCommand(context.Background(), client, bad, &out))
}

// ---- compile-time flag-parsing surface test ----------------------------

// TestParseCLIFlags_FlagSetIsPrivate verifies that parseCLIFlags uses a private
// flag.FlagSet and does not pollute flag.CommandLine (the global set).
func TestParseCLIFlags_FlagSetIsPrivate(t *testing.T) {
	before := make(map[string]bool)
	flag.CommandLine.Visit(func(f *flag.Flag) { before[f.Name] = true })

	_, _ = parseCLIFlags([]string{"--endpoint", "localhost:8443", "--tenant", "t", "--token", "x"})

	after := make(map[string]bool)
	flag.CommandLine.Visit(func(f *flag.Flag) { after[f.Name] = true })

	// parseCLIFlags must not have set any new flags on flag.CommandLine.
	for k := range after {
		assert.True(t, before[k], "parseCLIFlags must not pollute flag.CommandLine with %q", k)
	}
}

// ---- subcommand-boundary tests (regression for the Quickstart parse bug) ----

// TestSplitArgs_SpaceSeparatedGlobalFlags asserts splitArgs treats the VALUE of a
// space-separated global flag as part of the global args, not as the subcommand.
// The documented `harnessctl --endpoint host run "msg"` form regressed here: the
// host was parsed as the subcommand, leaving --endpoint with no argument so flag
// parsing failed with "flag needs an argument: -endpoint".
func TestSplitArgs_SpaceSeparatedGlobalFlags(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantGlob []string
		wantCmd  string
		wantSub  []string
	}{
		{
			name:     "endpoint space form then run",
			args:     []string{"--endpoint", "localhost:9000", "run", "hello world"},
			wantGlob: []string{"--endpoint", "localhost:9000"},
			wantCmd:  "run",
			wantSub:  []string{"hello world"},
		},
		{
			name:     "multiple value flags then run",
			args:     []string{"--endpoint", "h:1", "--tenant", "t", "run", "msg"},
			wantGlob: []string{"--endpoint", "h:1", "--tenant", "t"},
			wantCmd:  "run",
			wantSub:  []string{"msg"},
		},
		{
			name:     "equals form still works",
			args:     []string{"--endpoint=h:1", "run", "msg"},
			wantGlob: []string{"--endpoint=h:1"},
			wantCmd:  "run",
			wantSub:  []string{"msg"},
		},
		{
			name:     "boolean insecure consumes no value",
			args:     []string{"--insecure", "session"},
			wantGlob: []string{"--insecure"},
			wantCmd:  "session",
			wantSub:  []string{},
		},
		{
			name:     "insecure before a value flag then subcommand",
			args:     []string{"--insecure", "--endpoint", "h:1", "run", "msg"},
			wantGlob: []string{"--insecure", "--endpoint", "h:1"},
			wantCmd:  "run",
			wantSub:  []string{"msg"},
		},
		{
			name:     "double-dash terminator forces the subcommand",
			args:     []string{"--endpoint=h:1", "--", "run", "msg"},
			wantGlob: []string{"--endpoint=h:1"},
			wantCmd:  "run",
			wantSub:  []string{"msg"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			glob, cmd, sub := splitArgs(tc.args)
			assert.Equal(t, tc.wantGlob, glob, "global args")
			assert.Equal(t, tc.wantCmd, cmd, "subcommand")
			assert.Equal(t, tc.wantSub, sub, "subcommand args")
		})
	}
}

// TestSplitArgsThenParse_DocumentedQuickstartForm reproduces the exact reported
// failure end-of-parse: the README Quickstart `--endpoint localhost:9000 run
// "hello world"` must split so parseCLIFlags reads the endpoint (no "flag needs an
// argument" error) and the run subcommand carries the message.
func TestSplitArgsThenParse_DocumentedQuickstartForm(t *testing.T) {
	glob, cmd, sub := splitArgs([]string{"--endpoint", "localhost:9000", "run", "hello world"})
	require.Equal(t, "run", cmd)
	require.Equal(t, []string{"hello world"}, sub)

	cfg, err := parseCLIFlags(glob)
	require.NoError(t, err, "the endpoint value must attach to --endpoint, not be consumed as the subcommand")
	assert.Equal(t, "localhost:9000", cfg.Endpoint)
}

// TestRun_SpaceSeparatedEndpoint_EndToEnd drives the top-level run() entrypoint
// (which calls splitArgs) with the documented space-separated --endpoint form
// against a real plaintext gRPC server, asserting it dials and streams the run to
// a terminal result. This is the end-to-end guard the parseCLIFlags tests missed —
// they fed pre-split args and so never exercised splitArgs through run().
func TestRun_SpaceSeparatedEndpoint_EndToEnd(t *testing.T) {
	t.Setenv(devInsecureEnv, "") // force the --insecure plaintext path, not dev mTLS

	fake := &fakeOrchestrator{
		createSessionID: "sess-e2e-001",
		runEvents: []*boltropev1.RunEvent{
			{Seq: 1, Payload: &boltropev1.RunEvent_TextDelta{TextDelta: &boltropev1.TextDelta{Text: "hi"}}},
			{Seq: 2, Payload: &boltropev1.RunEvent_Result{Result: &boltropev1.RunResult{
				Subtype:   boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS,
				FinalText: "done",
			}}},
		},
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer()
	boltropev1.RegisterOrchestratorServiceServer(gs, fake)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.GracefulStop)

	var out bytes.Buffer
	err = run([]string{"--endpoint", lis.Addr().String(), "--insecure", "run", "hello world"}, &out)
	require.NoError(t, err, "space-separated --endpoint must dial and run end-to-end")

	output := out.String()
	assert.Contains(t, output, "sess-e2e-001", "the created session id must be printed")
	assert.Contains(t, output, "hi", "streamed text must be rendered")
	assert.Contains(t, output, "success", "the terminal result subtype must be printed")
}
