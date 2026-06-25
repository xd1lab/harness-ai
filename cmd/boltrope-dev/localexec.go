// SPDX-License-Identifier: Apache-2.0

package main

// ADR-0029 (AC-6 / AC-7 / AC-9 / AC-10) — the dev binary's LOCAL-EXEC opt-in.
//
// By DEFAULT the dev loop's app.ToolRuntimePort is the in-process, no-exec
// [Runtime] (toolruntime.go), which advertises a host-side-effect-free subset and
// REFUSES bash. When the operator opts in with --enable-local-exec, the loop's
// tool port becomes an IN-PROCESS BRIDGE over the tool-runtime execute.Service
// backed by the REAL Docker container runtime (per-session container,
// --network none, cgroup/PID limits via runtime.DefaultConfig), the in-memory
// dedup ledger (memDedup; no pgx), an FS blob store (a temp dir), and a
// deny-by-default egress broker.
//
// In production the orchestrator reaches the tool-runtime over a gRPC client (the
// toolrt adapter). That adapter is FORBIDDEN in the dev binary (ADR-0024 dep
// exclusion). The bridge here replaces it with a direct, in-process call into the
// execute.Service — importing only the clean execute/registry/runtime/egress/
// egressclient/blob/tools/domain packages, none of which drag pgx/spiffe/Service
// (verified by go list -deps; ADR-0029 AC-16).

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	orchapp "github.com/xd1lab/harness-ai/internal/orchestrator/app"
	orchdomain "github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/egress"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/egressclient"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/runtime"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/registry"
	trapp "github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/execute"
	trdomain "github.com/xd1lab/harness-ai/internal/toolruntime/domain"
	"github.com/xd1lab/harness-ai/internal/toolruntime/tools"
)

// maxDevBlobBytes caps a single offloaded dev tool-output blob (16 MiB), matching
// the production toolruntimed cap. Output larger than this is rejected by the FS
// blob store; the execute service offloads output over its inline threshold.
const maxDevBlobBytes int64 = 16 * 1024 * 1024

// resolveTools selects the loop's app.ToolRuntimePort from the resolved flags.
//
//   - When cfg.enableLocalExec is false (the DEFAULT): it returns the in-process,
//     no-exec [Runtime], byte-identical to the pre-ADR-0029 behavior. No Docker is
//     touched and no temp dir is created.
//   - When cfg.enableLocalExec is true: it builds the in-process bridge over the
//     execute.Service backed by the real Docker runtime and returns it. The
//     returned port is NEVER the *Runtime. Construction succeeds WITHOUT Docker
//     (the runtime dials the docker CLI lazily on first Create); only the first
//     real tool execution requires a reachable daemon.
//
// On the local-exec path the FS blob store's temp dir is created here; the caller
// (server.go) threads the returned cleanup into serveResult.Shutdown so the dir is
// removed on exit. On the default path no cleanup is needed (a no-op).
func resolveTools(cfg parsedRunFlags, env map[string]string) (orchapp.ToolRuntimePort, error) {
	if !cfg.enableLocalExec {
		return newRuntime(), nil
	}
	_, bridge, _, err := buildLocalExec(env)
	if err != nil {
		return nil, err
	}
	return bridge, nil
}

// buildLocalExec wires the in-process local-exec stack: the real Docker container
// runtime, a deny-by-default egress broker, the egress data-path fetcher, the
// native tool set bound to a per-session workspace router, the in-memory dedup
// ledger, and an FS blob store under a fresh temp dir. It returns the
// execute.Service, the orchestrator-side bridge over it, and a cleanup func that
// removes the temp dir.
//
// Construction never blocks on or requires Docker: runtime.New only records the
// docker CLI binary; the daemon is dialed lazily on the first Workspace.Create. A
// host without Docker can therefore construct the bridge and assert its wiring;
// only a real tool execution fails (surfaced as an error result, not a panic).
func buildLocalExec(env map[string]string) (*execute.Service, orchapp.ToolRuntimePort, func() error, error) {
	// (1) Container runtime. runtime.New + egressclient.New BOTH return (T, error)
	// and so cannot be inlined into the execute.Config composite literal; assign and
	// check each first. The conservative DefaultConfig (per-session container,
	// --network none, cgroup/PID limits) is overlaid with the prod env knobs so the
	// dev sandbox reuses the same image + docker binary as production.
	rcfg := runtime.DefaultConfig()
	if v := env["BOLTROPE_TOOLRT_IMAGE"]; v != "" {
		rcfg.Image = v
	}
	if v := env["BOLTROPE_TOOLRT_DOCKER_BIN"]; v != "" {
		rcfg.DockerBin = v
	}
	rt, err := runtime.New(rcfg)
	if err != nil {
		return nil, nil, nil, err
	}

	// (2) Deny-by-default egress broker (empty default allowlist = deny all). The
	// web tools (webfetch/websearch) therefore always deny in dev, matching the
	// --network none posture.
	broker := egress.New(egress.WithDefaultAllowedHosts(nil))

	fetcher, err := egressclient.New(broker, egressclient.Config{})
	if err != nil {
		return nil, nil, nil, err
	}

	// (3) Per-session workspace router with a deny-by-default policy (empty
	// allowlist) and the native tool set bound to it (searchURL="" — websearch then
	// denies unless a host is allowlisted, which it never is in dev).
	ws := newDevWorkspaces(rt)
	reg := registry.New(nil)
	for _, tool := range tools.Native(ws, fetcher, "") {
		if err := reg.Register(context.Background(), tool); err != nil {
			return nil, nil, nil, err
		}
	}

	// (4) FS blob store under a fresh temp dir + in-memory dedup ledger (no pgx).
	tmpDir, err := os.MkdirTemp("", "boltrope-dev-blobs")
	if err != nil {
		return nil, nil, nil, err
	}
	blobs := blob.NewFSStore(tmpDir, maxDevBlobBytes)

	svc, err := execute.NewService(execute.Config{
		Registry: reg,
		Runtime:  rt,
		Egress:   broker,
		Dedup:    newMemDedup(),
		Blobs:    blobs,
	})
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, nil, nil, err
	}

	cleanup := func() error { return os.RemoveAll(tmpDir) }
	bridge := newExecBridge(serviceExecutor{svc: svc, reg: reg})
	return svc, bridge, cleanup, nil
}

// ----------------------------------------------------------------------------
// devWorkspaces — the dev re-implementation of the production sessionWorkspaces
// (which lives in package main of cmd/boltrope-toolruntimed and is NOT
// importable). It routes each tool call to the calling session's own sandbox,
// provisioning it lazily with a deny-by-default egress policy (empty allowlist).
// ----------------------------------------------------------------------------

// devWorkspaces routes every tool call to the CALLING session's own Docker
// sandbox, provisioning it on first use with a deny-by-default egress policy. Two
// sessions never share a workspace (per-session isolation is the v1 containment
// boundary), so an empty session id is REFUSED rather than routed to a shared
// fallback. A per-session lock serializes the get-or-create so a concurrent first
// use provisions exactly one container per session.
type devWorkspaces struct {
	rt trapp.RuntimePort

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// newDevWorkspaces returns a [devWorkspaces] router over rt.
func newDevWorkspaces(rt trapp.RuntimePort) *devWorkspaces {
	return &devWorkspaces{rt: rt, locks: make(map[string]*sync.Mutex)}
}

// Workspace returns sessionID's live workspace, creating it on first use with a
// deny-by-default (empty allowlist) egress policy. An empty session id is refused.
func (r *devWorkspaces) Workspace(ctx context.Context, sessionID string) (trapp.Workspace, error) {
	if sessionID == "" {
		return nil, errors.New("boltrope-dev: tool call carries no session id; refusing shared-sandbox fallback")
	}
	lock := r.lockFor(sessionID)
	lock.Lock()
	defer lock.Unlock()

	if ws, err := r.rt.Get(ctx, sessionID); err == nil && ws != nil {
		return ws, nil
	}
	return r.rt.Create(ctx, sessionID, trapp.EgressPolicy{
		SessionID:    sessionID,
		AllowedHosts: nil, // deny-by-default: empty allowlist denies all egress.
	})
}

// lockFor returns the per-session mutex, creating it on first use.
func (r *devWorkspaces) lockFor(sessionID string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.locks[sessionID]
	if !ok {
		l = &sync.Mutex{}
		r.locks[sessionID] = l
	}
	return l
}

var _ trapp.SessionWorkspaces = (*devWorkspaces)(nil)

// ----------------------------------------------------------------------------
// execBridge — the in-process orchestrator app.ToolRuntimePort over the
// tool-runtime execute.Service (AC-9).
// ----------------------------------------------------------------------------

// executor is the minimal projection of the tool-runtime use case the bridge
// drives: one tool execution plus tool enumeration. The production
// *execute.Service has no ListTools method (that lives on the registry), so the
// real wiring composes the two via [serviceExecutor]; tests use a fake.
type executor interface {
	Execute(ctx context.Context, req execute.Request, em execute.Emitter) (execute.Result, error)
	ListTools(ctx context.Context) ([]trdomain.ToolSpec, error)
}

// serviceExecutor composes the execute.Service (Execute) with the registry
// (ListTools via List) so the pair satisfies [executor]. It is the real-wiring
// adapter the local-exec path constructs; the bridge stays decoupled from the
// concrete service so it is unit-testable against a fake.
type serviceExecutor struct {
	svc *execute.Service
	reg *registry.Registry
}

func (e serviceExecutor) Execute(ctx context.Context, req execute.Request, em execute.Emitter) (execute.Result, error) {
	return e.svc.Execute(ctx, req, em)
}

func (e serviceExecutor) ListTools(ctx context.Context) ([]trdomain.ToolSpec, error) {
	return e.reg.List(ctx)
}

// execBridge satisfies the orchestrator [orchapp.ToolRuntimePort] by converting
// orchestrator-side tool calls into tool-runtime execute.Requests, driving the
// execute use case, and streaming the single terminal result back as an
// app.ToolEvent. It carries the dev synthetic single-tenant id ([igrpc.DevTenantID])
// on every Request so the dedup ledger keys and any tenant re-check stay
// consistent with the principal the loop runs under.
type execBridge struct {
	exec executor
}

// newExecBridge returns the bridge over ex.
func newExecBridge(ex executor) *execBridge {
	return &execBridge{exec: ex}
}

var _ orchapp.ToolRuntimePort = (*execBridge)(nil)

// ExecuteTool converts exec into a tool-runtime execute.Request, runs it, and
// returns a single-shot [orchapp.ToolStream] yielding the terminal result then
// io.EOF. A non-nil Go error from the service denotes an infrastructure failure
// and is propagated; a tool that ran but errored is surfaced inside the result's
// IsError (never a Go error). Call.Args is already the parsed argument map
// ([llm.ToolCall.Args] is map[string]any), passed through unchanged.
func (b *execBridge) ExecuteTool(ctx context.Context, exec orchapp.ToolExecution) (orchapp.ToolStream, error) {
	req := execute.Request{
		TenantID:       igrpc.DevTenantID,
		SessionID:      exec.SessionID,
		CallID:         exec.Call.ID,
		ToolName:       exec.Call.Name,
		Args:           exec.Call.Args,
		IdempotencyKey: exec.IdempotencyKey,
	}
	res, err := b.exec.Execute(ctx, req, nil) // nil Emitter => no progress.
	if err != nil {
		return nil, err
	}
	return newBridgeStream(orchapp.ToolResult{
		Content:   res.Observation.Content,
		IsError:   res.Observation.IsError,
		Truncated: res.Observation.Truncated,
		BlobRef:   res.Observation.BlobRef,
	}), nil
}

// ListTools enumerates the registered tool specs and maps each into an
// orchestrator [orchapp.ToolDescriptor], translating the tool-runtime
// SideEffect/EgressClass enums into the orchestrator's DISTINCT (but
// string-equal) enums. Unrecognized values translate to the maximally-gated class
// (mutating / external) — fail-safe.
func (b *execBridge) ListTools(ctx context.Context, _ string) ([]orchapp.ToolDescriptor, error) {
	specs, err := b.exec.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	descs := make([]orchapp.ToolDescriptor, 0, len(specs))
	for _, spec := range specs {
		descs = append(descs, orchapp.ToolDescriptor{
			Name:        spec.Name,
			Description: spec.Description,
			JSONSchema:  []byte(spec.JSONSchema),
			SideEffect:  toOrchSideEffect(spec.SideEffect),
			EgressClass: toOrchEgressClass(spec.EgressClass),
		})
	}
	return descs, nil
}

// toOrchSideEffect translates a tool-runtime [trdomain.SideEffect] into the
// orchestrator's distinct [orchdomain.SideEffect]. An unrecognized value
// fail-safes to mutating (serialized + gated).
func toOrchSideEffect(s trdomain.SideEffect) orchdomain.SideEffect {
	switch s {
	case trdomain.SideEffectReadOnly:
		return orchdomain.SideEffectReadOnly
	case trdomain.SideEffectMutating:
		return orchdomain.SideEffectMutating
	default:
		return orchdomain.SideEffectMutating
	}
}

// toOrchEgressClass translates a tool-runtime [trdomain.EgressClass] into the
// orchestrator's distinct [orchdomain.EgressClass]. An unrecognized value
// fail-safes to external (the maximally-gated class).
func toOrchEgressClass(e trdomain.EgressClass) orchdomain.EgressClass {
	switch e {
	case trdomain.EgressClassNone:
		return orchdomain.EgressClassNone
	case trdomain.EgressClassInternal:
		return orchdomain.EgressClassInternal
	case trdomain.EgressClassExternal:
		return orchdomain.EgressClassExternal
	default:
		return orchdomain.EgressClassExternal
	}
}

// bridgeStream is a minimal [orchapp.ToolStream] yielding one terminal result
// then io.EOF (the bridge collapses the use case's single terminal Result; the
// dev local-exec path streams no interim progress to the loop).
type bridgeStream struct {
	result orchapp.ToolResult
	done   bool
}

// newBridgeStream wraps a terminal result as a single-shot stream.
func newBridgeStream(result orchapp.ToolResult) *bridgeStream {
	return &bridgeStream{result: result}
}

// Recv returns the terminal result once, then io.EOF.
func (s *bridgeStream) Recv() (orchapp.ToolEvent, error) {
	if s.done {
		return orchapp.ToolEvent{}, io.EOF
	}
	s.done = true
	r := s.result
	return orchapp.ToolEvent{Result: &r}, nil
}

// Close is a no-op; there are no resources to release.
func (s *bridgeStream) Close() error { return nil }

var _ orchapp.ToolStream = (*bridgeStream)(nil)
