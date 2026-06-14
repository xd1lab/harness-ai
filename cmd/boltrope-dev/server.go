// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/stub"
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/rest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
)

// httpReadHeaderTimeout bounds the header-read phase of an inbound REST request
// (the slowloris guard; gosec G112). It is generous because this is a loopback
// dev edge, not a public listener.
const httpReadHeaderTimeout = 10 * time.Second

// serveOpts parameterizes [newServer]. A "127.0.0.1:0" address binds an ephemeral
// loopback port (used by the e2e tests); the resolved address is read back off the
// returned [serveResult].
type serveOpts struct {
	// GRPCAddr is the gRPC listen address.
	GRPCAddr string
	// HTTPAddr is the REST/SSE listen address.
	HTTPAddr string
}

// serveResult is the running dev server: the resolved (post-bind) listen
// addresses plus a Shutdown hook. It assembles the MAXIMUM existing wiring with
// zero core change — exactly the in-process loop test/eval/harness.go already
// proves: agent.NewLoop over the real policy.Engine, the dev in-memory Store, the
// dev no-exec Runtime, and the keyless stub provider wrapped by devModel, fronted
// by igrpc.NewLoopRunner -> igrpc.NewServer and exposed via rest.NewHandler on a
// plaintext loopback HTTP listener (no mTLS/OIDC).
type serveResult struct {
	// GRPCAddr is the resolved gRPC listen address (the ephemeral port filled in
	// when the request used :0).
	GRPCAddr string
	// HTTPAddr is the resolved REST/SSE listen address.
	HTTPAddr string

	grpcServer *grpc.Server
	httpServer *http.Server
}

// newServer assembles and STARTS the dev server on the (possibly ephemeral)
// loopback addresses in opts, returning the running [serveResult]. The auth
// posture is dev-insecure (devAuthConfig): a synthetic single-tenant principal is
// injected on both edges so igrpc's authorizeTenant runs the same code path.
func newServer(opts serveOpts) (*serveResult, error) {
	authCfg := devAuthConfig()

	// One shared authenticator backs the REST facade AND the gRPC interceptors —
	// identical (dev-insecure) auth on every transport.
	auth, err := igrpc.NewAuthenticator(authCfg)
	if err != nil {
		return nil, fmt.Errorf("boltrope-dev: build authenticator: %w", err)
	}

	// The agent loop's dependency set: real policy engine, dev in-memory store, dev
	// no-exec runtime, stub-backed dev model, injected system clock/ids. The
	// approval gate denies by default (the stub never asks; a no-exec tool call is
	// gated but never executes), and hooks allow everything.
	store := newStore()

	pol, err := policy.NewEngine(policy.Config{})
	if err != nil {
		return nil, fmt.Errorf("boltrope-dev: build policy engine: %w", err)
	}

	gate := newDenyGate()
	deps := agent.Deps{
		EventLog:  store,
		Model:     newDevModel(stub.New()),
		Tools:     newRuntime(),
		Approvals: gate,
		Hooks:     newAllowHooks(),
		Policy:    pol,
		Context:   nil, // nil Context => the loop builds the window without compaction.
		Clock:     clock.System{},
		IDs:       ids.System{},
	}
	loopCfg := agent.Config{Model: "stub"}

	runner := igrpc.NewLoopRunner(deps, loopCfg)
	srv := igrpc.NewServer(store, gate, runner, ids.System{}, igrpc.Config{DefaultModel: "stub"})

	// gRPC server with the dev-insecure edge-auth interceptors (the SAME
	// interceptors the production daemon installs, on the SAME AuthConfig path —
	// dev only flips DevInsecure). Plaintext transport: harnessctl --insecure.
	unary, err := igrpc.NewAuthInterceptor(authCfg)
	if err != nil {
		return nil, fmt.Errorf("boltrope-dev: build unary auth interceptor: %w", err)
	}
	stream, err := igrpc.NewStreamAuthInterceptor(authCfg)
	if err != nil {
		return nil, fmt.Errorf("boltrope-dev: build stream auth interceptor: %w", err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
	)
	genproto.RegisterOrchestratorServiceServer(grpcServer, srv)

	// REST/JSON + SSE facade over the SAME server + the SAME authenticator.
	mux := http.NewServeMux()
	rest.NewHandler(srv, auth).Routes(mux)
	httpServer := &http.Server{
		Handler: mux,
		// A header-read timeout bounds a slowloris client even on this loopback-only
		// dev edge (gosec G112); the SSE Run stream keeps the body open after headers,
		// so only the header phase is bounded.
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}

	// Bind both listeners up front so the resolved (ephemeral) ports are known
	// before any goroutine serves.
	grpcLn, err := net.Listen("tcp", opts.GRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("boltrope-dev: listen gRPC %q: %w", opts.GRPCAddr, err)
	}
	httpLn, err := net.Listen("tcp", opts.HTTPAddr)
	if err != nil {
		_ = grpcLn.Close()
		return nil, fmt.Errorf("boltrope-dev: listen HTTP %q: %w", opts.HTTPAddr, err)
	}

	res := &serveResult{
		GRPCAddr:   grpcLn.Addr().String(),
		HTTPAddr:   httpLn.Addr().String(),
		grpcServer: grpcServer,
		httpServer: httpServer,
	}

	go func() { _ = grpcServer.Serve(grpcLn) }()
	go func() { _ = httpServer.Serve(httpLn) }()

	return res, nil
}

// Shutdown gracefully stops both listeners. It is bounded by ctx: the HTTP server
// is asked to shut down gracefully, and the gRPC server is stopped (a forceful
// Stop on ctx expiry so Shutdown always returns).
func (r *serveResult) Shutdown(ctx context.Context) error {
	httpErr := r.httpServer.Shutdown(ctx)

	done := make(chan struct{})
	go func() {
		r.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		r.grpcServer.Stop()
	}
	return httpErr
}

// denyGate is the dev binary's [app.ApprovalGate]: it denies every ask
// immediately. The keyless stub never requests a tool, and the no-exec runtime
// gates but never executes a tool, so a default-mode run reaches a clean terminal
// without ever blocking on a human approver (the headless dev path has none).
type denyGate struct{}

func newDenyGate() *denyGate { return &denyGate{} }

// Request denies every ask immediately.
func (*denyGate) Request(_ context.Context, _ app.ApprovalRequest) (domain.AskResolution, error) {
	return domain.AskDenied, nil
}

// Resolve is a no-op: nothing ever blocks on this gate, so there is no pending
// request to resolve.
func (*denyGate) Resolve(_ context.Context, _, _ string, _ domain.AskResolution) error { return nil }

var _ app.ApprovalGate = (*denyGate)(nil)

// allowHooks is the dev binary's no-op [app.HookRunner]: it allows every
// lifecycle event.
type allowHooks struct{}

func newAllowHooks() *allowHooks { return &allowHooks{} }

// Run allows every lifecycle event.
func (*allowHooks) Run(_ context.Context, _ app.HookInput) (app.HookDecision, error) {
	return app.HookDecision{Allow: true}, nil
}

var _ app.HookRunner = (*allowHooks)(nil)
