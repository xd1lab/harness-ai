// Command harnessctl is the thin gRPC client CLI for the Boltrope orchestrator
// (T-CMD-03 / DOD-09).  It provides subcommands to create and start sessions,
// run tasks (streaming TextDelta and ToolProgress live while surfacing
// ApprovalRequests), and issue out-of-band control actions (approve, deny,
// interrupt) plus fork over the OrchestratorService gRPC API.
//
// # Connection
//
// The target endpoint and authentication credentials are configured via flags or
// the corresponding BOLTROPE_CTL_* environment variables:
//
//	--endpoint  / BOLTROPE_CTL_ENDPOINT   gRPC target (host:port)
//	--tenant    / BOLTROPE_CTL_TENANT     tenant id (OPTIONAL — omit to use the
//	                                      authenticated tenant; if set it must
//	                                      match the authenticated principal)
//	--token     / BOLTROPE_CTL_TOKEN      bearer token sent as "authorization" metadata
//	--insecure  / BOLTROPE_CTL_INSECURE   plaintext transport, no TLS (local-only)
//	--permission-mode / BOLTROPE_CTL_PERMISSION_MODE  mode for a NEWLY created
//	                                      session: default|acceptEdits|plan
//	                                      (bypass is operator-only — server rejects it)
//
// The CLI chooses transport security in this order:
//
//   - BOLTROPE_DEV_INSECURE=1 → the SHARED-SEED static-cert dev mTLS path
//     ([grpcx.StaticDevClientCredentials]). The client presents the
//     spiffe://<trust-domain>/edge identity (the one the orchestrator's RBAC
//     admits to the client-facing RPCs) and pins the orchestrator's SPIFFE id
//     (spiffe://<trust-domain>/orchestrator by default). Because the dev CA is
//     derived from a shared seed (BOLTROPE_DEV_CA_SEED), this completes mutual
//     TLS against a `docker compose` edge brought up with the same env — this is
//     the documented Quickstart path against the compose mTLS edge.
//   - else --insecure / BOLTROPE_CTL_INSECURE → plaintext, no TLS. Only for a
//     locally running orchestrator started WITHOUT mTLS; it cannot handshake the
//     compose mTLS edge.
//   - else → an error directing the operator to enable one of the above (a
//     SPIRE-backed SPIFFE source for production is a later wiring task).
//
// The dev trust domain and pinned server id are configurable for non-default
// stacks:
//
//	--trust-domain / BOLTROPE_TRUST_DOMAIN     SPIFFE trust domain (default boltrope.local)
//	--server-id    / BOLTROPE_CTL_SERVER_ID    full SPIFFE id to pin (default spiffe://<td>/orchestrator)
//
// # Subcommands
//
//	session                        create a new session and print its id
//	run    <message>               submit a turn and stream events until terminal result
//	approve <call-id>              approve a pending tool call (Control RPC)
//	deny    <call-id> [reason]     deny a pending tool call (Control RPC)
//	interrupt                      cooperatively interrupt the in-flight turn (Control RPC)
//	fork    [--at-seq N]           fork the current session at the given seq
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	boltropev1 "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// devInsecureEnv unlocks the shared-seed static-cert dev mTLS path (mirrors the
// server-side gate in internal/platform/grpcx). When it equals "1" the CLI dials
// over dev mTLS instead of plaintext, so it can reach a `docker compose` edge
// running under the same dev fallback.
const devInsecureEnv = "BOLTROPE_DEV_INSECURE"

// defaultTrustDomain is the SPIFFE trust domain the compose dev stack uses
// (BOLTROPE_TRUST_DOMAIN); it must match the orchestrator's so the pinned server
// id and the shared dev CA line up.
const defaultTrustDomain = "boltrope.local"

// clientServiceSegment is the workload segment of the SPIFFE id the CLI presents
// in the dev mTLS path. The orchestrator's deny-by-default RBAC admits
// spiffe://<td>/edge (the mesh/edge identity) to the client-facing RPCs, so the
// CLI must present that identity to be authorized.
const clientServiceSegment = "edge"

// orchestratorServiceSegment is the workload segment of the orchestrator's own
// SPIFFE id; the CLI pins spiffe://<td>/orchestrator as the callee by default so
// a misrouted dial to the wrong service is rejected.
const orchestratorServiceSegment = "orchestrator"

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "harnessctl: %v\n", err)
		os.Exit(1)
	}
}

// run is the testable entry-point: it parses the top-level flags, dials, and
// dispatches to the appropriate sub-command handler.
func run(args []string, w io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: harnessctl [flags] <subcommand> [args]\nsubcommands: session, run, approve, deny, interrupt, fork")
	}

	// Separate global flags from the subcommand.  Global flags come before the
	// first non-flag token; everything after is the subcommand and its args.
	globalArgs, subcmd, subcmdArgs := splitArgs(args)

	cfg, err := parseCLIFlags(globalArgs)
	if err != nil {
		return err
	}

	conn, err := dial(cfg)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.Endpoint, err)
	}
	defer conn.Close() //nolint:errcheck // best-effort close on the exit path; nothing actionable on error

	client := boltropev1.NewOrchestratorServiceClient(conn)
	ctx := withToken(context.Background(), cfg.Token)

	switch subcmd {
	case "session":
		return createSessionCommand(ctx, client, cfg, w)

	case "run":
		if len(subcmdArgs) == 0 {
			return errors.New("run: message argument required")
		}
		return runCommand(ctx, client, cfg, strings.Join(subcmdArgs, " "), w)

	case "approve":
		if len(subcmdArgs) < 1 {
			return errors.New("approve: call-id argument required")
		}
		return approveCommand(ctx, client, cfg, subcmdArgs[0], w)

	case "deny":
		if len(subcmdArgs) < 1 {
			return errors.New("deny: call-id argument required")
		}
		reason := ""
		if len(subcmdArgs) >= 2 {
			reason = strings.Join(subcmdArgs[1:], " ")
		}
		return denyCommand(ctx, client, cfg, subcmdArgs[0], reason, w)

	case "interrupt":
		return interruptCommand(ctx, client, cfg, w)

	case "fork":
		// Parse --at-seq from subcmdArgs.
		atSeq := parseForkAtSeq(subcmdArgs)
		return forkCommand(ctx, client, cfg, atSeq, w)

	default:
		return fmt.Errorf("unknown subcommand %q; valid: session, run, approve, deny, interrupt, fork", subcmd)
	}
}

// cliConfig holds the resolved connection and session parameters.
type cliConfig struct {
	// Endpoint is the gRPC target (host:port).  Required.
	Endpoint string
	// Tenant is the tenant id.  OPTIONAL: when empty the CLI sends no tenant_id and
	// the orchestrator scopes the call to the authenticated tenant (the dev
	// principal under BOLTROPE_DEV_INSECURE, or the token's tenant in production).
	// When set, the server requires it to match the authenticated principal.
	Tenant string
	// Token is the bearer token sent as "authorization: Bearer <token>" metadata.
	// Optional: some deployments use mTLS-only auth.
	Token string
	// Insecure disables transport security (plaintext).  Intended for a local
	// orchestrator started without mTLS only; it cannot reach the compose mTLS
	// edge.  The dev mTLS path (BOLTROPE_DEV_INSECURE=1) takes precedence.
	Insecure bool
	// TrustDomain is the SPIFFE trust domain used to derive the client identity
	// and the default pinned server id in the dev mTLS path.  Defaults to
	// boltrope.local (the compose stack's domain).
	TrustDomain string
	// ServerID, when non-empty, overrides the pinned orchestrator SPIFFE id in the
	// dev mTLS path.  Defaults to spiffe://<TrustDomain>/orchestrator.
	ServerID string
	// SessionID is the target session.  When absent for run/approve/deny/interrupt
	// the CLI creates a new session first.
	SessionID string
	// AfterSeq is the resumable cursor for Run (FR-API-01).  Zero streams from
	// the beginning of the session.
	AfterSeq int64
	// PermissionMode is the session's standing permission mode applied when the CLI
	// CREATES a session (the `session` subcommand, or `run` without --session):
	// default|acceptEdits|plan. It is a session-creation setting (ADR-0019), so it
	// has no effect when --session targets an existing session. `bypass` is
	// operator-only and the orchestrator rejects a client-supplied bypass.
	PermissionMode string
}

// parseCLIFlags parses the global flags from args into a cliConfig.  Unknown
// flags are silently ignored (the loader coexists with test-runner flags and
// subcommand-specific flags that appear after the subcommand token).  A
// non-nil error is returned only when a required field is absent.
//
// Environment-variable fallbacks are applied after flag parsing: if a flag is
// unset and the corresponding BOLTROPE_CTL_* variable is present, its value
// is used.
func parseCLIFlags(args []string) (*cliConfig, error) {
	fs := flag.NewFlagSet("harnessctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // silence the built-in usage dump

	endpoint := fs.String("endpoint", "", "orchestrator gRPC endpoint (host:port)")
	tenant := fs.String("tenant", "", "tenant id (optional; omit to use the authenticated tenant)")
	token := fs.String("token", "", "bearer token for the authorization metadata header")
	insecureF := fs.Bool("insecure", false, "use plaintext transport, no TLS (local orchestrator without mTLS only)")
	trustDomain := fs.String("trust-domain", "", "SPIFFE trust domain for the dev mTLS path (default boltrope.local)")
	serverID := fs.String("server-id", "", "full SPIFFE id of the orchestrator to pin in the dev mTLS path (default spiffe://<trust-domain>/orchestrator)")
	session := fs.String("session", "", "session id (created if absent for run commands)")
	afterSeq := fs.Int64("after-seq", 0, "resumable cursor: replay events with seq > N")
	permissionMode := fs.String("permission-mode", "", "permission mode for a NEWLY created session: default|acceptEdits|plan (bypass is operator-only and rejected by the server)")

	// ContinueOnError: parse stops on first unknown flag by default.  We want to
	// ignore unknowns, so we filter args to known flags before parsing. The known
	// flag set (and each flag's value arity) lives in globalFlagValueArity, shared
	// with splitArgs so the two cannot drift.
	filtered := filterKnownArgs(args)
	if err := fs.Parse(filtered); err != nil && !errors.Is(err, flag.ErrHelp) {
		return nil, fmt.Errorf("harnessctl: flag parse: %w", err)
	}

	// Apply environment-variable fallbacks for flags that were not set.
	if *endpoint == "" {
		*endpoint = os.Getenv("BOLTROPE_CTL_ENDPOINT")
	}
	if *tenant == "" {
		*tenant = os.Getenv("BOLTROPE_CTL_TENANT")
	}
	if *token == "" {
		*token = os.Getenv("BOLTROPE_CTL_TOKEN")
	}
	if !*insecureF {
		if v := os.Getenv("BOLTROPE_CTL_INSECURE"); v == "1" || strings.EqualFold(v, "true") {
			*insecureF = true
		}
	}
	if *trustDomain == "" {
		*trustDomain = os.Getenv("BOLTROPE_TRUST_DOMAIN")
	}
	if *serverID == "" {
		*serverID = os.Getenv("BOLTROPE_CTL_SERVER_ID")
	}
	if *session == "" {
		*session = os.Getenv("BOLTROPE_CTL_SESSION")
	}
	if *permissionMode == "" {
		*permissionMode = os.Getenv("BOLTROPE_CTL_PERMISSION_MODE")
	}

	// Validate required fields. --tenant is OPTIONAL: when omitted the CLI sends an
	// empty tenant_id and the orchestrator scopes the call to the AUTHENTICATED
	// tenant (the dev principal under BOLTROPE_DEV_INSECURE, or the token's tenant
	// in production). Supply it only to assert a specific tenant — the server then
	// requires it to match the authenticated one.
	if strings.TrimSpace(*endpoint) == "" {
		return nil, errors.New("harnessctl: --endpoint (or BOLTROPE_CTL_ENDPOINT) is required")
	}

	return &cliConfig{
		Endpoint:       *endpoint,
		Tenant:         *tenant,
		Token:          *token,
		Insecure:       *insecureF,
		TrustDomain:    *trustDomain,
		ServerID:       *serverID,
		SessionID:      *session,
		AfterSeq:       *afterSeq,
		PermissionMode: *permissionMode,
	}, nil
}

// dial creates a [*grpc.ClientConn] to cfg.Endpoint, selecting transport
// security in priority order (see the package doc):
//
//  1. BOLTROPE_DEV_INSECURE=1 → the shared-seed static-cert dev mTLS path. This
//     takes precedence over --insecure because it is the documented Quickstart
//     path against the `docker compose` edge, which serves mTLS under the same
//     dev fallback. The connection is lazy (grpc.NewClient), so credential
//     construction — not a live handshake — is what can fail here.
//  2. cfg.Insecure → plaintext, no TLS (local orchestrator without mTLS only).
//  3. otherwise → an error telling the operator how to enable one of the above.
func dial(cfg *cliConfig) (*grpc.ClientConn, error) {
	if devInsecureEnabled() {
		creds, err := devClientCreds(cfg)
		if err != nil {
			return nil, err
		}
		return grpcx.Dial(grpcx.DialConfig{Target: cfg.Endpoint, Creds: creds})
	}
	if cfg.Insecure {
		return grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return nil, errors.New(
		"harnessctl: no transport security selected; set " + devInsecureEnv +
			"=1 for the dev mTLS path (the documented compose Quickstart), or --insecure for a local plaintext orchestrator " +
			"(SPIRE-backed SPIFFE credentials for production are a later wiring task)")
}

// devInsecureEnabled reports whether the shared-seed dev mTLS fallback is
// unlocked (BOLTROPE_DEV_INSECURE=1), mirroring the server-side gate.
func devInsecureEnabled() bool { return os.Getenv(devInsecureEnv) == "1" }

// devClientCreds builds the shared-seed static-cert dev mTLS client credentials
// for the orchestrator edge: the CLI presents spiffe://<td>/edge (the identity
// the orchestrator's RBAC admits to the client-facing RPCs) and pins the
// orchestrator's SPIFFE id as the callee. Both are derived from cfg.TrustDomain
// (default boltrope.local) unless cfg.ServerID overrides the pinned id. The
// shared dev CA (BOLTROPE_DEV_CA_SEED) is what lets this complete mutual TLS with
// a compose edge brought up under the same env.
func devClientCreds(cfg *cliConfig) (credentials.TransportCredentials, error) {
	tdStr := cfg.TrustDomain
	if tdStr == "" {
		tdStr = defaultTrustDomain
	}
	td, err := spiffeid.TrustDomainFromString(tdStr)
	if err != nil {
		return nil, fmt.Errorf("harnessctl: invalid trust domain %q: %w", tdStr, err)
	}

	clientID, err := spiffeid.FromSegments(td, clientServiceSegment)
	if err != nil {
		return nil, fmt.Errorf("harnessctl: build client SPIFFE id: %w", err)
	}

	serverID, err := pinnedServerID(cfg, td)
	if err != nil {
		return nil, err
	}

	creds, err := grpcx.StaticDevClientCredentials(grpcx.StaticDevConfig{
		TrustDomain: td,
		ServerID:    clientID, // ServerID is the identity THIS process presents (the client id).
	}, serverID)
	if err != nil {
		return nil, fmt.Errorf("harnessctl: dev mTLS client credentials: %w", err)
	}
	return creds, nil
}

// pinnedServerID resolves the orchestrator SPIFFE id the dev mTLS path pins as
// the callee: cfg.ServerID verbatim when set, else spiffe://<td>/orchestrator.
func pinnedServerID(cfg *cliConfig, td spiffeid.TrustDomain) (spiffeid.ID, error) {
	if cfg.ServerID != "" {
		id, err := spiffeid.FromString(cfg.ServerID)
		if err != nil {
			return spiffeid.ID{}, fmt.Errorf("harnessctl: invalid --server-id %q: %w", cfg.ServerID, err)
		}
		return id, nil
	}
	id, err := spiffeid.FromSegments(td, orchestratorServiceSegment)
	if err != nil {
		return spiffeid.ID{}, fmt.Errorf("harnessctl: build orchestrator SPIFFE id: %w", err)
	}
	return id, nil
}

// parsePermissionMode maps the --permission-mode string to the wire
// [boltropev1.PermissionMode], applied when the CLI creates a session (ADR-0019).
// An empty string yields UNSPECIFIED (the server applies its secure default).
// "bypass" is accepted by the parser but the orchestrator REJECTS a
// client-supplied bypass (operator-only, server-side) — the resulting
// InvalidArgument is surfaced verbatim so the constraint is explicit.
func parsePermissionMode(s string) (boltropev1.PermissionMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return boltropev1.PermissionMode_PERMISSION_MODE_UNSPECIFIED, nil
	case "default":
		return boltropev1.PermissionMode_PERMISSION_MODE_DEFAULT, nil
	case "acceptedits", "accept-edits", "accept_edits":
		return boltropev1.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, nil
	case "plan":
		return boltropev1.PermissionMode_PERMISSION_MODE_PLAN, nil
	case "bypass":
		return boltropev1.PermissionMode_PERMISSION_MODE_BYPASS, nil
	default:
		return boltropev1.PermissionMode_PERMISSION_MODE_UNSPECIFIED,
			fmt.Errorf("harnessctl: invalid --permission-mode %q (want: default|acceptEdits|plan|bypass)", s)
	}
}

// withToken returns a context that carries token as an "authorization" metadata
// header when token is non-empty.
func withToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// createSessionCommand calls CreateSession and prints the new session id.
func createSessionCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, w io.Writer) error {
	mode, err := parsePermissionMode(cfg.PermissionMode)
	if err != nil {
		return err
	}
	resp, err := client.CreateSession(ctx, &boltropev1.CreateSessionRequest{
		TenantId: cfg.Tenant,
		Mode:     mode,
	})
	if err != nil {
		return fmt.Errorf("CreateSession: %w", err)
	}
	_, err = fmt.Fprintf(w, "session: %s\n", resp.SessionId)
	return err
}

// runCommand submits message to a session (creating one if cfg.SessionID is
// empty) and streams RunEvents until the terminal Result, printing each frame
// to w.
func runCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, message string, w io.Writer) error {
	mode, err := parsePermissionMode(cfg.PermissionMode)
	if err != nil {
		return err
	}

	sessionID := cfg.SessionID
	if sessionID == "" {
		resp, err := client.CreateSession(ctx, &boltropev1.CreateSessionRequest{
			TenantId: cfg.Tenant,
			Mode:     mode,
		})
		if err != nil {
			return fmt.Errorf("CreateSession: %w", err)
		}
		sessionID = resp.SessionId
		if _, werr := fmt.Fprintf(w, "session: %s\n", sessionID); werr != nil {
			return werr
		}
	}

	// Build the RunRequest message part only when there is actual content.
	var msgPB *boltropev1.Message
	if strings.TrimSpace(message) != "" {
		msgPB = &boltropev1.Message{
			Role: boltropev1.Role_ROLE_USER,
			Content: []*boltropev1.ContentPart{
				{
					Part: &boltropev1.ContentPart_Text{
						Text: &boltropev1.TextPart{Text: message},
					},
				},
			},
		}
	}

	stream, err := client.Run(ctx, &boltropev1.RunRequest{
		TenantId:  cfg.Tenant,
		SessionId: sessionID,
		Message:   msgPB,
		AfterSeq:  cfg.AfterSeq,
	})
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}

	return renderStream(stream, w)
}

// renderStream drains a server-streaming Run RPC and renders each frame to w:
//   - TextDelta: printed inline (no newline, so chunks concatenate naturally).
//   - ThinkingDelta: printed with a "[thinking] " prefix.
//   - ToolProgress: printed with a "[tool] " prefix.
//   - ApprovalRequest: printed with a prominent "[approval required]" block
//     that shows the call_id so the operator can issue harnessctl approve.
//   - Result: the terminal frame; prints the outcome subtype and final text.
func renderStream(stream boltropev1.OrchestratorService_RunClient, w io.Writer) error {
	for {
		ev, err := stream.Recv()
		if err != nil {
			if isEOF(err) {
				return nil
			}
			return fmt.Errorf("Run stream: %w", err)
		}

		switch p := ev.Payload.(type) {
		case *boltropev1.RunEvent_TextDelta:
			if _, werr := fmt.Fprint(w, p.TextDelta.GetText()); werr != nil {
				return werr
			}

		case *boltropev1.RunEvent_ThinkingDelta:
			if t := p.ThinkingDelta.GetText(); t != "" {
				if _, werr := fmt.Fprintf(w, "[thinking] %s", t); werr != nil {
					return werr
				}
			}

		case *boltropev1.RunEvent_ToolProgress:
			tp := p.ToolProgress
			if msg := tp.GetMessage(); msg != "" {
				if _, werr := fmt.Fprintf(w, "\n[tool] %s\n", msg); werr != nil {
					return werr
				}
			}
			if chunk := tp.GetStdoutChunk(); len(chunk) > 0 {
				if _, werr := fmt.Fprintf(w, "[stdout] %s", chunk); werr != nil {
					return werr
				}
			}

		case *boltropev1.RunEvent_ApprovalRequest:
			ar := p.ApprovalRequest
			if _, werr := fmt.Fprintf(w,
				"\n[approval required]\n  tool:    %s\n  call_id: %s\n  args:    %s\n  reason:  %s\nRun: harnessctl approve %s\n",
				ar.GetToolName(), ar.GetCallId(), ar.GetArgsJson(), ar.GetReason(), ar.GetCallId(),
			); werr != nil {
				return werr
			}

		case *boltropev1.RunEvent_Result:
			r := p.Result
			if _, werr := fmt.Fprintf(w, "\n[result] subtype=%s turns=%d cost=%.6f USD\n",
				subtypeName(r.GetSubtype()), r.GetNumTurns(), r.GetCostUsd()); werr != nil {
				return werr
			}
			if ft := r.GetFinalText(); ft != "" {
				if _, werr := fmt.Fprintf(w, "%s\n", ft); werr != nil {
					return werr
				}
			}
			return nil
		}
	}
}

// approveCommand sends a Control{Approve{CallId: callID}} request for
// cfg.SessionID and prints a confirmation.
func approveCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, callID string, w io.Writer) error {
	resp, err := client.Control(ctx, &boltropev1.ControlRequest{
		TenantId:  cfg.Tenant,
		SessionId: cfg.SessionID,
		Action: &boltropev1.ControlRequest_Approve{
			Approve: &boltropev1.ApproveAction{CallId: callID},
		},
	})
	if err != nil {
		return fmt.Errorf("Control(approve): %w", err)
	}
	_, err = fmt.Fprintf(w, "approved call_id=%s head_seq=%d\n", callID, resp.HeadSeq)
	return err
}

// denyCommand sends a Control{Deny{CallId: callID, Reason: reason}} request.
func denyCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, callID, reason string, w io.Writer) error {
	resp, err := client.Control(ctx, &boltropev1.ControlRequest{
		TenantId:  cfg.Tenant,
		SessionId: cfg.SessionID,
		Action: &boltropev1.ControlRequest_Deny{
			Deny: &boltropev1.DenyAction{CallId: callID, Reason: reason},
		},
	})
	if err != nil {
		return fmt.Errorf("Control(deny): %w", err)
	}
	_, err = fmt.Fprintf(w, "denied call_id=%s reason=%q head_seq=%d\n", callID, reason, resp.HeadSeq)
	return err
}

// interruptCommand sends a Control{Interrupt{}} request for cfg.SessionID.
func interruptCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, w io.Writer) error {
	resp, err := client.Control(ctx, &boltropev1.ControlRequest{
		TenantId:  cfg.Tenant,
		SessionId: cfg.SessionID,
		Action: &boltropev1.ControlRequest_Interrupt{
			Interrupt: &boltropev1.InterruptAction{},
		},
	})
	if err != nil {
		return fmt.Errorf("Control(interrupt): %w", err)
	}
	_, err = fmt.Fprintf(w, "interrupt sent session=%s head_seq=%d\n", cfg.SessionID, resp.HeadSeq)
	return err
}

// forkCommand calls Fork at atSeq and prints the child session id.
func forkCommand(ctx context.Context, client boltropev1.OrchestratorServiceClient, cfg *cliConfig, atSeq int64, w io.Writer) error {
	resp, err := client.Fork(ctx, &boltropev1.ForkRequest{
		TenantId:  cfg.Tenant,
		SessionId: cfg.SessionID,
		AtSeq:     atSeq,
	})
	if err != nil {
		return fmt.Errorf("Fork: %w", err)
	}
	_, err = fmt.Fprintf(w, "fork: child_session=%s (from parent=%s at_seq=%d)\n",
		resp.SessionId, cfg.SessionID, atSeq)
	return err
}

// ---- helpers -----------------------------------------------------------

// globalFlagValueArity maps each known global flag name to whether it consumes a
// following value token in the space-separated form (`--flag value`). Every
// global flag takes a value except the boolean --insecure. It is the single
// source of truth shared by splitArgs (to find where the global flags end and the
// subcommand begins) and filterKnownArgs (to keep a "--flag value" pair together),
// so "known" and "takes a value" cannot drift apart. Centralizing it is what fixes
// the boundary bug where `--endpoint host run …` mistook the endpoint VALUE for
// the subcommand because splitArgs did not know --endpoint consumes the next token.
var globalFlagValueArity = map[string]bool{
	"endpoint":        true,
	"tenant":          true,
	"token":           true,
	"insecure":        false, // boolean: present/absent, consumes no value token
	"trust-domain":    true,
	"server-id":       true,
	"session":         true,
	"after-seq":       true,
	"permission-mode": true,
}

// splitArgs separates the leading global flags from the subcommand token and its
// arguments. The subcommand is the first token that is neither a flag nor the
// value of a preceding value-taking global flag. Crucially, a known global flag
// written in the space form (`--endpoint host`) consumes the following token as
// its value, so that token is NOT mistaken for the subcommand — without this,
// `harnessctl --endpoint localhost:9000 run …` parsed "localhost:9000" as the
// subcommand and left --endpoint with no argument, so the documented Quickstart
// command failed at flag parsing. An explicit "--" terminates the global flags and
// forces the next token to be the subcommand.
func splitArgs(args []string) (globalArgs []string, subcmd string, subcmdArgs []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			// Explicit terminator: everything after it is subcommand territory.
			if i+1 < len(args) {
				return args[:i], args[i+1], args[i+2:]
			}
			return args[:i], "", nil
		}
		if strings.HasPrefix(a, "-") {
			// A known value-taking global flag in space form (no "=") consumes the
			// NEXT token as its value; skip past it so it is not read as the
			// subcommand. The boolean --insecure and the "--flag=value" form take no
			// following token.
			name, _, hasEq := strings.Cut(strings.TrimLeft(a, "-"), "=")
			if !hasEq && globalFlagValueArity[name] && i+1 < len(args) {
				i++ // consume the value token alongside its flag
			}
			continue
		}
		// First token that is neither a flag nor a consumed flag value: the
		// subcommand. Everything after it belongs to the subcommand.
		return args[:i], a, args[i+1:]
	}
	return args, "", nil
}

// filterKnownArgs returns the subset of args that belong to known global flags,
// dropping unrecognized flag names while preserving the "--flag value" two-token
// form for value-taking flags. It lets the FlagSet (ContinueOnError) coexist with
// subcommand-specific flags that appear later. It shares globalFlagValueArity with
// splitArgs so "known" and "takes a value" cannot drift between the two.
func filterKnownArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			break
		}
		if !strings.HasPrefix(a, "-") {
			// Positional (subcommand token or its args); stop here.
			break
		}
		name, _, hasEq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		takesValue, known := globalFlagValueArity[name]
		if !known {
			// Unknown flag: skip. If it's in "--name value" form the value token
			// (if any) is a non-flag token and breaks the loop on the next iter.
			continue
		}
		out = append(out, a)
		if !hasEq && takesValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			out = append(out, args[i+1])
			i++
		}
	}
	return out
}

// parseForkAtSeq extracts the value of --at-seq from subcmdArgs, returning 0
// if absent or unparseable.
func parseForkAtSeq(args []string) int64 {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	atSeq := fs.Int64("at-seq", 0, "")
	_ = fs.Parse(args)
	return *atSeq
}

// isEOF returns true when err signals a cleanly closed server stream.
func isEOF(err error) bool {
	if err == nil {
		return false
	}
	// io.EOF is the normal end-of-stream signal from gRPC's Recv.
	if errors.Is(err, io.EOF) {
		return true
	}
	return false
}

// subtypeName returns a compact, lower-case label for a TerminationSubtype.
func subtypeName(s boltropev1.TerminationSubtype) string {
	switch s {
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_SUCCESS:
		return "success"
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_TURNS:
		return "error_max_turns"
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_BUDGET_USD:
		return "error_max_budget_usd"
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION:
		return "error_during_execution"
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES:
		return "error_max_structured_output_retries"
	case boltropev1.TerminationSubtype_TERMINATION_SUBTYPE_REFUSAL:
		return "refusal"
	default:
		return "unspecified"
	}
}
