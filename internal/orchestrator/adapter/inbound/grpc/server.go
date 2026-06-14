package grpc

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Compile-time assertion: *Server satisfies the generated server interface.
var _ genproto.OrchestratorServiceServer = (*Server)(nil)

// EventStore is the [Server]'s event-log dependency: the frozen
// [app.EventLogPort] (append/load/subscribe/fork an EXISTING stream) PLUS the
// session-creation half that inserts the active session aggregate row. The
// concrete pgx store (*eventstore.Store) satisfies it; CreateSession is
// deliberately outside [app.EventLogPort] (which only operates on an existing
// stream), so the transport depends on this consumer-side superset to open a
// stream before appending its first SessionStarted.
type EventStore interface {
	app.EventLogPort

	// CreateSession inserts a fresh session aggregate (status=active, head_seq=0)
	// owned by the context's tenant with the given permission mode (sessions.mode;
	// ADR-0019) and returns it. It is the creation half of the CreateSession RPC;
	// the caller appends the stream's first SessionStarted afterwards to bump
	// head_seq 0->1. mode has already been verified by the caller (client-supplied
	// bypass is rejected before this).
	CreateSession(ctx context.Context, sessionID string, mode domain.PermissionMode) (domain.Session, error)
}

// RunSpec is the input the [Server] hands a [Runner] to drive one Run. It is the
// edge-decoupled view of an agent run: the session, the (possibly empty) new
// user turn, the taint flag, the permission mode, and the live [agent.ClientSink]
// the runner forwards text/thinking deltas to. It deliberately uses kernel/domain
// types (no gen/) so a [Runner] never sees wire types.
type RunSpec struct {
	// SessionID is the target session/stream.
	SessionID string
	// TenantID is the verified owning tenant (already authorized by the handler).
	TenantID string
	// UserMessage is the new user turn to append before generating; a zero-value
	// message (no content) means a pure resume with no fresh input.
	UserMessage llm.Message
	// Tainted reports whether untrusted content has entered the session (threaded
	// into the policy taint gate).
	Tainted bool
	// Mode is the permission operating mode for this run.
	Mode policy.Mode
	// Sink forwards live generation deltas to the connected client. The runner
	// MUST forward through it (never block on it) so a slow client cannot
	// backpressure generation (NFR-REL-05).
	Sink ClientSink
}

// RunOutcome is the terminal result of a [Runner.Run]: the typed termination
// subtype plus the run's accumulated usage/cost/turn-count and the final text.
// It mirrors [agent.RunResult] without importing gen/, so the [Server] can build
// the terminal RunResult frame.
type RunOutcome struct {
	// Reason is the typed termination subtype.
	Reason domain.TerminationReason
	// FinalText is the model's final text-only response (empty for non-success
	// terminations that produced no final text).
	FinalText string
	// Usage is the run's accumulated usage.
	Usage llm.Usage
	// CostUSD is the run's accumulated cost.
	CostUSD float64
	// NumTurns is the number of turns executed.
	NumTurns int64
}

// ClientSink is re-exported from the agent package's shape so the transport can
// forward live deltas without the relay importing agent. It matches
// [agent.ClientSink]; the relay implements it and the production [Runner] passes
// it straight through to the loop.
type ClientSink interface {
	// OnTextDelta forwards an incremental chunk of assistant text.
	OnTextDelta(sessionID, turnID, text string)
	// OnThinkingDelta forwards an incremental chunk of reasoning/thinking text.
	OnThinkingDelta(sessionID, turnID, text string)
}

// Runner drives one agent run for a session, forwarding live deltas to the
// supplied sink and returning the terminal [RunOutcome]. It is the seam between
// the transport and the agent loop: the production implementation
// ([LoopRunner]) wraps an [agent.Loop]; tests inject a fake that appends events
// and drives the sink so the server is exercised without a real loop. The run's
// context cancellation IS the interrupt mechanism (FR-LOOP-03): the [Server]
// cancels it on Control.Interrupt.
type Runner interface {
	// Run executes the agent loop for spec until terminal, returning the outcome.
	// A non-nil error is an infrastructural failure (the typed termination is on
	// RunOutcome.Reason, not the error).
	Run(ctx context.Context, spec RunSpec) (RunOutcome, error)
}

// Config parameterizes the [Server].
type Config struct {
	// DefaultModel is the model id used for a session whose run does not override
	// it. It is recorded on the loop via the Runner; the server only needs it to
	// populate GetSession when no run has occurred. Optional.
	DefaultModel string
	// MaxInFlightPerTenant bounds the number of concurrent Run streams a single
	// tenant may have in flight (DoS/cost bound; architecture §8.7). A
	// non-positive value uses [DefaultMaxInFlightPerTenant].
	MaxInFlightPerTenant int
}

// DefaultMaxInFlightPerTenant is the per-tenant concurrent-Run cap applied when
// [Config.MaxInFlightPerTenant] is non-positive.
const DefaultMaxInFlightPerTenant = 16

// Server implements the generated [genproto.OrchestratorServiceServer]: the
// client-facing agent-control edge (CreateSession/GetSession/Run/Control/Fork).
// It maps gen/ ⇄ domain/llm, drives the [Runner] for Run, resolves the
// [app.ApprovalGate] for Control approve/deny, cancels the running loop for
// Control interrupt, and enforces tenant ownership + a per-tenant in-flight cap
// (architecture §8.7).
type Server struct {
	genproto.UnimplementedOrchestratorServiceServer

	log      EventStore
	gate     app.ApprovalGate
	runner   Runner
	ids      ids.IDGenerator
	cfg      Config
	maxFlite int

	mu sync.Mutex
	// running maps an active session id to the cancel func of its run loop, so
	// Control.Interrupt can cancel the loop context (FR-LOOP-03).
	running map[string]context.CancelFunc
	// inflight counts active Run streams per tenant for the concurrency cap.
	inflight map[string]int
}

// NewServer constructs a [Server]. log, gate, runner, and idgen are required;
// nil idgen falls back to a panic-on-use generator only via the platform default
// is the caller's concern, so the caller must pass a real one.
func NewServer(log EventStore, gate app.ApprovalGate, runner Runner, idgen ids.IDGenerator, cfg Config) *Server {
	maxFlite := cfg.MaxInFlightPerTenant
	if maxFlite <= 0 {
		maxFlite = DefaultMaxInFlightPerTenant
	}
	return &Server{
		log:      log,
		gate:     gate,
		runner:   runner,
		ids:      idgen,
		cfg:      cfg,
		maxFlite: maxFlite,
		running:  make(map[string]context.CancelFunc),
		inflight: make(map[string]int),
	}
}

// ---- ownership --------------------------------------------------------------

// authorizeTenant resolves the trusted tenant for a request: it is ALWAYS the
// authenticated principal's tenant, never the untrusted request body. A request
// MAY echo a tenant_id, but it is only honored as a guard — it must MATCH the
// principal when non-empty; an omitted request tenant_id simply uses the
// authenticated tenant. It returns UNAUTHENTICATED when the request is not
// authenticated and PERMISSION_DENIED when a non-empty request tenant_id does not
// match the principal — the public-edge analog of the RLS check (architecture
// §8.7). It returns the verified (principal) tenant on success.
func (s *Server) authorizeTenant(ctx context.Context, reqTenant string) (string, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "orchestrator: request is not authenticated")
	}
	if reqTenant != "" && reqTenant != p.TenantID {
		return "", status.Error(codes.PermissionDenied, "orchestrator: caller tenant does not match request tenant_id")
	}
	return p.TenantID, nil
}

// authorizeSession verifies the authenticated tenant owns sessionID by loading
// the session and comparing its tenant. A session owned by another tenant
// returns PERMISSION_DENIED; a missing session returns NOT_FOUND. The session's
// stored TenantID is the authority; when the store does not populate it (the
// in-memory fake), ownership falls back to the request-tenant match already done
// by [authorizeTenant].
func (s *Server) authorizeSession(ctx context.Context, tenant, sessionID string) (domain.Session, error) {
	sess, err := s.log.LoadSession(ctx, sessionID)
	if err != nil {
		return domain.Session{}, status.Errorf(codes.NotFound, "orchestrator: session %q not found", sessionID)
	}
	if sess.TenantID != "" && sess.TenantID != tenant {
		return domain.Session{}, status.Error(codes.PermissionDenied, "orchestrator: caller does not own the target session")
	}
	return sess, nil
}

// ---- CreateSession ----------------------------------------------------------

// CreateSession opens a fresh event-sourcing stream for the caller's tenant and
// returns its id. The session's owning tenant is the AUTHENTICATED principal's
// tenant (resolved by [authorizeTenant]); a request_id tenant is optional and,
// when present, must match the principal — so a client may omit tenant_id
// entirely and the trusted authenticated tenant is used. A client-supplied
// PERMISSION_MODE_BYPASS is rejected (operator-only, server-side; architecture
// §8.13). The session aggregate row is created first (active, head_seq=0) and
// then its SessionStarted is appended so the stream exists for a subsequent
// Run/Fork.
func (s *Server) CreateSession(ctx context.Context, req *genproto.CreateSessionRequest) (*genproto.CreateSessionResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	if req.GetMode() == genproto.PermissionMode_PERMISSION_MODE_BYPASS {
		return nil, status.Error(codes.InvalidArgument, "orchestrator: bypass mode is operator-only and cannot be set by a client request")
	}

	sessionID := s.ids.NewSessionID().String()
	// The session's standing permission mode is taken from the (verified, non-
	// bypass) request and persisted on the session aggregate (ADR-0019). An
	// UNSPECIFIED/DEFAULT request resolves to the most-restrictive ModeDefault.
	mode := fromGenModeDomain(req.GetMode())
	// Create the session aggregate row FIRST (status=active, head_seq=0) so the
	// subsequent Append has an active stream to write to — otherwise Append's
	// status='active' guard rejects with SessionNotActive. The session's tenant_id
	// is stamped from the verified principal: the auth interceptor placed it on the
	// RLS context (db.WithTenant(ctx, principal.TenantID)) and the store reads it
	// back to scope the row — so the persisted tenant is the authenticated one,
	// never the untrusted request body.
	if _, err := s.log.CreateSession(ctx, sessionID, mode); err != nil {
		return nil, mapCreateSessionError(err)
	}
	// Open the stream with a SessionStarted as its first event so LoadSession and
	// Subscribe have a head to work from; this bumps head_seq 0->1.
	reqID := s.ids.NewRequestID().String()
	if _, err := s.log.Append(ctx, sessionID, 0, 0, reqID, app.AppendInput{
		Event: domain.SessionStarted{},
		Actor: domain.ActorSystem,
	}); err != nil {
		return nil, mapAppendError(err)
	}
	_ = tenant // the tenant is stamped via the RLS context (above), not a payload field.
	return &genproto.CreateSessionResponse{SessionId: sessionID}, nil
}

// ---- GetSession -------------------------------------------------------------

// GetSession returns the materialized session projection (status, head seq,
// lineage) for an owned session. Accumulated usage/cost/turns are folded from the
// session's TurnFinished/TurnAborted events.
func (s *Server) GetSession(ctx context.Context, req *genproto.GetSessionRequest) (*genproto.GetSessionResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	sess, err := s.authorizeSession(ctx, tenant, req.GetSessionId())
	if err != nil {
		return nil, err
	}
	if sess.TenantID == "" {
		sess.TenantID = tenant
	}
	usage, cost, turns := s.foldTotals(ctx, req.GetSessionId())
	return &genproto.GetSessionResponse{Session: toGenSession(sess, usage, cost, turns)}, nil
}

// foldTotals loads the session's events and sums the per-turn usage/cost and turn
// count from TurnFinished/TurnAborted events (a lightweight read-side fold for
// GetSession; the authoritative rollup is projectord's job). On a load error it
// returns zeros rather than failing GetSession.
func (s *Server) foldTotals(ctx context.Context, sessionID string) (llm.Usage, float64, int64) {
	events, err := s.log.Load(ctx, sessionID, 0)
	if err != nil {
		return llm.Usage{}, 0, 0
	}
	var (
		usage llm.Usage
		cost  float64
		turns int64
	)
	for _, env := range events {
		switch ev := env.Event.(type) {
		case domain.TurnFinished:
			usage = addUsage(usage, ev.Usage)
			cost += ev.CostUSD
			turns = int64(ev.NumTurns)
		case domain.TurnAborted:
			usage = addUsage(usage, ev.UsageSoFar)
			cost += ev.CostUSD
		}
	}
	return usage, cost, turns
}

// addUsage returns the element-wise sum of two usage snapshots (edge copy; the
// loop has its own, but this package must not import the loop's unexported one).
func addUsage(a, b llm.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens + b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens,
		ReasoningTokens:  a.ReasoningTokens + b.ReasoningTokens,
	}
}

// ---- Run --------------------------------------------------------------------

// Run submits a user turn (or a pure resume) and server-streams resumable
// RunEvents until the run reaches a terminal Result. Delivery is driven by
// [app.EventLogPort.Subscribe] from req.after_seq, so a reconnecting client
// receives only events with seq strictly greater than after_seq and never
// duplicates frames (FR-API-01). The agent loop runs on a background goroutine
// whose ClientSink is this stream's relay; the loop tails the durable log so a
// slow client never backpressures upstream generation (NFR-REL-05). On the
// loop's terminal RunResult the relay emits the terminal RunResult frame and
// returns.
func (s *Server) Run(req *genproto.RunRequest, stream genproto.OrchestratorService_RunServer) error {
	ctx := stream.Context()
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return err
	}
	sess, err := s.authorizeSession(ctx, tenant, req.GetSessionId())
	if err != nil {
		return err
	}

	// Per-tenant in-flight cap (architecture §8.7).
	if err := s.acquireSlot(tenant); err != nil {
		return err
	}
	defer s.releaseSlot(tenant)

	r := &relay{
		server:    s,
		stream:    stream,
		sessionID: req.GetSessionId(),
		afterSeq:  req.GetAfterSeq(),
	}
	// The run inherits the session's standing permission mode (sessions.mode, set
	// at CreateSession from the verified request; ADR-0019). toPolicyMode maps the
	// persisted domain mode to the live policy mode (an explicit mapping — the two
	// spellings differ for accept-edits); an unset/zero mode resolves to the
	// secure, most-restrictive ModeDefault.
	return r.run(ctx, RunSpec{
		SessionID:   req.GetSessionId(),
		TenantID:    tenant,
		UserMessage: fromGenMessage(req.GetMessage()),
		Mode:        toPolicyMode(sess.Mode),
		Sink:        r,
	})
}

// acquireSlot reserves an in-flight slot for tenant, returning a typed
// RESOURCE_EXHAUSTED error when the per-tenant cap is reached.
func (s *Server) acquireSlot(tenant string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[tenant] >= s.maxFlite {
		return status.Errorf(codes.ResourceExhausted, "orchestrator: per-tenant in-flight Run cap (%d) reached", s.maxFlite)
	}
	s.inflight[tenant]++
	return nil
}

// releaseSlot frees an in-flight slot for tenant.
func (s *Server) releaseSlot(tenant string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[tenant] > 0 {
		s.inflight[tenant]--
	}
}

// registerRun records the cancel func for sessionID's running loop so
// Control.Interrupt can cancel it. A prior registration (a concurrent Run on the
// same session) is overwritten — last writer wins; the prior loop's own context
// governs its lifetime.
func (s *Server) registerRun(sessionID string, cancel context.CancelFunc) {
	s.mu.Lock()
	s.running[sessionID] = cancel
	s.mu.Unlock()
}

// unregisterRun removes sessionID's run registration if cancel is still the
// active one.
func (s *Server) unregisterRun(sessionID string) {
	s.mu.Lock()
	delete(s.running, sessionID)
	s.mu.Unlock()
}

// interrupt cancels the running loop for sessionID, returning true when a loop
// was registered. The loop's cooperative cancellation appends TurnAborted and
// exits (FR-LOOP-03).
func (s *Server) interrupt(sessionID string) bool {
	s.mu.Lock()
	cancel, ok := s.running[sessionID]
	s.mu.Unlock()
	if ok && cancel != nil {
		cancel()
	}
	return ok
}

// ---- Control ----------------------------------------------------------------

// Control performs an out-of-band action on an owned session (Approve, Deny,
// Interrupt, Reattach), decoupled from the Run data stream (architecture §4.2).
// Every action verifies the caller owns the session (FR-API-02); a foreign-tenant
// target returns PERMISSION_DENIED.
func (s *Server) Control(ctx context.Context, req *genproto.ControlRequest) (*genproto.ControlResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	sess, err := s.authorizeSession(ctx, tenant, req.GetSessionId())
	if err != nil {
		return nil, err
	}

	switch a := req.GetAction().(type) {
	case *genproto.ControlRequest_Approve:
		if err := s.gate.Resolve(ctx, req.GetSessionId(), a.Approve.GetCallId(), domain.AskAllowed); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "orchestrator: no pending approval to resolve: %v", err)
		}
	case *genproto.ControlRequest_Deny:
		if err := s.gate.Resolve(ctx, req.GetSessionId(), a.Deny.GetCallId(), domain.AskDenied); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "orchestrator: no pending approval to resolve: %v", err)
		}
	case *genproto.ControlRequest_Interrupt:
		s.interrupt(req.GetSessionId())
		// Interrupt is best-effort and idempotent: if no loop is running the
		// session is already idle, which is a no-op success.
	case *genproto.ControlRequest_Reattach:
		// Reattach records the client's resume intent; the replay is delivered on
		// a subsequent Run stream. We return the current head as the resume
		// cursor.
	default:
		return nil, status.Error(codes.InvalidArgument, "orchestrator: Control requires exactly one action")
	}

	// Re-load the head so the response reflects any event the action produced.
	if cur, err := s.log.LoadSession(ctx, req.GetSessionId()); err == nil {
		sess = cur
	}
	return &genproto.ControlResponse{HeadSeq: sess.HeadSeq}, nil
}

// ---- Fork -------------------------------------------------------------------

// Fork creates a child session branching the parent at at_seq under
// tenant-ownership enforcement: the caller's tenant must own the parent or the
// call returns PERMISSION_DENIED (FR-STATE-03 AC-2; architecture §8.9). The child
// continues from at_seq+1; the parent is unaffected.
func (s *Server) Fork(ctx context.Context, req *genproto.ForkRequest) (*genproto.ForkResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeSession(ctx, tenant, req.GetSessionId()); err != nil {
		return nil, err
	}

	childID := s.ids.NewSessionID().String()
	child, err := s.log.Fork(ctx, req.GetSessionId(), req.GetAtSeq(), childID)
	if err != nil {
		return nil, mapForkError(err)
	}
	return &genproto.ForkResponse{SessionId: child.ID}, nil
}

// ---- error mapping ----------------------------------------------------------

// mapAppendError maps an [app.EventLogPort.Append] sentinel to a gRPC status.
func mapAppendError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, app.ConflictError):
		return status.Error(codes.FailedPrecondition, "orchestrator: optimistic concurrency conflict")
	case errors.Is(err, app.FencedError):
		return status.Error(codes.FailedPrecondition, "orchestrator: writer fenced (stale lease)")
	case errors.Is(err, app.SessionNotActiveError):
		return status.Error(codes.FailedPrecondition, "orchestrator: session is not active")
	default:
		return status.Errorf(codes.Internal, "orchestrator: append failed: %v", err)
	}
}

// mapCreateSessionError maps an [EventStore.CreateSession] failure to a gRPC
// status. A unique-violation on the session id (the store surfaces a duplicate
// key) is reported as ALREADY_EXISTS; the store may also already return a typed
// status, which is passed through; everything else is Internal.
func mapCreateSessionError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if isDuplicateKey(err) {
		return status.Error(codes.AlreadyExists, "orchestrator: session already exists")
	}
	return status.Errorf(codes.Internal, "orchestrator: create session failed: %v", err)
}

// sqlStater is the driver-agnostic SQLSTATE-carrying error interface. The pgx
// driver's *pgconn.PgError satisfies it (its SQLState method returns the Code),
// so the transport detects a unique-violation WITHOUT importing the pgx driver
// itself. Keeping this edge package pgx-free is load-bearing: the single-process
// dev binary (cmd/boltrope-dev) depends on this transport and is required to be
// pgx-free (no Postgres in dev mode); a direct pgconn import here would drag the
// entire driver into the dev binary's transitive graph.
type sqlStater interface{ SQLState() string }

// isDuplicateKey reports whether err is (or wraps) a Postgres unique-violation
// (SQLSTATE 23505), i.e. a duplicate session id on the CreateSession INSERT. It
// matches via the driver-agnostic [sqlStater] interface, not a concrete pgx type.
func isDuplicateKey(err error) bool {
	var pgErr sqlStater
	return errors.As(err, &pgErr) && pgErr.SQLState() == pgUniqueViolation
}

// pgUniqueViolation is the Postgres SQLSTATE for a unique_violation.
const pgUniqueViolation = "23505"

// mapForkError maps a [app.EventLogPort.Fork] error to a gRPC status. A
// permission error from the store (cross-tenant fork rejected at the DB layer)
// is surfaced as PERMISSION_DENIED; everything else is Internal.
func mapForkError(err error) error {
	if err == nil {
		return nil
	}
	// The store may already return a PERMISSION_DENIED status (ADR-0013); pass it
	// through unchanged so the wire code is exact.
	if st, ok := status.FromError(err); ok && st.Code() == codes.PermissionDenied {
		return err
	}
	return status.Errorf(codes.Internal, "orchestrator: fork failed: %v", err)
}
