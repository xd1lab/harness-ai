package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// ErrMaxLiveSandboxes is returned by [Runtime.Create] when the max-live-sandboxes
// cap is reached; the caller should apply backpressure and retry later
// (architecture §10.6). Recover it with [errors.Is].
var ErrMaxLiveSandboxes = errors.New("runtime: max live sandboxes reached")

// ErrWorkspaceNotFound is returned by [Runtime.Get] when no live workspace exists
// for the session (e.g. after reaping). Callers re-Create on resume. Recover it with
// [errors.Is].
var ErrWorkspaceNotFound = errors.New("runtime: no live workspace for session")

// SessionStatus is the lifecycle status of a session as seen by the reaper. A
// sandbox whose session is finished/failed/abandoned is reclaimed (architecture
// §10.6).
type SessionStatus int

const (
	// SessionActive means the session is still running; its sandbox is retained.
	SessionActive SessionStatus = iota
	// SessionFinished means the session completed; its sandbox is eligible for
	// immediate reaping regardless of TTL.
	SessionFinished
	// SessionFailed means the session terminated in error; its sandbox is eligible
	// for immediate reaping regardless of TTL.
	SessionFailed
	// SessionAbandoned means the session was abandoned (e.g. lease expired); its
	// sandbox is eligible for immediate reaping regardless of TTL.
	SessionAbandoned
	// SessionUnknown means the status could not be determined; the sandbox is
	// retained (fail-safe: do not reap a sandbox we cannot classify).
	SessionUnknown
)

// SessionStatusFunc reports the current [SessionStatus] of a session, keyed off the
// authoritative sessions table (architecture §10.6). It is injected so the reaper
// does not import the orchestrator/event-store; production wiring supplies a query.
// A nil func disables status-based reaping (only TTLs apply).
type SessionStatusFunc func(ctx context.Context, sessionID string) (SessionStatus, error)

// Runtime is the container-backed [app.RuntimePort]. It manages per-session
// [container] workspaces over the Docker CLI and embeds the lifecycle manager
// (idle/absolute TTL, max-live cap, session-status reaper). It is safe for
// concurrent use.
type Runtime struct {
	cfg    Config
	runner commandRunner
	clk    clock.Clock
	status SessionStatusFunc

	mu      sync.Mutex
	live    map[string]*container
	stopped bool
}

// compile-time assertion that Runtime satisfies the RuntimePort port.
var _ app.RuntimePort = (*Runtime)(nil)

// Option configures a [Runtime].
type Option func(*Runtime)

// WithClock injects the [clock.Clock] used for TTL accounting and the
// SIGTERM→SIGKILL escalation. Production passes [clock.System]; tests pass a fake so
// kill/TTL timing is deterministic. The default is [clock.System].
func WithClock(c clock.Clock) Option {
	return func(r *Runtime) {
		if c != nil {
			r.clk = c
		}
	}
}

// WithSessionStatus injects the [SessionStatusFunc] the reaper consults to reclaim
// sandboxes of finished/failed/abandoned sessions (architecture §10.6). The default
// (nil) reaps on TTL only.
func WithSessionStatus(f SessionStatusFunc) Option {
	return func(r *Runtime) { r.status = f }
}

// withRunner injects the [commandRunner]; unit tests use it to supply a fake so no
// real Docker is needed. Production uses the default [execRunner].
func withRunner(cr commandRunner) Option {
	return func(r *Runtime) {
		if cr != nil {
			r.runner = cr
		}
	}
}

// New returns a container [Runtime] with the given Config (zero fields are filled
// with defaults). It validates the Config and returns an error on a nonsensical
// value. The docker CLI is invoked lazily on the first Create, so New does not probe
// for Docker; readiness is gated separately (architecture §10.1).
func New(cfg Config, opts ...Option) (*Runtime, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	r := &Runtime{
		cfg:  cfg,
		clk:  clock.System{},
		live: make(map[string]*container),
	}
	for _, o := range opts {
		o(r)
	}
	if r.runner == nil {
		r.runner = newExecRunner(cfg.KillGrace)
	}
	return r, nil
}

// Create provisions a fresh container workspace for sessionID with the given egress
// policy. Resume always re-attaches to a FRESH workspace: if a stale container with
// the same name lingers it is force-removed first (clean-workspace resume; ADR-0012;
// architecture §7.5). It respects the max-live cap and returns [ErrMaxLiveSandboxes]
// as backpressure when full (architecture §10.6).
func (r *Runtime) Create(ctx context.Context, sessionID string, egress app.EgressPolicy) (app.Workspace, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("runtime: Create requires a non-empty session id")
	}
	if egress.SessionID == "" {
		egress.SessionID = sessionID
	}

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil, fmt.Errorf("runtime: closed")
	}
	if existing, ok := r.live[sessionID]; ok {
		// An existing live workspace for this session: tear it down so resume gets a
		// clean container (no durable FS snapshot in v1).
		existing.markDestroyed()
		delete(r.live, sessionID)
	} else if len(r.live) >= r.cfg.MaxLive {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: %d live", ErrMaxLiveSandboxes, r.cfg.MaxLive)
	}
	r.mu.Unlock()

	name := containerName(sessionID)

	// Remove any stale container with this name from a prior process (best-effort;
	// ignore "no such container"). This guarantees a fresh workspace on resume.
	_, _ = r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})

	// Create then start the long-lived container.
	if res, err := r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: r.cfg.createArgs(name, egress)}); err != nil {
		return nil, fmt.Errorf("runtime: docker create for session %q: %w", sessionID, err)
	} else if res.ExitCode != 0 {
		return nil, fmt.Errorf("runtime: docker create for session %q: exit %d: %s", sessionID, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	if res, err := r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: startArgs(name)}); err != nil {
		// Roll back the created-but-not-started container so we do not leak it.
		_, _ = r.runner.Run(context.Background(), cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})
		return nil, fmt.Errorf("runtime: docker start for session %q: %w", sessionID, err)
	} else if res.ExitCode != 0 {
		_, _ = r.runner.Run(context.Background(), cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})
		return nil, fmt.Errorf("runtime: docker start for session %q: exit %d: %s", sessionID, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}

	now := r.clk.Now()
	c := &container{
		name:      name,
		sessionID: sessionID,
		cfg:       r.cfg,
		runner:    r.runner,
		clk:       r.clk,
		policy:    egress,
		lastUsed:  now,
		created:   now,
	}

	r.mu.Lock()
	// Re-check the cap and stopped flag under lock in case of a concurrent Create.
	if r.stopped {
		r.mu.Unlock()
		_, _ = r.runner.Run(context.Background(), cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})
		return nil, fmt.Errorf("runtime: closed")
	}
	r.live[sessionID] = c
	r.mu.Unlock()
	return c, nil
}

// Get returns the live workspace for sessionID or [ErrWorkspaceNotFound].
func (r *Runtime) Get(_ context.Context, sessionID string) (app.Workspace, error) {
	r.mu.Lock()
	c, ok := r.live[sessionID]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrWorkspaceNotFound, sessionID)
	}
	return c, nil
}

// Destroy tears down sessionID's container and releases its resources. It is
// idempotent: destroying an absent workspace is not an error (architecture §10.6).
func (r *Runtime) Destroy(ctx context.Context, sessionID string) error {
	r.mu.Lock()
	c, ok := r.live[sessionID]
	if ok {
		delete(r.live, sessionID)
	}
	r.mu.Unlock()

	// Always issue the docker rm even if we had no in-memory record, so a container
	// orphaned by a crash is still reclaimed (idempotent; ignore "no such container").
	name := containerName(sessionID)
	if ok {
		c.markDestroyed()
		name = c.name
	}
	res, err := r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})
	if err != nil {
		return fmt.Errorf("runtime: docker rm for session %q: %w", sessionID, err)
	}
	// `docker rm --force` of an absent container exits non-zero with "No such
	// container"; treat that as success (idempotent destroy).
	if res.ExitCode != 0 && !isNoSuchContainer(res.Stderr) {
		return fmt.Errorf("runtime: docker rm for session %q: exit %d: %s", sessionID, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// Close stops the runtime and destroys every live sandbox. It is safe to call once;
// subsequent Create calls return an error.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	sessions := make([]string, 0, len(r.live))
	for id := range r.live {
		sessions = append(sessions, id)
	}
	r.mu.Unlock()

	var firstErr error
	for _, id := range sessions {
		if err := r.Destroy(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// isNoSuchContainer reports whether docker's stderr indicates the container did not
// exist, which makes a remove idempotent.
func isNoSuchContainer(stderr []byte) bool {
	s := strings.ToLower(string(stderr))
	return strings.Contains(s, "no such container") || strings.Contains(s, "is not running") || strings.Contains(s, "no such object")
}
