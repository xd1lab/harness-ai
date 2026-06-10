package runtime

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/boltrope/boltrope/internal/platform/clock"
	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

// container is the per-session [app.Workspace] backed by a single long-lived docker
// container reached through the [commandRunner]. It is safe for concurrent use.
type container struct {
	name      string
	sessionID string
	cfg       Config
	runner    commandRunner
	clk       clock.Clock

	mu        sync.Mutex
	policy    app.EgressPolicy
	lastUsed  time.Time
	created   time.Time
	destroyed bool
}

// compile-time assertion that container satisfies the Workspace port.
var _ app.Workspace = (*container)(nil)

// Exec runs req inside the container via `docker exec`, enforcing the absolute
// wall-clock cap and wiring cancellation to a REAL process-tree kill.
//
// Cancellation contract (architecture §9.3): when ctx is cancelled OR the wall-clock
// cap fires, the host-side exec client's process group is signaled AND, as the
// guaranteed reaper, `docker kill` (SIGTERM→SIGKILL after KillGrace) is issued
// against the container's PID namespace, so a SIGTERM-trapping process, a
// double-forked detached child, or a fork bomb cannot survive. The hard PidsLimit
// bounds a fork bomb regardless of signaling. The returned [app.ExecResult] has
// Killed=true whenever the runtime terminated the process rather than it exiting.
func (c *container) Exec(ctx context.Context, req app.ExecRequest) (app.ExecResult, error) {
	if err := c.checkLive(); err != nil {
		return app.ExecResult{}, err
	}
	c.touch()

	// Absolute wall-clock cap: a derived context that the watcher reaps on expiry,
	// independent of any deadline the caller passed (architecture §9.3).
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wallTimer := c.clk.NewTimer(c.cfg.WallClock)
	defer wallTimer.Stop()

	// reapKilled records whether the watcher issued the `docker kill` reaper, so Exec
	// can report Killed deterministically. Guarded by reapMu against the watcher.
	var (
		reapMu     sync.Mutex
		reapKilled bool
	)
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-wallTimer.C():
			// Wall-clock cap hit: cancel the exec client, then reap the container.
			cancel()
		case <-execCtx.Done():
			// execCtx is cancelled either by a caller/parent cancellation (reap) or
			// by Exec's own cancel() after a clean Run (no reap needed). If the
			// caller did not cancel, nothing escaped — skip the reaper kill.
			if ctx.Err() == nil {
				return
			}
		}
		// Guaranteed reaper: kill the container's PID namespace. A detached context
		// is used so the kill itself runs even though execCtx is now cancelled.
		c.reapProcessTree()
		reapMu.Lock()
		reapKilled = true
		reapMu.Unlock()
	}()

	spec := cmdSpec{
		Name:         c.cfg.DockerBin,
		Args:         execArgs(c.name, c.cfg.Workdir, req),
		Stdin:        req.Stdin,
		ProcessGroup: true,
	}
	res, err := c.runner.Run(execCtx, spec)

	// Stop the watcher and wait for any in-flight reap to settle.
	cancel()
	<-watcherDone

	if err != nil {
		return app.ExecResult{}, fmt.Errorf("runtime: docker exec for session %q: %w", c.sessionID, err)
	}

	reapMu.Lock()
	killed := res.Killed || reapKilled || ctx.Err() != nil
	reapMu.Unlock()

	return app.ExecResult{
		ExitCode: res.ExitCode,
		Stdout:   res.Stdout,
		Stderr:   res.Stderr,
		Killed:   killed,
	}, nil
}

// reapProcessTree issues the guaranteed `docker kill` reaper against the container's
// PID namespace: SIGTERM first, then SIGKILL after KillGrace if anything survives.
// Because PID 1 (sleep infinity) stays alive, SIGTERM/SIGKILL here target the exec'd
// command's descendants; the container itself is torn down only by Destroy. Killing
// the namespace ensures detached/forked/SIGTERM-trapping children die (architecture
// §9.3).
func (c *container) reapProcessTree() {
	// Detached context: the reaper must run even though the caller's ctx is done.
	bg := context.Background()
	// SIGTERM the container's processes first (graceful).
	_, _ = c.runner.Run(bg, cmdSpec{Name: c.cfg.DockerBin, Args: killArgs(c.name, "TERM")})

	// Escalate to SIGKILL after the grace window. Use the injected clock so tests
	// drive the escalation deterministically.
	t := c.clk.NewTimer(c.cfg.KillGrace)
	defer t.Stop()
	<-t.C()
	_, _ = c.runner.Run(bg, cmdSpec{Name: c.cfg.DockerBin, Args: killArgs(c.name, "KILL")})
}

// Read returns the contents of the file at path within the container via
// `docker exec cat`. A non-zero exit (missing file, not readable) is surfaced as an
// error so the tool layer can report it.
func (c *container) Read(ctx context.Context, p string) ([]byte, error) {
	if err := c.checkLive(); err != nil {
		return nil, err
	}
	abs := c.abs(p)
	res, err := c.runner.Run(ctx, cmdSpec{
		Name: c.cfg.DockerBin,
		Args: execArgs(c.name, c.cfg.Workdir, app.ExecRequest{Cmd: []string{"cat", "--", abs}}),
	})
	if err != nil {
		return nil, fmt.Errorf("runtime: read %q: %w", abs, err)
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("runtime: read %q: exit %d: %s", abs, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

// Write writes data to the file at path within the container, creating or
// truncating it. It feeds the bytes over stdin to `tee` so the write is binary-safe
// and never interpolates data into the argv. Parent directories are created first.
func (c *container) Write(ctx context.Context, p string, data []byte) error {
	if err := c.checkLive(); err != nil {
		return err
	}
	abs := c.abs(p)
	if dir := path.Dir(abs); dir != "" && dir != "." && dir != "/" {
		if err := c.Mkdir(ctx, dir); err != nil {
			return err
		}
	}
	res, err := c.runner.Run(ctx, cmdSpec{
		Name:  c.cfg.DockerBin,
		Args:  execArgs(c.name, c.cfg.Workdir, app.ExecRequest{Cmd: []string{"tee", "--", abs}, Stdin: data}),
		Stdin: data,
	})
	if err != nil {
		return fmt.Errorf("runtime: write %q: %w", abs, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("runtime: write %q: exit %d: %s", abs, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// Mkdir creates the directory at path within the container (including parents) via
// `docker exec mkdir -p`.
func (c *container) Mkdir(ctx context.Context, p string) error {
	if err := c.checkLive(); err != nil {
		return err
	}
	abs := c.abs(p)
	res, err := c.runner.Run(ctx, cmdSpec{
		Name: c.cfg.DockerBin,
		Args: execArgs(c.name, c.cfg.Workdir, app.ExecRequest{Cmd: []string{"mkdir", "-p", "--", abs}}),
	})
	if err != nil {
		return fmt.Errorf("runtime: mkdir %q: %w", abs, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("runtime: mkdir %q: exit %d: %s", abs, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// NetworkPolicy returns the EgressPolicy currently in force for this workspace.
func (c *container) NetworkPolicy(_ context.Context) (app.EgressPolicy, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.policy, nil
}

// abs resolves p against the workspace working directory when it is relative, so a
// tool that passes a bare filename writes inside the workspace, not the container
// root.
func (c *container) abs(p string) string {
	if path.IsAbs(p) {
		return path.Clean(p)
	}
	return path.Clean(path.Join(c.cfg.Workdir, p))
}

// touch records the last-used instant for idle-TTL accounting.
func (c *container) touch() {
	c.mu.Lock()
	c.lastUsed = c.clk.Now()
	c.mu.Unlock()
}

// checkLive returns an error if the container has been destroyed.
func (c *container) checkLive() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyed {
		return fmt.Errorf("runtime: workspace for session %q has been destroyed", c.sessionID)
	}
	return nil
}

// markDestroyed flags the container as torn down so further operations fail fast.
func (c *container) markDestroyed() {
	c.mu.Lock()
	c.destroyed = true
	c.mu.Unlock()
}

// idleSince reports the last-used instant for the reaper.
func (c *container) idleSince() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUsed
}

// createdAt reports the creation instant for absolute-TTL accounting.
func (c *container) createdAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.created
}
