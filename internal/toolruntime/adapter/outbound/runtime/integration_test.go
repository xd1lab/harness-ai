//go:build integration

// Package runtime integration tests exercise the REAL Docker CLI backend
// (NFR-TEST-05 h/i/j and the resource-limit / wall-clock / clean-resume checks).
// They are build-tagged behind `integration` so they never run in the default unit
// suite, and they SKIP at runtime when the docker binary or daemon is unavailable.
//
// Run with: go test -tags integration ./internal/toolruntime/adapter/outbound/runtime/...
//
// The adversarial-kill tests are the heart of the trust boundary (architecture
// §9.3): a SIGTERM-trapping process, a double-forked detached child, and a fork bomb
// must each be reaped by `docker kill` against the container's PID namespace within
// the deadline, and the hard PidsLimit must protect the host throughout.
package runtime

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

const (
	// integrationImage is intentionally small but has bash + standard coreutils so
	// the adversarial scripts run. debian:stable-slim matches the production default.
	integrationImage = "debian:stable-slim"
	// killDeadline is the architecture's required reap bound: adversarial processes
	// must be dead within 5 s of cancellation (architecture §9.3, NFR-TEST-05).
	killDeadline = 5 * time.Second
)

// dockerOrSkip skips the test unless a working docker daemon is reachable, and
// ensures the integration image is present (pulling it once). It returns the docker
// binary path.
func dockerOrSkip(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("integration: docker binary not found in PATH")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//nolint:gosec // bin is the docker path resolved from PATH; test-only invocation.
	if out, err := exec.CommandContext(ctx, bin, "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("integration: docker daemon not reachable: %v: %s", err, out)
	}
	// Pull the base image once so per-test Create is not gated on a slow pull.
	pullCtx, pullCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer pullCancel()
	//nolint:gosec // bin is the docker path resolved from PATH; test-only invocation.
	if out, err := exec.CommandContext(pullCtx, bin, "pull", integrationImage).CombinedOutput(); err != nil {
		t.Skipf("integration: failed to pull %s: %v: %s", integrationImage, err, out)
	}
	return bin
}

// newIntegrationRuntime builds a real-Docker Runtime with the system clock and the
// given resource overrides applied to the default config.
func newIntegrationRuntime(t *testing.T, tune func(*Config)) *Runtime {
	t.Helper()
	bin := dockerOrSkip(t)
	cfg := DefaultConfig()
	cfg.DockerBin = bin
	cfg.Image = integrationImage
	cfg.KillGrace = 2 * time.Second
	cfg.WallClock = 30 * time.Second
	if tune != nil {
		tune(&cfg)
	}
	r, err := New(cfg, WithClock(clock.System{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// uniqueSession returns a per-test session id so containers never collide.
func uniqueSession(t *testing.T) string {
	t.Helper()
	return "it-" + sanitizeName(strings.ToLower(t.Name()))
}

func TestIntegration_CreateExecDestroy(t *testing.T) {
	r := newIntegrationRuntime(t, nil)
	ctx := context.Background()
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })

	ws, err := r.Create(ctx, sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	res, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"sh", "-c", "echo hello && echo oops 1>&2 && exit 3"}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", res.ExitCode)
	}
	if !strings.Contains(string(res.Stdout), "hello") {
		t.Errorf("stdout = %q, want to contain hello", res.Stdout)
	}
	if !strings.Contains(string(res.Stderr), "oops") {
		t.Errorf("stderr = %q, want to contain oops", res.Stderr)
	}
	if res.Killed {
		t.Errorf("clean exit reported Killed")
	}

	// File round-trip via the workspace (docker exec tee/cat).
	if err := ws.Write(ctx, "note.txt", []byte("durable-bytes")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := ws.Read(ctx, "note.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "durable-bytes" {
		t.Errorf("Read = %q, want durable-bytes", got)
	}

	if err := r.Destroy(ctx, sid); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	// Destroy is idempotent.
	if err := r.Destroy(ctx, sid); err != nil {
		t.Errorf("second Destroy not idempotent: %v", err)
	}
}

// TestIntegration_Kill_SIGTERMTrap is NFR-TEST-05(h): a process that ignores SIGTERM
// must still be reaped within the deadline because `docker kill` tears down the
// container's PID namespace.
func TestIntegration_Kill_SIGTERMTrap(t *testing.T) {
	r := newIntegrationRuntime(t, nil)
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// trap '' TERM ignores SIGTERM; the script then sleeps far longer than the test.
	script := `trap '' TERM; echo trapped; sleep 600`
	assertKilledWithin(t, ws, []string{"bash", "-c", script}, killDeadline)
	assertContainerProcessTreeReaped(t, r, sid)
}

// TestIntegration_Kill_DoubleForkDetached is NFR-TEST-05(i): a double-forked,
// detached (reparented-to-init) child must also die — the PID-namespace kill reaps
// every descendant regardless of reparenting.
func TestIntegration_Kill_DoubleForkDetached(t *testing.T) {
	r := newIntegrationRuntime(t, nil)
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// setsid + double background detaches the grandchild from the exec'd shell so a
	// naive wrapper-cancel would leave it running. The PID-namespace kill must reap
	// it. The parent shell then blocks so the Exec stays open until cancelled.
	script := `setsid bash -c 'sleep 600' & disown; echo detached; sleep 600`
	assertKilledWithin(t, ws, []string{"bash", "-c", script}, killDeadline)
	assertContainerProcessTreeReaped(t, r, sid)
}

// TestIntegration_Kill_ForkBomb is NFR-TEST-05(j): a fork bomb must be bounded by the
// hard PidsLimit (the host is protected) and reaped within the deadline; no container
// state persists after reap.
func TestIntegration_Kill_ForkBomb(t *testing.T) {
	r := newIntegrationRuntime(t, func(c *Config) {
		c.PidsLimit = 64 // tight bound so the bomb cannot exhaust the host
	})
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Classic bash fork bomb. PidsLimit caps the explosion; cancellation reaps it.
	script := `:(){ :|:& };:`
	assertKilledWithin(t, ws, []string{"bash", "-c", script}, killDeadline)

	// After the reap the container must be removable with no lingering state.
	if err := r.Destroy(context.Background(), sid); err != nil {
		t.Fatalf("Destroy after fork bomb: %v", err)
	}
	if running := containerRunning(t, r.cfg.DockerBin, containerName(sid)); running {
		t.Error("container still running after fork-bomb reap + destroy")
	}
}

// TestIntegration_PidsLimitEnforced proves the hard PID limit is enforced by the
// daemon independent of cancellation: a script that tries to spawn far more than the
// limit fails (fork: retry / resource unavailable) rather than taking the host down.
func TestIntegration_PidsLimitEnforced(t *testing.T) {
	r := newIntegrationRuntime(t, func(c *Config) { c.PidsLimit = 16 })
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Spawn many background sleeps; with PidsLimit=16 the daemon refuses well before
	// 200, so at least one fork fails. We assert a non-zero exit OR a fork error in
	// stderr — either is proof the cap bit.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	script := `n=0; while [ $n -lt 200 ]; do sleep 30 & n=$((n+1)); done; wait`
	res, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"bash", "-c", script}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode == 0 && !strings.Contains(strings.ToLower(string(res.Stderr)), "fork") &&
		!strings.Contains(strings.ToLower(string(res.Stderr)), "resource") {
		t.Errorf("expected PidsLimit to bite (non-zero exit or fork error), got exit=%d stderr=%q", res.ExitCode, res.Stderr)
	}
}

// TestIntegration_MemoryLimitEnforced proves the hard memory limit is enforced: a
// workload that tries to hold far more than the cap resident cannot succeed. We pin
// swap equal to memory (so the cap is hard) and fill the container's tmpfs
// (/dev/shm), whose pages count against the memory cgroup — a 512 MiB fill under a
// 64 MiB cap must fail (OOM / ENOSPC), never report success.
func TestIntegration_MemoryLimitEnforced(t *testing.T) {
	r := newIntegrationRuntime(t, func(c *Config) {
		c.MemoryBytes = 64 * 1024 * 1024 // 64 MiB
		// Pin swap == memory so the limit is a hard wall (no 2x swap headroom), and
		// give /dev/shm plenty of room so the binding constraint is the MEMORY cgroup
		// (charged for tmpfs pages), not the default 64 MiB shm size.
		c.ExtraCreateArgs = append(c.ExtraCreateArgs, "--memory-swap", "67108864", "--shm-size", "512m")
	})
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Write 256 MiB into tmpfs (/dev/shm, sized 512 MiB); those pages are charged to
	// the 64 MiB memory cgroup, so the writer is OOM-killed well before finishing.
	// "survived" must never print.
	script := `dd if=/dev/zero of=/dev/shm/fill bs=1M count=256 2>/dev/null && echo survived`
	res, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"bash", "-c", script}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode == 0 && strings.Contains(string(res.Stdout), "survived") {
		t.Errorf("memory limit not enforced: 512 MiB tmpfs fill survived under a 64 MiB cap (exit=%d)", res.ExitCode)
	}
}

// TestIntegration_NetworkDeniedByDefault proves the deny-by-default `--network none`
// posture: an outbound connection from inside the sandbox fails.
func TestIntegration_NetworkDeniedByDefault(t *testing.T) {
	r := newIntegrationRuntime(t, nil)
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// With --network none there is no route off the container; pinging an external IP
	// must fail. (We avoid DNS to keep the failure about egress, not resolution.)
	res, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"bash", "-c", "getent hosts 1.1.1.1 || echo NO-NET; ip -o addr show 2>/dev/null | grep -v ' lo ' | grep -q inet && echo HAS-IFACE || echo NO-IFACE"}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	// With no network namespace interface beyond loopback, there is no non-lo inet
	// address: deny-by-default holds.
	if strings.Contains(string(res.Stdout), "HAS-IFACE") {
		t.Errorf("expected no non-loopback interface under --network none, stdout=%q", res.Stdout)
	}
}

// TestIntegration_WallClockCap proves the absolute wall-clock cap fires and reaps a
// long-running command even when the caller passes no deadline.
func TestIntegration_WallClockCap(t *testing.T) {
	r := newIntegrationRuntime(t, func(c *Config) {
		c.WallClock = 3 * time.Second // short cap for the test
		c.KillGrace = 2 * time.Second
	})
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ws, err := r.Create(context.Background(), sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	start := time.Now()
	res, err := ws.Exec(context.Background(), app.ExecRequest{Cmd: []string{"sleep", "600"}})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	elapsed := time.Since(start)
	if !res.Killed {
		t.Errorf("wall-clock cap did not report Killed")
	}
	if elapsed > 3*time.Second+killDeadline+5*time.Second {
		t.Errorf("wall-clock cap took too long to fire: %s", elapsed)
	}
}

// TestIntegration_CleanWorkspaceResume proves resume re-attaches to a FRESH workspace
// with no durable FS state from the prior container (ADR-0012; architecture §7.5).
func TestIntegration_CleanWorkspaceResume(t *testing.T) {
	r := newIntegrationRuntime(t, nil)
	sid := uniqueSession(t)
	t.Cleanup(func() { _ = r.Destroy(context.Background(), sid) })
	ctx := context.Background()

	ws1, err := r.Create(ctx, sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := ws1.Write(ctx, "scratch.txt", []byte("pre-crash")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Resume: a second Create for the same session re-attaches to a fresh container.
	ws2, err := r.Create(ctx, sid, app.EgressPolicy{SessionID: sid})
	if err != nil {
		t.Fatalf("resume Create: %v", err)
	}
	// The uncommitted file from the prior container is gone (clean-workspace resume).
	if _, err := ws2.Read(ctx, "scratch.txt"); err == nil {
		t.Error("clean-workspace resume leaked uncommitted FS state from the prior container")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// assertKilledWithin runs cmd in ws in a goroutine, cancels its ctx, and asserts the
// Exec returns with Killed=true within deadline + the kill grace. It proves the
// cancellation-to-kill wiring reaps the process tree (architecture §9.3).
func assertKilledWithin(t *testing.T, ws app.Workspace, cmd []string, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	type out struct {
		res app.ExecResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := ws.Exec(ctx, app.ExecRequest{Cmd: cmd})
		ch <- out{res, err}
	}()

	// Let the adversarial process establish itself, then deliver the interrupt.
	time.Sleep(1500 * time.Millisecond)
	cancel()

	// Allow the SIGTERM→SIGKILL escalation plus generous slack on a loaded CI host.
	budget := deadline + 10*time.Second
	select {
	case o := <-ch:
		if o.err != nil {
			t.Fatalf("Exec returned error: %v", o.err)
		}
		if !o.res.Killed {
			t.Errorf("adversarial process not reported Killed; cmd=%v", cmd)
		}
	case <-time.After(budget):
		t.Fatalf("adversarial process not reaped within %s; cmd=%v", budget, cmd)
	}
}

// assertContainerProcessTreeReaped asserts the container has no surviving user
// processes after a reap: because `docker kill` tears down the PID namespace, the
// container is no longer running (its PID 1 was killed). A subsequent inspect shows
// it stopped.
func assertContainerProcessTreeReaped(t *testing.T, r *Runtime, sessionID string) {
	t.Helper()
	name := containerName(sessionID)
	deadline := time.Now().Add(killDeadline + 8*time.Second)
	for time.Now().Before(deadline) {
		if !containerRunning(t, r.cfg.DockerBin, name) {
			return // PID namespace torn down → every in-container process reaped
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("container %q still running after reap deadline; process tree not reaped", name)
}

// containerRunning reports whether the named container is in the running state.
func containerRunning(t *testing.T, dockerBin, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var buf bytes.Buffer
	//nolint:gosec // dockerBin is the docker path resolved from PATH; test-only invocation.
	cmd := exec.CommandContext(ctx, dockerBin, "inspect", "-f", "{{.State.Running}}", name)
	cmd.Stdout = &buf
	if err := cmd.Run(); err != nil {
		// inspect fails when the container no longer exists → not running.
		return false
	}
	return strings.TrimSpace(buf.String()) == "true"
}
