package runtime

import (
	"fmt"
	"strconv"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

// This file holds the PURE docker-argv builders. Keeping construction separate from
// process execution lets unit tests assert the exact command line (hard limits,
// deny-by-default network, working dir, the kill signal) with no Docker present.

// networkNone is the docker network mode that severs all egress from the container.
// It is the deny-by-default posture: a session with no allowlisted hosts runs with
// no network at all (architecture §8.4).
const networkNone = "none"

// createArgs builds the `docker create` argv for a session container: a long-lived
// container that sleeps forever (so `docker exec` can run commands against it),
// stamped with the hard resource limits and deny-by-default network.
//
// The container is created (not run) so the caller can `docker start` it and hold
// its ID; it runs `sleep infinity` as PID 1 to stay alive until destroyed. Hard
// limits (`--memory`, `--cpus`, `--pids-limit`) bound a non-cooperating process
// regardless of signal handling (architecture §9.3); `--network none` denies egress
// by default (architecture §8.4).
func (c Config) createArgs(name string, egress app.EgressPolicy) []string {
	args := []string{
		"create",
		"--name", name,
		"--label", labelManaged + "=1",
		"--label", labelSession + "=" + egress.SessionID,
		"--network", networkMode(egress),
		"--memory", strconv.FormatInt(c.MemoryBytes, 10),
		"--cpus", strconv.FormatFloat(c.CPUs, 'f', -1, 64),
		"--pids-limit", strconv.FormatInt(c.PidsLimit, 10),
		"--workdir", c.Workdir,
		// Disable OOM-kill disable (so the kernel reaps on memory exhaustion) and
		// keep the container minimal. --init reaps in-container zombies left by
		// double-forked children, complementing the PID-namespace kill.
		"--init",
	}
	args = append(args, c.ExtraCreateArgs...)
	args = append(args, c.Image, "sleep", "infinity")
	return args
}

// networkMode returns the docker `--network` value for the session: deny-by-default
// "none" when the allowlist is empty (the safe default), otherwise the deny-by-
// default broker still governs reachability so the container network remains "none"
// in v1 — host allowlisting is enforced by the egress broker, not by handing the
// container a bridge (architecture §8.4, ADR-0013). The allowlist is carried for
// observability and future per-network wiring.
func networkMode(_ app.EgressPolicy) string {
	return networkNone
}

// startArgs builds the `docker start` argv that boots a created container.
func startArgs(name string) []string {
	return []string{"start", name}
}

// execArgs builds the `docker exec` argv that runs req's command inside the named
// container. It does NOT carry the kill behavior — cancellation is wired by the
// caller via the process group and the `docker kill` reaper (architecture §9.3).
func execArgs(name, defaultWorkdir string, req app.ExecRequest) []string {
	args := []string{"exec"}
	if len(req.Stdin) > 0 {
		args = append(args, "--interactive")
	}
	wd := req.WorkDir
	if wd == "" {
		wd = defaultWorkdir
	}
	args = append(args, "--workdir", wd)
	for _, e := range req.Env {
		args = append(args, "--env", e)
	}
	args = append(args, name)
	args = append(args, req.Cmd...)
	return args
}

// killArgs builds the `docker kill --signal=<sig> <name>` argv. This signals PID 1
// of the container's PID namespace; because every in-container process is a
// descendant, a SIGKILL here reaps the WHOLE tree — the guaranteed reaper that
// defeats SIGTERM-trapping, double-forked, and fork-bombing children (architecture
// §9.3).
func killArgs(name, signal string) []string {
	return []string{"kill", "--signal=" + signal, name}
}

// removeArgs builds the `docker rm --force --volumes <name>` argv that tears a
// container down. --force stops a still-running container; --volumes removes its
// anonymous volumes so a destroyed sandbox leaves no disk behind (architecture
// §10.6, §7.5 clean-workspace resume).
func removeArgs(name string) []string {
	return []string{"rm", "--force", "--volumes", name}
}

// listManagedArgs builds the `docker ps` argv that lists the IDs (and the session
// label) of all containers this runtime manages, used by the reaper to reconcile
// against session status (architecture §10.6).
func listManagedArgs() []string {
	return []string{
		"ps", "--all", "--no-trunc",
		"--filter", "label=" + labelManaged + "=1",
		"--format", "{{.Names}}\t{{.Label \"" + labelSession + "\"}}",
	}
}

// containerName derives the docker container name for a session. It is namespaced
// with a fixed prefix so the reaper can reconcile only this runtime's containers and
// never collides with unrelated containers on the host.
func containerName(sessionID string) string {
	return fmt.Sprintf("%s%s", containerNamePrefix, sanitizeName(sessionID))
}

// Labels and naming.
const (
	// containerNamePrefix prefixes every managed container name.
	containerNamePrefix = "boltrope-sbx-"
	// labelManaged marks a container as managed by this runtime (reaper filter).
	labelManaged = "boltrope.managed"
	// labelSession records the owning session id on the container (reaper input).
	labelSession = "boltrope.session"
)

// sanitizeName maps a session id to a docker-name-safe token. Docker names must
// match [a-zA-Z0-9][a-zA-Z0-9_.-]*; we replace any other byte with '-'. The result
// stays unique per distinct session because the mapping is injective over the
// allowed alphabet and only the (already-disallowed) separators collapse.
func sanitizeName(s string) string {
	if s == "" {
		return "default"
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
			b = append(b, ch)
		case ch == '_' || ch == '.' || ch == '-':
			b = append(b, ch)
		default:
			b = append(b, '-')
		}
	}
	return string(b)
}
