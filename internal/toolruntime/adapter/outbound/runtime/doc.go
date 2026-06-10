// Package runtime implements the tool-runtime's
// [github.com/xd1lab/harness-ai/internal/toolruntime/app.RuntimePort] and
// [github.com/xd1lab/harness-ai/internal/toolruntime/app.Workspace] over the Docker
// CLI. It is the v1 container backend for the trust boundary that runs
// model-influenced code (ADR-0005; ADR-0014; architecture §7.5, §9.3, §10.6).
//
// # Why the Docker CLI (os/exec) and not a Go SDK
//
// The backend shells out to the `docker` binary via [os/exec] rather than linking a
// Go Docker SDK. This keeps the dependency surface tiny (no client library to track
// against daemon API versions), works identically on the Windows dev host and Linux
// CI, and makes the exact lifecycle commands trivially auditable. The single
// [commandRunner] seam injected into [Runtime] lets unit tests assert the precise
// argv and the cancellation-to-kill wiring with no real Docker present; production
// wiring uses [execRunner], a thin [os/exec] adapter.
//
// # Lifecycle (architecture §7.5, §10.6)
//
//   - Create starts a long-lived container per session from a small Linux base image
//     with deny-by-default network (`--network none` unless the egress policy names
//     hosts), hard resource limits (`--memory`, `--cpus`, `--pids-limit`) and an
//     absolute wall-clock cap, plus a workspace working directory.
//   - Exec runs a command via `docker exec`. Cancelling the ctx triggers a REAL
//     process-tree kill: the exec'd client process group is signaled AND, as the
//     guaranteed reaper, `docker kill` (SIGTERM→SIGKILL with a deadline) terminates
//     the container's whole PID namespace so detached / SIGTERM-trapping / forked
//     children cannot survive (architecture §9.3).
//   - Destroy force-removes the container (idempotent).
//   - The lifecycle manager ([Runtime.RunReaper]/[Runtime.reapOnce]) enforces idle
//     and absolute TTLs, a max-live-sandboxes cap with backpressure, and a reaper
//     keyed off session status that destroys sandboxes whose session is
//     finished/failed/abandoned (architecture §10.6).
//
// Resume always re-attaches to a FRESH workspace (clean-workspace resume; no durable
// FS snapshot in v1; ADR-0012 §"Clean-workspace resume"; architecture §7.5).
//
// # Concurrency
//
// [Runtime] and [container] are safe for concurrent use by multiple goroutines.
package runtime
