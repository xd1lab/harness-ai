# 17. Operability and observability: health/readiness, startup/migration gate, RED/USE metrics + SLOs, stuck-loop detection, sandbox lifecycle

Date: 2026-06-10
Status: Accepted

## Context

The draft had no health/readiness/startup/metrics story. A tracing-only observability
posture cannot tell a small team that the fleet is degrading, sandboxes are leaking,
the connection pool is exhausted, or an agent is stuck in a loop burning budget. Without
readiness gates on actual dependency reachability, a process that is up but whose
dependencies are down accepts traffic it cannot serve. Without a migration gate,
deploying services before the schema is current risks runtime errors or data corruption.
Without sandbox lifecycle management, per-session containers and disk accumulate
indefinitely.

## Decision

**Health and readiness on every service.** All services expose gRPC health checking
(`grpc.health.v1.Health`) plus HTTP `/livez` and `/readyz`. Readiness gates on actual
dependency reachability, not just process-up:

- Orchestrator readiness: PostgreSQL ping, downstream gRPC health for model-gateway
  and tool-runtime, SVID present.
- Model-gateway readiness: SVID present, optional provider reachability probe.
- Tool-runtime readiness: SVID present, container runtime available.
- Projectord readiness: PostgreSQL ping; reports projection lag as a metric.

**Startup ordering and migration gate.** `cmd/boltrope-migrate` is a release gate: it
runs to completion before any orchestrator instance accepts traffic. Event-store schema
changes are expand/contract, forward-only (ADR-0011) so a rolling deploy is safe.
`docker-compose.yml` declares ordering via `depends_on` and `healthcheck` (Postgres
healthy → migrate completes → services start). For Kubernetes: an init container or
readiness gate runs migrate; services use readiness probes so traffic is not routed to
not-yet-ready pods. SPIRE bootstrap ordering: no service completes an mTLS handshake
until the SPIRE agent has attested it and issued an SVID. The dev static-cert fallback
(`BOLTROPE_DEV_INSECURE=1`, ADR-0013) gives a deterministic, SPIRE-free start for
laptops and CI.

**RED metrics per RPC.** Request count, error rate broken down by typed termination
subtype (success, error_max_turns, error_max_budget_usd, error_during_execution,
error_max_structured_output_retries), and duration for Run, Generate, ExecuteTool, and
Control. Errors are typed so alerts can distinguish budget exhaustion from execution
errors from infrastructure failures.

**USE/saturation gauges.** Errgroup worker-pool occupancy, live sandbox count versus
cap, PostgreSQL connection-pool utilization, blob-store usage, and projection lag. These
are the dimensions most likely to cause silent degradation before error rates rise.

**Baseline SLOs and alerts.** A small team needs: event-store append error rate,
sandbox count near cap, pool exhaustion, projection lag, and stuck-session count.
These cover the five failure modes most likely to be invisible without explicit
instrumentation.

**Stuck-loop detection.** Repeated identical tool calls or no progress within a window
raise an operational signal before the eventual max-turns or max-budget cap fires. This
bounds the budget burned by a stuck agent and gives operators an early warning distinct
from a normal long-running turn.

**OTel GenAI spans.** Orchestrator emits an `invoke_agent` span per Run and per
sub-agent; model-gateway emits a `chat` span per Generate with gen_ai.* attributes
including usage and finish reason; tool-runtime emits an `execute_tool` span per
ExecuteTool. Trace context propagates over gRPC via otelgrpc. trace_id and span_id
correlate into slog JSON logs. Logs stay slog-JSON (OTel logs are still beta).

**Degradation policy.** When a dependency is down the orchestrator fails the in-flight
turn with a typed `error_during_execution` after a bounded retry-with-backoff budget,
rather than hanging. New Run requests are rejected with a typed unavailable error while
a hard dependency is down (readyz is already false). Append idempotency (ADR-0011)
makes the retry-with-backoff on the append path safe.

**Projectord safe-advance and lifecycle.** Projectord is a named, deployed owner of
cost-rollup and OTel-export projections, not an orphaned in-process concern. It uses
the xmin-bounded safe-advance cursor (ADR-0011) so out-of-order commits are never
silently dropped. LISTEN/NOTIFY is only a wakeup hint; on reconnect the poller resumes
from the stored cursor checkpoint. If projectord lags or restarts it never blocks a
turn. A max-lag alert fires when a projection falls behind threshold.

**Sandbox lifecycle management.** A `sandboxmgr` in tool-runtime prevents container
and disk leaks:

- Idle TTL and absolute TTL per sandbox.
- Max-live-sandboxes cap per node with backpressure.
- Reconciliation and GC loop that reaps containers whose session is finished, failed,
  or abandoned (keyed off session status in PostgreSQL) and on orchestrator crash.

Combined with the clean-workspace resume model (ADR-0012), an abandoned mid-turn
container is safely reaped and a fresh one created on resume.

## Consequences

- Readiness gates on dependency reachability mean a service that cannot serve traffic
  does not receive it; health probes drive load-balancer and orchestrator routing.
- The migration gate prevents schema-current/code-ahead mismatches during rolling
  deploys; expand/contract DDL ordering makes the gate safe for zero-downtime updates.
- RED metrics broken down by typed termination subtype give actionable alert signal:
  a spike in error_during_execution is different from error_max_turns and requires a
  different response.
- USE gauges catch the silent-degradation failure modes (pool exhaustion, sandbox cap)
  before error rates rise.
- Stuck-loop detection bounds budget burned by pathological agent behavior and provides
  an early warning distinct from normal long-running reasoning turns.
- The sandbox TTL and GC loop prevent container and disk accumulation, which would
  otherwise exhaust node resources silently.
- Projectord's independence from the request path means projection lag never blocks a
  turn; the max-lag alert provides the necessary operational signal for projection health.
