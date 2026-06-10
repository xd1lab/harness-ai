# 19. Session-scoped permission mode (resolves the ADR-0018 §4 deferral)

Date: 2026-06-10
Status: Accepted

## Context

ADR-0018 §4 recorded a deferral: the per-run permission mode was not wired
end-to-end. `RunRequest` carries no `mode` field; `CreateSessionRequest.mode`
(and `Session.mode`) existed in the proto, but the orchestrator only validated a
client-supplied bypass and otherwise **dropped** the value — `Run` hardcoded
`policy.ModeDefault` and `toGenSession` hardcoded `UNSPECIFIED`. So every agent
run executed under the most-restrictive mode regardless of any requested mode,
and the mode-carrying API surface was inert. This is the follow-up that makes the
mode functional and honest.

A permission mode is a **session-level** setting (it is chosen once when the
session is created and applies to every run on that session), so it belongs on
the session aggregate alongside `status` and `lease_epoch`, not on each
`RunRequest`. This avoids any change to the frozen `RunRequest`/event payloads.

## Decision

**The permission mode is persisted on the session aggregate as `sessions.mode`.**

- **Schema (additive, forward-only):** migration `0004_session_mode.up.sql` adds
  `sessions.mode TEXT NOT NULL DEFAULT 'default'` plus a CHECK constraint
  (`mode IN ('default','acceptEdits','plan','bypass')`). `ADD COLUMN ... DEFAULT`
  is metadata-only on PostgreSQL 11+ (no rewrite) and there is no down-migration,
  consistent with the forward-only log-table policy (ADR-0011). The aggregate
  table already carries directly-mutated control state (`status`, `lease_epoch`),
  so a mode column fits the existing model; the value is NOT folded from events.
- **Persisted value spelling.** The stored value is `domain.PermissionMode`
  (event.go vocabulary), whose accept-edits spelling is `"acceptEdits"` — which
  deliberately DIFFERS from `policy.Mode`'s `"accept_edits"`. The orchestrator
  edge therefore converts between the two by **explicit mapping, never a cast**
  (`fromGenModeDomain`, `toPolicyMode`, `toGenMode` in the grpc adapter's
  mapping.go). domain still does not import policy (no cycle).
- **Write path.** `CreateSession` stamps the mode from the VERIFIED request
  (`fromGenModeDomain`); the empty/unspecified request resolves to the secure
  `ModeDefault`. A client-supplied `bypass` is rejected (unchanged; operator-only,
  server-side). The eventstore persists it in the same transaction that inserts
  the session row.
- **Read path.** `Run` reads the session via the ownership check it already
  performs and maps `sessions.mode -> policy.Mode` (`toPolicyMode`) into the
  `RunSpec`, so the loop's policy pipeline runs under the session's mode.
  `GetSession` surfaces it (`toGenMode`).
- **Fork inheritance.** A fork inherits its parent's mode (a fork continues the
  same session configuration).
- **CLI.** `harnessctl --permission-mode default|acceptEdits|plan` sets the mode
  when it creates a session (the `session` subcommand, or `run` without
  `--session`). `bypass` is accepted by the parser but rejected by the server.

## Consequences

- The mode-carrying API surface is now functional: a client may create a session
  in `acceptEdits`/`plan` and every run on it honors that mode; `GetSession`
  reflects it. The secure default (`default` — ask for risk-tiered tools) still
  applies to any session created without a mode and to all pre-migration rows.
- `bypass` remains operator-only and server-side; no client request can store it.
  (An operator-set-bypass path is still future work — the column permits the
  value but no wired path stores it from a request.)
- The deny/ask-by-default posture is unchanged for the keyless demo, which
  continues to run under `ModeDefault` (the stub never calls a tool anyway;
  ADR-0018).
- This resolves ADR-0018 §4. The two-spelling reality between domain and policy
  is now explicit (mapping functions, not casts), removing a latent footgun.
- Verified: unit tests (the mode flows `CreateSession -> session -> RunSpec`, and
  bypass is rejected) and a real-Postgres integration test (persist/load/fork
  round-trip against migration 0004); full `go test`, `go vet`, `gofmt`, and
  `golangci-lint` are green.
