# The durable event ledger (and surviving a crash)

Boltrope's core claim is that a session is an **append-only event log in
PostgreSQL**, not state in a process's memory. That log is what lets a crashed
run resume where it stopped, what makes every run replayable and auditable
afterward, and what stops an agent from repeating an action it already took.
This example makes that tangible.

## Run it

Keyless dev stack up, plus `psql` reachable through the compose Postgres
container:

```bash
./demo.sh
```

## What it does

1. Creates a session and runs a task over REST.
2. **Reads the durable log directly** from Postgres — the source of truth:

   ```
    seq |   event_type
   -----+------------------
      1 | SessionStarted
      2 | MessageAppended
      3 | TurnStarted
      4 | AssistantMessage
      5 | TurnFinished
   ```

   The per-session `seq` is a contiguous integer sequence the database
   enforces (optimistic + fenced append) — there are no gaps, and a re-sent
   request is a no-op rather than a double-append.

3. **Restarts the orchestrator mid-life** (`docker compose restart
   orchestratord`) and then reads the session back:

   ```
   before restart:  headSeq = 5
   after  restart:  headSeq = 5   ← projection rebuilt from the durable log
   ```

   The orchestrator holds nothing durable itself: `GET /v1/sessions/{id}`
   re-derives the projection from the log every time, so killing and replacing
   the process loses nothing.

## Why it matters

- **Actions don't double-fire.** A durable ledger records each mutating tool
  call, so a crash-and-restart resumes without repeating an action that already
  happened — an email already sent is not sent again, a file already written is
  not written twice.
- **A crashed run resumes where it stopped.** Cost and progress are events in
  the log; a resumed session continues from the last recorded turn instead of
  replaying completed work. _(For long, expensive runs that also means you
  aren't re-charged for work already done; for short runs the saving is
  negligible.)_
- **The log is auditable.** Every approval decision, tool call, and cost is an
  immutable row — replayable and inspectable long after the run.
- **Tenant-scoped at the database.** The `events`/`sessions` tables enforce
  PostgreSQL Row-Level Security: the application connects as a non-owner role
  with `FORCE ROW LEVEL SECURITY`, so a forgotten `WHERE` clause cannot leak
  another tenant's rows. (This demo runs single-tenant; the isolation is the
  same machinery.)

This is the difference from harnesses that keep session state in memory or in
local files: there is nothing to lose on a restart, and nothing to reconcile
by hand.
