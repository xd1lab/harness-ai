# Call Boltrope as an MCP server (MCP Server mode)

Boltrope exposes **itself** as a Model Context Protocol (MCP) server, so any
compliant MCP client (Claude Desktop, Cursor, another agent) can delegate a whole
governed task to it: create a session, run an agent task, inspect state,
approve/deny a pending tool call, and fork. This is the "callee" position
(ADR-0022) — Boltrope as a sandboxed, tenant-isolated, auditable, durable,
replayable execution backend other agents call over the network.

The MCP endpoint is `POST /mcp` on the orchestrator's HTTP listener (the same one
as the REST facade and `/readyz`). It is a thin adapter over the *same*
`igrpc.Server` methods, so OIDC auth, multi-tenant RLS, the approval gate, the
per-tenant in-flight cap, durable resumable delivery, and at-most-once mutating
actions are all inherited — identical to the gRPC and REST edges.

## Run it

Against the keyless dev stack (`docker compose ... up -d --wait`):

```bash
./run.sh "Say hello."
```

`run.sh` is ~50 lines of POSIX shell. It performs the MCP handshake and a run:

1. `initialize` → `serverInfo` + `capabilities: { tools }`;
2. `tools/list` → the 5 tools `create_session`, `run`, `get_session`, `control`, `fork`;
3. `tools/call create_session` → a new `session_id`;
4. `tools/call run` (with a `_meta.progressToken`) → the reply streams back on a
   `text/event-stream` leg as `notifications/progress` frames, then the terminal
   `result` frame carrying the `CallToolResult`.

## The run + approval loop (the call stays open)

A `run` `tools/call` keeps its SSE leg **open** until the run terminates — exactly
like the REST `POST .../run`. When the agent hits a risky tool call, the server
emits an **in-band approval** `notifications/progress` frame carrying the
`call_id`:

```
event: approval_request
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{
        "progressToken":"p1","progress":3,"message":"approval_request",
        "call_id":"call-abc","tool_name":"bash","reason":"mutating tool requires approval",
        "args_json":"{...}","after_seq":7}}
```

You resolve it with a **concurrent** `tools/call control` on a **separate**
connection while the `run` call stays open:

```bash
curl -X POST "$BASE/mcp" -H 'Content-Type: application/json' -d '{
  "jsonrpc":"2.0","id":9,"method":"tools/call",
  "params":{"name":"control","arguments":{"session_id":"<sid>","action":"approve","call_id":"call-abc"}}}'
```

`control` resolves the in-process approval gate, the loop unblocks, and the rest of
the run streams on the **same** open `run` leg until `status:"completed"`. A run
needing N approvals is **one** `run` call interleaved with **N** `control` calls.

> **Why a concurrent `control` instead of "end the call, re-call run"?** Ending the
> `run` call cancels its request context, which aborts the loop and removes the
> pending gate entry — a later approve would then hit `FailedPrecondition`. Keeping
> the call open is the mechanism the REST facade already proves in production. See
> ADR-0022 and `DECISIONS.md` (the 2026-06-15 amendment).

## Two details that matter

- **Send a `progressToken` for `run`.** Without `_meta.progressToken` the response
  is a single `application/json` `CallToolResult` (no streaming, and — like REST's
  `approval_request` needing an open SSE — no in-band approval). For any run that
  may hit an approval, always send a `progressToken`.
- **`id:` is the durable seq.** Every SSE frame's `id:` is the event's `seq` in the
  Postgres log. If the leg drops mid-run, reconnect by re-calling `run` with the
  last `after_seq` you saw (pure resume) — no duplicated, no skipped frames.

## Production is the same call plus a token

Set `BOLTROPE_URL` and a `Bearer` token (an OIDC access token whose `tenant_id`
claim is a registered tenant); the MCP edge validates it exactly as the gRPC edge
does. See the
[OIDC walkthrough](../../deploy/README.md#client-edge-auth-in-production-oidc).
If a *browser-based* MCP client must reach `/mcp` directly, add its origin to
`BOLTROPE_MCP_ALLOWED_ORIGINS` (the DNS-rebinding guard); non-browser clients send
no `Origin` and are unaffected.
