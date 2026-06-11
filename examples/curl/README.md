# Drive Boltrope with `curl`

No SDK, no client binary — just the REST/JSON + SSE facade on the
orchestrator's HTTP listener. The facade calls the *same* gRPC server methods
(same auth, same ownership checks, same event stream), so anything you can do
with `harnessctl` you can do with `curl`.

## Run it

Against the keyless dev stack (`docker compose ... up -d --wait`):

```bash
./run.sh "Say hello."
```

`run.sh` is ~20 lines of POSIX shell. It:

1. `POST /v1/sessions` → a new session id;
2. `POST /v1/sessions/{id}/run` → streams the reply as Server-Sent Events.

## What you see

```
session: 019eb57c-dba7-7b4d-878d-be04ab6be3f1

id: 4
event: text_delta
data: {"seq":"4", "textDelta":{"text":"I received your task and I am working on it."}}

id: 5
event: result
data: {"seq":"5", "result":{"subtype":"TERMINATION_SUBTYPE_SUCCESS", "finalText":"...", "numTurns":"1"}}
```

(That text is the deterministic stub provider — point the stack at a real model
for real output.)

## The two details that matter

- **`id:` is the durable sequence number.** Every SSE frame's `id:` is the
  event's `seq` in the Postgres log. If the stream drops, resume *exactly*
  where you left off with the standard `Last-Event-ID` header — no duplicated
  and no skipped frames:

  ```bash
  curl -N -X POST "$BASE/v1/sessions/$SID/run" \
    -H 'Last-Event-ID: 4' -d '{"text":""}'
  ```

- **Production is the same call plus a token.** Set `BOLTROPE_URL` and a
  `Bearer` token (an OIDC access token whose `tenant_id` claim is a registered
  tenant); the facade validates it exactly as the gRPC edge does. See the
  [OIDC walkthrough](../../deploy/README.md#client-edge-auth-in-production-oidc).

## Approvals, control, fork

The other routes are just as plain:

```bash
# approve / deny / interrupt a pending tool call:
curl -X POST "$BASE/v1/sessions/$SID/control" -d '{"action":"approve","call_id":"<id>"}'
# branch a session at a point in its history:
curl -X POST "$BASE/v1/sessions/$SID/fork" -d '{"at_seq":4}'
# read the current projection:
curl "$BASE/v1/sessions/$SID"
```

The [python/](../python/) example wires the approval loop to an interactive
prompt.
