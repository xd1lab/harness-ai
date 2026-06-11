# Python client — no SDK, just `requests`

[`run_task.py`](run_task.py) is a ~100-line client that drives a full session
over the REST/SSE facade: create → run (streaming) → interactive approvals →
result. There is no Boltrope Python SDK to install; the facade is plain HTTP +
Server-Sent Events.

## Run it

```bash
pip install requests
python run_task.py "Write a hello-world Go program."
```

Against the keyless dev stack it streams the stub provider's reply. Point the
stack at a real model (see [Configuring a provider](../../README.md#configuring-a-provider))
to drive real tool use — then the approval prompt actually fires:

```
[approval required] tool=write call_id=abc123 — allow? [y/N]
```

Answering `y`/`N` issues a `POST /v1/sessions/{id}/control` with
`approve`/`deny`. The decision is persisted as an event, so it is part of the
auditable, replayable history — not just a transient UI choice.

## Production

```bash
export BOLTROPE_URL=https://boltrope-api.example.com
export BOLTROPE_TOKEN=<OIDC access token; tenant_id claim = a registered tenant>
python run_task.py "..."
```

The same script, unchanged — the only difference is the base URL and the Bearer
token the facade validates (identical checks to the gRPC edge). The token
contract is in the [OIDC walkthrough](../../deploy/README.md#client-edge-auth-in-production-oidc).
