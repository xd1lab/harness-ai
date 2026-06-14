# Boltrope examples

Runnable, self-contained walkthroughs. Each subdirectory has its own README
stating exactly which stack it needs and what it demonstrates.

Bring the keyless dev stack up first (no API key, no client tooling):

```bash
cp .env.example deploy/.env
docker compose -f deploy/docker-compose.yml up --build -d --wait
```

| Example | Shows | Needs |
| --- | --- | --- |
| [curl/](curl/) | Drive a session over REST/JSON + SSE with nothing but `curl` | keyless stub stack |
| [mcp-server/](mcp-server/) | Call Boltrope as an MCP server (initialize → tools/list → create_session + run; the call-stays-open approval loop) with nothing but `curl` | keyless stub stack |
| [durable-resume/](durable-resume/) | The durable event ledger: inspect the per-session log, and watch a session survive an orchestrator crash | keyless stub stack + `psql` |
| [python/](python/) | A ~100-line `requests`-only client with interactive approvals | keyless stub stack |
| [web-research/](web-research/) | Enable `webfetch`/`websearch` through the deny-by-default egress data path | a real model + an allowlisted host |

The first three run end-to-end against the **keyless stub provider** — a
deterministic, network-free model substitute — so you can see the harness
mechanics (sessions, the event stream, the durable ledger, crash recovery)
without any credentials. Point the stack at a real model to see actual agent
work; see [Configuring a provider](../README.md#configuring-a-provider).

> The stub provider replies with one fixed line and never calls a tool — it
> exercises the *harness*, not model intelligence. Examples that need real
> tool-calling (like `web-research/`) say so in their README.
