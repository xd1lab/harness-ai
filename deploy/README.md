# Boltrope — local stack (`docker compose`)

This directory contains the **dev / single-tenant** deployment of the full
Boltrope stack: PostgreSQL, the migration gate, and the four services
(`orchestratord`, `modelgwd`, `toolruntimed`, `projectord`). It is the
`docker compose up` path from the spec's quickstart (UC-OPS-01, DOD-05).

> **This stack is for local development and trusted-code / single-tenant use.**
> It runs with `BOLTROPE_DEV_INSECURE=1` (static-cert mTLS instead of SPIRE) and
> bind-mounts the host docker socket into the tool-runtime. Read the
> [Security caveats](#security-caveats) before exposing it to anything you care
> about. Production posture is summarized at the end.

---

## Quickstart

From the **repository root** (not this directory):

```sh
# 1. (optional) Configure. The compose file lives in deploy/, so docker compose
#    reads deploy/.env. You do NOT need a .env for the keyless default: every
#    value has an inline default and the model-gateway defaults to the built-in
#    `stub` provider (deterministic, no API key, no model backend), so the stack
#    runs a task out of the box. Copy the template only when you want to point at
#    a hosted provider or a local Ollama/vLLM (see Configuration below):
#       cp .env.example deploy/.env   # note: the TEMPLATE defaults to openaicompat
#                                     # (needs a host /v1 endpoint), not stub.

# 2. (optional) pre-pull the sandbox base image for a fast first tool call.
docker pull debian:stable-slim

# 3. Bring up the stack: Postgres → migrate (gate) → grant → 4 services.
#    --wait blocks until every service's /readyz is green (or a timeout).
make run-compose
#   …equivalently, without make:
#   docker compose -f deploy/docker-compose.yml up --build --wait

# 4. Watch it run.
make compose-logs        # docker compose -f deploy/docker-compose.yml logs -f

# 5. Tear it down (keeps the Postgres + blob volumes).
make down                # docker compose -f deploy/docker-compose.yml down
#   destructive variant that also deletes the volumes:
#   make down-volumes     # docker compose -f deploy/docker-compose.yml down --volumes
```

> `docker compose` automatically loads `deploy/.env` because its project
> directory defaults to the compose file's directory. There is no need to pass
> `--env-file`. The repo-root `.env.example` is the canonical, git-tracked
> template; `deploy/.env` is your git-ignored working copy.

The orchestrator's HTTP endpoints are published on the host:

```sh
curl -fsS http://localhost:8080/livez    # 200 "ok"    — process is up
curl -fsS http://localhost:8080/readyz   # 200 "ready" — DB reachable + SVID present
curl -fsS http://localhost:8080/metrics  # Prometheus RED/USE metrics
```

The gRPC edge is published on `localhost:9000`.

### Driving a session with `harnessctl`

`harnessctl` is the thin gRPC client (`cmd/harnessctl`). Build it locally with
the repo's Go toolchain and point it at the published edge:

```sh
go build -o harnessctl ./cmd/harnessctl
# In dev-insecure mode the orchestrator injects a fixed dev principal whose tenant
# is a fixed UUID, so --tenant is OPTIONAL: omit it and the call is scoped to that
# authenticated tenant. (If you DO pass --tenant it must equal the dev tenant UUID
# 0de0c0de-0000-4000-8000-000000000000, else the server returns PermissionDenied.)
# BOLTROPE_DEV_INSECURE=1 selects the shared-seed dev mTLS dial path that matches
# the compose edge (see the mTLS note below).
BOLTROPE_DEV_INSECURE=1 ./harnessctl --endpoint localhost:9000 run "hello world"
```

> **mTLS for the dev edge.** Under `BOLTROPE_DEV_INSECURE=1` the orchestrator's
> gRPC edge speaks **static-cert mTLS** (it has no plaintext listener). Set the
> same `BOLTROPE_DEV_INSECURE=1` on `harnessctl` and it dials over the
> **shared-seed dev CA** (`grpcx.StaticDevClientCredentials`): it presents the
> `spiffe://boltrope.local/edge` identity the orchestrator's RBAC admits and pins
> `spiffe://boltrope.local/orchestrator`, so the handshake completes against the
> compose edge. Override the trust domain / pinned callee with `--trust-domain` /
> `--server-id`, and keep `BOLTROPE_DEV_CA_SEED` consistent across the stack and
> the client (both default to the same well-known seed when unset). The bare
> `--insecure` flag is plaintext-only — use it for a local orchestrator started
> without mTLS, not the compose dev edge.

---

## What comes up, and in what order

```
postgres            (healthcheck: pg_isready)
  └─ boltrope-migrate    one-shot: runs embedded DDL as the OWNER, exits 0   [gate]
       └─ boltrope-grant one-shot: ALTER ROLE boltrope_app … LOGIN           [dev glue]
            ├─ modelgwd        /readyz
            ├─ toolruntimed    /readyz  (also probes `docker version`)
            ├─ projectord      /readyz
            └─ orchestratord   /readyz  (starts after modelgwd + toolruntimed are healthy)
```

Ordering is enforced with `depends_on` + `healthcheck` (NFR-OPS-01, NFR-OPS-02):

- **Postgres healthy → migrate.** `boltrope-migrate` only starts once
  `pg_isready` passes, so the DDL never runs against a still-initializing server.
  It applies the embedded, forward-only migrations and exits `0` — the
  release gate. Services `depends_on` it `completed_successfully`, so none ever
  serves against an unmigrated schema.
- **migrate → grant → services.** `migrations/0003_rls_policies.up.sql` creates
  the non-owner `boltrope_app` role `NOBYPASSRLS` **and `NOLOGIN`** on purpose;
  the login credential is provisioned out-of-band. The `boltrope-grant` one-shot
  does that in dev (`ALTER ROLE … WITH LOGIN PASSWORD`) so the services connect
  as the RLS-bound role and Row-Level Security is genuinely enforced — rather
  than connecting as the superuser, which would bypass RLS entirely.
- **Readiness gates.** Each service's `/readyz` gates on real dependency
  reachability (a Postgres ping, SVID presence, and — for the tool-runtime —
  `docker version`). `make run-compose` uses `--wait`, so the command only
  returns once every service's `/readyz` is green — i.e. the stack is wired up,
  its dependencies are reachable, and it is accepting work (FR-OBS-05). It does
  not, by itself, exercise a model round-trip; that is what the keyless `stub`
  provider plus the CI smoke task add on top.

### Database roles

| Role | Who uses it | RLS |
|---|---|---|
| `boltrope_owner` (superuser) | `boltrope-migrate`, `boltrope-grant` | bypasses RLS (owner) |
| `boltrope_app` (`NOBYPASSRLS`) | all four services | **enforced** (`FORCE ROW LEVEL SECURITY`) |

The owner DSN and the app DSN are assembled by the compose file from the
`BOLTROPE_DB_*` values in `.env`. Point the stack at an **external** Postgres by
setting `BOLTROPE_POSTGRES__DSN` (services) and the migrate `--postgres-dsn`.

---

## Configuration

Everything is driven from `deploy/.env` (copied from the repo-root `.env.example`
template, which documents every variable). With no `.env` at all, the inline
compose defaults apply. The most common edits:

- **Keyless default (`stub`).** Out of the box the compose file sets
  `BOLTROPE_MODELGW_PROVIDER=stub` — a built-in deterministic, network-free
  provider that needs no API key and no model backend, so the stack runs a task
  immediately. It is for local demo / CI smoke only, never production.
- **Talk to a hosted provider.** Set `BOLTROPE_MODELGW_PROVIDER` to
  `anthropic` | `gemini` | `openai`, put the key in the matching
  `ANTHROPIC_API_KEY` / `GEMINI_API_KEY` / `OPENAI_API_KEY`, and set
  `BOLTROPE_MODELGW_API_KEY_ENV` to that variable's **name**. Provider keys live
  only in the model-gateway's environment (NFR-SEC-05).
- **Use a local OpenAI-compatible model.** Set `BOLTROPE_MODELGW_PROVIDER=openaicompat`
  and `BOLTROPE_MODELGW_OPENAI_BASE_URL` (default points at an Ollama/vLLM on the
  host via `host.docker.internal`). This is the value the `.env.example` template
  ships with, so copying the template switches the stack from `stub` to
  `openaicompat` — which then expects a reachable `/v1` endpoint.
- **Shared dev-CA seed.** All services share one `BOLTROPE_DEV_CA_SEED` (anchored
  in the compose file) so their static-cert dev mTLS identities derive from the
  same CA and trust each other across containers. Override per-stack in `.env`;
  never use the dev seed in production.
- **Ports.** `BOLTROPE_ORCH_GRPC_PORT` / `BOLTROPE_ORCH_HTTP_PORT` if `9000` /
  `8080` clash locally.

Validate the rendered config any time (no `.env` required — defaults are inline):

```sh
make compose-config
# docker compose -f deploy/docker-compose.yml config
```

---

## Security caveats

This compose stack deliberately trades isolation for local ergonomics. Three
things make it **unsafe for multi-tenant or untrusted-code or production** use:

1. **`docker.sock` is mounted into the tool-runtime (docker-out-of-docker).**
   The tool-runtime shells out to the `docker` CLI to create per-session sandbox
   containers (ADR-0005). In compose it does that against the **host** docker
   daemon via the bind-mounted `/var/run/docker.sock`. Anything that can talk to
   that socket can start a privileged container and **own the host** — it is
   root-equivalent. The sandbox containers are therefore *siblings* of the
   services, sharing the host kernel. This is acceptable only for single-tenant /
   trusted-code dev. Productionize with a rootless daemon, a socket proxy that
   restricts the API surface, or — the planned path — a microVM / gVisor backend
   behind the same `RuntimePort`.

2. **`BOLTROPE_DEV_INSECURE=1` (static-cert mTLS, not SPIRE).** Inter-service and
   edge mTLS use ephemeral self-signed certs minted at startup instead of
   SPIRE-issued SVIDs (ADR-0013, NFR-SEC-01). The services log a loud warning on
   start. This path is present in **every** build but env-gated: it never
   engages unless `BOLTROPE_DEV_INSECURE=1` is explicitly set, and it MUST NOT
   be used in production. Without that variable, a process that has no SPIFFE
   source refuses to start; release images build with `-tags spire` so the SPIRE
   Workload API path is available.

3. **Throwaway DB credentials in `.env.example`.** `boltrope_owner_pw` /
   `boltrope_app_pw` are placeholders for a local database. Never reuse them, and
   never commit a populated `.env` (it is git-ignored).

Even in dev, two controls remain active by design: **tenant isolation via RLS**
(the app role cannot cross tenants) and **deny-by-default egress** from the
sandbox.

---

## Known limitations

- **No bundled OTLP collector.** `BOLTROPE_OTLP__ENDPOINT` defaults to `otel:4317`
  but the stack does not start a collector. Point it at your own; an unreachable
  collector only drops spans/metrics and never blocks a turn.
- **No bundled real model backend.** The keyless default is the built-in `stub`
  provider, which runs with no backend. For real output, either point
  `openaicompat` at an OpenAI-compatible endpoint (Ollama/vLLM) on the host, or
  set a hosted provider (`anthropic`/`gemini`/`openai`) + key in `.env`.
- **`-race` images.** The release/compose images are `CGO_ENABLED=0` static
  binaries, so the race detector (which needs cgo) is not available *in the
  images*; run `go test -race` on the host instead.

---

## Production deployment (summary)

This compose file is **not** the production deployment. For production:

- Run **SPIRE** so each workload gets an auto-rotating X.509 SVID; build the
  services with the `spire` build tag and **do not** set `BOLTROPE_DEV_INSECURE`.
  Readiness gates on SVID presence, so a workload with no identity is never ready.
- Configure **client-edge OIDC auth** — see the walkthrough below. Without
  `BOLTROPE_OIDC_ISSUER` a production orchestrator refuses to start (fail-closed).
- Run migrations as a **Kubernetes init container / Job** (the same
  `boltrope-migrate` binary) gating the rollout, mirroring the compose gate.
- Provision the `boltrope_app` login credential through your secrets manager;
  give the services the non-owner DSN and keep the owner DSN for migrations only.
- Replace the docker-socket sandbox with a hardened backend (rootless, socket
  proxy, or microVM) per ADR-0005.
- Publish multi-arch images + SBOM + signatures via the release pipeline
  (GoReleaser; NFR-PORT-04).

## Client-edge auth in production (OIDC)

In production (no `BOLTROPE_DEV_INSECURE`) the orchestrator validates every
client call against your identity provider (FR-API-03; ADR-0020). Wiring is two
environment variables on **orchestratord**:

```bash
BOLTROPE_OIDC_ISSUER=https://idp.example.com/realms/boltrope   # required
BOLTROPE_OIDC_AUDIENCE=boltrope                                # recommended
```

**What happens at startup (fail-closed):** the orchestrator performs OIDC
discovery (`<issuer>/.well-known/openid-configuration`), verifies the document
asserts the same issuer, and fetches the JWKS it points at. An unreachable IdP,
an issuer mismatch, or a key set with no usable RS256 signing keys refuses
startup — there is no silently open (or silently unverifiable) edge. **Key
rotation needs no restart**: a token signed by a key published after startup
triggers an inline JWKS re-fetch (rate-limited to once a minute).

**The token contract.** Boltrope is a resource server: any IdP that can issue
an **RS256** JWT access token works (Keycloak, Dex, Auth0, Okta, Entra ID —
RS256 is the default everywhere). The token MUST carry:

| Claim | Requirement |
| --- | --- |
| `iss` | equals `BOLTROPE_OIDC_ISSUER` |
| `aud` | contains `BOLTROPE_OIDC_AUDIENCE` (when set) |
| `exp` | required; expired tokens are rejected |
| `tenant_id` | **a UUID matching a row in the `tenants` table** — this is the RLS scoping tenant; every session/event the caller touches is bound to it at the database layer |
| `sub` | the principal identity, recorded for audit |

`alg=none` and any non-RS256 algorithm are rejected before the key is even
consulted (pinned parser + a second explicit check).

**Register the tenant** (one-time, as the owner role — same pattern as the dev
`boltrope-grant` one-shot):

```sql
INSERT INTO tenants (id, name)
VALUES ('8a3d2f1e-9c4b-4f6a-b1d2-3e4f5a6b7c8d', 'acme-corp')
ON CONFLICT (id) DO NOTHING;
```

**Example: Keycloak.** Create a realm (its URL is your issuer), a client
(e.g. `boltrope`, with the audience mapped), and a **user-attribute mapper**
that emits the user's `tenant_id` attribute as a token claim named `tenant_id`.
Dex: use the static client + a middleware/connector that injects the claim.
The claim NAME is configurable server-side via the interceptor's `TenantClaim`
(defaults to `tenant_id`).

**Call the edge with a token:**

```bash
TOKEN=$(curl -s -X POST "$ISSUER/protocol/openid-connect/token" \
  -d grant_type=client_credentials -d client_id=boltrope -d client_secret=$SECRET \
  | jq -r .access_token)

harnessctl --endpoint orchestrator.example.com:9000 --token "$TOKEN" \
    run "Summarize the open incidents."
# (or BOLTROPE_CTL_TOKEN=$TOKEN; the token rides as `authorization: Bearer` metadata)
```

A rejected token returns `UNAUTHENTICATED` with a deliberately coarse message
(no oracle for why a token failed). A valid token scopes every query to the
token's tenant via PostgreSQL row-level security — there is no code path that
queries across tenants.
