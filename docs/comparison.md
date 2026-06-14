<!-- SPDX-License-Identifier: Apache-2.0 -->

# How Boltrope compares

There are excellent agent harnesses already. This page helps you pick the right
one for your team — and is honest about the many cases where that isn't
Boltrope. It compares Boltrope with two popular open-source projects that also
describe themselves as agent harnesses:

- **[deepagents](https://github.com/langchain-ai/deepagents)** (LangChain) — a
  Python library on LangGraph. `pip install`, a few lines, and you have an
  agent. Large, active community and the LangChain ecosystem behind it.
- **[hive](https://github.com/aden-hive/hive)** (Aden, YC) — "AI employees" you
  run on the desktop, with a large catalog of tool integrations and a
  bounty-driven contributor community.

Both are more adopted, more featureful for getting started, and have larger
communities than Boltrope, which is a young, backend-only project. If you want
to be productive in five minutes, or you want a desktop/browser experience with
many pre-built integrations, start with one of them.

## Who each one is for

| If you are… | Best fit |
| --- | --- |
| Prototyping an agent in Python, fast, inside an existing LangChain app | **deepagents** |
| A non-developer or SMB wanting "AI employees" with many ready integrations | **hive** |
| A platform team that must **self-host**, prove **tenant isolation**, keep an **auditable record**, and run agents that take real actions safely | **Boltrope** |

Boltrope's bet is the third row: teams for whom *where the data lives*, *who can
see it*, and *what survives a restart* are requirements, not nice-to-haves. If
that is not you, one of the others is very likely the better tool today.

## Side by side

| | Boltrope | deepagents | hive |
| --- | --- | --- | --- |
| **Shape** | Backend microservices (Go) you deploy | Python library you import | Desktop app + integrations |
| **Maturity / community** | Young, one maintainer, no production users yet | Large, active, LangChain-backed | Large, bounty-driven |
| **Get-started speed** | `docker compose up` (a stack) | `pip install` + ~5 lines | Install the app |
| **Session state** | Append-only event log in **PostgreSQL** | Your responsibility (bring your own store) | Local JSON / SQLite |
| **Crash-resume** | Resumes from the durable log where it stopped; a durable ledger keeps actions **at-most-once**, so a crash-restart won't repeat a side effect (and won't re-charge completed work) | Not built in | Not a server model |
| **Multi-tenant isolation** | **DB-enforced** (PostgreSQL RLS, non-owner role, `FORCE ROW LEVEL SECURITY`) | Not provided | Not provided |
| **Client-edge auth** | OIDC/JWT, **fail-closed** (refuses to start without an issuer) | App's responsibility | n/a (desktop) |
| **Tool sandbox** | Per-session container, `--network none` by default, process-tree kill, at-most-once mutating tools | Trust-based by default | Runs with the user's access |
| **Egress** | Deny-by-default allowlist + a hardened fetch path (DNS-pinned, SSRF-checked) | Open unless you restrict it | Broad by design |
| **Release artifacts** | Signed images + SBOM + multi-arch (cosign, GHCR) | PyPI package | App releases |
| **Tool-call testing** | Deterministic CI suite | Deterministic CI suite | Deterministic CI suite |
| **Provider coupling** | Normalized `Provider` interface (Anthropic/OpenAI/Gemini/OpenAI-compatible) | LangChain model abstractions | Vendor-managed |

A note on the last few rows: all three projects have real, deterministic
test suites for the agent loop — Boltrope does **not** claim superior test
rigor here. And deepagents is fully self-hostable; only its optional managed
deploy path is tied to LangSmith. We mention these explicitly because it would
be easy (and wrong) to imply otherwise.

## Where the others are ahead

- **Adoption and ecosystem.** deepagents rides LangChain's community,
  documentation, and integrations. Boltrope has none of that gravity yet.
- **Time to first agent.** A Python `pip install` beats standing up a
  multi-service stack when you just want to experiment.
- **Breadth of pre-built tools/integrations.** hive ships a large catalog;
  Boltrope ships a small set of native tools plus an MCP client.
- **Front end.** Neither a UI nor a hosted product exists for Boltrope — it is
  a backend you integrate. The others give you something to *use*, not just
  deploy.

## What Boltrope deliberately doesn't do (yet)

- No GUI, no hosted SaaS — it is infrastructure you run.
- Containers-only sandboxing; microVM/gVisor backends are roadmap, so
  *mutually-untrusted* multi-tenant code execution is out of scope for v1.
- In-sandbox `bash` has no network at all; web access is limited to two
  allowlisted tool clients (see [ADR-0021](decisions/0021-egress-data-path.md)).
- A small native tool set rather than a large integration catalog.

The [roadmap](../README.md#roadmap--deferred) tracks these openly.

## The honest bottom line

For most people getting started today, deepagents or hive will get you further,
faster. Boltrope earns its keep only when self-hosting, DB-enforced tenant
isolation, an auditable record of every run, and crash-safe handling of
real-world actions are hard requirements — the kind of constraints platform and
security teams have, and that the alternatives leave to you. If that describes your situation, we'd
genuinely like to [hear from you](../README.md#community--support).
