<!-- SPDX-License-Identifier: Apache-2.0 -->

# Boltrope documentation

This directory holds the design and decision record for Boltrope — a
provider-portable, event-sourced AI agent harness in Go. Start with the
[root README](../README.md) for the what/why and a copy-paste quickstart, then
use this index to go deeper.

## Start here

| If you want to… | Read |
|---|---|
| Understand the system and run it | [../README.md](../README.md) |
| Get a one-page architecture orientation | [../ARCHITECTURE.md](../ARCHITECTURE.md) |
| Know exactly what v1 must do | [spec/00-system-specification.md](spec/00-system-specification.md) |
| Understand how it is built | [architecture/00-architecture.md](architecture/00-architecture.md) |
| Know *why* a choice was made | [decisions/](decisions/) (ADR index) |

## Specification

- **[spec/00-system-specification.md](spec/00-system-specification.md)** — the v1
  system specification (Gate 2, accepted): purpose & scope, personas & use cases,
  the 41 functional requirements + non-functional requirements with testable
  acceptance criteria, the [multi-LLM support matrix](spec/00-system-specification.md#6-multi-llm-support-matrix),
  external interfaces, data & state, and the v1 definition of done.

## Architecture

- **[architecture/00-architecture.md](architecture/00-architecture.md)** — the v1
  architecture (Gate 3, final): service decomposition, inter-service contracts,
  the PostgreSQL event-store schema, durability & exactly-once side effects, the
  security model, the concurrency/cancellation model, operability, and provider
  streaming across four families.
- **[architecture/01-impact-analysis.md](architecture/01-impact-analysis.md)** —
  impact analysis of the accepted architecture.
- **[architecture/02-implementation-plan.md](architecture/02-implementation-plan.md)**
  — the dependency-ordered, test-first (TDD) implementation plan: parallelization
  waves, per-task component/FR/tests-first notes, and the FR→task traceability map.

## Decisions (ADRs)

- **[decisions/](decisions/)** — lightweight, Nygard-style Architecture Decision
  Records, immutable once `Accepted`. See the [ADR index](decisions/README.md) for
  the full list (0001–0017), covering the toolchain, license, v1 scope, multi-LLM
  strategy, sandbox isolation, engineering/OSS conventions, eval strategy, the
  service decomposition, inter-service communication, the event-store schema,
  durability, the security model, concurrency/cancellation, the repository layout,
  the provider abstraction, and operability/observability.

## Research

- **[research/00-research-report.md](research/00-research-report.md)** — the
  upstream research report (feature taxonomy, OSS survey, multi-LLM landscape, and
  best-practices checklist) that informed the specification and decisions.

## Conventions

These documents are written and gated in stages (research → spec → architecture →
implementation plan → implementation). Each carries a `Status` and `Date` header.
The frozen contract surface — the `.proto` files under
[`../proto/boltrope/v1/`](../proto/boltrope/v1) with committed stubs in `../gen/`,
and the consumer-defined Go ports — is the boundary the implementation builds
behind; changing a contract requires escalation, not a silent edit.
