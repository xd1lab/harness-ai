# 1. Build and runtime toolchain

Date: 2026-06-10
Status: Accepted

## Context

The project must be a high-quality, portable open-source Go + PostgreSQL backend.
The development host (Windows 10) had **Docker 29.4.3** and **git** available, but
**Go, psql, make, protoc, and golangci-lint were not installed**. We need both fast
local TDD iteration *and* reproducible builds that match CI and contributors'
machines.

Options considered:

1. **Docker-only toolchain** — run every `go`/lint/test command inside a container.
   Maximally reproducible, but per-invocation container startup slows the
   tight red-green-refactor TDD loop.
2. **Local toolchain only** — fast, but not reproducible and pushes setup burden
   onto every contributor's host.
3. **Hybrid** — local Go for the inner dev loop, Docker for builds/CI/runtime and
   for any tool that is awkward to install on the host.

## Decision

We will use a **hybrid toolchain**:

- Install **Go 1.26.4** locally for the fast TDD inner loop (`go test`, `go vet`,
  `go build`).
- Provide **Docker + Docker Compose** for reproducible builds, CI, PostgreSQL, and
  running the services. CI uses the official `golang` image so build results are
  identical everywhere.
- Run auxiliary tools (`golangci-lint`, `protoc`, DB `migrate`) via Docker or Go
  *tool dependencies* (`go.mod` `tool` directives) rather than requiring host
  installs.
- If host permission/configuration problems block any step, fall back to running
  that step in a Docker container rather than fighting the host.

## Consequences

- ✅ Fast local feedback while keeping builds reproducible and contributor setup
  minimal (`docker compose up`).
- ✅ CI and local builds use the same Go image → no "works on my machine".
- ⚠️ Two code paths to keep working (local + Docker); mitigated by making Docker the
  source of truth for releases and CI.
- 📌 Operational note for this environment: spawned shells do not inherit the
  newly-persisted `PATH`, so local `go` invocations are prefixed with the Go env in
  scripts. A fresh terminal (post-restart) picks up Go from the persisted user PATH.
