# 6. Engineering & OSS conventions

Date: 2026-06-10
Status: Accepted

## Context

A high-quality, adoptable Go OSS project needs consistent, automated engineering
conventions from day one. These are settled directly from the research best-practices
checklist (all facts re-verified at Gate-1) so every service follows the same rules.

## Decision

- **Layout:** official [go.dev module layout](https://go.dev/doc/modules/layout) —
  `cmd/<app>/main.go` entrypoints, `internal/` for non-public code, root packages only
  if importable. **No `pkg/` / `api/` / `configs/` scaffolding** until a concrete need
  exists. Flat and feature-oriented first.
- **Architecture:** pragmatic hexagonal — pure **domain**, infra-agnostic
  **application** (use cases), **ports** + **adapters** at the edges; dependencies
  point inward; small interfaces defined in the **consumer** package. Add layers only
  where complexity earns them.
- **Logging:** stdlib `log/slog` with `JSONHandler` in prod; level from config;
  trace/span ids via `context.Context`; implement `LogValuer` on secret-bearing types
  for redaction.
- **Errors:** wrap with `fmt.Errorf("...: %w", err)`; inspect with `errors.Is`/
  `errors.As` (never `==`/type-assert across wraps); exported sentinels only for
  conditions callers branch on; typed errors only where a transport maps to a status.
- **Config:** typed struct via **`knadh/koanf`**; precedence **flags > env > file >
  defaults**; validate on startup and **fail fast**; secrets via env only.
- **Linting:** **golangci-lint v2** (`linters.default: standard` + curated `enable`:
  errcheck, govet, staticcheck, revive, gosec, misspell, gocritic, bodyclose);
  formatters gofumpt + goimports; fail CI on findings.
- **Testing:** table-driven unit tests with consumer-interface mocks; `httptest` for
  handlers; **`testcontainers-go`** for Postgres integration tests behind
  `//go:build integration`; always `go test -race` in CI; pragmatic **~75% coverage
  floor** (a signal, not a target).
- **CI/CD:** GitHub Actions (lint, unit matrix, integration, build) with **all
  third-party actions pinned to commit SHAs** and minimal `GITHUB_TOKEN` permissions.
- **Versioning/release:** Conventional Commits → **release-please** for the release PR
  + changelog → **GoReleaser** on tag (multi-arch static binaries, GHCR images, SBOM
  via syft, keyless cosign signing + SLSA provenance).
- **Community/supply-chain:** `SECURITY.md` with **private** vulnerability reporting,
  Contributor Covenant `CODE_OF_CONDUCT.md`, issue/PR templates, Dependabot, OpenSSF
  Scorecard.

## Consequences

- ✅ Uniform, automated quality bar; low-friction onboarding; strong supply-chain
  posture.
- ⚠️ Up-front CI/release wiring cost, paid once.
