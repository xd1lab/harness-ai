# Contributing to Boltrope

Thanks for your interest in improving Boltrope! By participating you agree to abide by
our [Code of Conduct](CODE_OF_CONDUCT.md).

## Developer Certificate of Origin (DCO)

We use the [DCO](https://developercertificate.org/) instead of a CLA. **Every commit
must be signed off:**

    git commit -s -m "feat: add the thing"

This adds a `Signed-off-by: Your Name <you@example.com>` trailer certifying you wrote
the patch (or have the right to submit it) under the project's Apache-2.0 license. PRs
with unsigned commits fail the DCO check.

## Commit messages — Conventional Commits

We follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).
The type drives semantic versioning:

- `fix:` → PATCH
- `feat:` → MINOR
- `feat!:` or a `BREAKING CHANGE:` footer → MAJOR
- supporting types: `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`

## Development workflow

1. Fork and branch from `main` (e.g. `feat/short-description`).
2. Follow the conventions in
   [docs/decisions/0006-engineering-conventions.md](docs/decisions/0006-engineering-conventions.md).
3. **Write tests first (TDD).** Keep `go test ./...` green and run `go test -race ./...`.
4. Lint (`golangci-lint run`) and format (`gofumpt`, `goimports`) before pushing.
5. Open a PR with a Conventional-Commit title and a completed checklist.

> Build & run instructions live in the [README](README.md) (added once the
> architecture is finalized).

## Reviews & CI

All PRs require green CI (lint, unit `-race`, integration, build) and at least one
maintainer approval. Third-party GitHub Actions are **pinned to commit SHAs** — keep
them pinned when updating.

## Reporting

Use the issue templates for bugs and features. For **security** issues, do not open a
public issue — see [SECURITY.md](SECURITY.md).
