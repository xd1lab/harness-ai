<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0020: Production client-edge auth — OIDC discovery + JWKS Keyfunc wiring

- **Status:** Accepted
- **Date:** 2026-06-11
- **Relates to:** FR-API-03 (edge bearer/OIDC auth), NFR-SEC-01 (fail-closed posture), ADR-0013 (identity), the 2026-06-11 competitive audit ("flagship gate unfinished: production auth carries a 'wired in a later ops task' note")

## Context

The edge-auth interceptor (`internal/orchestrator/adapter/inbound/grpc/auth.go`)
has been complete since v1: pinned-algorithm JWT validation (alg=none rejected
twice), required `exp`, `iss`/`aud` checks, tenant-claim extraction feeding the
RLS GUC, and a fail-closed constructor — production mode without a `Keyfunc`
refuses to start. What was missing was the **supply side**: nothing constructed
a `jwt.Keyfunc` from a real identity provider, so the production path could
only ever fail closed. The client CLI already sends `authorization: Bearer`
(`harnessctl --token` / `BOLTROPE_CTL_TOKEN`).

The 2026-06-11 competitive analysis ranked this the single highest-value
unfinished item: every verified differentiator (DB-enforced multi-tenancy,
fail-closed posture, durable audit log) is only purchasable by a customer who
can actually log in.

## Decision

1. **New dependency-light platform package `internal/platform/oidc`** that
   turns an issuer URL into a `jwt.Keyfunc`:
   - **Discovery:** `GET {issuer}/.well-known/openid-configuration`; the
     document's `issuer` MUST equal the configured issuer (OIDC Core issuer
     validation — defends against mix-up attacks); `jwks_uri` is taken from
     the document, never guessed.
   - **JWKS:** keys are fetched from `jwks_uri` and parsed from JWK (RSA
     `kty:"RSA"` only in v1 — RS256 is the universal IdP default; `use:"enc"`
     and non-RSA keys are skipped). Zero usable keys is a startup error.
   - **Rotation:** the Keyfunc resolves by `kid`; an unknown `kid` triggers an
     inline re-fetch, rate-limited by a minimum refresh interval (default 1
     minute, injected `clock.Clock` for deterministic tests) so a forged-kid
     flood cannot turn the IdP into a DoS amplifier. A token with no `kid` is
     accepted only when the set has exactly one signing key.
   - **Transport hygiene:** issuer/jwks URLs MUST be https (loopback excepted,
     for tests and sidecar IdPs); responses are size-capped (1 MiB) and
     time-limited (10 s default client).
2. **Startup is fail-closed end-to-end:** in production mode
   (`BOLTROPE_DEV_INSECURE` unset) the orchestrator builds the Keyfunc from
   `BOLTROPE_OIDC_ISSUER` BEFORE serving; a missing issuer, unreachable IdP,
   issuer mismatch, or empty key set is a refused start, not a silently open
   or silently closed edge. Audience pinning comes from
   `BOLTROPE_OIDC_AUDIENCE` (recommended; empty disables the `aud` check as
   before). Algorithms remain pinned to `RS256`.
3. **No new module dependencies.** Discovery + JWK parsing is ~200 lines of
   stdlib (`crypto/rsa`, `encoding/base64`, `math/big`, `net/http`) plus the
   already-present `golang-jwt/jwt/v5`. Mirrors the project's MCP-client and
   docker-CLI decisions (ADR-0005): a small, auditable implementation over a
   third-party dependency at a trust boundary.

## Alternatives considered

- **`github.com/MicahParks/keyfunc`** (JWKS library): solid, but adds a
  dependency at the most security-sensitive boundary for functionality that is
  small and fully testable in-house; background-goroutine refresh model also
  fits the daemon lifecycle less cleanly than refresh-on-miss.
- **`github.com/coreos/go-oidc`**: brings an ID-token verifier designed for
  relying-party login flows (nonce, code exchange) — far more surface than the
  resource-server JWT validation needed here, and it pins its own claim
  semantics rather than composing with the existing pinned-parser interceptor.
- **ES256/EdDSA support now:** deferred. Every mainstream IdP (Keycloak, Dex,
  Auth0, Okta, Entra ID) signs RS256 by default; adding curves is additive
  later (parse `kty:"EC"`, extend the pinned set via config) and not worth the
  test surface today.

## Consequences

- The production edge is now reachable end-to-end: IdP issues an RS256 access
  token carrying `tenant_id` (claim name configurable via the interceptor's
  `TenantClaim`), `harnessctl --token` presents it, the interceptor verifies
  it against live IdP keys, and the verified tenant scopes RLS.
- Deployments MUST register tenants: the `tenant_id` claim must be a UUID
  matching a `tenants` row (FK + RLS) — documented in deploy/README's OIDC
  walkthrough.
- The IdP must be reachable at orchestrator startup (deliberate fail-closed
  trade-off; a crash-looping orchestrator is observable, a silently
  unverifiable edge is not). Key ROTATION never blocks startup — it is
  refresh-on-miss at request time.
- Deferred, documented: ES256/EdDSA keys; `BOLTROPE_OIDC_ALGORITHMS` override;
  multi-issuer federation; JWKS pre-warm refresh on a timer (refresh-on-miss
  is sufficient for rotation because IdPs serve old+new keys overlapping).
