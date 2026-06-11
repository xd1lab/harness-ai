<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0021: Egress data path — in-process hardened fetcher for webfetch/websearch

- **Status:** Accepted
- **Date:** 2026-06-11
- **Relates to:** FR-TOOL-06 (per-session deny-by-default egress), NFR-SEC-04 (no unrestricted egress), ADR-0003 (v1 scope — egress-proxy data path deferred), ADR-0013 (security model — egress broker is the real exfiltration control), the 2026-06-11 competitive audit ("research-type tasks fail entirely — web tools are dead")

## Context

v1 shipped the egress **policy** layer (the per-session deny-by-default broker)
but no **data path** behind it. `webfetch` shelled out to `curl` *inside* the
sandbox, which runs `--network none` — so even an allowlisted host was
unreachable, and `websearch` had no backend at all. The honest amendments to
NFR-SEC-04 / FR-TOOL-06 recorded this: the broker decisions were enforced, but
the only thing they gated was a path that was already severed. Net effect: any
task needing to read a web page or search failed.

The load-bearing invariant must hold — *no unrestricted egress from any
model-influenced path* — so we cannot simply give the sandbox a network. The
in-sandbox `bash` must stay `--network none`.

## Decision

**Add an egress DATA PATH as a hardened in-process HTTP client in the
tool-runtime** (`internal/toolruntime/adapter/outbound/egressclient`,
implementing a new `app.WebFetcher` port), and rewire `webfetch`/`websearch`
onto it. The fetch happens in the toolruntimed process — the trust boundary
that already holds the broker — not in the sandbox. The sandbox stays
`--network none`; in-sandbox egress remains *severed, not proxied* (the
sandbox-namespace forward proxy is still deferred — ADR-0003).

The client is hardened because it is now a deliberate, model-reachable egress
path:

1. **Per-request broker mediation.** The `EgressBroker` is consulted with the
   bare hostname for the initial request **and for every redirect hop** before
   it is followed — a 302 to a non-allowlisted host is refused, not chased.
2. **DNS-pinned dialing (anti-rebinding).** The dialer resolves the hostname
   itself, vets each resulting IP, and dials the vetted IP literally, so the
   address that passed the check *is* the address connected to — closing the
   resolve-check/dial-resolve TOCTOU.
3. **Public-address-only by default (SSRF).** Loopback, RFC1918/ULA,
   link-local (incl. the `169.254.169.254` cloud-metadata address), multicast
   and unspecified destinations are refused even for an allowlisted host.
   `AllowPrivate` (`BOLTROPE_TOOLRT_EGRESS_ALLOW_PRIVATE`) opts in for
   deployments whose targets live on a private network (e.g. an in-cluster
   SearXNG).
4. **Bounded.** `http`/`https` schemes only; no proxy-from-environment (an
   env proxy would be an unaudited path around the host decisions); capped
   body size, redirect count, and wall-clock timeout.

**websearch** queries a configured **SearXNG-compatible JSON endpoint**
(`BOLTROPE_TOOLRT_SEARCH_URL`, `?q=&format=json`) through the same fetcher and
renders the top results. Its backend host is not in its arguments, so the tool
implements a new optional `app.EgressTargeter` interface that the execute
service's egress gate consults to adjudicate the real destination (the gate
peels the registry's validation decorator via `Unwrap` to find it); the data
path independently re-gates the actual fetch.

### Defense in depth — two enforcement points, unchanged invariant

The broker is now consulted at **two** layers for an external tool: the
execute service's `egressGate` (before execution) and the fetcher (per request
+ per redirect). Both deny-by-default and fail closed. The service gate stops a
denied call from running at all; the fetcher additionally re-gates redirects
the gate never saw.

## Alternatives considered

- **Sandbox-namespace forward proxy** (the originally-sketched data path):
  give the sandbox a network interface pointed at a filtering proxy. Heavier
  (per-session network plumbing, a proxy process, TLS interception or SNI
  filtering) and still deferred under ADR-0003. The in-process fetcher
  delivers the user-visible capability (web reachable on the allowlist) now,
  without weakening the `--network none` sandbox; the proxy remains the path to
  re-enabling in-sandbox `bash` egress later.
- **Keep curl-in-sandbox, give the sandbox a bridge network.** Rejected: it
  reintroduces an unrestricted egress path from model-controlled `bash`, the
  exact thing NFR-SEC-04 forbids.
- **A maintained search SDK / a specific search vendor.** Rejected for v1:
  SearXNG's JSON API is an open, self-hostable, vendor-neutral contract that
  keeps the search backend an operator choice and the dependency surface a
  plain HTTP GET.

## Consequences

- `webfetch`/`websearch` work when their target host is allowlisted — the
  research-task gap is closed without touching the sandbox containment.
- The fetcher is a new attack surface, mitigated by the hardening above; the
  unit tests cover denial-without-dial, redirect re-gating, the redirect cap,
  body truncation, scheme denial, the SSRF/private-address refusal, and
  hostname-only broker consults.
- Deployments opt in explicitly: empty allowlist ⇒ deny-all (unchanged safe
  default); `BOLTROPE_TOOLRT_SEARCH_URL` unset ⇒ websearch denies. Nothing
  becomes reachable without operator configuration.
- The frozen FR-TOOL-06 / NFR-SEC-04 acceptance text and their honesty
  amendments still hold verbatim: the invariant is unchanged; this ADR fills in
  the data path the amendments described as deferred.
