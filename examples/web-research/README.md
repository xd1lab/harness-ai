# Web access through the egress data path

By default Boltrope's sandbox runs with **no network** (`--network none`), so
`webfetch` and `websearch` return `egress denied` until you deliberately
allowlist a host. This example turns them on — safely — through the egress data
path ([ADR-0021](../../docs/decisions/0021-egress-data-path.md)).

> **What this example needs:** a **real model** (the stub provider never calls
> tools, so it can't exercise `webfetch`/`websearch`) and an **allowlisted
> host**. Unlike the other examples it is not a one-command run against the
> keyless stack — it is a configuration walkthrough you apply, then drive with
> a normal task.

## How egress works here

The fetch does **not** happen in the sandbox. It happens in the tool-runtime
process — the trust boundary — through a hardened HTTP client that:

- consults the deny-by-default allowlist for the initial host **and every
  redirect hop** (a redirect to a non-allowlisted host is refused, not
  followed);
- resolves DNS itself and dials the vetted IP literally (no DNS-rebinding);
- refuses non-public destination addresses by default — loopback, private,
  link-local, and the cloud-metadata address — even for an allowlisted host
  (SSRF defense);
- caps body size, redirects, and time.

The sandbox stays `--network none`, so in-sandbox `bash` still has no network.
Only these two tool clients reach out, and only to hosts you list.

## Enable it (compose)

Add to `deploy/.env`, then recreate the tool-runtime:

```bash
# Hosts webfetch/websearch may reach ("*.suffix" wildcards allowed):
BOLTROPE_TOOLRT_EGRESS_ALLOWLIST=en.wikipedia.org,*.githubusercontent.com

# websearch backend: any SearXNG-compatible JSON endpoint (allowlist its host too):
BOLTROPE_TOOLRT_SEARCH_URL=https://searx.example.org/search

# Point the stack at a real model (example: a local Ollama):
BOLTROPE_MODELGW_PROVIDER=openaicompat
BOLTROPE_DEFAULT_MODEL=your-tool-calling-model
```

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
```

Then drive a research task (via any client — see [curl/](../curl/) or
[python/](../python/)):

```bash
./run.sh "Summarize https://en.wikipedia.org/wiki/Raft_(algorithm)"
```

The model calls `webfetch`; the tool-runtime fetches `en.wikipedia.org`
(allowlisted) and returns the page; a fetch of any other host comes back
`egress denied`.

## Self-hosting the search backend

If your SearXNG runs on the same private network as the stack, its host will
resolve to a private address. Allow that explicitly (it is off by default):

```bash
BOLTROPE_TOOLRT_EGRESS_ALLOW_PRIVATE=1
```

In the Helm chart the same three knobs are
`toolRuntime.egress.{allowlist,searchURL,allowPrivate}`.

## The honest boundary

This re-enables web access for the **tool-runtime's own outbound tools**. It
does **not** give the sandbox a network — in-sandbox `bash`/`curl` remain
severed. A forward proxy that would give the sandbox namespace a
per-connection-gated path is still [roadmap](../../README.md#roadmap--deferred).
