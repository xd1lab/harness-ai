# syntax=docker/dockerfile:1
#
# Boltrope — multi-stage, multi-target image build.
# ============================================================================
# ONE parameterized Dockerfile produces all six Boltrope images via build
# targets. A single shared `builder` stage compiles every static binary once
# (so the Go build cache is reused across targets); each final stage then copies
# just the one binary it needs onto a minimal runtime base. Build a specific
# image with `--target <name>`:
#
#     docker build --target orchestratord -t boltrope/orchestratord .
#     docker build --target toolruntimed  -t boltrope/toolruntimed  .
#     docker build --target migrate        -t boltrope/migrate        .
#
# `deploy/docker-compose.yml` builds each service from this file by setting the
# `target` of its build block, so a plain `docker compose up --build` produces
# every image from this single source.
#
# Design choices
# ----------------------------------------------------------------------------
#   * golang:1.26 builder, CGO disabled → fully static binaries (no libc at
#     runtime), so the runtime base can be distroless/static (NFR-PORT-01,
#     NFR-PORT-04). -race is therefore unavailable in these images (it needs
#     cgo); that is a CI concern, not a release-image one.
#   * The three long-lived service images + the migrate one-shot run on
#     gcr.io/distroless/static-debian12:nonroot — no shell, no package manager,
#     a non-root user, and a tiny attack surface.
#   * The tool-runtime image is the ONE exception: it shells out to the `docker`
#     CLI to launch per-session sandbox containers (see cmd/boltrope-toolruntimed
#     and internal/toolruntime/adapter/outbound/runtime), so its base must carry
#     the docker client. It uses alpine + the `docker-cli` package. This is
#     "docker-out-of-docker": the container talks to the HOST docker daemon via a
#     mounted socket — NOT a nested daemon. See deploy/README.md for the security
#     caveat (a mounted docker.sock is root-equivalent on the host).
#
# Build args
# ----------------------------------------------------------------------------
#   GO_VERSION       builder Go image tag (default 1.26).
#   ALPINE_VERSION   tool-runtime runtime base tag.
#   DISTROLESS_TAG   distroless runtime base tag.
#   VERSION          version string stamped into binaries via -ldflags (the
#                    `version` var in each cmd/*/vars.go); defaults to "dev".

ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.21
ARG DISTROLESS_TAG=nonroot

# ── builder ────────────────────────────────────────────────────────────────
# Compiles all six static binaries once. Module downloads and the build cache
# are mounted as BuildKit caches so repeat builds are fast.
FROM golang:${GO_VERSION} AS builder

WORKDIR /src

# Download modules first (cached unless go.mod/go.sum change). NEVER `go get` or
# `go mod tidy` here — the committed go.mod/go.sum are authoritative.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Bring in the rest of the source.
COPY . .

ARG VERSION=dev
# The version var lives in each cmd's package main (vars.go: `var version`).
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# -s -w strips the symbol table/DWARF (smaller binaries); -X stamps the version.
# -tags spire wires the SPIRE Workload API source into the four daemons (the
# untagged build carries a nil stub; see cmd/*/spiffe_spire.go and the ADR-0013
# amendment). The dev static-cert fallback remains present but env-gated behind
# BOLTROPE_DEV_INSECURE=1. migrate/harnessctl have no SPIFFE wiring and stay
# untagged.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    for svc in orchestratord modelgwd toolruntimed projectord; do \
        go build -tags spire -ldflags="-s -w -X main.version=${VERSION}" \
            -o "/out/boltrope-${svc}" "./cmd/boltrope-${svc}"; \
    done; \
    go build -ldflags="-s -w" -o /out/boltrope-migrate ./cmd/boltrope-migrate; \
    go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/harnessctl ./cmd/harnessctl

# ── busybox source (static, for healthchecks only) ──────────────────────────
# distroless has no shell or HTTP client, so a compose `healthcheck` cannot probe
# /readyz on its own. We copy a single STATIC busybox multi-call binary into the
# distroless images purely to provide `wget` for the healthcheck (and nothing
# runs it except the healthcheck). busybox:uclibc ships a fully static
# /bin/busybox. This keeps the runtime minimal while making /livez+/readyz
# container healthchecks real (NFR-OPS-02, FR-OBS-05).
FROM busybox:uclibc AS busybox-src

# ── shared distroless runtime ────────────────────────────────────────────────
# Base for the pure-Go services + the migrate one-shot. distroless/static has no
# shell and runs as an unprivileged user (the :nonroot tag). It carries CA
# certificates (needed for outbound TLS to OTLP/hosted providers, though the
# gateway image is the only one that dials a provider).
FROM gcr.io/distroless/static-debian12:${DISTROLESS_TAG} AS distroless-runtime
# Static busybox under /busybox, used ONLY by the compose healthcheck (e.g.
# `/busybox/busybox wget -q -O- http://127.0.0.1:8080/readyz`). It does not
# change the entrypoint or add a shell to the service's runtime path.
COPY --from=busybox-src /bin/busybox /busybox/busybox

# ── orchestratord ────────────────────────────────────────────────────────────
FROM distroless-runtime AS orchestratord
COPY --from=builder /out/boltrope-orchestratord /usr/local/bin/boltrope-orchestratord
# gRPC (9000) + HTTP health/readiness/metrics + REST facade (8080) by convention;
# the actual addresses come from config (BOLTROPE_SERVER__GRPC_ADDR / __HTTP_ADDR).
EXPOSE 9000 8080
ENTRYPOINT ["/usr/local/bin/boltrope-orchestratord"]

# ── modelgwd ─────────────────────────────────────────────────────────────────
FROM distroless-runtime AS modelgwd
COPY --from=builder /out/boltrope-modelgwd /usr/local/bin/boltrope-modelgwd
EXPOSE 9000 8080
ENTRYPOINT ["/usr/local/bin/boltrope-modelgwd"]

# ── projectord ───────────────────────────────────────────────────────────────
FROM distroless-runtime AS projectord
COPY --from=builder /out/boltrope-projectord /usr/local/bin/boltrope-projectord
EXPOSE 9000 8080
ENTRYPOINT ["/usr/local/bin/boltrope-projectord"]

# ── migrate (one-shot gate) ──────────────────────────────────────────────────
# Runs the embedded DDL and exits 0; the compose ordering gate depends on this
# completing successfully before any service starts (NFR-OPS-01, NFR-OPS-02).
FROM distroless-runtime AS migrate
COPY --from=builder /out/boltrope-migrate /usr/local/bin/boltrope-migrate
ENTRYPOINT ["/usr/local/bin/boltrope-migrate"]

# ── harnessctl (client CLI) ──────────────────────────────────────────────────
# The thin gRPC client used by the quickstart (`harnessctl run "hello world"`).
FROM distroless-runtime AS harnessctl
COPY --from=builder /out/harnessctl /usr/local/bin/harnessctl
ENTRYPOINT ["/usr/local/bin/harnessctl"]

# ── toolruntimed (needs the docker CLI) ──────────────────────────────────────
# The ONLY image that is not distroless: the tool-runtime shells out to `docker`
# to create/exec/kill per-session sandbox containers, so the docker CLI must be
# present. The readiness probe runs `docker version` against the mounted host
# socket, so an image without the client is never ready (FR-OBS-05).
#
# SECURITY: mounting the host /var/run/docker.sock into this container grants it
# control of the host docker daemon — effectively host root. This is acceptable
# ONLY for single-tenant / trusted-code dev (ADR-0005). Production wants a
# rootless/socket-proxied or microVM backend. Documented in deploy/README.md.
FROM alpine:${ALPINE_VERSION} AS toolruntimed
# docker-cli: the client only (no daemon). ca-certificates: outbound TLS.
# tini: a minimal init so SIGTERM reaches the daemon and zombies are reaped.
RUN apk add --no-cache docker-cli ca-certificates tini \
    && addgroup -S boltrope \
    && adduser -S -G boltrope boltrope
COPY --from=builder /out/boltrope-toolruntimed /usr/local/bin/boltrope-toolruntimed
# Run as non-root. NOTE: access to the mounted docker.sock requires the
# container user to be in the socket's group; the compose file sets
# `group_add` to the host docker gid (or run as root in pure-dev). See
# deploy/README.md.
USER boltrope
EXPOSE 9000 8080
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/boltrope-toolruntimed"]
