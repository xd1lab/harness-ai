# SPIRE for Boltrope — worked example

Production Boltrope (the Helm chart with `devInsecure` off, which is the
default) requires every workload to hold a SPIRE-issued X.509 SVID: the
daemons refuse to start without one (`ErrNoServerIdentity`, NFR-SEC-01), every
inter-service connection is mutually authenticated, and the callee identity is
pinned per service (architecture §8.1). This directory gets a cluster from
zero to "Boltrope pods become Ready".

## 1. Install SPIRE (official SPIFFE chart)

Use the maintained
[helm-charts-hardened](https://github.com/spiffe/helm-charts-hardened) chart —
it installs the SPIRE server, agents (DaemonSet), the SPIFFE CSI driver, and
the controller-manager that turns `ClusterSPIFFEID` resources into
registration entries:

```bash
helm repo add spire https://spiffe.github.io/helm-charts-hardened/
helm upgrade --install -n spire-mgmt --create-namespace spire-crds spire/spire-crds
helm upgrade --install -n spire-mgmt spire spire/spire \
  --set global.spire.trustDomain=boltrope.example.com \
  --set global.spire.clusterName=my-cluster
```

The trust domain here MUST equal the Boltrope chart's `trustDomain` value —
it is the namespace both sides authorize within.

## 2. Register the Boltrope workloads

```bash
kubectl apply -f clusterspiffeids.yaml
```

[`clusterspiffeids.yaml`](clusterspiffeids.yaml) matches the pods by the
labels the Boltrope chart stamps and issues the four pinned identities
(`/orchestrator`, `/model-gateway`, `/tool-runtime`, `/projectord`). The
`className` is `spire-mgmt-spire` for the install command above (the chart's
controller-manager class defaults to `<release-namespace>-<release-name>`);
adjust it if you installed SPIRE under different names, and add a
`namespaceSelector` if you want to confine issuance to Boltrope's namespace.

## 3. Point Boltrope at the agent socket

The SPIFFE chart exposes the agent Workload API through the **SPIFFE CSI
driver** (`csi.spiffe.io`) — the Boltrope chart's default:

```yaml
spire:
  enabled: true
  socket:
    mode: csi          # mounts csi.spiffe.io read-only into every pod
    socketFile: spire-agent.sock
```

Each daemon then reads
`BOLTROPE_SPIFFE_ENDPOINT_SOCKET=unix:///spiffe-workload-api/spire-agent.sock`
(set by the chart) and connects to the Workload API before serving. If your
SPIRE install exposes a plain hostPath socket directory instead, set
`spire.socket.mode=hostPath` and `spire.socket.hostPath` accordingly.

## 4. Verify

```bash
# SVIDs minted? (one entry per Boltrope component)
kubectl -n spire-mgmt exec deploy/spire-server -- \
  /opt/spire/bin/spire-server entry show

# Boltrope pods Ready? Readiness deliberately gates on SVID presence
# (FR-OBS-05): a pod that never becomes Ready almost always means its
# ClusterSPIFFEID did not match (check labels) or the CSI mount is absent.
kubectl get pods -l app.kubernetes.io/name=boltrope
```

A successful rollout proves the whole chain: agent attestation → SVID issuance
→ mTLS handshakes with pinned peer identities → `/readyz` green.

## Notes

- **Identity names are wire contract.** orchestratord pins
  `spiffe://<td>/model-gateway` and `spiffe://<td>/tool-runtime` when dialing;
  the downstream RBAC admits only `spiffe://<td>/orchestrator`. Changing the
  template segments breaks mutual auth — they are not free-form.
- **Rotation is automatic.** The daemons hold a live Workload API source
  (`workloadapi.X509Source`); SVID rotation needs no restart.
- **harnessctl** has no SPIRE path: in production drive the REST/SSE facade
  with an OIDC bearer token (see `deploy/README.md`), or run gRPC clients
  in-mesh with their own SVID.
