{{/*
Boltrope chart helpers: naming, labels, render-time fail-closed guards, and
the env/volume fragments shared by every daemon.
*/}}

{{- define "boltrope.name" -}}
{{- default "boltrope" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "boltrope.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "boltrope.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "boltrope.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "boltrope.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "boltrope.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* selectorLabels expects (dict "ctx" $ "component" "<name>") */}}
{{- define "boltrope.selectorLabels" -}}
app.kubernetes.io/name: {{ include "boltrope.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/* image expects (dict "ctx" $ "component" "<image-suffix>") */}}
{{- define "boltrope.image" -}}
{{- $tag := default .ctx.Chart.AppVersion .ctx.Values.image.tag -}}
{{- printf "%s/boltrope-%s:%s" .ctx.Values.image.registry .component $tag -}}
{{- end }}

{{- define "boltrope.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "boltrope.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/*
Render-time fail-closed guards (NFR-SEC-01). Included by every workload
template so a partial `helm template -s` still trips them.
*/}}
{{- define "boltrope.guards" -}}
{{- if and .Values.devInsecure.enabled (not .Values.devInsecure.acknowledged) -}}
{{- fail "devInsecure.enabled=true disables ALL transport security and edge auth (static dev CA + fixed dev principal). This is never acceptable in production. If this is a throwaway dev cluster, set devInsecure.acknowledged=true to proceed." -}}
{{- end -}}
{{- if not .Values.devInsecure.enabled -}}
{{- if not .Values.spire.enabled -}}
{{- fail "production (devInsecure disabled) requires spire.enabled=true: without a SPIFFE Workload API source every daemon refuses to start (ErrNoServerIdentity, NFR-SEC-01). Install SPIRE (see deploy/k8s/spire/) or explicitly opt into the acknowledged dev-insecure mode." -}}
{{- end -}}
{{- $_ := required "oidc.issuer is required in production (FR-API-03): orchestratord refuses to start without BOLTROPE_OIDC_ISSUER. Set oidc.issuer to your IdP issuer URL." .Values.oidc.issuer -}}
{{- end -}}
{{- $_ := required "postgres.appDSNSecret.name is required: an existing Secret holding the non-owner (RLS-bound) application DSN." .Values.postgres.appDSNSecret.name -}}
{{- end }}

{{/*
Env shared by every daemon: addresses, trust domain, DSN (from the app-role
Secret), blob dir, OTLP, and the model-gateway endpoint the shared config
loader validates for every service.
*/}}
{{- define "boltrope.commonEnv" -}}
- name: BOLTROPE_SERVER__GRPC_ADDR
  value: ":9000"
- name: BOLTROPE_SERVER__HTTP_ADDR
  value: ":8080"
- name: BOLTROPE_TRUST_DOMAIN
  value: {{ .Values.trustDomain | quote }}
- name: BOLTROPE_LOG_LEVEL
  value: {{ .Values.logLevel | quote }}
- name: BOLTROPE_POSTGRES__VERSION
  value: {{ .Values.postgres.version | quote }}
- name: BOLTROPE_POSTGRES__DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.appDSNSecret.name | quote }}
      key: {{ .Values.postgres.appDSNSecret.key | quote }}
- name: BOLTROPE_MODEL_GATEWAY__ENDPOINT
  value: {{ printf "%s-modelgwd:9000" (include "boltrope.fullname" .) | quote }}
- name: BOLTROPE_BLOB__DIR
  value: /var/lib/boltrope/blobs
{{- if .Values.otlp.endpoint }}
- name: BOLTROPE_OTLP__ENDPOINT
  value: {{ .Values.otlp.endpoint | quote }}
{{- if .Values.otlp.insecure }}
- name: BOLTROPE_OTLP__INSECURE
  value: "1"
{{- end }}
{{- end }}
{{- end }}

{{/*
Identity env: the SPIRE Workload API socket in production, or the
acknowledged dev-insecure escape hatch.
*/}}
{{- define "boltrope.identityEnv" -}}
{{- if .Values.devInsecure.enabled -}}
- name: BOLTROPE_DEV_INSECURE
  value: "1"
- name: BOLTROPE_DEV_CA_SEED
  value: {{ .Values.devInsecure.caSeed | quote }}
{{- else -}}
- name: BOLTROPE_SPIFFE_ENDPOINT_SOCKET
  value: {{ printf "unix://%s/%s" .Values.spire.socket.mountPath .Values.spire.socket.socketFile | quote }}
{{- end -}}
{{- end }}

{{- define "boltrope.identityVolumes" -}}
{{- if and (not .Values.devInsecure.enabled) .Values.spire.enabled -}}
- name: spiffe-workload-api
{{- if eq .Values.spire.socket.mode "csi" }}
  csi:
    driver: {{ .Values.spire.socket.csiDriver }}
    readOnly: true
{{- else if eq .Values.spire.socket.mode "hostPath" }}
  hostPath:
    path: {{ .Values.spire.socket.hostPath }}
    type: Directory
{{- else }}
{{- fail (printf "spire.socket.mode must be csi or hostPath, got %q" .Values.spire.socket.mode) }}
{{- end }}
{{- end -}}
{{- end }}

{{- define "boltrope.identityVolumeMounts" -}}
{{- if and (not .Values.devInsecure.enabled) .Values.spire.enabled -}}
- name: spiffe-workload-api
  mountPath: {{ .Values.spire.socket.mountPath | quote }}
  readOnly: true
{{- end -}}
{{- end }}

{{- define "boltrope.blobsVolume" -}}
- name: blobs
  persistentVolumeClaim:
    claimName: {{ default (printf "%s-blobs" (include "boltrope.fullname" .)) .Values.blobs.existingClaim }}
{{- end }}

{{/* Liveness/readiness against the daemon HTTP listener (FR-OBS-05:
readiness gates on SVID presence + downstream health, so an identity-less or
schema-less pod never receives traffic). */}}
{{- define "boltrope.probes" -}}
livenessProbe:
  httpGet:
    path: /livez
    port: http
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: http
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 12
{{- end }}

{{/* Hardened defaults for the distroless service containers. */}}
{{- define "boltrope.containerSecurityContext" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop: ["ALL"]
{{- end }}
