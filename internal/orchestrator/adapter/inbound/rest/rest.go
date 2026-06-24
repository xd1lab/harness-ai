// SPDX-License-Identifier: Apache-2.0

// Package rest is the orchestrator's REST/JSON + SSE inbound facade
// (FR-API-01/02/03; spec §2 "v1 REST facade covers at minimum Run via SSE +
// Control, with identical auth").
//
// # Design: a thin shell over the gRPC server
//
// The facade deliberately implements NO orchestration of its own: every route
// builds the corresponding boltrope.v1 request message and invokes the SAME
// [igrpc.Server] method the gRPC transport serves — ownership checks,
// per-tenant in-flight caps, interrupt registration, permission-mode
// persistence, and event→frame mapping are therefore shared by construction
// and can never drift between transports. Run's server-stream is adapted to
// Server-Sent Events by a [grpc.ServerStream] shim (see sse.go); every frame
// carries its durable seq as the SSE `id:`, so the standard `Last-Event-ID`
// header is the resume cursor (FR-API-01).
//
// Auth is the same [igrpc.Authenticator] instance the gRPC interceptors use
// (FR-API-03 "identical auth"): the middleware verifies the Authorization
// bearer and places the principal with [igrpc.ContextWithPrincipal], which
// also scopes the RLS tenant. In dev-insecure mode the dev principal is
// injected exactly as on the gRPC edge.
//
// Request bodies are small hand-shaped JSON envelopes optimized for curl
// (e.g. {"text": "..."} for Run); responses are canonical protojson of the
// boltrope.v1 response messages, so the wire vocabulary stays the proto
// contract. TLS termination for this surface is the deployment's concern
// (ingress), as documented in deploy/README.md.
package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
)

// maxBodyBytes caps every request body (the facade's inputs are tiny).
const maxBodyBytes = 1 << 20 // 1 MiB

// Handler serves the REST facade over the shared gRPC [igrpc.Server] and the
// shared edge [igrpc.Authenticator].
type Handler struct {
	grpc *igrpc.Server
	auth *igrpc.Authenticator
}

// NewHandler builds the facade. Both dependencies are required: srv is the
// transport-shared orchestrator server, auth the transport-shared edge
// authenticator.
func NewHandler(srv *igrpc.Server, auth *igrpc.Authenticator) *Handler {
	return &Handler{grpc: srv, auth: auth}
}

// Routes registers the facade's routes on mux (Go 1.22 method+path patterns),
// each wrapped in the auth middleware.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/sessions", h.withAuth(h.createSession))
	mux.HandleFunc("GET /v1/sessions", h.withAuth(h.listSessions))
	mux.HandleFunc("GET /v1/sessions/{id}", h.withAuth(h.getSession))
	mux.HandleFunc("GET /v1/sessions/{id}/usage", h.withAuth(h.getSessionUsage))
	mux.HandleFunc("POST /v1/sessions/{id}/run", h.withAuth(h.run))
	// POST /v1/sessions/{id}/control with {"action":"interrupt"} is the admin STOP:
	// it cooperatively interrupts a live run (resumable) and is an idempotent no-op
	// success on an already-finished/idle session (Feature I / ADR-0027 — STOP reuses
	// the existing Control interrupt, no new route).
	mux.HandleFunc("POST /v1/sessions/{id}/control", h.withAuth(h.control))
	mux.HandleFunc("POST /v1/sessions/{id}/fork", h.withAuth(h.fork))
}

// ---- auth middleware ---------------------------------------------------------

// withAuth verifies the request's bearer token via the shared authenticator
// and places the verified principal (and RLS tenant) on the request context —
// the REST analog of the gRPC auth interceptors.
func (h *Handler) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.auth.VerifyBearer(httpBearer(r))
		if err != nil {
			writeStatusError(w, err)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next(w, r.WithContext(igrpc.ContextWithPrincipal(r.Context(), p)))
	}
}

// httpBearer extracts the bearer token from the Authorization header ("" when
// absent/malformed — verification decides what that means per mode).
func httpBearer(r *http.Request) string {
	const prefix = "bearer "
	v := r.Header.Get("Authorization")
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}

// ---- request envelopes ---------------------------------------------------------

// createSessionBody is the POST /v1/sessions request envelope.
type createSessionBody struct {
	// Mode is the session's standing permission mode: "default", "acceptEdits",
	// or "plan" (same vocabulary as harnessctl --permission-mode). Empty means
	// the server default. "bypass" is rejected server-side (operator-only).
	Mode string `json:"mode"`
	// Metadata is an optional opaque key/value bag recorded on the session.
	Metadata map[string]string `json:"metadata"`
}

// runBody is the POST /v1/sessions/{id}/run request envelope.
type runBody struct {
	// Text is the user message for this run. Empty means a pure resume/reattach
	// (stream from the cursor without appending a new user turn).
	Text string `json:"text"`
	// AfterSeq is the resume cursor: only events with seq > after_seq are
	// streamed. The standard SSE Last-Event-ID header takes precedence.
	AfterSeq int64 `json:"after_seq"`
	// OutputSchema is an OPTIONAL JSON Schema object constraining this run's final
	// response to structured output. Passed inline as a JSON object (curl-friendly);
	// the facade forwards the raw bytes onto RunRequest.output_schema. Omit for
	// free-form output. A non-object value is rejected with HTTP 400.
	OutputSchema json.RawMessage `json:"output_schema"`
	// Strict requests provider-native strict schema enforcement where supported;
	// otherwise the loop validates and retries. Meaningful only with output_schema.
	Strict bool `json:"strict"`
}

// controlBody is the POST /v1/sessions/{id}/control request envelope.
type controlBody struct {
	// Action is one of "approve", "deny", "interrupt", "reattach".
	Action string `json:"action"`
	// CallID identifies the pending approval for approve/deny.
	CallID string `json:"call_id"`
	// FromSeq is the reattach cursor for "reattach".
	FromSeq int64 `json:"from_seq"`
}

// forkBody is the POST /v1/sessions/{id}/fork request envelope.
type forkBody struct {
	// AtSeq is the parent seq to branch at.
	AtSeq int64 `json:"at_seq"`
}

// ---- handlers -----------------------------------------------------------------

// createSession maps POST /v1/sessions onto the shared CreateSession.
func (h *Handler) createSession(w http.ResponseWriter, r *http.Request) {
	var body createSessionBody
	if !decodeBody(w, r, &body) {
		return
	}
	mode, err := parseMode(body.Mode)
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, err.Error())
		return
	}
	resp, err := h.grpc.CreateSession(r.Context(), &genproto.CreateSessionRequest{
		Mode:     mode,
		Metadata: body.Metadata,
	})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// getSession maps GET /v1/sessions/{id} onto the shared GetSession.
func (h *Handler) getSession(w http.ResponseWriter, r *http.Request) {
	resp, err := h.grpc.GetSession(r.Context(), &genproto.GetSessionRequest{
		SessionId: r.PathValue("id"),
	})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// listSessions maps GET /v1/sessions onto the shared ListSessions (Feature I /
// ADR-0027). It carries NO tenant_id query param (the tenant is the authenticated
// principal); query params: status (repeated; active|finished|failed),
// created_after_ms / created_before_ms / page_size (decimal), page_token (opaque),
// descending (bool). An unparseable numeric/status/descending is a typed 400
// BEFORE any store call. The response is protojson of ListSessionsResponse.
func (h *Handler) listSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	statuses, err := parseStatuses(q["status"])
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, err.Error())
		return
	}
	createdAfterMs, err := parseOptionalInt64(q.Get("created_after_ms"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "created_after_ms must be a decimal Unix epoch ms")
		return
	}
	createdBeforeMs, err := parseOptionalInt64(q.Get("created_before_ms"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "created_before_ms must be a decimal Unix epoch ms")
		return
	}
	pageSize, err := parseOptionalPageSize(q.Get("page_size"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "page_size must be a decimal integer")
		return
	}
	descending, err := parseOptionalBool(q.Get("descending"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "descending must be true or false")
		return
	}

	resp, err := h.grpc.ListSessions(r.Context(), &genproto.ListSessionsRequest{
		Status:          statuses,
		CreatedAfterMs:  createdAfterMs,
		CreatedBeforeMs: createdBeforeMs,
		PageToken:       q.Get("page_token"),
		PageSize:        pageSize,
		Descending:      descending,
	})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// getSessionUsage maps GET /v1/sessions/{id}/usage onto the shared GetSessionUsage
// (Feature I / ADR-0027): per-session accumulated usage/cost/turns folded from the
// event log, tagged with its provenance. Returns protojson of
// GetSessionUsageResponse.
func (h *Handler) getSessionUsage(w http.ResponseWriter, r *http.Request) {
	resp, err := h.grpc.GetSessionUsage(r.Context(), &genproto.GetSessionUsageRequest{
		SessionId: r.PathValue("id"),
	})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// run maps POST /v1/sessions/{id}/run onto the shared streaming Run, adapting
// the gRPC server-stream to SSE. Failures BEFORE the first frame surface as a
// plain JSON error with the mapped HTTP status; failures after streaming began
// surface as a terminal `event: error` frame (the HTTP status is already
// committed, exactly like a broken gRPC stream).
func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	var body runBody
	if !decodeBody(w, r, &body) {
		return
	}
	afterSeq := body.AfterSeq
	if lei := r.Header.Get("Last-Event-ID"); lei != "" {
		n, err := strconv.ParseInt(lei, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, codes.InvalidArgument, "Last-Event-ID must be a decimal seq")
			return
		}
		afterSeq = n
	}

	req := &genproto.RunRequest{
		SessionId: r.PathValue("id"),
		AfterSeq:  afterSeq,
		Strict:    body.Strict,
	}
	if body.Text != "" {
		req.Message = userTextMessage(body.Text)
	}
	// A non-object output_schema is rejected at the facade BEFORE any run starts
	// (fail-closed-early), giving a typed 400 rather than a mid-run loop failure.
	if schema, ok := normalizeOutputSchema(body.OutputSchema); ok {
		req.OutputSchema = schema
	} else if len(body.OutputSchema) > 0 {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "output_schema must be a JSON object")
		return
	}

	stream := newSSEStream(r.Context(), w)
	err := h.grpc.Run(req, stream)
	switch {
	case err == nil:
	case !stream.started():
		writeStatusError(w, err)
	case !errors.Is(err, r.Context().Err()) || r.Context().Err() == nil:
		// The stream broke mid-flight for a non-disconnect reason: emit a
		// terminal error frame so the client sees a typed end, not silence.
		stream.sendError(err)
	}
}

// control maps POST /v1/sessions/{id}/control onto the shared Control.
func (h *Handler) control(w http.ResponseWriter, r *http.Request) {
	var body controlBody
	if !decodeBody(w, r, &body) {
		return
	}
	req := &genproto.ControlRequest{SessionId: r.PathValue("id")}
	switch strings.ToLower(strings.TrimSpace(body.Action)) {
	case "approve":
		req.Action = &genproto.ControlRequest_Approve{Approve: &genproto.ApproveAction{CallId: body.CallID}}
	case "deny":
		req.Action = &genproto.ControlRequest_Deny{Deny: &genproto.DenyAction{CallId: body.CallID}}
	case "interrupt":
		req.Action = &genproto.ControlRequest_Interrupt{Interrupt: &genproto.InterruptAction{}}
	case "reattach":
		req.Action = &genproto.ControlRequest_Reattach{Reattach: &genproto.ReattachAction{FromSeq: body.FromSeq}}
	default:
		writeError(w, http.StatusBadRequest, codes.InvalidArgument,
			fmt.Sprintf("unknown control action %q (want approve|deny|interrupt|reattach)", body.Action))
		return
	}
	resp, err := h.grpc.Control(r.Context(), req)
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// fork maps POST /v1/sessions/{id}/fork onto the shared Fork.
func (h *Handler) fork(w http.ResponseWriter, r *http.Request) {
	var body forkBody
	if !decodeBody(w, r, &body) {
		return
	}
	resp, err := h.grpc.Fork(r.Context(), &genproto.ForkRequest{
		SessionId: r.PathValue("id"),
		AtSeq:     body.AtSeq,
	})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	writeProto(w, resp)
}

// ---- helpers -------------------------------------------------------------------

// userTextMessage wraps a plain user string as the proto Message the Run RPC
// expects (one text part, role user).
func userTextMessage(text string) *genproto.Message {
	return &genproto.Message{
		Role: genproto.Role_ROLE_USER,
		Content: []*genproto.ContentPart{
			{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: text}}},
		},
	}
}

// normalizeOutputSchema validates that an inline output_schema is a JSON object
// (the only shape a JSON Schema document takes) and returns its raw bytes. An
// empty/omitted value yields (nil, true) — free-form, no error. A non-object
// (array/number/string/null) yields (nil, false) so the caller rejects it with a
// typed 400 before any run starts (IMPACT §4 fail-closed-early).
func normalizeOutputSchema(raw json.RawMessage) (schema []byte, ok bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, true
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}
	return []byte(raw), true
}

// parseStatuses maps the repeated ?status= query tokens (active|finished|failed,
// case-insensitive) onto the proto SessionStatus OR-filter for ListSessions. An
// empty list yields nil (all statuses); an unknown token is a typed error the
// caller maps to a 400.
func parseStatuses(raw []string) ([]genproto.SessionStatus, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]genproto.SessionStatus, 0, len(raw))
	for _, s := range raw {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "":
			continue
		case "active":
			out = append(out, genproto.SessionStatus_SESSION_STATUS_ACTIVE)
		case "finished":
			out = append(out, genproto.SessionStatus_SESSION_STATUS_FINISHED)
		case "failed":
			out = append(out, genproto.SessionStatus_SESSION_STATUS_FAILED)
		default:
			return nil, fmt.Errorf("unknown status %q (want active|finished|failed)", s)
		}
	}
	return out, nil
}

// parseOptionalInt64 parses an optional decimal query param. An empty value is 0
// (the "unset" sentinel for the int64 filter/cursor fields), not an error.
func parseOptionalInt64(s string) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// parseOptionalPageSize parses the optional page_size query param into an int32
// (the proto field width). An empty value is 0 (the server resolves it to the
// default); a value outside int32 range is parsed-bounded (ParseInt with bitSize
// 32 rejects overflow), so the server's clamp never sees an overflowed value.
func parseOptionalPageSize(s string) (int32, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

// parseOptionalBool parses an optional bool query param. An empty value is false,
// not an error; a non-bool token is an error.
func parseOptionalBool(s string) (bool, error) {
	if strings.TrimSpace(s) == "" {
		return false, nil
	}
	return strconv.ParseBool(s)
}

// parseMode maps the facade's mode vocabulary onto the proto enum, mirroring
// harnessctl's tolerant spelling ("acceptEdits"/"accept-edits"/"accept_edits").
// "bypass" maps to the enum so the server's operator-only rejection fires (the
// guard lives in ONE place); unknown strings are an edge-side 400.
func parseMode(s string) (genproto.PermissionMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED, nil
	case "default":
		return genproto.PermissionMode_PERMISSION_MODE_DEFAULT, nil
	case "acceptedits", "accept-edits", "accept_edits":
		return genproto.PermissionMode_PERMISSION_MODE_ACCEPT_EDITS, nil
	case "plan":
		return genproto.PermissionMode_PERMISSION_MODE_PLAN, nil
	case "bypass":
		return genproto.PermissionMode_PERMISSION_MODE_BYPASS, nil
	default:
		return genproto.PermissionMode_PERMISSION_MODE_UNSPECIFIED,
			fmt.Errorf("unknown permission mode %q (want default|acceptEdits|plan)", s)
	}
}

// decodeBody decodes the request's JSON body into v. An empty body is allowed
// (v keeps its zero value). On a malformed body it writes a 400 and reports
// false.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, errEmptyBody(err)) {
			return true
		}
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// errEmptyBody reports err back when it denotes an empty body (io.EOF), so
// decodeBody can treat "no body" as "all defaults" without importing io for
// one sentinel.
func errEmptyBody(err error) error {
	if err != nil && err.Error() == "EOF" {
		return err
	}
	return errors.New("not-empty")
}

// writeProto writes a protojson response (the canonical proto3 JSON mapping of
// the boltrope.v1 message).
func writeProto(w http.ResponseWriter, m proto.Message) {
	b, err := protojson.Marshal(m)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codes.Internal, "encode response: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// writeStatusError maps a gRPC status error onto the equivalent HTTP status
// (the standard grpc-gateway mapping) with a small JSON error envelope.
func writeStatusError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	writeError(w, httpStatus(st.Code()), st.Code(), st.Message())
}

// writeError writes the facade's JSON error envelope.
func writeError(w http.ResponseWriter, httpCode int, code codes.Code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code":  code.String(),
		"error": msg,
	})
}

// httpStatus is the canonical gRPC-code → HTTP-status mapping.
func httpStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499 // client closed request
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
