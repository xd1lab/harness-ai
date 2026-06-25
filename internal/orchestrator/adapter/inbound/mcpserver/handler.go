// SPDX-License-Identifier: Apache-2.0

// Package mcpserver exposes Boltrope ITSELF as a Model Context Protocol (MCP)
// server so external MCP clients (Claude Desktop, Cursor, other agents) can
// delegate work to Boltrope: create a session, run a task, inspect state,
// approve/deny a pending tool call, and fork — all over the standard MCP wire.
//
// # Design: a thin shell over the gRPC server (the sibling of the REST facade)
//
// Like internal/orchestrator/adapter/inbound/rest, this adapter implements NO
// orchestration of its own: every MCP tool builds the corresponding boltrope.v1
// request and invokes the SAME [igrpc.Server] method the gRPC transport and the
// REST/SSE facade already invoke. Ownership checks, per-tenant in-flight caps,
// the approval gate, permission-mode persistence, durable resumable delivery, and
// at-most-once mutating actions are therefore inherited BY CONSTRUCTION and can
// never drift between transports. The position this unlocks — Boltrope as a
// sandboxed, tenant-isolated, auditable execution backend other agents CALL — is
// the #1 strategic differentiator (the "callee").
//
// # Transport (per DECISIONS.md / ADR-0022): Streamable HTTP only
//
// A single MCP endpoint mounted on the daemon HTTP listener beside the REST
// facade and the health endpoints:
//
//   - POST /mcp — a JSON-RPC 2.0 request; the server answers with
//     application/json (a single response) OR, for a tools/call of run that
//     carries _meta.progressToken, text/event-stream (an SSE leg that carries
//     notifications/progress — including the in-band approval — then the final
//     JSON-RPC response).
//   - GET /mcp, DELETE /mcp — 405 + Allow: POST (standalone listening stream and
//     explicit session termination are deferred to roadmap).
//
// # Auth + RLS (reused verbatim — no new auth code)
//
// Every POST /mcp flows through the MCP analog of REST's withAuth: extract the
// bearer, verify it with the SHARED [igrpc.Authenticator.VerifyBearer], and place
// the principal AND the RLS tenant scope via [igrpc.ContextWithPrincipal]. In
// dev-insecure mode it short-circuits to the dev principal, exactly as on the
// gRPC/REST edges. The only net-new security primitive at this edge is the
// Origin/DNS-rebinding guard (BOLTROPE_MCP_ALLOWED_ORIGINS).
//
// # Run + approval (per the DECISIONS.md amendment): call-stays-open
//
// A run tools/call keeps its SSE leg OPEN across an approval, exactly like REST's
// POST .../run: the approval is surfaced as an in-band notifications/progress
// frame and the human decision arrives on a CONCURRENT control call on a separate
// connection (Go serves each request on its own goroutine against the same
// in-process *approval.Gate). The previously-recorded "end-the-call, re-call-run"
// model is unimplementable against the real wiring (ending the call cancels the
// run request context → the gate's pending entry is removed → a later approve
// hits FailedPrecondition); see the DECISIONS.md amendment and ADR-0022.
package mcpserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
)

// maxBodyBytes caps every request body (MCP inputs are tiny envelopes).
const maxBodyBytes = 1 << 20 // 1 MiB

// Handler serves the MCP Server-mode surface over the shared gRPC [igrpc.Server]
// and the shared edge [igrpc.Authenticator] — the exact siblings the REST facade
// uses.
type Handler struct {
	grpc           *igrpc.Server
	auth           *igrpc.Authenticator
	version        string
	allowedOrigins []string
}

// NewHandler builds the MCP handler. srv is the transport-shared orchestrator
// server; auth the transport-shared edge authenticator; version the build version
// reported in initialize.serverInfo; allowedOrigins the comma-list of browser
// origins permitted by the DNS-rebinding guard (an empty list rejects any present
// Origin, allows an absent one). This 4-arg shape is the NET-NEW signature
// ([FIX-1]); rest.NewHandler takes only (srv, auth).
func NewHandler(srv *igrpc.Server, auth *igrpc.Authenticator, version string, allowedOrigins []string) *Handler {
	return &Handler{grpc: srv, auth: auth, version: version, allowedOrigins: allowedOrigins}
}

// Routes registers the MCP endpoint on mux (Go 1.22 method+path patterns). POST
// is the live path; GET/DELETE return 405 + Allow: POST (deferred features
// signaled conformantly).
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /mcp", h.handlePost)
	mux.HandleFunc("GET /mcp", methodNotAllowed)
	mux.HandleFunc("DELETE /mcp", methodNotAllowed)
}

// methodNotAllowed answers the deferred GET/DELETE methods with 405 + Allow: POST
// (AC-17): the spec-conformant signal that the path exists but the method is
// unsupported in v1.
func methodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Allow", "POST")
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// handlePost is the single POST /mcp entrypoint: it enforces the Origin guard,
// authenticates the bearer (placing the principal + RLS tenant), then dispatches
// the JSON-RPC request.
func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	if !h.originAllowed(r) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	p, err := h.auth.VerifyBearer(httpBearer(r))
	if err != nil {
		h.writeAuthError(w, err)
		return
	}
	ctx := igrpc.ContextWithPrincipal(r.Context(), p)
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	h.dispatch(w, r)
}

// originAllowed implements the MCP Streamable-HTTP DNS-rebinding guard (§2.4): an
// absent Origin is allowed (non-browser clients send none); a present Origin must
// be in the allowlist, else it is rejected (an empty allowlist + present Origin
// fails closed).
func (h *Handler) originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, allowed := range h.allowedOrigins {
		if strings.EqualFold(strings.TrimSpace(allowed), origin) {
			return true
		}
	}
	return false
}

// writeAuthError surfaces an auth failure as a JSON-RPC error with the mapped
// HTTP status (401 for Unauthenticated) and, on 401, a WWW-Authenticate: Bearer
// hint (the OAuth-resource-server signal; AC-13). The id is null because the body
// may not have been parsed.
func (h *Handler) writeAuthError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	if st.Code() == codes.Unauthenticated {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSON(w, httpStatus(st.Code()), statusErrorResponse(nil, err))
}

// dispatch parses the JSON-RPC request and routes it to the method handler. Parse
// and validity errors are JSON-RPC errors with HTTP 200 (the protocol carries the
// error). A notification (no id) is acknowledged with 202 and no body.
func (h *Handler) dispatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(nil, codeParseError, "read body: "+err.Error(), nil))
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusOK, errorResponse(nil, codeParseError, "parse error: "+err.Error(), nil))
		return
	}
	if req.JSONRPC != jsonRPCVersion || req.Method == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidRequest, "not a valid JSON-RPC 2.0 request", nil))
		return
	}

	switch req.Method {
	case "initialize":
		h.handleInitialize(w, &req)
	case "notifications/initialized":
		// A client notification: accept with 202, no body.
		w.WriteHeader(http.StatusAccepted)
	case "tools/list":
		writeJSON(w, http.StatusOK, resultResponse(req.ID, map[string]any{"tools": toolCatalog()}))
	case "tools/call":
		h.handleToolsCall(w, r, &req)
	default:
		if req.isNotification() {
			// Unknown notifications are silently accepted (no id to answer).
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeMethodNotFound, "method not found: "+req.Method, nil))
	}
}

// handleInitialize answers the MCP initialize handshake (AC-1): serverInfo,
// protocolVersion, and the tools-only capability set. It also stamps an advisory
// Mcp-Session-Id header (durable seq is the authoritative continuation state, so
// this is metadata only).
func (h *Handler) handleInitialize(w http.ResponseWriter, req *rpcRequest) {
	result := map[string]any{
		"protocolVersion": mcpProtocolVersion,
		"serverInfo":      map[string]any{"name": serverName, "version": h.version},
		"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
		"instructions": "Boltrope MCP Server mode. Open a 'run' tools/call with a _meta.progressToken; " +
			"watch for an in-band approval notifications/progress frame and resolve it with a CONCURRENT " +
			"'control' (approve/deny) call on a separate connection while the run call stays open.",
	}
	w.Header().Set("Mcp-Session-Id", newSessionCorrelationID())
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// toolCallParams is the tools/call params envelope.
type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Meta      struct {
		ProgressToken json.RawMessage `json:"progressToken"`
	} `json:"_meta"`
}

// handleToolsCall decodes the tools/call envelope and dispatches to the named
// tool. Unknown tools and bad arguments are JSON-RPC InvalidParams (-32602), not
// CallToolResults (the protocol-vs-tool-error split, §4.6).
func (h *Handler) handleToolsCall(w http.ResponseWriter, r *http.Request, req *rpcRequest) {
	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "invalid tools/call params: "+err.Error(), nil))
		return
	}
	switch params.Name {
	case "create_session":
		h.toolCreateSession(w, r, req, params)
	case "get_session":
		h.toolGetSession(w, r, req, params)
	case "control":
		h.toolControl(w, r, req, params)
	case "fork":
		h.toolFork(w, r, req, params)
	case "run":
		h.toolRun(w, r, req, params)
	case "list_sessions":
		h.toolListSessions(w, r, req, params)
	case "get_session_usage":
		h.toolGetSessionUsage(w, r, req, params)
	case "list_session_events":
		h.toolListSessionEvents(w, r, req, params)
	case "get_state_at_seq":
		h.toolGetStateAtSeq(w, r, req, params)
	case "get_session_cost":
		h.toolGetSessionCost(w, r, req, params)
	case "get_tenant_cost":
		h.toolGetTenantCost(w, r, req, params)
	case "verify_session_integrity":
		h.toolVerifySessionIntegrity(w, r, req, params)
	default:
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "unknown tool: "+params.Name, nil))
	}
}

// ---- create_session ---------------------------------------------------------

func (h *Handler) toolCreateSession(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args createSessionArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	mode, err := parseMode(args.Mode)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, err.Error(), nil))
		return
	}
	resp, err := h.grpc.CreateSession(r.Context(), &genproto.CreateSessionRequest{Mode: mode, Metadata: args.Metadata})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	result := textResult("created session "+resp.GetSessionId(),
		map[string]any{"session_id": resp.GetSessionId()})
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// ---- get_session ------------------------------------------------------------

func (h *Handler) toolGetSession(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args getSessionArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.GetSession(r.Context(), &genproto.GetSessionRequest{SessionId: args.SessionID})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	sess, err := protoToMap(resp.GetSession())
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode session: "+err.Error(), nil))
		return
	}
	result := textResult("session "+args.SessionID+" status "+resp.GetSession().GetStatus().String(),
		map[string]any{"session": sess})
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// ---- control ----------------------------------------------------------------

func (h *Handler) toolControl(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args controlArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	creq := &genproto.ControlRequest{SessionId: args.SessionID}
	switch strings.ToLower(strings.TrimSpace(args.Action)) {
	case "approve":
		creq.Action = &genproto.ControlRequest_Approve{Approve: &genproto.ApproveAction{CallId: args.CallID}}
	case "deny":
		creq.Action = &genproto.ControlRequest_Deny{Deny: &genproto.DenyAction{CallId: args.CallID}}
	case "interrupt":
		creq.Action = &genproto.ControlRequest_Interrupt{Interrupt: &genproto.InterruptAction{}}
	case "reattach":
		creq.Action = &genproto.ControlRequest_Reattach{Reattach: &genproto.ReattachAction{FromSeq: args.FromSeq}}
	default:
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams,
			"unknown control action "+args.Action+" (want approve|deny|interrupt|reattach)", nil))
		return
	}
	resp, err := h.grpc.Control(r.Context(), creq)
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	result := textResult(args.Action+" applied; head_seq derived",
		map[string]any{"head_seq": resp.GetHeadSeq()})
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// ---- fork -------------------------------------------------------------------

func (h *Handler) toolFork(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args forkArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.Fork(r.Context(), &genproto.ForkRequest{SessionId: args.SessionID, AtSeq: args.AtSeq})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	result := textResult("forked "+args.SessionID+" -> "+resp.GetSessionId(),
		map[string]any{"session_id": resp.GetSessionId()})
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// ---- list_sessions (Feature I / ADR-0027) -----------------------------------

func (h *Handler) toolListSessions(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args listSessionsArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	statuses, err := parseStatuses(args.Status)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, err.Error(), nil))
		return
	}
	resp, err := h.grpc.ListSessions(r.Context(), &genproto.ListSessionsRequest{
		Status:          statuses,
		CreatedAfterMs:  args.CreatedAfterMs,
		CreatedBeforeMs: args.CreatedBeforeMs,
		PageToken:       args.PageToken,
		PageSize:        args.PageSize,
		Descending:      args.Descending,
	})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode sessions: "+err.Error(), nil))
		return
	}
	result := textResult("listed sessions", out)
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// ---- get_session_usage (Feature I / ADR-0027) -------------------------------

func (h *Handler) toolGetSessionUsage(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args getSessionUsageArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.GetSessionUsage(r.Context(), &genproto.GetSessionUsageRequest{SessionId: args.SessionID})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode usage: "+err.Error(), nil))
		return
	}
	result := textResult("session "+args.SessionID+" usage", out)
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// toolListSessionEvents maps the list_session_events tool onto the shared
// ListSessionEvents (Feature M / event-read): redacted event descriptors,
// keyset-paginated on seq.
func (h *Handler) toolListSessionEvents(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args listSessionEventsArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.ListSessionEvents(r.Context(), &genproto.ListSessionEventsRequest{
		SessionId:      args.SessionID,
		AfterSeq:       args.AfterSeq,
		PageSize:       args.PageSize,
		IncludePayload: args.IncludePayload,
	})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode events: "+err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, textResult("session "+args.SessionID+" events", out)))
}

// toolGetStateAtSeq maps the get_state_at_seq tool onto the shared GetStateAtSeq
// (Feature M / event-read): the folded control/billing projection at at_seq.
func (h *Handler) toolGetStateAtSeq(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args getStateAtSeqArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.GetStateAtSeq(r.Context(), &genproto.GetStateAtSeqRequest{
		SessionId: args.SessionID,
		AtSeq:     args.AtSeq,
	})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode state: "+err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, textResult("session "+args.SessionID+" state", out)))
}

// toolGetSessionCost maps the get_session_cost tool onto the shared GetSessionCost
// (Feature O / cost-read): the session's per-model cost rollup plus the total.
func (h *Handler) toolGetSessionCost(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args getSessionCostArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.GetSessionCost(r.Context(), &genproto.GetSessionCostRequest{SessionId: args.SessionID})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode session cost: "+err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, textResult("session "+args.SessionID+" cost", out)))
}

// toolGetTenantCost maps the get_tenant_cost tool onto the shared GetTenantCost
// (Feature O / cost-read): the authenticated tenant's per-model cost aggregate.
func (h *Handler) toolGetTenantCost(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args getTenantCostArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	resp, err := h.grpc.GetTenantCost(r.Context(), &genproto.GetTenantCostRequest{})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	out, err := protoToMap(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode tenant cost: "+err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, textResult("tenant cost", out)))
}

// toolVerifySessionIntegrity maps the verify_session_integrity tool onto the
// shared VerifySessionIntegrity (Batch-5A tamper-evident hash-chain): it recomputes
// the per-event content hash and the per-session hash-chain over the owned session
// and reports the first tampered seq. Mirrors toolListSessionEvents — session_id is
// required (else InvalidParams); the tenant is the authenticated principal (no
// tenant_id arg), so ownership is enforced by the shared method.
func (h *Handler) toolVerifySessionIntegrity(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args verifySessionIntegrityArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}
	resp, err := h.grpc.VerifySessionIntegrity(r.Context(), &genproto.VerifySessionIntegrityRequest{
		SessionId: args.SessionID,
		FromSeq:   args.FromSeq,
		ToSeq:     args.ToSeq,
	})
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	// EmitUnpopulated: the verify verdict valid=false / first_bad_seq=0 are
	// load-bearing defaults the client must see (mirrors rest.writeProtoFull).
	out, err := protoToMapFull(resp)
	if err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInternalError, "encode integrity: "+err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, textResult("session "+args.SessionID+" integrity", out)))
}

// parseStatuses maps the list_sessions status string OR-filter (active|finished|
// failed, case-insensitive) onto the proto SessionStatus enum; an unknown token is
// a JSON-RPC InvalidParams error.
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
			return nil, errInvalidStatus(s)
		}
	}
	return out, nil
}

// writeToolStatusError surfaces a shared-method gRPC status error as a JSON-RPC
// error with the mapped HTTP status (§4.6), adding WWW-Authenticate on 401. Used
// for the unary tools and for run failures raised BEFORE the SSE preamble.
func (h *Handler) writeToolStatusError(w http.ResponseWriter, req *rpcRequest, err error) {
	st := status.Convert(err)
	if st.Code() == codes.Unauthenticated {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSON(w, httpStatus(st.Code()), statusErrorResponse(req.ID, err))
}

// ---- helpers ----------------------------------------------------------------

// decodeArgs strictly decodes a tool's arguments. An absent arguments object is
// treated as all-defaults (the zero value); a malformed one is InvalidParams.
func decodeArgs(w http.ResponseWriter, req *rpcRequest, raw json.RawMessage, v any) bool {
	if len(raw) == 0 {
		return true
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "invalid arguments: "+err.Error(), nil))
		return false
	}
	return true
}

// httpBearer extracts the bearer token from the Authorization header (the MCP
// analog of rest.httpBearer; that symbol is unexported, so the switch is
// replicated and pinned by TestBearerExtraction). "" when absent/malformed.
func httpBearer(r *http.Request) string {
	const prefix = "bearer "
	v := r.Header.Get("Authorization")
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}

// parseMode maps the MCP mode vocabulary onto the proto enum (mirrors
// rest.parseMode): tolerant accept-edits spellings, "bypass" mapped through so
// the server's single operator-only guard fires, unknown strings an edge error.
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
			errInvalidMode(s)
	}
}

// writeJSON encodes v as the JSON-RPC response body with the given HTTP status.
func writeJSON(w http.ResponseWriter, httpCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(v)
}
