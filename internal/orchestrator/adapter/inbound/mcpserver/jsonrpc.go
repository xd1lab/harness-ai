// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JSON-RPC 2.0 framing for the hand-rolled MCP server, mirroring the existing
// hand-rolled MCP client's vocabulary
// (internal/toolruntime/adapter/outbound/mcp/jsonrpc.go: request/response/error).
// The server is the inverse direction (we receive requests, send responses and
// server→client notifications), but the wire structs are the same shape so the
// repo keeps one JSON-RPC dialect.

// jsonRPCVersion is the only protocol version the server accepts on a request
// and stamps on every response/notification it emits.
const jsonRPCVersion = "2.0"

// rpcRequest is a decoded inbound JSON-RPC request (or notification when ID is
// absent). ID is kept as json.RawMessage so it round-trips verbatim whether the
// client used a string or a number ([FIX-7]); a nil ID denotes a notification.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// isNotification reports whether the request is a JSON-RPC notification (no id),
// which the server acknowledges with HTTP 202 and no body.
func (r *rpcRequest) isNotification() bool { return len(r.ID) == 0 }

// rpcResponse is an outbound JSON-RPC response. Exactly one of Result/Error is
// set. ID echoes the request's id verbatim (or JSON null for a pre-parse error).
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// rpcError is the JSON-RPC error object. Data carries the underlying gRPC code
// string (data.grpc_code) for server-defined status errors so clients can branch.
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// rpcNotification is an outbound server→client JSON-RPC notification (no id),
// e.g. notifications/progress on the run SSE leg.
type rpcNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// Standard JSON-RPC 2.0 error codes (per the spec) plus one server-defined code.
const (
	// codeParseError — the body was not valid JSON.
	codeParseError = -32700
	// codeInvalidRequest — valid JSON but not a JSON-RPC 2.0 request object.
	codeInvalidRequest = -32600
	// codeMethodNotFound — unknown JSON-RPC method.
	codeMethodNotFound = -32601
	// codeInvalidParams — unknown tool, bad/missing arguments, validation, or a
	// shared-method InvalidArgument.
	codeInvalidParams = -32602
	// codeInternalError — a shared-method Internal failure.
	codeInternalError = -32603
	// codeServerError is the server-defined code for the remaining gRPC statuses
	// (Unauthenticated / PermissionDenied / NotFound / FailedPrecondition /
	// ResourceExhausted). The underlying code string is carried in data.grpc_code.
	codeServerError = -32001
)

// resultResponse builds a successful JSON-RPC response carrying result for id.
func resultResponse(id json.RawMessage, result any) rpcResponse {
	b, err := json.Marshal(result)
	if err != nil {
		return errorResponse(id, codeInternalError, "encode result: "+err.Error(), nil)
	}
	return rpcResponse{JSONRPC: jsonRPCVersion, ID: idOrNull(id), Result: b}
}

// errorResponse builds a JSON-RPC error response for id. data (optional) is
// marshaled into the error's data field.
func errorResponse(id json.RawMessage, code int, msg string, data any) rpcResponse {
	e := &rpcError{Code: code, Message: msg}
	if data != nil {
		if b, err := json.Marshal(data); err == nil {
			e.Data = b
		}
	}
	return rpcResponse{JSONRPC: jsonRPCVersion, ID: idOrNull(id), Error: e}
}

// idOrNull returns id, or a JSON null when id is absent (a pre-parse error has
// no request id to echo).
func idOrNull(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

// statusErrorResponse maps a gRPC status error from a shared igrpc.Server method
// onto a JSON-RPC error response, choosing the JSON-RPC code from the gRPC code
// and carrying the gRPC code string in data.grpc_code (§4.6). The companion HTTP
// status is the caller's concern via httpStatus.
func statusErrorResponse(id json.RawMessage, err error) rpcResponse {
	st := status.Convert(err)
	code := jsonRPCCode(st.Code())
	data := map[string]string{"grpc_code": st.Code().String()}
	return errorResponse(id, code, st.Message(), data)
}

// jsonRPCCode maps a gRPC code to the JSON-RPC error code per §4.6: the standard
// codes for argument/internal failures, and the server-defined -32001 for the
// auth/ownership/precondition/exhaustion family (their gRPC code string rides in
// data.grpc_code).
func jsonRPCCode(c codes.Code) int {
	switch c {
	case codes.InvalidArgument, codes.OutOfRange:
		return codeInvalidParams
	case codes.Internal:
		return codeInternalError
	case codes.Unauthenticated, codes.PermissionDenied, codes.NotFound,
		codes.FailedPrecondition, codes.ResourceExhausted, codes.AlreadyExists,
		codes.Aborted, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled,
		codes.Unimplemented:
		return codeServerError
	default:
		return codeServerError
	}
}

// httpStatus is the canonical gRPC-code → HTTP-status mapping, REPLICATED
// verbatim from rest.httpStatus (rest.go:356) because that symbol is unexported.
// TestHTTPStatusParity pins this table against the documented REST values so the
// two cannot drift — notably FailedPrecondition→400 (the v2 fix; AC-10b) and
// AlreadyExists/Aborted→409.
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
