package grpcx

import (
	"context"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// RBACPolicy is a deny-by-default, per-RPC service authorization table: it maps a
// fully-qualified gRPC method ("/package.Service/Method") to the set of peer
// SPIFFE IDs allowed to call it. A method absent from the map is denied to every
// caller (deny-by-default); an empty allowlist for a present method also denies
// all. This is the coarse verb gate of architecture §8.1 — for example
// {"/boltrope.v1.ToolRuntime/ExecuteTool": {orchestratorID}} lets only the
// orchestrator invoke ExecuteTool. It is paired with the tenant token + RLS row
// checks elsewhere; service identity alone is never sufficient.
//
// An RBACPolicy is read-only after construction and safe for concurrent use.
type RBACPolicy map[string][]spiffeid.ID

// allows reports whether the given peer SPIFFE ID may call method under this
// policy. It is deny-by-default: an unlisted method, or a caller not in a listed
// method's allowlist, returns false.
func (p RBACPolicy) allows(method string, caller spiffeid.ID) bool {
	allowed, ok := p[method]
	if !ok {
		return false
	}
	for _, id := range allowed {
		if id == caller {
			return true
		}
	}
	return false
}

// peerSPIFFEID extracts the verified peer SPIFFE ID from the RPC context. The ID
// is taken from the leaf of the peer's verified certificate chain
// (PeerCertificates[0]), which the SPIFFE mTLS handshake has already
// authenticated and authorized against the trust bundle. It returns an
// Unauthenticated status error when there is no peer, no TLS auth info, or no
// SPIFFE ID on the presented certificate — so a connection that somehow bypassed
// mTLS cannot pass the verb gate.
func peerSPIFFEID(ctx context.Context) (spiffeid.ID, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return spiffeid.ID{}, status.Error(codes.Unauthenticated, "grpcx: no peer information on RPC (mTLS required)")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return spiffeid.ID{}, status.Error(codes.Unauthenticated, "grpcx: peer is not mutually-authenticated over TLS")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return spiffeid.ID{}, status.Error(codes.Unauthenticated, "grpcx: peer presented no client certificate")
	}
	id, err := x509svid.IDFromCert(tlsInfo.State.PeerCertificates[0])
	if err != nil {
		return spiffeid.ID{}, status.Errorf(codes.Unauthenticated, "grpcx: peer certificate carries no valid SPIFFE ID: %v", err)
	}
	return id, nil
}

// authorize is the shared verb-gate check used by both the unary and stream RBAC
// interceptors: it resolves the peer SPIFFE ID and enforces the allowlist,
// returning a typed status error (Unauthenticated when no peer identity,
// PermissionDenied when the caller is not allowed for the method).
func (p RBACPolicy) authorize(ctx context.Context, method string) error {
	caller, err := peerSPIFFEID(ctx)
	if err != nil {
		return err
	}
	if !p.allows(method, caller) {
		return status.Errorf(codes.PermissionDenied,
			"grpcx: caller %q is not authorized to call %s (deny-by-default service RBAC, architecture §8.1)",
			caller.String(), method)
	}
	return nil
}

// UnaryRBACInterceptor returns a unary server interceptor enforcing the
// deny-by-default per-RPC SPIFFE-ID allowlist in policy. A caller whose verified
// peer SPIFFE ID is not permitted for the invoked method is rejected with
// codes.PermissionDenied before the handler runs; a request lacking peer mTLS
// identity is rejected with codes.Unauthenticated (architecture §8.1).
func UnaryRBACInterceptor(policy RBACPolicy) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := policy.authorize(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamRBACInterceptor returns a stream server interceptor enforcing the same
// deny-by-default per-RPC SPIFFE-ID allowlist as [UnaryRBACInterceptor], applied
// to the stream's method before the stream handler runs.
func StreamRBACInterceptor(policy RBACPolicy) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := policy.authorize(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
