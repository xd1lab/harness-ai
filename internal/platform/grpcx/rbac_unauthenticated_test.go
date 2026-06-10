package grpcx_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// certWithoutSPIFFEID self-signs a certificate that carries NO URI SAN, i.e. no
// SPIFFE ID. A peer presenting such a cert authenticated at the TLS layer
// somehow, but it has no identity the verb gate can authorize — it must still
// be rejected (fail closed).
func certWithoutSPIFFEID(t *testing.T) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "no-spiffe-id"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

// TestUnaryRBACInterceptor_RejectsNonMTLSPeers covers the Unauthenticated arm
// of the verb gate: a connection that somehow reached the interceptor WITHOUT a
// verified SPIFFE mTLS identity must be rejected before the handler — never
// treated as some default caller (architecture §8.1 fail-closed identity).
func TestUnaryRBACInterceptor_RejectsNonMTLSPeers(t *testing.T) {
	policy := grpcx.RBACPolicy{testMethod: {mustID(t, "/ns/default/sa/orchestrator")}}

	tests := []struct {
		name      string
		ctx       context.Context
		wantInMsg string
	}{
		{
			name:      "no peer information on the context",
			ctx:       context.Background(),
			wantInMsg: "no peer information",
		},
		{
			name:      "peer without TLS auth info",
			ctx:       peer.NewContext(context.Background(), &peer.Peer{}),
			wantInMsg: "not mutually-authenticated",
		},
		{
			name: "TLS peer that presented no client certificate",
			ctx: peer.NewContext(context.Background(), &peer.Peer{
				AuthInfo: credentials.TLSInfo{},
			}),
			wantInMsg: "no client certificate",
		},
		{
			name: "client certificate without a SPIFFE ID",
			ctx: peer.NewContext(context.Background(), &peer.Peer{
				AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
					PeerCertificates: []*x509.Certificate{certWithoutSPIFFEID(t)},
				}},
			}),
			wantInMsg: "no valid SPIFFE ID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, any) (any, error) {
				handlerCalled = true
				return "ok", nil
			}

			resp, err := grpcx.UnaryRBACInterceptor(policy)(
				tc.ctx, nil, &grpc.UnaryServerInfo{FullMethod: testMethod}, handler)
			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, codes.Unauthenticated, status.Code(err),
				"a missing mTLS identity is Unauthenticated, not PermissionDenied")
			assert.Contains(t, err.Error(), tc.wantInMsg)
			assert.False(t, handlerCalled, "the handler must never run without a verified peer identity")
		})
	}
}

// TestStreamRBACInterceptor_RejectsNonMTLSPeer mirrors the unary fail-closed
// check on the streaming side, where the identity comes from the stream's
// context.
func TestStreamRBACInterceptor_RejectsNonMTLSPeer(t *testing.T) {
	policy := grpcx.RBACPolicy{streamMethod: {mustID(t, "/ns/default/sa/orchestrator")}}

	handlerCalled := false
	err := grpcx.StreamRBACInterceptor(policy)(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error {
			handlerCalled = true
			return nil
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, handlerCalled)
}
