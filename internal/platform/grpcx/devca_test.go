package grpcx_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// devSeedLookup returns a LookupEnv that reports BOLTROPE_DEV_INSECURE=1 and
// BOLTROPE_DEV_CA_SEED=seed, leaving every other variable unset. It lets a test
// construct the static-cert dev credentials with a chosen shared seed without
// mutating the process environment (the suite stays hermetic).
func devSeedLookup(seed string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		switch key {
		case "BOLTROPE_DEV_INSECURE":
			return "1", true
		case "BOLTROPE_DEV_CA_SEED":
			return seed, true
		default:
			return "", false
		}
	}
}

// dialDevPair builds a server (StaticDevCredentials) and a client
// (StaticDevClientCredentials) INDEPENDENTLY — each from its own config and seed
// — wires them over bufconn, and returns the dialed client connection. The two
// credential constructors never share a CA object: if the handshake succeeds it
// is only because both derived the byte-identical shared CA from their seeds,
// which is exactly the cross-process compose property the fix provides.
func dialDevPair(t *testing.T, serverSeed, clientSeed string) *grpc.ClientConn {
	t.Helper()

	serverID := mustID(t, "/ns/default/sa/toolruntime")
	clientID := mustID(t, "/ns/default/sa/orchestrator")

	// Silence the mandatory dev-mode warnings; they are asserted elsewhere.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	serverCreds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
		TrustDomain: testTrustDomain,
		ServerID:    serverID,
		Logger:      logger,
		LookupEnv:   devSeedLookup(serverSeed),
	})
	require.NoError(t, err)
	require.NotNil(t, serverCreds)

	clientCreds, err := grpcx.StaticDevClientCredentials(grpcx.StaticDevConfig{
		TrustDomain: testTrustDomain,
		ServerID:    clientID, // used here as the CLIENT identity
		Logger:      logger,
		LookupEnv:   devSeedLookup(clientSeed),
	}, serverID) // authorize the server to be exactly serverID
	require.NoError(t, err)
	require.NotNil(t, clientCreds)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.Creds(serverCreds))

	// A tiny unknown-service echo so we can drive testMethod without generated
	// stubs (mirrors the harness in interceptors_test.go).
	desc := &grpc.ServiceDesc{
		ServiceName: "boltrope.test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Unary",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptyMsg)
				if err := dec(in); err != nil {
					return nil, err
				}
				return in.Payload, nil
			},
		}},
	}
	srv.RegisterService(desc, new(struct{}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestStaticDevCredentials_SharedSeedHandshakeSucceeds is the fix's core
// guarantee: a server built by StaticDevCredentials and a client built by
// StaticDevClientCredentials in SEPARATE constructor calls — exactly the
// cross-process compose case — complete a REAL mTLS handshake and an RPC
// succeeds, provided they share BOLTROPE_DEV_CA_SEED. The old per-process
// ephemeral CA made this impossible; the in-repo tests missed it because they
// shared a single grpcxtest.CA across both sides.
func TestStaticDevCredentials_SharedSeedHandshakeSucceeds(t *testing.T) {
	conn := dialDevPair(t, "shared-seed-abc", "shared-seed-abc")

	var got string
	err := conn.Invoke(context.Background(), testMethod, &emptyMsg{Payload: "ping"}, &got, grpc.ForceCodec(rawStringCodec{}))
	require.NoError(t, err, "mTLS handshake + RPC must succeed when both sides share the dev CA seed")
	assert.Equal(t, "ping", got)
}

// TestStaticDevCredentials_DifferentSeedsFailHandshake is the negative control:
// when the server and client derive their dev CA from DIFFERENT seeds they no
// longer share a trust anchor, so the mutual-TLS handshake must fail and the RPC
// must error (Unavailable, since the transport handshake is rejected). This is
// the exact failure the shared-CA derivation turns into a success for matching
// seeds while still rejecting genuinely distinct CAs.
func TestStaticDevCredentials_DifferentSeedsFailHandshake(t *testing.T) {
	conn := dialDevPair(t, "server-seed-one", "client-seed-two")

	var got string
	err := conn.Invoke(context.Background(), testMethod, &emptyMsg{Payload: "ping"}, &got, grpc.ForceCodec(rawStringCodec{}))
	require.Error(t, err, "handshake must fail when the two sides derive different dev CAs")
	assert.Equal(t, codes.Unavailable, status.Code(err),
		"a rejected TLS handshake surfaces as Unavailable")
}
