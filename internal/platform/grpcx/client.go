package grpcx

import (
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// DialConfig parameterizes [Dial]: the target, transport credentials, and extra
// dial options.
type DialConfig struct {
	// Target is the gRPC target (e.g. "dns:///tool-runtime:8443"). Required.
	Target string
	// Creds are the client transport credentials — typically built by
	// [SPIFFEClientCredentials] (pinning the callee's SPIFFE ID) or
	// [StaticDevClientCredentials]. Required: Dial does not fall back to
	// plaintext.
	Creds credentials.TransportCredentials
	// OTelOptions are passed to the OTel gRPC client stats handler. Optional.
	OTelOptions []otelgrpc.Option
	// Extra are additional dial options appended after the ones Dial installs
	// (credentials, stats handler). Use for keepalive, retry/service config, etc.
	// Optional.
	Extra []grpc.DialOption
}

// Dial creates a [*grpc.ClientConn] to cfg.Target over mutual TLS, with the OTel
// gRPC client stats handler installed so the outgoing call carries a client span
// and injects W3C trace context into gRPC metadata (FR-OBS-01 propagation). It
// uses grpc.NewClient, so the connection is established lazily on first RPC;
// credentials are mandatory (no insecure fallback).
func Dial(cfg DialConfig) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(cfg.Creds),
		grpc.WithStatsHandler(ClientStatsHandler(cfg.OTelOptions...)),
	}
	opts = append(opts, cfg.Extra...)

	conn, err := grpc.NewClient(cfg.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: dial %s: %w", cfg.Target, err)
	}
	return conn, nil
}
