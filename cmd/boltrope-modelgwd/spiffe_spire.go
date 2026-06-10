//go:build spire

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// spiffeSource connects to the SPIRE Workload API and returns the live,
// auto-rotating X509 source for inter-service mTLS (the production identity
// path; architecture §8.1). The endpoint is taken from
// BOLTROPE_SPIFFE_ENDPOINT_SOCKET, falling back to go-spiffe's default
// SPIFFE_ENDPOINT_SOCKET handling when empty. A connection failure here means no
// SVID, so [daemon.ServerCredentials] then fails closed unless dev-insecure is
// set. The blocking initial fetch is bounded by the process startup context.
func spiffeSource() grpcx.SPIFFESource {
	src, err := grpcx.NewSPIFFESource(context.Background(), os.Getenv("BOLTROPE_SPIFFE_ENDPOINT_SOCKET"))
	if err != nil {
		slog.Default().Error("modelgwd: SPIRE Workload API source unavailable", slog.Any("error", err))
		return nil
	}
	return src
}
