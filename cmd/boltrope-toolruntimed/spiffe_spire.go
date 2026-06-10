//go:build spire

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// spiffeSource connects to the SPIRE Workload API and returns the live,
// auto-rotating X509 source for inter-service mTLS (the production identity path;
// architecture §8.1). A connection failure here means no SVID, so
// [daemon.ServerCredentials] then fails closed unless dev-insecure is set.
func spiffeSource() grpcx.SPIFFESource {
	src, err := grpcx.NewSPIFFESource(context.Background(), os.Getenv("BOLTROPE_SPIFFE_ENDPOINT_SOCKET"))
	if err != nil {
		slog.Default().Error("toolruntimed: SPIRE Workload API source unavailable", slog.Any("error", err))
		return nil
	}
	return src
}
