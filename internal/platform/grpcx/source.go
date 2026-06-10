package grpcx

import (
	"context"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

// X509Source is the SPIFFE source of the workload's X509-SVID and the trust
// bundle, maintained (and auto-rotated) via the SPIRE Workload API. It is a type
// alias for go-spiffe's [workloadapi.X509Source]; it satisfies [SPIFFESource], so
// it can be passed directly to [SPIFFEServerCredentials]/[SPIFFEClientCredentials].
// Build one with [NewSPIFFESource]; close it on shutdown to release the Workload
// API connection.
type X509Source = workloadapi.X509Source

// NewSPIFFESource connects to the SPIRE Workload API and returns an [X509Source]
// once the initial SVID has been received. socketPath is the Workload API
// endpoint (e.g. "unix:///run/spire/sockets/agent.sock"); when empty, go-spiffe
// falls back to the SPIFFE_ENDPOINT_SOCKET environment variable. The call blocks
// until the first SVID arrives or ctx is done, so a service that requires SPIFFE
// identity fails fast at startup when no agent is reachable (the SPIRE bootstrap
// ordering of architecture §8.1: no mTLS handshake before attestation).
//
// The returned source must be closed when the process shuts down. It is the
// production counterpart of the ephemeral material minted by
// [StaticDevCredentials]; exactly one of the two is wired per process.
func NewSPIFFESource(ctx context.Context, socketPath string) (*X509Source, error) {
	var opts []workloadapi.X509SourceOption
	if socketPath != "" {
		opts = append(opts, workloadapi.WithClientOptions(workloadapi.WithAddr(socketPath)))
	}
	src, err := workloadapi.NewX509Source(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: connect to SPIRE Workload API: %w", err)
	}
	return src, nil
}
