//go:build !spire

package main

import "github.com/boltrope/boltrope/internal/platform/grpcx"

// spiffeSource returns the live SPIFFE source for the health endpoint's mTLS. In
// the default build (no `spire` tag) the SPIRE Workload API client is not
// compiled in, so this returns nil and the daemon falls back to the
// BOLTROPE_DEV_INSECURE static-cert path — or, in a non-dev process, exits
// because no identity is available (NFR-SEC-01). Build with `-tags spire` to wire
// the real Workload API source (see spiffe_spire.go).
func spiffeSource() grpcx.SPIFFESource { return nil }
