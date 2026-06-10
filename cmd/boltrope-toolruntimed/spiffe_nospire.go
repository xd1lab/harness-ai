//go:build !spire

package main

import "github.com/xd1lab/harness-ai/internal/platform/grpcx"

// spiffeSource returns the live SPIFFE source for inter-service mTLS. The SPIRE
// Workload API wiring is opt-in behind the `spire` build tag — release images
// build with `-tags spire` (see Dockerfile) — so this default untagged build
// returns nil and the daemon falls back to the env-gated BOLTROPE_DEV_INSECURE
// static-cert path — or, in a non-dev process, exits because no identity is
// available (NFR-SEC-01). See spiffe_spire.go for the tagged Workload API
// wiring.
func spiffeSource() grpcx.SPIFFESource { return nil }
