package daemon

import (
	"crypto/rand"
	"encoding/binary"
	mathrand "math/rand/v2"
)

// NewJitter returns a non-deterministic random source suitable for retry backoff
// jitter — a *math/rand/v2.Rand seeded from crypto/rand. It satisfies the
// `interface{ Float64() float64 }` the model-gateway retry policy injects
// (internal/modelgateway/app/retry.Rand) without that package importing a
// concrete RNG, keeping the deterministic test seam (an injected fake) intact
// while production wiring supplies real entropy here at the edge.
//
// The crypto seed means two processes do not share a backoff schedule (avoiding
// synchronized retry storms); the cheap math/rand generator is fine for jitter,
// which needs spread, not cryptographic unpredictability.
func NewJitter() *mathrand.Rand {
	var seed [16]byte
	// crypto/rand.Read never returns a short read; ignore the error per its
	// contract (a failure is catastrophic and would panic the reader internally).
	_, _ = rand.Read(seed[:])
	//nolint:gosec // G404: jitter needs spread, not cryptographic unpredictability; the SEED is from crypto/rand.
	return mathrand.New(mathrand.NewPCG(
		binary.LittleEndian.Uint64(seed[0:8]),
		binary.LittleEndian.Uint64(seed[8:16]),
	))
}
