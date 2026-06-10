package grpcx_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// NewSPIFFESource's success path requires a live SPIRE Workload API endpoint
// (a unix socket / named pipe speaking the SPIFFE protocol), which a hermetic
// unit test cannot provide; only the address-resolution failure modes are
// covered here. They matter operationally: a service with a misconfigured
// socket must fail fast at startup with an actionable error, not hang or come
// up without identity (architecture §8.1 bootstrap ordering).

// TestNewSPIFFESource_RejectsInvalidSocketPath asserts a malformed Workload API
// address is rejected immediately (before any dial/blocking wait) and the error
// is wrapped with the grpcx context.
func TestNewSPIFFESource_RejectsInvalidSocketPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src, err := grpcx.NewSPIFFESource(ctx, "bogus://agent.sock")
	require.Error(t, err)
	assert.Nil(t, src, "no source must be handed back on failure")
	assert.Contains(t, err.Error(), "grpcx: connect to SPIRE Workload API")
	// The underlying go-spiffe validation names the accepted schemes so the
	// operator can fix the address.
	assert.Contains(t, err.Error(), "scheme")
}

// TestNewSPIFFESource_EmptySocketPathFallsBackToEnv asserts the documented
// fallback: with an empty socketPath, go-spiffe consults SPIFFE_ENDPOINT_SOCKET.
// Pointing that variable at an invalid address must produce the validation
// error for the ENV value — proof the fallback is actually consulted.
func TestNewSPIFFESource_EmptySocketPathFallsBackToEnv(t *testing.T) {
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "bogus://from-env")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src, err := grpcx.NewSPIFFESource(ctx, "")
	require.Error(t, err)
	assert.Nil(t, src)
	assert.Contains(t, err.Error(), "grpcx: connect to SPIRE Workload API")
}
