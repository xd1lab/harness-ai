package grpcx

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"log/slog"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var internalTestTD = spiffeid.RequireTrustDomainFromString("boltrope.local")

// TestNewDevCA_DeterministicAcrossIndependentCalls is the heart of the fix: two
// INDEPENDENT newDevCA calls with the same seed must derive the same CA KEY and
// the same CA public key / subject, because that public key is the trust anchor
// a server process and a client process (which never share memory) both verify
// peers against. The CA cert's own self-signature is randomized by crypto/x509
// and is deliberately NOT asserted to be byte-identical. A different seed must
// produce a different CA.
func TestNewDevCA_DeterministicAcrossIndependentCalls(t *testing.T) {
	seed := []byte("compose-shared-seed")

	a, err := newDevCA(internalTestTD, seed)
	require.NoError(t, err)
	b, err := newDevCA(internalTestTD, seed)
	require.NoError(t, err)

	// The derived CA key must be identical (deterministic key derivation) — this
	// is the property that makes cross-process trust work.
	require.IsType(t, &ecdsa.PrivateKey{}, a.key)
	assert.Zero(t, a.key.D.Cmp(b.key.D), "deterministic key D must match for the same seed")

	// The CA public keys (the trust anchors) and subjects must match.
	assert.True(t, a.cert.PublicKey.(*ecdsa.PublicKey).Equal(b.cert.PublicKey),
		"independent CAs from the same seed must share a public key")
	assert.Equal(t, a.cert.Subject.String(), b.cert.Subject.String())
	assert.Equal(t, a.cert.NotBefore, b.cert.NotBefore)
	assert.Equal(t, a.cert.NotAfter, b.cert.NotAfter)

	// The trust bundles must each contain exactly one authority sharing that key.
	authA := a.bundle.X509Authorities()
	authB := b.bundle.X509Authorities()
	require.Len(t, authA, 1)
	require.Len(t, authB, 1)
	assert.True(t, authA[0].PublicKey.(*ecdsa.PublicKey).Equal(authB[0].PublicKey))

	// A different seed must yield a different CA key.
	c, err := newDevCA(internalTestTD, []byte("a-totally-different-seed"))
	require.NoError(t, err)
	assert.NotZero(t, a.key.D.Cmp(c.key.D), "different seeds must produce different CA keys")
	assert.False(t, a.cert.PublicKey.(*ecdsa.PublicKey).Equal(c.cert.PublicKey),
		"different seeds must produce different CA public keys")
}

// TestNewDevCA_IssuedLeafChainsToSharedCA asserts a leaf SVID minted by one
// derived CA verifies against the trust bundle of an INDEPENDENT CA derived from
// the same seed — the cross-process trust chain in miniature.
func TestNewDevCA_IssuedLeafChainsToSharedCA(t *testing.T) {
	seed := []byte("compose-shared-seed")

	server, err := newDevCA(internalTestTD, seed)
	require.NoError(t, err)
	client, err := newDevCA(internalTestTD, seed)
	require.NoError(t, err)

	id, err := spiffeid.FromPath(internalTestTD, "/ns/default/sa/toolruntime")
	require.NoError(t, err)

	svid, _, err := server.issueSVID(id, rand.Reader)
	require.NoError(t, err)
	require.Len(t, svid.Certificates, 1)

	// Verify the server-issued leaf against the CLIENT-derived bundle.
	leaf := svid.Certificates[0]
	authorities := client.bundle.X509Authorities()
	require.Len(t, authorities, 1)

	roots := x509.NewCertPool()
	roots.AddCert(authorities[0])
	chains, err := leaf.Verify(x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	require.NoError(t, err, "leaf issued by one shared-seed CA must verify against an independently-derived CA bundle")
	assert.NotEmpty(t, chains)
}

// TestResolveDevCASeed_DefaultsWithWarning asserts that when BOLTROPE_DEV_CA_SEED
// is unset the resolver returns the fixed default seed AND logs a WARN-level
// warning naming the variable, so an operator is never silently relying on the
// well-known constant.
func TestResolveDevCASeed_DefaultsWithWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	seed, usedDefault := resolveDevCASeed(func(string) (string, bool) { return "", false }, logger)
	assert.True(t, usedDefault)
	assert.Equal(t, []byte(defaultDevCASeed), seed)
	assert.Contains(t, buf.String(), devCASeedEnv, "the warning must name the seed env var")

	// An explicit, non-empty seed is used verbatim and logs nothing.
	buf.Reset()
	seed, usedDefault = resolveDevCASeed(func(string) (string, bool) { return "explicit", true }, logger)
	assert.False(t, usedDefault)
	assert.Equal(t, []byte("explicit"), seed)
	assert.Empty(t, buf.String(), "an explicit seed must not trigger the default-seed warning")

	// An empty value is treated as unset (defaults + warns).
	buf.Reset()
	seed, usedDefault = resolveDevCASeed(func(string) (string, bool) { return "", true }, logger)
	assert.True(t, usedDefault)
	assert.Equal(t, []byte(defaultDevCASeed), seed)
	assert.Contains(t, buf.String(), devCASeedEnv)
}

// TestDeterministicECDSAKey_StableAndLabelSeparated asserts the key derivation
// primitive is a pure function of (seed, label): equal inputs give equal keys,
// and a different label under the same seed gives an independent key.
func TestDeterministicECDSAKey_StableAndLabelSeparated(t *testing.T) {
	seed := []byte("seed")

	k1, err := deterministicECDSAKey(seed, "label-a")
	require.NoError(t, err)
	k2, err := deterministicECDSAKey(seed, "label-a")
	require.NoError(t, err)
	assert.Zero(t, k1.D.Cmp(k2.D), "same seed+label must yield the same key")

	k3, err := deterministicECDSAKey(seed, "label-b")
	require.NoError(t, err)
	assert.NotZero(t, k1.D.Cmp(k3.D), "different label must yield an independent key")
}
