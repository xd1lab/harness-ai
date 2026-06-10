// Package grpcxtest provides an in-process SPIFFE certificate authority for
// exercising [github.com/xd1lab/harness-ai/internal/platform/grpcx] over real
// mutual TLS in unit tests (bufconn), without a running SPIRE agent.
//
// It mints short-lived X509-SVIDs for arbitrary SPIFFE IDs under a single test
// trust domain and builds the matching server/client *tls.Config values using
// the same go-spiffe tlsconfig helpers production uses, so tests authenticate
// and authorize exactly as deployments do. It is a test fake in the spirit of
// the other in-repo *test packages (clocktest/idstest/...); it is never imported
// by production code.
package grpcxtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
)

// CA is an in-memory SPIFFE certificate authority for tests. Build one with
// [NewCA]; it issues SVIDs valid for the given trust domain.
type CA struct {
	td     spiffeid.TrustDomain
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	bundle *x509bundle.Bundle
}

// NewCA creates a self-signed CA for the trust domain and returns it. It fails
// the test on any crypto error.
func NewCA(t *testing.T, td spiffeid.TrustDomain) *CA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "boltrope-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &CA{
		td:     td,
		caCert: caCert,
		caKey:  key,
		bundle: x509bundle.FromX509Authorities(td, []*x509.Certificate{caCert}),
	}
}

// Bundle returns the trust bundle (the CA root) as an x509bundle.Source, suitable
// for verifying peers in this trust domain.
func (c *CA) Bundle() *x509bundle.Bundle { return c.bundle }

// issueSVID mints a leaf X509-SVID for id signed by the CA and returns it as an
// x509svid.SVID (which is an x509svid.Source).
func (c *CA) issueSVID(t *testing.T, id spiffeid.ID) *x509svid.SVID {
	t.Helper()

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	uri, err := url.Parse(id.String())
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: id.Path()},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.caCert, &leafKey.PublicKey, c.caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &x509svid.SVID{
		ID:           id,
		Certificates: []*x509.Certificate{leaf},
		PrivateKey:   leafKey,
	}
}

// ServerTLSConfig returns a *tls.Config that presents an SVID for serverID and
// requires, verifies, and authorizes any client SVID in the trust domain
// (mTLS). Use it as the server side of an mTLS bufconn pair.
func (c *CA) ServerTLSConfig(t *testing.T, serverID spiffeid.ID) *tls.Config {
	t.Helper()
	svid := c.issueSVID(t, serverID)
	return tlsconfig.MTLSServerConfig(svid, c.bundle, tlsconfig.AuthorizeMemberOf(c.td))
}

// ClientTLSConfig returns a *tls.Config that presents an SVID for callerID and
// verifies + authorizes the server's SVID to be exactly serverID. Use it as the
// client side of an mTLS bufconn pair.
func (c *CA) ClientTLSConfig(t *testing.T, callerID, serverID spiffeid.ID) *tls.Config {
	t.Helper()
	svid := c.issueSVID(t, callerID)
	return tlsconfig.MTLSClientConfig(svid, c.bundle, tlsconfig.AuthorizeID(serverID))
}
