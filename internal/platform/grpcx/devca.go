package grpcx

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha512"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"golang.org/x/crypto/hkdf"
)

// devCASeedEnv is the environment variable that supplies the shared seed from
// which the deterministic development CA is derived. Every process in a local
// `docker compose`/CI stack that sets the SAME value mints the SAME CA cert/key
// — and therefore the same trust bundle — so the static-cert dev fallback can
// complete cross-process mutual TLS (a server in one container trusts a client
// in another). When unset, [defaultDevCASeed] is used and a loud warning is
// logged, because an unset seed across hosts would still be deterministic but is
// a well-known constant and must never be mistaken for a secret.
const devCASeedEnv = "BOLTROPE_DEV_CA_SEED"

// defaultDevCASeed is the fixed fallback seed used when BOLTROPE_DEV_CA_SEED is
// unset. It is intentionally a fixed, well-known constant: the dev fallback is
// present in every build but inert unless BOLTROPE_DEV_INSECURE=1 is explicitly
// set, and a *stable* default is what lets a default `docker compose up`
// work cross-service with no extra configuration. It is NOT a secret and must
// never be used in production.
const defaultDevCASeed = "boltrope-dev-ca-seed/v1/DO-NOT-USE-IN-PRODUCTION"

// devCANotBefore / devCANotAfter bound the validity of the deterministic dev CA
// and the leaf SVIDs it signs. They are FIXED instants (not relative to
// time.Now) so the certificate fields are reproducible across independent
// processes, and the window is deliberately wide — years — so it always covers
// "now" in any dev/CI environment regardless of container clock skew. The dev
// fallback is present in every build but never engages unless
// BOLTROPE_DEV_INSECURE=1 is explicitly set (and it must never be set in
// production), so a long-lived dev cert is not a production concern.
func devCANotBefore() time.Time { return time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC) }
func devCANotAfter() time.Time  { return time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC) }

// resolveDevCASeed returns the seed bytes for the deterministic dev CA, reading
// BOLTROPE_DEV_CA_SEED via lookup (defaulting to [os.LookupEnv]). When the
// variable is unset or empty it returns [defaultDevCASeed] and logs a WARN-level
// warning via logger so an operator is never surprised that a well-known
// constant seed is in use. The boolean reports whether the default was used.
func resolveDevCASeed(lookup func(string) (string, bool), logger *slog.Logger) ([]byte, bool) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if logger == nil {
		logger = slog.Default()
	}
	if v, ok := lookup(devCASeedEnv); ok && v != "" {
		return []byte(v), false
	}
	logger.Warn(
		"INSECURE DEV MODE: " + devCASeedEnv + " is unset — deriving the shared dev CA from a FIXED, well-known default seed; " +
			"set " + devCASeedEnv + " to a per-stack value for any non-throwaway environment (never use the dev fallback in production)",
	)
	return []byte(defaultDevCASeed), true
}

// deterministicReader builds an io.Reader whose byte stream is a pure function
// of seed and label. It is implemented with HKDF-SHA-512 (RFC 5869): the seed is
// the input keying material and label is the HKDF info, so distinct labels under
// the same seed yield independent, non-overlapping streams. Reading the CA
// scalar from this stream (see [deterministicECDSAKey]) is what lets
// independently-constructed processes derive an identical CA key.
//
// HKDF's expand step can emit at most 255*HashLen bytes (255*64 = 16320 with
// SHA-512), far more than scalar derivation consumes via rejection sampling, so
// the reader never reaches its entropy limit in practice.
func deterministicReader(seed []byte, label string) io.Reader {
	// Salt is fixed (nil) on purpose: the seed is the only secret-ish input and
	// per-purpose separation is provided by the info/label, so the derivation
	// stays a pure function of (seed, label) and is reproducible across hosts.
	return hkdf.New(sha512.New, seed, nil, []byte(label))
}

// deterministicECDSAKey derives a P-256 ECDSA private key deterministically from
// seed and label. Two calls with identical (seed, label) always return the same
// key; differing labels return independent keys. This is the primitive the
// shared dev CA is built from.
//
// It does NOT use [ecdsa.GenerateKey]: since Go 1.20 that function reads a
// variable, randomized amount from its rand argument (extra-entropy masking), so
// a fixed reader does NOT yield a reproducible key. Instead the private scalar is
// read straight from the HKDF stream and validated via [ecdh.P256], which accepts
// exactly the canonical [1, N-1] range, with rejection sampling for the
// vanishingly rare out-of-range draw. The public point is computed by ecdh (no
// deprecated crypto/elliptic scalar-mult call), then bridged into the
// [ecdsa.PrivateKey] shape that x509 certificate signing requires.
func deterministicECDSAKey(seed []byte, label string) (*ecdsa.PrivateKey, error) {
	curve := ecdh.P256()
	r := deterministicReader(seed, label)
	// P-256 scalars are 32 bytes; the curve order is just under 2^256 so almost
	// every 32-byte draw is in range. The loop bounds the (astronomically
	// unlikely) infinite case defensively.
	for attempt := 0; attempt < 128; attempt++ {
		scalar := make([]byte, 32)
		if _, err := io.ReadFull(r, scalar); err != nil {
			return nil, fmt.Errorf("read deterministic scalar (label %q): %w", label, err)
		}
		ek, err := curve.NewPrivateKey(scalar)
		if err != nil {
			// Out of [1, N-1]; draw the next block from the same stream.
			continue
		}
		// ecdh public key bytes are the uncompressed SEC1 point 0x04||X||Y.
		pub := ek.PublicKey().Bytes()
		if len(pub) != 65 || pub[0] != 0x04 {
			return nil, fmt.Errorf("unexpected ecdh public key encoding (label %q)", label)
		}
		return &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     new(big.Int).SetBytes(pub[1:33]),
				Y:     new(big.Int).SetBytes(pub[33:65]),
			},
			D: new(big.Int).SetBytes(scalar),
		}, nil
	}
	return nil, fmt.Errorf("derive deterministic ECDSA key (label %q): exhausted rejection-sampling attempts", label)
}

// devCA is a SPIFFE certificate authority whose private key is derived
// deterministically from a shared seed. Every process that derives a devCA from
// the same seed obtains the SAME CA key — and therefore the same CA public key,
// which is the trust anchor — so SVID leaves signed by CAs in different processes
// validate against each other. That shared trust anchor is the property the
// cross-process dev mTLS path requires; the CA cert's own self-signature is
// randomized by crypto/x509 and need not match. Build one with [newDevCA].
type devCA struct {
	td     spiffeid.TrustDomain
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	bundle *x509bundle.Bundle
}

// newDevCA derives the shared development CA for trust domain td from seed. The
// CA key is HKDF-derived from seed and the self-signed certificate uses fixed,
// seed-independent template fields (constant serial and subject, fixed validity
// window) so the trust anchor — the CA public key plus identity — is identical
// for every process that shares the seed. The CA is scoped to the trust domain
// via the seed label so different trust domains do not collide.
func newDevCA(td spiffeid.TrustDomain, seed []byte) (*devCA, error) {
	key, err := deterministicECDSAKey(seed, "boltrope/dev-ca/"+td.String())
	if err != nil {
		return nil, fmt.Errorf("derive dev CA key: %w", err)
	}

	// Template fields are deterministic in the seed: a constant serial, subject,
	// fixed validity window, and the seed-derived key. The CA's *self-signature*
	// is still randomized by crypto/x509 (Go masks ECDSA signatures with extra
	// entropy), so the CA cert DER is not byte-identical across processes — but
	// that is irrelevant to cross-process trust: a peer verifies a leaf against
	// the CA's PUBLIC KEY, which IS identical here because the key is
	// deterministic. The shared public key is the trust anchor, not the CA cert's
	// own signature.
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "boltrope-dev-shared-ca"},
		NotBefore:             devCANotBefore(),
		NotAfter:              devCANotAfter(),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(deterministicReader(seed, "boltrope/dev-ca-sign/"+td.String()), tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create dev CA cert: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse dev CA cert: %w", err)
	}

	return &devCA{
		td:     td,
		cert:   cert,
		key:    key,
		bundle: x509bundle.FromX509Authorities(td, []*x509.Certificate{cert}),
	}, nil
}

// issueSVID mints a leaf X509-SVID for id, signed by the shared dev CA, and
// returns it together with the CA trust bundle. The leaf key is per-process
// random (each process presents its own freshly-minted leaf), but because the
// signing CA is the shared, seed-derived authority, a peer that derived the same
// CA from the same seed will trust the leaf. The returned bundle is the shared
// CA root, which is exactly the bundle handed to the go-spiffe tlsconfig
// MTLS{Server,Client}Config helpers so peers verify against the shared trust
// anchor.
//
// leafRand supplies randomness for the leaf key and signing; production passes
// crypto/rand.Reader. It is a parameter so tests can drive deterministic leaves
// when they need to assert byte-stable output, though determinism of the leaf is
// NOT required for the cross-process handshake (only the CA must match).
func (c *devCA) issueSVID(id spiffeid.ID, leafRand io.Reader) (*x509svid.SVID, *x509bundle.Bundle, error) {
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), leafRand)
	if err != nil {
		return nil, nil, fmt.Errorf("generate leaf key: %w", err)
	}
	uri, err := url.Parse(id.String())
	if err != nil {
		return nil, nil, fmt.Errorf("parse SPIFFE ID URI: %w", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: id.Path()},
		NotBefore:    devCANotBefore(),
		NotAfter:     devCANotAfter(),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	leafDER, err := x509.CreateCertificate(leafRand, leafTmpl, c.cert, &leafKey.PublicKey, c.key)
	if err != nil {
		return nil, nil, fmt.Errorf("create leaf SVID: %w", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return nil, nil, fmt.Errorf("parse leaf SVID: %w", err)
	}

	svid := &x509svid.SVID{
		ID:           id,
		Certificates: []*x509.Certificate{leaf},
		PrivateKey:   leafKey,
	}
	return svid, c.bundle, nil
}
