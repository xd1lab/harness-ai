// SPDX-License-Identifier: Apache-2.0

// Package auditsign provides the Ed25519 signer and verifier that anchor
// Boltrope's signed audit-checkpoint chain (Batch-5B, ADR-0034).
//
// # Why this exists
//
// Batch-5A made the event log tamper-EVIDENT: an in-DB hash-chain
// (events.content_hash / events.chain_hash) lets a verifier DETECT mutation —
// but an attacker holding DB write access can recompute every content/chain
// hash, leaving the chain internally consistent. This package adds an
// Ed25519 signature anchored by a key that lives OUTSIDE the events DB
// (resolved from the environment via [secret.SecretsPort]). A checkpoint hash
// computed over the events' content-hashes is signed; a rewritten event yields
// a different recomputed checkpoint hash whose stored signature no longer
// verifies. That makes the log tamper-PROOF, not merely tamper-evident.
//
// # Key handling (operator-tier, never logged)
//
// The private key is resolved from BOLTROPE_AUDIT_SIGNING_KEY (base64 of either
// a 32-byte Ed25519 seed OR a 64-byte ed25519.PrivateKey, validated by length
// after decode). A non-empty BOLTROPE_AUDIT_SIGNING_KEY_ID is REQUIRED when a
// key is present; construction fails loudly otherwise. The key is held only as a
// [secret.Secret]; it is revealed solely at the [ed25519.Sign] call site and is
// NEVER logged, exported, or persisted. The signer struct exposes only key_id +
// algo to logs.
//
// # Disabled-with-warning default
//
// When BOLTROPE_AUDIT_SIGNING_KEY resolves to [secret.ErrNotFound], [NewSigner]
// returns [ErrSigningDisabled] (a sentinel) so wiring attaches NO signer and
// emits one loud WARN. No ephemeral key is generated — an ephemeral key would
// silently fail to anchor checkpoints durably.
//
// # Trust boundary (ADR-0013 / ADR-0034)
//
// This package is OPERATOR-TIER infrastructure (like OTLP/metrics export). It
// imports only the standard library and [secret]; it MUST NOT import the egress
// broker, which governs MODEL-INFLUENCED channels only.
package auditsign

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"

	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// Environment variable / secret names resolved via [secret.SecretsPort].
const (
	// EnvSigningKey is the base64-encoded Ed25519 private material: either a
	// 32-byte seed or a 64-byte ed25519.PrivateKey (validated by length after
	// base64 decode). Resolving to [secret.ErrNotFound] disables signing.
	EnvSigningKey = "BOLTROPE_AUDIT_SIGNING_KEY"
	// EnvSigningKeyID is the key identifier stored alongside each checkpoint
	// signature; REQUIRED (non-empty) when a signing key is present.
	EnvSigningKeyID = "BOLTROPE_AUDIT_SIGNING_KEY_ID"
	// EnvPublicKey is the base64-encoded 32-byte Ed25519 public key for
	// verify-only deployments that hold no private key.
	EnvPublicKey = "BOLTROPE_AUDIT_PUBLIC_KEY"
)

// Algo is the signature algorithm constant stored on each checkpoint row.
const Algo = "ed25519"

// ErrSigningDisabled is the sentinel returned by [NewSigner] /
// [NewSignerWithLogger] when no BOLTROPE_AUDIT_SIGNING_KEY is configured. It is
// the safe default (disabled-with-warning); callers branch on it with
// [errors.Is] to attach no signer and emit a single loud WARN. It is NOT an
// error condition the operator must fix — only a notice that checkpoints are off.
var ErrSigningDisabled = errors.New("auditsign: signing disabled (no BOLTROPE_AUDIT_SIGNING_KEY configured)")

// Signer signs audit-checkpoint hashes with an Ed25519 private key. The private
// key is held as a [secret.Secret] and revealed only at the Sign call site; the
// struct exposes only key_id + algo (see [Signer.KeyID] / [Signer.Algo]) so it
// is safe to log.
type Signer struct {
	// priv holds the 64-byte ed25519.PrivateKey wrapped so it redacts itself if
	// ever logged; revealed only inside Sign.
	priv  secret.Secret
	pub   ed25519.PublicKey
	keyID string
}

// Verifier verifies audit-checkpoint signatures against a configured Ed25519
// public key. It needs no private material, so a verify-only deployment can
// construct one from BOLTROPE_AUDIT_PUBLIC_KEY alone.
type Verifier struct {
	pub   ed25519.PublicKey
	keyID string
}

// NewSigner resolves the signing key from secrets and returns a [Signer].
// It delegates to [NewSignerWithLogger] with a discarding logger.
func NewSigner(ctx context.Context, secrets secret.SecretsPort) (*Signer, error) {
	return NewSignerWithLogger(ctx, secrets, slog.New(slog.DiscardHandler))
}

// NewSignerWithLogger resolves the signing key and constructs a [Signer],
// logging only non-sensitive facts (key_id, algo) via logger. The private key
// is never logged.
//
// Behavior:
//   - BOLTROPE_AUDIT_SIGNING_KEY absent ([secret.ErrNotFound]) -> a single loud
//     WARN naming the env var and the tamper-EVIDENT-not-PROOF caveat, and
//     [ErrSigningDisabled] (the safe default; no ephemeral key generated).
//   - key present but base64 decodes to neither 32 nor 64 bytes -> loud error.
//   - key present but BOLTROPE_AUDIT_SIGNING_KEY_ID empty -> loud error.
func NewSignerWithLogger(ctx context.Context, secrets secret.SecretsPort, logger *slog.Logger) (*Signer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	keySec, err := secrets.Get(ctx, EnvSigningKey)
	if err != nil {
		if errors.Is(err, secret.ErrNotFound) {
			logger.Warn("audit checkpoint signing DISABLED: no BOLTROPE_AUDIT_SIGNING_KEY configured; the in-DB hash-chain is tamper-EVIDENT but not externally tamper-PROOF")
			return nil, ErrSigningDisabled
		}
		return nil, fmt.Errorf("auditsign: resolve %s: %w", EnvSigningKey, err)
	}

	priv, pub, err := decodePrivateKey(keySec)
	if err != nil {
		return nil, err
	}

	keyID, err := requireKeyID(ctx, secrets)
	if err != nil {
		return nil, err
	}

	logger.Info("audit checkpoint signing enabled", slog.String("key_id", keyID), slog.String("algo", Algo))

	// Hold the private key only as a redacting Secret; never as a plain field.
	return &Signer{
		priv:  secret.New(string(priv)),
		pub:   pub,
		keyID: keyID,
	}, nil
}

// NewVerifier resolves a public key for signature verification. It prefers the
// derived public key from a configured signing key, and falls back to
// BOLTROPE_AUDIT_PUBLIC_KEY for verify-only deployments. The key_id is resolved
// from BOLTROPE_AUDIT_SIGNING_KEY_ID and is required.
func NewVerifier(ctx context.Context, secrets secret.SecretsPort) (*Verifier, error) {
	keyID, err := requireKeyID(ctx, secrets)
	if err != nil {
		return nil, err
	}

	// Prefer deriving the public key from the private key when present.
	if keySec, err := secrets.Get(ctx, EnvSigningKey); err == nil {
		_, pub, derr := decodePrivateKey(keySec)
		if derr != nil {
			return nil, derr
		}
		return &Verifier{pub: pub, keyID: keyID}, nil
	} else if !errors.Is(err, secret.ErrNotFound) {
		return nil, fmt.Errorf("auditsign: resolve %s: %w", EnvSigningKey, err)
	}

	// Verify-only: resolve the public key directly.
	pubSec, err := secrets.Get(ctx, EnvPublicKey)
	if err != nil {
		if errors.Is(err, secret.ErrNotFound) {
			return nil, fmt.Errorf("auditsign: no %s and no %s configured: %w", EnvSigningKey, EnvPublicKey, ErrSigningDisabled)
		}
		return nil, fmt.Errorf("auditsign: resolve %s: %w", EnvPublicKey, err)
	}
	raw, err := base64.StdEncoding.DecodeString(pubSec.Reveal())
	if err != nil {
		return nil, fmt.Errorf("auditsign: %s is not valid base64: %w", EnvPublicKey, err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("auditsign: %s decodes to %d bytes, want %d (ed25519 public key)", EnvPublicKey, len(raw), ed25519.PublicKeySize)
	}
	return &Verifier{pub: ed25519.PublicKey(raw), keyID: keyID}, nil
}

// Sign signs the checkpoint hash with the Ed25519 private key and returns the
// signature plus the key_id. The private key is revealed only here.
func (s *Signer) Sign(checkpointHash []byte) (sig []byte, keyID string) {
	priv := ed25519.PrivateKey(s.priv.RevealBytes())
	return ed25519.Sign(priv, checkpointHash), s.keyID
}

// KeyID returns the configured key identifier (safe to log).
func (s *Signer) KeyID() string { return s.keyID }

// Algo returns the signature algorithm constant ("ed25519").
func (s *Signer) Algo() string { return Algo }

// PublicKey returns a copy of the derived Ed25519 public key.
func (s *Signer) PublicKey() ed25519.PublicKey {
	out := make(ed25519.PublicKey, len(s.pub))
	copy(out, s.pub)
	return out
}

// Verify reports whether sig is a valid Ed25519 signature over checkpointHash
// for the configured public key. keyID must match the Verifier's configured
// key_id; a mismatch returns false (the verifier holds exactly one key).
func (v *Verifier) Verify(checkpointHash, sig []byte, keyID string) bool {
	if keyID != v.keyID {
		return false
	}
	return ed25519.Verify(v.pub, checkpointHash, sig)
}

// KeyID returns the configured key identifier (safe to log).
func (v *Verifier) KeyID() string { return v.keyID }

// Algo returns the signature algorithm constant ("ed25519").
func (v *Verifier) Algo() string { return Algo }

// requireKeyID resolves BOLTROPE_AUDIT_SIGNING_KEY_ID and fails loudly if it is
// missing or empty (a signature with no key_id cannot be attributed/verified).
func requireKeyID(ctx context.Context, secrets secret.SecretsPort) (string, error) {
	idSec, err := secrets.Get(ctx, EnvSigningKeyID)
	if err != nil {
		if errors.Is(err, secret.ErrNotFound) {
			return "", fmt.Errorf("auditsign: %s is required when a signing key is present (no default key_id)", EnvSigningKeyID)
		}
		return "", fmt.Errorf("auditsign: resolve %s: %w", EnvSigningKeyID, err)
	}
	id := idSec.Reveal()
	if id == "" {
		return "", fmt.Errorf("auditsign: %s is required when a signing key is present (no default key_id)", EnvSigningKeyID)
	}
	return id, nil
}

// decodePrivateKey base64-decodes the signing-key secret and accepts EITHER a
// 32-byte seed or a 64-byte ed25519.PrivateKey, validated by decoded length. It
// returns the full 64-byte private key and its derived public key. Any other
// length is rejected loudly. The decoded bytes are never logged.
func decodePrivateKey(keySec secret.Secret) (priv ed25519.PrivateKey, pub ed25519.PublicKey, err error) {
	raw, derr := base64.StdEncoding.DecodeString(keySec.Reveal())
	if derr != nil {
		return nil, nil, fmt.Errorf("auditsign: %s is not valid base64: %w", EnvSigningKey, derr)
	}
	switch len(raw) {
	case ed25519.SeedSize: // 32-byte seed
		priv = ed25519.NewKeyFromSeed(raw)
	case ed25519.PrivateKeySize: // 64-byte full private key
		priv = ed25519.PrivateKey(raw)
	default:
		return nil, nil, fmt.Errorf("auditsign: %s decodes to %d bytes, want %d (seed) or %d (private key)", EnvSigningKey, len(raw), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
	pub = priv.Public().(ed25519.PublicKey)
	return priv, pub, nil
}
