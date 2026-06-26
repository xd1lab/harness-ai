// SPDX-License-Identifier: Apache-2.0

package auditsign

// RED (test-first) unit tests for Batch-5B's Ed25519 audit-checkpoint signer +
// verifier (AC-5, AC-6, AC-16). Authored BEFORE the implementation; they
// reference symbols that do NOT exist yet — NewSigner, NewVerifier, Signer,
// Verifier, ErrSigningDisabled, the env var names — so the package does NOT
// compile. That absence is the RED proof.
//
// Pinned (SPEC AC-5/AC-6/AC-16):
//   - key resolved via secret.SecretsPort from BOLTROPE_AUDIT_SIGNING_KEY:
//     base64-encoded Ed25519 seed (32 bytes) OR full private key (64 bytes),
//     validated by length after base64 decode.
//   - BOLTROPE_AUDIT_SIGNING_KEY_ID is REQUIRED when a key is present; signer
//     construction fails loudly if the key is set but key_id is empty.
//   - algo constant "ed25519"; Sign(hash) -> (sig, keyID); Verify(hash, sig,
//     keyID) bool; public key derived from the private key (and also resolvable
//     from BOLTROPE_AUDIT_PUBLIC_KEY for verify-only deployments).
//   - DISABLED-WITH-WARNING is the safe default: when the signing key resolves
//     to secret.ErrNotFound, NewSigner returns ErrSigningDisabled (no ephemeral
//     key is generated).
//   - KEY SECRECY: the private key never appears in logs (slog capture) nor in
//     anything the signer exposes other than via the signature.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// mapSecrets is a tiny in-memory secret.SecretsPort for the unit tests: a name
// present in the map resolves to its value; an absent name is secret.ErrNotFound.
type mapSecrets struct{ m map[string]string }

func (s mapSecrets) Get(_ context.Context, name string) (secret.Secret, error) {
	v, ok := s.m[name]
	if !ok || v == "" {
		return secret.Secret{}, errors.Join(secret.ErrNotFound)
	}
	return secret.New(v), nil
}

var _ secret.SecretsPort = mapSecrets{}

// genSeed returns a fresh Ed25519 seed (32 bytes) base64-encoded plus its public
// key, for the happy-path tests.
func genSeed(t *testing.T) (seedB64 string, pub ed25519.PublicKey, priv ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	seed := priv.Seed() // 32 bytes
	return base64.StdEncoding.EncodeToString(seed), pub, priv
}

// TestNewSigner_DisabledWhenNoKey (AC-6): an absent BOLTROPE_AUDIT_SIGNING_KEY
// yields ErrSigningDisabled — NOT an ephemeral key, NOT a crash.
func TestNewSigner_DisabledWhenNoKey(t *testing.T) {
	_, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{}})
	if !errors.Is(err, ErrSigningDisabled) {
		t.Fatalf("NewSigner with no key = %v, want ErrSigningDisabled (disabled-with-warning default)", err)
	}
}

// TestNewSigner_RequiresKeyID (AC-5): a key present but no key_id is a loud
// construction failure (NOT a silent default key id).
func TestNewSigner_RequiresKeyID(t *testing.T) {
	seedB64, _, _ := genSeed(t)
	_, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey: seedB64,
		// EnvSigningKeyID deliberately omitted.
	}})
	if err == nil || errors.Is(err, ErrSigningDisabled) {
		t.Fatalf("NewSigner with key but no key_id = %v, want a loud non-disabled error", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "key_id") && !strings.Contains(strings.ToLower(err.Error()), "key id") {
		t.Fatalf("error %q should mention the missing key_id requirement", err.Error())
	}
}

// TestNewSigner_AcceptsSeedAndSigns (AC-5): a 32-byte base64 seed + key_id
// constructs a signer whose Sign produces a verifiable Ed25519 signature, and
// the reported key_id/algo are exposed.
func TestNewSigner_AcceptsSeedAndSigns(t *testing.T) {
	seedB64, pub, _ := genSeed(t)
	s, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey:   seedB64,
		EnvSigningKeyID: "k1",
	}})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if s.KeyID() != "k1" {
		t.Fatalf("KeyID() = %q, want k1", s.KeyID())
	}
	if s.Algo() != "ed25519" {
		t.Fatalf("Algo() = %q, want ed25519", s.Algo())
	}

	hash := sha256.Sum256([]byte("checkpoint-hash"))
	sig, keyID := s.Sign(hash[:])
	if keyID != "k1" {
		t.Fatalf("Sign keyID = %q, want k1", keyID)
	}
	if !ed25519.Verify(pub, hash[:], sig) {
		t.Fatal("signature does not verify against the derived public key (Sign is not signing the checkpoint hash)")
	}
}

// TestNewSigner_AcceptsFullPrivateKey (AC-5): a 64-byte base64 ed25519
// private key is also accepted (validated by decoded length).
func TestNewSigner_AcceptsFullPrivateKey(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	full := base64.StdEncoding.EncodeToString(priv) // 64 bytes
	s, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey:   full,
		EnvSigningKeyID: "k2",
	}})
	if err != nil {
		t.Fatalf("NewSigner with 64-byte private key: %v", err)
	}
	hash := sha256.Sum256([]byte("x"))
	sig, _ := s.Sign(hash[:])
	if !ed25519.Verify(pub, hash[:], sig) {
		t.Fatal("64-byte-private-key signer did not produce a verifiable signature")
	}
}

// TestNewSigner_RejectsBadLength (AC-5): a base64 value that decodes to neither
// 32 nor 64 bytes is rejected loudly.
func TestNewSigner_RejectsBadLength(t *testing.T) {
	bad := base64.StdEncoding.EncodeToString([]byte("too-short"))
	_, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey:   bad,
		EnvSigningKeyID: "k3",
	}})
	if err == nil || errors.Is(err, ErrSigningDisabled) {
		t.Fatalf("NewSigner with wrong key length = %v, want a loud validation error", err)
	}
}

// TestVerifier_FromDerivedPublicKey (AC-5): a Verifier built from the same
// signing key verifies a good signature and rejects a tampered hash.
func TestVerifier_FromDerivedPublicKey(t *testing.T) {
	seedB64, _, _ := genSeed(t)
	secrets := mapSecrets{m: map[string]string{
		EnvSigningKey:   seedB64,
		EnvSigningKeyID: "k1",
	}}
	s, err := NewSigner(context.Background(), secrets)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	v, err := NewVerifier(context.Background(), secrets)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	hash := sha256.Sum256([]byte("cp"))
	sig, keyID := s.Sign(hash[:])
	if !v.Verify(hash[:], sig, keyID) {
		t.Fatal("Verifier rejected a valid signature over the checkpoint hash")
	}

	// A different hash must not verify against the same signature.
	other := sha256.Sum256([]byte("cp-tampered"))
	if v.Verify(other[:], sig, keyID) {
		t.Fatal("Verifier accepted a signature over a DIFFERENT hash (forgery/tamper would pass)")
	}
}

// TestVerifier_FromPublicKeyOnly (AC-5): a verify-only deployment resolves
// BOLTROPE_AUDIT_PUBLIC_KEY (no private key) and still verifies signatures from
// the matching signer.
func TestVerifier_FromPublicKeyOnly(t *testing.T) {
	seedB64, pub, _ := genSeed(t)
	signer, err := NewSigner(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey:   seedB64,
		EnvSigningKeyID: "k1",
	}})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	verifyOnly := mapSecrets{m: map[string]string{
		EnvPublicKey:    base64.StdEncoding.EncodeToString(pub),
		EnvSigningKeyID: "k1",
	}}
	v, err := NewVerifier(context.Background(), verifyOnly)
	if err != nil {
		t.Fatalf("NewVerifier (public-key-only): %v", err)
	}

	hash := sha256.Sum256([]byte("cp"))
	sig, keyID := signer.Sign(hash[:])
	if !v.Verify(hash[:], sig, keyID) {
		t.Fatal("public-key-only Verifier rejected a valid signature")
	}
}

// TestSigner_PrivateKeyNeverLogged (AC-16 key secrecy): nothing the signer logs
// at construction contains the private key bytes (base64 or raw). We capture
// slog output and assert the seed/private bytes are absent; only key_id + algo
// may appear.
func TestSigner_PrivateKeyNeverLogged(t *testing.T) {
	seedB64, _, priv := genSeed(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// NewSignerWithLogger threads a logger so the loud-warning/info path is
	// captured; the signer logs only key_id + algo, never the key.
	_, err := NewSignerWithLogger(context.Background(), mapSecrets{m: map[string]string{
		EnvSigningKey:   seedB64,
		EnvSigningKeyID: "k1",
	}}, logger)
	if err != nil {
		t.Fatalf("NewSignerWithLogger: %v", err)
	}

	out := buf.String()
	if strings.Contains(out, seedB64) {
		t.Fatal("signer logged the base64-encoded private key (KEY SECRECY violated)")
	}
	rawSeedB64 := base64.StdEncoding.EncodeToString(priv.Seed())
	if strings.Contains(out, rawSeedB64) {
		t.Fatal("signer logged the raw seed bytes (KEY SECRECY violated)")
	}
	if strings.Contains(out, base64.StdEncoding.EncodeToString(priv)) {
		t.Fatal("signer logged the full private key (KEY SECRECY violated)")
	}
}

// TestDisabledSigner_LogsLoudWarning (AC-6): the disabled path emits a single
// loud WARN naming BOLTROPE_AUDIT_SIGNING_KEY and the tamper-EVIDENT-not-PROOF
// caveat, so an operator sees that checkpoints are off.
func TestDisabledSigner_LogsLoudWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	_, err := NewSignerWithLogger(context.Background(), mapSecrets{m: map[string]string{}}, logger)
	if !errors.Is(err, ErrSigningDisabled) {
		t.Fatalf("disabled NewSignerWithLogger = %v, want ErrSigningDisabled", err)
	}
	out := buf.String()
	if !strings.Contains(out, "BOLTROPE_AUDIT_SIGNING_KEY") {
		t.Fatalf("disabled warning %q should name the missing env var", out)
	}
	if !strings.Contains(strings.ToUpper(out), "WARN") {
		t.Fatalf("disabled message %q should be a WARN", out)
	}
}
