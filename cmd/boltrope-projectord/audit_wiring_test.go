package main

// RED (test-first) wiring tests for Batch-5B's env-gated signer + SIEM exporter
// in projectord (AC-6, AC-17). Authored BEFORE the implementation; they
// reference symbols that do NOT exist yet — loadAuditSettings, the auditSettings
// struct fields, the env var names — so the package does NOT compile. That
// absence is the RED proof.
//
// Pinned (SPEC AC-6/AC-17):
//   - WithAuditSigner attaches only when BOLTROPE_AUDIT_SIGNING_KEY is set;
//     otherwise a loud WARN is emitted and the signer is NOT run.
//   - WithSIEMExporter attaches only when BOLTROPE_SIEM_FILE or
//     BOLTROPE_SIEM_HTTP_URL is set.
//   - the signer + exporter run as SEPARATE Runners with their OWN subscription
//     names (independent cursors), never sharing the cost-rollup cursor.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLoadAuditSettings_SignerGatedByKey asserts the signer is considered enabled
// only when the signing key env var is present.
func TestLoadAuditSettings_SignerGatedByKey(t *testing.T) {
	t.Run("disabled when key unset", func(t *testing.T) {
		// ensure unset
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "")
		as := loadAuditSettings()
		assert.False(t, as.SignerEnabled(), "signer must be disabled when BOLTROPE_AUDIT_SIGNING_KEY is unset")
	})

	t.Run("enabled when key set", func(t *testing.T) {
		t.Setenv("BOLTROPE_AUDIT_SIGNING_KEY", "c29tZS1zZWVk")
		as := loadAuditSettings()
		assert.True(t, as.SignerEnabled(), "signer must be enabled when the signing key is configured")
	})
}

// TestLoadAuditSettings_SIEMGatedBySink asserts the SIEM exporter is enabled when
// EITHER the file sink OR the http sink env var is set.
func TestLoadAuditSettings_SIEMGatedBySink(t *testing.T) {
	t.Run("disabled with no sink", func(t *testing.T) {
		t.Setenv("BOLTROPE_SIEM_FILE", "")
		t.Setenv("BOLTROPE_SIEM_HTTP_URL", "")
		as := loadAuditSettings()
		assert.False(t, as.SIEMEnabled(), "SIEM must be disabled with neither sink configured")
	})

	t.Run("enabled with file sink", func(t *testing.T) {
		t.Setenv("BOLTROPE_SIEM_FILE", "/tmp/siem.ndjson")
		t.Setenv("BOLTROPE_SIEM_HTTP_URL", "")
		as := loadAuditSettings()
		assert.True(t, as.SIEMEnabled(), "SIEM must be enabled when the file sink is set")
	})

	t.Run("enabled with http sink", func(t *testing.T) {
		t.Setenv("BOLTROPE_SIEM_FILE", "")
		t.Setenv("BOLTROPE_SIEM_HTTP_URL", "https://siem.example/ingest")
		as := loadAuditSettings()
		assert.True(t, as.SIEMEnabled(), "SIEM must be enabled when the http sink is set")
	})
}

// TestAuditSettings_IndependentSubscriptions asserts the signer and exporter use
// their own subscription names, distinct from the cost-rollup default — so their
// cursors are independent and a failing SIEM sink cannot stall cost-rollup.
func TestAuditSettings_IndependentSubscriptions(t *testing.T) {
	as := loadAuditSettings()
	cost := loadProjectorSettings().Subscription
	assert.NotEqual(t, cost, as.SignerSubscription(), "signer subscription must differ from cost-rollup")
	assert.NotEqual(t, cost, as.SIEMSubscription(), "SIEM subscription must differ from cost-rollup")
	assert.NotEqual(t, as.SignerSubscription(), as.SIEMSubscription(), "signer and SIEM subscriptions must differ from each other")
}
