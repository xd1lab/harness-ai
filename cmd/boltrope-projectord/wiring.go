// Command boltrope-projectord is the read-side projection worker daemon (T-CMD-02;
// architecture §10.4). It runs the xmin-bounded safe-advance projection loop —
// cost rollup, projection-lag gauge, and the orphan-blob sweeper — as a
// background worker while serving gRPC health + HTTP /livez,/readyz,/metrics via
// the shared [github.com/xd1lab/harness-ai/internal/platform/daemon] harness.
// It never blocks an append: it reads the events table directly from a gap-safe
// cursor (architecture §10.4).
//
// The worker is resilient: it connects to PostgreSQL inside the run loop and
// retries on failure (with backoff) rather than failing the process, so a
// transient DB outage degrades projection (and readiness) without crashing
// projectord. The cursor read is authoritative; LISTEN/NOTIFY is only a wakeup
// hint, so the safety-net poll guarantees progress even without notifications.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	"github.com/xd1lab/harness-ai/internal/orchestrator/projection"
	"github.com/xd1lab/harness-ai/internal/platform/auditsign"
	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/daemon"
	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

const serviceName = "projectord"

// projectorSettings are the projectord-specific knobs read from the environment.
type projectorSettings struct {
	// Subscription is the event_subscriptions row this worker owns (horizontal
	// sharding is by subscription name; architecture §10.4). Defaults to
	// "cost-rollup".
	Subscription string
	// SweepInterval is how often the orphan-blob sweeper runs; zero disables it.
	SweepInterval time.Duration
	// TrustDomain is the SPIFFE trust domain for the health endpoint's mTLS.
	TrustDomain string
}

// loadProjectorSettings reads the projectord-specific environment.
func loadProjectorSettings() projectorSettings {
	return projectorSettings{
		Subscription:  envOr("BOLTROPE_PROJECTOR_SUBSCRIPTION", "cost-rollup"),
		SweepInterval: envDuration("BOLTROPE_PROJECTOR_SWEEP_INTERVAL", 5*time.Minute),
		TrustDomain:   envOr("BOLTROPE_TRUST_DOMAIN", "boltrope.local"),
	}
}

// auditSettings are the Batch-5B (ADR-0034) operator-tier knobs gating the signed
// audit-checkpoint signer and the SIEM exporter. Both run as SEPARATE projection
// Runners with their OWN subscription names (independent cursors), so a failing
// SIEM sink can never stall cost-rollup (AC-17). Each consumer is env-gated:
//   - the signer attaches only when BOLTROPE_AUDIT_SIGNING_KEY is configured (else
//     a single loud WARN at startup; no ephemeral key — AC-6);
//   - the SIEM exporter attaches only when a file or HTTP sink is configured.
//
// Secret resolution uses the bare env var names (NO prefix), matching the names the
// auditsign package and the SIEMExporter document — i.e. EnvSecrets is constructed
// without a prefix so BOLTROPE_AUDIT_SIGNING_KEY resolves to $BOLTROPE_AUDIT_SIGNING_KEY.
type auditSettings struct {
	// signerSubscription is the event_subscriptions row the audit-checkpoint signer
	// owns (default "audit-checkpoint"); independent of cost-rollup/siem-export.
	signerSubscription string
	// siemSubscription is the event_subscriptions row the SIEM exporter owns
	// (default "siem-export"); independent of cost-rollup/audit-checkpoint.
	siemSubscription string
	// checkpointEvery is the leaf-count checkpoint boundary N
	// (BOLTROPE_AUDIT_CHECKPOINT_EVERY); <=0 lets the signer use its default.
	checkpointEvery int

	// signingKey is the raw BOLTROPE_AUDIT_SIGNING_KEY value (presence gates the
	// signer). It is read only to test presence here; the auditsign signer resolves
	// and holds the key material as a redacting secret — this field is the bare
	// gating flag and is NEVER logged.
	signingKey string
	// siemFile / siemHTTPURL are the SIEM sink targets (presence gates the exporter).
	siemFile    string
	siemHTTPURL string
}

// loadAuditSettings reads the Batch-5B audit-checkpoint + SIEM environment. It does
// NOT resolve the private key here (only its presence, to gate wiring); the signer
// itself resolves and holds the key material via [secret.SecretsPort] (AC-5/AC-16).
func loadAuditSettings() auditSettings {
	return auditSettings{
		signerSubscription: envOr("BOLTROPE_AUDIT_SUBSCRIPTION", "audit-checkpoint"),
		siemSubscription:   envOr("BOLTROPE_SIEM_SUBSCRIPTION", "siem-export"),
		checkpointEvery:    envInt("BOLTROPE_AUDIT_CHECKPOINT_EVERY", 0),
		signingKey:         os.Getenv(auditsign.EnvSigningKey),
		siemFile:           os.Getenv("BOLTROPE_SIEM_FILE"),
		siemHTTPURL:        os.Getenv("BOLTROPE_SIEM_HTTP_URL"),
	}
}

// SignerEnabled reports whether the audit-checkpoint signer should be wired: it is
// gated solely on BOLTROPE_AUDIT_SIGNING_KEY being present (AC-6). When false the
// signer is NOT attached and projectord emits one loud WARN at startup.
func (a auditSettings) SignerEnabled() bool { return a.signingKey != "" }

// SIEMEnabled reports whether the SIEM exporter should be wired: it is gated on
// EITHER the file sink OR the HTTP sink being configured (AC-13/AC-17).
func (a auditSettings) SIEMEnabled() bool { return a.siemFile != "" || a.siemHTTPURL != "" }

// SignerSubscription returns the signer's independent subscription name.
func (a auditSettings) SignerSubscription() string { return a.signerSubscription }

// SIEMSubscription returns the SIEM exporter's independent subscription name.
func (a auditSettings) SIEMSubscription() string { return a.siemSubscription }

// envOr returns the value of env var key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDuration parses env var key as a Go duration, falling back to def on
// absence or a parse error.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// envInt parses env var key as a base-10 integer, falling back to def on absence or
// a parse error.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// connectRetryDelay is the backoff between projection-DB connection attempts. The
// worker keeps retrying (until shutdown) so a transient outage never kills the
// process (architecture §10.4).
const connectRetryDelay = 2 * time.Second

// Run wires projectord and serves it until ctx is cancelled or a signal arrives.
// The projection loop runs as the daemon's background worker; gRPC health + HTTP
// health/metrics are served alongside it. logw is the log sink.
func Run(ctx context.Context, cfg *config.Config, logw io.Writer) error {
	tel, err := daemon.SetupTelemetry(ctx, serviceName, version, cfg, logw)
	if err != nil {
		return err
	}

	ps := loadProjectorSettings()
	as := loadAuditSettings()

	// Emit the single loud startup WARN when audit-checkpoint signing is disabled
	// (no BOLTROPE_AUDIT_SIGNING_KEY): the in-DB hash-chain stays tamper-EVIDENT but
	// is not externally tamper-PROOF (AC-6). No ephemeral key is generated.
	if !as.SignerEnabled() {
		tel.Logger.Warn("audit checkpoint signing DISABLED: no BOLTROPE_AUDIT_SIGNING_KEY configured; the in-DB hash-chain is tamper-EVIDENT but not externally tamper-PROOF")
	}

	metrics, err := projection.NewOTelMetrics(tel.Metrics)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return fmt.Errorf("projectord: build metrics: %w", err)
	}

	credsCfg, err := serverCredsConfig(ps.TrustDomain, cfg, tel)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	creds, err := daemon.ServerCredentials(credsCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	// readyConn is shared between the readiness probe (a ping) and is reset by the
	// worker as it connects/reconnects; a non-nil value means the DB is reachable.
	w := &worker{
		dsn:      cfg.Postgres.DSN,
		blobDir:  cfg.Blob.Dir,
		settings: ps,
		audit:    as,
		secrets:  secret.NewEnvSecrets(),
		metrics:  metrics,
		log:      tel.Logger,
	}

	return daemon.Run(ctx, daemon.RunInput{
		GRPCAddr:    cfg.Server.GRPCAddr,
		HTTPAddr:    cfg.Server.HTTPAddr,
		Creds:       creds,
		Policy:      projectorRBAC(ps.TrustDomain),
		Telemetry:   tel,
		HasIdentity: func() bool { return daemon.HasServerIdentity(credsCfg) },
		Service: daemon.Service{
			Register:        func(srv *grpc.Server) { _ = srv }, // health only; no app RPCs
			ReadinessChecks: []daemon.ReadinessCheck{w.readiness()},
			Background:      w.run,
			Closers:         []func() error{func() error { _ = tel.Shutdown(ctx); return nil }},
		},
	})
}

// worker owns the projection loop's lifecycle: connecting (with retry), building
// the source/runner/sweeper, and running until the context is cancelled.
type worker struct {
	dsn      string
	blobDir  string
	settings projectorSettings
	audit    auditSettings
	secrets  secret.SecretsPort
	metrics  projection.MetricSink
	log      *slog.Logger
}

// run is the daemon background worker. It fans out into INDEPENDENT projection
// loops, one per subscription, each with its OWN pgx.Conn (a single pgx.Conn is not
// concurrency-safe, and one Runner owns one subscription/cursor — architecture
// §10.4):
//
//   - cost-rollup (always): the cost projector + lag gauge + orphan-blob sweeper;
//   - audit-checkpoint (only when BOLTROPE_AUDIT_SIGNING_KEY is set — AC-6/AC-17):
//     the Ed25519 signed-checkpoint signer;
//   - siem-export (only when BOLTROPE_SIEM_FILE or BOLTROPE_SIEM_HTTP_URL is set —
//     AC-13/AC-17): the descriptors+hashes-only SIEM exporter.
//
// Each loop is independently resilient (its own reconnect/backoff), so a failing
// SIEM sink or a signer DB error degrades only its own subscription and can NEVER
// stall cost-rollup's cursor (AC-17). run returns ctx.Err() once every loop has
// observed cancellation (a clean shutdown).
func (w *worker) run(ctx context.Context) error {
	var wg sync.WaitGroup

	loops := []struct {
		name string
		fn   func(context.Context) error
	}{
		{name: w.settings.Subscription, fn: w.runCostOnce},
	}
	if w.audit.SignerEnabled() {
		loops = append(loops, struct {
			name string
			fn   func(context.Context) error
		}{name: w.audit.SignerSubscription(), fn: w.runSignerOnce})
	}
	if w.audit.SIEMEnabled() {
		loops = append(loops, struct {
			name string
			fn   func(context.Context) error
		}{name: w.audit.SIEMSubscription(), fn: w.runSIEMOnce})
	}

	for _, lp := range loops {
		wg.Add(1)
		go func(name string, fn func(context.Context) error) {
			defer wg.Done()
			w.resilientLoop(ctx, name, fn)
		}(lp.name, lp.fn)
	}

	wg.Wait()
	return ctx.Err()
}

// resilientLoop runs fn (a single-pass projection runner over one subscription)
// until ctx is cancelled, reconnecting with backoff on any non-cancel failure so a
// transient DB outage degrades that subscription without crashing projectord
// (architecture §10.4). A failure in one loop never touches another loop's cursor.
func (w *worker) resilientLoop(ctx context.Context, subscription string, fn func(context.Context) error) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("projectord: projection loop ended; retrying",
				slog.String("subscription", subscription), slog.Any("error", err))
			if !sleepCtx(ctx, connectRetryDelay) {
				return
			}
			continue
		}
		// fn returned without error only because ctx was cancelled.
		return
	}
}

// runCostOnce opens a fresh connection, builds the cost-rollup source/runner (cost
// projector + lag gauge + orphan-blob sweeper), and runs the loop until it returns
// (on ctx cancel or a read error the runner could not absorb). The connection is
// closed before returning so a reconnect starts clean.
func (w *worker) runCostOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	src := projection.NewSource(conn)

	opts := []projection.RunnerOption{
		projection.WithMetrics(w.metrics),
		projection.WithLogger(w.log),
		// Persist the per-turn cost rollup into session_cost_events as the worker
		// tails the feed (Feature O / cost-read; ADR-0026). The projector shares the
		// worker's operator-tier connection: it reads the GLOBAL feed (via the source)
		// and writes the per-tenant cost rows, scoping each write to the source row's
		// tenant. Writes are idempotent over the xmin cursor (ON CONFLICT global_id),
		// so a crash re-read never double-counts; the GetSessionCost/GetTenantCost read
		// path serves this same table, RLS-scoped.
		projection.WithCostProjector(projection.NewCostProjector(conn)),
	}
	cfg := projection.Config{Subscription: w.settings.Subscription}
	if w.settings.SweepInterval > 0 {
		cfg.SweepInterval = w.settings.SweepInterval
		store := blob.NewFSStore(w.blobDir, 0)
		opts = append(opts, projection.WithSweeper(projection.NewSweeper(conn, store)))
	}

	runner := projection.NewRunner(cfg, src, opts...)
	// No LISTEN/NOTIFY waker in v1 wiring; the safety-net poll drives catch-up
	// (the cursor read is authoritative; architecture §10.4).
	return runner.Run(ctx, nil)
}

// runSignerOnce opens a DEDICATED connection (a single pgx.Conn is not
// concurrency-safe, so the signer must NOT share the cost-rollup connection) and
// runs the audit-checkpoint signer on its OWN subscription (default
// "audit-checkpoint"), so its cursor is independent of cost-rollup/siem-export
// (AC-17). It resolves the Ed25519 signing key via the operator-tier secrets port;
// on [auditsign.ErrSigningDisabled] it attaches NO signer and returns nil (the
// loop ends cleanly — the startup WARN already announced the disabled state). A key
// that is set-but-malformed is a hard, retried error (the operator must fix it).
func (w *worker) runSignerOnce(ctx context.Context) error {
	signer, err := auditsign.NewSignerWithLogger(ctx, w.secrets, w.log)
	if err != nil {
		if errors.Is(err, auditsign.ErrSigningDisabled) {
			// Defensive: SignerEnabled() gates this loop, so this is only reached if
			// the key vanished between the startup check and connect. End cleanly.
			return nil
		}
		return fmt.Errorf("build audit signer: %w", err)
	}

	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	src := projection.NewSource(conn)
	cfg := projection.Config{Subscription: w.audit.SignerSubscription()}
	runner := projection.NewRunner(cfg, src,
		projection.WithLogger(w.log),
		projection.WithAuditSigner(projection.NewAuditSigner(conn, signer, w.audit.checkpointEvery)),
	)
	return runner.Run(ctx, nil)
}

// runSIEMOnce opens a DEDICATED connection and runs the SIEM exporter on its OWN
// subscription (default "siem-export"), so a failing SIEM sink stalls only its own
// subscription and can never block cost-rollup (AC-13/AC-17). The optional bearer
// token is resolved via the operator-tier secrets port (BOLTROPE_SIEM_HTTP_BEARER)
// so it redacts in logs; the exporter uses a plain net/http client — NOT the egress
// broker (operator-tier; AC-14).
func (w *worker) runSIEMOnce(ctx context.Context) error {
	bearer := ""
	if sec, err := w.secrets.Get(ctx, "BOLTROPE_SIEM_HTTP_BEARER"); err == nil {
		bearer = sec.Reveal()
	}

	exporter, err := projection.NewSIEMExporter(projection.SIEMConfig{
		FilePath:   w.audit.siemFile,
		HTTPURL:    w.audit.siemHTTPURL,
		HTTPBearer: bearer,
	})
	if err != nil {
		return fmt.Errorf("build SIEM exporter: %w", err)
	}

	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	src := projection.NewSource(conn)
	cfg := projection.Config{Subscription: w.audit.SIEMSubscription()}
	runner := projection.NewRunner(cfg, src,
		projection.WithLogger(w.log),
		projection.WithSIEMExporter(exporter),
	)
	return runner.Run(ctx, nil)
}

// readiness builds the /readyz check gating on PostgreSQL reachability: it opens
// a short-lived connection and pings. projectord is "ready" when it can reach the
// feed it projects (FR-OBS-05).
func (w *worker) readiness() daemon.ReadinessCheck {
	return daemon.ReadinessCheck{
		Name: "postgres",
		Probe: func(ctx context.Context) error {
			conn, err := pgx.Connect(ctx, w.dsn)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close(context.Background()) }()
			return conn.Ping(ctx)
		},
	}
}

// sleepCtx sleeps for d or until ctx is cancelled, returning true if the full
// duration elapsed and false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// serverCredsConfig assembles the [daemon.CredsConfig] for this service.
func serverCredsConfig(trustDomain string, cfg *config.Config, tel *daemon.Telemetry) (daemon.CredsConfig, error) {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("projectord: invalid trust domain %q: %w", trustDomain, err)
	}
	id, err := spiffeid.FromSegments(td, serviceName)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("projectord: build server SPIFFE id: %w", err)
	}
	return daemon.CredsConfig{
		TrustDomain: td,
		ServerID:    id,
		DevInsecure: cfg.DevInsecure,
		Source:      spiffeSource(),
		Logger:      tel.Logger,
	}, nil
}
