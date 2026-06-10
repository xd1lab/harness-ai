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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	"github.com/xd1lab/harness-ai/internal/orchestrator/projection"
	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/daemon"
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
	metrics  projection.MetricSink
	log      *slog.Logger
}

// run is the daemon background worker: it (re)connects to PostgreSQL and runs the
// projection loop, retrying on any connection/loop failure until ctx is
// cancelled, at which point it returns ctx.Err() (a clean shutdown). A transient
// DB outage therefore degrades projection without crashing the process
// (architecture §10.4).
func (w *worker) run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("projectord: projection loop ended; retrying", slog.Any("error", err))
			if !sleepCtx(ctx, connectRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		// runOnce returned without error only because ctx was cancelled.
		return ctx.Err()
	}
}

// runOnce opens a fresh connection, builds the source/runner, and runs the loop
// until it returns (on ctx cancel or a read error the runner could not absorb).
// The connection is closed before returning so a reconnect starts clean.
func (w *worker) runOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	src := projection.NewSource(conn)

	opts := []projection.RunnerOption{
		projection.WithMetrics(w.metrics),
		projection.WithLogger(w.log),
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
