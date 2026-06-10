package projection

import (
	"context"
	"log/slog"
	"time"
)

// MetricSink is the metric sink the [Runner] publishes to: the projection
// lag gauge (USE saturation) and the running cost counter (both FR-OBS-02 /
// FR-OBS-01). It is a consumer-defined interface so the runner is unit-testable
// with a fake sink and the OTel/obs dependency stays at the wiring edge
// ([NewOTelMetrics]). A nil sink is tolerated by the runner ([Runner] no-ops the
// publish), so a minimal deployment can run without metrics.
type MetricSink interface {
	// SetProjectionLag publishes the current projection lag in unprocessed,
	// settled-below-xmin events (the USE gauge; FR-OBS-02).
	SetProjectionLag(events int64)
	// AddCost adds a folded per-batch cost delta (USD) to the running cost
	// counter, attributed to a tenant (the cost metric; FR-OBS-01/§11.6). The
	// counter is monotonic, so the runner passes the batch delta, never the total.
	AddCost(ctx context.Context, tenantID string, deltaUSD float64)
}

// Config parameterizes a [Runner].
type Config struct {
	// Subscription is the event_subscriptions row name this worker owns (e.g.
	// "cost-rollup"). Horizontal sharding is by subscription name (architecture
	// §10.4). Required.
	Subscription string
	// BatchSize caps rows read per [Source.FetchBatch]; the worker drains
	// repeatedly until a poll returns fewer than this (a short read = caught up).
	// Defaults to 500 when zero.
	BatchSize int
	// PollInterval is the safety-net poll period: the worker catches up on every
	// wakeup AND at least this often, so a missed/coalesced NOTIFY never strands an
	// event (LISTEN/NOTIFY is only a hint; the cursor read is authoritative;
	// architecture §10.4). Defaults to 1s when zero.
	PollInterval time.Duration
	// SweepInterval is how often the orphan-blob sweeper runs. Zero disables the
	// sweeper (e.g. for a cost-only subscription). The sweep is far less frequent
	// than the poll.
	SweepInterval time.Duration
}

// withDefaults returns a copy of cfg with zero fields filled by the documented
// defaults.
func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	return c
}

// Runner is the read-side projection worker: it consumes the GLOBAL event feed
// from a gap-safe, xmin-bounded cursor, folds the per-(tenant, session) cost
// rollup, publishes the lag and cost metrics, and runs the orphan-blob sweeper —
// all without ever blocking an append (architecture §10.4). One Runner owns one
// subscription; run it in its own goroutine via [Runner.Run].
//
// The cost rollup is held in memory as the running accumulator; the durable read
// model is the cursor in event_subscriptions plus the metrics it emits. On
// restart the worker resumes from the saved cursor and re-folds forward (the
// accumulator is rebuilt from cursor-onward, so process-restart cost totals are
// the forward sum from the resume point — the durable, monotonic record is the
// cost counter, which a metrics backend persists; architecture §10.4).
type Runner struct {
	cfg     Config
	src     *Source
	sweeper *Sweeper // nil when SweepInterval == 0
	metrics MetricSink
	log     *slog.Logger
	now     func() time.Time

	// totals is the in-memory cost rollup accumulator, keyed by (tenant, session).
	totals map[SessionKey]*CostTotals
	// cursor is the worker's current position; persisted via the source after each
	// folded batch.
	cursor Cursor
}

// RunnerOption configures a [Runner].
type RunnerOption func(*Runner)

// WithSweeper attaches an orphan-blob [Sweeper] the runner invokes on
// [Config.SweepInterval]. Without it (or with SweepInterval == 0) the sweeper is
// disabled.
func WithSweeper(sw *Sweeper) RunnerOption { return func(r *Runner) { r.sweeper = sw } }

// WithMetrics sets the metric sink ([MetricSink]). When unset the runner
// still functions; metric publishes are no-ops.
func WithMetrics(m MetricSink) RunnerOption { return func(r *Runner) { r.metrics = m } }

// WithLogger sets the structured logger. When unset a discarding logger is used.
func WithLogger(l *slog.Logger) RunnerOption {
	return func(r *Runner) {
		if l != nil {
			r.log = l
		}
	}
}

// WithNow overrides the runner's clock (used to date the sweeper's grace cutoff);
// the default is time.Now. It lets a deployment or test supply a controllable
// time source without exposing the field.
func WithNow(now func() time.Time) RunnerOption {
	return func(r *Runner) {
		if now != nil {
			r.now = now
		}
	}
}

// NewRunner constructs a [Runner] for cfg over src. It panics if cfg.Subscription
// is empty (a programming error caught at wiring).
func NewRunner(cfg Config, src *Source, opts ...RunnerOption) *Runner {
	if cfg.Subscription == "" {
		panic("projection: Config.Subscription is required")
	}
	r := &Runner{
		cfg:    cfg.withDefaults(),
		src:    src,
		log:    slog.New(discardHandler{}),
		now:    time.Now,
		totals: make(map[SessionKey]*CostTotals),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run drives the worker until ctx is cancelled, then returns ctx.Err(). It
// ensures the subscription row exists, loads the durable cursor, does an initial
// catch-up, then catches up again on every wakeup (a [Waker] hint), on each
// PollInterval tick, and runs the sweeper on each SweepInterval tick. A read
// error from a single poll is logged and retried on the next tick (the worker is
// resilient: a transient DB blip must not kill projectord), but a cursor-load
// failure at start is fatal (the worker cannot safely begin).
//
// waker is an optional LISTEN/NOTIFY wakeup channel (a receive is a hint to catch
// up now; coalescing is fine because the cursor read is authoritative). Pass nil
// for pure polling.
func (r *Runner) Run(ctx context.Context, waker <-chan struct{}) error {
	if err := r.src.EnsureSubscription(ctx, r.cfg.Subscription); err != nil {
		return err
	}
	cur, err := r.src.LoadCursor(ctx, r.cfg.Subscription)
	if err != nil {
		return err
	}
	r.cursor = cur
	r.log.InfoContext(ctx, "projection worker starting",
		slog.String("subscription", r.cfg.Subscription), slog.String("cursor", r.cursor.String()))

	poll := time.NewTicker(r.cfg.PollInterval)
	defer poll.Stop()

	var sweepC <-chan time.Time
	if r.sweeper != nil && r.cfg.SweepInterval > 0 {
		sweepT := time.NewTicker(r.cfg.SweepInterval)
		defer sweepT.Stop()
		sweepC = sweepT.C
	}

	// Initial catch-up before waiting on any wakeup.
	r.catchUp(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			r.catchUp(ctx)
		case _, alive := <-waker:
			if !alive {
				waker = nil // listener closed; degrade to pure polling
				continue
			}
			r.catchUp(ctx)
		case <-sweepC:
			r.runSweep(ctx)
		}
	}
}

// catchUp drains all currently-available settled events: it folds batch after
// batch (advancing and persisting the cursor each time) until a poll returns a
// short batch, then publishes the residual lag. It is the safe-advance core: each
// batch is bounded below xmin by the query, the cursor advances only over rows the
// query admitted, and the cursor is saved only AFTER its batch folded into the
// rollup, so a crash re-reads from the last fully-projected position.
//
// Errors are logged and end the catch-up early (retried on the next tick); the
// worker never dies on a transient read error.
func (r *Runner) catchUp(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		rows, err := r.src.FetchBatch(ctx, r.cursor, r.cfg.BatchSize)
		if err != nil {
			r.log.WarnContext(ctx, "projection fetch failed; will retry", slog.String("err", err.Error()))
			return
		}
		if len(rows) == 0 {
			break
		}
		if err := r.foldAndAdvance(ctx, rows); err != nil {
			r.log.ErrorContext(ctx, "projection fold/advance failed; will retry", slog.String("err", err.Error()))
			return
		}
		if len(rows) < r.cfg.BatchSize {
			break // short read: caught up to xmin
		}
	}
	r.publishLag(ctx)
}

// foldAndAdvance folds one batch into the cost rollup, advances the cursor to the
// batch's last row, persists it, and emits the batch's cost delta. The ORDER of
// operations is deliberate: fold first (so the in-memory rollup reflects the
// batch), then SAVE the cursor (so a crash before the save re-reads the batch —
// at-least-once; the durable record is the monotonic cost counter and the cursor,
// and a re-read only re-emits a delta already counted, which is the documented
// at-least-once trade for never SKIPPING an event; NFR-REL-04 prioritizes
// no-skip over exactly-once for the audit/cost feed).
func (r *Runner) foldAndAdvance(ctx context.Context, rows []EventRow) error {
	// Compute the per-tenant cost delta of this batch before folding, for the
	// counter (the rollup map holds the running total; the counter wants the delta).
	delta := batchCostByTenant(rows)

	var err error
	r.totals, err = RollupFold(r.totals, rows)
	if err != nil {
		return err
	}

	cursorRows := make([]rowCursor, len(rows))
	for i, row := range rows {
		cursorRows[i] = row.rowCursor()
	}
	newCursor, _ := r.cursor.Advance(cursorRows)

	if err := r.src.SaveCursor(ctx, r.cfg.Subscription, newCursor); err != nil {
		return err
	}
	r.cursor = newCursor

	for tenantID, d := range delta {
		r.addCost(ctx, tenantID, d)
	}
	return nil
}

// runSweep runs one orphan-blob reclamation pass, logging the outcome. A sweep
// error is logged and swallowed (the next tick retries); a slow/failing blob
// backend must not kill the worker.
func (r *Runner) runSweep(ctx context.Context) {
	n, err := r.sweeper.Sweep(ctx, r.now())
	if err != nil {
		r.log.WarnContext(ctx, "orphan-blob sweep failed; will retry", slog.String("err", err.Error()))
		return
	}
	if n > 0 {
		r.log.InfoContext(ctx, "orphan-blob sweep reclaimed blobs", slog.Int("reclaimed", n))
	}
}

// publishLag reads the residual lag and publishes it to the metric sink.
func (r *Runner) publishLag(ctx context.Context) {
	lag, err := r.src.Lag(ctx, r.cursor)
	if err != nil {
		r.log.WarnContext(ctx, "projection lag read failed", slog.String("err", err.Error()))
		return
	}
	if r.metrics != nil {
		r.metrics.SetProjectionLag(lag)
	}
}

// addCost publishes a per-tenant cost delta to the metric sink.
func (r *Runner) addCost(ctx context.Context, tenantID string, deltaUSD float64) {
	if r.metrics != nil && deltaUSD != 0 {
		r.metrics.AddCost(ctx, tenantID, deltaUSD)
	}
}

// Totals returns a snapshot copy of the current in-memory cost rollup, keyed by
// (tenant, session). It is exported for the orchestrator's read path and for
// assertions; the returned map is a copy so the caller cannot mutate the worker's
// accumulator.
func (r *Runner) Totals() map[SessionKey]CostTotals {
	out := make(map[SessionKey]CostTotals, len(r.totals))
	for k, v := range r.totals {
		out[k] = *v
	}
	return out
}

// batchCostByTenant sums a batch's turn-terminal cost per tenant for the cost
// counter. A malformed payload is ignored here (it is surfaced as a hard error by
// RollupFold, which runs next); this helper only feeds the monotonic counter.
func batchCostByTenant(rows []EventRow) map[string]float64 {
	out := make(map[string]float64)
	for _, row := range rows {
		cost, _, ok, err := row.turnCost()
		if err != nil || !ok || cost == 0 {
			continue
		}
		out[row.TenantID] += cost
	}
	return out
}

// runOnce drives a single catch-up pass (no loop, no tickers). It is used by tests
// and by callers that prefer to schedule the worker themselves; production uses
// [Runner.Run]. It returns after the worker has caught up to xmin once.
func (r *Runner) runOnce(ctx context.Context) error {
	if err := r.src.EnsureSubscription(ctx, r.cfg.Subscription); err != nil {
		return err
	}
	cur, err := r.src.LoadCursor(ctx, r.cfg.Subscription)
	if err != nil {
		return err
	}
	r.cursor = cur
	r.catchUp(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// discardHandler is a no-op slog.Handler so a Runner without a configured logger
// is silent rather than nil-panicking.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }
