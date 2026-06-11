package execute

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// BlobThresholdBytes is the inline-output threshold: a tool result whose content
// exceeds this many bytes is offloaded to the blob store and replaced inline with
// a truncated stand-in plus a populated [domain.Observation.BlobRef] (FR-STATE-05;
// architecture §6.4).
const BlobThresholdBytes = 32 << 10 // 32 KiB

// blobMediaType is the media type recorded for offloaded tool output. Tool output
// is treated as opaque UTF-8 text.
const blobMediaType = "text/plain; charset=utf-8"

// Progress is one interim tool-execution event emitted before the terminal
// result: a human-readable note and/or a fragment of partial stdout. It mirrors
// the wire ToolProgress shape but is the app layer's own type so this package
// imports no gen/ (architecture §12.4).
type Progress struct {
	// Message is an optional human-readable progress note.
	Message string
	// StdoutChunk is an optional ordered fragment of the tool's stdout produced so
	// far.
	StdoutChunk []byte
}

// Emitter receives interim [Progress] events during a single [Service.Execute].
// The gRPC server adapter implements it by mapping each event to an
// ExecuteToolEvent_Progress on the server stream; tests use a recording fake. A
// non-nil error from Progress aborts the execution (e.g. the client stream is
// gone).
type Emitter interface {
	// Progress delivers one interim event. Returning an error stops execution.
	Progress(ctx context.Context, p Progress) error
}

// nopEmitter discards progress; it is used when a caller passes a nil Emitter.
type nopEmitter struct{}

func (nopEmitter) Progress(context.Context, Progress) error { return nil }

// Masker is the optional best-effort output masker applied to tool output before
// it leaves the trust boundary. It is defense-in-depth for log/telemetry hygiene
// ONLY and is never a containment boundary — the real exfiltration control is the
// egress broker (ADR-0013; architecture §8.10). A nil Masker leaves output
// unchanged.
type Masker interface {
	// Mask returns s with known/registry secrets redacted. It must not error: a
	// best-effort masker degrades to identity rather than failing the call.
	Mask(s string) string
}

// Request is the input to [Service.Execute]: one tool call in a session's
// sandbox plus the durable dedup key. It mirrors the fields of the wire
// ExecuteToolRequest but is the app layer's own type.
type Request struct {
	// TenantID owns the session; it scopes the sandbox, the dedup ledger, and the
	// blob store, and is the tenant a cached result is re-checked against
	// (architecture §7.3).
	TenantID string
	// SessionID is the session whose per-session workspace the tool runs in.
	SessionID string
	// CallID is the opaque per-call identifier the model assigned (carried through
	// to the result; matched by the orchestrator).
	CallID string
	// ToolName is the registered tool to invoke.
	ToolName string
	// Args is the parsed argument object. It is validated against the tool's JSON
	// Schema by the registry's decorator before execution (FR-TOOL-01).
	Args map[string]any
	// IdempotencyKey is the log-derived dedup key hash(session_id, seq_of_ToolCall)
	// supplied by the orchestrator (architecture §7.2). A Mutating call whose key is
	// already completed returns the prior result without re-executing.
	IdempotencyKey string
	// Timeout is the tool's own execution timeout; zero applies no per-call
	// deadline beyond the inherited ctx. On expiry the inherited ctx is cancelled,
	// which propagates to the in-sandbox process-group kill (architecture §9.3).
	Timeout time.Duration
}

// Result is the terminal outcome of [Service.Execute]: the model-visible
// observation (already carrying Truncated/BlobRef when the output was offloaded).
type Result struct {
	// Observation is the normalized terminal result fed back to the model. A tool
	// that ran but produced an error reports it via Observation.IsError; a denied
	// or invalid call is likewise an error Observation, never a Go error
	// (FR-TOOL-01).
	Observation domain.Observation
	// BlobSizeBytes is the full byte size of the offloaded output when
	// Observation.BlobRef is set (0 otherwise). It carries the authoritative size
	// the transport reports on the wire BlobRef without re-reading the blob store;
	// the frozen [domain.Observation] does not carry it (architecture §6.4).
	BlobSizeBytes int64
}

// Config injects the use-case collaborators. Registry, Runtime, Egress, Dedup,
// and Blobs are required; Masker is optional (nil = no masking) and
// InlineThreshold defaults to [BlobThresholdBytes] when zero.
type Config struct {
	// Registry resolves tools by name (returning a validate-then-execute
	// decorator). Required.
	Registry app.ToolRegistry
	// Runtime provisions/looks up the per-session [app.Workspace] sandbox so
	// cancellation maps to a real in-sandbox kill. Required.
	Runtime app.RuntimePort
	// Egress is the deny-by-default per-session broker gating External-class
	// tools. Required.
	Egress app.EgressBroker
	// Dedup is the durable at-most-once tool-execution ledger. Required.
	Dedup app.DedupStore
	// Blobs offloads oversized tool output. Required.
	Blobs blob.BlobStorePort
	// Masker, when non-nil, redacts output before it leaves the boundary
	// (defense-in-depth only, §8.10).
	Masker Masker
	// InlineThreshold overrides [BlobThresholdBytes] when > 0; output larger than
	// it is offloaded to the blob store.
	InlineThreshold int
}

// Service is the ExecuteTool use-case. Construct one with [NewService]. It is
// safe for concurrent use when its collaborators are.
type Service struct {
	reg       app.ToolRegistry
	runtime   app.RuntimePort
	egress    app.EgressBroker
	dedup     app.DedupStore
	blobs     blob.BlobStorePort
	masker    Masker
	threshold int
}

// NewService validates cfg and returns a *Service. It returns a non-nil error
// when a required collaborator is missing.
func NewService(cfg Config) (*Service, error) {
	switch {
	case cfg.Registry == nil:
		return nil, errors.New("toolruntime/execute: Config.Registry must not be nil")
	case cfg.Runtime == nil:
		return nil, errors.New("toolruntime/execute: Config.Runtime must not be nil")
	case cfg.Egress == nil:
		return nil, errors.New("toolruntime/execute: Config.Egress must not be nil")
	case cfg.Dedup == nil:
		return nil, errors.New("toolruntime/execute: Config.Dedup must not be nil")
	case cfg.Blobs == nil:
		return nil, errors.New("toolruntime/execute: Config.Blobs must not be nil")
	}
	threshold := cfg.InlineThreshold
	if threshold <= 0 {
		threshold = BlobThresholdBytes
	}
	return &Service{
		reg:       cfg.Registry,
		runtime:   cfg.Runtime,
		egress:    cfg.Egress,
		dedup:     cfg.Dedup,
		blobs:     cfg.Blobs,
		masker:    cfg.Masker,
		threshold: threshold,
	}, nil
}

// Execute runs one tool call and returns its terminal [Result], streaming any
// interim [Progress] through em (a nil em discards progress). The flow is:
// registry lookup → ensure sandbox → dedup guard → egress gate (External tools)
// → execute → offload large output → record completion.
//
// Tool-level failures (unknown tool, schema violation, egress denial, a tool
// that ran but errored) are returned as an error [domain.Observation] with a nil
// Go error so the gRPC edge surfaces them as result.is_error, never an RPC fault
// (FR-TOOL-01). A non-nil Go error denotes an infrastructure failure (e.g. the
// dedup ledger or blob store is unreachable, or the emitter aborted).
func (s *Service) Execute(ctx context.Context, req Request, em Emitter) (Result, error) {
	if em == nil {
		em = nopEmitter{}
	}

	// (1) Resolve the tool. An unknown tool is an error result, not an RPC fault.
	tool, err := s.reg.Get(ctx, req.ToolName)
	if err != nil {
		if errors.Is(err, app.ErrToolNotFound) {
			return Result{Observation: errResult("unknown tool %q", req.ToolName)}, nil
		}
		// A non-not-found error (e.g. a lazy MCP load failure) is infrastructural.
		return Result{}, fmt.Errorf("toolruntime/execute: resolve tool %q: %w", req.ToolName, err)
	}
	spec := tool.Spec()

	// (3) Dedup guard BEFORE any side effect. Begin is get-or-create: it returns
	// the existing record when the key is already known; a completed call returns
	// the prior result WITHOUT re-executing — and without performing any egress, so
	// a cached result is correctly returned even under a since-tightened egress
	// policy (at-most-once; architecture §7.2). The ledger re-checks the caller's
	// tenant before returning bytes (§7.3).
	rec := app.ExecutionRecord{
		TenantID:       req.TenantID,
		SessionID:      req.SessionID,
		IdempotencyKey: req.IdempotencyKey,
	}
	existing, err := s.dedup.Begin(ctx, rec)
	if err != nil {
		return Result{}, fmt.Errorf("toolruntime/execute: dedup begin: %w", err)
	}
	if existing.Status == app.ExecCompleted {
		return Result{Observation: existing.Result}, nil
	}

	// (4) Egress gate for External-class tools, before any execution. Deny-by-
	// default, fail-closed on ambiguity (architecture §8.4). This is the service
	// placement of the infra control; the tool/MCP client also enforces it. A
	// denied fresh call records a DEFINITE failed terminal (not a stuck "started"),
	// so recovery never adjudicates it as an unknown that might double-execute.
	if spec.IsExternal() {
		if obs, blocked := s.egressGate(ctx, req, spec); blocked {
			s.recordTerminal(ctx, rec, obs, app.ExecFailed)
			return Result{Observation: obs}, nil
		}
	}

	// (2) Best-effort readiness touch of the per-session sandbox. The native
	// tools resolve the CALLING session's own workspace per execution (via
	// [SessionWorkspaces], provisioning it on first use); an in-sandbox tool's
	// cancellation maps to a real process-group kill because the resolved
	// Workspace.Exec honors execCtx (architecture §9.3). A tool that needs no
	// sandbox (e.g. a pure MCP proxy) runs regardless, so a missing live
	// workspace here is not fatal — hence the error is intentionally ignored.
	_, _ = s.runtime.Get(ctx, req.SessionID)

	// (5) Execute. Optionally apply a per-call timeout that, on expiry, cancels
	// the ctx and thereby the in-sandbox process tree.
	execCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	// Emit a starting progress note (best-effort relay; an emitter error aborts).
	if perr := em.Progress(ctx, Progress{Message: "executing " + spec.Name}); perr != nil {
		return Result{}, fmt.Errorf("toolruntime/execute: emit progress: %w", perr)
	}

	obs, runErr := tool.Execute(execCtx, req.SessionID, req.Args)
	if runErr != nil {
		// A runtime/execution failure is surfaced to the model as an error
		// observation and recorded as a failed (definite) outcome.
		failObs := domain.Observation{Content: fmt.Sprintf("tool %q failed: %v", spec.Name, runErr), IsError: true}
		s.recordTerminal(ctx, rec, failObs, app.ExecFailed)
		return Result{Observation: failObs}, nil
	}

	// (5b) Best-effort masking (defense-in-depth only; §8.10), then offload
	// oversized output to the blob store.
	if s.masker != nil {
		obs.Content = s.masker.Mask(obs.Content)
	}
	obs, blobSize, err := s.offload(ctx, req, obs)
	if err != nil {
		return Result{}, fmt.Errorf("toolruntime/execute: offload output: %w", err)
	}

	// (6) Record completion in the dedup ledger.
	status := app.ExecCompleted
	if obs.IsError {
		status = app.ExecFailed
	}
	s.recordTerminal(ctx, rec, obs, status)
	return Result{Observation: obs, BlobSizeBytes: blobSize}, nil
}

// egressGate checks an External-class tool's target host against the session's
// deny-by-default allowlist. It returns blocked=true with an error observation
// when the host is denied, unparseable, or the broker errors (fail-closed).
func (s *Service) egressGate(ctx context.Context, req Request, spec domain.ToolSpec) (domain.Observation, bool) {
	host, ok := hostFromArgs(req.Args)
	if !ok {
		// An External tool whose target host cannot be determined fails closed.
		return errResult("tool %q: egress denied: cannot determine target host", spec.Name), true
	}
	allowed, err := s.egress.Allow(ctx, req.SessionID, host)
	if err != nil {
		return errResult("tool %q: egress denied for host %q: %v", spec.Name, host, err), true
	}
	if !allowed {
		return errResult("tool %q: egress denied: host %q is not on the session allowlist", spec.Name, host), true
	}
	return domain.Observation{}, false
}

// offload moves oversized output to the blob store. When len(content) exceeds the
// configured threshold it writes the full bytes under a per-tenant content key
// (sha256 of the bytes), replaces inline content with a truncated descriptor, and
// sets Truncated + BlobRef (architecture §6.4). It returns the full byte size on
// offload (0 otherwise). Smaller output is returned unchanged.
func (s *Service) offload(ctx context.Context, req Request, obs domain.Observation) (domain.Observation, int64, error) {
	if len(obs.Content) <= s.threshold {
		return obs, 0, nil
	}
	full := []byte(obs.Content)
	sum := sha256.Sum256(full)
	key := hex.EncodeToString(sum[:])
	ref := blob.Ref{TenantID: req.TenantID, Key: key}
	objStored, err := s.blobs.Put(ctx, ref, blobMediaType, bytes.NewReader(full))
	if err != nil {
		return obs, 0, err
	}
	obs.Content = descriptor(full, s.threshold)
	obs.Truncated = true
	obs.BlobRef = key
	return obs, objStored.SizeBytes, nil
}

// recordTerminal records a terminal ledger entry, swallowing a ledger error: the
// model-visible result is already determined and an observability/ledger write
// failure must not turn a completed side effect into a reported infra fault. (A
// failed Complete leaves the entry "started"; recovery adjudicates it as unknown,
// the safe posture for a Mutating tool — ADR-0012.)
func (s *Service) recordTerminal(ctx context.Context, rec app.ExecutionRecord, obs domain.Observation, status app.ExecutionStatus) {
	rec.Status = status
	rec.Result = obs
	_ = s.dedup.Complete(ctx, rec)
}

// errResult builds a terminal error [Result]'s observation from a format string.
func errResult(format string, args ...any) domain.Observation {
	return domain.Observation{Content: fmt.Sprintf(format, args...), IsError: true}
}

// descriptor builds a lightweight truncated stand-in for offloaded content: the
// first threshold bytes plus a marker noting the full size and that the bytes
// were offloaded to the blob store (architecture §6.4 "first/last N bytes +
// summary").
func descriptor(full []byte, threshold int) string {
	head := full
	if len(head) > threshold {
		head = head[:threshold]
	}
	var b strings.Builder
	b.Write(head)
	fmt.Fprintf(&b, "\n…[output truncated: %d bytes total, full content offloaded to blob store]", len(full))
	return b.String()
}

// hostFromArgs extracts a candidate target host from a tool's validated args for
// the egress gate. It looks for a "url"/"endpoint" string field (parsed for its
// host) or a "host" string field. It returns ok=false when no host can be
// determined so the caller fails closed (architecture §8.4).
func hostFromArgs(args map[string]any) (string, bool) {
	if h, ok := stringField(args, "host"); ok && h != "" {
		return h, true
	}
	for _, key := range []string{"url", "endpoint", "uri"} {
		if raw, ok := stringField(args, key); ok {
			if u, err := url.Parse(strings.TrimSpace(raw)); err == nil {
				if host := u.Hostname(); host != "" {
					return host, true
				}
			}
		}
	}
	return "", false
}

// stringField returns the string value of args[key], or ok=false when absent or
// not a string.
func stringField(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
