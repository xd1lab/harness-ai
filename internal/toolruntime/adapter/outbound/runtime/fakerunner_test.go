package runtime

import (
	"context"
	"strings"
	"sync"
)

// fakeRunner is a deterministic in-test [commandRunner]. It records every cmdSpec it
// receives and returns scripted results matched by the docker subcommand (the first
// arg: create/start/exec/kill/rm/ps). A nil result for a subcommand returns a
// zero-exit success. A handler may block until released, to exercise the
// cancellation-to-kill wiring without real Docker.
type fakeRunner struct {
	mu    sync.Mutex
	calls []cmdSpec

	// handlers maps a docker subcommand to a function producing its result. When a
	// handler is absent the runner returns a zero-exit success.
	handlers map[string]func(ctx context.Context, spec cmdSpec) (cmdResult, error)
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{handlers: make(map[string]func(ctx context.Context, spec cmdSpec) (cmdResult, error))}
}

// Run records spec and dispatches to the handler for its docker subcommand.
func (f *fakeRunner) Run(ctx context.Context, spec cmdSpec) (cmdResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, spec)
	h := f.handlers[subcommand(spec)]
	f.mu.Unlock()
	if h != nil {
		return h(ctx, spec)
	}
	return cmdResult{ExitCode: 0}, nil
}

// on registers a handler for a docker subcommand.
func (f *fakeRunner) on(sub string, h func(ctx context.Context, spec cmdSpec) (cmdResult, error)) {
	f.mu.Lock()
	f.handlers[sub] = h
	f.mu.Unlock()
}

// callsFor returns the recorded specs whose docker subcommand equals sub.
func (f *fakeRunner) callsFor(sub string) []cmdSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []cmdSpec
	for _, c := range f.calls {
		if subcommand(c) == sub {
			out = append(out, c)
		}
	}
	return out
}

// signalsFor returns the kill signals (TERM/KILL) recorded for `docker kill` calls,
// in order.
func (f *fakeRunner) signalsFor() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if subcommand(c) != "kill" {
			continue
		}
		for _, a := range c.Args {
			if strings.HasPrefix(a, "--signal=") {
				out = append(out, strings.TrimPrefix(a, "--signal="))
			}
		}
	}
	return out
}

// subcommand returns the docker subcommand (the first non-flag arg) of a spec.
func subcommand(spec cmdSpec) string {
	if len(spec.Args) == 0 {
		return ""
	}
	return spec.Args[0]
}

// argValue returns the value following the named flag in args, or "".
func argValue(args []string, flag string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

// hasArg reports whether args contains the exact token.
func hasArg(args []string, token string) bool {
	for _, a := range args {
		if a == token {
			return true
		}
	}
	return false
}
