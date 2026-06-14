// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
)

// newSessionCorrelationID mints an opaque, advisory Mcp-Session-Id returned on
// initialize. Per ADR-0022 / the transport decision, the AUTHORITATIVE
// continuation state is the durable seq cursor in the event log, NOT MCP session
// state — so this id is correlation metadata only (the server does not reject
// later requests that omit it; stateful enforcement is deferred to roadmap).
//
// crypto/rand is used (not math/rand) for an unguessable id; a rand read failure
// is effectively impossible on a healthy host, but on the off chance it errors we
// return a fixed sentinel rather than panic — the id is advisory, so a degraded
// value does not affect correctness.
func newSessionCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "mcp-session-unavailable"
	}
	return "mcp-" + hex.EncodeToString(b[:])
}
