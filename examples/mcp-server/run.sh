#!/usr/bin/env sh
# SPDX-License-Identifier: Apache-2.0
# Drive Boltrope as an MCP server (MCP Server mode, ADR-0022) with only curl + sh.
#
#   ./run.sh "Say hello."
#
# Boltrope exposes ITSELF as a Model Context Protocol server on POST /mcp — the
# same HTTP listener as the REST facade and /readyz. This script performs the MCP
# handshake (initialize → tools/list), then create_session + run via tools/call.
#
# Against a production deployment, export BOLTROPE_URL and BOLTROPE_TOKEN (an OIDC
# access token whose tenant_id claim is a registered tenant). Auth and RLS are
# inherited from the shared edge — identical to gRPC/REST.
set -eu

BASE="${BOLTROPE_URL:-http://localhost:8080}"
TOKEN="${BOLTROPE_TOKEN:-}"
TASK="${1:-Say hello.}"

# A Bearer header only when a token is set (the dev stack needs none).
auth() {
	if [ -n "$TOKEN" ]; then printf 'Authorization: Bearer %s' "$TOKEN"; else printf 'X-No-Auth: 1'; fi
}

# A single JSON-RPC POST to /mcp; prints the response body.
rpc() {
	curl -fsS -X POST "$BASE/mcp" \
		-H "$(auth)" -H 'Content-Type: application/json' -H 'Accept: application/json' \
		-d "$1"
}

# 1. initialize — the MCP handshake. Returns serverInfo + capabilities{tools}.
echo "== initialize =="
rpc '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
echo
echo

# 2. tools/list — the 5 tools (create_session, run, get_session, control, fork).
echo "== tools/list =="
rpc '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
echo
echo

# 3. tools/call create_session — open a session. Response carries session_id.
echo "== tools/call create_session =="
resp=$(rpc '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_session","arguments":{"mode":"default"}}}')
printf '%s\n\n' "$resp"
sid=$(printf '%s' "$resp" | sed -n 's/.*"session_id"[: ]*"\([^"]*\)".*/\1/p')
echo "session: $sid"
echo

# 4. tools/call run — stream the reply on a text/event-stream leg by sending a
#    _meta.progressToken. Each SSE frame's `id:` is the durable seq; the final
#    `event: result` frame carries the CallToolResult (status:"completed").
echo "== tools/call run (SSE) =="
curl -fsS -N -X POST "$BASE/mcp" \
	-H "$(auth)" -H 'Content-Type: application/json' -H 'Accept: text/event-stream' \
	-d "$(printf '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"run","arguments":{"session_id":"%s","text":%s},"_meta":{"progressToken":"p1"}}}' \
		"$sid" "$(printf '%s' "$TASK" | sed 's/"/\\"/g; s/^/"/; s/$/"/')")"
