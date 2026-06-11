#!/usr/bin/env sh
# SPDX-License-Identifier: Apache-2.0
# Drive a Boltrope session over the REST/SSE facade with only curl + sh.
#
#   ./run.sh "Say hello."
#
# Against a production deployment, export BOLTROPE_URL and BOLTROPE_TOKEN
# (an OIDC access token whose tenant_id claim is a registered tenant).
set -eu

BASE="${BOLTROPE_URL:-http://localhost:8080}"
TOKEN="${BOLTROPE_TOKEN:-}"
TASK="${1:-Say hello.}"

# A Bearer header only when a token is set (the dev stack needs none).
auth() {
	if [ -n "$TOKEN" ]; then printf 'Authorization: Bearer %s' "$TOKEN"; else printf 'X-No-Auth: 1'; fi
}

# 1. Create a session. The response is {"sessionId": "..."}.
resp=$(curl -fsS -X POST "$BASE/v1/sessions" \
	-H "$(auth)" -H 'Content-Type: application/json' \
	-d '{"mode":"default"}')
sid=$(printf '%s' "$resp" | sed -n 's/.*"sessionId"[: ]*"\([^"]*\)".*/\1/p')
echo "session: $sid"
echo

# 2. Run the task and stream the reply as Server-Sent Events. Each frame's
#    `id:` is the durable seq — resume with `Last-Event-ID: <seq>` if dropped.
curl -fsS -N -X POST "$BASE/v1/sessions/$sid/run" \
	-H "$(auth)" -H 'Content-Type: application/json' \
	-d "$(printf '{"text":%s}' "$(printf '%s' "$TASK" | sed 's/"/\\"/g; s/^/"/; s/$/"/')")"
