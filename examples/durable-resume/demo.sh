#!/usr/bin/env sh
# SPDX-License-Identifier: Apache-2.0
# Demonstrate the durable Postgres event ledger: inspect a session's log, then
# restart the orchestrator and show the session survives (the projection is
# rebuilt from the log, which lives in Postgres, not in the process).
#
#   ./demo.sh
#
# Needs the keyless dev stack up (docker compose -f deploy/docker-compose.yml
# up -d --wait) and run from the repo root or this directory.
set -eu

BASE="${BOLTROPE_URL:-http://localhost:8080}"
COMPOSE="docker compose -f deploy/docker-compose.yml"
# Allow running from examples/durable-resume/ too.
[ -f deploy/docker-compose.yml ] || COMPOSE="docker compose -f ../../deploy/docker-compose.yml"

pg() { $COMPOSE exec -T postgres psql -U boltrope_owner -d boltrope "$@"; }

echo "==> create a session and run a task"
sid=$(curl -fsS -X POST "$BASE/v1/sessions" -H 'Content-Type: application/json' \
	-d '{"mode":"default"}' | sed -n 's/.*"sessionId"[: ]*"\([^"]*\)".*/\1/p')
echo "    session: $sid"
curl -fsS -N -X POST "$BASE/v1/sessions/$sid/run" -H 'Content-Type: application/json' \
	-d '{"text":"Say hello."}' >/dev/null
echo

echo "==> the durable event log for this session (the source of truth):"
pg -c "SELECT seq, event_type FROM events WHERE session_id='$sid' ORDER BY seq;"

before=$(curl -fsS "$BASE/v1/sessions/$sid" | sed -n 's/.*"headSeq"[: ]*"\([0-9]*\)".*/\1/p')
echo "==> headSeq before restart: $before"

echo "==> restarting orchestratord (simulating a crash)..."
$COMPOSE restart orchestratord >/dev/null 2>&1
# Wait for the HTTP listener to come back.
i=0
while [ "$i" -lt 30 ]; do
	if curl -fsS "$BASE/v1/sessions/$sid" >/dev/null 2>&1; then break; fi
	i=$((i + 1)); sleep 1
done

after=$(curl -fsS "$BASE/v1/sessions/$sid" | sed -n 's/.*"headSeq"[: ]*"\([0-9]*\)".*/\1/p')
echo "==> headSeq after  restart: $after"
echo
if [ "$before" = "$after" ] && [ -n "$after" ]; then
	echo "OK: the session survived the restart — the projection was rebuilt from the durable log."
else
	echo "MISMATCH: before=$before after=$after"
	exit 1
fi
