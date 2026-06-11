#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Drive a Boltrope agent session from Python — no SDK, just HTTP + SSE.

The orchestrator's REST facade mirrors the gRPC API (same auth, same
ownership checks, same event stream):

    POST /v1/sessions                  -> {"sessionId": ...}
    POST /v1/sessions/{id}/run         -> text/event-stream (SSE)
    POST /v1/sessions/{id}/control     -> approve / deny / interrupt / reattach
    GET  /v1/sessions/{id}             -> session projection
    POST /v1/sessions/{id}/fork        -> {"sessionId": <child>}

Every SSE frame carries its durable sequence number as the `id:` field, so a
dropped stream resumes exactly with the standard Last-Event-ID header (or the
"after_seq" body field) and never duplicates frames.

Usage (against the keyless dev compose stack):

    pip install requests
    python run_task.py "Write a hello-world Go program."

Against a production deployment, set BOLTROPE_URL and BOLTROPE_TOKEN (an
OIDC access token whose tenant_id claim is a registered tenant).
"""

import json
import os
import sys

import requests

BASE = os.environ.get("BOLTROPE_URL", "http://localhost:8080")
TOKEN = os.environ.get("BOLTROPE_TOKEN", "")  # dev stack: not needed


def _headers() -> dict:
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}


def create_session(mode: str = "default") -> str:
    r = requests.post(
        f"{BASE}/v1/sessions", json={"mode": mode}, headers=_headers(), timeout=30
    )
    r.raise_for_status()
    return r.json()["sessionId"]


def control(session: str, action: str, call_id: str = "") -> None:
    r = requests.post(
        f"{BASE}/v1/sessions/{session}/control",
        json={"action": action, "call_id": call_id},
        headers=_headers(),
        timeout=30,
    )
    r.raise_for_status()


def run(session: str, text: str, after_seq: int = 0) -> None:
    """Stream one run; on an approval request, ask the human at this terminal."""
    with requests.post(
        f"{BASE}/v1/sessions/{session}/run",
        json={"text": text, "after_seq": after_seq},
        headers=_headers(),
        stream=True,
        timeout=600,
    ) as resp:
        resp.raise_for_status()
        event = ""
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:  # blank line = end of one SSE event
                continue
            if raw.startswith("event: "):
                event = raw[len("event: ") :]
            elif raw.startswith("data: "):
                frame = json.loads(raw[len("data: ") :])
                handle(session, event, frame)


def handle(session: str, event: str, frame: dict) -> None:
    if event == "text_delta":
        print(frame.get("textDelta", {}).get("text", ""), end="", flush=True)
    elif event == "thinking_delta":
        pass  # reasoning text; surface it if your UI wants it
    elif event == "approval_request":
        req = frame.get("approvalRequest", {})
        call_id = req.get("callId", "")
        tool = req.get("toolName", "?")
        answer = input(f"\n[approval required] tool={tool} call_id={call_id} — allow? [y/N] ")
        control(session, "approve" if answer.strip().lower() == "y" else "deny", call_id)
    elif event == "result":
        res = frame.get("result", {})
        print(
            f"\n[result] subtype={res.get('subtype')} "
            f"turns={res.get('numTurns', 0)} cost={res.get('costUsd', 0)} USD"
        )
    elif event == "error":
        print(f"\n[stream error] {frame}", file=sys.stderr)


def main() -> None:
    task = sys.argv[1] if len(sys.argv) > 1 else "Write a hello-world Go program."
    session = create_session()
    print(f"session: {session}")
    run(session, task)


if __name__ == "__main__":
    main()
