"""A fake agent local API (/local/v1): it queues jobs and records every call. Used by the SDK's tests
and by `python -m beacon_sdk.dev`."""

from __future__ import annotations

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "test-worker-token"


class FakeAgent:
    def __init__(self, on_call=None) -> None:
        self.on_call = on_call  # (method, path, body), after the call is recorded
        self.jobs: list[dict] = []  # handed out by /jobs/claim, in order
        self.calls: list[tuple[str, str, dict]] = []  # (method, path, body)
        self.stop_requested = False
        self.cancel: list[str] = []
        self.lost: set[str] = set()  # job ids that answer 409 lease_lost
        self.unreachable = 0  # the next job calls answer 503, as when Beacon restarts
        self.config = {"installation": {"quality": 80}, "source": {"slug": "s", "rate_limit": {"requests": 10, "per_s": 1}}}
        self.secrets = {"cookies": "a=b", "proxy_url": None}
        self._lock = threading.Lock()
        agent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # quiet
                pass

            def _answer(self, status: int, env: dict) -> None:
                body = json.dumps(env).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self, method: str) -> None:
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    return self._answer(401, {"success": False, "code": "unauthenticated", "message": "no"})
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
                path = self.path.removeprefix("/local/v1")
                with agent._lock:
                    agent.calls.append((method, path, body))
                status, data = agent.route(method, path, body)
                if agent.on_call:
                    agent.on_call(method, path, body)
                if status >= 400:
                    return self._answer(status, {"success": False, "code": data, "message": data})
                self._answer(status, {"success": True, "data": data})

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/local/v1"

    def route(self, method: str, path: str, body: dict) -> tuple[int, object]:
        if path == "/heartbeat":
            return 200, {"stop_requested": self.stop_requested, "cancel": self.cancel}
        if path == "/jobs/claim":
            with self._lock:
                if self.jobs:
                    return 200, self.jobs.pop(0)
            if not self.stop_requested:
                time.sleep(0.05)  # a short long-poll
            return 200, None
        if path == "/config":
            return 200, self.config
        if path == "/secrets":
            return 200, self.secrets
        if path == "/discover/releases":
            return 200, {"new": len(body.get("releases", []))}
        if path == "/discover/works":
            return 200, {"new": len(body.get("works", []))}
        m = re.fullmatch(r"/jobs/([^/]+)/(progress|log|outputs|complete|fail)", path)
        if m:
            if self.unreachable > 0:
                self.unreachable -= 1
                return 503, "service_unavailable"
            if m.group(1) in self.lost:
                return 409, "lease_lost"
            return 200, None
        return 404, "not_found"

    def job(self, id: str = "job_1", type: str = "download", **payload) -> dict:
        j = {"id": id, "type": type, "attempt": 1, "payload": payload, "inputs": [], "params": {},
             "out_dir": "/tmp/beacon-test/" + id + "/", "idempotency_key": "k:" + id}
        self.jobs.append(j)
        return j

    def finals(self) -> list[tuple[str, dict]]:
        """The complete/fail calls: (path, body)."""
        return [(p, b) for m, p, b in self.calls if p.endswith(("/complete", "/fail"))]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
