from __future__ import annotations

import atexit
import json
import os
import shutil
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

HEARTBEAT_S = 10
CLAIM_WAIT_S = 30
# How long a call waits out "Beacon is unreachable" (a restart, a deploy) before giving up. A job that
# gives up stays held by the agent until the worker restarts. ponytail: no buffering past that.
UNREACHABLE_RETRY_S = 600


class Retryable(Exception):
    """Fail the job; Beacon retries it."""


class NeedsReview(Exception):
    """Fail the job and send its release to review with this reason."""


class AgentError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code = status, code


@dataclass
class Job:
    """A claimed job (docs/05-workers.md §4)."""

    id: str
    type: str
    attempt: int
    payload: dict
    inputs: list
    params: dict
    out_dir: str
    idempotency_key: str = ""
    _done: bool = field(default=False, repr=False)


class Worker:
    """Connects to the agent (BEACON_AGENT_URL / BEACON_WORKER_TOKEN) and heartbeats in the background."""

    def __init__(self) -> None:
        try:
            self._url = os.environ["BEACON_AGENT_URL"].rstrip("/")
            self._token = os.environ["BEACON_WORKER_TOKEN"]
        except KeyError as e:
            raise SystemExit(f"beacon_sdk: {e.args[0]} isn't set: run the worker through the Beacon agent") from None
        self.id = os.environ.get("BEACON_WORKER_ID", "")
        self.work_root = os.environ.get("BEACON_WORK_ROOT", ".")
        self._stop = threading.Event()
        self._cancel: set[str] = set()
        self._held: list[Job] = []  # claimed and not finished; run() holds several at once
        self._held_lock = threading.Lock()
        self._throttle_lock = threading.Lock()
        self._next_request, self._gap, self._rate_checked = 0.0, 0.0, -1e9
        self._beat()
        threading.Thread(target=self._heartbeat_loop, name="beacon-heartbeat", daemon=True).start()
        # A job is never left without a final call: an exception that ends the program fails it with
        # the outcome its type chooses; any other exit fails it as retryable.
        prev = sys.excepthook

        def hook(t, e, tb):
            if isinstance(e, NeedsReview):
                self._abandon(str(e), retryable=False, review=str(e))
            else:
                self._abandon(f"{t.__name__}: {e}", retryable=True)
            prev(t, e, tb)

        sys.excepthook = hook
        atexit.register(self._abandon, "the worker exited mid-job", True)

    # ---- transport ----

    def _call(self, method: str, path: str, body: Any = None, timeout: float = 40, retry: bool = True) -> Any:
        """One local API call. A 503 (the agent can't reach Beacon) is retried with backoff for
        UNREACHABLE_RETRY_S, so a Beacon restart doesn't drop the jobs in flight."""
        deadline, wait = time.monotonic() + UNREACHABLE_RETRY_S, 1.0
        while True:
            try:
                return self._call_once(method, path, body, timeout)
            except AgentError as e:
                if not retry or e.status != 503 or time.monotonic() + wait > deadline:
                    raise
                print(f"beacon_sdk: {path}: {e}; retrying in {wait:.0f}s", file=sys.stderr)
                time.sleep(wait)
                wait = min(wait * 2, 30)

    def _call_once(self, method: str, path: str, body: Any, timeout: float) -> Any:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self._url + path, data=data, method=method,
                                     headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                env = json.load(res)
        except urllib.error.HTTPError as e:
            with e:  # an error response holds a socket too
                try:
                    env = json.load(e)
                except ValueError:
                    env = {"code": "bad_response", "message": e.reason}
            raise AgentError(e.code, env.get("code", ""), env.get("message", "")) from None
        return env.get("data")

    def _beat(self) -> None:
        data = self._call("POST", "/heartbeat", {}, retry=False) or {}
        if data.get("stop_requested"):
            self._stop.set()
        self._cancel = set(data.get("cancel") or [])

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(HEARTBEAT_S)
            try:
                self._beat()
            except Exception as e:  # the agent restarts us if we go quiet for too long
                print(f"beacon_sdk: heartbeat failed: {e}", file=sys.stderr)

    # ---- runtime ----

    def should_stop(self, job: Job | None = None) -> bool:
        """True once the agent asked the worker to stop, or Beacon to cancel that job."""
        return self._stop.is_set() or (job is not None and job.id in self._cancel)

    def config(self) -> dict:
        """{"installation": {...}, "source": {...} | None}"""
        return self._call("GET", "/config")

    def secrets(self) -> dict:
        """{"cookies": str | None, "proxy_url": str | None} of the worker's source. Never store them on disk."""
        return self._call("GET", "/secrets")

    def throttle(self) -> None:
        """Blocks as needed to respect the source's rate_limit; call it before each request to the site."""
        now = time.monotonic()
        if self._rate_checked + 60 < now:  # the source's settings can change while the worker runs
            src = (self.config() or {}).get("source") or {}
            rl = src.get("rate_limit") or {}
            self._gap = rl["per_s"] / rl["requests"] if rl.get("requests") and rl.get("per_s") else 0.0
            self._rate_checked = now
        with self._throttle_lock:
            wait = self._next_request - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._next_request = max(time.monotonic(), self._next_request) + self._gap

    def self_check(self, binaries: tuple[str, ...] | list[str] = (), config: tuple[str, ...] | list[str] = ()) -> list[str]:
        """The common start-up checks: binaries on PATH and keys present in the installation config.
        Returns what's wrong ([] when all is well); the worker decides whether to exit."""
        problems = [f"binary not found: {b}" for b in check_binaries(*binaries)]
        installation = (self.config() or {}).get("installation") or {}
        problems += [f"config key missing: {k}" for k in config if installation.get(k) in (None, "")]
        return problems

    def discover_releases(self, job: Job, releases: list[dict], work: dict | None = None) -> int:
        """Report a check's releases ([{number, title?, url?, published_at?}]) and, on the first check,
        the work's {title, external_id}. Returns how many were new."""
        new = 0
        for i in range(0, max(len(releases), 1), 500):
            data = self._call("POST", "/discover/releases", {"job_id": job.id, "work_id": job.payload["work"]["id"],
                                                             "work": work or {}, "releases": releases[i:i + 500]})
            new += (data or {}).get("new", 0)
        return new

    def discover_works(self, job: Job, works: list[dict]) -> int:
        """Report a batch of a scrape's works ([{url, external_id?, title?}]). Returns how many were new."""
        new = 0
        for i in range(0, len(works), 500):
            data = self._call("POST", "/discover/works", {"job_id": job.id, "works": works[i:i + 500]})
            new += (data or {}).get("new", 0)
        return new

    # ---- jobs ----

    def run(self, handle: Callable[[Job], Any]) -> None:
        """Runs handle(job) for every job, several at once: it keeps claiming while jobs run, and the
        agent hands out up to the installation's max_concurrency (edited in Beacon, applied live), so a
        long scrape doesn't hold back downloads. Each job runs in its own thread; throttle() is shared,
        so the source's rate limit holds across them. An exception from handle chooses the outcome
        (NeedsReview: review; anything else: retryable). Returns once the agent asks the worker to stop
        and the running jobs end."""
        threads: list[threading.Thread] = []
        for job in self._claims():
            t = threading.Thread(target=self._run_one, args=(handle, job), name=f"beacon-{job.id}", daemon=True)
            t.start()
            threads = [x for x in threads if x.is_alive()] + [t]
        for t in threads:
            t.join()

    def _run_one(self, handle: Callable[[Job], Any], job: Job) -> None:
        try:
            try:
                handle(job)
            except NeedsReview as e:
                self.fail(job, str(e), retryable=False, review=str(e))
            except Exception as e:  # noqa: BLE001 - any other failure is retryable
                traceback.print_exc()
                self.fail(job, f"{type(e).__name__}: {e}", retryable=True)
            else:
                if not job._done:
                    self.fail(job, "the worker returned without finishing the job", retryable=True)
        except Exception as e:  # noqa: BLE001 - the agent is gone; the lease expires and Beacon retries
            print(f"beacon_sdk: job {job.id}: {e}", file=sys.stderr)

    def jobs(self) -> Iterator[Job]:
        """Jobs for this worker, one at a time; ends when the agent asks to stop. run() takes several."""
        for job in self._claims():
            yield job  # an exception in the loop body ends the program: the excepthook fails the job
            if not job._done:
                self.fail(job, "the worker moved on without finishing the job", retryable=True)

    def _claims(self) -> Iterator[Job]:
        while not self._stop.is_set():
            try:
                data = self._call("POST", "/jobs/claim", {"wait_s": CLAIM_WAIT_S}, timeout=CLAIM_WAIT_S + 15, retry=False)
            except (AgentError, OSError) as e:
                print(f"beacon_sdk: claim failed: {e}", file=sys.stderr)
                self._stop.wait(5)
                continue
            if not data:
                continue
            job = Job(id=data["id"], type=data["type"], attempt=data.get("attempt", 1), payload=data.get("payload") or {},
                      inputs=data.get("inputs") or [], params=data.get("params") or {}, out_dir=data["out_dir"],
                      idempotency_key=data.get("idempotency_key", ""))
            with self._held_lock:
                self._held.append(job)
            yield job

    def progress(self, job: Job, pct: int, message: str = "") -> None:
        self._job_call(job, "progress", {"pct": int(pct), "message": message})

    def log(self, job: Job, level: str, message: str, **fields: Any) -> None:
        self._job_call(job, "log", {"level": level, "message": message, "fields": fields})

    def output(self, job: Job, port: str, path: str) -> None:
        """Register a file in job.out_dir as the output for a pipeline port (e.g. "video")."""
        self._job_call(job, "outputs", {"port": port, "path": os.path.abspath(path)})

    def complete(self, job: Job, result: Any = None) -> None:
        self._finish(job, "complete", {"result": result})

    def fail(self, job: Job, error: str, retryable: bool = True, review: str | None = None) -> None:
        self._finish(job, "fail", {"error": str(error), "retryable": retryable, "review": review or ""})

    def _job_call(self, job: Job, action: str, body: dict) -> None:
        try:
            self._call("POST", f"/jobs/{job.id}/{action}", body)
        except AgentError as e:
            if e.code != "lease_lost":
                raise
            self._cancel.add(job.id)  # lost: should_stop(job) tells the worker to drop it

    def _finish(self, job: Job, action: str, body: dict) -> None:
        if job._done:
            return
        try:
            self._call("POST", f"/jobs/{job.id}/{action}", body)
        except AgentError as e:
            if e.code != "lease_lost":
                raise
            print(f"beacon_sdk: job {job.id}: lease lost, result dropped", file=sys.stderr)
        job._done = True
        with self._held_lock:
            if job in self._held:
                self._held.remove(job)

    def _abandon(self, error: str, retryable: bool, review: str | None = None) -> None:
        with self._held_lock:
            held = list(self._held)
        for job in held:
            if job._done:
                continue
            try:
                self.fail(job, error, retryable=retryable, review=review)
            except Exception as e:
                print(f"beacon_sdk: couldn't fail job {job.id}: {e}", file=sys.stderr)


def check_binaries(*names: str) -> list[str]:
    """Self-check helper: the binaries that aren't on PATH."""
    return [n for n in names if shutil.which(n) is None]
