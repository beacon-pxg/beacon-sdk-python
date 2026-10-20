"""The SDK against a fake agent (beacon_sdk._fake_agent). Run: uv run python -m unittest discover -s tests -v"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import beacon_sdk
from beacon_sdk import _worker
from beacon_sdk._fake_agent import TOKEN, FakeAgent

SRC = str(Path(__file__).resolve().parent.parent / "src")


class WorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.agent = FakeAgent()
        self.addCleanup(self.agent.close)
        env = {"BEACON_AGENT_URL": self.agent.url, "BEACON_WORKER_TOKEN": TOKEN, "BEACON_WORKER_ID": "nw_1"}
        patch = mock.patch.dict(os.environ, env)
        patch.start()
        self.addCleanup(patch.stop)
        hb = mock.patch.object(_worker, "HEARTBEAT_S", 0.05)
        hb.start()
        self.addCleanup(hb.stop)
        # Worker() hooks sys.excepthook and atexit; keep them from leaking between tests.
        self.addCleanup(setattr, sys, "excepthook", sys.excepthook)
        at = mock.patch("atexit.register")
        at.start()
        self.addCleanup(at.stop)

    def worker(self) -> beacon_sdk.Worker:
        w = beacon_sdk.Worker()
        self.addCleanup(w._stop.set)  # ends the heartbeat thread
        return w

    def stop_after(self, seconds: float) -> None:
        def later():
            time.sleep(seconds)
            self.agent.stop_requested = True

        threading.Thread(target=later, daemon=True).start()

    def test_job_lifecycle(self) -> None:
        self.agent.job("job_1", release={"number": 7})
        w = self.worker()
        self.stop_after(0.3)
        seen = []
        for job in w.jobs():  # ends once the agent asks the worker to stop
            seen.append(job)
            self.assertEqual(job.payload["release"]["number"], 7)
            self.assertEqual(job.out_dir, "/tmp/beacon-test/job_1/")
            w.progress(job, 50, "half")
            w.log(job, "info", "fetched", pages=3)
            w.output(job, "images", "pages/001.webp")
            w.complete(job, {"pages": 3})
        self.assertEqual([j.id for j in seen], ["job_1"])
        paths = [p for _, p, _ in self.agent.calls if p.startswith("/jobs/job_1")]
        self.assertEqual(paths, ["/jobs/job_1/progress", "/jobs/job_1/log", "/jobs/job_1/outputs", "/jobs/job_1/complete"])
        out = next(b for _, p, b in self.agent.calls if p.endswith("/outputs"))
        self.assertTrue(os.path.isabs(out["path"]), "output paths are sent absolute")
        log = next(b for _, p, b in self.agent.calls if p.endswith("/log"))
        self.assertEqual(log["fields"], {"pages": 3})

    def test_run_takes_jobs_while_others_run(self) -> None:
        self.agent.job("job_1")
        self.agent.job("job_2")
        w = self.worker()
        both = threading.Barrier(2, timeout=2)  # each job waits for the other: only passes if they overlap

        def handle(job):
            both.wait()
            w.complete(job)

        self.stop_after(0.5)
        w.run(handle)
        self.assertEqual(sorted(p for p, _ in self.agent.finals()), ["/jobs/job_1/complete", "/jobs/job_2/complete"])

    def test_run_outcomes(self) -> None:
        self.agent.job("job_1", kind="review")
        self.agent.job("job_2", kind="boom")
        self.agent.job("job_3", kind="forgot")
        w = self.worker()

        def handle(job):
            if job.payload["kind"] == "review":
                raise beacon_sdk.NeedsReview("selector changed")
            if job.payload["kind"] == "boom":
                raise ValueError("bad page")

        self.stop_after(0.5)
        with mock.patch("traceback.print_exc"):
            w.run(handle)
        got = {p: (b["retryable"], b["review"]) for p, b in self.agent.finals()}
        self.assertEqual(got, {"/jobs/job_1/fail": (False, "selector changed"), "/jobs/job_2/fail": (True, ""),
                               "/jobs/job_3/fail": (True, "")})

    def test_beacon_unreachable_is_waited_out(self) -> None:
        self.agent.job("job_1")
        self.agent.unreachable = 2  # Beacon restarting: two 503s, then it answers
        w = self.worker()
        self.stop_after(0.5)
        with mock.patch.object(_worker.time, "sleep"):
            for job in w.jobs():
                w.complete(job, {"ok": True})
        # Two 503s and the call that lands; no fail on the way.
        self.assertEqual([p for p, _ in self.agent.finals()], ["/jobs/job_1/complete"] * 3)
        self.assertEqual(self.agent.unreachable, 0)

    def test_moving_on_without_finishing_fails_the_job(self) -> None:
        self.agent.job("job_1")
        w = self.worker()
        self.stop_after(0.3)
        for _ in w.jobs():
            pass
        (path, body), = self.agent.finals()
        self.assertEqual(path, "/jobs/job_1/fail")
        self.assertTrue(body["retryable"])

    def test_finishing_twice_sends_one_call(self) -> None:
        self.agent.job("job_1")
        w = self.worker()
        self.stop_after(0.3)
        for job in w.jobs():
            w.complete(job)
            w.fail(job, "too late")
        self.assertEqual([p for p, _ in self.agent.finals()], ["/jobs/job_1/complete"])

    def test_cancel_and_lease_lost(self) -> None:
        self.agent.job("job_1")
        w = self.worker()
        job = next(w.jobs())
        self.assertFalse(w.should_stop(job))
        self.agent.cancel = ["job_1"]
        deadline = time.time() + 2
        while not w.should_stop(job) and time.time() < deadline:
            time.sleep(0.02)
        self.assertTrue(w.should_stop(job), "a cancel reaches should_stop(job) through the heartbeat")
        self.assertFalse(w.should_stop(), "a cancel isn't a stop of the whole worker")

        self.agent.cancel = []
        self.agent.job("job_2")
        self.agent.lost.add("job_2")
        w.complete(job)
        job2 = next(w.jobs())
        w.progress(job2, 10)  # lost: no exception, should_stop(job) turns true
        self.assertTrue(w.should_stop(job2))
        w.complete(job2)  # the result is dropped, without raising

    def test_config_secrets_self_check(self) -> None:
        w = self.worker()
        self.assertEqual(w.config()["installation"]["quality"], 80)
        self.assertEqual(w.secrets()["cookies"], "a=b")
        problems = w.self_check(binaries=["sh", "surely-not-a-binary-xyz"], config=["quality", "preset"])
        self.assertEqual(problems, ["binary not found: surely-not-a-binary-xyz", "config key missing: preset"])
        self.assertEqual(w.self_check(binaries=["sh"], config=["quality"]), [])

    def test_throttle_spaces_requests(self) -> None:
        w = self.worker()  # rate_limit 10 per 1 s: 0.1 s apart
        start = time.monotonic()
        for _ in range(4):
            w.throttle()
        self.assertGreaterEqual(time.monotonic() - start, 0.29)

    def test_discover_in_batches(self) -> None:
        w = self.worker()
        job = _worker.Job(id="job_9", type="scrape", attempt=1, payload={"work": {"id": "work_1"}}, inputs=[], params={}, out_dir="/tmp/")
        works = [{"url": f"https://x/{i}"} for i in range(1200)]
        self.assertEqual(w.discover_works(job, works), 1200)
        self.assertEqual(sum(1 for _, p, _ in self.agent.calls if p == "/discover/works"), 3)
        # A check with no releases still reports the work's details once.
        self.assertEqual(w.discover_releases(job, [], work={"title": "T"}), 0)
        body = next(b for _, p, b in self.agent.calls if p == "/discover/releases")
        self.assertEqual((body["work_id"], body["work"]), ("work_1", {"title": "T"}))

    def test_outside_the_agent(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit):
            beacon_sdk.Worker()


class EndingTest(unittest.TestCase):
    """How a worker process that ends mid-job leaves that job: never without a final call."""

    def run_worker(self, body: str) -> list[tuple[str, dict]]:
        agent = FakeAgent()
        self.addCleanup(agent.close)
        agent.job("job_1")
        code = "import sys\nfrom beacon_sdk import Worker, NeedsReview, Retryable\nw = Worker()\nfor job in w.jobs():\n" + textwrap.indent(body, "    ")
        env = {**os.environ, "BEACON_AGENT_URL": agent.url, "BEACON_WORKER_TOKEN": TOKEN, "PYTHONPATH": SRC}
        subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, timeout=20)
        return agent.finals()

    def test_needs_review(self) -> None:
        (path, body), = self.run_worker('raise NeedsReview("captcha on page 3")')
        self.assertEqual(path, "/jobs/job_1/fail")
        self.assertEqual((body["retryable"], body["review"]), (False, "captcha on page 3"))

    def test_unhandled_exception_is_retryable(self) -> None:
        (path, body), = self.run_worker('raise ValueError("boom")')
        self.assertEqual(path, "/jobs/job_1/fail")
        self.assertTrue(body["retryable"])
        self.assertIn("ValueError: boom", body["error"])

    def test_exit_mid_job(self) -> None:
        (path, body), = self.run_worker("sys.exit(0)")
        self.assertEqual(path, "/jobs/job_1/fail")
        self.assertTrue(body["retryable"])

    def test_explicit_fail_isnt_doubled(self) -> None:
        finals = self.run_worker('w.fail(job, "no pages", retryable=False)\nsys.exit(0)')
        self.assertEqual([(p, b["retryable"]) for p, b in finals], [("/jobs/job_1/fail", False)])


if __name__ == "__main__":
    unittest.main()
