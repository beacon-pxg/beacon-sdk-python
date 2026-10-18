"""Run a worker on one job with a fake agent: no Beacon, no node, no tags. From the worker's folder:

    uv run python -m beacon_sdk.dev scrape   https://site.example
    uv run python -m beacon_sdk.dev check    https://site.example/work/17/ [--known 3]
    uv run python -m beacon_sdk.dev download https://site.example/work/17/1/ [--number 1]
    uv run python -m beacon_sdk.dev process  --input video=clip.mp4 -p preset=hh_dual

Config, the source and params come from dev.toml (keep it out of git: it can hold cookies), with
-c key=value / -p key=value on top. Only what you set is sent, as Beacon does: the worker applies its
own defaults. Values are checked against the worker's config_schema / params_schema.

    command = ["python", "-m", "my_worker"]   # default: [tool.beacon] entrypoint
    [config]                                   # the installation's config
    [params]                                   # the pipeline node's params (converters)
    [source]                                   # base_url, rate_limit = {requests, per_s}, cookies, proxy_url
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import tomllib
from pathlib import Path

from ._fake_agent import TOKEN, FakeAgent

JOBS = ("scrape", "check", "download", "process")
QUIET = ("/heartbeat", "/jobs/claim", "/config", "/secrets")


def coerce(raw: str, prop: dict):
    """A -c/-p value as its schema type ("30" → 30, "false" → False, "a,b" → ["a", "b"])."""
    t = prop.get("type")
    if t == "boolean":
        if raw.lower() in ("true", "1", "yes", "on"):
            return True
        if raw.lower() in ("false", "0", "no", "off"):
            return False
        raise ValueError(f"not a boolean: {raw!r}")
    if t == "integer":
        return int(raw)
    if t == "number":
        return float(raw)
    if t == "array":
        return [coerce(x.strip(), prop.get("items") or {}) for x in raw.split(",") if x.strip()]
    if t == "object" or (t is None and raw[:1] in "[{"):
        return json.loads(raw)
    return raw


def pairs(items: list[str], schema: dict, what: str) -> dict:
    props = schema.get("properties") or {}
    out = {}
    for item in items:
        key, sep, raw = item.partition("=")
        if not sep:
            sys.exit(f"{what}: expected key=value, got {item!r}")
        try:
            out[key] = coerce(raw, props.get(key) or {})
        except ValueError as e:
            sys.exit(f"{what} {key}: {e}")
    return out


TYPES = {"string": str, "integer": int, "number": (int, float), "boolean": bool, "array": list, "object": dict}


def problems(values: dict, schema: dict, what: str) -> list[str]:
    """What Beacon would reject: unknown keys, wrong types, values outside enum/minimum/maximum."""
    props, out = schema.get("properties") or {}, []
    for key, v in values.items():
        prop = props.get(key)
        if prop is None:
            if schema.get("additionalProperties") is False:
                out.append(f"{what}.{key}: not in the schema (known: {', '.join(props) or 'none'})")
            continue
        want = TYPES.get(prop.get("type"))
        if want and (not isinstance(v, want) or (prop.get("type") in ("integer", "number") and isinstance(v, bool))):
            out.append(f"{what}.{key}: expected {prop['type']}, got {v!r}")
        elif "enum" in prop and v not in prop["enum"]:
            out.append(f"{what}.{key}: one of {prop['enum']}, got {v!r}")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            if "minimum" in prop and v < prop["minimum"]:
                out.append(f"{what}.{key}: at least {prop['minimum']}, got {v}")
            elif "maximum" in prop and v > prop["maximum"]:
                out.append(f"{what}.{key}: at most {prop['maximum']}, got {v}")
    return out


def schema_file(beacon: dict, key: str) -> dict:
    name = beacon.get(key)
    return json.loads(Path(name).read_text()) if name else {}


class Printer:
    """One line per thing the worker reports; the job's final call ends the run."""

    def __init__(self, as_json: bool):
        self.as_json, self.final, self.outputs = as_json, None, []
        self.done = threading.Event()

    def __call__(self, method: str, path: str, body: dict) -> None:
        if path in QUIET:
            return
        if path.endswith(("/complete", "/fail")):
            self.final = (path.rsplit("/", 1)[1], body)
        if path.endswith("/outputs"):
            self.outputs.append(body)
        if self.as_json:
            print(json.dumps({"call": path, "body": body}, ensure_ascii=False), flush=True)
        else:
            for line in self.lines(path, body):
                print(line, flush=True)
        if self.final:
            self.done.set()

    def lines(self, path: str, b: dict):
        action = path.rsplit("/", 1)[1]
        if action == "progress":
            yield f"  progress {b.get('pct', 0):>3}%  {b.get('message', '')}"
        elif action == "log":
            fields = " ".join(f"{k}={v}" for k, v in (b.get("fields") or {}).items())
            yield f"  log      {b.get('level', ''):<5} {b.get('message', '')} {fields}".rstrip()
        elif action == "outputs":
            size = os.path.getsize(b["path"]) if os.path.exists(b["path"]) else "missing!"
            yield f"  output   {b.get('port')}  {b.get('path')}  ({size} bytes)"
        elif action == "works":
            for w in b.get("works", []):
                yield f"  work     {w.get('external_id') or '-'}  {w.get('title') or ''!r}  {w.get('url')}"
        elif action == "releases":
            if b.get("work"):
                yield f"  work     {json.dumps(b['work'], ensure_ascii=False)}"
            for r in b.get("releases", []):
                yield f"  release  {r.get('number')}  {r.get('title') or ''!r}  {r.get('url') or ''}"
        elif action == "complete":
            yield f"✓ complete  {json.dumps(b.get('result'), ensure_ascii=False)}"
        elif action == "fail":
            how = f"review: {b['review']}" if b.get("review") else ("retryable" if b.get("retryable") else "not retryable")
            yield f"✗ fail ({how})  {b.get('error')}"
        else:
            yield f"  {path}  {json.dumps(b, ensure_ascii=False)}"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    command = argv[argv.index("--") + 1:] if "--" in argv else []
    argv = argv[:argv.index("--")] if "--" in argv else argv

    p = argparse.ArgumentParser(prog="python -m beacon_sdk.dev", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("job", choices=JOBS)
    p.add_argument("url", nargs="?", help="scrape: the base URL (default: [source] base_url); check/download: the page")
    p.add_argument("-c", dest="config", action="append", default=[], metavar="KEY=VALUE", help="installation config")
    p.add_argument("-p", dest="params", action="append", default=[], metavar="KEY=VALUE", help="node params (process)")
    p.add_argument("--input", action="append", default=[], metavar="PORT=PATH", help="process: an input file")
    p.add_argument("--known", type=float, help="check: the last release number Beacon already has")
    p.add_argument("--number", type=float, default=1, help="download/process: the release number (default 1)")
    p.add_argument("--work-url", help="download: the work's URL")
    p.add_argument("--file", default="dev.toml", help="default: dev.toml")
    p.add_argument("--out", default="out", help="where job dirs go (default: ./out)")
    p.add_argument("--json", action="store_true", help="one raw JSON line per call")
    args = p.parse_args(argv)

    beacon = tomllib.loads(Path("pyproject.toml").read_text()).get("tool", {}).get("beacon", {})
    if not beacon:
        sys.exit("no [tool.beacon] in ./pyproject.toml: run this from the worker's folder")
    dev = tomllib.loads(Path(args.file).read_text()) if Path(args.file).exists() else {}
    if args.job not in beacon.get("job_types", JOBS):
        sys.exit(f"this worker doesn't run {args.job} jobs (job_types = {beacon.get('job_types')})")

    config = {**dev.get("config", {}), **pairs(args.config, schema_file(beacon, "config_schema"), "-c")}
    params = {**dev.get("params", {}), **pairs(args.params, schema_file(beacon, "params_schema"), "-p")}
    bad = problems(config, schema_file(beacon, "config_schema"), "config") + problems(params, schema_file(beacon, "params_schema"), "params")
    if bad:
        sys.exit("\n".join(bad))

    src = dev.get("source", {})
    num = int(args.number) if args.number == int(args.number) else args.number
    payload: dict = {}
    if args.job == "scrape":
        base = args.url or src.get("base_url") or sys.exit("scrape needs a URL or [source] base_url")
        payload = {"source": {"base_url": base}}
    elif args.job == "check":
        url = args.url or sys.exit("check needs the work's URL")
        known = None if args.known is None else (int(args.known) if args.known == int(args.known) else args.known)
        payload = {"work": {"id": "work_dev", "url": url, "external_id": None}, "known": {"last_number": known}}
    elif args.job == "download":
        url = args.url or sys.exit("download needs the release's URL")
        payload = {"release": {"id": "rel_dev", "number": num, "url": url, "work": {"id": "work_dev", "url": args.work_url or url}}}
    else:
        payload = {"release": {"id": "rel_dev", "number": num}}
    inputs = []
    for item in args.input:
        port, sep, path = item.partition("=")
        if not sep or not Path(path).expanduser().exists():
            sys.exit(f"--input {item!r}: expected PORT=PATH to an existing file")
        inputs.append({"port": port, "path": str(Path(path).expanduser().resolve())})

    root = Path(args.out).resolve()
    out_dir = root / f"{args.job}-{time.strftime('%Y%m%d-%H%M%S')}"
    out_dir.mkdir(parents=True)
    printer = Printer(args.json)
    agent = FakeAgent(on_call=printer)
    agent.config = {"installation": config, "source": {
        "slug": src.get("slug", "dev"), "base_url": src.get("base_url") or (args.url if args.job == "scrape" else None),
        "rate_limit": src.get("rate_limit"), "requires_proxy": bool(src.get("proxy_url"))} if beacon.get("kind") == "scraper" else None}
    agent.secrets = {"cookies": src.get("cookies") or None, "proxy_url": src.get("proxy_url") or None}
    agent.jobs.append({"id": "job_dev", "type": args.job, "attempt": 1, "payload": payload, "inputs": inputs,
                       "params": params, "out_dir": str(out_dir) + os.sep, "idempotency_key": "dev"})

    cmd = (command or dev.get("command") or beacon["entrypoint"]) + ["--headless"]
    env = {**os.environ, "BEACON_AGENT_URL": agent.url, "BEACON_WORKER_TOKEN": TOKEN, "BEACON_WORKER_ID": "nw_dev",
           "BEACON_WORK_ROOT": str(root)}
    if not args.json:
        print(f"→ {args.job} {args.url or ''}  ({' '.join(cmd)})  out: {out_dir}", flush=True)
    started = time.monotonic()
    proc = subprocess.Popen(cmd, env=env)
    try:
        while not printer.done.wait(0.2):
            if proc.poll() is not None:
                break
        # The job is over: no need to wait for the next heartbeat's stop (up to 10 s).
        agent.stop_requested = True
        try:
            proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    except KeyboardInterrupt:
        proc.kill()
        return 130
    finally:
        agent.close()

    if printer.final is None:
        print(f"✗ the worker exited ({proc.returncode}) without finishing the job", file=sys.stderr)
        return 2
    if not args.json:
        print(f"  {time.monotonic() - started:.1f} s, {len(printer.outputs)} output(s) in {out_dir}")
    return 0 if printer.final[0] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
