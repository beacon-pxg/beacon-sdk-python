# beacon-sdk-python

The SDK workers use to talk to the Beacon agent on their node (`docs/05-workers.md` §6 in the
`beacon` repo). Standard library only.

```python
from beacon_sdk import Worker

w = Worker()              # reads the agent's environment, starts the heartbeat
print(w.config())         # installation config + source settings
for job in w.jobs():      # ends when the agent asks the worker to stop
    w.progress(job, 10, "fetching")
    path = download(job.payload["release"]["url"], job.out_dir, stop=lambda: w.should_stop(job))
    w.output(job, "video", path)
    w.complete(job, {"duration_s": 1440})
```

An exception that ends the worker fails its current job: `NeedsReview` sends it to review, anything
else (including `Retryable`) is retried by Beacon. `fail(job, error, retryable=False)` chooses
explicitly.

Scrapers also get `secrets()` (the source's cookies and proxy, never stored on disk), `throttle()`
(call it before each request to the site: it respects the source's rate limit) and
`discover_releases()` / `discover_works()` for `check` and `scrape` jobs.

`self_check(binaries=[...], config=[...])` returns what's wrong at start-up (missing binaries or
installation config keys); the worker decides whether to exit.

## Running a worker without Beacon

`python -m beacon_sdk.dev` runs the worker in the current folder on one job, against a fake agent, and
prints what it reports (works, releases, progress, logs, outputs, complete/fail):

```bash
uv run python -m beacon_sdk.dev scrape   https://site.example
uv run python -m beacon_sdk.dev check    https://site.example/work/17/ [--known 3]
uv run python -m beacon_sdk.dev download https://site.example/work/17/1/ -c page_image="div.reader img"
uv run python -m beacon_sdk.dev process  --input video=clip.mp4 -p preset=hh_dual
```

The config, params and source come from `dev.toml` (keep it out of git: it can hold cookies), with
`-c key=value` / `-p key=value` on top, checked against the worker's schemas. Files land in `./out/`.
`--json` prints one raw line per call. `python -m beacon_sdk.dev -h` shows the `dev.toml` keys.

## Tests

```bash
uv run python -m unittest discover -s tests -v
```

They run against a fake agent (`beacon_sdk._fake_agent`, the same one `dev` uses). Beacon's CI also runs the SDK against the real
agent (`TestSDKContract` in `beacon/cmd/beacon-agent`).

Status: v0.5, the whole contract of `docs/05-workers.md` §6.
