# beacon-sdk-python

The library every Beacon worker uses to do its job. A worker (a scraper or a converter) never talks
to Beacon: it talks only to the Beacon agent on its own PC, over `127.0.0.1`, and this SDK is that
conversation. It claims jobs, reports progress and results, and keeps the worker alive and in step
with the agent. Standard library only.

The contract it implements is `docs/05-workers.md` in the `beacon` repo.

## Quick start

A complete converter: it takes a video, writes a file and hands it on.

```python
from beacon_sdk import Worker

w = Worker()  # reads the agent's environment and starts heartbeating


def handle(job):
    video = next(i["path"] for i in job.inputs if i["port"] == "video")
    w.progress(job, 10, "converting")
    out = convert(video, job.out_dir, stop=lambda: w.should_stop(job))  # your code
    w.output(job, "video_hls", out)
    w.complete(job, {"source": video})


w.run(handle)  # returns when the agent asks the worker to stop
```

`handle` runs once per job. Write everything under `job.out_dir`, register what the next step needs
with `output()`, and end with `complete()`. Check `should_stop(job)` in long loops: it turns true
when the worker is stopping or Beacon cancelled that job.

## Add it to a worker

In the worker's `pyproject.toml`, pin the SDK to a tag of this repo:

```toml
[project]
dependencies = ["beacon-sdk"]

[tool.uv.sources]
beacon-sdk = { git = "https://github.com/beacon-pxg/beacon-sdk-python", tag = "<latest tag>" }
```

Commit the `uv.lock` too: the agent installs with `uv sync --frozen`. It then starts the worker with
the `entrypoint` from `[tool.beacon]`, adding `--headless`, and once with `--self-check` after every
install. The worker exits 0 when the check passes and prints what's wrong otherwise:

```python
problems = w.self_check(binaries=["ffmpeg"], config=["folder"])  # or check_binaries("ffmpeg")
```

The worker can't be run by hand outside the agent: `Worker()` exits if `BEACON_AGENT_URL` isn't set.
To run it without Beacon, see [Run a worker locally](#run-a-worker-locally).

## How a job ends

Each job ends exactly once, and the SDK makes sure it does even if the code doesn't:

| In `handle` | Outcome in Beacon |
| --- | --- |
| `w.complete(job, result)` | Succeeded; its outputs go to the next step |
| `raise NeedsReview("why")` | Failed, and the release waits for a person to review it |
| `w.fail(job, "why", retryable=False)` | Failed for good, no retry |
| `raise Retryable(...)` or any other exception | Failed; Beacon retries it with backoff |
| Returns without `complete` or `fail` | Failed, retryable ("returned without finishing the job") |
| The worker is stopping (restart, update) | Left unfinished; the agent puts it back in the queue without using an attempt |

An exception that kills the whole process fails its held jobs the same way.

## What each kind of worker uses

| Call | For | What it does |
| --- | --- | --- |
| `config()` | all | `{"installation": {...}, "source": {...}}`: the installation's config and, for scrapers, the source |
| `progress(job, pct, message)` | all | Progress shown on the job and release pages |
| `log(job, level, message, **fields)` | all | A line in the job's log |
| `output(job, port, path)` | all | Registers a file in `job.out_dir` for a pipeline port (`video`, `images`…) |
| `complete(job, result)` / `fail(job, …)` | all | Ends the job (above) |
| `should_stop(job)` | all | True when stopping or the job was cancelled |
| `secrets()` | scrapers | The source's cookies and proxy. Keep them in memory, never on disk |
| `throttle()` | scrapers | Call before each request to the site: it waits as the source's rate limit asks |
| `discover_works(job, works)` | scrapers, `scrape` | Reports the works a catalog scrape finds (`[{url, external_id?, title?}]`), in batches |
| `discover_releases(job, releases, work)` | scrapers, `check` | Reports a work's releases (`[{number, title?, url?, published_at?}]`) and, the first time, its title and id |

What a job carries (`job.payload`, `job.inputs`, `job.params`) depends on its type; the shapes are in
`docs/05-workers.md`. Converters read their input files from `job.inputs` and their pipeline settings
from `job.params`.

## Several jobs at once

`run(handle)` runs each job in its own thread and keeps claiming while they work. The agent hands out
up to the installation's **max concurrency**, which is edited in Beacon and applies live, so a long
scrape doesn't hold back downloads. `throttle()` is shared across threads, so the site's rate limit
still holds.

For a worker that must take one job at a time, use the loop instead:

```python
for job in w.jobs():
    ...
    w.complete(job)
```

## When the agent or Beacon is down

- If the agent can't reach Beacon (a restart, a deploy), calls are retried with backoff for up to 10
  minutes, so jobs in flight survive it.
- A worker that stops heartbeating is restarted by the agent.
- If the agent asks the worker to stop, `run()` and `jobs()` stop claiming, and the jobs still
  running are handed back to the queue, not failed.

## Run a worker locally

`python -m beacon_sdk.dev` runs the worker in the current folder on one job against a fake agent: no
Beacon, no node, no tags. It prints every call the worker makes (works, releases, progress, logs,
outputs, the final result):

```bash
uv run python -m beacon_sdk.dev scrape   https://site.example
uv run python -m beacon_sdk.dev check    https://site.example/work/17/ --known 3
uv run python -m beacon_sdk.dev download https://site.example/work/17/1/ -c page_image="div.reader img"
uv run python -m beacon_sdk.dev process  --input video=clip.mp4 -p preset=hh_dual
```

The installation config, the source and the node params come from a `dev.toml` next to the worker
(keep it out of git: it can hold cookies), with `-c key=value` and `-p key=value` on top. They're
checked against the worker's schemas. Files land in `./out/`, and `--json` prints one raw line per
call. `python -m beacon_sdk.dev -h` lists the `dev.toml` keys.

To run it inside a whole local Beacon, with real jobs and pipelines, follow `docs/07-local-dev.md`
in the `beacon` repo.

## Develop the SDK

```bash
uv run python -m unittest discover -s tests -v
```

The tests run against a fake agent (`beacon_sdk._fake_agent`, the same one `dev` uses). Beacon's CI
also runs the SDK against the real agent (`TestSDKContract` in `beacon/cmd/beacon-agent`), so a
change to the contract breaks there first.

To release a change, bump `version` in `pyproject.toml`, tag the commit with that version and push
the tag. Then each worker that should get it updates the `tag` in its `[tool.uv.sources]`, runs
`uv lock`, and tags a new version of its own.

## See also

- `docs/05-workers.md` in `beacon`: the full contract (job types, payloads, ports, `[tool.beacon]`).
- Real workers to copy from: `scraper-example-python` (a site behind Cloudflare),
  `scraper-video-example-python` (a local folder as a site), `converter-video-example-python`
  (ffmpeg to HLS).
