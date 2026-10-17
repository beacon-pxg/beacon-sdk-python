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

Status: v0.3, the whole contract of `docs/05-workers.md` §6.
