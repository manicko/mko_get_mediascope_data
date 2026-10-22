---
id: async-concurrency-architecture
domain: overview
tags:
  - architecture
  - concurrency
  - async
  - pipeline
related:
  - doc-maintenance-rules
  - settings-layers-architecture
  - product-overview
  - current-behavior
  - data-flow-architecture
  - system-architecture
  - open-questions
---

## Purpose

Documents the two-level concurrency model that drives the async report pipeline in
`core/reports.py` and `core/network.py`: a worker pool that bounds remote calculation
capacity, and a client-side `asyncio.Semaphore` that bounds HTTP concurrency. The two
levels are intentionally independent and must not be coupled.

## Main Concepts

The report pipeline is two-stage. First, up to `max_tasks` report tasks are submitted
to the Mediascope remote base for calculation. Second, each task's readiness is polled
until a terminal status is reached, after which the result is fetched and exported to
CSV. The two stages have different concurrency requirements and are therefore limited at
two independent levels — a worker pool (Level 1) and a client-side
`asyncio.Semaphore` (Level 2).

| Level | Knob | Default | Where enforced | What it limits |
|---|---|---|---|---|
| Level 1 — Remote calculation capacity | `max_tasks` | 100 | `MediaReport.__init__` (reports.py:114); `asyncio.Queue` maxsize (reports.py:141); worker pool (reports.py:277-280) | In-flight tasks in the remote Mediascope calculation base |
| Level 2 — Client HTTP concurrency | `max_concurrent_requests` | 5 | `NetworkClient.__init__` (network.py:103); `asyncio.Semaphore` (network.py:110) | Concurrent HTTP requests issued by the client |

### Level 1 — Remote calculation capacity (max_tasks)

`MediaReport.__init__` accepts `max_tasks: int = 100` (reports.py:114). It stores the
value at `self.max_tasks = max_tasks` (reports.py:138) and uses it to bound the task
queue: `self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (reports.py:141).
`processing_tasks` creates one worker coroutine per slot:
`task_group = [asyncio.create_task(self.worker(timeout=timeout)) for _ in range(self.max_tasks)]`
(reports.py:277-280).

Each worker owns one task end-to-end: it pulls an item from the queue (reports.py:208),
calls `_process_item` which calls `send_task` (reports.py:168), optionally calls
`wait_task` (reports.py:171), optionally calls `extract_data` (reports.py:174), then
calls `task_done()` in `finally` (reports.py:257). This level bounds how many
calculations run concurrently in the remote base — it is the submit-then-poll
window.

### Level 2 — Client HTTP concurrency (max_concurrent_requests)

`NetworkClient.__init__` accepts `max_concurrent_requests: int = 5` (network.py:103)
and creates `self._semaphore = asyncio.Semaphore(max_concurrent_requests)`
(network.py:110). Most API calls route through `NetworkClient.call`, which acquires a
permit before each attempt and **defers releasing it until the dispatched synchronous
operation actually completes** — not merely until the async wait is cancelled. The call
itself delegates to `_invoke` (network.py:117), which offloads `func` to a worker thread
via `asyncio.ensure_future(asyncio.to_thread(...))` and bounds it with
`asyncio.wait_for(asyncio.shield(inner), timeout=self._call_timeout)`. When the wait
times out, `wait_for` cancels only the outer wait while `asyncio.shield` keeps the inner
thread task alive; `_invoke` then raises `CallTimeoutError` (a `TimeoutError` subclass,
`core/errors.py:16`) carrying `thread_task` — the retained inner task — so the permit
can be released when the thread truly finishes:

```python
# _attempt_with_permit (network.py:149): permit held until the thread completes
deferred_release = False
await self._semaphore.acquire()
try:
    return await self._invoke(func, *args, **kwargs)
except CallTimeoutError as err:
    if err.thread_task is not None:
        err.thread_task.add_done_callback(self._drain_thread_and_release)
    deferred_release = True
    raise
finally:
    if not deferred_release:
        self._semaphore.release()
```

**CPU/local calls bypass the semaphore entirely.** `NetworkClient.call` accepts an
`api_call: bool = True` parameter (network.py:188); when `api_call=False`, the method
returns `await self._invoke(func, *args, **kwargs)` immediately (network.py:226-228)
— *before* the initial jitter sleep, the `asyncio.Semaphore`, and the retry loop.
Such calls are still bounded by `_invoke`'s `asyncio.wait_for(asyncio.shield(inner),
timeout=self._call_timeout)` (default 300 s, network.py:113). Because `api_call=False`
calls acquire no permit, on `CallTimeoutError` they attach a drain-only callback
(`_consume_thread_result`, network.py:137) that consumes the retained thread's terminal
exception and re-raises, so the caller still observes a `TimeoutError` without an
unretrieved-exception warning (network.py:229-232). They skip all concurrency-limiting
and retry machinery.
This discriminator exists because some vendor calls are pure CPU (e.g. `build_task`,
`result2table`); they never hit the network and would only add spurious semaphore
contention, so they are short-circuited to `_invoke`'s thread offload + timeout.
The `get_connection` constructor call is **not** local: the vendor
`MediaVortexTask`/`MediaVortexCats` constructor performs authenticated catalog I/O
during construction, so it now uses the normal API classification (retry loop
enabled) instead of `api_call=False`.

The smoothing sleep `await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))`
(network.py:238) runs **before** the permit is acquired, so a polling worker holds
a permit only for the brief HTTP status call and sleeps outside the semaphore the
rest of the cycle. Combined with `asyncio.sleep(sleep_interval)` between polls
(reports.py:566), this produces a staggered polling cadence.

| Call-site | `sleep_time` param | Effective pre-call smoothing |
|---|---|---|
| `get_status` (poll) | 30 (reports.py:570) | uniform 15–60 s |
| Default (other API calls) | 2 (network.py:104) | uniform 1–4 s |

Level 2 thus bounds both HTTP concurrency and per-call smoothing. The poll cadence
in `wait_task` follows an increasing schedule: 30 → 50 → 60 s (capped at 60),
avoiding both initial over-polling and excessive wait for completion.

### Why the two levels are independent

The two parameters limit different resources:

- **Level 1** bounds the in-flight task count against the remote Mediascope calculation
  engine.
- **Level 2** bounds the client's concurrent HTTP connection budget.

Because the resources are independent, the levels must be tuned independently. Deriving
`max_tasks` from `max_concurrent_requests` would collapse remote-base capacity from 100 to
 5, serializing a 100-task report into ~20 sequential batches (~20× slower) and
 destroying the submit-then-poll overlap. This intentional independence is exactly why
 audit finding ASY-004 was re-characterized to "not substantiated."

### Task lifecycle

Each task flows through the worker as follows:

```text
worker loop (reports.py:207)
  └── get item from queue            (reports.py:208)
  └── _process_item                  (reports.py:177)
      ├── send_task                  (reports.py:168)
      │   ├── builder call           (reports.py:480, api_call=False)
      │   └── sender call            (reports.py:499, config-driven retry)
      │   [task_stage_complete stage=send, reports.py:504-513]
      ├── wait_task (poll loop)      (reports.py:171)
      │   ├── get_status poll call   (reports.py:568, sleep_time=self.poll.sleep_time=30)
      │   └── asyncio.sleep backoff  (reports.py:566)
      │   [task_stage_complete stage=wait, reports.py:596-602]
      └── extract_data               (reports.py:174)
          ├── get_result call        (reports.py:611)
          ├── result2table call      (reports.py:619, api_call=False)
          │   [task_stage_complete stage=extract — reports.py:651-657]
          └── csv_to_file via to_thread  (reports.py:673-680, atomic publication — PIPE-012)
      [task_complete (end-to-end), reports.py:190-198]
```

The tree above describes the per-item pipeline inside a single worker. How the worker
group is created, driven, and torn down — including early-exit and cancellation safety —
is described in the next section.

### Worker lifecycle and report shutdown

`MediaReport.processing_tasks` owns the worker group for the entire report lifetime.
The worker group is created first; then the **entire** remaining lifecycle — task
generation, `report_started`, enqueue, and `queue.join()` — is enclosed in a single
`try/finally`, so the workers are always cancelled and gathered on every exit path:
normal drain, generation failure, cancellation while blocked on `queue.put`, zero
tasks, and the all-skipped path.

```text
processing_tasks (reports.py:291)
  └── create worker group           (reports.py:309-312)  — one worker per slot
  └── reported_started = False      (reports.py:314)
  └── try:
        ├── generate_tasks ()       (reports.py:317-320)  — materialise all tasks
        ├── total == 0 → NoReportFoundError (reports.py:324-330)
        ├── _disambiguate_task_names (reports.py:332-335)  — PIPE-014, after zero-check, before report_started
        ├── report_started (total)  (reports.py:337-338)  — flips reported_started=True
        ├── enqueue loop (put)      (reports.py:342-353)
        ├── enqueued == 0 → return  (reports.py:355-360)  — all-skipped, early exit
        └── queue.join ()           (reports.py:362)
    finally:
        ├── cancel + gather workers  (reports.py:363-366)  — always runs
        ├── report_completed (iff reported_started) (reports.py:367-368)
        └── WorkerFailureError supervision (reports.py:372-379)
```

**Exit-path accounting (ASY-010).** The `try` opens immediately after worker-group
creation and the trivial `report_id` / `reported_started = False` initialization
(reports.py:309-315) — only those non-failing setup lines sit between creation
and `try:`. This is deliberate: a failure during `generate_tasks()` or a `CancelledError`
while a worker is blocked on `await task_queue.put()` (queue full) is covered by the
enclosing `finally` (reports.py:363-379), which always cancels and gathers the worker
group, so no worker outlives the report. (The earlier `finally` only covered
`queue.join()`.)

**`report_completed` ordering.** `report_completed` fires only when `report_started`
already fired, tracked by the `reported_started` flag (reports.py:314), set to `True`
solely at the `report_started` call site (reports.py:338). The zero-task path raises
`NoReportFoundError` *before* `report_started` (flag still `False`), so
`report_completed` is never signalled for a report that never started — preventing a
spurious "completed" from masking the original error. That `NoReportFoundError`
(and the one raised by `get_period` for an out-of-range period) is caught per entry
by `AppService.run_report`, which logs a warning and continues to the next entry
(PIPE-013), so a missing-period report does not abort a multi-entry run.

**`WorkerFailureError` supervision (ASY-008).** The `finally` block first cancels and gathers all
workers (reports.py:327-329), then calls `report_completed` (only when
`reported_started`, reports.py:330-331), then runs the fail-fast check. The check is
guarded by `sys.exc_info()[0] is None` (reports.py:335): it only raises
`WorkerFailureError` (`core/errors.py:46`) when no other exception is already in flight.
This avoids masking a real error (generation failure, `CancelledError`,
`NoReportFoundError`) that is already propagating; a worker that died unexpectedly with
no in-flight exception is surfaced as
`WorkerFailureError("Worker exited unexpectedly: …") from <worker-exception>`, chaining
the original cause.

**`_write_error_artifact` isolation (ASY-008).** Each worker's three error paths — the
failed-task branch (`reports.py:227`), the `TimeoutError` branch (`reports.py:244`), and
the `Exception` branch (`reports.py:255`) — write their error artifact through
`_write_error_artifact` (reports.py:149-161), which calls `TVTask.to_file` (`tasks.py:30`)
in a worker thread. The helper wraps persistence in its own `try/except Exception`,
logging once via the module logger (`reports.py:36`) and never propagating. Because the
worker's outer `finally` always calls `task_done()` (`reports.py:257`), a persistence
failure can no longer kill a worker or strand `queue.join()`: every dequeued item still
reaches `task_done()`, and any remaining queued items are drained by the still-living
workers. This is a fail-fast pool policy — a secondary write failure is logged once and
the report continues with the remaining items rather than hanging.

**Per-item vs. report-level isolation.** Per-item isolation lives in the worker
(`try/except/finally` with `task_done()` in `finally`, see the task lifecycle tree
above); report-level isolation lives in `processing_tasks` (`try/finally` over the whole
coordinator lifetime). The two layers are independent: neither a secondary
error-artifact write (ASY-008) nor an unexpected worker exit (ASY-008
`WorkerFailureError`) can strand `queue.join()`.

### Call-site reference

| Call | Source location | `sleep_time` param | `api_call` | Effective pre-call smoothing |
|---|---|---|---|---|
| `builder` | reports.py:480-482 | default (2) | **False** | bypassed (no jitter, no semaphore) |
| `sender` | reports.py:499-502 | default (2), config-driven retry | default (True) | uniform 1–4 s |
| `get_status` (poll) | reports.py:568-570 | 30 (`self.poll.sleep_time`) | default (True) | uniform 15–60 s |
| `get_result` | reports.py:611-613 | default (2) | default (True) | uniform 1–4 s |
| `result2table` | reports.py:619-622 | default (2) | **False** | bypassed (no jitter, no semaphore) |
| `get_connection` (via `_get_connection_with_attempt_cleanup`) | services.py:281-286 | default (2), transport-only `retry_on` (`_CONSTRUCTOR_RETRIABLE`) | default (True) | uniform 1–4 s |
| `get_availability_period` | reports.py:398-400 | default (2) | default (True) | uniform 1–4 s |
| `get_tv_region` | reports.py:720-722 | default (2) | default (True) | uniform 1–4 s |

All calls are bounded by `_invoke` (network.py:117), which offloads the synchronous
function to a thread and bounds it with `asyncio.wait_for(asyncio.shield(inner),
timeout=self._call_timeout)` (default 300 s, network.py:113). A timeout raises
`CallTimeoutError` (a `TimeoutError` subclass, so it is classified as retryable by
`RETRIABLE_EXCEPTIONS`) and retains the inner thread task so the permit can be released
on actual completion (network.py:127-134). Calls with `api_call=False` (`builder`, `result2table`)
still route through `_invoke` (so they remain thread-offloaded and
timeout-bounded) but skip the initial jitter sleep, the `asyncio.Semaphore`, and the
retry loop entirely (network.py:226-228). `get_connection` is a constructor but not a
local call — it performs authenticated catalog I/O during construction, so it now enters
the normal retry path with a transport-only `retry_on` tuple (`_CONSTRUCTOR_RETRIABLE`,
`RETRIABLE_EXCEPTIONS` minus `TimeoutError` subclasses). Transient transport errors during
constructor catalog startup are retried (bounded by `max_attempts`); permanent errors
(`BadRequestError`/`AuthorizationError`/`FileNotFoundError`) re-raise immediately, and
`CallTimeoutError`, HTTP 429, and 5xx are not retried. The task-submission POST (`sender`,
reports.py:499) is config-driven via `SendTaskSettings` — it is **not** `api_call=False`;
by default it enters the application retry loop for `RETRIABLE_EXCEPTIONS`
(`ConnectionError`, `Timeout` including `ReadTimeout`) while HTTP 429/5xx are
re-raised, and `retryable=False` makes it a single attempt. Retrying a selected
transport error does not prove the request body was unapplied, and the vendor's
independent `urllib3.Retry` layer can replay a POST regardless of the application
retry settings, so unique task creation is best-effort (D-16).

### Retry and backoff

`NetworkClient.call` retries transient errors up to `max_attempts=10` (network.py:105)
with exponential backoff + jitter:

```python
base = 1.2                                    # network.py:336
backoff = _sleep_time * (base ** (attempt - 1))  # network.py:337
jitter = uniform(0, 5)                        # network.py:338
await asyncio.sleep(backoff + jitter)         # network.py:339
```

`call()` accepts an optional `task_name: str | None = None` parameter (`network.py:190`).
When provided, every retry/backoff log message appends `task=<task_name>` (via `_task_ctx`
at `network.py:235`), so transient errors and the final "Max retries exceeded" failure are
traceable to their originating task. All per-task call sites in `reports.py` pass
`task_name=task.name`; calls without a task context (e.g. the `get_connection` constructor call
through `_get_connection_with_attempt_cleanup` at `services.py:285`) pass `None`, leaving log messages unchanged.

Retryable conditions:
- Members of `RETRIABLE_EXCEPTIONS` (network.py:34–47): transport errors such as
  `ConnectTimeout`, `ConnectionError`, `Timeout`, `socket.timeout`, and `TimeoutError`
  — including `CallTimeoutError` raised by `_invoke` when `asyncio.wait_for` times out
  on the shielded thread future; because `CallTimeoutError` subclasses `TimeoutError`, it
  is classified as retryable (`core/errors.py:16`, network.py:42)
- HTTP 429 (`TooManyRequestsError`) — handled by a dedicated `except` clause
   (network.py:286–300), retried with exponential backoff (the vendor does not
   expose the `Retry-After` header)

Client errors raise immediately without retry:
- `BadRequestError` — HTTP 400, configuration is wrong
- `AuthorizationError` — HTTP 401, token invalid/expired
- `FileNotFoundError` — local file missing (credentials)
- Other `MediascopeApiError` — HTTP 4xx (except 429) and 5xx when `retry_5xx=False`.
  5xx (`ServerError`, HTTP 500/502/503/504) is retried by default (`retry_5xx=True`)
  and can be disabled by setting `retry_5xx=False`.

When `retry_5xx=True`, 5xx responses (`MediascopeApiError` with `code >= 500`) are
retried instead of raised immediately (network.py:314–325). HTTP 429 is always
retried with exponential backoff — the vendor does not expose the `Retry-After`
header (network.py:286–300).

The task-submission POST (`sender`, reports.py:499) is config-driven via
`SendTaskSettings`: by default it enters the application retry loop for
`RETRIABLE_EXCEPTIONS` (`ConnectionError`, `Timeout` including `ReadTimeout`),
while HTTP 429/5xx are re-raised; `retryable=False` makes it a single attempt.
Because a selected transport error does not prove the request body was
unapplied, and the vendor's independent `urllib3.Retry` layer can replay a POST
regardless of application retries, unique task creation is best-effort (D-16).
Separately, `builder` (reports.py:480-482) and `result2table` (reports.py:619-622) pass `api_call=False`, which bypasses the entire
`call` wrapper — jitter, semaphore, and retry loop — returning through `_invoke`
directly (network.py:226-228).

Error-log messages redact call args and kwargs by default via `_redact_args`
(network.py:171); set `log_errors_detail=True` to include them for debugging.

After all attempts are exhausted, `MaxRetriesExceeded("Network retry limit
exceeded")` is raised (network.py:346). Since `MaxRetriesExceeded` subclasses
`RuntimeError`, existing `pytest.raises(RuntimeError)` assertions remain valid. The
last timed-out attempt's thread may still be running when this propagates; its retained
permit is released by the `_drain_thread_and_release` done-callback when the thread
actually finishes, so the caller does **not** wait for it. The previous `except HTTPError`
handler was removed — the vendor client never raises `requests.HTTPError`, so that
branch was dead code.

### Vendor session lifecycle (EXT-010 / EXT-011)

The vendor constructor `MediaVortexTask.__init__` allocates two `requests.Session`
objects (one via `network_module`, one via `cats.msapi_network`) as side effects
of authenticated catalog I/O during construction.  Because the vendor exposes no
close lifecycle, those sessions were previously left to garbage collection with no
deterministic release guarantee.  `core/vendor_lifecycle.py` is the adapter seam
that owns that decision:

| Function | Role |
|---|---|
| `capture_sessions` | Snapshots the two sessions (and their owning vendor networks) right after a successful construction, so release can re-check ownership by identity. |
| `capture_singleton_sessions` | Snapshots the sessions from the partially-initialized vendor singletons when the constructor raised mid-way (no `MediaVortexTask` instance was returned). |
| `release_sessions` | Closes the captured sessions idempotently (`WeakSet`-guarded), skipping any session whose owner has already been replaced by a later entry, and skipping entirely when deferred work is outstanding. |

**Report-boundary release contract.**  `AppService.run_report` captures sessions
after the constructor succeeds (line `services.py:287`) and releases them in a
`finally` block on **every** exit path: success, constructor partial failure
(`captured is None` → `capture_singleton_sessions()`), execution failure, and the
`NoReportFoundError` skip path (`services.py:330-335`).

**Deferred-work guard (reuses ASY-009).**  The release is guarded by
`network_client.has_outstanding_deferred()` (`services.py:334`): when a timed-out
`asyncio.to_thread` future is still running (the `CallTimeoutError` path that
retains the inner thread via `asyncio.shield`), the sessions it may still hold are
left intact and a warning is logged instead of closing beneath in-flight work.

**Per-attempt cleanup (EXT-011).**  The constructor is invoked through
`AppService._get_connection_with_attempt_cleanup` (`services.py:208-238`), which
wraps `get_connection` so that each retried attempt's intermediate-attempt
sessions are captured via `capture_singleton_sessions()` and released via
`release_sessions()` (again respecting the deferred-work guard) before the
original exception propagates to `NetworkClient`'s retry loop.  The
`WeakSet`-based idempotency in `release_sessions` ensures the per-entry
`finally` does not close a session already closed by an intermediate attempt.

### Change guidance

| What to tune | How |
|---|---|
| Increase remote calculation concurrency | Raise `max_tasks` in `MediaReport.__init__` (reports.py:114). No change to Level 2 required. |
| Reduce client HTTP load | Lower `max_concurrent_requests` in `NetworkClient.__init__` (network.py:103). No change to Level 1 required. |
| Adjust polling cadence | Change `sleep_time=30` on the `get_status` poll (reports.py:570) for smoothing; the poll-interval ramp in `wait_task` (reports.py:589-594) follows 30 → 50 → 60 s. |
| **Never do** | Do **not** derive `max_tasks = max_concurrent_requests`. Coupling the two levels collapses throughput (~20× slower for 100-task reports). |

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [core/reports.py](../../src/mko_get_mediascope_data/core/reports.py)
- [core/network.py](../../src/mko_get_mediascope_data/core/network.py)
- [core/vendor_lifecycle.py](../../src/mko_get_mediascope_data/core/vendor_lifecycle.py)
- [core/errors.py](../../src/mko_get_mediascope_data/core/errors.py)
