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
| Level 1 — Remote calculation capacity | `max_tasks` | 100 | `MediaReport.__init__` (reports.py:95); `asyncio.Queue` maxsize (reports.py:117); worker pool (reports.py:206-208) | In-flight tasks in the remote Mediascope calculation base |
| Level 2 — Client HTTP concurrency | `max_concurrent_requests` | 5 | `NetworkClient.__init__` (network.py:58); `asyncio.Semaphore` (network.py:67) | Concurrent HTTP requests issued by the client |

### Level 1 — Remote calculation capacity (max_tasks)

`MediaReport.__init__` accepts `max_tasks: int = 100` (reports.py:95). It stores the
value at `self.max_tasks = max_tasks` (reports.py:116) and uses it to bound the task
queue: `self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (reports.py:117).
`processing_tasks` creates one worker coroutine per slot:
`task_group = [asyncio.create_task(self.worker(timeout=timeout)) for _ in range(self.max_tasks)]`
(reports.py:206-208).

Each worker owns one task end-to-end: it pulls an item from the queue (reports.py:173),
calls `_process_item` which calls `send_task` (reports.py:146), optionally calls
`wait_task` (reports.py:148), optionally calls `extract_data` (reports.py:150), then
calls `task_done()` in `finally` (reports.py:199-200). This level bounds how many
calculations run concurrently in the remote base — it is the submit-then-poll
window.

### Level 2 — Client HTTP concurrency (max_concurrent_requests)

`NetworkClient.__init__` accepts `max_concurrent_requests: int = 5` (network.py:58)
and creates `self._semaphore = asyncio.Semaphore(max_concurrent_requests)` (network.py:67).
Most API calls route through `NetworkClient.call`, which acquires the permit only for the
duration of the actual HTTP call. The call itself delegates to `_invoke` (network.py:74),
which runs `func` in a worker thread via `asyncio.to_thread` and bounds it with
`asyncio.wait_for(timeout=self._call_timeout)`:

```python
async with self._semaphore:          # network.py:110
    return await self._invoke(func, *args, **kwargs)  # network.py:111
```

**CPU/local calls bypass the semaphore entirely.** `NetworkClient.call` accepts an
`api_call: bool = True` parameter (network.py:98); when `api_call=False`, the method
returns `await self._invoke(func, *args, **kwargs)` immediately (network.py:101-102)
— *before* the initial jitter sleep, the `asyncio.Semaphore`, and the retry loop.
Such calls are still bounded by `_invoke`'s `asyncio.wait_for(timeout=self._call_timeout)`
(default 300 s, network.py:78) but skip all concurrency-limiting and retry machinery.
This discriminator exists because some vendor calls are pure CPU (e.g. `build_task`,
`result2table`) or local constructor calls (e.g. `get_connection`); they never hit the
network and would only add spurious semaphore contention, so they are short-circuited
to `_invoke`'s thread offload + timeout.

The smoothing sleep `await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))`
(network.py:107) runs **before** the permit is acquired, so a polling worker holds
a permit only for the brief HTTP status call and sleeps outside the semaphore the
rest of the cycle. Combined with `asyncio.sleep(sleep_interval)` between polls
(reports.py:411), this produces a staggered polling cadence.

Poll calls pass `sleep_time=2` (reports.py:413-414), yielding a uniform 1–4 s
smoothing delay — the same as regular calls, which use the default `sleep_time=2`
(network.py:96), yielding 1–4 s smoothing. Level 2 thus bounds both HTTP
concurrency and per-call smoothing at a 1–4 s granularity.

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
worker loop (reports.py:161)
  └── get item from queue            (reports.py:173)
  └── _process_item                  (reports.py:138)
      ├── send_task                  (reports.py:146)
      │   ├── builder call           (reports.py:365, api_call=False)
      │   └── sender call            (reports.py:370, retryable=False)
      │   [task_stage_complete stage=send, reports.py:372-377]
      ├── wait_task (poll loop)      (reports.py:148)
      │   ├── get_status poll call   (reports.py:413-414, sleep_time=2)
      │   └── asyncio.sleep backoff  (reports.py:411)
      │   [task_stage_complete stage=wait, reports.py:435-440]
      └── extract_data               (reports.py:150)
          ├── get_result call        (reports.py:449-451)
          ├── result2table call      (reports.py:454-457, api_call=False)
          │   [task_stage_complete stage=extract — reports.py:481-486]
          └── csv_to_file via to_thread  (reports.py:495)
      [task_complete (end-to-end), reports.py:153-159]
```

### Call-site reference

| Call | Source location | `sleep_time` param | `api_call` | Effective pre-call smoothing |
|---|---|---|---|---|
| `builder` | reports.py:362-364 | default (2) | **False** | bypassed (no jitter, no semaphore) |
| `sender` | reports.py:367 | default (2), `retryable=False` | default (True) | uniform 1–4 s |
| `get_status` (poll) | reports.py:410-412 | 2 | default (True) | uniform 1–4 s |
| `get_result` | reports.py:446-448 | default (2) | default (True) | uniform 1–4 s |
| `result2table` | reports.py:454-457 | default (2) | **False** | bypassed (no jitter, no semaphore) |
| `get_connection` | services.py:186-190 | — | **False** | bypassed (no jitter, no semaphore) |
| `get_availability_period` | reports.py:284-286 | default (2) | default (True) | uniform 1–4 s |
| `get_tv_region` | reports.py:554-556 | default (2) | — (direct vendor call) | N/A (no `NetworkClient`) |

All calls are bounded by `_invoke` (network.py:74), which applies `asyncio.wait_for(timeout=self._call_timeout)`
(default 300 s, network.py:63). Calls with `api_call=False` (`builder`, `result2table`,
`get_connection`) still route through `_invoke` (so they remain thread-offloaded and
timeout-bounded) but skip the initial jitter sleep, the `asyncio.Semaphore`, and the
retry loop entirely (network.py:101-102). The task-submission POST (`sender`,
reports.py:367) passes `retryable=False` — it is **not** `api_call=False`; it keeps
the semaphore + jitter but executes exactly once with no retries to prevent duplicate
task creation.

### Retry and backoff

`NetworkClient.call` retries transient errors up to `max_attempts=10` (network.py:62)
with exponential backoff + jitter:

```python
base = 1.2                                    # network.py:170
backoff = _sleep_time * (base ** (attempt - 1))  # network.py:171
jitter = uniform(0, 5)                        # network.py:172
await asyncio.sleep(backoff + jitter)         # network.py:173
```

Retryable conditions:
- Members of `RETRIABLE_EXCEPTIONS` (network.py:32–44): transport errors such as
  `ConnectTimeout`, `ConnectionError`, `Timeout`, `socket.timeout`, and `TimeoutError`
  (from `asyncio.wait_for`, network.py:78)
- HTTP 429 (`TooManyRequestsError`) — handled by a dedicated `except` clause
   (network.py:143–148), retried with exponential backoff (the vendor does not
   expose the `Retry-After` header)

Client errors raise immediately without retry:
- `BadRequestError` — HTTP 400, configuration is wrong
- `AuthorizationError` — HTTP 401, token invalid/expired
- `FileNotFoundError` — local file missing (credentials)
- Other `MediascopeApiError` — HTTP 4xx (except 429) and 5xx when `retry_5xx=False` (default)

When `retry_5xx=True`, 5xx responses (`MediascopeApiError` with `code >= 500`) are
retried instead of raised immediately (network.py:150–160). HTTP 429 is always
retried with exponential backoff — the vendor does not expose the `Retry-After`
header (network.py:139–144).

The task-submission POST (`sender`, reports.py:370) passes `retryable=False`, so it
executes exactly once with no retries. Separately, `builder` (reports.py:365-367) and
`result2table` (reports.py:478-481) pass `api_call=False`, which bypasses the entire
`call` wrapper — jitter, semaphore, and retry loop — returning through `_invoke`
directly (network.py:101-102).

Error-log messages redact call args and kwargs by default via `_redact_args`
(network.py:81); set `log_errors_detail=True` to include them for debugging.

After all attempts are exhausted, `MaxRetriesExceeded("Network retry limit
exceeded")` is raised (network.py:179). Since `MaxRetriesExceeded` subclasses
`RuntimeError`, existing `pytest.raises(RuntimeError)` assertions remain valid.
The previous `except HTTPError` handler was removed — the vendor client never raises
`requests.HTTPError`, so that branch was dead code.

### Change guidance

| What to tune | How |
|---|---|
| Increase remote calculation concurrency | Raise `max_tasks` in `MediaReport.__init__` (reports.py:94). No change to Level 2 required. |
| Reduce client HTTP load | Lower `max_concurrent_requests` in `NetworkClient.__init__` (network.py:58). No change to Level 1 required. |
| Adjust polling cadence | Change `sleep_time=2` on the `get_status` poll (reports.py:413-414) for polls; the default `sleep_time=2` applies to all other calls. |
| **Never do** | Do **not** derive `max_tasks = max_concurrent_requests`. Coupling the two levels collapses throughput (~20× slower for 100-task reports). |

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [core/reports.py](../../src/mko_get_mediascope_data/core/reports.py)
- [core/network.py](../../src/mko_get_mediascope_data/core/network.py)
