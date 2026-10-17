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
| Level 1 — Remote calculation capacity | `max_tasks` | 100 | `MediaReport.__init__` (reports.py:94); `asyncio.Queue` maxsize (reports.py:116); worker pool (reports.py:194-197) | In-flight tasks in the remote Mediascope calculation base |
| Level 2 — Client HTTP concurrency | `max_concurrent_requests` | 5 | `NetworkClient.__init__` (network.py:51); `asyncio.Semaphore` (network.py:57) | Concurrent HTTP requests issued by the client |

### Level 1 — Remote calculation capacity (max_tasks)

`MediaReport.__init__` accepts `max_tasks: int = 100` (reports.py:94). It stores the
value at `self.max_tasks = max_tasks` (reports.py:115) and uses it to bound the task
queue: `self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (reports.py:116).
`processing_tasks` creates one worker coroutine per slot:
`task_group = [asyncio.create_task(self.worker(timeout=timeout)) for _ in range(self.max_tasks)]`
(reports.py:194-197).

Each worker owns one task end-to-end: it pulls an item from the queue (reports.py:157),
calls `_process_item` which calls `send_task` (reports.py:143), optionally calls
`wait_task` (reports.py:145), optionally calls `extract_data` (reports.py:147), then
calls `task_done()` in `finally` (reports.py:188). This level bounds how many
calculations run concurrently in the remote base — it is the submit-then-poll
window.

### Level 2 — Client HTTP concurrency (max_concurrent_requests)

`NetworkClient.__init__` accepts `max_concurrent_requests: int = 5` (network.py:51)
and creates `self._semaphore = asyncio.Semaphore(max_concurrent_requests)` (network.py:57).
Every API call routes through `NetworkClient.call`, which acquires the permit only for the
duration of the actual HTTP call:

```python
async with self._semaphore:          # network.py:73
    return await asyncio.to_thread(func, *args, **kwargs)  # network.py:74
```

The smoothing sleep `await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))`
(network.py:71) runs **before** the permit is acquired (network.py:75), so a polling
worker holds a permit only for the brief HTTP status call and sleeps outside the
semaphore the rest of the cycle. Combined with `asyncio.sleep(2)` between polls
(reports.py:388), this produces the required slow, staggered polling cadence.

Poll calls pass `sleep_time=40` (reports.py:390-392), yielding a uniform 20–80 s
delay; regular calls use the default `sleep_time=2` (network.py:65), yielding 1–4 s
smoothing. Level 2 is therefore what enforces "sequential polling with large delays."

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
worker loop (reports.py:149)
  └── get item from queue            (reports.py:157)
  └── _process_item
      ├── send_task                  (reports.py:143)
      │   ├── builder call           (reports.py:350)
      │   └── sender call            (reports.py:355)
      ├── wait_task (poll loop)      (reports.py:145)
      │   ├── get_status poll call   (reports.py:390, sleep_time=40)
      │   └── asyncio.sleep backoff  (reports.py:388)
      └── extract_data               (reports.py:147)
          ├── get_result call        (reports.py:418)
          ├── result2table call      (reports.py:426)
          └── csv_to_file via to_thread  (reports.py:442)
```

### Call-site reference

| Call | Source location | `sleep_time` param | Effective pre-call smoothing |
|---|---|---|---|
| `builder` | reports.py:350 | default (2) | uniform 1–4 s |
| `sender` | reports.py:355 | default (2) | uniform 1–4 s |
| `get_status` (poll) | reports.py:390-392 | 40 | uniform 20–80 s |
| `get_result` | reports.py:418 | default (2) | uniform 1–4 s |
| `result2table` | reports.py:426 | default (2) | uniform 1–4 s |
| `get_availability_period` | reports.py:269 | default (2) | uniform 1–4 s |
| `get_tv_region` | reports.py:516 | default (2) | uniform 1–4 s |

### Retry and backoff

`NetworkClient.call` retries transient errors up to `max_attempts=10` (network.py:55)
with exponential backoff:

```python
base = 1.2                                    # network.py:123
backoff = _sleep_time * (base ** (attempt - 1))  # network.py:124
jitter = uniform(0, 5)                        # network.py:125
await asyncio.sleep(backoff + jitter)         # network.py:126
```

Client errors (`BadRequestError`, `AuthorizationError`, `FileNotFoundError`, HTTP 4xx)
raise immediately. After all attempts are exhausted, `MaxRetriesExceeded("Network retry limit
exceeded")` is raised (network.py:128). Since `MaxRetriesExceeded` subclasses
`RuntimeError`, existing `pytest.raises(RuntimeError)` assertions remain valid.

### Change guidance

| What to tune | How |
|---|---|
| Increase remote calculation concurrency | Raise `max_tasks` in `MediaReport.__init__` (reports.py:94). No change to Level 2 required. |
| Reduce client HTTP load | Lower `max_concurrent_requests` in `NetworkClient.__init__` (network.py:51). No change to Level 1 required. |
| Adjust polling cadence | Change `sleep_time=40` on the `get_status` poll (reports.py:390-392) for polls; the default `sleep_time=2` applies to all other calls. |
| **Never do** | Do **not** derive `max_tasks = max_concurrent_requests`. Coupling the two levels collapses throughput (~20× slower for 100-task reports). |

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [core/reports.py](../../src/mko_get_mediascope_data/core/reports.py)
- [core/network.py](../../src/mko_get_mediascope_data/core/network.py)
