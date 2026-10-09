---
phase: "03"
phase_name: "async-concurrency"
id_prefix: "ASY"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/03-audit-async-concurrency.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Async Concurrency & Worker Safety

## Summary

Seven findings across CRITICAL / HIGH / MEDIUM / LOW bands. The worker lifecycle has no exception isolation: any API error inside a worker skips `task_done()`, deadlocking `queue.join()` and hanging every invocation. A synchronous API call in the TV_REG strategy bypasses the Semaphore and blocks the event loop. Worker cancellation and the pool/semaphore sizing are not properly disciplined, and the queue depth is not observable.

## Scope

**Examined:** `core/reports.py` (`MediaReport.worker`, `MediaReport.processing_tasks`, `TVMediaReport.wait_task`, `TVRegTaskStrategy.generate`), `core/network.py` (`NetworkClient.__init__`, `NetworkClient.call`), `core/services.py` (`AppService.run_report` instantiation site), `core/tasks.py` (`TVTask`).

**Tools:** `grep` (symbol lookup), `read` (static source), static control-flow analysis of the async pipeline. No runtime execution available (phase instructions require Docker start, but this project has no Docker composition).

**Not verified:** Runtime behaviour under load or cancellation — no Docker services to start; tests mock `create_report` entirely and provide no coverage of the worker pool lifecycle.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| ASY-001 | CRITICAL | `core/reports.py::MediaReport.worker` | Worker exception skips `task_done()`, `queue.join()` deadlocks forever |
| ASY-002 | HIGH | `core/reports.py::TVRegTaskStrategy.generate` | Synchronous `get_tv_region()` call bypasses Semaphore, retry, and `to_thread` |
| ASY-003 | MEDIUM | `core/reports.py::MediaReport.processing_tasks` | Workers cancelled with bare `cancel()`, never awaited or gathered |
| ASY-004 | MEDIUM | `core/reports.py::MediaReport.__init__` | Pool size (100) and Semaphore permits (5) are decoupled — 95 workers perpetually blocked |
| ASY-005 | MEDIUM | `core/reports.py::TVMediaReport.wait_task` | `while True` poll loop has no timeout, worker can hang indefinitely |
| ASY-006 | MEDIUM | `core/reports.py::MediaReport.processing_tasks` | Queue depth is non-observable — no logging of backpressure or stall |
| ASY-007 | LOW | `core/network.py::NetworkClient.call` | Exhausted-retry raises generic `RuntimeError`, indistinguishable from permanent failures |

---

#### ASY-001 — [CRITICAL] Worker exception skips `task_done()`, `queue.join()` deadlocks forever

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker`
`src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `task_done`
**Checked:** static
**Why this band:** The CRITICAL taxonomy band is "a queue `task_done` that is never called so `join()` deadlocks forever, hanging every invocation." The worker has no `try/except/finally` around any of its `await` calls, so any exception — including the common `NetworkClient.call` `RuntimeError` after exhausted retries, or `BadRequestError`/`AuthorizationError` — propagates before `task_done()` is reached.
**Problem:** `MediaReport.worker` (line 141–153) has no error isolation. If `send_task`, `wait_task`, or `extract_data` raises, execution exits the `while True` body before `self.task_queue.task_done()` (line 153) is called. `processing_tasks` then never returns from `await self.task_queue.join()` (line 161), hanging every invocation.
**Repro:** 1. Configure a report whose `send_task` triggers an API error (e.g. invalid filter → `BadRequestError` from `NetworkClient.call`). 2. `send_task` raises at line 263 or 269. 3. The exception propagates out of `worker` — `task_done()` at line 153 is never reached. 4. `queue.join()` at line 161 waits indefinitely for the unacked item. 5. The process hangs until killed externally.
**Impact:** A single malformed task or transient API error (4xx, exhausted retries) hangs the entire report run. The on-call operator must kill the process; partial CSV output from completed tasks survives, but all unprocessed tasks and in-flight results are lost.
**Root cause:** No `try/finally` around the worker's queue-item processing to guarantee `task_done()` is always called.
**Fix:** Wrap the worker body in `try/finally` so `self.task_queue.task_done()` is always called, or use a bounded semaphore-pattern that releases on exception. Target symbol: `MediaReport.worker` — add a `finally: self.task_queue.task_done()` around the per-item processing block.
**Verify:** new test needed: `test_worker_exception_does_not_deadlock_join` — assert that after a worker raises, `queue.join()` still returns and all remaining items are processed.
**Related:** ASY-003, ASY-005

**Evidence — `grep` of `reports.py` for `task_done`, `while True`, `join`, `create_task`, `cancel`:**
```text
core/reports.py:92:         max_tasks: int = 100,
core/reports.py:115:         self.task_queue = asyncio.Queue(maxsize=self.max_tasks)
core/reports.py:141:         while True:
core/reports.py:150:                 await asyncio.to_thread(item.to_yaml, self.export_path)
core/reports.py:153:             self.task_queue.task_done()
core/reports.py:159:         task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]
core/reports.py:161:         await self.task_queue.join()
core/reports.py:162:         [t.cancel() for t in task_group]
core/reports.py:277:         while True:
core/reports.py:387:         available_regions = report.catalogs.get_tv_region()
```

**Evidence — `grep` of `network.py` for `RuntimeError`, `Semaphore`:**
```text
core/network.py:55:         self._semaphore = asyncio.Semaphore(max_concurrent_requests)
core/network.py:126:         raise RuntimeError("Network retry limit exceeded")
```

---

#### ASY-002 — [HIGH] Synchronous `get_tv_region()` call bypasses Semaphore, retry, and `to_thread`

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate`
**Disambiguator:** `get_tv_region`
**Checked:** static
**Why this band:** The HIGH taxonomy band includes exceeding the concurrency ceiling. `TVRegTaskStrategy.generate` calls `report.catalogs.get_tv_region()` (line 387) directly — a synchronous API call that bypasses `NetworkClient.call` entirely, meaning no `Semaphore` permit, no retry/backoff, and no `asyncio.to_thread` bridge. The event loop is blocked for the full duration of the API call.
**Problem:** At line 387, `available_regions = report.catalogs.get_tv_region()` executes a synchronous network call inside an `async def generate` method, which runs on the event loop thread during the producer phase of `processing_tasks`. This call: (a) holds no `Semaphore` — it is unbounded relative to the 5-permit ceiling; (b) has no retry/backoff — a single transient failure aborts the entire report; (c) blocks the event loop — no worker can make progress during the call.
**Repro:** 1. Configure a TV_REG report. 2. `TVMediaReport.generate_tasks` (line 335) delegates to `TVRegTaskStrategy.generate`. 3. Line 387 calls `get_tv_region()` synchronously. 4. During the (blocking) API call, all 100 worker coroutines and the producer are frozen. 5. If the API call fails, no retry is attempted — the report raises immediately.
**Impact:** TV_REG report generation has no concurrency control and no fault tolerance on this call path. Transient network blips abort the report. Event-loop blocking degrades throughput for all concurrent work. Sellers running TV_REG reports experience intermittent total failure.
**Root cause:** `TVRegTaskStrategy.generate` reaches the Mediascope API directly via `report.catalogs` instead of going through `NetworkClient.call`, which is the only code path that applies the `Semaphore`, retry/backoff, and `to_thread` bridge.
**Fix:** Route `get_tv_region()` through `self.network_client.call(self.catalogs.get_tv_region)`. Target symbol: `TVRegTaskStrategy.generate` — line 387.
**Verify:** new test needed: `test_tv_reg_strategy_routes_get_tv_region_via_network_client` — assert that `network_client.call` is invoked for the catalog lookup, not the raw method.
**Related:** ASY-004

**Evidence — `grep` of `reports.py` for `get_tv_region` (isolated call site):**
```text
core/reports.py:387:         available_regions = report.catalogs.get_tv_region()
```

*(All other API calls in `TVMediaReport` route through `self.network_client.call` — see lines 197, 258, 263, 280, 301, 309.)*

---

#### ASY-003 — [MEDIUM] Workers cancelled with bare `cancel()`, never awaited or gathered

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `t.cancel`
**Checked:** static
**Why this band:** The MEDIUM taxonomy band is "a cancellation path that is not awaited, risking partial state across Python versions." Line 162 calls `t.cancel()` on each worker but never awaits or gathers the results. Additionally, if `generate_tasks()` or `queue.join()` raises before line 162 is reached, the `task_group` is never cleaned up — workers are orphaned as pending tasks.
**Problem:** `processing_tasks` (line 155–162) creates 100 worker tasks and only cancels them with `[t.cancel() for t in task_group]` (line 162) — a fire-and-forget list comprehension that discards the return value and never `await`s. If `generate_tasks()` (line 160) raises mid-stream, the cancel at line 162 is never reached and all workers remain as dangling pending tasks with potentially in-flight `asyncio.to_thread` calls. Even in the normal flow, the cancellation is not confirmed before the method returns.
**Repro:** 1. Inject an exception during `generate_tasks` (e.g. a strategy that raises). 2. The async list comprehension at line 160 propagates the exception. 3. Line 162 is never executed. 4. The 100 workers created at line 159 remain pending — some may have called `task_queue.get()` and are mid-`send_task` with a live `asyncio.to_thread` blocking in a worker thread. 5. The exception propagates to `run_report` and `asyncio.run`, which performs a coarse `_cancel_all_tasks` at loop shutdown — but in-flight `to_thread` calls in C-extension-backed Mediascope API code may not be interruptible.
**Impact:** Resource leak across reports within a single invocation (workers accumulate). Abrupt shutdown may leave Mediascope API tasks orphaned on the remote side, continuing to consume server-side resources after the client process exits.
**Root cause:** No `try/finally` or `asyncio.gather(..., return_exceptions=True)` around the worker lifecycle to ensure graceful cancellation is awaited in all exit paths.
**Fix:** Replace line 162 with explicit cancellation and await: `for t in task_group: t.cancel(); await asyncio.gather(*task_group, return_exceptions=True)`. Wrap the entire `processing_tasks` body in `try/finally` so cleanup runs on any exit path. Target symbol: `MediaReport.processing_tasks`.
**Verify:** new test needed: `test_workers_cancelled_and_awaited_on_exception` — assert that when `generate_tasks` raises, all worker tasks are cancelled and joined within a bounded time.
**Related:** ASY-001

**Evidence — `grep` of `reports.py`:**
```text
core/reports.py:159:         task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]
core/reports.py:161:         await self.task_queue.join()
core/reports.py:162:         [t.cancel() for t in task_group]
```

---

#### ASY-004 — [MEDIUM] Pool size (100) and Semaphore permits (5) are decoupled

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.__init__`
`src/mko_get_mediascope_data/core/network.py::NetworkClient.__init__`
**Disambiguator:** `max_tasks`
**Checked:** static
**Why this band:** The pool size (100) and the Semaphore limit (5) are set independently with no relationship enforced. 95 of 100 worker coroutines are perpetually blocked at `async with self._semaphore` inside `NetworkClient.call`, creating a misleading concurrency ceiling. The MEDIUM band covers architectural drift that obscures the effective concurrency model.
**Problem:** `MediaReport.__init__` sets `max_tasks=100` (line 92) and creates `asyncio.Queue(maxsize=100)` (line 115) with 100 worker coroutines (line 159). `NetworkClient` is instantiated in `AppService.run_report` (line 130, `services.py`) with the default `max_concurrent_requests=5` (line 51 of `network.py`). No configuration propagates the Semaphore limit to the pool size — they are set by unrelated defaults. At any moment, up to 100 workers can be in the queue-processing state, but only 5 can hold a Semaphore permit to make an API call; the remaining 95 are suspended at `async with self._semaphore` (line 73 of `network.py`).
**Repro:** 1. Run a TV report with many intervals × target audiences (e.g. 100+ tasks). 2. Observe via `asyncio.all_tasks()` that ~100 worker coroutines exist but only ~5 are inside `NetworkClient.call` at any time. 3. The 95 blocked workers consume coroutine frames and block semaphore slots are never released faster than the 5-call ceiling allows.
**Impact:** The 100-worker pool creates a false impression of 100-way concurrency; actual API throughput is capped at 5. Debugging is harder because stack traces show 100 worker tasks but only 5 make progress. Resource waste is minimal (coroutine frames are cheap), but the disconnect between pool and Semaphore is a maintainability risk if either is changed independently.
**Root cause:** `max_tasks` (pool/queue) and `max_concurrent_requests` (Semaphore) are defined in separate classes (`MediaReport` and `NetworkClient`) with no shared configuration or assertion.
**Fix:** Derive the worker pool size from the `NetworkClient` Semaphore limit (e.g. `max_tasks = max_concurrent_requests` or a small multiple), or assert at startup that `max_tasks` does not exceed a reasonable multiple of the Semaphore limit. Target symbol: `MediaReport.__init__` parameter `max_tasks`.
**Verify:** new test needed: `test_pool_size_aligned_with_semaphore` — assert that `max_tasks` does not exceed `network_client._semaphore._bound_value` by an arbitrary constant.
**Related:** ASY-002

**Evidence — `services.py` instantiation site (no override of `max_concurrent_requests`):**
```text
core/services.py:130:             network_client: NetworkClient = NetworkClient()
```

**Evidence — `reports.py` pool and queue:**
```text
core/reports.py:92:         max_tasks: int = 100,
core/reports.py:114:         self.max_tasks = max_tasks
core/reports.py:115:         self.task_queue = asyncio.Queue(maxsize=self.max_tasks)
```

**Evidence — `network.py` Semaphore default:**
```text
core/network.py:49:     def __init__(
core/network.py:50:         self,
core/network.py:51:         max_concurrent_requests: int = 5,
core/network.py:55:         self._semaphore = asyncio.Semaphore(max_concurrent_requests)
```

---

#### ASY-005 — [MEDIUM] `while True` poll loop has no timeout

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task`
**Disambiguator:** `while True`
**Checked:** static
**Why this band:** The MEDIUM band covers robustness gaps that can escalate under adverse conditions. `wait_task` (lines 277–293) polls the Mediascope API in a `while True` loop with only a 2-second `asyncio.sleep` between checks and no timeout, deadline, or maximum iteration count. If the remote task never reaches a terminal status (`DONE`, `FAILED`, `CANCELLED`), the worker hangs indefinitely — never calling `task_done()`, deadlocking `join()`.
**Problem:** `TVMediaReport.wait_task` (lines 271–293) contains `while True:` (line 277) with no exit condition other than the API returning a terminal status. The loop sleeps 2 seconds between polls (`await asyncio.sleep(2)` at line 278) and calls `network_client.call` to check status. If the Mediascope API returns non-terminal status (e.g. `"RUNNING"`) indefinitely — due to a stuck remote task or a status-field change — the worker never returns, never calls `task_done()`, and `queue.join()` at line 161 never completes.
**Repro:** 1. Submit a task to the Mediascope API that enters `RUNNING` state but never completes (e.g. due to a server-side processing bug). 2. `wait_task` polls every 2 seconds, receiving `"RUNNING"` status each time. 3. The worker never breaks out of the loop. 4. `task_done()` at line 153 is never called for this item. 5. `queue.join()` waits forever for the unacked item. 6. All other workers eventually drain the queue and block on `get()`, but `join()` still never returns. 7. The process hangs until killed.
**Impact:** A single stuck remote task hangs the entire report run indefinitely. The operator has no way to interrupt or timeout the poll. This is more likely during Mediascope backend maintenance windows.
**Root cause:** No `asyncio.wait_for` deadline or iteration cap on the polling loop.
**Fix:** Wrap the poll loop in `asyncio.wait_for` with a configurable timeout (e.g. 30 minutes), or add a maximum iteration count. Target symbol: `TVMediaReport.wait_task`.
**Verify:** new test needed: `test_wait_task_terminates_on_timeout` — assert that `wait_task` raises or returns after a bounded number of poll iterations even if status never becomes terminal.
**Related:** ASY-001

**Evidence — `grep` of `reports.py` for `while True` (both worker and wait_task):**
```text
core/reports.py:141:         while True:
core/reports.py:277:         while True:
```

*(Line 141 is the `worker` loop — exits via cancellation. Line 277 is the `wait_task` poll loop — exits only via terminal status from the API.)*

---

#### ASY-006 — [MEDIUM] Queue depth is non-observable

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `task_queue.put`
**Checked:** static
**Why this band:** The MEDIUM taxonomy band includes "a queue depth that is unbounded or non-observable." The `Queue(maxsize=100)` is bounded, but no code path logs the queue depth, pending count, or producer-stall duration. When backpressure stalls the producer (the `async for` at line 160 blocks on a full queue), there is no signal.
**Problem:** `processing_tasks` (line 155–162) creates `Queue(maxsize=self.max_tasks)` (line 115, maxsize=100) and feeds it via `[await self.task_queue.put(t) async for t in self.generate_tasks()]` (line 160). When the queue is full, `put` blocks the producer, providing backpressure. However, nothing logs: (a) the current queue depth (`qsize()`); (b) when the producer enters a stall; (c) how long the stall lasts. The worker method (line 134–153) also has no logging of queue state.
**Repro:** 1. Configure a report with 200 tasks. 2. Run with 5 workers (Semaphore limit). 3. The producer fills the 100-slot queue and blocks on the 101st `put`. 4. No log message indicates backpressure. 5. An operator monitoring the run sees only the periodic `print("=")` progress dot from `send_task` (line 265) — which stops during the stall — with no indication of why.
**Impact:** Silent backpressure stalls are invisible to operators. Without observability, it is impossible to distinguish a slow API from a stalled producer or a deadlock. This compounds ASY-001 and ASY-005, where a hang would produce no diagnostic signal.
**Root cause:** No `qsize()` instrumentation in `processing_tasks` or `worker`.
**Fix:** Add `logger.debug` calls in `processing_tasks` before the producer loop and in the worker to log `self.task_queue.qsize()` when the queue crosses a threshold (e.g. 80% full) or when the producer stalls. Target symbol: `MediaReport.processing_tasks` and `MediaReport.worker`.
**Verify:** new test needed: `test_queue_depth_logged_under_backpressure` — assert that a debug log records `qsize()` when the queue is full and the producer blocks.
**Related:** ASY-001, ASY-005

**Evidence — `grep` of `reports.py` for `qsize`, `logger` in processing context:**
```text
core/reports.py:115:         self.task_queue = asyncio.Queue(maxsize=self.max_tasks)
core/reports.py:160:         [await self.task_queue.put(t) async for t in self.generate_tasks()]
core/reports.py:161:         await self.task_queue.join()
```

*(No `qsize()` usage or queue-depth logging appears anywhere in `reports.py`.)*

---

#### ASY-007 — [LOW] Exhausted-retry raises generic `RuntimeError`

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `RuntimeError`
**Checked:** static
**Why this band:** The LOW band covers observability and error-classification drift with no runtime consequence today. `NetworkClient.call` raises `RuntimeError("Network retry limit exceeded")` (line 126) after all retries are exhausted. This is a generic builtin that callers cannot catch specifically — any `except RuntimeError` elsewhere would also catch unrelated errors.
**Problem:** After `max_attempts` retries, `NetworkClient.call` raises `RuntimeError("Network retry limit exceeded")` (line 126). This is not a custom exception type — it is `RuntimeError`, the most generic built-in exception in Python. Callers in `MediaReport.worker` (which has no exception handling — see ASY-001) cannot distinguish an exhausted-retry from any other `RuntimeError` raised anywhere in the call stack.
**Repro:** 1. Create a `NetworkClient(max_attempts=3, sleep_time=0)`. 2. Call `client.call(failing_func)` where `failing_func` raises `ConnectTimeout`. 3. After 3 attempts, `RuntimeError("Network retry limit exceeded")` is raised. 4. The test at `tests/test_network.py::test_network_retry_and_fail` asserts this exact behavior: `with pytest.raises(RuntimeError, match="Network retry limit exceeded")`.
**Impact:** No runtime issue today — the exception IS raised with a descriptive message. However, callers cannot programmatically distinguish an exhausted-retry from other unrelated `RuntimeError` instances, limiting automated error handling and retry-of-retry logic.
**Root cause:** No custom exception class defined for the exhausted-retry condition.
**Fix:** Define `MaxRetriesExceeded(RuntimeError)` (subclass for backward compatibility) in `core/errors.py` and raise it at line 126. Target symbol: `NetworkClient.call` — line 126. Update: `tests/test_network.py::test_network_retry_and_fail` currently asserts `RuntimeError`; if subclassing `RuntimeError`, the existing test still passes via inheritance.
**Verify:** `tests/test_network.py::test_network_retry_and_fail` asserts current `RuntimeError` behavior — the fix should subclass `RuntimeError` to preserve compatibility.
**Related:** ASY-001

**Evidence — `grep` of `network.py`:**
```text
core/network.py:126:         raise RuntimeError("Network retry limit exceeded")
```

**Evidence — existing test asserting the current behavior:**
```text
tests/test_network.py:7: @pytest.mark.asyncio
tests/test_network.py:8: async def test_network_retry_and_fail():
tests/test_network.py:9:     """Проверяем, что после max_attempts поднимается RuntimeError"""
tests/test_network.py:15:     with pytest.raises(RuntimeError, match="Network retry limit exceeded"):
tests/test_network.py:16:         await client.call(failing_func)
```

---

## Fix Order

- **Dependencies:** ASY-001 must land before or alongside ASY-003 and ASY-005 — the `try/finally` in `worker` that guarantees `task_done()` is the foundation for all shutdown discipline. ASY-002 and ASY-004 are independent of the worker-lifecycle fixes.
- **Merge candidates:** ASY-001 + ASY-003 + ASY-007 share the `worker`/`processing_tasks`/`call` code paths — a single PR modifying all three is coherent. ASY-003 and ASY-005 are both about `processing_tasks` shutdown — merge if possible.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** The blocking-work bridge (the synchronous `get_tv_region` call in ASY-002, and the `asyncio.to_thread` usage in `worker`) overlaps with phase 09 (blocking-work bridge) — ASY-002 is filed here because the Semaphore-bypass aspect is owned by this phase; the `to_thread` discipline is owned by phase 09. The `print("=")` progress statement at `reports.py:265` and the `print()` calls in `services.py:117-158` are code-quality issues owned by phase 10 — not filed here.
- **Rollout risk:** Fixing ASY-001 (guaranteeing `task_done` in `finally`) changes the hang-on-error behaviour to a continue-on-error behaviour — existing report runs that currently hang on an API error will now complete (with some tasks skipped). `tests/test_services.py::test_run_report_with_real_models` mocks `create_report` entirely, so it will not be affected; no shipped test asserts the hang.

## Appendices

### Appendix A — Semaphore scope (single shared instance, not per-call)

```text
core/network.py:55:         self._semaphore = asyncio.Semaphore(max_concurrent_requests)
core/network.py:73:             async with self._semaphore:
core/network.py:74:             return await asyncio.to_thread(func, *args, **kwargs)
```

The Semaphore is held only for the duration of `asyncio.to_thread(func, ...)` (the actual API call), not across the backoff sleep at line 124. This is correct — the retry backoff runs outside the Semaphore, so a retrying call does not throttle the pool. No finding filed for this property.

### Appendix B — No test coverage of the worker lifecycle

```text
tests/test_network.py:7:  @pytest.mark.asyncio
tests/test_network.py:8:  async def test_network_retry_and_fail():           # retry exhaustion only
tests/test_reports.py:27: @pytest.mark.asyncio
tests/test_reports.py:28:  async def test_default_task_strategy(mock_report): # strategy only, no worker
tests/test_reports.py:35: async def test_dict_data_strategy():                 # data prep only, no worker
tests/test_services.py:9:  @pytest.mark.asyncio
tests/test_services.py:10: async def test_run_report_with_real_models(tmp_path, mocker, tv_yaml_content):
tests/test_services.py:48:     mocker.patch("mko_get_mediascope_data.core.reports.TVMediaReport.create_report", new_callable=AsyncMock)  # creates_report mocked entirely
```

No test exercises `MediaReport.worker`, `MediaReport.processing_tasks`, `TVMediaReport.wait_task`, or the Semaphore-pool interaction. All seven ASY findings require new tests.

### Appendix C — Pool and Semaphore defaults (no configuration propagation)

```text
core/reports.py:92:  max_tasks: int = 100,          # worker pool + queue maxsize
core/network.py:51:  max_concurrent_requests: int = 5,  # Semaphore permits
core/services.py:130: network_client: NetworkClient = NetworkClient()  # default, no override
```

`func.__name__` in `NetworkClient.call` (line 83, 90, 115) references the function name for logging, but `self.catalogs.get_tv_region` at `reports.py:387` does not go through `call` at all.
