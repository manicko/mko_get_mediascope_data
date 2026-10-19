# Code-Context Report — Performance Audit (PERF-001/002/003)

**Audit phase file:** `.ai/audit/99-validation/13-performance-validated-findings.md`
**Original findings:** `.ai/audit/13-performance/findings.md`
**Current repo HEAD:** `57ad5d9` (`8ed385f` ← 67 commits ← HEAD)
**Validation report HEAD:** `8ed385f` (explicitly stated on p. 3, line 73)
**Audit `code_ref`:** `d093d9c`

---

## 0. Executive Summary

The validation report at `.ai/audit/99-validation/13-performance-validated-findings.md`
was completed against commit **`8ed385f`**. The repository has since advanced **67
commits** to **`57ad5d9`**. Of those 67 commits, at least **six** implement code
changes that directly alter the architecture the validation report describes:

| Commit | Fix | Affects |
|---|---|---|
| `c0b52c9` | EXT-001: `retryable` parameter on `NetworkClient.call` | PERF-001 (partial) |
| `9b3b63e` | EXT-002: `call_timeout` + `_invoke()` + `asyncio.wait_for` | PERF-001 cross-ref |
| `131f125` | EXT-004: `get_status` poll `sleep_time` 40 → 2 | PERF-001 Appendix B, PERF-003 cross-ref |
| `dd5dd8d` | EXT-008: PII redaction (`_redact_args`) | NetworkClient.call |
| `3f3dd56` | TST-012: worker `timeout` param + `_process_item` extraction | PERF-003 target surface |
| `664e8f3` + `4877882` etc. | PIPE-004/006/007: `wait_task` poll guards, real pipeline tests | Test-coverage claims |

**Bottom line:** All three PERF findings remain **structurally valid** at HEAD, but the
validation report's line-number mappings, Evidence Index, test-coverage assertions, and
VAL findings are **stale relative to HEAD `57ad5d9`**. The most consequential drift is
the test suite: the validation report asserts the worker pipeline is unexercised by
tests, but **26 tests now run the real `create_report → send_task → wait_task →
extract_data → worker` pipeline** without mocking `create_report`.

---

## 1. What Is Already Implemented — PERF-001 / 002 / 003

### PERF-001 — Confirmed at HEAD

**Finding:** CPU/local DataFrame work (`build_task`, `result2table`, `get_connection`) is
routed through the I/O Semaphore and the initial-jitter gate.

#### Already addressed (partially — EXT-001 `retryable` flag)

The `retryable: bool = True` parameter **exists** on `NetworkClient.call`
(`core/network.py:97`). It was added by commit `c0b52c9` (EXT-001) to prevent
re-trying the task-submission POST (preventing duplicate task creation). At HEAD,
exactly **one** call site uses `retryable=False`:

```
src/mko_get_mediascope_data/core/reports.py:354
    task.key = await self.network_client.call(self.sender, task_json, retryable=False)
```

This is the `TVMediaReport.send_task` API POST — the only call classified as
non-retryable.

#### NOT addressed (the core PERF-001 defect remains)

The `retryable` flag does **not** implement PERF-001's recommendation. Three reasons:

1. **Initial jitter is applied unconditionally, before the `retryable` check.**
   `core/network.py:103`:
   ```python
   await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))  # line 103 — before retryable check
   ```
   Line 103 runs for **every** `call()` invocation, including `retryable=False` calls.
   The `if not retryable:` branch is at line 105 — two lines *after* the jitter.

2. **Even non-retryable calls still acquire the I/O Semaphore.**
   `core/network.py:106`:
   ```python
   if not retryable:
       async with self._semaphore:          # line 106 — still semaphore-gated
           return await self._invoke(func, *args, **kwargs)  # line 107
   ```

3. **CPU/local calls are still dispatched with `retryable=True` (default).**
   The two local/CPU invocations that PERF-001 identifies — `build_task` and
   `result2table` — are called with the default `retryable=True`:

   | Call site | Method | `retryable` | File:line (HEAD) |
   |---|---|---|---|
   | `build_task` | `self.builder` → `network_client.call` | `True` (default) | `reports.py:349-351` |
   | `send_task` (API POST) | `self.sender` → `network_client.call` | `False` | `reports.py:354` |
   | `get_status` | `self.report_service.get_status` → `network_client.call` | `True` (default) | `reports.py:391-393` |
   | `get_result` | `self.report_service.get_result` → `network_client.call` | `True` (default) | `reports.py:419-421` |
   | `result2table` | `self.report_service.result2table` → `network_client.call` | `True` (default) | `reports.py:427-429` |

   A **fourth** CPU/local call — `get_connection` in `services.py:186-189` — also uses
   `network_client.call` with the default `retryable=True`:
   ```python
   report_service: MediaVortexTask = await network_client.call(
       self.get_connection, report_settings.media,
   )
   ```
   `get_connection` (services.py:150-164) constructs a `MediaVortexTask` object and
   reads credential files — it is **local I/O**, not an API call, yet it pays the
   initial jitter and occupies a Semaphore slot. The validation report does not
   enumerate this call site.

4. **`DefaultDataStrategy.prepare` is still `async def` with no `await`.**
   `core/reports.py:581-602` — the method is declared `async def prepare(...)` but
   contains no `await` in its body (all pandas operations are synchronous). It runs
   in the event loop, blocking all coroutines while it executes. This is the
   "under-isolated" half of PERF-001's "inconsistently over-gated or under-isolated"
   claim — **unchanged** at HEAD.

5. **`csv_to_file` is still dispatched via bare `asyncio.to_thread` without the Semaphore.**
   `core/reports.py:443-450` — dispatched via `asyncio.to_thread(utils.csv_to_file, ...)`
   but not through `NetworkClient.call`. No Semaphore gate, no jitter. Matches the
   validation report's claim (was `reports.py:325`; now `reports.py:443`).

#### Summary for PERF-001

The `retryable` parameter is a **partial scaffolding** that PERF-001's recommendation
foreshadowed ("It requires `call` to distinguish API from local invocations (e.g. a
flag)"). The flag exists, but it serves EXT-001's purpose (non-retried POST), not
PERF-001's purpose (bypass Semaphore + suppress jitter for CPU/local work). The
extension is architecturally feasible — `call` already branches on `retryable` at
`network.py:105` — but it has not been applied to the CPU/local call sites.

### PERF-002 — Confirmed at HEAD

**Finding:** No data-volume backpressure; oversized result loads full `resultBody`
into memory.

| Element | Status at HEAD | Location |
|---|---|---|
| `max_tasks = 100` default | **Unchanged** — still default 100, never set from config | `reports.py:94` (MediaReport.__init__), `reports.py:243` (TVMediaReport.__init__) |
| `asyncio.Queue(maxsize=self.max_tasks)` | **Unchanged** — task-count cap only, no byte limit | `reports.py:116` |
| `services.py` constructs `TVMediaReport` | **Confirmed** — `max_tasks` not passed; default 100 applies | `services.py:193-202` (no `max_tasks=` kwarg) |
| `extract_data` size guard on `result2table` | **Not implemented** — no byte/size guard | `reports.py:413-450` |
| Vendored `result2table` full materialization | **Identical behavior** — iterates `resultBody` into dict-of-lists, `pd.DataFrame(res)` | vendored `tasks.py:1451-1496` |

No size guard, streaming boundary, or byte-volume backpressure has been added since
the audit. PERF-002 is **fully unaddressed**.

### PERF-003 — Confirmed at HEAD (with partial contextual mitigation)

**Finding:** No per-task latency measurement or attribution; slow vs stuck task
indistinguishable.

| Element | Status at HEAD | Location |
|---|---|---|
| Report-level timing in `run_report` | **Present** — `start_time` / `end_time` via `datetime.now()`, "Total duration" log | `services.py:172`, `:173-175`, `:208`, `:209-212` |
| Per-task clock in `worker` | **Absent** — no `datetime.now()`, `perf_counter`, or elapsed computation | `reports.py:149-188` |
| Per-task clock in `wait_task` | **Absent** — `loop.time()` (line 373) is used only for `poll_deadline` enforcement, NOT logged or attributed per-task | `reports.py:362-411` |
| Per-task clock in `send_task` | **Absent** — `logger.info("=")` is the sole signal; no timing context | `reports.py:343-360` (jitter at `:356`) |
| Per-task clock in `extract_data` | **Absent** — no clock calls in the method body | `reports.py:413-450` |

**Partial mitigation applied post-validation (PIPE-006, not PERF-003):**
`wait_task` now has `max_polls: int = 60` and `poll_deadline: float = 3600.0` parameters
(`reports.py:362-363`). The inner loop checks `polls >= max_polls` (line 378) and
`loop.time() - start > poll_deadline` (line 383) before each poll. This bounds the
poll loop — addressing the unbounded-poll escalation trigger — but it does **not**
add per-task latency *measurement* or *attribution* (the core of PERF-003). The
`loop.time()` value at line 373 is used purely for deadline enforcement, never logged
or surfaced as a metric.

**`DefaultDataStrategy.prepare` timing status:** still `async def` with no `await`
(`reports.py:581-602`) — no timing, runs synchronously in the event loop.

---

## 2. Plan vs. Current Implementation — Drift Analysis

### 2.1 Line-number drift table

The validation report was validated against `8ed385f`. At current HEAD `57ad5d9`,
all cited line numbers have shifted:

| Symbol | code_ref `d093d9c` | validation `8ed385f` | **current HEAD `57ad5d9`** |
|---|---|---|---|
| `NetworkClient.call` signature | ~line 59 | line 59 | `network.py:92` |
| Initial jitter `asyncio.sleep(uniform(...))` | network.py:69 | network.py:69 | **`network.py:103`** |
| `async with self._semaphore` (retryable path) | network.py:73 | network.py:73 | **`network.py:111`** |
| `self.builder = build_task` | reports.py:110 | reports.py:110 | **`reports.py:111`** |
| `asyncio.Queue(maxsize=self.max_tasks)` | reports.py:115 | reports.py:115 | **`reports.py:116`** |
| `max_tasks=100` default | reports.py:92 | reports.py:92 | **`reports.py:94`** |
| `worker` method start | reports.py:134 | reports.py:134 | **`reports.py:149`** |
| `logger.info("=")` (was `print("=")`) | reports.py:265 | reports.py:265 | **`reports.py:356`** |
| `build_task` via `network_client.call` | reports.py:258 | reports.py:258 | **`reports.py:349`** |
| `result2table` via `network_client.call` | reports.py:309 | reports.py:309 | **`reports.py:427`** |
| `csv_to_file` via `asyncio.to_thread` | reports.py:325 | reports.py:325 | **`reports.py:443`** |
| `DefaultDataStrategy.prepare` | reports.py:435 | reports.py:435 | **`reports.py:581`** |
| `send_task` API POST `retryable=False` | N/A | N/A | **`reports.py:354`** (new at HEAD) |
| `start_time = datetime.now()` | services.py:116 | services.py:143 | **`services.py:172`** |
| `end_time = datetime.now()` | services.py:153 | services.py:179 | **`services.py:208`** |
| "Total duration" log string | services.py:153 | services.py:179 | **`services.py:211`** |
| `time_unit` field | models.py:64 | models.py:64 | **`models.py:71`** |

The validation report's Evidence Index (pp. 427-443) cites all the `8ed385f` line
numbers. **Every one of these is stale at current HEAD.** The validation report's
Conformance section (line 74) acknowledged the `d093d9c → 8ed385f` shift but did
not anticipate the additional `8ed385f → 57ad5d9` shift.

### 2.2 Feature-level drift (changes after validation)

| Feature | Validation HEAD `8ed385f` | Current HEAD `57ad5d9` | Impact on validation report |
|---|---|---|---|
| `retryable` parameter on `call()` | **Absent** | Present (`network.py:97`, `105-107`) | PERF-001 Block 5 says "requires a flag" — the flag now exists but doesn't serve PERF-001's purpose |
| `call_timeout` / `_invoke()` / `asyncio.wait_for` | **Absent** | Present (`network.py:63`, `:74-79`) | RELAE-002 cross-ref is now implemented in code, not just audit finding |
| `get_status` poll `sleep_time` | 40 | 2 (`reports.py:392`) | PERF-001 Appendix B magnitudes stale (20-80 s → 1-4 s); PERF-003 EXT-004 cross-ref stale |
| `MaxRetriesExceeded` exception | Absent (used `RuntimeError`) | Present (`network.py:175`) | Error-handling structure changed; tests now assert `MaxRetriesExceeded` |
| 429/5xx retry classification (`retry_5xx`, `TooManyRequestsError`) | Absent (used `HTTPError`) | Present (`network.py:64`, `:139-156`) | PERF-001 EXT-005 cross-ref: "429/5xx currently un-retried" is now stale |
| `wait_task` poll guards (`max_polls`, `poll_deadline`) | Absent (`while True`) | Present (`reports.py:362-363`) | PIPE-006 implemented; PERF-003 "unindistinguishable slow vs stuck" partially mitigated |
| `worker` timeout parameter | Absent | Present (`reports.py:149`, `timeout` param) | PERF-003 target surface has expanded — `_process_item` extracted at `reports.py:137` |
| Settings file format | `.yaml` | `.json5` (JSON5 migration, commits `a596d86`, `b2ff6e4`, `6b44fa8`) | VAL-002's `nat_tv_brands_last.yaml` path is stale; file is now `.json5` |
| Conftest fixture name | `local_app_service` + `mock_network_client` | `hermetic_app_service` (no old fixtures) | Validation report's fixture references are stale |

### 2.3 Test-count drift

| Metric | Validation report claim | Current HEAD |
|---|---|---|
| Tests at `8ed385f` | "23 passed" (Summary p. 30); "12 passed" (Evidence Index p. 446) | **110 tests collected, 110 passed** |
| `test_network.py` | ~3-5 tests (retry exhaustion) | **20 tests** (covers retryable, call_timeout, 429/5xx classification, PII redaction, backoff, semaphore) |
| `test_reports.py` | 1 test (`test_dict_data_strategy`) | **4 tests** (+ PIPE-008, PIPE-011) |
| `test_reports_lifecycle.py` | **Did not exist** | **24 tests** — exercises REAL `create_report → worker → send_task → wait_task → extract_data` pipeline |
| `test_services.py` | `local_app_service`, mocks `create_report` | `hermetic_app_service`, **2 slow tests run real pipeline** (`test_run_report_with_real_models`, `test_run_report_multi_entry_yields_multiple_paths`) |

The validation report's central rollout-safety assertion — "test_services.py mocks
create_report entirely and patches NetworkClient.call, so the worker loop is not
exercised there — the fix cannot break it; verified by test source
(tests/test_services.py:47-50)" — is **invalid at current HEAD**. The file has
474 lines (was 145), uses `hermetic_app_service`, and two tests run the real pipeline.

### 2.4 VAL-001 (grep count) — Validation report is **incorrect**

The validation report claims (p. 273-290, VAL-001):

> "An independent content grep for `perf_counter|timeit|elapsed|duration` across
> `src/mko_get_mediascope_data/core/` returns **0 matches**."

At current HEAD, the grep returns **1 match**:
```
src/mko_get_mediascope_data/core/services.py:211:
    f"Total duration: {end_time - start_time} {'-' * 10}"
```

This match is the word "duration" in the report-level log message — the exact
string PERF-003 is about. It was also present at `8ed385f` (confirmed via `git show
8ed385f:.../services.py`). The original PERF-003 findings.md (p. 152-153) correctly
reported "1 match" but misattributed it to `time_unit` (models.py:64). The validation
report corrected the attribution but **incorrectly concluded 0 matches**. The correct
answer: **1 match, and it is "Total duration" in services.py** — which is the report-
level timing PERF-003 already acknowledges, not a per-task timing primitive.

This does **not** change PERF-003's verdict (no per-task timing exists), but VAL-001's
stated observation is factually wrong.

### 2.5 VAL-002 — Stale file path

VAL-002 references `settings/reports/last_category/nat_tv_brands_last.yaml`. At current
HEAD, this file is `.json5`:
`src/mko_get_mediascope_data/settings/reports/last_category/nat_tv_brands_last.json5`.
The JSON5 migration (commits `a596d86`, `b2ff6e4`, `6b44fa8`) occurred between
`8ed385f` and `57ad5d9`. The file's *content* (3 audiences, monthly, TV_CROSSTAB,
~15-month period) matches VAL-002's corrected description; only the *extension* is
stale.

---

## 3. Relevant Architecture, Dependencies, and Constraints

### 3.1 PERF-001 — Code path

The full call chain for a single report task:

```
AppService.run_report (services.py:166)
  └─ network_client.call(get_connection, media)          ← local I/O, retryable=True
     → TVMediaReport.create_report (reports.py:224)
       └─ get_period → get_available_period
          └─ network_client.call(catalogs.get_availability_period)  ← API, retryable=True
       └─ processing_tasks (reports.py:190)
          └─ 100 × worker(timeout=...) (reports.py:149)   ← 100 workers (max_tasks)
             └─ _process_item (reports.py:137)
                ├─ send_task (reports.py:343)
                │   ├─ network_client.call(builder)                    ← CPU/local, retryable=True
                │   ├─ network_client.call(sender, ..., retryable=False) ← API POST, non-retryable
                │   └─ logger.info("=")                                ← sole per-task signal (reports.py:356)
                ├─ wait_task (reports.py:362)
                │   └─ network_client.call(get_status, ..., sleep_time=2)  ← API poll, retryable=True
                └─ extract_data (reports.py:413)
                    ├─ network_client.call(get_result)                 ← API, retryable=True
                    ├─ network_client.call(result2table, ...)          ← CPU/local, retryable=True
                    ├─ prepare_data → DefaultDataStrategy.prepare      ← async def, NO await (reports.py:581)
                    └─ asyncio.to_thread(csv_to_file, ...)             ← NOT semaphore-gated (reports.py:443)
```

**Two-level concurrency design** (intentional, per ASY-004 re-characterization):
- **Level 1** — `asyncio.Queue(maxsize=max_tasks)` where `max_tasks=100` (default).
  Created at `reports.py:116`. Bounds the count of in-flight tasks, not bytes.
  100 worker coroutines are spawned (`reports.py:195-196`):
  `asyncio.create_task(self.worker(timeout=timeout)) for _ in range(self.max_tasks)`.
- **Level 2** — `asyncio.Semaphore(max_concurrent_requests)` where
  `max_concurrent_requests=5` (default). Created at `network.py:67`. Bounds
  concurrent API calls inside `NetworkClient.call`. All workers contend for the
  same 5-slot Semaphore when they reach `call()`.

Both defaults are hardcoded — neither `max_tasks` nor `max_concurrent_requests` is
configurable from JSON5 settings. `services.py:185` constructs `NetworkClient()`
with no args; `services.py:193-202` constructs `TVMediaReport` without `max_tasks=`.

**Strategy pattern** (`reports.py:30-50`): `ReportFactory` selects strategies
at construction time and passes instances to `TVMediaReport`:
```python
task_strategy=report_strategies.task_strategy()   # reports.py:199
data_strategy=report_strategies.data_strategy()   # reports.py:200
```
`task_strategy` returns `TVRegTaskStrategy` for `MediaType.TV_REG`, else
`DefaultTVTaskStrategy`. `data_strategy` returns `DictDataStrategy` for
`ReportSubtype.DYNAMICS_BY_SPOTS_DICT`, else `DefaultDataStrategy`. The strategies
are **invoked inside** the PERF-001/PERF-002/PERF-003 code paths:
- `DefaultTVTaskStrategy.generate` (line 486) — produces tasks; called from
  `generate_tasks` → `processing_tasks`
- `DefaultDataStrategy.prepare` (line 581) — async-without-await, the
  "under-isolated" CPU path PERF-001 notes
- `DictDataStrategy.prepare` (line 606) — alternative data strategy

### 3.2 PERF-001 — `retryable` parameter and the flag pattern

The `retryable` parameter (`network.py:97`) implements a two-branch structure:

```python
async def call(self, func, *args, sleep_time=2, retryable=True, **kwargs):
    _sleep_time = max(sleep_time, self._sleep_time)
    await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))  # jitter ALWAYS (line 103)
    if not retryable:                                                  # line 105
        async with self._semaphore:                                   # line 106
            return await self._invoke(func, *args, **kwargs)          # line 107
    for attempt in ...:                                                # line 109
        try:
            async with self._semaphore:                               # line 111
                return await self._invoke(func, *args, **kwargs)      # line 112
            ...backoff...
```

The `retryable=False` branch was designed for EXT-001 (non-retried POST). It does
**not** suppress the initial jitter (line 103 runs first) or bypass the Semaphore
(line 106). Extending it for PERF-001 would require either:
- A second parameter (e.g., `api_call: bool = True`) that gates both jitter and
  Semaphore, or
- Restructuring so `retryable=False` also skips jitter + Semaphore.

The existing branch at `network.py:105-107` is a **natural extension point** — the
architecture already anticipates a non-standard call path. No new abstraction is
needed; the fix is a parameter addition + branch expansion in `call`.

### 3.3 PERF-002 — Code path

`extract_data` (`reports.py:413-450`):
1. `get_result` via `network_client.call` (`reports.py:419-421`) — fetches full result JSON
2. `result2table` via `network_client.call` (`reports.py:427-429`) — vendored method
   fully materializes `data['resultBody']` into a dict-of-lists then `pd.DataFrame`
   (`tasks.py:1451`, `:1466-1479`, `:1496`). No streaming, no size guard.
3. `prepare_data` (`reports.py:438`) → `DefaultDataStrategy.prepare` (async, no await)
4. `csv_to_file` via `asyncio.to_thread` (`reports.py:443-450`) — writes to disk

The `task_queue` (`reports.py:116`) has `maxsize=max_tasks` (count-based). Nothing
bounds the **byte size** of an individual result or the aggregate data held across
workers before the CSV write drains it.

### 3.4 PERF-003 — Code path

`worker` (`reports.py:149-188`) wraps `_process_item` (`reports.py:137-148`) with
`asyncio.wait_for(coro, timeout=timeout)` when `timeout` is set. `_process_item` chains
`send_task → wait_task → extract_data`. Neither `worker`, `_process_item`, `send_task`,
`wait_task`, nor `extract_data` emit any timing or duration log.

The **sole** per-task signal is `logger.info("=")` at `reports.py:356` (inside
`send_task`). It conveys only that a task key was received — no task identity, no
stage, no elapsed time.

Report-level timing in `services.py:172` (`start_time`) / `services.py:208`
(`end_time`) / `services.py:211` ("Total duration") uses `datetime.now()`, not
`perf_counter` — suitable for human-readable wall-clock but not for latency attribution
(loses monotonicity, coarser granularity).

### 3.5 Test coverage — what tests mock vs. what they exercise

**test_network.py (20 tests at HEAD)** — all mock `asyncio.sleep` via
`AsyncMock`. Tests:
- `test_call_respects_semaphore` — patches `_invoke`, asserts Semaphore caps
  concurrency (relevant to PERF-001, but does NOT assert CPU bypass)
- `test_send_task_not_retried_on_retryable_error` — asserts `retryable=False` → 1 call
- `test_call_raises_timeout_after_call_timeout` — mocks `_invoke`, asserts TimeoutError retry
- `test_call_retries_on_429` / `test_call_5xx_not_retried_by_default` /
  `test_call_5xx_retried_when_enabled` — EXT-005 coverage
- `test_call_redacts_pii_in_error_logs` / `test_call_logs_full_args_when_detail_enabled`
  — EXT-008 coverage
- `test_backoff_grows_exponentially` / `test_initial_jitter_in_range` — asserts
  the jitter/backoff formula at `network.py:103`, `:166-169`
- `test_semaphore_released_after_exhausted_retries` — asserts cleanup

**No test asserts** that CPU/local work bypasses the Semaphore or that the initial
jitter is suppressed for non-API calls (the PERF-001 verify criterion:
`test_cpu_local_paths_run_without_semaphore` is absent).

**test_reports_lifecycle.py (24 tests at HEAD)** — the most consequential change
since validation. These tests exercise the **real** `create_report → worker →
send_task → wait_task → extract_data → prepare_data → csv_to_file` pipeline with a
doubled `MediaVortexTask` (MagicMock for the vendor client only). `NetworkClient`
is instantiated for real (`NetworkClient(sleep_time=0, max_attempts=1)`); only
`asyncio.sleep` is mocked. Key tests:
- `test_create_report_writes_csv_and_marks_queue_done` — real pipeline, asserts CSV output
- `test_create_report_skips_done_files` — asserts `build_task`/`send_task`/
  `get_status`/`get_result` are NOT called when file exists
- `test_create_report_timeout_does_not_hang` — `wait_task` polls forever → outer
  `asyncio.wait_for` timeout fires → error JSON5 written
- `test_wait_task_max_polls_limit` — inner `max_polls` guard (PIPE-006)
- `test_wait_task_poll_deadline_exceeded` — inner `poll_deadline` guard
- `test_worker_handles_exception_and_writes_error_json5` — `send_task` raises
  RuntimeError → `except Exception` branch → error JSON5
- `test_worker_log_error_true_writes_json5` / `test_worker_log_error_false_no_json5` —
  `log_error` True/False branches, spy on `logger.warning`
- `test_worker_does_not_log_ready_on_failed` — **spies on `logger.info` and
  `logger.warning`** (lines 1028-1029), asserts "is ready" NOT logged for FAILED task

**test_services.py (14 tests at HEAD)** — uses `hermetic_app_service` (not the
validation's `local_app_service`). Two tests marked `@pytest.mark.slow` run the real
pipeline:
- `test_run_report_with_real_models` — injects mock `NetworkClient`/`MediaVortexTask`
  via constructor, runs `hermetic_app_service.run_report()` end-to-end, asserts CSV output
- `test_run_report_multi_entry_yields_multiple_paths` — same, multi-entry config

The validation report's claim "test_services.py mocks `create_report` entirely
(tests/test_services.py:47-50)" is **invalid at HEAD** — these tests do NOT mock
`create_report`; they inject doubles and call the real method.

### 3.6 Strategy pattern relationship to PERF findings

The Strategy pattern (`reports.py:30-50`) selects `TaskGenerationStrategy` and
`DataPreparationStrategy` at construction time. It is **orthogonal** to all three
PERF findings:
- PERF-001 concerns `NetworkClient.call` gating (strategy-independent)
- PERF-002 concerns `result2table` memory materialization (vendor method, not a strategy)
- PERF-003 concerns timing instrumentation (strategy-independent)

The strategies' `prepare` method (`DefaultDataStrategy.prepare`, `reports.py:581-602`)
is the `async def` with no `await` that PERF-001 identifies as "under-isolated" —
but this is a property of the strategy implementation, not the pattern itself.
Adding timing to `worker` or `extract_data` (PERF-003) would not require strategy
changes; timing would live in the report class methods that *call* strategies.

---

## 4. Important Discrepancies or Risks

### 4.1 PERF-001 fix — What could break

**Current state that tests DO assert:**

1. `test_call_respects_semaphore` (test_network.py:51) — patches `_invoke`, asserts
   `active["max"] <= 2` when `max_concurrent_requests=2`. If PERF-001's fix moves
   CPU/local calls off the Semaphore, this test still passes (it only tests API calls
   through `call`).

2. `test_send_task_not_retried_on_retryable_error` (test_network.py:122) — passes
   `retryable=False` and asserts 1 call, `ConnectTimeout` raised. If the fix extends
   `retryable` to also bypass jitter/Semaphore, this test still passes (it mocks
   `asyncio.sleep`).

3. **Lifecycle tests that exercise the real pipeline** (test_reports_lifecycle.py,
   24 tests) — these call `network_client.call` with REAL `NetworkClient` instances
   (only `asyncio.sleep` is mocked). If the fix changes `call`'s signature or
   branch structure, these tests' real `call()` invocations would be affected:
   - `build_task` (reports.py:349) → `call` with `retryable=True` (default) → currently
     goes through jitter + Semaphore + retry loop. If changed to `retryable=False` or
     a new `api_call=False` flag, the jitter (mocked) and Semaphore behavior changes.
   - `result2table` (reports.py:427) → same situation.
   - These tests don't assert on jitter or Semaphore behavior directly, but they
     DO exercise the full `extract_data` path. A signature change to `call` or a
     change in which calls are `retryable` could cause `MagicError` or unexpected
     call-count mismatches if the mock `side_effect` in `mock_call_side_effect`
     (test_services.py:111-114) doesn't handle the new parameter.

4. `test_run_report_with_real_models` and
   `test_run_report_multi_entry_yields_multiple_paths` (test_services.py) use
   `mock_network_client.call = AsyncMock(side_effect=mock_call_side_effect)` where:
   ```python
   async def mock_call_side_effect(func, *args, **kwargs):
       if getattr(func, "__name__", None) == "get_connection":
           return mock_report_service
       return func(*args, **kwargs)
   ```
   This side-effect ignores `retryable` and `sleep_time` kwargs — so adding more
   non-retryable calls would not break it. But if `call`'s signature changes (e.g.,
   adding `api_call` before `retryable`), the mock would still work (kwargs are
   absorbed). **Low risk** for these tests.

**Can the `retryable` parameter be extended?**

Yes. The current structure (`network.py:105-107`) already has a non-retryable branch.
Two extension paths:
- **Option A** (minimal): Repurpose `retryable` to mean "non-API/CPU" — but this breaks
  EXT-001's semantics (the POST must be non-retried). The POST also needs to bypass
  jitter + Semaphore. This conflates two concerns.
- **Option B** (cleaner): Add a second parameter, e.g.
  `cpu_bound: bool = False` or `api_call: bool = True`. When `cpu_bound=True`, skip
  the initial jitter (`network.py:103`) and acquire NO Semaphore. When
  `retryable=False` (EXT-001), keep current behavior (jitter + Semaphore, no retry).
  Call sites for `build_task` and `result2table` would pass `cpu_bound=True`.

Option B is the natural extension of the existing `retryable` flag pattern — it adds
a second boolean discriminator without disturbing EXT-001's behavior.

### 4.2 PERF-002 fix — Reject or stream?

**Config mechanism**: No size/threshold config currently exists in the JSON5 settings.
`Data` model (`models.py:133-148`) has `options`, `statistics`, `slices` — all
list/dict fields. A `max_result_rows` or `max_result_bytes` threshold would need to
be added somewhere:
- As a `Data` model field (persisted in JSON5 settings) — but this would require
  settings-schema changes, which the validation report's "Backward compatibility"
  section says the fixes should avoid.
- As a `NetworkClient.__init__` parameter with a default — minimal, no config
  schema change. `NetworkClient(max_concurrent_requests=5, ...)` could gain
  `max_result_bytes: int | None = None`.

**Reject vs. stream**: The vendored `result2table` (`tasks.py:1451-1496`) iterates
`data['resultBody']` into a dict-of-lists before `pd.DataFrame(res)`. It does NOT
support streaming or slicing — it's an all-or-nothing vendor method. To "stream",
you would need to bypass `result2table` entirely and build the DataFrame row-by-row,
or pre-slice `data['resultBody']` and call `result2table` per slice (but the vendor
method doesn't accept a slice parameter).

**Recommendation**: Given the vendored client's all-or-nothing design, a **reject**
size guard is more practical than streaming. `extract_data` (`reports.py:413-450`)
could check `len(res_json.get('resultBody', []))` against a threshold before calling
`result2table` (`reports.py:427-429`), and raise/skip if exceeded. This requires:
1. A threshold parameter (on `TVMediaReport.__init__` or `NetworkClient`)
2. A check before `network_client.call(self.report_service.result2table, ...)`
3. A new test: `test_large_result_is_rejected_or_streamed` (the validation report
   confirms this test is "currently absent" — it remains absent at HEAD)

**What could break**: The `df.empty` branch (`reports.py:434-436`) is exercised by
`test_worker_log_error_false_no_json5` (test_reports_lifecycle.py:726) which mocks
`result2table.return_value = pd.DataFrame()`. A size guard that rejects empty
DataFrames would break this test — so the guard must use `>` (strictly greater than
threshold), not `>=`, or must check the raw `resultBody` before calling
`result2table` (not the DataFrame after).

### 4.3 PERF-003 fix — Structured logging risk analysis

**What could break** — the lifecycle tests now use `mocker.spy` on the module-level
logger:

| Test | Lines | Spies on | Assertion | Risk if PERF-003 adds per-task `logger.info` |
|---|---|---|---|---|
| `test_worker_log_error_true_writes_json5` | 642-723 | `logger.warning` | `error_payload["log_error"] == True` | Low — adds no `warning` calls |
| `test_worker_log_error_false_no_json5` | 726-818 | `logger.warning` | `any("No data" in msg ...)` | Low — checks subset, not count |
| `test_worker_does_not_log_ready_on_failed` | 977-1081 | `logger.info` + `logger.warning` | `assert not any("is ready" in msg ...)`; `assert any("will be skipped" in msg ...)` | **Medium** — new `logger.info` calls from timing would be captured by the spy. The test checks that NO info message contains "is ready" — so as long as timing messages don't contain "is ready", they won't break. BUT if PERF-003 changes `logger.info("=")` (reports.py:356) to a structured timing message, the test's `mock_info.call_args_list` would include it — which is fine since the assertion is negative ("is ready" must NOT appear). |

**The `logger.info("=")` at reports.py:356**: This is the sole per-task signal mentioned
in all three audit documents. No test asserts on the `"="` string directly (confirmed
via grep — no test references `"="` in logger context). Replacing it with structured
timing (e.g., `logger.info("Task %s sent in %f s", task.name, elapsed)`) is safe for
existing tests. However, the lifecycle tests that spy on `logger.info`
(`test_worker_does_not_log_ready_on_failed`) would intercept the new timing message
— they check `str(call.args[0])` for "is ready", so a timing message like
"Task X sent in 0.5 s" would pass through without matching.

**Recommendation for PERF-003**: Use `logging.Logger.info` with **positional
lazy-formatting** (not f-strings) for structured records, e.g.:
```python
logger.info(
    "task_stage_complete",
    extra={"task": task.name, "stage": "send_task", "elapsed": elapsed, ...}
)
```
This is compatible with the existing `mocker.spy` assertions (they check `call.args[0]`
for substring presence). The risk is **low** — the existing spy-based tests assert on
message *content* (substring checks), not call counts.

### 4.4 Validation report staleness — summary of invalidated claims

| Validation report claim | Actual at HEAD | Severity |
|---|---|---|
| "Test suite: 12 passed, none exercise worker pipeline" (p. 446) | 110 tests; 26 exercise real pipeline | **High** — rollout safety assessment invalidated |
| "test_services.py mocks create_report entirely (tests/test_services.py:47-50)" (p. 161, 398) | test_services.py is 474 lines; real pipeline tests; no create_report mock | **High** |
| "`perf_counter\|timeit\|elapsed\|duration` grep in core/ → 0 matches" (VAL-001) | 1 match: `services.py:211` ("Total duration") | Medium |
| `nat_tv_brands_last.yaml` path (VAL-002) | `.json5` (JSON5 migration) | Low |
| `retryable` parameter not yet present | Present (EXT-001), `reports.py:354` | Medium — validation didn't account for it |
| `call_timeout` / `_invoke` / `asyncio.wait_for` not present | Present (EXT-002) | Medium |
| `get_status sleep_time=40` | `sleep_time=2` (EXT-004) | Medium — Appendix B magnitudes stale |
| `wait_task` `while True` unbounded | Now bounded (`max_polls=60`, `poll_deadline=3600`) | High — PIPE-006 implemented |
| `worker` has no timeout | Now has `timeout` param + `_process_item` | High — TST-012 implemented |
| Line refs: reports.py:258, :309, :325, :436 | :349, :427, :443, :581 | Medium — all stale |
| `conftest.py` has `mock_network_client` | Replaced by `hermetic_app_service` | Medium |
