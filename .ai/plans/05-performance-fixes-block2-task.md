---
id: PERF-003-block2
task_id: "05-performance-fixes-block2-task"
title: "Block 2 — PERF-003 (per-task latency instrumentation)"
priority: high
status: ready
depends_on: ["PERF-001-002-block1"]
source_plan: .ai/plans/05-performance-fixes.md
source_section: "Block 2 — PERF-003 (per-task latency instrumentation)"
source_code_context: .ai/audit/99-validation/13-performance-code-context.md
source_findings: .ai/audit/99-validation/13-performance-validated-findings.md
finding_ids: ["PERF-003"]
---

# Task — Block 2: PERF-003 (per-task latency instrumentation)

## Overview

**Finding:** No per-task latency measurement or attribution; a slow task is
indistinguishable from a stuck task. The sole per-task signal is
`logger.info("=")` in `send_task` — it conveys only that a task key was
received, with no task identity, no stage, and no elapsed time.

Report-level timing exists in `AppService.run_report` via `datetime.now()`
(`services.py`, `start_time` / `end_time` / "Total duration"), but it is
wall-clock-only (non-monotonic, coarse) and aggregates across **all** tasks —
it cannot attribute latency to an individual task or stage.

This task implements **Block 2** of the execution plan
(`.ai/plans/05-performance-fixes.md`, §4 "Block 2"). Block 1
(`PERF-001-002-block1-task`) is already in place: the `api_call` discriminator
on `NetworkClient.call` and the `max_result_rows` guard in `extract_data` are
live. This task operates on the post-Block-1 state and is independent of
PERF-001/002 — it adds observability only, touching no call-path branching.

### Implementation approach (decided, verbatim from plan)

> 1. **Add timing instrumentation** using `time.perf_counter()` in `_process_item`
>    (wrapping send_task→wait_task→extract_data) and in each stage method
>    (send_task, wait_task, extract_data). Use `time.perf_counter` for
>    monotonic, high-resolution timing (NOT datetime.now which is used for
>    report-level display).
>
> 2. **Replace `logger.info("=")`**: In `send_task`, after the task key is
>    received, replace the bare progress marker with a structured per-task log
>    record that includes: task name, target, interval, stage durations, and
>    total. Use positional lazy-formatting (`logger.info` with `%s`/`%f` args,
>    not f-strings) so the existing spy-based tests (which check
>    `str(call.args[0])` for substrings like "is ready") are not affected.
>
> 3. **Per-task total**: In `_process_item` (or `worker`), record the total
>    elapsed time for the full pipeline and log a structured record at task
>    completion.

### What is NOT in scope

- PERF-001 — `NetworkClient.call` jitter/Semaphore/retryable restructuring
  (handled in Block 1). No changes to `network.py`.
- PERF-002 — `max_result_rows` guard / result-size backpressure
  (handled in Block 1). No changes to `extract_data`'s guard logic.
- Any changes to the Strategy pattern (`ReportFactory`,
  `TaskGenerationStrategy`, `DataPreparationStrategy`).
- Configuration schema changes (no new JSON5 settings fields).
- `max_concurrent_requests` or `max_tasks` configurability.
- Changes to `worker`'s error-handling, queue, or control flow (the
  `if item.status is False` / `to_file` / `task_done()` branches are untouched).

---

## Implementation sequence (ordered, dependency-safe)

### Step 1 — Add `import time` to `reports.py`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** module-level imports

`reports.py` currently imports `asyncio`, `logging`, etc. (lines 1–9) but does
**not** import `time`. Add `import time` in the stdlib import group, alphabetically
after `logging` (alphabetical: `asyncio`, `logging`, `time`):

```python
import asyncio
import logging
import time
```

`time.perf_counter` is the only clock source used for this task. It is
monotonic, high-resolution, and process-local — ideal for sub-task latency
attribution. It must **not** replace the `datetime.now()` calls in
`services.py` (those remain for human-readable report-level wall-clock
output).

### Step 2 — Add per-task total timing to `_process_item`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `MediaReport._process_item` method

`_process_item` (the per-item pipeline that `worker` wraps in
`asyncio.wait_for`) currently chains `send_task → wait_task → extract_data`
with no timing. Add `time.perf_counter()` at entry and log the total in a
`finally` block so the duration is recorded on **every** exit path — success,
stage failure, exception, and timeout-induced cancellation:

```python
async def _process_item(self, item: TVTask) -> None:
    """Execute the per-item pipeline: send, wait, extract.

    Extracted from ``worker`` so the caller can wrap it with
    ``asyncio.wait_for`` and isolate per-task failures.
    """
    start = time.perf_counter()
    try:
        await self.send_task(item)
        if item.status is True:
            await self.wait_task(item)
        if item.status is True:
            await self.extract_data(item)
    finally:
        elapsed = time.perf_counter() - start
        logger.info(
            "task_complete name=%s target=%s interval=%s total=%.4f",
            item.name,
            item.target,
            item.interval,
            elapsed,
        )
```

Key decisions:

- **try/finally** (not just a trailing log) so the total is emitted even when
  a stage raises (`test_worker_handles_exception_and_writes_error_json5`) or
  the outer `asyncio.wait_for` cancels the coroutine
  (`test_create_report_timeout_does_not_hang`). The `finally` block contains
  **only** the synchronous `logger.info` call — it does not catch or suppress
  the propagating exception/timeout.
- **Lazy formatting** (`%s`/`%.4f` with trailing args) — the format string
  `task_complete name=%s target=%s interval=%s total=%.4f` is `call.args[0]`
  in the spy; it contains neither `"is ready"` nor `"will be skipped"`, so
  `test_worker_does_not_log_ready_on_failed` (negative assertion on info) is
  unaffected.
- `item.interval` is passed as `%s` (stringifies the tuple or `None` — both
  are safe for `%s`).
- `worker` is **not** modified — the existing `if item.status is False` /
  `logger.warning("...will be skipped...")` / `logger.info(f"Task {item.name}
  is ready.")` branches are left intact.

### Step 3 — Replace `logger.info("=")` and add stage timing in `send_task`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `TVMediaReport.send_task` method

Current code (post-Block-1, `build_task` already passes `api_call=False`):

```python
async def send_task(self, task: TVTask):
    task_json = await self.network_client.call(
        self.builder, task.report_type, **task.settings, api_call=False
    )
    if task_json is not False:
        task.key = {}
        task.key = await self.network_client.call(self.sender, task_json, retryable=False)
        logger.info("=")
    if not task.key:
        task.status = False
        ...
```

Changes:

1. Add `stage_start = time.perf_counter()` as the first statement of the
   method body (before the `self.builder` call).
2. Replace `logger.info("=")` with a structured lazy-formatted record that
   includes the task name, target, and the send-stage duration:

```python
logger.info(
    "task_stage_complete stage=send name=%s target=%s duration=%.4f",
    task.name,
    task.target,
    time.perf_counter() - stage_start,
)
```

Rationale:

- This is a direct replacement of the `logger.info("=")` marker — it lives at
  the same point (after `task.key` is received, inside the
  `if task_json is not False:` block).
- The format string `task_stage_complete stage=send name=%s target=%s
  duration=%.4f` does **not** contain `"is ready"`, so the negative assertion
  in `test_worker_does_not_log_ready_on_failed` is unaffected.
- When `task_json is False` (builder returned `False`), `send_task` returns
  without logging — this is acceptable: `_process_item`'s `finally` still
  records the total, and no existing test exercises this path.
- `test_worker_log_error_true_writes_json5` (send_task returns `""`) still
  reaches this log point (task_json `{"task_json": "mock"}` is truthy, and
  `task.key` is set to `""` before the log), but that test asserts only on
  `error_payload["log_error"]`, not on info calls — so the extra log is inert.

### Step 4 — Add stage timing to `wait_task`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `TVMediaReport.wait_task` method

Current code (post-PIPE-006):

```python
async def wait_task(self, task, *, max_polls: int = 60, poll_deadline: float = 3600.0):
    loop = asyncio.get_running_loop()
    start = loop.time()          # used for poll_deadline enforcement — NOT logged
    polls = 0
    sleep_interval = 2.0
    while True:
        ...
        if terminal in ("DONE", "FAILED", "CANCELLED"):
            ...
            break
        ...
    # ← end of method, no timing log
```

Changes:

1. Add `stage_start = time.perf_counter()` immediately after the existing
   `start = loop.time()` line (the `loop.time()` value is **unchanged** — it
   remains for `poll_deadline` enforcement only; it is intentionally **not**
   repurposed to avoid coupling `loop.time()` semantics with wall-clock
   reporting).
2. After the `while True` loop terminates (all `break` paths fall through
   to this point), add a structured timing log:

```python
logger.info(
    "task_stage_complete stage=wait name=%s target=%s duration=%.4f",
    task.name,
    task.target,
    time.perf_counter() - stage_start,
)
```

Key decisions:

- The log is placed **after** the `while` loop, so it fires on every terminal
  path: `DONE`, `FAILED`, `CANCELLED`, `max_polls` exhaustion, `poll_deadline`
  exhaustion, and `False` status. All paths `break` out of the loop and reach
  this line.
- No existing test asserts on `wait_task` info output. The direct-call tests
  (`test_wait_task_max_polls_limit`, `test_wait_task_resets_status_on_failed`,
  `test_wait_task_resets_status_on_cancelled`) assert on `task.error` /
  `task.status` — unaffected by an info log.
- The lazy-format string does not contain `"is ready"` or `"will be skipped"`.

### Step 5 — Add stage timing to `extract_data`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `TVMediaReport.extract_data` method

Current code (post-Block-1 — `api_call=False` on `result2table`,
`max_result_rows` guard with early `return`):

```python
async def extract_data(self, task):
    """..."""
    # retrieve result
    res_json = await self.network_client.call(self.report_service.get_result, task.key)
    if res_json is False:
        task.status = False
        task.log_error = True
        task.error = "Task json is not returned"
    if task.status:
        result_body = res_json.get("resultBody", [])
        if len(result_body) > self.max_result_rows:
            ...
            logger.error(...)
            return                          # ← early return (size guard)
        df = await self.network_client.call(self.report_service.result2table, ...)
        ...
        else:
            await asyncio.to_thread(utils.csv_to_file, ...)
    # ← end of method, no timing log
```

Changes:

1. Add `stage_start = time.perf_counter()` as the first statement of the
   method body (before the `res_json = await ...` call).
2. Add a timing log **before** the size-guard `return`:

```python
logger.info(
    "task_stage_complete stage=extract name=%s target=%s duration=%.4f",
    task.name,
    task.target,
    time.perf_counter() - stage_start,
)
return
```

3. Add a timing log at the **end** of the method (after the `if task.status:`
   block, at method-body indentation level) covering all other exit paths
   (`res_json is False`, empty `DataFrame`, `None` DataFrame, prepare
   failure, and successful CSV write):

```python
logger.info(
    "task_stage_complete stage=extract name=%s target=%s duration=%.4f",
    task.name,
    task.target,
    time.perf_counter() - stage_start,
)
```

Key decisions:

- The size guard is Block 1's `max_result_rows` check — its logic and the
  `return` are **unchanged**. Only the timing log is inserted before the
  existing `return`.
- `test_worker_log_error_false_no_json5` mocks `result2table` to return an
  empty `pd.DataFrame()` → `df.empty` branch sets `task.status=False,
  task.log_error=False` and falls through to the end-of-method timing log.
  The test spies only on `logger.warning` (not `info`) and asserts
  `any("No data" in msg ...)`. The new info log does not interfere.
- The lazy-format string does not contain `"is ready"` or `"will be skipped"`.

---

## Architectural constraints

1. **No line-number dependencies.** All references use semantic code units
   (`MediaReport._process_item`, `TVMediaReport.send_task`,
   `TVMediaReport.wait_task`, `TVMediaReport.extract_data`). Line numbers in
   this document are illustrative only — locate targets by symbol.

2. **`time` import in `reports.py`.** Add `import time` to the stdlib import
   group. Do **not** import `perf_counter` into `services.py` — the report-level
   `datetime.now()` timing there is intentionally left untouched (different
   granularity/semantics, human-readable).

3. **`_process_item` owns the per-task total.** The total elapsed time is
   recorded in `MediaReport._process_item`, not in `worker`. The `worker`
   method is not modified.

4. **`try/finally` in `_process_item` is logging-only.** The `finally` block
   must contain **only** the `logger.info` call. It must **not** `return`,
   `await`, `yield`, or swallow the propagating exception/timeout. This
   preserves `worker`'s `except TimeoutError` / `except Exception` branches
   unchanged.

5. **Lazy formatting only (no f-strings for new timing logs).** All four new
   log records use `%s`/`%.4f` positional args — `logger.info(fmt, *args)`.
   This is required because the lifecycle tests spy on `logger.info` and
   inspect `str(call.args[0])` (the format string). F-strings would embed
   interpolated values into `args[0]`, risking substring collisions with the
   existing `"is ready"`, `"will be skipped"`, and `"No data"` assertions.

6. **Format string safety.** None of the four timing format strings may
   contain the substrings `"is ready"`, `"will be skipped"`, or `"No data"`.
   (Verified: `task_stage_complete stage=…` and `task_complete …` contain
   neither.)

7. **`loop.time()` in `wait_task` is preserved.** The existing
   `start = loop.time()` / `loop.time() - start > poll_deadline` logic is
   **not** repurposed. It remains for deadline enforcement. `time.perf_counter()`
   is used **only** for the new stage-duration log.

8. **`retryable` / `api_call` semantics preserved.** No changes to
   `NetworkClient.call` or the `retryable=False` task POST. PERF-001/002
   Block 1 changes are already live and must not be regressed.

9. **`TVTask` is unchanged.** Stage durations are logged inline (computed via
   local `stage_start` variables), not stored as new `TVTask` attributes. No
   model/schema changes.

---

## Files and targets (semantic)

```
src/mko_get_mediascope_data/core/reports.py
├── (module imports)            ← add `import time`
├── class: MediaReport
│   └── method: _process_item   ← add perf_counter start + try/finally total log
└── class: TVMediaReport
    ├── method: send_task       ← add stage_start; replace logger.info("=")
    ├── method: wait_task       ← add stage_start; log after while-loop
    └── method: extract_data    ← add stage_start; log before size-guard return + at end

tests/test_reports_lifecycle.py
└── (new) async function: test_worker_emits_per_task_duration
```

### Semantic anchors (modification points)

| Location | Anchor | Action |
|---|---|---|
| `reports.py` imports | `import logging` (line 4) | insert `import time` after it |
| `MediaReport._process_item` | `await self.send_task(item)` (start of body) | insert `start = time.perf_counter()` before it; wrap body in `try:`; add `finally:` with total log |
| `TVMediaReport.send_task` | `task_json = await self.network_client.call(self.builder, ...)` | insert `stage_start = time.perf_counter()` before it |
| `TVMediaReport.send_task` | `logger.info("=")` | replace with `logger.info("task_stage_complete stage=send …", task.name, task.target, time.perf_counter() - stage_start)` |
| `TVMediaReport.wait_task` | `start = loop.time()` | insert `stage_start = time.perf_counter()` after it |
| `TVMediaReport.wait_task` | after the `while True` loop body (after the final `sleep_interval = min(sleep_interval * 2, 60.0)` statement, at method-body indentation) | insert stage duration log after the loop closes |
| `TVMediaReport.extract_data` | `res_json = await self.network_client.call(self.report_service.get_result, task.key)` | insert `stage_start = time.perf_counter()` before it |
| `TVMediaReport.extract_data` | `return` (the size-guard early return, inside `if len(result_body) > self.max_result_rows:`) | insert stage duration log before it |
| `TVMediaReport.extract_data` | end of method (after the `if task.status:` block / `asyncio.to_thread(...)`) | insert stage duration log at method-body indent |

---

## Required changes (structured)

### Change 2.1 — Add `import time`

- **action:** `add_import`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **description:** >
  Add `import time` to the stdlib import group (after `import logging`,
  before `from abc import ...`). Required because `reports.py` does not yet
  import `time` but `_process_item`, `send_task`, `wait_task`, and
  `extract_data` all need `time.perf_counter()`.

### Change 2.2 — Per-task total in `_process_item`

- **action:** `wrap_body_with_try_finally`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `MediaReport._process_item`
- **description:** >
  Record `start = time.perf_counter()` at the top of the method body. Wrap the
  existing three `await` calls (send_task, conditional wait_task, conditional
  extract_data) in a `try:` block. Add a `finally:` block that computes
  `elapsed = time.perf_counter() - start` and emits:
  `logger.info("task_complete name=%s target=%s interval=%s total=%.4f", item.name, item.target, item.interval, elapsed)`.
  The `finally` is logging-only (no return/await/catch).
- **constraints:**
  - Must not change the conditional staging logic
    (`if item.status is True:`).
  - Must not swallow exceptions — let them propagate to `worker`.

### Change 2.3 — Replace `logger.info("=")` in `send_task` with stage timing

- **action:** `add_timing` + `replace_log`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `TVMediaReport.send_task`
- **description:** >
  Add `stage_start = time.perf_counter()` as the first statement of the
  method. Replace the existing `logger.info("=")` line with a structured
  lazy-formatted record:
  `logger.info("task_stage_complete stage=send name=%s target=%s duration=%.4f", task.name, task.target, time.perf_counter() - stage_start)`.
  The replacement sits at the same position (inside `if task_json is not
  False:`, after `task.key` is assigned).
- **constraints:**
  - The `retryable=False` keyword on the `self.sender` call is preserved.
  - The `api_call=False` keyword on the `self.builder` call (Block 1) is
    preserved.

### Change 2.4 — Add stage timing to `wait_task`

- **action:** `add_timing`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `TVMediaReport.wait_task`
- **description:** >
  Add `stage_start = time.perf_counter()` immediately after the existing
  `start = loop.time()` line. After the `while True` loop body ends (all
  `break` paths reach this point), emit:
  `logger.info("task_stage_complete stage=wait name=%s target=%s duration=%.4f", task.name, task.target, time.perf_counter() - stage_start)`.
  Do not modify the loop's poll-count / poll-deadline logic.
- **constraints:**
  - `start = loop.time()` and the `poll_deadline` check are unchanged.
  - The `max_polls` / `poll_deadline` parameters and their checks are
    unchanged (PIPE-006).

### Change 2.5 — Add stage timing to `extract_data`

- **action:** `add_timing`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `TVMediaReport.extract_data`
- **description:** >
  Add `stage_start = time.perf_counter()` as the first statement of the
  method body (before the `get_result` call). Emit the same
  `task_stage_complete stage=extract` record at two points:
  (a) immediately before the existing size-guard `return` (inside
  `if len(result_body) > self.max_result_rows:`), and
  (b) at the end of the method (after the `if task.status:` block, at
  method-body indentation). Both use
  `time.perf_counter() - stage_start` for the duration.
- **constraints:**
  - The Block 1 `max_result_rows` guard logic and its `return` are unchanged.
  - The `api_call=False` keyword on the `result2table` call (Block 1) is
    preserved.
  - Only one of the two log points fires per invocation (the guard returns,
    the success path reaches the end).

### Change 2.6 — Add test `test_worker_emits_per_task_duration`

- **action:** `add_test`
- **file:** `tests/test_reports_lifecycle.py`
- **target:** module-level async function (in the PERF-003 / Phase 5.3 section)
- **description:** >
  Verify that the real `create_report` pipeline emits structured per-task
  timing logs. This test mirrors the setup of
  `test_create_report_writes_csv_and_marks_queue_done` (success path:
  `get_status→{"taskStatus":"DONE"}`, `get_result→{"result":"mock_data"}`,
  `result2table` returns a real DataFrame from `_build_result_df`) and the
  spy pattern of `test_worker_does_not_log_ready_on_failed`
  (`mocker.spy(reports_logger, "info")` where `reports_logger =
  logging.getLogger("mko_get_mediascope_data.core.reports")`).
- **test approach:**
  1. Parse `ReportSettings` from `tv_json5_content`.
  2. Obtain `Data` settings from `hermetic_app_service`.
  3. Build `result_df` via `_build_result_df`.
  4. Double `MediaVortexTask` (build_task→{"task_json":"mock"},
     send_task→"mock_task_key_123", get_status→{"taskStatus":"DONE"},
     get_result→{"result":"mock_data"}, result2table→result_df).
  5. Real `NetworkClient(sleep_time=0, max_attempts=1)`; mock `asyncio.sleep`.
  6. Spy on `reports_logger.info` (do **not** spy on `warning` — the success
     path does not emit warnings).
  7. `await asyncio.wait_for(report.create_report(), timeout=10.0)`.
  8. Assertions:
     - `report.task_queue.qsize() == 0` (queue drained — no hang).
     - At least one info call has `args[0]` containing `"task_complete"` (the
       per-task total from `_process_item`).
     - At least one info call has `args[0]` containing
       `"task_stage_complete"` (a stage-level duration from
       `send_task` / `wait_task` / `extract_data`).
     - For every timing-related info call (those whose `args[0]` contains
       `"task_complete"` or `"task_stage_complete"`): at least one
       non-format argument is a `float` (the duration value), and at least
       one non-format argument is a non-empty `str` (the task name).
     - Cross-reference: extract the task name from the success-path
       `logger.info(f"Task {item.name} is ready.")` call (f-string —
       `args[0]` is the rendered string; strip `"Task "` prefix and
       `" is ready."` suffix) and assert that name also appears among the
       `str(a)` values of at least one timing call's arguments.
     - Negative: no timing call's `args[0]` (format string) contains
       `"is ready"` and no timing call's `args[0]` contains
       `"will be skipped"`.
- **not slow:** This test uses mocked `asyncio.sleep` and a single mocked
  task — it runs in the fast suite. Do **not** add `@pytest.mark.slow`.
- **constraints:**
  - Must not assert on exact call counts of `logger.info` (the number of
    tasks emitted by `DefaultTVTaskStrategy` depends on the fixture's target
    audiences and period — counts are not stable contracts).
  - Must not assert on specific duration magnitudes (timings are
    environment-dependent; assert on type/presence only).

---

## Risk register

| Risk | Mitigation | Severity |
|---|---|---|
| `import time` collides with existing symbol | grep `reports.py` — no `time` name currently exists; safe to add | Low |
| `try/finally` in `_process_item` swallows the exception | `finally` is logging-only (no `return`/`catch`); exceptions propagate normally to `worker` | Low |
| `try/finally` finally-block runs during `asyncio.wait_for` cancellation (`test_create_report_timeout_does_not_hang`) | `logger.info` is synchronous — safe under `CancelledError`; finally does not `await` | Low |
| New info logs captured by `test_worker_does_not_log_ready_on_failed` spy break the `"is ready"` negative assertion | Format strings use `"task_stage_complete"` / `"task_complete"` — neither contains `"is ready"` or `"will be skipped"` | Low |
| New info logs captured by `test_worker_log_error_false_no_json5` spy break the `"No data"` warning assertion | That test spies only on `warning`, not `info`; timing logs are `info`-level | Low |
| `test_worker_log_error_true_writes_json5` — send_task returns `""` | Timing log fires inside `if task_json is not False:` (task_json is truthy); test asserts only on error JSON5 `log_error` field — info logs are inert | Low |
| `test_worker_handles_exception_and_writes_error_json5` — send_task raises before the `logger.info("=")` site | The send-stage timing log is never reached (exception propagates first); `_process_item`'s `finally` still logs the total; test asserts only on `error_payload["error"]` — info logs are inert | Low |
| `wait_task` direct-call tests assert `task.error` / `task.status` | New info log is added **after** the `while` loop, after all status/error assignments — does not alter `task` state | Low |
| mypy strict mode flags `time.perf_counter() - stage_start` or `%s` with `None` interval | `stage_start` is `float`; `time.perf_counter()` is `float`; `%.4f` expects `float` ✓; `%s` accepts `str | None` ✓; `task.interval` is `tuple | None`, stringified by `%s` ✓ | Low |
| ruff `G004` (f-string in logging) flags existing f-string logs | Pre-existing — the codebase already uses `logger.info(f"…")` extensively in `worker`; ruff does not flag them (G004 not enabled). New logs use lazy formatting (strictly better) | Low |

---

## Acceptance criteria

- [ ] `imports time` added to `src/mko_get_mediascope_data/core/reports.py`
- [ ] `MediaReport._process_item` records `start = time.perf_counter()` and
      logs `"task_complete … total=%.4f"` in a `finally` block (all exit paths)
- [ ] `TVMediaReport.send_task` records `stage_start = time.perf_counter()` and
      `logger.info("=")` is replaced with
      `logger.info("task_stage_complete stage=send … duration=%.4f", …)` using
      lazy `%s`/`%.4f` formatting
- [ ] `TVMediaReport.wait_task` records `stage_start = time.perf_counter()` and
      logs `"task_stage_complete stage=wait … duration=%.4f"` after the
      `while` loop; `loop.time()` / `poll_deadline` logic unchanged
- [ ] `TVMediaReport.extract_data` records `stage_start = time.perf_counter()`
      and logs `"task_stage_complete stage=extract … duration=%.4f"` at the end
      of the method **and** before the size-guard `return`
- [ ] All new timing log records use positional lazy-formatting (no f-strings)
- [ ] No new timing log format string contains `"is ready"`, `"will be skipped"`,
      or `"No data"`
- [ ] `NetworkClient.call` signature unchanged (no `api_call`/`retryable` changes)
- [ ] `worker` method unchanged (error handling, queue, control flow intact)
- [ ] `TVTask` unchanged (no new attributes)
- [ ] `services.py` `datetime.now()` report-level timing unchanged
- [ ] New test `test_worker_emits_per_task_duration` passes
- [ ] All 112 existing tests pass (110 original + 2 Block 1 additions)
- [ ] `ruff check` clean on `src/mko_get_mediascope_data/core/reports.py` and
      `tests/test_reports_lifecycle.py`
- [ ] `mypy` clean on `src/mko_get_mediascope_data/core/reports.py`

### Verification sequence

```bash
uv run ruff check src/mko_get_mediascope_data/core/reports.py \
                    tests/test_reports_lifecycle.py
uv run mypy src/mko_get_mediascope_data/core/reports.py
uv run pytest tests/test_reports_lifecycle.py::test_worker_emits_per_task_duration -v
uv run pytest -m "not slow"                        # fast suite, expect 112 + 1 = 113 passed
uv run pytest -m slow                              # the 2 slow end-to-end tests
uv run pytest                                      # full suite, expect 113 passed
```
