---
id: PERF-001-002-block1
task_id: "05-performance-fixes-block1-task"
title: "Block 1 — PERF-001 api_call discriminator + PERF-002 max_result_rows guard"
priority: high
status: ready
depends_on: []
source_plan: .ai/plans/05-performance-fixes.md
source_section: "Block 1 — PERF-001 + PERF-002 (extract_data + NetworkClient.call rework)"
source_findings: .ai/audit/99-validation/13-performance-validated-findings.md
source_code_context: .ai/audit/99-validation/13-performance-code-context.md
finding_ids: ["PERF-001", "PERF-002"]
---

# Task — Block 1: PERF-001 (CPU/local call bypass) + PERF-002 (result-size guard)

## Overview

Two confirmed MEDIUM findings from the performance audit remain structurally valid at
HEAD. This task implements both — **Block 1** of the execution plan
(`.ai/plans/05-performance-fixes.md`, §4 "Block 1").

- **PERF-001** — `build_task`, `result2table`, and `get_connection` (local/CPU work)
  are routed through the I/O `Semaphore` and the initial-jitter gate in
  `NetworkClient.call`, causing unnecessary delays and slot contention for work that
  does not perform network I/O.
- **PERF-002** — `extract_data` has no data-volume guard; an oversized `resultBody`
  is fully materialized by the vendored `result2table` into memory with no ceiling.

### Implementation approach (decided, verbatim from plan)

> Add `api_call: bool = True` parameter to `NetworkClient.call`. When
> `api_call=False`: skip the initial jitter, skip Semaphore acquisition, single
> attempt (no retry loop), still use `_invoke` (to_thread + optional call_timeout).
> Call sites pass `api_call=False` for: `build_task`, `result2table`, `get_connection`.
> API call sites keep `api_call=True` (default).
>
> Add `max_result_rows: int = 10000` to `TVMediaReport.__init__`. In `extract_data`,
> check `len(res_json.get('resultBody', []))` against `max_result_rows` before calling
> `result2table`. If exceeded: set `task.status=False`, `task.log_error=True`,
> `task.error=<clear message>`. Existing worker error-handling path writes the error
> artifact.

### What is NOT in scope (Block 2 carries these)

- PERF-003: per-task latency instrumentation in `worker`/`_process_item` and stage
  methods; replacement of `logger.info("=")`.
- Any changes to the Strategy pattern (`ReportFactory`,
  `TaskGenerationStrategy`, `DataPreparationStrategy`).
- Configuration schema changes (no new JSON5 settings fields).
- `max_concurrent_requests` or `max_tasks` configurability (both remain hardcoded
  defaults).

---

## Implementation sequence (ordered, dependency-safe)

### Step 1 — Add `api_call` parameter to `NetworkClient.call`

**File:** `src/mko_get_mediascope_data/core/network.py`
**Target:** `NetworkClient.call` method

Changes:

1. Add `api_call: bool = True` to the `call` signature, positioned after
   the existing `retryable: bool = True` parameter (preserving EXT-001's
   `retryable` semantics for the task-submission POST).

2. Restructure the branch so that `api_call=False` takes the fast path
   **before** the initial jitter is applied. The new control flow:

   ```
   if not api_call:
       return await self._invoke(func, *args, **kwargs)

   # --- API call path (existing) ---
   _sleep_time = max(sleep_time, self._sleep_time)
   await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))   # initial jitter
   if not retryable:
       async with self._semaphore:
           return await self._invoke(func, *args, **kwargs)
   for attempt in range(1, self._max_attempts + 1):
       ...existing retry loop...
   ```

   - `api_call=False` → no jitter, no Semaphore, no `for` retry loop, no
     `MaxRetriesExceeded`. Delegates directly to `_invoke` (which still
     applies `asyncio.to_thread` + optional `call_timeout`).
   - `api_call=True` (default) → **unchanged** behavior: initial jitter +
     Semaphore + retry loop. The `retryable` parameter continues to control
     whether the retry loop runs (EXT-001: task POST uses
     `retryable=False, api_call=True`).
   - Error logging via `_redact_args` is preserved for the API path; the
     CPU/local path delegates directly to `_invoke` which raises raw errors
     to the caller.

   **Non-goals:** Do not remove the existing `retryable` parameter or alter
   EXT-001 semantics (`retryable=False` still means "non-retried API call").

### Step 2 — Update CPU/local call sites to pass `api_call=False`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Targets:** `TVMediaReport.send_task` method, `TVMediaReport.extract_data` method

**File:** `src/mko_get_mediascope_data/core/services.py`
**Target:** `AppService.run_report` method

Changes — exactly three call sites:

| Call site | Function | Current | Target |
|---|---|---|---|
| `send_task` | `self.builder` (`build_task`) | `network_client.call(self.builder, ...)` | add `api_call=False` |
| `extract_data` | `self.report_service.result2table` | `network_client.call(self.report_service.result2table, ...)` | add `api_call=False` |
| `run_report` | `self.get_connection` | `network_client.call(self.get_connection, ...)` | add `api_call=False` |

API call sites — **must NOT change** (keep `api_call=True` default):

| Call site | Function |
|---|---|
| `get_available_period` | `self.catalogs.get_availability_period` |
| `wait_task` | `self.report_service.get_status` |
| `extract_data` | `self.report_service.get_result` |
| `send_task` | `self.sender` (task POST — already `retryable=False`) |

### Step 3 — Add `max_result_rows` parameter to `TVMediaReport.__init__`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `TVMediaReport.__init__` method

Changes:

1. Add `max_result_rows: int = 10000` parameter to
   `TVMediaReport.__init__`, positioned after `check_done` (the last
   existing parameter) to avoid disrupting positional argument order.

2. Store as instance attribute: `self.max_result_rows = max_result_rows`.

3. `AppService.run_report` constructs `TVMediaReport` without `max_tasks=`
   today (uses default); it should also not pass `max_result_rows` (use the
   default). No change needed in `services.py` for this parameter — the
   default of 10 000 applies.

### Step 4 — Add size guard in `extract_data`

**File:** `src/mko_get_mediascope_data/core/reports.py`
**Target:** `TVMediaReport.extract_data` method

Changes (inserted between the `get_result` call and the `result2table` call,
i.e. after the existing `res_json is False` guard and before the
`if task.status:` block that calls `result2table`):

```python
# PERF-002: guard against oversized result bodies before materialization
result_body = res_json.get("resultBody", [])
if len(result_body) > self.max_result_rows:
    task.status = False
    task.log_error = True
    task.error = (
        f"Result size {len(result_body)} exceeds maximum "
        f"of {self.max_result_rows} rows"
    )
    logger.error(
        f"Task '{task.name}' result size {len(result_body)} "
        f"exceeds max_result_rows {self.max_result_rows} — skipping extraction."
    )
    return
```

Key design decisions:

- The guard checks `res_json.get("resultBody", [])` **before** calling
  `result2table` — NOT the DataFrame after. This avoids breaking
  `test_worker_log_error_false_no_json5` which mocks `result2table` to
  return an empty `pd.DataFrame()` (the `df.empty` branch at `Reports.py`
  `extract_data` body). With `>`, an empty result body (0 rows) never
  triggers rejection.
- On rejection, the method sets `task.status=False`, `task.log_error=True`,
  and `task.error` to a clear message, then returns. The `worker` method's
  `if item.status is False` / `if item.log_error` branch handles writing the
  error artifact via `item.to_file(self.export_path)` — no changes needed
  in `worker`.
- `result_body` is computed once and reused; no duplicate `res_json.get`
  calls.

### Step 5 — Add test `test_cpu_local_calls_bypass_semaphore_and_jitter`

**File:** `tests/test_network.py`

The test verifies that when `api_call=False`:

1. `asyncio.sleep` is **never** called (initial jitter is skipped).
2. The `Semaphore` is **never** acquired (bypassed entirely).
3. The wrapped function executes exactly once (no retry loop).
4. The return value is forwarded correctly.

Test approach (mirrors existing `test_call_respects_semaphore` style):

- Create `NetworkClient(max_concurrent_requests=2, max_attempts=3, sleep_time=2)`.
- Patch `asyncio.sleep` with `AsyncMock` — assert `call_count == 0` afterward.
- Patch `_invoke` with an `AsyncMock` that returns a sentinel value — assert
  `call_count == 1`.
- Spy on `self._semaphore.acquire` — assert it was never awaited.
- Assert the returned value matches the sentinel.

### Step 6 — Add test `test_large_result_is_rejected`

**File:** `tests/test_reports_lifecycle.py`

The test verifies that `extract_data` rejects oversized `resultBody` and that
the `worker` writes an error artifact with `log_error=True`.

Test approach (mirrors existing lifecycle test patterns — real
`TVMediaReport`, real `NetworkClient` with `sleep_time=0, max_attempts=1`,
doubled `MediaVortexTask`, `asyncio.sleep` mocked):

1. Parse `ReportSettings` from `tv_json5_content` fixture.
2. Build `Data` settings via `hermetic_app_service`.
3. Mock `MediaVortexTask` so `get_result` returns a dict with a
   `resultBody` list exceeding 10 000 entries (e.g.
   `[{"dummy": i} for i in range(10001)]`).
4. Mock `result2table` to return a valid DataFrame (it should **never** be
   called — assert `assert_not_called()`).
5. Construct `TVMediaReport` with the real `NetworkClient`.
6. Run `create_report()` within `asyncio.wait_for(..., timeout=10.0)`.
7. Assert:
   - `result2table` was not called (size guard short-circuits).
   - An error JSON5 exists in `export_path/errors/`.
   - The error payload has `log_error is True`.
   - `task.error` contains "exceeds maximum" or "exceeds max_result_rows".

### Step 7 — Lint + test gate

Run locally:

```bash
uv run ruff check src/mko_get_mediascope_data/core/network.py \
                    src/mko_get_mediascope_data/core/reports.py \
                    src/mko_get_mediascope_data/core/services.py \
                    tests/test_network.py \
                    tests/test_reports_lifecycle.py
uv run mypy src/mko_get_mediascope_data/core/network.py \
              src/mko_get_mediascope_data/core/reports.py \
              src/mko_get_mediascope_data/core/services.py
uv run pytest                          # all 110 + 2 new = 112 tests
uv run pytest -m slow                  # the 2 slow tests
```

---

## Files and targets (semantic)

### Production code

```
src/mko_get_mediascope_data/core/network.py
  ├── class: NetworkClient
  │   ├── method: __init__
  │   └── method: call           ← add api_call parameter; restructure branches
  ├── method: NetworkClient._invoke  ← unchanged (reused by both paths)
  └── method: NetworkClient._redact_args  ← unchanged

src/mko_get_mediascope_data/core/reports.py
  ├── class: TVMediaReport (subclass of MediaReport)
  │   ├── method: __init__       ← add max_result_rows parameter + attribute
  │   ├── method: send_task      ← pass api_call=False to builder call
  │   └── method: extract_data   ← add resultBody size guard; pass api_call=False
  └── class: MediaReport
      ├── method: worker         ← unchanged (error path already handles log_error=True)
      └── method: _process_item  ← unchanged

src/mko_get_mediascope_data/core/services.py
  └── class: AppService
      └── method: run_report     ← pass api_call=False to get_connection call
```

### Test code

```
tests/test_network.py
  └── (new) async function: test_cpu_local_calls_bypass_semaphore_and_jitter

tests/test_reports_lifecycle.py
  └── (new) async function: test_large_result_is_rejected
```

### Semantic anchors (insertion / modification points)

| Location | Anchor | Action |
|---|---|---|
| `NetworkClient.call` signature | after `retryable: bool = True` parameter | add `api_call: bool = True` |
| `NetworkClient.call` body start | before `# Small initial delay to smooth peak loads` / `await asyncio.sleep(uniform(...))` | insert `if not api_call:` early-return before the jitter |
| `TVMediaReport.__init__` signature | after `check_done: bool \| None = True` parameter | add `max_result_rows: int = 10000` |
| `TVMediaReport.__init__` body | after `super().__init__(...)` call | set `self.max_result_rows = max_result_rows` |
| `TVMediaReport.send_task` body | at `task_json = await self.network_client.call(self.builder, ...)` | add `api_call=False` |
| `TVMediaReport.extract_data` body | after `res_json = await self.network_client.call(self.report_service.get_result, task.key)` and the `res_json is False` guard, before `if task.status:` | insert resultBody size guard |
| `TVMediaReport.extract_data` body | at `df: DataFrame \| None = await self.network_client.call(self.report_service.result2table, ...)` | add `api_call=False` |
| `AppService.run_report` body | at `report_service: MediaVortexTask = await network_client.call(self.get_connection, report_settings.media)` | add `api_call=False` |

---

## Required changes (structured)

### Change 1.1 — `NetworkClient.call`: add `api_call` discriminator

- **action:** `modify_signature` + `restructure_branches`
- **file:** `src/mko_get_mediascope_data/core/network.py`
- **target:** `NetworkClient.call`
- **description:** >
  Add `api_call: bool = True` parameter. When `api_call=False`, bypass
  the initial jitter (`asyncio.sleep(uniform(...))`), bypass the
  `asyncio.Semaphore`, skip the retry loop entirely, and delegate
  directly to `_invoke` for a single attempt. When `api_call=True`
  (default), behavior is unchanged (jitter + Semaphore + retry loop).
  The existing `retryable` parameter (EXT-001) is preserved independently —
  it controls retry-vs-single-attempt within the API path.
- **constraints:**
  - `api_call` must be a keyword-only parameter to avoid positional
    argument collisions with `sleep_time`.
  - `_invoke` must always be used (preserves `asyncio.to_thread` +
    `call_timeout` semantics for thread offloading).
  - No new exception types introduced.

### Change 1.2 — Update CPU/local call sites

- **action:** `update_call_site`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **targets:**
  - `TVMediaReport.send_task` — `self.builder` call
  - `TVMediaReport.extract_data` — `self.report_service.result2table` call
- **file:** `src/mko_get_mediascope_data/core/services.py`
- **target:** `AppService.run_report` — `self.get_connection` call
- **description:** >
  Add `api_call=False` keyword argument to each of the three CPU/local
  call sites. Do NOT modify API call sites (`get_status`,
  `get_result`, `get_availability_period`, task-submission POST via
  `self.sender`).

### Change 1.3 — Add `max_result_rows` to `TVMediaReport.__init__`

- **action:** `add_parameter`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `TVMediaReport.__init__`
- **description:** >
  Add `max_result_rows: int = 10000` parameter. Store as
  `self.max_result_rows`. Default 10 000 is above any currently-passing
  test's data volume and matches the audit's probe scale.

### Change 1.4 — Add result-size guard in `extract_data`

- **action:** `insert_guard`
- **file:** `src/mko_get_mediascope_data/core/reports.py`
- **target:** `TVMediaReport.extract_data`
- **description:** >
  After the `get_result` call and its `res_json is False` guard, but
  before the `result2table` call, check
  `len(res_json.get("resultBody", []))` against `self.max_result_rows`.
  If exceeded, set `task.status=False`, `task.log_error=True`,
  `task.error=<descriptive message>`, log an error, and return early.
  The `worker` method's existing error-handling path writes the
  artifact via `item.to_file(self.export_path)`.
- **constraints:**
  - Guard must use strict `>` (not `>=`) so empty result bodies
    (0 rows) do not trigger rejection — preserves
    `test_worker_log_error_false_no_json5`.
  - Guard checks `resultBody` length on `res_json`, NOT `df.empty`
    after `result2table` — preserves the empty-DataFrame test path.

### Change 1.5 — Add test `test_cpu_local_calls_bypass_semaphore_and_jitter`

- **action:** `add_test`
- **file:** `tests/test_network.py`
- **target:** module-level function
- **description:** >
  Assert `api_call=False` skips initial jitter (0 `asyncio.sleep`
  calls), bypasses the `Semaphore` (0 `acquire` calls), executes the
  wrapped function exactly once (no retry loop), and forwards the
  return value.

### Change 1.6 — Add test `test_large_result_is_rejected`

- **action:** `add_test`
- **file:** `tests/test_reports_lifecycle.py`
- **target:** module-level function
- **description:** >
  Assert `extract_data` rejects an oversized `resultBody` (10 001
  entries) by setting `task.status=False` / `task.log_error=True`,
  skipping `result2table` entirely, and triggering the `worker` error
  artifact path (error JSON5 with `log_error is True`).

---

## Architectural constraints

1. **No line-number dependencies.** All references use semantic code units
   (class, method, function, parameter name). The implementor should locate
   targets by symbol, not by line number — line numbers will have shifted.

2. **`api_call` is keyword-only.** Must not collide with positional args
   (`func`, `*args`, `sleep_time`). Place after `retryable` in the signature.

3. **`retryable` semantics preserved.** EXT-001's task-submission POST uses
   `retryable=False, api_call=True` (default). It still gets jitter +
   Semaphore, just no retry loop. This must not change.

4. **`_invoke` is always the execution wrapper.** Both the CPU/local fast
   path and the API path must route through `_invoke` (preserving
   `asyncio.to_thread` + `call_timeout`). No direct `func(*args)` calls.

5. **Size guard checks `resultBody` before `result2table`.** Never checks
   `df.empty` — the guard operates on raw API response data, not the
   post-materialization DataFrame.

6. **Worker error path is reused.** The `worker` method's existing
   `if item.status is False: if item.log_error: ... to_file` branch
   already handles error artifact writing. Do not modify `worker` or
   `_process_item`.

7. **`max_result_rows` default of 10 000.** Must not break any existing
   passing test. All lifecycle/slow tests use mock `resultBody` with
   `{"result": "mock_data"}` (no `resultBody` key → `len([]) == 0`,
   never exceeds threshold).

8. **No config schema changes.** `max_result_rows` lives on
   `TVMediaReport.__init__`, not on any Pydantic `Data` / `ReportSettings`
   model or JSON5 settings template.

9. **No new abstractions.** The fix adds a parameter and a guard — no new
   classes, no new modules, no Strategy pattern changes.

---

## Risk register

| Risk | Mitigation | Severity |
|---|---|---|
| `api_call` parameter breaks existing `call()` callers | Default `True`; keyword-only; all existing callers omit it → unaffected | Low |
| Lifecycle tests (real `NetworkClient`) break under new branch | `api_call=True` default preserves all API-path behavior unchanged | Low |
| `test_worker_log_error_false_no_json5` breaks (empty DataFrame path) | Guard checks `resultBody` length on `res_json` before `result2table`; empty body → `len([]) == 0`, never exceeds default | Low |
| `test_run_report_with_real_models` / `test_run_report_multi_entry_yields_multiple_paths` mock `call` via `AsyncMock(side_effect=...)` — new kwarg not absorbed | `api_call` is keyword-only in `call`; the mock's `side_effect(func, *args, **kwargs)` absorbs it; `get_connection` is the only CPU call in `services.py` and its mock returns `mock_report_service` before dispatching | Low |
| `test_call_respects_semaphore` asserts Semaphore caps concurrency | Test only exercises `api_call=True` (default) path → Semaphore still active → assertion holds | Low |
| `test_send_task_not_retried_on_retryable_error` asserts `retryable=False` → 1 call | `retryable=False` + `api_call=True` (default) → still skips retry loop → 1 call. Jitter still runs (mocked `asyncio.sleep`). Assertion holds | Low |
| `test_backoff_grows_exponentially` / `test_initial_jitter_in_range` assert jitter formula | These tests use `api_call=True` (default) → jitter path unchanged → assertions hold | Low |

---

## Acceptance criteria

- [ ] `NetworkClient.call` has `api_call: bool = True` parameter
- [ ] `api_call=False` path skips initial jitter, skips `Semaphore`, single attempt via `_invoke`
- [ ] `api_call=True` (default) path is behavior-identical to current (jitter + Semaphore + retry loop)
- [ ] `retryable` parameter semantics unchanged (EXT-001 task POST unaffected)
- [ ] `TVMediaReport.send_task`: `build_task` call passes `api_call=False`
- [ ] `TVMediaReport.extract_data`: `result2table` call passes `api_call=False`
- [ ] `AppService.run_report`: `get_connection` call passes `api_call=False`
- [ ] API call sites (`get_status`, `get_result`, `get_availability_period`, task POST) keep `api_call=True` (default, no explicit kwarg needed)
- [ ] `TVMediaReport.__init__` has `max_result_rows: int = 10000` parameter
- [ ] `extract_data` checks `resultBody` length before `result2table` call
- [ ] Oversized result sets `task.status=False`, `task.log_error=True`, clear `task.error` message, returns early
- [ ] `result2table` is NOT called when size guard triggers
- [ ] Test `test_cpu_local_calls_bypass_semaphore_and_jitter` passes
- [ ] Test `test_large_result_is_rejected` passes
- [ ] All 110 existing tests pass (including 2 `@pytest.mark.slow` tests)
- [ ] `ruff check` clean on all touched source + test files
- [ ] `mypy` clean on all touched source files
