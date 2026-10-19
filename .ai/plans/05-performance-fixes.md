---
plan_id: "05-performance-fixes"
source: ".ai/audit/99-validation/13-performance-validated-findings.md"
source_code_context: ".ai/audit/99-validation/13-performance-code-context.md"
status: active
agent_role: "tech-lead"
findings_covered: ["PERF-001", "PERF-002", "PERF-003"]
---

# Execution Plan — Performance Audit Fixes (PERF-001 / 002 / 003)

## 1. Overview

Implement the three confirmed MEDIUM findings from the Phase 13 performance audit
into production code and tests. All three findings are **structurally valid at HEAD**
(`57ad5d9`), confirmed by the Auditor's code-context report
(`.ai/audit/99-validation/13-performance-code-context.md`).

| ID | Severity | Title | Fix targets |
|---|---|---|---|
| PERF-001 | MEDIUM | CPU/local work routed through I/O Semaphore + initial jitter | `NetworkClient.call`, `TVMediaReport.send_task`, `TVMediaReport.extract_data`, `AppService.run_report` |
| PERF-002 | MEDIUM | No data-volume backpressure; oversized result in memory | `TVMediaReport.extract_data`, `TVMediaReport.__init__` |
| PERF-003 | MEDIUM | No per-task latency measurement | `MediaReport.worker`, `_process_item`, `TVMediaReport.send_task`/`wait_task`/`extract_data` |

## 2. Key findings from the Auditor (code-context report)

- The `retryable` parameter on `NetworkClient.call` exists (EXT-001) but does NOT
  address PERF-001: it still applies the initial jitter (line 103) and acquires the
  Semaphore (line 106) even for `retryable=False` calls.
- A 4th CPU/local call site not enumerated in the validation report: `get_connection`
  in `services.py:186-189` routes local I/O (credential file reading + MediaVortexTask
  construction) through `call` with the I/O Semaphore + jitter.
- The vendored `result2table` (tasks.py:1451-1496) materializes the entire `resultBody`
  into a dict-of-lists then `pd.DataFrame` — no streaming, no size cap. A **reject** guard
  in `extract_data` is the practical approach (not stream).
- 110 tests at HEAD (not the 12-23 in the validation report). 24 lifecycle tests in
  `test_reports_lifecycle.py` exercise the **real** pipeline with a doubled
  `MediaVortexTask` and a real `NetworkClient` (only `asyncio.sleep` is mocked).
- `test_run_report_with_real_models` and `test_run_report_multi_entry_yields_multiple_paths`
  in `test_services.py` (both `@pytest.mark.slow`) run the **real** `create_report` —
  they do NOT mock `create_report` as the validation report claimed.
- The `retryable` parameter provides the natural extension point in `call()`: add a second
  boolean discriminator (e.g., `api_call`) that controls jitter + Semaphore + retry
  behavior. No new abstraction needed.

## 3. Implementation approach (decided)

### PERF-001 — Add `api_call` discriminator to `NetworkClient.call`

Add `api_call: bool = True` parameter to `NetworkClient.call`. When `api_call=False`:
- Skip the initial jitter (`network.py:103`)
- Skip Semaphore acquisition
- Single attempt (no retry loop)
- Still use `_invoke` (to_thread + optional call_timeout)

Call sites updated to pass `api_call=False` for CPU/local work:
- `reports.py:349` — `build_task` (local JSON construction, no `send_request`)
- `reports.py:427` — `result2table` (local DataFrame materialization)
- `services.py:186` — `get_connection` (local I/O: credential file + client construction)

API call sites keep `api_call=True` (default): `get_status`, `get_result`,
`get_availability_period`, and the task-submission POST (`retryable=False, api_call=True`).

### PERF-002 — Result-size guard in `extract_data`

Add `max_result_rows: int = 10000` parameter to `TVMediaReport.__init__`. In
`extract_data`, check `len(res_json.get('resultBody', []))` against `max_result_rows`
before calling `result2table`. If exceeded, set `task.status=False`,
`task.log_error=True`, `task.error="Result size ... exceeds maximum ..."` — the
existing `worker` error-handling path writes the error artifact and continues.

Rationale for reject (not stream): the vendored `result2table` is all-or-nothing; it
has no slice/stream API. A guard on `resultBody` length before invocation is the
minimal, safe approach. The default 10000 matches the audit's probe scale and is
above any currently-passing test's data volume.

### PERF-003 — Per-task latency instrumentation

Add `time.perf_counter()` timing in `_process_item` (wrapping `send_task → wait_task →
extract_data`) and in each stage method. Replace the bare `logger.info("=")` at
reports.py:356 with a structured per-task log record containing task name, target,
interval, stage durations, and total. Use positional lazy-formatting for
compatibility with existing spy-based test assertions.

## 4. Execution blocks and order

**Dependency:** Block 1 must complete before Block 2 (Block 2 modifies the same
methods — `send_task`, `extract_data` — for timing instrumentation).

### Block 1 — PERF-001 + PERF-002 (extract_data + NetworkClient.call rework)

| Step | Agent | Description |
|---|---|---|
| 1.1 | Planner | Create detailed task description with semantic code units |
| 1.2 | Implementor | Implement `api_call` parameter in `NetworkClient.call`; update 3 CPU/local call sites; add `max_result_rows` + size guard in `extract_data`; add tests |
| 1.3 | Implementor | Run lint + tests (including slow) locally; commit |

**Acceptance criteria:**
- `NetworkClient.call` has `api_call: bool = True` parameter
- CPU/local calls (`build_task`, `result2table`, `get_connection`) pass `api_call=False`
- API calls (`get_status`, `get_result`, `get_availability_period`, task POST) keep
  `api_call=True`
- `TVMediaReport.__init__` accepts `max_result_rows: int = 10000`
- `extract_data` checks `resultBody` length before `result2table`; oversized results
  set `task.status=False`, `task.log_error=True`, clear error message
- New tests: `test_cpu_local_calls_bypass_semaphore_and_jitter`,
  `test_large_result_is_rejected`
- All 110 existing tests remain green (including 2 slow tests)

### Block 2 — PERF-003 (per-task latency instrumentation)

| Step | Agent | Description |
|---|---|---|
| 2.1 | Planner | Create detailed task description with semantic code units |
| 2.2 | Implementor | Add `perf_counter` timing in `_process_item` + each stage; replace
  `logger.info("=")` with structured per-task log; add tests |
| 2.3 | Implementor | Run lint + tests (including slow) locally; commit |

**Acceptance criteria:**
- `_process_item` records per-stage durations (send_task, wait_task, extract_data)
- Each stage method (`send_task`, `wait_task`, `extract_data`) records its own duration
- `logger.info("=")` replaced with structured per-task log (task name, target, interval,
  stage durations, total)
- New test: `test_worker_emits_per_task_duration`
- All 110 existing tests remain green (spy-based tests in test_reports_lifecycle.py
  check for "is ready" absence and "will be skipped" presence — new timing messages
  must not contain these substrings)

## 5. Risks

| Risk | Mitigation |
|---|---|
| Changing `call()` signature breaks lifecycle tests | `api_call` is a keyword arg with default `True`; existing call sites unaffected |
| Size guard threshold breaks `test_worker_log_error_false_no_json5` (empty DataFrame) | Guard checks `resultBody` length BEFORE calling `result2table`, not the DataFrame after |
| PERF-003 logging changes break spy-based tests | New log messages avoid "is ready" and "will be skipped" substrings; tests use substring checks |
| `get_connection` bypass in services.py breaks test_services.py mocks | Mock `side_effect` absorbs arbitrary kwargs; `api_call=False` is keyword-arg, invisible to side_effect |

## 6. Files to modify

**Block 1:**
- `src/mko_get_mediascope_data/core/network.py` — `NetworkClient.call` signature + branch
- `src/mko_get_mediascope_data/core/reports.py` — `send_task`, `extract_data`,
  `TVMediaReport.__init__`
- `src/mko_get_mediascope_data/core/services.py` — `get_connection` call site
- `tests/test_network.py` — add `test_cpu_local_calls_bypass_semaphore_and_jitter`
- `tests/test_reports_lifecycle.py` — add `test_large_result_is_rejected`

**Block 2:**
- `src/mko_get_mediascope_data/core/reports.py` — `worker`/`_process_item`,
  `send_task`, `wait_task`, `extract_data`
- `tests/test_reports_lifecycle.py` — add `test_worker_emits_per_task_duration`

## 7. Definition of done (per block)

1. Change is minimal and follows existing patterns (Strategy composition, Pydantic,
   module-level loggers, `asyncio.to_thread` for CPU work)
2. Tests cover the new behavior; `uv run pytest` green; `ruff check` clean on
   touched paths; no new mypy errors in touched files
3. Documentation updated by doc-specialist in a separate commit
4. No unrelated changes in the diff
