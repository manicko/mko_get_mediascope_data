---
phase: "04"
phase_name: "data-pipeline"
id_prefix: "PIPE"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/04-audit-data-pipeline.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Data Pipeline & Report Lifecycle

## Summary

Eleven findings (1 CRITICAL, 5 HIGH, 1 MEDIUM, 4 LOW). The highest severity is
CRITICAL: the CSV writer uses `mode="x"` and swallows `FileExistsError`, so a task
whose output already exists is reported as ready/done while no file is produced — the
run prints "Отчёт успешно завершён" with no output. Compounding this, the worker loop has
no exception handling: any raised error (network, 4xx, auth, retry-exhausted) kills the
worker before `task_done()`, so `queue.join()` hangs the entire batch indefinitely with
no error and no output. Separately, a valid `date_filter`-only configuration (the one the
project's own test fixture uses) crashes `get_period` with `AttributeError` before any API
call. The broadest consequence: a single transient API error or a pre-existing CSV can
make an entire report run either silently succeed with no output or hang forever.

## Scope

**Examined:** `core/reports.py` (`TVMediaReport.get_period`, `send_task`, `wait_task`,
`extract_data`, `worker`, `processing_tasks`, `DefaultTVTaskStrategy`,
`TVRegTaskStrategy`, `DefaultDataStrategy`), `core/tasks.py` (`TVTask.to_yaml`,
`log_error`), `core/utils.py` (`csv_to_file`, `get_last_period`, `slice_period`,
`iter_period_slices`), `core/models.py` (`PeriodModel`, `LastTimeModel`, `ReportSettings`),
`core/services.py` (`AppService.run_report`, `export_path` resolution), `app.py`
(`run` success path), `settings/reports/*` and `settings/defaults/*` templates,
`tests/conftest.py`, `tests/test_services.py`, `tests/test_reports.py`,
`tests/test_utils.py`.

**Tools:** `git ls-files`, Grep (content search), AST (source inspection of
`wait_task`/`worker`), `python -c`/`uv run python` runtime probes reproducing every
finding, `ruff check` (clean on `core/` and `app.py`).

**Not verified:** live Mediascope API calls (no credentials); full end-to-end run
(no `MediaVortexTask` connection). The shipped `tests/test_services.py` mocks
`TVMediaReport.create_report`, and `tests/test_reports.py` mocks the report object, so
none of the `get_period`/`worker`/`extract_data`/`csv_to_file`-success paths are
exercised by the suite — these findings are not covered by tests.

<!-- GATES — every cited path appears in git ls-files; symbols verified by runtime probe. -->

## Findings

| ID       | Severity   | Anchor                                                         | Title                                                                                  |
| -------- | ---------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| PIPE-001 | CRITICAL   | `src/mko_get_mediascope_data/core/utils.py::csv_to_file`       | `mode="x"` + swallowed `FileExistsError` reports a skipped CSV write as task success    |
| PIPE-002 | HIGH       | `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker` | No try/except in worker: a raised exception kills the worker before `task_done()`        |
| PIPE-003 | HIGH       | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period` | `get_period` crashes on `date_filter` + `last_time: null` (valid model combo)          |
| PIPE-004 | HIGH       | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.processing_tasks` | Zero-task report completes silently with no output and no error                       |
| PIPE-005 | HIGH       | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period` | Out-of-range `date_filter` silently clamped; entirely-out-of-range yields an inverted (start>end) period |
| PIPE-006 | HIGH       | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task` | `wait_task` polls `while True` with no deadline or iteration cap                        |
| PIPE-007 | MEDIUM     | `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker` | Raised exceptions bypass `TVTask.to_yaml`; only `status is False` is serialised         |
| PIPE-008 | LOW        | `src/mko_get_mediascope_data/core/reports.py::DefaultDataStrategy.prepare` | `KeyError` logged with column but not task name/interval                                |
| PIPE-009 | LOW        | `src/mko_get_mediascope_data/core/tasks.py::TVTask`            | `log_error` write-only; None-vs-empty distinction has no effect                         |
| PIPE-010 | LOW        | `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_yaml`    | Dead `cwd/data/output` path logic; `to_yaml(folder=None)` silently no-ops               |
| PIPE-011 | LOW        | `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate` | Already-exported task skipped silently (no warning) unlike `DefaultTVTaskStrategy`      |

---

#### PIPE-001 — [CRITICAL] `mode="x"` + swallowed `FileExistsError` reports a skipped CSV write as task success

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `mode="x"`
**Checked:** ran
**Why this band:** Directly matches the CRITICAL criterion — "a report task whose result is
silently skipped because the CSV file already exists (mode `"x"`), so the run reports
success while no output is produced."
**Problem:** `csv_to_file` (utils.py:188-200) opens the output with `mode="x"` (exclusive
create) and catches `FileExistsError` with a `logger.warning` + `return`, raising nothing.
`extract_data` (reports.py:319-332) calls it without inspecting a return value and never
sets `task.status = False` on a skip. Back in `MediaReport.worker` (reports.py:148-152), a
task whose status is still `True` takes the `else` branch and logs
`Задача {item.name} готова` ("ready"), so the run records success while the file was
never (re)written. The `check_done` guard is a pre-filter that matches on filename; it is
not a safety net for write collisions, `check_done=False` re-runs, or any naming mismatch.
**Repro:**
1. Write `report1.csv.gz` once via `csv_to_file(df, out, file_prefix="report1", add_time=False, …)`
   → 1 file (`./report1.csv.gz`).
2. Call `csv_to_file` again with the same `file_prefix` → no exception raised by the
   caller; the second write is swallowed and the file count stays at 1.
3. Trace `worker`: after `extract_data`, `item.status` is still `True`, so the `else`
   branch fires ("ready") and `task_done()` marks a skipped task as completed.

**Impact:** A re-run with `check_done=False`, two tasks colliding on a filename, or a
`check_done` match failure silently produces no CSV while the CLI prints
`✅ Отчёт успешно завершён`. Users believe data was written when it was not.
**Root cause:** `csv_to_file` treats "file already exists" as a non-error (log + swallow)
and the surrounding pipeline has no channel to mark the task as skipped/failed.
**Fix:** In `csv_to_file` (or `extract_data`), on `FileExistsError` set
`task.status = False` with a distinct error or return a sentinel the worker honours, or
open in `"w"`/`"wb"` when re-runs are intended. At minimum, the worker must not log
"ready" for a task whose write was skipped. Target symbol: `csv_to_file` and
`MediaReport.worker`.
**Verify:** existing `test_utils.py::test_csv_to_file` only asserts a single successful
write; new test needed: `test_csv_to_file_duplicate_write_does_not_report_success`
**Related:** PIPE-004 (silent completion), PIPE-007 (raised exceptions bypass `to_yaml`)

**Evidence — `csv_to_file` second write swallowed:**
```text
1st write -> files: ['report1.csv.gz']
2nd write raised: None
files after 2nd: 1 (swallowed FileExistsError -> no exception to caller)
```
*(supports: "no exception to caller on duplicate name")*

**Evidence — `csv_to_file` mode + except (utils.py):**
```text
src/mko_get_mediascope_data/core/utils.py:188:     data_frame.to_csv(..., mode="x", ...)
src/mko_get_mediascope_data/core/utils.py:199:     except FileExistsError:
src/mko_get_mediascope_data/core/utils.py:200:         logger.warning(f"File report {out_file} already exists. Skip it.")
```

#### PIPE-002 — [HIGH] No try/except in worker: a raised exception kills the worker before `task_done()`

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker`
**Disambiguator:** `self.task_queue.task_done()`
**Checked:** ran
**Why this band:** A single transient/4xx/auth failure (all of which `NetworkClient.call`
re-raises) terminates the worker without marking the item done, so `task_queue.join()`
never returns; the whole batch stalls with no output and no error. Matches HIGH
("consuming API quota and worker time"); it is not CRITICAL only because the run does not
report success (it hangs rather than falsely completing).
**Problem:** `worker` (reports.py:141-153) is a bare `while True` with no `try/except`.
`send_task` (143), `wait_task` (145), and `extract_data` (147) can all raise —
`NetworkClient.call` re-raises `BadRequestError`, `AuthorizationError`,
`FileNotFoundError`, and `RuntimeError("Network retry limit exceeded")`. Any such raise
exits `worker` before `self.task_queue.task_done()` (153). Because the queue item was
already `get()`-ed, `await self.task_queue.join()` (processing_tasks.py:161) waits forever.
The other workers are stranded too, and `app.py` never reaches the success print (it is
stuck inside `asyncio.run`).
**Repro:** Patch `send_task` to `raise RuntimeError("boom from API send")`; put one
`TVTask` on a `max_tasks=1` queue; `await asyncio.wait_for(queue.join(), timeout=2)` →
`TimeoutError` (join never returns).
**Impact:** One bad task — a 4xx from the API, an expired token, or a single network
timeout — hangs the entire report run indefinitely; no CSV, no error YAML, no message.
**Root cause:** `worker` performs no exception isolation; `task_done()` is unconditional
and only reachable on the happy path.
**Fix:** Wrap the per-item body in `try/except`: on exception, set `task.status = False`,
`task.error`, log with `task.name`, and call `to_yaml`; move `task_done()` into a `finally`.
Target symbol: `MediaReport.worker`.
**Verify:** no shipped test covers the worker loop (create_report is mocked in
`test_services.py`); new test needed: `test_worker_serializes_send_exception_and_does_not_hang`
**Related:** PIPE-007 (no error file for raised exceptions, same root cause)

**Evidence — `queue.join()` hangs after worker raises:**
```text
queue.join() HUNG >2s (task_done never called -> worker raised before line 153)
```
*(supports: "join never returns after a worker raises")*

**Evidence — `worker` body has no try/except (reports.py):**
```text
src\mko_get_mediascope_data\core\reports.py:141:     while True:
src\mko_get_mediascope_data\core\reports.py:142:         item = await self.task_queue.get()
src\mko_get_mediascope_data\core\reports.py:143:         await self.send_task(item)
src\mko_get_mediascope_data\core\reports.py:144:         if item.status is True:
src\mko_get_mediascope_data\core\reports.py:145:             await self.wait_task(item)
src\mko_get_mediascope_data\core\reports.py:146:         if item.status is True:
src\mko_get_mediascope_data\core\reports.py:147:             await self.extract_data(item)
src\mko_get_mediascope_data\core\reports.py:148:         if item.status is False:
src\mko_get_mediascope_data\core\reports.py:149:             logger.warning(f"Расчет: '{item.name}' будет пропущен.")
src\mko_get_mediascope_data\core\reports.py:150:             await asyncio.to_thread(item.to_yaml, self.export_path)
src\mko_get_mediascope_data\core\reports.py:153:         self.task_queue.task_done()
```
*(supports: "task_done() at 153 is only reachable on the happy path")*

#### PIPE-003 — [HIGH] `get_period` crashes on `date_filter` + `last_time: null`

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period`
**Disambiguator:** `frequency = self.report_settings.period.last_time.time_unit`
**Checked:** ran
**Why this band:** A configuration that `PeriodModel` explicitly permits (and the project's
own `tv_yaml_content` fixture uses — `last_time: null` with an explicit `date_filter`)
crashes before any API work, producing no data and no CSV. Not CRITICAL only because the
error surfaces (AttributeError) rather than being silent.
**Problem:** `get_period` (reports.py:241) unconditionally does
`frequency = self.report_settings.period.last_time.time_unit`, even when the period is
supplied via `date_filter` (the `last_time is None` branch). `frequency` is only needed
for `slice_period` when `multiple_files=True`, yet it is read on every code path. The
result: any `date_filter`-only report raises `AttributeError: 'NoneType' object has no
attribute 'time_unit'` inside the `except Exception` handler (reports.py:248-250) which
logs and re-raises, aborting `create_report`.
**Repro:** Build a `ReportSettings` with
`period: {"date_filter": ["2025-01-01","2025-01-07"], "last_time": None}` and call
`await rep.get_period()` → `AttributeError` at reports.py:241.
**Impact:** The simplest, most intuitive period configuration (`date_filter` only) fails
for every TV report; users get an obscure `AttributeError` instead of data.
**Root cause:** `frequency` is coupled to `last_time` even when `last_time` is legitimately
absent (its sole runtime use is the `multiple_files` slice).
**Fix:** Derive `frequency` only when `multiple_files` is True, and fall back to a sane
default (or require `last_time` only in that branch). Target symbol:
`TVMediaReport.get_period`.
**Verify:** no shipped test covers `get_period` (create_report is mocked in
`test_services.py`); `tests/conftest.py::tv_yaml_content` actually encodes the crashing
config; new test needed: `test_get_period_with_date_filter_only`
**Related:** PIPE-005 (clamp logic, same method)

**Evidence — repro output:**
```text
=== REPRO A: get_period crashes with last_time=None + explicit date_filter ===
  CRASH: AttributeError -> 'NoneType' object has no attribute 'time_unit'

  File ".../core/reports.py", line 241, in get_period
    frequency = self.report_settings.period.last_time.time_unit
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'time_unit'
```
*(supports: "valid date_filter-only config crashes get_period")*

#### PIPE-004 — [HIGH] Zero-task report completes silently with no output and no error

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.processing_tasks`
**Disambiguator:** `await self.task_queue.join()`
**Checked:** ran
**Why this band:** Matches HIGH — "a zero-task report that completes with no output and
no error." `processing_tasks` puts nothing on the queue, `queue.join()` returns at once,
and `run_report` returns the export path as if data was written.
**Problem:** `processing_tasks` (reports.py:155-162) creates workers, drains
`generate_tasks` into the queue, then `await self.task_queue.join()`. If the strategy
yields zero tasks — e.g. `target_audiences: {}` (DefaultTVTaskStrategy line 369 inner loop
never runs; TVRegTaskStrategy line 411 likewise), or a period that `iter_period_slices`
yields nothing (utils.py:40-41 guard `if overall_start > overall_end: return`), e.g. the
inverted period from PIPE-005 — `join()` completes immediately and `create_report`
returns. No log, no error, no CSV, and `app.py:52` still prints `✅ Отчёт успешно
завершён`.
**Repro:** Use `target_audiences: {}`;
`tasks = await asyncio.run(_consume(strategy.generate(report)))` → `0` tasks; the run
exits with success.
**Impact:** An empty/misconfigured audience or an out-of-range period silently produces a
successful-looking run with no data at all.
**Root cause:** No "did I produce at least one task?" assertion in `processing_tasks` or
`create_report`.
**Fix:** After `queue.join()`, assert at least one task was yielded/enqueued and raise a
`NoReportFoundError` (already defined in `core/errors.py`) if the task set was empty.
Target symbol: `TVMediaReport.processing_tasks` / `create_report`.
**Verify:** existing tests mock `create_report`; new test needed:
`test_zero_task_report_raises_no_report_found`
**Related:** PIPE-005 (inverted period feeds the zero-task path), PIPE-001 (silent success)

**Evidence — repro output:**
```text
=== REPRO F: zero-task report (empty target_audiences) ===
  zero-task report -> generated tasks count: 0 (no output, no error)
```
*(supports: "empty target_audiences yields zero tasks and the run exits as success")*

#### PIPE-005 — [HIGH] Out-of-range `date_filter` silently clamped; inverted period when entirely out of range

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period`
**Disambiguator:** `period = max(available_period[0], period[0]), min(`
**Checked:** ran
**Why this band:** Matches HIGH — "a period that is clamped … silently truncating data."
When the requested range does not overlap the API's available range, the `max`/`min` clamp
produces a period with start AFTER end (start > end), which then either slices to zero
tasks (PIPE-004) or is sent to the API as an invalid range. Not CRITICAL only because the
clamp is silent-truncation rather than a silent CSV skip.
**Problem:** `get_period` (reports.py:242-244) clamps with
`max(available_period[0], period[0]), min(available_period[1], period[1])` and logs
nothing. For a requested range entirely before the available range (e.g. 1900 vs API
[2024, 2025]), this yields `(2024-01-01, 1900-02-01)` — start after end. The clamp is only
correct when the two ranges overlap; otherwise it silently produces an invalid/intersecting
nothing range with no warning to the user.
**Repro:** Requests `date_filter: ['1900-01-01','1900-02-01']` against an API reporting
availability `[2024-01-01, 2025-12-31]`; `get_period()` returns
`[(2024-01-01, 1900-02-01)]` with no warning.
**Impact:** A user requesting historical (or future) data beyond the API window gets no
data and no notice; with `multiple_files=True` it silently becomes a zero-task run
(PIPE-004), with `multiple_files=False` it is sent to the API as an inverted range.
**Root cause:** Clamping intersects two ranges without checking overlap, and emits no
warning on truncation.
**Fix:** Intersect explicitly and, when `period[0] > period[1]` after clamping, raise
`NoReportFoundError` (or log a warning + skip); always log when the resolved period is
narrower than requested. Target symbol: `TVMediaReport.get_period`.
**Verify:** no shipped test covers clamping; new test needed:
`test_get_period_out_of_range_clamped_with_warning`
**Related:** PIPE-004 (zero-task follow-on), PIPE-003 (same method)

**Evidence — repro output:**
```text
=== REPRO H: get_period silent clamp (no warning logged) ===
  available_period from API: [datetime.date(2024, 1, 1), datetime.date(2025, 12, 31)]
  resolved period (clamped): [(datetime.date(2024, 1, 1), datetime.date(1900, 2, 1))] (requested 1900-01-01..1900-02-01 -> truncated to API range, no warning)
```
*(supports: "entirely-out-of-range request yields an inverted start-after-end period, silently")*

#### PIPE-006 — [HIGH] `wait_task` polls `while True` with no deadline or iteration cap

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task`
**Disambiguator:** `while True`
**Checked:** ran (AST inspection of `wait_task`)
**Why this band:** A task stuck in a non-terminal status (`RUNNING`/`WAITING`) is polled
forever with a fixed 2 s sleep, burning API quota and holding a worker for the lifetime of
the run — HIGH ("consuming API quota and worker time"). Not CRITICAL only because a
normally-terminating task is unaffected.
**Problem:** `wait_task` (reports.py:277-293) is an unconditional `while True` that breaks
only on `status is False` or a terminal `taskStatus` (`DONE`/`FAILED`/`CANCELLED`,
reports.py:292). There is no `max_iterations`, no overall `deadline`/`timeout`, and no
clock check. The `sleep_time=40` passed to `get_status` is a `NetworkClient` retry-jitter
hint, not a polling cap. A task that never reaches a terminal status pins one worker and
one queue slot permanently.
**Repro:** Inspect `TVMediaReport.wait_task` source: `"while True" in source` → `True`;
`any(k in source for k in ("max_attempts","deadline","timeout","max_iter","time.monotonic"))`
→ `False`.
**Impact:** A stuck remote task converts one healthy worker into a permanent leak; under
`saturation` (max_tasks workers) the whole pipeline can freeze.
**Root cause:** No termination condition beyond the remote status machine.
**Fix:** Add an overall deadline (e.g. 24 h) and an iteration cap, breaking into a
`status is False` (timeout) on exhaustion. Target symbol:
`TVMediaReport.wait_task`.
**Verify:** not covered by tests; new test needed: `test_wait_task_terminates_on_deadline`
**Related:** PIPE-002 (worker death compounds here: a `get_status` raise also hangs)

**Evidence — source inspection:**
```text
wait_task source contains 'while True': True
wait_task source contains 'max_attempts' or 'deadline' or 'timeout': False
```

#### PIPE-007 — [MEDIUM] Raised exceptions bypass `TVTask.to_yaml`; only `status is False` is serialised

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker`
**Disambiguator:** `await asyncio.to_thread(item.to_yaml, self.export_path)`
**Checked:** ran
**Why this band:** MEDIUM — the error-serialisation story is incomplete: only logical
failures (those that set `task.status = False`) produce a retryable error file. Failures
that raise (network, 4xx, auth, retry-exhausted) never reach `to_yaml`, so they leave no
recoverable artifact (and as PIPE-002 shows, they hang instead). It is the block-6
"every failure class?" question; MEDIUM because the logical-failure path is covered and
the missing coverage overlaps the HIGH worker fix.
**Problem:** In `worker` (reports.py:144-150), `to_yaml` is invoked only when
`item.status is False`. `send_task`/`wait_task`/`extract_data` call `NetworkClient.call`,
which re-raises `BadRequestError`, `AuthorizationError`, `FileNotFoundError`, and
`RuntimeError`. Those exceptions propagate out of `worker` (see PIPE-002) and never reach
the `to_yaml` branch, so there is no error YAML to inspect or retry. Only the
`status is False` branch (logical failures: no key, no result json, empty/None dataframe,
KeyError → None) serializes.
**Repro:** With `send_task` raising `RuntimeError`, observe no `.yaml` appears in the
errors folder (the worker hangs at `queue.join()` instead) — see PIPE-002 repro.
**Impact:** Operators cannot tell which task/network stage failed from the error folder;
transient and permanent API errors are invisible and conflated with a hang.
**Root cause:** No `except` in `worker` converts a raised exception into a
`status is False` task.
**Fix:** In `worker`, wrap the body in `try/except Exception` that sets `task.error`/`
task.status = False` and still reaches `to_yaml`, then `task_done()` in `finally`
(resolves PIPE-002 as well). Target symbol: `MediaReport.worker`.
**Verify:** not covered by tests; new test needed:
`test_worker_writes_error_yaml_on_raised_exception`
**Related:** PIPE-002 (identical root cause; this finding is the block-6 lens on the same defect)

#### PIPE-008 — [LOW] `KeyError` in strategy logged with the column but not the task name/interval

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DefaultDataStrategy.prepare`
**Disambiguator:** `except KeyError as err:`
**Checked:** ran
**Why this band:** LOW — observability only; the offending column IS present in the log
(`err` carries it) but the message is not attributed to the task whose `prepare_data`
failed, so under concurrent workers the line cannot be correlated to the failing task.
**Problem:** `DefaultDataStrategy.prepare` (reports.py:452-453) catches `KeyError` and logs
`f"Ошибка '{err}' при обработке данных."` — `err` contains the missing column name(s),
but `task.name`/`interval`/`target` are not passed to (or referenced by) `prepare`, so the
log line is indistinguishable across the `max_tasks` concurrent workers. The error YAML is
per-task (PIPE-007), but the log warning is not.
**Repro:** Call `DefaultDataStrategy.prepare(report_mock, df)` with a df missing the
columns named in `report.output_columns` → returns `None`; the single emitted log record
contains the column names but no task name.
**Impact:** Debugging a failed task requires cross-referencing the error YAML; the
warning log alone is ambiguous under concurrency.
**Root cause:** `prepare` has no reference to the `TVTask` being processed.
**Fix:** Pass the task (or its name) into `prepare`/`DataPreparationStrategy.prepare` and
include `task.name`/`task.interval` in the log message. Target symbol:
`DefaultDataPreparationStrategy.prepare` / `TVMediaReport.extract_data`.
**Verify:** `tests/test_reports.py::test_dict_data_strategy` exercises `DictDataStrategy`
only, not `DefaultDataStrategy` KeyError logging; new test needed:
`test_default_data_strategy_keyerror_logs_task_name`
**Related:** PIPE-009 (same failure path: `extract_data` sets `status=False` and falls
through to `to_yaml`)

**Evidence — repro output:**
```text
=== REPRO G: KeyError logged without task name/interval ===
  prepare returned: None
  log records: ["Ошибка '\"None of [Index(['missing_col', 'stat_x'], dtype='str')] are in the [columns]"' при обработке данных."]
  (column name present in err, but NO task name / interval in message)
```
*(supports: "column name present, task name absent")*

#### PIPE-009 — [LOW] `log_error` is write-only; the None-vs-empty distinction has no effect

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask`
**Disambiguator:** `self.log_error = False`
**Checked:** static (content search)
**Why this band:** LOW — dead field with no runtime consequence today, but it makes
`extract_data`'s None-vs-empty-vs-error logic misleading: both an empty dataframe and a
real error land in the identical `status=False` → `to_yaml` path.
**Problem:** `TVTask.log_error` is assigned in `tasks.py:22` and in six places in
`reports.py` (268, 285, 306, 314 set `True`; 318, 323 set `False`), but is never read.
Consequently the `None` dataframe branch (reports.py:313-314, `log_error=True`) and the
`df.empty` branch (reports.py:317-318, `log_error=False`) — and the `prepare_data is None`
branch (reports.py:321-323, `log_error=False`) — all behave identically downstream:
`status=False` → error YAML, no CSV. The "is this an error or just empty?" signal is dead.
**Repro:** `select_str_pattern="log_error"` over `core/` returns only assignment sites
and the `__init__` default — zero reads.
**Impact:** An empty but legitimate result and a genuine error are handled the same way;
the flag the code uses to distinguish them cannot be actioned.
**Root cause:** `log_error` was intended as a severity discriminator but is never
consumed by `worker`/`extract_data`/`csv_to_file`.
**Fix:** Either consume `log_error` (e.g. skip `to_yaml` when `log_error is False`,
writing an empty/SKIP marker instead) or remove the field and the dead branches. Target
symbol: `TVTask` / `MediaReport.worker`.
**Verify:** `tests/test_models.py` only checks field construction; new test needed:
`test_empty_dataframe_disposition`
**Related:** PIPE-008 (same `extract_data` failure path)

**Evidence — `log_error` occurrences (all writes, zero reads):**
```text
src/mko_get_mediascope_data/core/reports.py:268: task.log_error = True
src/mko_get_mediascope_data/core/reports.py:285: task.log_error = True
src/mko_get_mediascope_data/core/reports.py:306: task.log_error = True
src/mko_get_mediascope_data/core/reports.py:314: task.log_error = True
src/mko_get_mediascope_data/core/reports.py:318: task.log_error = False
src/mko_get_mediascope_data/core/reports.py:323: task.log_error = False
src/mko_get_mediascope_data/core/tasks.py:22:     self.log_error = False
```

#### PIPE-010 — [LOW] `TVTask.to_yaml` has dead `cwd/data/output` path logic; `to_yaml(folder=None)` is a silent no-op

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_yaml`
**Disambiguator:** `Path.joinpath(root_dir, "data", "output", folder, "errors")`
**Checked:** ran (runtime probe + AST read)
**Why this band:** LOW — the effective behaviour is correct *by accident* (the absolute
`export_path` passed by the worker overrides the `cwd` base in `Path.joinpath`), but the
code is misleading and would scatter error files into the process CWD (`cwd/data/output/
<folder>/errors`) if `folder` were ever relative; and `to_yaml(folder=None)` writes
nothing silently. This is the only block-6 finding near the CRITICAL "writes to CWD"
criterion, but that criterion does not actually trigger today.
**Problem:** `to_yaml` (tasks.py:29-34) computes
`folder = Path.joinpath(root_dir, "data", "output", folder, "errors")` with
`root_dir = Path().absolute()` (the CWD). Because the worker always passes the absolute
`self.export_path`, pathlib's absolute-override makes this resolve to
`export_path/errors/<name>.yaml` — not the CWD. So the CRITICAL "writes to working
directory" does **not** reproduce under the real call site; but (a) the `cwd/data/output`
literals are dead/misleading, (b) with a *relative* folder it DOES write into
`cwd/data/output/<folder>/errors`, and (c) `to_yaml(folder=None)` hits the early
`if folder is not None:` skip and silently produces no file and no log.
**Repro:** (absolute folder) file lands at `<export>/errors/period_target.yaml`, not
`errors/period_target.yaml` under CWD; (relative folder `"rel_out"`) file lands at
`cwd/data/output/rel_out/errors/period_target.yaml`; (`folder=None`) returns `None`, writes
nothing.
**Impact:** Latent — if `export_path` ever becomes relative (or `to_yaml` is called
directly), error files vanish into an arbitrary CWD location and are unrecoverable; a
`None` folder silently drops a failure with no indication.
**Root cause:** `to_yaml` ignores its `folder` argument as a base path and rebuilds a
CWD-relative path, relying on an absolute-override coincidence.
**Fix:** Write error files at `Path(folder) / "errors" / (name + ".yaml")` directly
(respecting the configured export folder), and make `folder=None` raise `TypeError` rather
than silently no-op. Target symbol: `TVTask.to_yaml`.
**Verify:** not covered by tests; new test needed:
`test_to_yaml_writes_into_export_folder_only`; `test_to_yaml_folder_none_raises`
**Related:** PIPE-007 (error-serialisation coverage)

**Evidence — repro output (absolute folder):**
```text
=== REPRO B: to_yaml target directory (absolute export_path) ===
  ACTUAL file written to: .../tmp.../output/reports/finance/raw_data/errors/period_target.yaml
  exists [cwd/errors/name.yaml]? False
```
**Evidence — repro output (relative folder):**
```text
=== REPRO B2: to_yaml with a RELATIVE folder (latent path) ===
  exists [cwd/data/output/rel_out/errors/name.yaml]? True
  exists [cwd/rel_out/errors/name.yaml]? False
```
**Evidence — repro output (folder=None):**
```text
to_yaml(None) return value: None
any noop_task.yaml written anywhere in cwd? []
```

#### PIPE-011 — [LOW] `TVRegTaskStrategy` skips already-exported tasks without logging, unlike `DefaultTVTaskStrategy`

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate`
**Disambiguator:** `if task.name in report.done_files:`
**Checked:** static (code read)
**Why this band:** LOW — observability inconsistency with no behavioural consequence (the
skip is correct under `check_done=True`); only `DefaultTVTaskStrategy` logs a warning.
**Problem:** `TVRegTaskStrategy.generate` (reports.py:430-431) skips an already-exported
task with a bare `if task.name in report.done_files: continue` and no log, whereas
`DefaultTVTaskStrategy.generate` (reports.py:378-380) emits
`logger.warning(f"Файл: '{task.name}' уже есть в папке, пропускаем.")` before `continue`.
For TV_REG reports the user receives no indication that tasks were dropped.
**Repro:** Static read — the TV_REG `done_files` hit path contains no `logger.warning`;
the DefaultTV path at reports.py:379 does.
**Impact:** TV_REG re-runs hide skipped tasks from the log, complicating verification of
which slices were actually re-downloaded.
**Root cause:** The `continue` branch in `TVRegTaskStrategy` omits the warning present in
the other strategy.
**Fix:** Add the same `logger.warning(...)` to the `TVRegTaskStrategy` skip branch (or
centralise the skip into the shared `Report` base). Target symbol:
`TVRegTaskStrategy.generate`.
**Verify:** `tests/test_reports.py` exercises only `DefaultTVTaskStrategy`; new test needed:
`test_tvreg_skip_logs_warning`
**Related:** PIPE-004 (both touch the task-generation/done-file contract)

---

## Fix Order

- **Dependencies:** PIPE-002 and PIPE-007 share the root cause (no exception isolation in
  `MediaReport.worker`); the worker `try/except` + `finally: task_done()` fix resolves both
  the hang and the missing error file together — do it first. PIPE-001 is independent but
  should be paired with the worker fix so a skipped write is also recorded. PIPE-003/005
  (period resolution) are independent of the worker; PIPE-005 must be fixed before PIPE-004
  can be validated for the out-of-range case.
- **Merge candidates:** PIPE-002 + PIPE-007 (one `try/except` in `worker`);
  PIPE-003 + PIPE-005 (both in `TVMediaReport.get_period`).
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** Phase 01 (ENT-005) already flags `print()` in `core/`
  including `reports.py:265` (send progress) and `services.py:117/124/154`; this phase does
  not re-assign them. Phase 02 (CFG-001) shows most bundled report YAMLs use `last_time`
  with `period_type`/`period_num` (not the model's `time_unit`/`unit_count`), so real
  configs reach `get_period` only after those templates are fixed; PIPE-003 is blocked by
  CFG-001 for the template-driven path but reproduces with any date_filter-only config.
- **Rollout risk:** Fixing PIPE-001 (mark skipped writes as failed) will make
  `check_done=False` re-runs re-send already-written tasks — that is the intended
  correction, but `test_utils.py::test_csv_to_file` (single write → 1 file) must stay
  green. Fixing PIPE-002 must not change `test_services.py::test_run_report_with_real_models`
  (it mocks `create_report` and `NetworkClient.call`, so the worker loop is not exercised
  there). Fixing PIPE-003 must preserve the success path for normally-terminating tasks.

---

## Appendices

### Appendix A — Full repro run output

```text
=== REPRO A: get_period crashes with last_time=None + explicit date_filter ===
  CRASH: AttributeError -> 'NoneType' object has no attribute 'time_unit'
  File ".../core/reports.py", line 241, in get_period
    frequency = self.report_settings.period.last_time.time_unit

=== REPRO B: to_yaml target directory (absolute export_path) ===
  ACTUAL file written to: .../tmp.../output/reports/finance/raw_data/errors/period_target.yaml
  exists [cwd/errors/name.yaml]? False

=== REPRO B2: to_yaml with a RELATIVE folder (latent path) ===
  exists [cwd/data/output/rel_out/errors/name.yaml]? True
  exists [cwd/rel_out/errors/name.yaml]? False

=== REPRO C: csv_to_file mode='x' swallowed -> caller sees success ===
  1st write -> files: ['report1.csv.gz']
  2nd write raised: None
  files after 2nd: 1 (swallowed FileExistsError -> no exception to caller)

=== REPRO D: worker hangs when send_task raises (task_done never called) ===
  queue.join() HUNG >2s (task_done never called -> worker raised before line 153)

=== REPRO E: wait_task has no upper bound (while True, no deadline) ===
  wait_task source contains 'while True': True
  wait_task source contains 'max_attempts' or 'deadline' or 'timeout': False

=== REPRO F: zero-task report (empty target_audiences) ===
  zero-task report -> generated tasks count: 0 (no output, no error)

=== REPRO H: get_period silent clamp (no warning logged) ===
  available_period from API: [datetime.date(2024, 1, 1), datetime.date(2025, 12, 31)]
  resolved period (clamped): [(datetime.date(2024, 1, 1), datetime.date(1900, 2, 1))]
```

*(supports: PIPE-003, PIPE-005, PIPE-006, PIPE-004, PIPE-010, PIPE-001, PIPE-002)*

### Appendix B — Repro script location

The runtime probes live in `.ai/audit/04-data-pipeline/_repro.py` (run via
`uv run python .ai/audit/04-data-pipeline/_repro.py` with `PYTHONIOENCODING=utf-8`); it
constructs real `ReportSettings`/`TVMediaReport`/`TVTask` objects with mocked
`NetworkClient`/`MediaVortexTask` and asserts the observed behaviour above.

### Appendix C — Phase 04 block coverage map

```text
Block | Topic                                | Finding(s)
1     | Period resolution / API clamping     | PIPE-003, PIPE-005
2     | Task generation (Strategy, check_done, zero-task) | PIPE-004, PIPE-011
3     | Worker send->wait->extract ordering  | PIPE-002, PIPE-006
4     | Result extraction / dataframe conv.  | PIPE-008, PIPE-009
5     | CSV export safety / file-existence   | PIPE-001
6     | Error serialisation / task-to-yaml   | PIPE-007, PIPE-010
```
