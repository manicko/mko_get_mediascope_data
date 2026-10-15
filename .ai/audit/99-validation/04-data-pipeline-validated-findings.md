---
phase: "04"
phase_name: "data-pipeline"
id_prefix: "PIPE"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/04-audit-data-pipeline.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Data Pipeline & Report Lifecycle

## Summary

Eleven findings (1 CRITICAL, 5 HIGH, 1 MEDIUM, 4 LOW). All eleven anchors resolve to
the cited location, all eleven disambiguator tokens exist inside their primary anchors,
and all eleven claims reproduce under runtime probe or static inspection.
**11 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 unsettled.**

No finding was downgraded, re-typed, or withdrawn. The sole imprecision —
PIPE-008's recommendation names `DefaultDataPreparationStrategy` (which does not exist;
the class is `DefaultDataStrategy`) — does not affect the finding's verdict.

One validation-level finding (VAL-001) is recorded: a recommendation target naming
error in PIPE-008.

## Scope

**Examined:** `core/utils.py::csv_to_file`, `core/reports.py` (`MediaReport.worker`,
`TVMediaReport.get_period`, `TVMediaReport.wait_task`, `TVMediaReport.processing_tasks`,
`TVMediaReport.extract_data`, `DefaultDataStrategy.prepare`, `DefaultTVTaskStrategy.generate`,
`TVRegTaskStrategy.generate`), `core/tasks.py::TVTask.to_yaml` and `TVTask` definition,
`core/models.py` (`PeriodModel`, `LastTimeModel`, `ReportSettings`), `core/services.py`
(`AppService.run_report`), `core/network.py` (`NetworkClient.call` exception contract),
`core/errors.py` (`NoReportFoundError`), `app.py` (`run` success path),
`tests/conftest.py` (`tv_yaml_content` fixture), `tests/test_utils.py`,
`tests/test_reports.py`, `tests/test_services.py`, `tests/test_models.py`,
`.ai/audit/04-data-pipeline/_repro.py`, all `settings/reports/**/*.yaml` templates,
`.kilo/commands/audit/phases/04-audit-data-pipeline.md`,
`.ai/audit/templates/audit-findings.md`,
`.ai/audit/01-entry-architecture/findings.md`,
`.ai/audit/02-config-secrets/findings.md`,
`.ai/audit/99-validation/01-entry-architecture-validated-findings.md`,
`.ai/audit/99-validation/02-config-secrets-validated-findings.md`.

**Runtime probes performed:**
- REPRO A: `get_period` with `date_filter` + `last_time=None` → `AttributeError` at reports.py:241
- REPRO B: `to_yaml` with absolute `export_path` → file lands in `<export>/errors/`, not CWD
- REPRO B2: `to_yaml` with relative folder → file lands in `cwd/data/output/<folder>/errors/`
- REPRO C: `csv_to_file` duplicate write with `mode="x"` → `FileExistsError` swallowed, no exception to caller
- REPRO D: `worker` with raising `send_task` → `queue.join()` hangs >2s (TimeoutError)
- REPRO E: `wait_task` source AST inspection → `while True` present, no deadline/timeout/iteration cap
- REPRO F: zero-task report (`target_audiences={}`) → 0 tasks generated, no error
- REPRO G: `DefaultDataStrategy.prepare` with missing columns → returns `None`, log contains column name but no task name
- REPRO H: `get_period` with out-of-range `date_filter` → inverted period `(2024-01-01, 1900-02-01)`, no warning
- grep: `log_error` across `core/` → 7 matches, all assignments, zero reads
- grep: `print(` across `core/` → 4 active calls at `code_ref d093d9c`; 0 active at HEAD (5 commented-out: reports.py:58, 102, 257, 290, 333 — commit 606c899)
- `ruff check` on `core/` and `app.py` → all checks passed
- `git rev-parse --short HEAD` → `8ed385f` (code_ref was `d093d9c`; drift from post-validation remediation, see Conformance)
- `git ls-files` on all 12 cited paths → all tracked
- `pytest` → 23 passed (`create_report` mocked in `test_services.py`, report object mocked in `test_reports.py`)

**Not verified:** Live Mediascope API calls (no credentials); full end-to-end report run
(`MediaVortexTask` not connected). All 11 findings reproduce via runtime probes with
mocked `NetworkClient`/`MediaVortexTask`, matching the findings' own methodology.

## Conformance

**Validated against:** commit `d093d9c` (Phase 04 baseline)
**HEAD at re-validation:** `8ed385f` (commit `e8fd1867` code-identical to `b04d177`)
**Survival at HEAD:** all 11 findings survive — anchors resolve, disambiguators exist, claims reproduce. Mechanism corrections: `print()` calls replaced with `logger.info()` (commit `606c899`); Russian log text translated to English (commit `b04d177`).
**Correction type:** runtime-probe + evidence-index updates — `pytest` 12→23, `git rev-parse` d093d9c→8ed385f, `print()` scan 4→0 active (5 commented-out), PIPE-001/008 Russian→English log text.

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). All 11 findings re-verified at HEAD (11/11 via
`.ai/tmp/doc_probe_reverify_audit.py`); three sections gained precision notes:
PIPE-005 (inverted period → `iter_period_slices` yields zero intervals → silent
exit-0 run), PIPE-007 (exception hangs the run before reaching the error path;
recovery-critical per D-09), PIPE-009 (`log_error` is the hook for the D-08/D-15
warning-vs-error split). Mapping: PIPE-001/OUT-003 → W-02 slice 2; PIPE-002 →
D-09/D-17, gap G-02, W-02 slice 1; PIPE-003/004/005 → D-08/D-15, gap G-01, W-02
slice 3; PIPE-006 → D-17, W-02 slice 4; PIPE-007/008/009 → D-09, W-02 slices 1-2;
PIPE-011 → D-10, gap G-03.

---

## Findings

| ID      | Disposition  | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|--------------------------|-----------------|
| PIPE-001 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes             |
| PIPE-002 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| PIPE-003 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| PIPE-004 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| PIPE-005 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| PIPE-006 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| PIPE-007 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM        | yes             |
| PIPE-008 | confirmed   | LOW        | LOW vs. LOW              | yes             |
| PIPE-009 | confirmed   | LOW        | LOW vs. LOW              | yes             |
| PIPE-010 | confirmed   | LOW        | LOW vs. LOW              | yes             |
| PIPE-011 | confirmed   | LOW        | LOW vs. LOW              | yes             |

### PIPE-001 — confirmed — [CRITICAL] `mode="x"` + swallowed `FileExistsError`

**Anchor resolved:** `core/utils.py::csv_to_file` — function at line 167; `mode="x"` at line 192;
`except FileExistsError` at line 199; `logger.warning(...)` at line 200.

**Claim tested:** `csv_to_file` opens output with `mode="x"` (pandas exclusive-create), catches
`FileExistsError` with `logger.warning` + bare `return`, and raises nothing to the caller.
`extract_data` (reports.py:325-332 at `code_ref d093d9c`; 295-326 at HEAD) calls `csv_to_file` via `asyncio.to_thread` without inspecting
the return value and does not set `task.status = False` on a skip. The worker's `else` branch
(reports.py:151-152) logs `Задача {item.name} готова` ("ready") for any task whose `status` remains
`True`. At HEAD the else-branch log reads `Task {item.name} is ready.` (English, commit `b04d177`).
**Evidence (runtime probe REPRO C):**
```text
1st write -> files: ['report1.csv.gz']
2nd write raised: None
files after 2nd: 1 (swallowed FileExistsError -> no exception to caller)
```

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/utils.py:192:     mode="x",
src/mko_get_mediascope_data/core/utils.py:199:     except FileExistsError:
src/mko_get_mediascope_data/core/utils.py:200:         logger.warning(f"File report {out_file} already exists. Skip it.")
```

**Rubric grading:** Phase 04 CRITICAL = "a report task whose result is silently skipped because
the CSV file already exists (mode 'x'), so the run reports success while no output is produced."
Matches exactly. No re-grade.

**Load-bearing artefact:** Code (`core/utils.py::csv_to_file` and `core/reports.py::MediaReport.worker`).
The `check_done` guard (reports.py:72-73, 378-380) is a pre-filter, not a write-collision safety net;
`check_done=False` re-runs bypass it entirely.

**Recommendation carry-out:** Fix targets `csv_to_file` and `MediaReport.worker` — both exist and
are stable. At minimum, the worker must not log "ready" for a task whose write was skipped. ✓

### PIPE-002 — confirmed — [HIGH] No try/except in worker

**Anchor resolved:** `core/reports.py::MediaReport.worker` — method at line 134. The disambiguator
`self.task_queue.task_done()` is at line 153. The full worker body (lines 141-153) has no
`try/except`.

**Claim tested:** `worker` is a bare `while True` with no exception isolation. `send_task` (143),
`wait_task` (145), and `extract_data` (147) can all raise via `NetworkClient.call`, which re-raises
`BadRequestError` (network.py:76-78), `AuthorizationError` (network.py:80-86),
`FileNotFoundError` (network.py:88-93), `HTTPError` (network.py:101-111), and
`RuntimeError("Network retry limit exceeded")` (network.py:126). Any raise exits `worker` before
`task_done()` (line 153), leaving the queue item un-acked, so `queue.join()` (line 161) hangs forever.

**Evidence (runtime probe REPRO D):**
```text
queue.join() HUNG >2s (task_done never called -> worker raised before line 153)
```

**Evidence (source — worker body, reports.py:141-153):**
```text
src/mko_get_mediascope_data/core/reports.py:141:     while True:
src/mko_get_medioscope_data/core/reports.py:142:         item = await self.task_queue.get()
src/mko_get_mediascope_data/core/reports.py:143:         await self.send_task(item)
src/mko_get_medioscope_data/core/reports.py:144:         if item.status is True:
src/mko_get_medioscope_data/core/reports.py:145:             await self.wait_task(item)
src/mko_get_medioscope_data/core/reports.py:146:         if item.status is True:
src/mko_get_medioscope_data/core/reports.py:147:             await self.extract_data(item)
src/mko_get_medioscope_data/core/reports.py:148:         if item.status is False:
src/mko_get_medioscope_data/core/reports.py:149:             logger.warning(f"Расчет: '{item.name}' будет пропущен.")
src/mko_get_mediascope_data/core/reports.py:150:             await asyncio.to_thread(item.to_yaml, self.export_path)
src/mko_get_mediascope_data/core/reports.py:153:         self.task_queue.task_done()
```

**Rubric grading:** Phase 04 HIGH = "a task that fails to send but is still polled... consuming API
quota and worker time." The worker death causes the entire batch to stall — "consuming worker time."
Not CRITICAL because the run does not report success (it hangs, not falsely completes). HIGH carried
= HIGH on rubric. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.worker`).
`NetworkClient.call` (network.py:59-126) is the re-raise source — verified against source.

**Recommendation carry-out:** "Wrap the per-item body in try/except; move `task_done()` into a
`finally`." Target `MediaReport.worker` exists and is stable. Also resolves PIPE-007. ✓

### PIPE-003 — confirmed — [HIGH] `get_period` crashes on `date_filter` + `last_time: null`

**Anchor resolved:** `core/reports.py::TVMediaReport.get_period` — method at line 223. The
disambiguator `frequency = self.report_settings.period.last_time.time_unit` is at line 241.

**Claim tested:** `get_period` unconditionally reads `self.report_settings.period.last_time.time_unit`
at line 241, even when `date_filter` is set and `last_time` is `None`. `frequency` is only consumed
at line 246 (`utils.slice_period(period, frequency)`) inside the `if self.report_settings.multiple_files:`
branch — but it is read on every code path at line 241. `PeriodModel` (models.py:68-80) permits
`date_filter` set + `last_time=None`; the `tv_yaml_content` conftest fixture (conftest.py:45-48)
encodes exactly this config.

**Evidence (runtime probe REPRO A):**
```text
CRASH: AttributeError -> 'NoneType' object has no attribute 'time_unit'
  File ".../core/reports.py", line 241, in get_period
    frequency = self.report_settings.period.last_time.time_unit
```

**Rubric grading:** Phase 04 HIGH is "a period that is clamped to the wrong API boundary, silently
truncating data." PIPE-003 is a crash (not silent truncation), but it is not CRITICAL because
the CRITICAL band is specifically "silently skipped because the CSV file already exists" /
"error serialisation that writes to the working directory" — neither applies. A crash that prevents
all data output for the most intuitive config is correctly HIGH. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.get_period` and `core/models.py::PeriodModel`).
The conftest fixture (`tests/conftest.py::tv_yaml_content`) encodes the crashing config — no
test exercises this path (create_report is mocked in test_services.py).

**Recommendation carry-out:** "Derive `frequency` only when `multiple_files` is True."
Target `TVMediaReport.get_period` exists and is stable. ✓

### PIPE-004 — confirmed — [HIGH] Zero-task report completes silently

**Anchor resolved:** `core/reports.py::TVMediaReport.processing_tasks` — method at line 155. The
disambiguator `await self.task_queue.join()` is at line 161.

**Claim tested:** `processing_tasks` (lines 155-162) creates workers, drains `generate_tasks` into
the queue, then `await self.task_queue.join()`. When the strategy yields zero tasks (e.g.
`target_audiences: {}` causes the inner loop in `DefaultTVTaskStrategy.generate` at line 369 to
never run), `join()` returns immediately (nothing to wait for), `create_report` returns, and
`app.py:52` prints `✅ Отчёт успешно завершён` — no output, no error.

**Evidence (runtime probe REPRO F):**
```text
zero-task report -> generated tasks count: 0 (no output, no error)
```

**Rubric grading:** Phase 04 HIGH = "a zero-task report that completes with no output and no
error." Matches exactly. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.processing_tasks` / `create_report`).
`NoReportFoundError` exists in `core/errors.py:1` and is never raised in this path — the findings
recommend using it. ✓

**Recommendation carry-out:** "After `queue.join()`, assert at least one task was yielded."
Targets exist and are stable. ✓

### PIPE-005 — confirmed — [HIGH] Out-of-range `date_filter` silently clamped; inverted period

**Anchor resolved:** `core/reports.py::TVMediaReport.get_period` — same method as PIPE-003. The
disambiguator `period = max(available_period[0], period[0]), min(` is at lines 242-244.

**Claim tested:** `get_period` clamps the requested `date_filter` to the API's available period using
`max(available_period[0], period[0])` and `min(available_period[1], period[1])`. When the requested
range does not overlap the available range (e.g. 1900 vs API [2024, 2025]), this produces
`(2024-01-01, 1900-02-01)` — start after end. No warning is logged (the only logging in `get_period`
is the `except Exception` handler at line 249).

**Evidence (runtime probe REPRO H):**
```text
available_period from API: [datetime.date(2024, 1, 1), datetime.date(2025, 12, 31)]
resolved period (clamped): [(datetime.date(2024, 1, 1), datetime.date(1900, 2, 1))] (requested 1900-01-01..1900-02-01 -> truncated to API range, no warning)
start > end? True
```

**Rubric grading:** Phase 04 HIGH = "a period that is clamped to the wrong API boundary, silently
truncating data." The clamp produces an inverted period (start > end) with no warning — directly
matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.get_period`). The inverted period
feeds PIPE-004's zero-task path — dependency is valid. ✓

**Conformance note 2026-10-15 (re-probed at HEAD):** the downstream path is now
pinned precisely — `slice_period` delegates to `iter_period_slices`
(`core/utils.py`), which returns immediately when `overall_start > overall_end`,
so the inverted period yields **zero intervals**; `processing_tasks` then runs an
empty queue and `run_report` returns normally with a success message and exit
code 0 (no warning). Mapping: owner decision D-08 (missing data must warn, never
interrupt), D-15 (no error file for missing data), gap G-01, work item W-02
slice 3. The recommendation to raise conflicts with D-08 as written; W-02 slice
3 specifies "warn and continue" instead.

**Recommendation carry-out:** "Intersect explicitly; raise `NoReportFoundError` when
`period[0] > period[1]` after clamping." `NoReportFoundError` exists (errors.py:1). Target
`TVMediaReport.get_period` is stable. ✓

### PIPE-006 — confirmed — [HIGH] `wait_task` polls `while True` with no deadline

**Anchor resolved:** `core/reports.py::TVMediaReport.wait_task` — method at line 271. The
disambiguator `while True` is at line 277.

**Claim tested:** `wait_task` (lines 277-293) is an unconditional `while True` that breaks only on
`status is False` (line 283) or a terminal `taskStatus` (`DONE`/`FAILED`/`CANCELLED`, line 292).
No `max_iterations`, no `deadline`, no `timeout`, no `time.monotonic()` check exists.
`sleep_time=40` passed to `get_status` is a `NetworkClient` retry-jitter hint, not a polling cap.

**Evidence (source inspection REPRO E):**
```text
wait_task source contains 'while True': True
wait_task source contains 'max_attempts' or 'deadline' or 'timeout': False
```

**Rubric grading:** Phase 04 HIGH = "consuming API quota and worker time." An unbounded poll loop
consumes API quota (each iteration calls `get_status` via `NetworkClient.call`) and pins a worker
permanently. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.wait_task`).

**Recommendation carry-out:** "Add an overall deadline and iteration cap." Target
`TVMediaReport.wait_task` exists and is stable. ✓

### PIPE-007 — confirmed — [MEDIUM] Raised exceptions bypass `TVTask.to_yaml`

**Anchor resolved:** `core/reports.py::MediaReport.worker` — same method as PIPE-002. The
disambiguator `await asyncio.to_thread(item.to_yaml, self.export_path)` is at line 150.

**Claim tested:** `to_yaml` is invoked only in the `if item.status is False:` branch (line 148-150).
Raised exceptions from `send_task`/`wait_task`/`extract_data` (which call `NetworkClient.call` and
re-raise) propagate out of `worker` before reaching the `to_yaml` call — they are never converted
to a `status = False` task, so no error YAML is produced. Only logical failures (setting
`status = False`) serialize.

**Evidence:** Confirmed via REPRO D — when `send_task` raises `RuntimeError`, no `.yaml` appears in
the errors folder (the worker hangs at `queue.join()` instead). Source inspection: `to_yaml` is
at worker line 150, inside `if item.status is False:` only.

**Rubric grading:** Phase 04 MEDIUM is "an empty dataframe treated as success rather than a
distinct failure; a KeyError in the strategy..." The phase 04 MEDIUM band does not enumerate
"incomplete error serialisation" explicitly, but "a band left empty is a valid outcome rather than
a gap" — MEDIUM is the closest fit for an observability gap with no runtime consequence for the
happy path. The findings correctly note MEDIUM because "the logical-failure path is covered and
the missing coverage overlaps the HIGH worker fix." No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.worker` and
`core/tasks.py::TVTask.to_yaml`).

**Recommendation carry-out:** "Wrap the body in try/except that sets `task.status = False` and
still reaches `to_yaml`." Target `MediaReport.worker` exists and is stable. Resolves PIPE-002 as
well. ✓

**Conformance note 2026-10-15:** owner decision D-09 makes the failed-task
description critical (a persistent failure must remain diagnosable across
re-runs), so this finding moves from observability to recovery-critical; it is
covered by work item W-02 slices 1-2 together with PIPE-002. Note also that in
the current code the raised exception never even reaches the error path — the
run hangs first (see the PIPE-002 / ASY-001 note).

### PIPE-008 — confirmed — [LOW] `KeyError` in strategy logged with column but not task name

**Anchor resolved:** `core/reports.py::DefaultDataStrategy.prepare` — method at line 436. The
disambiguator `except KeyError as err:` is at line 452.

**Claim tested:** `DefaultDataStrategy.prepare` catches `KeyError` and logs
`f"Error '{err}' while processing data."` at line 453 (English; `f"Ошибка '{err}' при обработке данных."` at `code_ref d093d9c`) — `err` carries the column name(s), but
`prepare`'s signature `(self, report, df)` has no reference to the `TVTask` being processed, so
the log message cannot include `task.name`/`task.interval`.

**Evidence (runtime probe REPRO G):**
```text
prepare returned: None
log record: Error '"None of [Index(['missing_col', 'stat_x'], ...)] are in the [columns]"' while processing data.
(column name present in err, but NO task name / interval in message)
```

**Rubric grading:** Phase 04 LOW = "observability drift with no runtime consequence today: an
error file missing a context field." The `KeyError` IS logged (with column name) but lacks task
attribution. LOW matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::DefaultDataStrategy.prepare`).
`TVMediaReport.extract_data` (line 295) calls `self.prepare_data(df)` (line 320) which delegates
to `self.data_strategy.prepare(self, df)` (line 340) — no task reference is passed.

**Recommendation carry-out:** "Pass the task into `prepare`." The target symbol named is
`DefaultDataPreparationStrategy.prepare` — **does not exist** (see VAL-001). The correct class
is `DefaultDataStrategy` (reports.py:435). `TVMediaReport.extract_data` (reports.py:295) exists
and is stable. ✓ (minus the naming error)

### PIPE-009 — confirmed — [LOW] `log_error` is write-only; None-vs-empty distinction has no effect

**Anchor resolved:** `core/tasks.py::TVTask` — class at line 14. The disambiguator
`self.log_error = False` is at line 22.

**Claim tested:** `log_error` is assigned at 7 sites — `tasks.py:22` (init, `False`) and six places
in `reports.py` (268, 285, 306, 314 set `True`; 318, 323 set `False`). A grep across all of `core/`
returns exactly these 7 assignment sites and **zero reads**. Consequently, the `df is None` branch
(reports.py:313-314, `log_error=True`), the `df.empty` branch (reports.py:317-318, `log_error=False`),
and the `prepare_data is None` branch (reports.py:321-323, `log_error=False`) all behave identically
downstream: `status=False` → `to_yaml`, no CSV.

**Conformance note 2026-10-15:** this flag is exactly the hook the owner's
decisions need — D-15 requires "no data" to become a **warning** (no error
file) while a processing failure must still produce the error file (D-09), so
`log_error` (or an explicit reason field) becomes observable in work item W-02
slice 2. Mapping: D-08 / D-15, gap G-01, W-02 slice 2.

**Evidence (grep across core/):**
```text
src/mko_get_mediascope_data/core/reports.py:268:             task.log_error = True
src/mko_get_mediascope_data/core/reports.py:285:                 task.log_error = True
src/mko_get_mediascope_data/core/reports.py:306:             task.log_error = True
src/mko_get_mediascope_data/core/reports.py:314:                 task.log_error = True
src/mko_get_mediascope_data/core/reports.py:318:                 task.log_error = False
src/mko_get_mediascope_data/core/reports.py:323:                 task.log_error = False
src/mko_get_mediascope_data/core/tasks.py:22:     self.log_error = False
```

**Rubric grading:** Phase 04 LOW = "observability drift with no runtime consequence today: an error
file missing a context field." A write-only field with zero reads is dead code — no runtime
consequence today. LOW matches. No re-grade.

**Load-bearing artefact:** Code (`core/tasks.py::TVTask` and `core/reports.py::TVMediaReport.extract_data`).

**Recommendation carry-out:** "Either consume `log_error` or remove the field." Targets exist and
are stable. ✓

### PIPE-010 — confirmed — [LOW] `TVTask.to_yaml` dead `cwd/data/output` path logic

**Anchor resolved:** `core/tasks.py::TVTask.to_yaml` — method at line 29. The disambiguator
`Path.joinpath(root_dir, "data", "output", folder, "errors")` is at line 33.

**Claim tested:** `to_yaml` computes `folder = Path.joinpath(root_dir, "data", "output", folder,
"errors")` with `root_dir = Path().absolute()` (CWD). Under the worker's real call site
(`item.to_yaml(self.export_path)`, line 150), `folder` is an absolute `Path`, so pathlib's
absolute-component override makes the result `export_path/errors/<name>.yaml` — correct **by
accident**. With a relative `folder`, the dead `cwd/data/output` literals activate and write to
`CWD/data/output/<folder>/errors/<name>.yaml`. With `folder=None`, the `if folder is not None:`
guard (line 30) skips the entire body — silent no-op, returns `None`.

**Evidence (runtime probes B, B2):**
```text
=== REPRO B (absolute folder) ===
  ACTUAL file written to: .../tmp.../output/reports/finance/raw_data/errors/period_target.yaml
  exists [cwd/errors/period_target.yaml]? False

=== REPRO B2 (relative folder) ===
  exists [cwd/data/output/rel_out/errors/period_target.yaml]? True

=== to_yaml(None) ===
  return value: None
  no file written anywhere in cwd
```

**Rubric grading:** Phase 04 CRITICAL covers "error serialisation that writes to the working
directory instead of the export folder." The findings correctly note this does **not** reproduce
under the real call site (absolute path overrides CWD) — the dead `cwd/data/output` literals are
dormant. This is LOW ("a comment about the export path that does not match the configured value").
No re-grade.

**Load-bearing artefact:** Code (`core/tasks.py::TVTask.to_yaml` and `core/reports.py::MediaReport.worker`
at line 150, which passes `self.export_path`).

**Recommendation carry-out:** "Write error files at `Path(folder) / 'errors' / (name + '.yaml')`
directly; make `folder=None` raise `TypeError`." Target `TVTask.to_yaml` exists and is stable. ✓

### PIPE-011 — confirmed — [LOW] `TVRegTaskStrategy` skips already-exported tasks without logging

**Anchor resolved:** `core/reports.py::TVRegTaskStrategy.generate` — method at line 385. The
disambiguator `if task.name in report.done_files:` is at line 430.

**Claim tested:** `TVRegTaskStrategy.generate` (lines 430-431) skips an already-exported task with
a bare `if task.name in report.done_files: continue` — no log.
`DefaultTVTaskStrategy.generate` (lines 378-380) emits
`logger.warning(f"Файл: '{task.name}' уже есть в папке, пропускаем.")` before `continue`.

**Evidence (source inspection):**
```text
TVRegTaskStrategy.generate: contains done_files+continue? True
TVRegTaskStrategy.generate: contains logger.warning?  False
DefaultTVTaskStrategy.generate: contains logger.warning? True

reports.py:430: if task.name in report.done_files:
reports.py:431:     continue
reports.py:378: if task.name in report.done_files:
reports.py:379:     logger.warning(f"Файл: '{task.name}' уже есть в папке, пропускаем.")
```

**Rubric grading:** Phase 04 LOW = "observability drift with no runtime consequence today." The skip
is correct behavior under `check_done=True`; the only issue is missing observability for TV_REG
re-runs. LOW matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVRegTaskStrategy.generate`).

**Recommendation carry-out:** "Add the same `logger.warning(...)` to the `TVRegTaskStrategy` skip
branch." Target `TVRegTaskStrategy.generate` exists and is stable. ✓

---

## Validation-Level Findings (VAL-)

| ID      | Severity | Disposition | Description |
|---------|----------|-------------|-------------|
| VAL-001 | LOW      | confirmed   | PIPE-008 recommendation names `DefaultDataPreparationStrategy.prepare` — class does not exist; actual class is `DefaultDataStrategy` |

### VAL-001 — [LOW] PIPE-008 recommendation target naming error

**Template mandate:** `.ai/audit/templates/audit-findings.md:70` — "a Fix that names no target
symbol — 'be more careful' is not a fix." Recommendations must name a real, resolvable target.

**Observed:** PIPE-008's **Anchor** and **Disambiguator** correctly reference
`core/reports.py::DefaultDataStrategy.prepare` (line 436, disambiguator `except KeyError as err:`
at line 452). However, the **Fix** section names `DefaultDataPreparationStrategy.prepare` as the
target symbol. The class `DefaultDataPreparationStrategy` does **not exist** in the codebase:

```text
class DataPreparationStrategy(ABC)    # base class (reports.py:352)
class DefaultDataStrategy(...)       # concrete (reports.py:435) — CORRECT name
class DictDataStrategy(...)          # concrete (reports.py:457)
```

**Impact:** A developer following the recommendation will not find `DefaultDataPreparationStrategy`
and must infer the intended target. The finding itself (the `KeyError` logged without task name) is
fully valid and confirmed; only the recommendation's target name is wrong.

**Cause:** Likely a typo or confusion with the base-class name `DataPreparationStrategy`.

**Disposition:** confirmed. Does not affect finding accuracy or verdict.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 01 finding | Phase 02 finding | Phase 04 finding | Shared root cause | Disposition |
|---|---|---|---|---|---|
| print() in core/ | ENT-005 (MEDIUM) | CFG-010 (LOW) | — (acknowledged, not re-claimed) | `print()` in `core/services.py` (117,124,154) and `core/reports.py` (265) at `code_ref d093d9c`; 0 active at HEAD (logger.info, commit 606c899) | cross-reference; all confirmed at code_ref; print() removed at HEAD |
| yaml_to_dict None return | ENT-003 (CRITICAL) | CFG-005 (MEDIUM) | — | `core/utils.py::yaml_to_dict` catches errors, returns `None` | cross-reference; both confirmed |
| last_time field-name drift | — | CFG-001 (HIGH) | PIPE-003 notes it | Templates use `period_type`/`period_num`; model requires `time_unit`/`unit_count` | cross-reference; CFG-001 blocks template-driven path, PIPE-003 reproduces with `date_filter`-only config |

**Phase 1 ownership check:** Phase 04 examines `get_period`, `worker`, `extract_data`,
`csv_to_file`, `wait_task`, `processing_tasks`, strategies, `to_yaml`, `TVTask` — all data-pipeline
behaviours. The `print()` calls at `reports.py:265` and `services.py:117,124,154` (at `code_ref d093d9c`)
were replaced with `logger.info()` at HEAD (commit `606c899`) and are correctly
**not** re-claimed as PIPE findings (they are owned by ENT-005 / CFG-010). `app.py:52`'s
`console.print` is Rich-formatted CLI output — allowed by AGENTS.md. No phase claims a concern
that another phase owns exclusively and reaches an opposite conclusion.

**Severity divergence rationale:** `print()` in `core/` is ENT-005 (MEDIUM, entry-architecture
layer-discipline), CFG-010 (LOW, config-secrets context), and absent from Phase 04 (not a
data-pipeline concern). Each phase's grading is correct under its own rubric. No conflict.

No cross-phase conflicts detected. No finding was withdrawn, downgraded, or re-graded due to
cross-phase interaction.

## Rollout Safety Assessment

**Dependencies identified in source (validated):**
- PIPE-002 + PIPE-007 share root cause (no exception isolation in `MediaReport.worker`); the worker
  `try/except` + `finally: task_done()` fix resolves both the hang and the missing error file. ✓
- PIPE-003 + PIPE-005 both in `TVMediaReport.get_period`; PIPE-005's fix (raise on inverted period)
  must land before PIPE-004 can be validated for the out-of-range case (inverted period → zero
  tasks). ✓
- PIPE-001 should be paired with the PIPE-002 worker fix so a skipped write is also recorded. ✓

**Rollout risk: test impact**
- `tests/test_utils.py::test_csv_to_file` writes once and asserts `len(files) == 1` — must stay
  green after PIPE-001 fix. Confirmed: it performs a single successful write, never a duplicate. ✓
- `tests/test_services.py::test_run_report_with_real_models` mocks `create_report` (line 47-50) and
  `NetworkClient.call` (line 41-44) — the worker loop is never exercised. PIPE-002/007 fixes
  (adding try/except to worker) will not affect this test. ✓
- `tests/test_reports.py` tests `DefaultTVTaskStrategy.generate` and `DictDataStrategy.prepare` only
  — does not exercise `TVRegTaskStrategy.generate`, `wait_task`, `get_period`, or `to_yaml`. No
  existing test blocks the PIPE fixes. ✓
- `tests/test_models.py` only tests Pydantic model construction — does not test `log_error`
  behavior. ✓

**Rollout risk: behavioural changes**
- Fixing PIPE-001 (raise or mark failed on `FileExistsError` instead of swallowing) will make
  `check_done=False` re-runs re-send already-written tasks — the findings identify this as the
  intended correction.
- Fixing PIPE-003 (derive `frequency` only when `multiple_files=True`) must preserve the success
  path for `last_time`-based configs. Confirmed: `get_last_period` (utils.py:88) uses
  `settings.time_unit` and is called only in the `date_filter is None` branch (line 235).
  Moving the `frequency` read to the `multiple_files` branch preserves both paths.
- Fixing PIPE-010 (`folder=None` raises `TypeError`) will not break the production path —
  `to_yaml` is only called from `worker` line 150 with `self.export_path` (never `None`).

All 11 recommendations name concrete, stable targets. All fix targets were verified to exist in
the `d093d9c` tree. No fix has been applied — all 11 findings remain in the current tree unmodified.

## Evidence Index

| Probe | Result | Supports |
|---|---|---|
| `csv_to_file` duplicate write (`mode="x"`) | 2nd write returns None, 1 file remains | PIPE-001 |
| `worker` with raising `send_task` | `queue.join()` hangs >2s (TimeoutError) | PIPE-002, PIPE-007 |
| `get_period` with `last_time=None` + `date_filter` | `AttributeError: 'NoneType' has no attribute 'time_unit'` at reports.py:241 | PIPE-003 |
| Zero-task report (`target_audiences={}`) | 0 tasks generated, no error | PIPE-004 |
| `get_period` with out-of-range `date_filter` | `start > end` (`(2024-01-01, 1900-02-01)`), no warning | PIPE-005, PIPE-004 (cross-ref) |
| `wait_task` source AST inspection | `while True` present; no deadline/timeout/max_attempts | PIPE-006 |
| `DefaultDataStrategy.prepare` with missing columns | returns `None`; log has column name, no task name | PIPE-008 |
| grep `log_error` across `core/` | 7 assignments, 0 reads | PIPE-009 |
| `to_yaml` absolute/relative/None folder | absolute → export folder; relative → CWD/data/output; None → silent no-op | PIPE-010 |
| `TVRegTaskStrategy.generate` source read | `done_files` + `continue`, no `logger.warning` | PIPE-011 |
| `print()` AST scan across `core/` | 4 active calls at `code_ref d093d9c`; 0 active at HEAD (5 commented-out — commit 606c899) | cross-ref ENT-005, CFG-010 |
| `ruff check src/.../core/ src/.../app.py` | all checks passed | methodology (Phase 04 Tools claim) |
| `git rev-parse --short HEAD` | `8ed385f` (code_ref was `d093d9c`; drift from remediation) | drift noted |
| `git ls-files` on 12 cited paths | all tracked | anchor validity (all 11 findings) |
| `pytest` full suite | 23 passed | test coverage gap confirmed |
