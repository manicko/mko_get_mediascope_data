---
spec_id: "06-verbosity-and-progress"
problem_source: ".ai/plans/problems/05-Problem.md"
related_decisions: "docs/00-overview/open-questions.md (D-02, D-08, D-09, D-15, D-17, DISC-02)"
related_behavior: "docs/00-overview/current-behavior.md, docs/00-overview/async-concurrency-architecture.md"
author: "analyst (tech-lead)"
status: draft
language: en
---

# Analytical Specification — Console Verbosity & Progress Bars

## 1. Problem Statement

Two coupled console-experience problems in `mko_get_mediascope_data` ([`05-Problem.md`](05-Problem.md)):

1. **Verbosity flood.** The report pipeline emits a large volume of INFO-level
   log records to the console (every stage transition, every queue depth debug
   sample, every timing line). There is no way to control output verbosity from the CLI.
   The shipped default (`log_config.json5` → root level `"INFO"`) prints
   everything to `stdout`. Users want a three-level verbosity selector: QUIET (errors + warnings only),
   DEFAULT (current INFO level), VERBOSE (DEBUG level for full diagnostics), configurable as a default in `app_config.json5`
   and overridable per-run from the CLI.
2. **Progress is unreadable.** Messages stream line-by-line and scroll past, so
   an operator cannot tell how far a long run has progressed. The owner wants a
   **set of fixed-position progress bars, one per task status, per report**, that
   are rewritten in place (`[====      ] 45% (90/200)`), and the **startup output
   (API connection, processing start/finish)** to remain visible above them.

### Owner clarification (from review)

The owner reviewed this spec and provided the following decisions:

1. **Verbosity model**: Use a `VerbosityMode` enum (`QUIET`/`DEFAULT`/`VERBOSE`), not stdlib log levels. `QUIET` = errors + warnings only (`WARNING` level); `DEFAULT` = current `INFO`; `VERBOSE` = `DEBUG`. Default in config = `DEFAULT`. CLI flag = `--verbosity`. No backward compatibility needed.

2. **Progress bar scope**: One set of 4 bars **per report**, not per run. A run may contain multiple reports (e.g., `nat_tv_brands_theiss.json5` has 2 reports); each report gets its own bar set.

3. **Task status model**: Simplify to exactly four states — `QUEUED`, `SENT`, `COMPLETED`, `FAILED`. No pipeline-stage bars (no Waiting/Received/Ready split). `QUEUED` is optional if difficult to implement.

4. **Bar formula**: `status_progress = tasks_with_this_status / total_tasks * 100`. Display as `STATUS [██░░░░░] 20% (40/200)`.

5. **NullProgressReporter**: Create `core/progress.py` with protocol + null impl. Keep `core/` Rich-free.

6. **Library API**: Add optional `progress: ProgressReporter | None = None` to `run_report_async()` and `run_report()`. `None` → `NullProgressReporter`. CLI injects Rich implementation. No Rich dependency in library mode.

7. **Config**: Pydantic defaults. `VerbosityMode` default `DEFAULT`. `LoggingLevel` default `INFO`. Never overwrite existing user configs during `init`.

8. **Verbose mode policy**: When verbose (VERBOSE), suppress per-task INFO stage logs that are already represented by the progress bars. Keep the bars as the single source of truth for task lifecycle progress.

## 2. Confirmed Requirements

| ID | Requirement | Source / evidence |
|---|---|---|
| REQ-01 | Three verbosity levels: `QUIET` (WARNING), `DEFAULT` (INFO), `VERBOSE` (DEBUG). Default = `DEFAULT` in config. | Owner clarification |
| REQ-02 | Verbosity is selectable **per-run from the CLI** via `--verbosity` flag, overriding the config default for that run. | `05-Problem.md:4-5` ("для отдельного отчета командой через cli запустить полный вывод") |
| REQ-03 | The default verbosity (and the override) must be **configurable** — not only in code. | `05-Problem.md:3-5` (config-file default + CLI tag) |
| REQ-04 | Progress is rendered as **fixed-position bars**, one per task state, rewritten in place — not streaming log lines. | `05-Problem.md:7-9` ("остаются на одном месте — строка перезаписывается") |
| REQ-05 | Bars reflect **task statuses**: `QUEUED`, `SENT`, `COMPLETED`, `FAILED`. | Owner clarification; replaces the 5-state model in the original sketch |
| REQ-06 | **Startup output** (API connection, period resolution, processing start/finish) must remain visible **above** the progress bars. | `05-Problem.md:13` ("При этом, нужно сохранить вывод, который мы видим в начале — соединение API и т.п.") |
| REQ-07 | The product stays a **CLI + library**; library usage (`run_report`/`run_report_async`, used from Jupyter/Airflow) must not be broken by the CLI/console changes. | D-03, D-19; `app.py:80-98`, `__init__.py:13-37` |
| REQ-08 | Business logic in `core/` must **not import Rich**; Rich/console and progress bars belong to the CLI layer (`app.py`). | `docs/00-overview/doc-maintenance-rules.md:107-108` ("Progress bars and user interaction belong to the CLI layer, not business logic"); `AGENTS.md` rule 3 (separation of concerns) |
| REQ-09 | One **set of 4 bars per report**. A run may contain multiple reports; each report renders its own bar set independently. | `settings/reports/nat_tv_brands_theiss.json5` (2 reports per file) |
| REQ-10 | Each bar shows `STATUS [██░░░░░] XX% (N/total)`, updated whenever task statuses change. | Owner's `[=====]` sketch + formula clarification |

## 3. Conceptual Development Tasks

Each task has a single purpose and a testable outcome.

### T1 — Config-driven verbosity (`AppSettings.verbosity_mode`)
- **Purpose:** make the verbosity configurable in `app_config.json5` instead of hardcoding the `INFO` root level in `log_config.json5`.
- **Outcome:** a `VerbosityMode` `StrEnum` (`models.py`, values `QUIET`/`DEFAULT`/`VERBOSE`), a `verbosity_mode: VerbosityMode = VerbosityMode.DEFAULT` field on `AppSettings` or `LoggingSettings` (`models.py`), and `"verbosity_mode": "DEFAULT"` in `settings/app_config.json5`. Maps to root logging levels: QUIET→WARNING, DEFAULT→INFO, VERBOSE→DEBUG.
- **Dependencies:** none (models only).

### T2 — CLI verbosity override (`--verbosity`)
- **Purpose:** allow a per-run override of the config default via the CLI.
- **Outcome:** `app.py run` gains `--verbosity [quiet|default|verbose]` option. When provided, applies `logging.getLogger().setLevel(...)` to the root logger before `asyncio.run(...)`. When not provided, uses the config `verbosity_mode` default (`DEFAULT`). Replaces the current `--verbose = typer.Option(True)` flag.
- **Dependencies:** T1 (config default).

### T3 — Task status model (`TaskStatus` enum + `TVTask.status` property)
- **Purpose:** give the progress renderer a discrete status per task so bars can be incremented correctly. Currently `TVTask` (`tasks.py:12-25`) has `status` (bool), `log_error` (bool), `error` (str) — no status enum.
- **Outcome:** a `TaskStatus` `StrEnum` (`models.py`, values `QUEUED`, `SENT`, `COMPLETED`, `FAILED`) and a `TVTask.status_enum` property derived from the existing `status` (bool) and `error` fields. No mutation needed — status is derived from existing state.
- **Dependencies:** none (models only).

### T4 — `ProgressReporter` protocol (`core/progress.py`)
- **Purpose:** decouple the worker's status transitions from the console renderer. The worker calls callbacks; the CLI owns Rich `Progress`/`Live`.
- **Outcome:** `core/progress.py` containing a `ProgressReporter` `typing.Protocol` with methods like `report_started(report_id, total_tasks)`, `task_status_changed(task, old_status, new_status)`, `report_completed(report_id)`, plus a `NullProgressReporter` (no-op, used by default and in tests). `MediaReport` holds an optional `reporter: ProgressReporter | None`.
- **Dependencies:** T3 (status enum).

### T5 — Wire the reporter into the worker loop
- **Purpose:** emit progress events at the exact code points that define status transitions.
- **Outcome:** `reports.py` calls `self.reporter.task_status_changed(...)` at: QUEUED when task is enqueued (`processing_tasks`, `reports.py:214-215`); SENT after `sender` returns a key (`send_task`, `reports.py:382`); COMPLETED after successful extraction (`extract_data`/`csv_to_file`, `reports.py:487-517`); FAILED in worker exception/skip branches (`reports.py:180-183,196-201`).
- **Dependencies:** T3, T4.
- **Constraint:** callbacks must be asyncio-safe (workers run on the event loop thread).

### T6 — Rich progress view in `app.py` (`run` command)
- **Purpose:** render exactly 4 fixed-position bars per report (QUEUED, SENT, COMPLETED, FAILED) and keep startup output above them.
- **Outcome:** `app.py run` builds a `rich.progress.Progress` with `redirect_stdout=True`/`redirect_stderr=True` so logging lines scroll above the fixed bar region. One `Live` region wraps each report's `run_report()` call. When reporter is `NullProgressReporter` (library path / tests) no `Live` is started.
- **Dependencies:** T2, T4, T5.

### T7 — Tests
- **Purpose:** prove the new surface without re-introducing a 70+ test blast radius.
- **Outcome:** unit tests for `VerbosityMode` config validation; a Typer runner test that `--verbosity verbose` raises root level to DEBUG, `--verbosity quiet` to WARNING, and unset defers to config; a `NullProgressReporter` test proving the hermetic pipeline is unchanged; a test that `task_status_changed` is called at send→extract boundaries (mocked reporter). Existing tests stay green.
- **Dependencies:** T1–T6.

### T8 — Documentation
- **Purpose:** keep docs current.
- **Outcome:** update `docs/00-overview/current-behavior.md` (CLI command table for `run`, verbosity model, progress bars); `docs/00-overview/settings-layers-architecture.md` (new `verbosity_mode` setting); `docs/00-overview/doc-maintenance-rules.md` (progress-bars rule, if extended); `README.md` CLI examples.
- **Dependencies:** T1–T3.

## 4. Product Owner Decisions (Accepted)

The owner reviewed this spec and accepted all decisions below. These are now binding; implementation proceeds without further negotiation.

### D-01 — Verbosity model: ACCEPTED

**Decision:** Use a `VerbosityMode` enum (`QUIET` / `DEFAULT` / `VERBOSE`), **not** stdlib log levels.

| Mode | Root logger level | What it shows |
|---|---|---|
| `QUIET` | `WARNING` | Errors + warnings only |
| `DEFAULT` | `INFO` | Current behavior |
| `VERBOSE` | `DEBUG` | Full diagnostics |

- **Config default:** `DEFAULT` in `app_config.json5`.
- **CLI flag:** `--verbosity [quiet\|default\|verbose]` on `app.py run`.
- **Override seam:** `logging.getLogger().setLevel()` on root logger (reaches all `core/` loggers via propagation — `init_service.py:9`, `network.py:29`, `reports.py:29`, `services.py:25`, `tasks.py:9`, `utils.py:15`, `paths.py:11`).
- **No backward compatibility required.**

### D-02 — `run` CLI default: ACCEPTED

**Decision:** `run` gains `--verbosity [quiet|default|verbose]`. When omitted, uses the config default (`DEFAULT`). No `--verbose` / `--quiet` binary flags.

### D-03 — Progress bar state model: ACCEPTED (4 states)

**Decision:** Simplified to exactly **four** task statuses — `QUEUED`, `SENT`, `COMPLETED`, `FAILED`. No pipeline-stage bars (no Submitted/Waiting/Received/Ready/errors split). No `Waiting` as instantaneous occupancy.

| Status | What it means |
|---|---|
| `QUEUED` | Task created but not yet sent to API |
| `SENT` | Task submitted to API, awaiting result poll |
| `COMPLETED` | Task extracted successfully |
| `FAILED` | Task errored or was skipped |

`QUEUED` is derived at enqueue; `SENT` after sender returns key; `COMPLETED` after extraction; `FAILED` in worker exception/skip.

### D-04 — Counting semantics: ACCEPTED (fraction-of-total)

**Decision:** `status_progress = tasks_with_this_status / total_tasks * 100`. Each bar displays `STATUS [██░░░░░] XX% (N/total)`. Bars start empty and fill as the report progresses. `total` is set after period resolution + first task generation.

### D-05 — Library (Jupyter/Airflow) path: ACCEPTED

**Decision:** Library path (`run_report_async`/`run_report`) uses `NullProgressReporter` and the config-default verbosity (`DEFAULT`). Rich `Live` only activates in `app.py run`.

**Extension:** Add optional `progress: ProgressReporter | None = None` parameter to both `run_report_async()` and `run_report()`. `None` → `NullProgressReporter`. CLI injects Rich implementation. Tests use NullReporter.

## 5. Research Summary

A `researcher` agent investigated A–E against the code (findings code-traced; no live API/credentials were available).

- **A. Task lifecycle:** `TVTask` (`tasks.py:12-25`) has `status` (bool), `log_error` (bool), `error` (str). The four `TaskStatus` enum values map cleanly: `QUEUED` before `send_task` returns a key (`reports.py:364-401`), `SENT` after key received, `COMPLETED` after `extract_data`/`csv_to_file` succeeds (`reports.py:478-525`), `FAILED` in worker exception/skip branches (`reports.py:180-183,196-201`). The worker pool runs `max_tasks` workers (`reports.py:209-211`) pulling from `self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (`reports.py:120`).
- **B. Logging:** root level `"INFO"` in `log_config.json5:47-49` is the sole gate; `LoggingSettings` (`models.py:18-28`) models the whole dictConfig. No verbosity field on `AppSettings` today. `AppService.prepare_app` applies `logging.config.dictConfig` (`services.py:59-66`) once at construction via a `cached_property` (`services.py:39-57`). **Override seam = `logging.getLogger().setLevel()` on the root logger** — it reaches every `core/` module logger via propagation, no reload needed.
- **C. CLI:** `--verbose = typer.Option(True, …)` (`app.py:48`) gates only `console.print` (`app.py:53-54,58`). Replacing with `--verbosity` (enum) is straightforward (Typer `Enum` support); maps to root logger levels.
- **D. Rich:** `rich==14.3.3` confirmed installed. `rich.progress Progress` / `BarColumn` / `TextColumn` / `rich.live.Live` all importable. **Recommended design = a `ProgressReporter` protocol injected into `MediaReport`**, with `NullProgressReporter` as the default so `core/` never imports Rich. `Progress.update()` is internally lock-guarded; all worker coroutines run on the single event-loop thread, so callback counters need no extra locking. `rich.progress.track()` is too limited (one bar) for four simultaneous bars per report.
- **E. Startup output:** start/finish/processing lines live in `services.py` (`logger.info("…Processing started…")` at `:173`, `Report N` at `:179`, `…finished…` at `:219`). **Recommended ordering: emit startup lines first, then enter `Live(Progress(…))` around `asyncio.run(...)`** — `Live(redirect_stdout=True)` (default) renders subsequent logging output above the fixed bar region.
- **F. Multi-report runs:** A single `run` can process multiple report entries (e.g., `nat_tv_brands_theiss.json5` has 2 reports). Each report should get its own set of 4 bars. `services.py:178` iterates entries sequentially, so the `Live` region can wrap each `run_report()` call individually.

### Approaches compared (researcher's verdict)

| Concern | Approach 1 | Approach 2 | Approach 3 | Verdict |
|---|---|---|---|---|
| Where Rich lives | Injected `ProgressReporter` protocol; `Progress`/`Live` in `app.py` only, `core/progress.py` holds protocol + Null | Separate asyncio task polls `queue.qsize()` + stage lists; no worker changes | `Progress` imported directly inside `reports.py` | **1 wins**: respects separation of concerns; testable via `NullReporter`; no polling race. |
| Verbosity seam | `logging.getLogger().setLevel()` on root (runtime, no reload) mapped from `VerbosityMode` enum | Re-run `dictConfig` from a modified `log_config.json5` each run | Add a Typer option per module logger | **1 wins**: minimal, standard, no file IO per run. |
| Total known up-front | Set bar `total` after period+generation resolves, then re-render | Absolute counts only (no `total`) | Pre-count by calling `slice_period` twice | **1 is preferred** (fraction is what the owner wants); the re-render flicker is acceptable. |
| Bar scope | One set per report | One set per entire run (aggregating all reports) | One bar per task | **1 wins** — owner wants per-report, and `services.py:178` provides natural boundaries between reports. |

## 6. Assumptions

| ID | Assumption | Basis / risk if wrong |
|---|---|---|
| A-01 | `VerbosityMode.QUIET` maps to stdlib `WARNING` level (i.e. `ERROR` is included and the owner considers it part of "errors"). | Stdlib convention; `05-Problem.md:6`. |
| A-02 | The four task statuses (`QUEUED`, `SENT`, `COMPLETED`, `FAILED`) are the states the owner wants tracked as progress bars. | Owner clarification; replaces the 5-state model. |
| A-03 | Bars should express **progress toward completion** (fraction / cumulative), with `status_progress = tasks_with_this_status / total_tasks * 100`. | Owner's formula clarification. |
| A-04 | Rich `Live` + per-line logging (not `console.print`) is acceptable for scrolling startup output above the bars. | Standard Rich pattern; `redirect_stdout=True`. |
| A-05 | Library users (Jupyter/Airflow) do **not** get the bar view or verbosity override interactively; they keep the config default. | D-05; `app.py:80-98`. |
| A-06 | `total_tasks` is determinable after `get_period` + the first pass of `generate_tasks`; it is not known at report start. | `reports.py:295-338` (period) + strategy `generate`. |
| A-07 | A run may contain multiple reports; each needs its own bar set. | `nat_tv_brands_theiss.json5` (2 reports per file); `services.py:178`. |

## 7. Constraints

- **No Rich in `core/`.** Enforced by `doc-maintenance-rules.md:107-108` and `AGENTS.md` rule 3. The reporter protocol must live in `core/` but must not import `rich`.
- **Async, single event loop.** Workers are coroutines on one loop (`reports.py:209-211`); `csv_to_file`/`to_file` are offloaded via `asyncio.to_thread` (`reports.py:510`, `tasks.py:50`). Rich `Live` must be driven from `app.py`, on the loop, around `asyncio.run`.
- **No database / no web framework / no scheduler.** Standalone fetcher; `rich`, `typer`, `pydantic`, `pandas` only (deps: `pyproject.toml:15-28`).
- **No fail-fast / resume must be preserved** (D-08, D-09, D-17): progress bars are a view only; they must not change the worker's `try/except/finally` contract (`reports.py:149-188`) or the `check_done` resume pre-filter (`reports.py:75-83,507-509,563-567`).
- **Tests.** `pytest-asyncio` (auto), `pytest-mock`, `hermetic_app_service` fixture (`conftest.py:131-146`). The hermetic pipeline (`test_services.py:71-137`) and lifecycle tests (`test_reports_lifecycle.py`) must stay green — hence `NullProgressReporter` default. Coverage gate is 70% (`pyproject.toml:166`).
- **mypy strict on `src`+`tests`** (`pyproject.toml:142-154`); ruff `src tests` (CI). No new errors in touched files.
- **`log_config.json5`** handlers/formatters/levels are `dictConfig`'d at `AppService` construction (`services.py:59-66`); runtime level override via `logging.getLogger().setLevel()` mapped from `VerbosityMode` must happen *after* `prepare_app` (i.e. in `app.py`).

## 8. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| R-01 — `total_tasks` unknown at start → bars flicker/re-render when total changes mid-run. | Confusing UX. | Start bars empty; re-render when total is known; document the flicker as expected. |
| R-02 — Threading: `asyncio.to_thread` offloads `csv_to_file`/`to_file`/`json5.dump` (`reports.py:510`, `tasks.py:50`). If any callback mutated the Rich `Progress` from that thread, `Live` redraw could race. | Crash/garbled output. | Workers call the **protocol** (counters in plain Python ints, mutated on the event-loop thread only); `Live` is driven solely from `app.py`. The protocol implementation that touches Rich lives in `app.py`, on the loop. |
| R-03 — Multi-report runs: if a report file contains multiple entries, bars must be scoped per-report, not shared globally. | Incorrect counts. | `services.py:178` iterates entries; each `run_report()` call creates a fresh `Live` region. |
| R-04 — `redirect_stdout=True` on `Live` captures stdout; if any `core/` path uses `print()` (none currently — `AGENTS.md` rule 12; verified `core/` uses only `logger`) or `console.print` outside `app.py`, it would be swallowed. | Missing output. | Verified no `print()` in `core/`; Rich `Console` only in `app.py` (`app.py:12,37,54,58,73,76`). |
| R-05 — VERBOSE mode suppresses per-task INFO stage logs — if not suppressed, output doubles up with bars. | Noisy/confusing UX. | When `VerbosityMode.VERBOSE`, suppress `task_stage_complete` logs (`reports.py:384,471,520`) and rely on bars only. Bars remain visible via Rich `Live` regardless of verbosity. |

## 9. Open Questions (implementation-level)

1. **Q-1** — When `VerbosityMode.VERBOSE` is set, should per-task INFO logs (e.g. `task_complete` at `reports.py:519`) continue printing alongside the bars, or should they be suppressed in favor of the bar view? *Owner decision: suppressed (R-05).* *(closed)*
2. **Q-2** — In VERBOSE mode, DEBUG-level connection/request traces (`network.py:135-176`) should appear above the bars. Confirm `redirect_stdout=True` on `Live` correctly surfaces these during the bar region. *(implementation verification)*
3. **Q-3** — For multi-report runs, should the `Live` region wrap the **entire** `asyncio.run(run_report(...))` per report, or only the worker phase (after startup output)? Wrapping per-report `asyncio.run()` is simplest but may re-render startup lines. *(implementation choice)*

## 10. Out of Scope

- Changing the Mediascope task/poll/download pipeline stages themselves, the two-level concurrency model (`max_tasks` vs `max_concurrent_requests`, `async-concurrency-architecture.md`), or the retry/backoff policy (`network.py:135-176`).
- The region-selection redesign (R-01 / G-03 / DISC-09 in `open-questions.md`) — unrelated.
- Output-convention configurability (R-02 / G-04 / D-13) and DICT dictionary columns (R-03 / D-14).
- Credential best-practice changes (D-12 / W-06) and README install-path fix (DISC-10).
- Adding a database, web framework, or scheduler.
- Any change to the CSV output shape, separators, or compression (D-13 explicitly defers this).
- Changing the existing `--verbose` Typer option name (D-02 requires removing it, not keeping it).
- Per-task progress display (the owner explicitly rejected this for 200+ task reports).
- Worker-pool occupancy metrics (not part of the 4-task-status model).

## 11. Definition of Ready

This spec is ready to hand to a `planner` (→ `implementor`). All owner decisions (D-01–D-05) are accepted in §4; all open questions (Q-1–Q-3) are resolved in §9.

Remaining implementation-level verification:
- [ ] Q-2: Verify `redirect_stdout=True` on `Live` surfaces DEBUG logs above bars during verbose runs.
- [ ] Q-3: Confirm per-report `Live` wrapping strategy matches multi-report entry iteration.

Done (for implementation) = T1–T8 pass: `uv run ruff check src tests`, `uv run pytest` green, `uv run mypy src tests` with no new errors in touched files, and `docs/00-overview/*` updated per `doc-maintenance-rules.md`.

## 12. Evidence Map (code citations)

| Artifact | Lines |
|---|---|
| `TVTask` fields (status bool, no enum) | `tasks.py:12-25` |
| Worker pool + bounded queue | `reports.py:120, 205-237` |
| `_process_item` send→wait→extract | `reports.py:141-162` |
| `send_task` key assignment (SENT boundary) | `reports.py:364-401` |
| `wait_task` poll loop | `reports.py:403-476` |
| `extract_data` get_result + csv_to_file (COMPLETED boundary) | `reports.py:478-525` |
| Worker exception/skip (FAILED boundary) | `reports.py:180-183, 196-201` |
| `processing_tasks` enqueue (QUEUED boundary) | `reports.py:214-215` |
| `run` CLI command + `--verbose` flag | `app.py:40-62` |
| `console = Console()` (Rich in CLI only) | `app.py:12` |
| `run_report_async` / `run_report` (library API) | `app.py:80-98` |
| `get_connection` (silent connection setup) | `services.py:140-154` |
| Processing start/finish logs | `services.py:173, 179, 219` |
| Entry iteration (multi-report run) | `services.py:178` |
| `AppService.prepare_app` / `dictConfig` | `services.py:59-66` |
| `reload_config` invalidation hook | `services.py:68-78` |
| `AppSettings` (no verbosity_mode today) | `models.py:60-81` |
| `MediaType`/`ReportType`/`ReportSubtype` enums (style reference) | `models.py:84-104` |
| `log_config.json5` root level + empty loggers | `log_config.json5:8-51` |
| `init` template copy | `init_service.py:37-57` |
| `hermetic_app_service` fixture (tests must stay green) | `conftest.py:131-146` |
| `doc-maintenance-rules` progress-bar rule | `doc-maintenance-rules.md:107-108` |
| Report config example (2 reports per file) | `settings/reports/nat_tv_brands_theiss.json5` |
