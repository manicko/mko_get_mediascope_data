---
plan_id: "07-verbosity-and-progress"
source_spec: ".ai/plans/problems/06-verbosity-and-progress_spec.md"
created_by: "tech-lead"
status: active
language: en
---

# Execution Plan — Console Verbosity & Progress Bars

## Overview

Implements the verbosity selector and per-report progress bars described in
`06-verbosity-and-progress_spec.md`. The spec is already decomposed into T1–T8
and includes a research summary (§5) and accepted owner decisions (§4). This
plan reorganises those tasks into dependency-aware execution blocks and resolves
two audit-level technical decisions (see *Resolved Decisions* below).

## Resolved Decisions (from audit)

| ID | Decision | Rationale |
|---|---|---|
| RES-01 | **T3: `TVTask` gains an explicit `task_status: TaskStatus` field** (not a derived property). | The spec proposed deriving a 4-state enum from `status: bool` + `error: str`, but `status=True` already covers three states (QUEUED/SENT/COMPLETED). The Auditor confirmed only `task.key` (SENT marker) is distinguishable; COMPLETED has no marker. Adding an explicit field set at each transition point is the clean, maintainable approach. Existing `status` bool is retained for backward compat. |
| RES-02 | **VERBOSITY maps level to root logger AND all handlers** (a `configure_logging_level()` helper). | `log_config.json5` sets `level: "INFO"` on handlers (lines 27, 35) as well as root (line 48). Setting only the root logger level would still drop DEBUG records at the handler. The `--verbosity` seam must lower handler levels too, or VERBOSE mode won't show DEBUG logs (RISK-04). |
| RES-03 | **T8: update `doc-maintenance-rules.md:107`** to allow multi-bar `Progress`+`Live`. | The doc mandates `rich.progress.track()` (single bar), but the spec needs 4 simultaneous bars per report. The researcher already noted `track()` is "too limited". The rule must be extended. |

## Execution Blocks

### Block 1 — Config & Task Status Models (T1 + T3)

**Semantic units:**
- `core/models.py` — add `VerbosityMode` StrEnum; add `TaskStatus` StrEnum; add `verbosity_mode: VerbosityMode` to `AppSettings`; add `task_status: TaskStatus` field to relevant model
- `core/tasks.py` — add `task_status: TaskStatus = TaskStatus.QUEUED` to `TVTask`; derive `status` bool setter compatibility
- `settings/app_config.json5` — add `"verbosity_mode": "DEFAULT"`
- `settings/log_config.json5` — set handler `level: "NOTSET"` (inherit from root) instead of hardcoded `"INFO"`, so root-level overrides propagate

**Acceptance criteria:**
- `VerbosityMode.QUIET/DEFAULT/VERBOSE` exist and map to WARNING/INFO/DEBUG
- `TaskStatus.QUEUED/SENT/COMPLETED/FAILED` exist
- `AppSettings.verbosity_mode` defaults to `DEFAULT`
- `TVTask.task_status` defaults to `QUEUED`
- `log_config.json5` handlers use `"NOTSET"` so `configure_logging_level()` controls everything

### Block 2 — CLI Verbosity Override + ProgressReporter Protocol (T2 + T4)

**Semantic units:**
- `core/progress.py` — new module: `ProgressReporter` Protocol with `report_started`, `task_status_changed`, `report_completed`; `NullProgressReporter` no-op impl
- `app.py` — replace `--verbose` bool with `--verbosity [quiet|default|verbose]`; add `configure_logging_level(mode: VerbosityMode)` helper; add `progress: ProgressReporter | None = None` param to `run_report` / `run_report_async`

**Acceptance criteria:**
- `--verbosity quiet` → root+handlers at WARNING; `--verbosity verbose` → DEBUG; omit → config default
- `ProgressReporter` protocol is pure-Python (no Rich import)
- `NullProgressReporter` is a no-op
- Library API signatures accept optional `progress` param

### Block 3 — Wire Reporter into Worker Loop (T5)

**Semantic units:**
- `core/reports.py` — `MediaReport` gains `reporter: ProgressReporter | None = None`; `TVTask` task_status set at: QUEUED (enqueue), SENT (after `send_task` key), COMPLETED (after extraction), FAILED (exception/skip branches)

**Acceptance criteria:**
- Reporter callbacks fire at exactly the 4 status transition points specified in T5
- Callbacks are asyncio-safe (single event-loop thread)
- When reporter is `NullProgressReporter`, no behavior change

### Block 4 — Rich Progress View (T6)

**Semantic units:**
- `app.py` — `run` command builds `rich.progress.Progress` with 4 `BarColumn`s per status; wraps each `run_report()` call in `rich.live.Live(redirect_stdout=True)`

**Acceptance criteria:**
- 4 fixed-position bars render per report (not per run)
- Bars show `STATUS [██░░░░░] XX% (N/total)`
- Startup output (API connection, processing start/finish) scrolls above bars
- `Live` not started when reporter is `NullProgressReporter`

### Block 5 — Tests (T7)

**Semantic units:**
- `tests/test_verbosity.py` — new: `VerbosityMode` config validation; Typer runner tests for `--verbosity` flag mapping
- `tests/test_progress.py` — new: `NullProgressReporter` no-op; mocked reporter `task_status_changed` calls at send→extract boundaries
- `tests/test_services.py` — verify hermetic pipeline stays green (no reporter needed)

**Acceptance criteria:**
- `uv run ruff check src tests` clean
- `uv run pytest` green
- `uv run mypy src tests` no new errors in touched files

### Block 6 — Documentation (T8)

**Semantic units:**
- `docs/00-overview/current-behavior.md` — CLI command table for `run`, verbosity model, progress bars
- `docs/00-overview/settings-layers-architecture.md` — new `verbosity_mode` setting
- `docs/00-overview/doc-maintenance-rules.md:107` — extend progress-bar rule for multi-bar `Progress`+`Live`
- `README.md` — CLI examples for `--verbosity`

**Acceptance criteria:**
- Docs reflect new `--verbosity` flag, `VerbosityMode`, `ProgressReporter` protocol, and per-report bars
- `doc-maintenance-rules.md` updated to allow multi-bar approach

## Risk Register

| Risk | Mitigation |
|---|---|
| RISK-03 (spec deviation): `TVTask` bool can't distinguish 4 states | Resolved via RES-01: explicit `task_status` field |
| RISK-04: handler-level INFO drops DEBUG in VERBOSE | Resolved via RES-02: `configure_logging_level()` lowers handlers too |
| RISK-05: VERBOSE mode suppresses per-task INFO logs | Suppress `task_stage_complete` logs when VERBOSE (spec R-05) |
| Coverage gate (70%) | New tests in Block 5 cover new code paths |

## Execution Order

```
Block 1 → Block 2 → Block 3 → Block 4 → Block 5 → Block 6
```

Blocks 1–2 are parallelizable in concept but the workflow restricts to 1
Implementor at a time, so each block is implemented and committed sequentially.
Documentation (Block 6) is handled by Doc-specialist after all code blocks.
