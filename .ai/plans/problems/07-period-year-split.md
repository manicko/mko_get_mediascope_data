---
plan_id: "07-period-year-split"
source_decisions: "Mediascope API 366-day task period limit"
source_behavior: "docs/00-overview/current-behavior.md"
phase: "implementation"
status: "applied-2026-10-28"
agent_role: "tech-lead"
findings_covered:
  - "API rejects tasks with periods > 366 days"
  - "get_period returns single unsliced interval when multiple_files=False"
  - "No 366-day guard exists anywhere in codebase"
---

# Execution Plan: Split Periods by Calendar Year to Respect API's 366-Day Limit

## Problem

The Mediascope API rejects tasks whose period exceeds 366 days with:
> `CrossTab, kitId = 7: нельзя посчитать задачу с периодом большой, чем 366 дней.`

The failing report entry (`settings/reports/nat_tv_brands_theiss.json5`,
`DYNAMICS_BY_SPOTS_DICT` subtype) has `multiple_files: false` and
`date_filter: ["2023-01-01", "2026-12-31"]` — a 4-year span. When
`multiple_files=False`, `TVMediaReport.get_period()` returns a single interval
covering the entire clamped period, producing one API task with a period far
exceeding the 366-day limit.

## Root Cause

`reports.py:505-513` — the `multiple_files` flag conflates two orthogonal
concerns:
- **File output granularity** (one CSV per interval vs. one consolidated CSV)
- **API task granularity** (each `date_filter=[interval]` becomes one API task)

The API's 366-day limit is a **task-level** constraint. Every interval returned
by `get_period()` becomes a separate API task, so **every interval must be
≤366 days**, regardless of `multiple_files`.

## Solution

The 366-day per-task limit (`MAX_PERIOD_DAYS = 366` in `core/utils.py`) is
enforced per returned interval, but splitting is no longer **always**
year-pre-split before the `multiple_files` branch. Instead:

- `multiple_files=True` (default): the clamped period is sliced **directly** by
  `period.time_unit` (`utils.slice_period`). No year pre-splitting is applied, so
  `time_unit` intervals (e.g. weeks) can cross calendar-year boundaries; each slice
  is at most one unit, so it never exceeds 366 days.
- `multiple_files=False`: the whole clamped period is a single interval unless it
  spans more than 366 days, in which case it is split into calendar-year chunks
  (`utils.split_period_by_year`). `time_unit` is unused when `multiple_files=False`.

> **Implementation revision (2026-10-28):** the initial implementation followed
> the "always split by year" approach (Block 2, step 1 below) and was then
> corrected. Year pre-splitting on the `multiple_files=True` path fragmented
> year-crossing `time_unit` intervals (e.g. a week spanning Dec 30–Jan 5) and
> produced duplicate period coverage, so that path was revised to slice directly
> by `time_unit`. The `multiple_files=False` path retains calendar-year splitting
> but only when the period actually exceeds 366 days (≤366-day periods stay a
> single interval). Documented in `docs/00-overview/current-behavior.md`.

### Execution Blocks

#### Block 1: Utility function — `utils.py`

Add `iter_year_slices(start: date, end: date) -> Iterator[tuple[date, date]]`
that splits a date range into calendar-year-aligned chunks. Each chunk starts
on Jan 1 of a year (except the first chunk, which starts at `start`) and ends
on Dec 31 of a year (except the last chunk, which ends at `end`).

Example: `(2023-06-15, 2025-03-10)` →
`[(2023-06-15, 2023-12-31), (2024-01-01, 2024-12-31), (2025-01-01, 2025-03-10)]`

Each chunk is guaranteed ≤366 days (calendar year, including leap years).

Add `split_period_by_year(period: tuple[date, date]) -> list[tuple[str, str]]`
that wraps `iter_year_slices` and returns ISO-format string tuples (matching
the existing `get_period()` output format).

**Acceptance criteria:**
- Each yielded chunk is within a single calendar year boundary
- No chunk exceeds 366 days
- Single-year periods return a single chunk
- Edge cases handled: exact year boundaries, leap years

#### Block 2: Modify `get_period()` — `reports.py`

In `TVMediaReport.get_period()` (`reports.py:505-522`), after clamping to the
available period:

1. If `multiple_files=True`: slice the clamped period **directly** by
   `period.time_unit` via `utils.slice_period(period, frequency)`
   (`utils.py:99-116`). No year pre-splitting — `time_unit` intervals may cross
   calendar-year boundaries. Each slice is at most one unit, so ≤366 days.
2. If `multiple_files=False`:
   - If the clamped period spans more than `MAX_PERIOD_DAYS = 366` days
     (`utils.py:27`), split it into calendar-year chunks via
     `utils.split_period_by_year(period)` (`utils.py:148-160`).
   - Otherwise, return the whole clamped period as a single interval:
     `[(period[0].isoformat(), period[1].isoformat())]`.
   - `time_unit` is unused. Each chunk becomes its own API task + CSV output
     (no file merging).

**Acceptance criteria:**
- `multiple_files=True`: ≤366-day periods slice by `time_unit` (1+ slices);
  >366-day periods slice directly by `time_unit` across year boundaries (no year
  pre-split)
- `multiple_files=False`: ≤366-day period → single interval; >366-day period →
  calendar-year chunks, each ≤366 days
- All returned intervals are ≤366 days and ISO-format strings (not date objects)

#### Block 3: Tests — `tests/test_utils.py` and `tests/test_reports_lifecycle.py`

**`test_utils.py`:**
- `test_iter_year_slices_single_year`: period within one calendar year → 1 chunk
- `test_iter_year_slices_multi_year`: 3-year period → 3 chunks
- `test_iter_year_slices_leap_year`: period spanning a leap year boundary
- `test_iter_year_slices_exact_boundaries`: Jan 1 → Dec 31 edge case
- `test_iter_year_slices_partial_start_year`: mid-year start produces a shorter
  first chunk
- `test_split_period_by_year_returns_strings`: result is `list[tuple[str, str]]`
- `test_split_period_by_year_all_chunks_leq_366`: every chunk ≤366 days
- `test_split_period_by_year_single_year_short`: a <366-day single-year period
  returns one chunk

**`test_reports_lifecycle.py`:**
- `test_get_period_long_period_multiple_files_false_splits_by_year`:
  >366-day period with `multiple_files=False` → multiple year chunks
- `test_get_period_long_period_multiple_files_true_slices_by_time_unit`:
  >366-day period with `multiple_files=True` → sliced directly by time_unit
  (no year pre-split), intervals crossing calendar-year boundaries
- `test_get_period_short_period_unchanged_multiple_files_false`: <366-day
  single-year period with `multiple_files=False` → unchanged (single interval)
- `test_get_period_multiple_files_false_returns_single_period`: ≤366-day period
  with `multiple_files=False` → single interval (PIPE date-serialization: string
  dates, not date objects)
- `test_get_period_last_time_calls_get_last_period`: date computation from `last_time`
- `test_send_task_with_multiple_files_false_passes_string_date_filter`: PIPE
  date-serialization — `build_task` receives string dates, not date objects

#### Block 4: Documentation

Update `docs/00-overview/current-behavior.md` and
`docs/00-overview/data-flow-architecture.md` to document:
- The 366-day API limit (`MAX_PERIOD_DAYS = 366`)
- The `time_unit` default (now `y`) on `PeriodModel`
- How `get_period()` now branches on `multiple_files`: direct `time_unit`
  slicing (no year pre-split) for `multiple_files=True`; single interval for
  `multiple_files=False` unless the period exceeds 366 days, in which case it
  is split into calendar-year chunks

## Implementation Sequence

Block 1 → Block 2 → Block 3 → Block 4 (sequential, single Implementor)

## Risks and Mitigations

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Multiple output files when `multiple_files=False` + year-split | Low — the API physically cannot process >366 days; splitting is mandatory | Only triggered when the period spans >366 days; each year chunk becomes a separate task; the DICT strategy runs per chunk. The ≤366-day case stays a single interval (no extra files) |
| Breaking existing tests with short periods | Low | A ≤366-day period with `multiple_files=False` returns a single interval — identical to before; `multiple_files=True` slices directly by `time_unit` without year pre-splitting |
| Leap year edge case (Jan 1 - Dec 31 = 366 days) | Low | Calendar year boundaries guarantee ≤366 days inclusive; `multiple_files=True` `time_unit` slices are each ≤1 unit |
| Resume/`check_done` compatibility | Low | File naming uses interval dates, so year-split (and `time_unit`) intervals produce standard filenames |
| Year-crossing `time_unit` intervals (e.g. a week spanning Dec 30–Jan 5) | Low — the regression that motivated the 2026-10-28 revision | `multiple_files=True` slices directly by `time_unit` without year pre-splitting, so a single interval can cross the year boundary instead of being fragmented into two |

## Constraints from Project Standards

- No `Any` types (strict Pydantic v2 + type hints)
- Enums for constants (366-day limit as a named constant in `utils.py`)
- English-only comments and documentation
- `logger = logging.getLogger(__name__)` in core modules
- Tests must verify logic and component interaction, not trivial details
