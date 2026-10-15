---
plan_id: "02-audit-conformance-corrections"
target: ".ai/audit/99-validation/*.md (13 files)"
status: "applied-2026-10-15"
agent_role: "implementor (launch blocked by provider rate limit; applied by tech-lead directly — 4 launch attempts rejected)"
session: "ses_ec08526f0ffeX13tXISNlaWkHl (never started; kept for audit trail)"
reviewed: "2026-10-15"
---

# Audit Corpus Conformance Corrections (ready to apply)

## Why

Owner ruling D-05: the audit corpus is **not** a source of truth — only the code
and the owner's decisions are authoritative. This file is the reviewed,
code-verified correction list for the 13 reports in
`.ai/audit/99-validation/`. Every correction below was re-verified against
HEAD by the tech lead (probes in `.ai/tmp/`, vendor library sources in
`.venv/Lib/site-packages/mediascope_api/`).

## How to run

~~Resume the implementor session (`task_id=ses_ec08526f0ffeX13tXISNlaWkHl`) with
the brief from the review message, or launch a fresh implementor pointed at
this file.~~

**APPLIED 2026-10-15.** Four attempts to launch/resume the implementor session
were rejected by the provider ("Poolside: Rate limit exceeded"), so the
corrections were applied directly by the tech lead, file by file, with each
claim re-verified against the code before editing (per D-05). Verification
after application: all links in the corpus and the plan resolve (including 3
pre-existing broken links in `10-code-quality` fixed en route); `uv run pytest`
→ 23 passed; `git status` on `src/`, `tests/`, `pyproject.toml` is empty (no
code was touched). Per-file outcome record: each file's `## Conformance`
section (or the newly inserted one for 15/16/17) carries a "Conformance review
2026-10-15" block listing the applied corrections and the D-*/G-*/W-*/R-*
mapping; findings that were already accurate at HEAD (CFG-007 two-path
mechanism, EXT-005 dead-`except HTTPError` analysis, EXT-006 singleton
analysis, PERF-001 scope, AUT-001 vendor-refresh nuance, NUM-001 probe record)
were left as-is and only gained the mapping — noted here so the record is
honest about which corrections were substantive and which were confirmatory.

## Task A — every file

After the existing header/scope section insert:

```
### Conformance (2026-10-15)

Reviewed against the code at HEAD and the owner decision register (D-05: the
audit is a lead source, not a source of truth). Corrections applied during this
review are listed below; mechanism changes cite the verifying code location.
Mapping: <IDs> -> <D-*/G-*/W-*/R-*>.
```

## Task B — per-file corrections (verified)

### 01-entry-architecture-validated-findings.md
1. **ENT-001 → "fixed at HEAD"** (was "confirmed at code_ref"). At `d093d9c`
   `sys.stdout.reconfigure(...)` was module-scope in `app.py`; at HEAD it is the
   first statement inside `run()` (`src/mko_get_mediascope_data/app.py:45`).

### 02-config-secrets-validated-findings.md
2. **CFG-007 + any "subtype without defaults block" finding → two-path
   mechanism.** `data_lang: "ru"` (default) crashes with `TypeError` inside
   `en_to_ru(None)` (`core/utils.py`, `len(None)`); `data_lang: "en"` reaches
   `Data(**merged)` and raises a Pydantic `ValidationError`
   (`core/services.py:125`) because `slices`/`statistics`/`sortings`/`options`
   are non-optional `dict | list` fields (`core/models.py`, `Data`). Probes:
   `.ai/tmp/doc_probe_reverify_audit.py`, `.ai/tmp/doc_probe_options_none.py`.
3. **CFG-003** → map to D-12 / W-06.

### 03-async-concurrency-validated-findings.md
4. **ASY-001 → add the verified nuance**: when an exception escapes a worker,
   `task_done()` is skipped (`core/reports.py:153`) so `queue.join()`
   (`core/reports.py:161`) never returns **and no traceback is printed** (the
   worker task object is never awaited); symptom = console stops advancing,
   process never exits. Verified by executing the real `MediaReport.worker`
   with `RuntimeError` and `BadRequestError` in `send_task` (both hang) —
   `.ai/tmp/doc_probe_worker_hang_classes.py`. Add the three failure classes:
   (a) failures before the pool (validation/connection/availability) abort
   cleanly with exit 1; (b) logical failures (`status=False`) write the error
   YAML and continue; (c) exceptions raised inside a worker hang silently. Map
   D-09/D-17, gap G-02, work W-02.
5. **ASY-004** → keep "not substantiated" and cite owner decision D-04
   (`max_tasks` and the HTTP semaphore are intentionally independent).

### 04-data-pipeline-validated-findings.md
6. **PIPE-005 → mechanism correction**: a non-overlapping requested period is
   clipped to an inverted period and `iter_period_slices` returns immediately
   (`if overall_start > overall_end: return`, `core/utils.py`) → `slice_period`
   yields **zero intervals** → zero tasks → exit 0 with success. Probe-verified.
7. **PIPE-009** (`log_error` write-only) → add the owner requirement this block
   exists to serve: "no data" must become a **warning**, "processing failure"
   must be an **error file** (D-08, D-15). Map G-01, W-02 slice 2.
8. **PIPE-007** → add: in current code the raised exception hangs the run first
   (ASY-001), so error serialization is unreachable for exceptions.

### 05-config-templates-validated-findings.md
9. **TPL-003** → same two-path refinement as item 2.
10. **Add DISC-07** (verified): `settings/defaults/tv/crosstab.yaml` parses OK at
    commit `3408747` and fails from `b04d177` onward — `yaml.safe_load`
    `ParserError` at line 138 (`TOP_NAT_TV_PROGRAMS.slices` sequence closed with
    `}` instead of `]`); the i18n translation commit introduced the regression.
    Probe: `.ai/tmp/doc_probe_git_defaults.py`. Map G-05/W-01.
11. **Add D-06/D-11 mapping**: repair all templates to the current schema;
    nothing deleted; templates intentionally incomplete.

### 09-external-api-validated-findings.md
12. **EXT-005 — MECHANISM CORRECTION (important).** The vendor raises
    `MediascopeApiError` subclasses, **not** `requests.HTTPError`
    (`.venv/Lib/site-packages/mediascope_api/core/errors.py`; `_raise_error` in
    `core/net.py`: 400→`BadRequestError`, 401→`AuthorizationError`,
    403→`AccessForbiddenError`, 404→`NotFoundError`/`NoDataError`,
    429→`TooManyRequestsError`, 5xx→`ServerError`). Our `except HTTPError`
    branch (`core/network.py:101-111`) therefore never fires for vendor
    responses; those exceptions hit the generic handler
    (`core/network.py:113-118`) and are **re-raised un-retried** — 429 and 5xx
    are NOT retried today; only `requests` transport errors
    (`RETRIABLE_EXCEPTIONS`) are retried. Rewrite mechanism and required fix.
13. **EXT-002** confirmed; cite `net.py:192-193` (no `timeout=` argument).
14. **EXT-003** confirmed; cite `core/reports.py:289-293` (break on
    DONE/FAILED/CANCELLED, `task.status` stays True → extraction attempted for
    failed tasks).
15. **EXT-004 — mechanism correction.** `sleep_time=40` binds to
    `NetworkClient.call`'s **own** parameter (`core/network.py:63`) and never
    reaches the vendor `get_status(self, tsk)`
    (`mediavortex/tasks.py:1208`, no such parameter). Pre-call smoothing is
    `asyncio.sleep(uniform(_sleep_time*0.5, _sleep_time*2))` = uniform(20s, 80s)
    (`core/network.py:66-69`) plus `await asyncio.sleep(2)` in the poll loop
    (`core/reports.py:278`) → **effective poll gap ~22–82 seconds**.
16. **EXT-008 — scope precisely.** Confirmed for our code: the generic handler
    logs `args`/`kwargs` (`core/network.py:113-117`), which can include the
    vendor task dict containing `userName`. Not automatic for the vendor: the
    bearer token is printed only inside the explicit debug helper
    `get_curl_request` (`net.py:445-448`). Additionally record the verified side
    effect: on JSON decode failure `_req_to_json` writes
    `json-error-<timestamp>.txt` **into the current working directory**
    (`net.py:451-465`).
17. **EXT-001** keep as an unreproduced risk; add the mechanism: `send_task`
    returns the vendor task JSON (`mediavortex/tasks.py:979` → `_send_task`) and
    `task.key` holds that dict (`core/reports.py:262-263`), so a retry after a
    lost response can resubmit the same calculation.
18. **EXT-006** confirmed; cite `net.py:73` (one `requests.Session` per
    `MediascopeApiNetwork`) and `core/services.py:127-135` + the per-entry loop
    (new `MediaVortexTask`, hence new Session, per report entry; none closed).

### 10-code-quality-validated-findings.md
19. **QLT-006** exact site: `core/reports.py:459-461` —
    `getattr(report.data_settings, "slices", None) or report.data_settings.get("slices", None)`;
    `Data` is a Pydantic BaseModel without `.get`; `AttributeError` fires
    whenever `slices` is empty/None (in `DictDataStrategy.prepare`).
20. **QLT-018**: `en_to_ru` (`core/utils.py`) catches only `TypeError`; a
    non-string item makes `param.replace(...)` raise `AttributeError` (uncaught).
21. **QLT-019**: `extra="allow"` absorbs unknown report keys, but the merge
    overlays only keys already present in `DATA_DEFAULTS`
    (`core/services.py:113-115`); legacy keys `folder`/`category_name` have zero
    references in `core/`.
22. Conformance block also notes **QLT-016** (`core/errors.py` classes never
    raised — grep-verified) and **QLT-017** (`core/logger.py` is a 0-byte
    placeholder).

### 11-test-coverage-validated-findings.md
23. **TST-002 — stronger evidence.** `tests/test_services.py::
    test_run_report_with_real_models` uses the **real** `get_app_service()`
    (line 55) instead of the isolated `local_app_service` fixture (lines
    90-99), so running the suite creates `<user export folder>/finance/raw_data`
    in the real user config area (observed on the dev machine: created
    2026-10-14 20:36). Baseline: 23 tests, all passing.
24. **TST-001** mechanism: `TVMediaReport.create_report` is mocked
    (`test_services.py:49-52`) and the mocked `result2table` returns an empty
    DataFrame (line 35) — no CSV/output-file assertion exists.

### 12-production-ops-validated-findings.md
25. **OPS-003 — split.** Named-logger part confirmed (`log_config.yaml` loggers
    `__main__`/`tests`/`app` never match module `__name__` values, so only
    `root` applies); `print()` part falsified at HEAD (see CFG-010 — every
    remaining `print(` in `core/` is a comment).
26. **README discrepancies** → add D-19 as DISC-10 (no PyPI; intended install
    `pip install <github_url>`), DISC-01 (`list_reports` vs actual
    `list-reports`), DISC-02 (`--no-verbose` invalid, exit code 2).

### 13-performance-validated-findings.md
27. **PERF-001 — scope correction.** Only `result2table` runs through
    `NetworkClient.call` (`core/reports.py:309-311`) and holds a semaphore
    permit for CPU work; `csv_to_file` runs via `asyncio.to_thread` directly
    (`core/reports.py:325`) and is **not** semaphore-gated; the pre-call jitter
    applies to every call (`core/network.py:69`).
28. **PERF-002** keep; cite vendor result handling (`get_result` and
    `result2table` holding the full `resultBody` in memory —
    `mediavortex/tasks.py:1405-1424`, `1455-1458`).

### 15-authorization-validated-findings.md
29. **AUT-001 — nuance.** The vendor calls `refresh_token()` before every
    request and re-authenticates only when expired (`net.py:178`, `127-138`), so
    the missing refresh-on-401 hook in our `NetworkClient`
    (`core/network.py:46` TODO) is a robustness gap, not the primary failure
    path; a 401 still raises immediately (`core/network.py:80-86`).
30. **AUT-002** confirmed with mechanism: 403 raises `AccessForbiddenError`
    (`errors.py:29-35`), not among our dedicated handlers → generic handler logs
    "Unexpected error" (`core/network.py:113-118`).
31. **AUT-003** confirmed by absence: `get_connection`
    (`core/services.py:127-135`) performs no credential/scope validation.

### 16-rendered-surface-validated-findings.md
32. **OUT-001/OUT-002/OUT-003** confirmed + re-probed at HEAD
    (`.ai/tmp/doc_probe_reverify_audit.py`: error YAML contains
    `!!python/tuple`, `safe_load` raises `ConstructorError`; duplicate CSV write
    returns `None` with no exception). **OUT-005** (interval missing from error
    YAML) relevant to D-09 (failed-task description is critical). **OUT-004**
    maps to D-13 / W-04.

### 17-arithmetic-validated-findings.md
33. **NUM-001** — verify the exact mechanism at HEAD first (`str_to_date` in
    `core/utils.py`; where range-format strings originate; where the pivot is
    skipped in `utils.pivot_df_frequency` / `DefaultDataStrategy.prepare`), then
    correct the headline/mechanism to match the code. Anything not verifiable
    must be marked unverified, not asserted.

## Task C — all files

Update Summary-table status cells where a mechanism or status changed
("fixed at HEAD", "mechanism corrected 2026-10-15"). Never delete findings.

## Verification commands for the executor

- `uv run pytest` (expect 23 passed)
- `Select-String -Path "src\mko_get_mediascope_data\core\*.py" -Pattern 'print\('` → all commented
- `... -Pattern 'log_error'` → 5 writes / 0 reads
- `... -Pattern 'data_settings\.get'` → single site `reports.py:461`
- Probes: `uv run python .ai\tmp\doc_probe_reverify_audit.py`,
  `uv run python .ai\tmp\doc_probe_worker_hang_classes.py`,
  `uv run python .ai\tmp\doc_probe_current_state.py`
