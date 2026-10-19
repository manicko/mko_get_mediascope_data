# Test-Quality Improvement Plan
## `mko_get_mediascope_data`

**Plan ID:** `test_quality_improvement_plan`
**Source audit:** `.ai/audit/tests/audit_report_1.md` (54 validated findings, commit `3efc4d7`)
**Author:** Kilo — Senior Staff Architecture Auditor (planning pass)
**Status:** Approved for execution
**Constraint:** This plan modifies **tests only**. The single `[PROD-FIX]` is a prerequisite gate
listed under Phase 0 and is explicitly **out of scope** for the test changes themselves — it must be
applied to production code (`src/mko_get_mediascope_data/core/tasks.py`) *before* Block 5 can proceed.

---

## 0. Overview

The suite passes 69/69 today but contains 1 latent production crash with **zero** coverage, 6
CRITICAL vacuous tests (false confidence), 14 HIGH-risk wrong-code-path / wrong-enum hazards, 22
MEDIUM items (magic numbers, internal spying, fragile coupling), and 9 LOW items (style/debt).

This plan is organized into 8 phases. Each phase lists the concrete file, the test symbol(s), and
the **exact** change required. Phases are sequenced by a 4-factor priority:

1. **Correctness / regression risk** — does a defect mask a real production failure?
2. **Business importance** — does the area affect live report runs?
3. **Impact on test quality** — does the change remove false confidence or close a coverage gap?
4. **Effort** — how much coordinated change is required?

A dependency graph and a validation checklist follow the phased work.

**Notational keys**

- `[PROD-FIX]`  = production code change required first (gate)
- `[T-REWRITE]` = test body must be substantially rewritten
- `[T-UPDATE]`  = test intent is sound; assertions/exercise need strengthening
- `[T-DELETE]`  = remove entirely
- `[BEST-PRACTICE]` = improvement beyond current spec
- `[DOC-UPDATE]` = no code change (comment accuracy only)

---

## Phase 0 — Production fix prerequisite (GATE, not test work)

This is the single `[PROD-FIX]` finding. It is **blocked-by** nothing and **blocks** Block 5
(test rewrite) and the Reporting coverage gap for `multiple_files=False`.

### [PROD-FIX] Date serialization crash in `TVTask.to_file`

- **File:** `src/mko_get_mediascope_data/core/tasks.py`
- **Symbol:** `TVTask.to_file` (lines 51–59, the `json5.dump(...)` call)
- **Root cause:** `self.interval` is typed `tuple[str, str] | tuple[date, date] | None`
  (line 22). Production assigns `date`-typed intervals when `multiple_files=False`
  — `Report.get_period` (reports.py:336–338) returns `[period]` where `period` is
  `tuple[date, date]` (reports.py:321). `json5.dump` has **no `default` serializer**, so
  `date` objects raise `TypeError: Object of type date is not JSON serializable`.
- **Failure trigger:** a failed task whose error artifact write hits `to_file` with a `date`
  interval → `TypeError` masks the original task error inside the `except Exception` block
  (reports.py:181–186), which itself calls `to_file` re-entrantly → **silently swallowed failure**.
- **Recommended fix (Option A — smallest blast radius):** add a `default=` serializer to the
  `json5.dump` call at tasks.py:52–58:
  ```python
  json5.dump(
      dump_settings,
      outfile,
      indent=2,
      ensure_ascii=False,
      quote_keys=True,
      trailing_commas=False,
      default=lambda o: o.isoformat() if isinstance(o, date) else str(o),
  )
  ```
- **Acceptable alternatives (if a different normalization layer is preferred):**
  - Option B — normalize at assignment in `DefaultTVTaskStrategy.generate` /
    `TVRegTaskStrategy.generate` (reports.py:506, 570).
  - Option C — make `get_period` return string tuples for the `multiple_files=False` path
    (reports.py:338), matching the `slice_period(..., as_strings=True)` path used for
    `multiple_files=True`.
- **Verification gate:** the rewrite of Block 5 (B5-1) and the new `multiple_files=False`
  reporting test both exercise the `date` interval. These must pass after the fix and **fail**
  (TypeError) if the fix is reverted.

> **Decision point for the implementor:** confirm Option A vs B vs C. The plan below assumes the
> fix is applied; Block 5 changes are marked `[BLOCKED]` until then.

---

## Phase 1 — Delete redundant / vacuous tests (low effort, high false-confidence removal)

Removing vacuous and redundant tests is safe (the retained tests or lifecycle tests provide
equal-or-better coverage) and reduces the 8–26s real-sleep burden.

### 1.1 Block 4 — delete T1, T2, T3, T7 + dead fixture + dead import

- **File:** `tests/test_network.py`
- **Delete `test_network_retry_and_fail` (T1, lines 21–30).**
  - *Justification:* identical intent to `test_call_retries_with_default_max_attempts` (T8), which
    asserts `call_count == 10`. T1 asserts only the `MaxRetriesExceeded` type and does NOT mock
    `asyncio.sleep`, incurring a real 8–26s delay (constructor `sleep_time=0` is overridden by
    `call()`'s `sleep_time=2` default at network.py:96, 100). Redundant + slow.
- **Delete `test_mock_network_client_retries_on_retryable_error` (T2, lines 33–57).**
  - *Justification:* CRITICAL vacuous test — replaces `client.call` with an `AsyncMock` and loops
    manually; asserts `call_count` on the **mock**, never exercising `NetworkClient.call`'s real
    retry loop. Provides zero signal on production retry logic.
- **Delete `test_call_returns_result_on_success` (T3, lines 60–70).**
  - *Justification:* redundant with `test_call_preserves_return_type` (T9, lines 167–178), which
    is a strict superset (asserts return value + type + per-key types).
- **Delete `test_default_max_attempts_is_10` (T7, lines 137–140).**
  - *Justification:* redundant with T8 (exercises the real retry loop to exhaustion) and pokes
    private `_max_attempts`. T8 already pins the default-attempt count.
- **Remove dead import** `MediascopeApiError` (line 11) — re-add when the G7 coverage test
  (Phase 4) is written (it will then be a real consumer, not a `# noqa: F401` no-op).

### 1.2 Block 4 — remove the `mock_network_client` fixture (consumer of T2 only)

- **File:** `tests/conftest.py`
- **Delete the `mock_network_client` fixture (lines 14–18).**
  - *Justification:* B4-2's `test_mock_network_client_retries_on_retryable_error` (T2) was its
    **only consumer**. With T2 deleted (1.1), the fixture is dead. This dependency is explicit:
    **T2 must be deleted before/alongside** the fixture removal so no in-repo reference dangles.
  - **Side effect check:** confirm no other test file references `mock_network_client`. (Verified:
    only `test_network.py` consumed it; `test_services.py` uses class-level patching of
    `NetworkClient.call`, not the fixture — B8-2.)

### 1.3 Block 3 — delete redundant `exists()` assertion (line 22)

- **File:** `tests/test_init_credentials.py`
- **Delete the redundant assertion** at line 22 (`assert app_paths.connection_config.exists()`).
  - *Justification:* it resolves to the identical `Path` as line 21 (`cred_file`). Both assert the
    same file existence; line 22 adds no signal.

### 1.4 Block 1 — delete 2 of 3 near-identical lazy-import tests

- **File:** `tests/test_app.py`
- **Delete `test_lazy_import_run_report_async` (lines 28–32).**
- **Delete `test_lazy_import_initialize_settings` (lines 35–39).**
- **Keep `test_lazy_import_run_report` (lines 21–25)** as the single representative of the PEP 562
  `__getattr__` callable path, **plus** the existing `test_lazy_import_raises_attribute_error`
  (lines 42–47) which covers the `AttributeError` fall-through.
  - *Justification:* all three exercise the same `__getattr__` symbol (B1-3); keeping one is
    sufficient. `test_lazy_import_same_object_as_app_module` (lines 69–74) is **not** flagged and
    is retained.

---

## Phase 2 — Fix wrong enum values + wrong code path (PREREQ for Block 6, correctness)

Block 6's strategy tests use `MediaType` values where `ReportType`/`ReportSubtype` are required,
and `test_dict_data_strategy` passes a `dict` to a method that only ever receives a `Data` model.
These must be corrected before any Block 6 assertions are meaningful.

### 2.1 Block 6 — `mock_report` fixture: correct enum attributes (B6-5 enrichment)

- **File:** `tests/test_reports.py`
- **Symbol:** `mock_report` fixture (lines 15–30)
- **Changes:**
  - Set `report.type = MagicMock(value=ReportType.crosstab.value)` (not `"TV"`).
    `MediaType.TV.value` is `"TV"`; `ReportType.crosstab.value` is `"crosstab"`. The factory uses
    `report.report_settings` (not `report.type`) — see reports.py:70 `self.type: ReportType`.
  - Set `report.subtype = MagicMock(value=ReportSubtype.DYNAMICS_BY_SPOTS.value)`
    (not `"crosstab"` — that is a `ReportType` value).
  - Add `report.catalogs = MagicMock()` and `report.output_columns = {...}` (currently missing;
    B6-5). This prepares the fixture for the richer `test_dict_data_strategy` rewrite in 3.2.
  - Add `from mko_get_mediascope_data.core.models import ReportType, ReportSubtype` import (already
    imported indirectly; make explicit).

### 2.2 Block 6 — delete `test_default_task_strategy` (B6-1, wrong enums + dead coverage)

- **File:** `tests/test_reports.py`
- **Delete `test_default_task_strategy` (lines 33–38).**
  - *Justification:* uses `report.type.value="TV"` and `report.subtype.value="crosstab"` (both
    wrong enums — table §3.3) and only 2 shallow assertions. It is fully superseded by the
    lifecycle test `test_create_report_writes_csv_and_marks_queue_done` (test_reports_lifecycle.py)
    which exercises `DefaultTVTaskStrategy.generate` with real `ReportSettings`/`Data` models and
    asserts on actual CSV output + queue draining.

---

## Phase 3 — Rewrite invalid / vacuous tests (correctness + regression)

### 3.1 Block 1 — rewrite `test_package_import_does_not_bootstrap` (B1-1)

- **File:** `tests/test_app.py`
- **Symbol:** `test_package_import_dos_not_bootstrap` (lines 50–66)
- **Problem:** patches `AppService.__init__` but `__init__.py` never instantiates `AppService`
  during a reload (it uses PEP 562 `__getattr__`); `mock_init.assert_not_called()` therefore
  passes **unconditionally** regardless of production behavior.
- **Rewrite:**
  - Ensure `"mko_get_mediascope_data.app"` is **not** in `sys.modules` before reload (purge if
    present).
  - `importlib.reload(mko_get_mediascope_data)`.
  - Assert `"mko_get_mediascope_data.app" not in sys.modules` (proves the package body does not
    eagerly import `app`).
  - Restore `sys.modules` state in a `finally` (avoid polluting other tests).
- **Delete** the now-unnecessary `mocker.patch("mko_get_mediascope_data.core.services.AppService.__init__")`.

### 3.2 Block 6 — rewrite `test_dict_data_strategy` to exercise the real `Data` path (B6-2)

- **File:** `tests/test_reports.py`
- **Symbol:** `test_dict_data_strategy` (lines 41–152)
- **Problem:** passes a plain `dict` as `report_mock.data_settings` (line 94), exercising the
  `isinstance(data_settings, dict)` branch at reports.py:611 — which is **dead code** in
  production (`Report.__init__` at reports.py:65 stores a `Data` BaseModel; `DictDataStrategy.prepare`
  always takes the `else` branch at reports.py:613–614).
- **Rewrite:** replace the `report_mock = MagicMock()` + `report_mock.data_settings = {"slices": [...]}`
  construction with a real `Data` model (or a `MagicMock` that has a `.slices` attribute), so the
  production `else` branch (`getattr(data_settings, "slices", None)`) is exercised. Concretely:
  - Build `report.data_settings = Data(slices=["channel", "program", "category"], ...)`.
  - Keep the 30-row DataFrame and the 5 group of assertions (column order, row counts, dedupe,
    parenthetical stripping, whitespace trimming) — they remain valid against the `else` branch.
  - Add an assertion that the `isinstance(data_settings, dict)` path is **not** the one taken
    (implicitly covered by passing a `Data` instance).

### 3.3 Block 6 — rewrite `test_tv_reg_task_strategy_skip_logs_warning` with correct enums (B6-4)

- **File:** `tests/test_reports.py`
- **Symbol:** `test_tv_reg_task_strategy_skip_logs_warning` (lines 191–229)
- **Problems:**
  - `report.type = MagicMock(value="TV_REG")` — `"TV_REG"` is `MediaType.TV_REG.value`, not a
    `ReportType` value (table §3.3).
  - Only the **skip** path (done-file exists) is tested; the **non-skip** path (real task
    generation with region iteration, `company_filter` construction, `id==99` skip) is unverified.
- **Rewrite:**
  - Set `report.type = MagicMock(value=ReportType.crosstab.value)` and
    `report.subtype = MagicMock(value=ReportSubtype.TOP_NAT_TV_ADVERTISERS.value)` (or another
    TV_REG-appropriate subtype). Use the enriched `mock_report` fixture from 2.1 where possible.
  - **Keep** the done-file-skip assertion (yielded tasks == 0, "already exists" warning).
  - **Add** a second test (`test_tv_reg_task_strategy_generates_tasks_for_regions`) covering the
    non-skip path: assert `settings["company_filter"]` contains `"regionId IN ({reg_id})"`,
    iterate `regions_list` (excluding `id==99`), and yield one task per region × interval.
  - Wire `report.report_settings.model_dump()` to return `{"regions": [...]}` for the explicit
    region-list branch (reports.py:525).

### 3.4 Block 5 — rewrite `test_to_file_writes_valid_json5` (B5-1) — [BLOCKED on Phase 0]

- **File:** `tests/test_tasks.py`
- **Symbol:** `test_to_file_writes_valid_json5` (lines 25–46)
- **Problem:** sets `task.interval = ("2025-01-01", "2025-01-07")` — **string** tuples. Production
  returns `tuple[date, date]` (reports.py:321, 338). The test never exercises the `date` code path
  and therefore cannot catch the `TypeError` crash from §4.
- **Rewrite (only viable after Phase 0 fix is applied):**
  - Set `task.interval = (date(2025, 1, 1), date(2025, 1, 7))` (import `date` from `datetime`).
  - Assert `to_file(tmp_path)` does **not** raise (this is the regression guard for the
    serialization crash).
  - Round-trip the JSON5 and assert `data["interval"] == ["2025-01-01", "2025-01-07"]` (the
    `default` serializer should produce `isoformat()` → `"YYYY-MM-DD"`).
- **Dependency:** this task is strictly `[BLOCKED]` on the Phase 0 `[PROD-FIX]`. Mark it
  **deferred** until the production fix is confirmed in CI.

### 3.5 Block 7 — rewrite `test_create_report_timeout_does_not_hang` (B7-1)

- **File:** `tests/test_reports_lifecycle.py`
- **Symbol:** `test_create_report_timeout_does_not_hang` (lines 262–347)
- **Problem:** mocks `asyncio.sleep` with `_yielding_sleep(0)` (real `sleep(0)`). The 60 mocked
  poll iterations complete in microseconds, hitting `wait_task`'s `max_polls=60` guard
  (reports.py:378–382) **before** the 0.5s `asyncio.wait_for` deadline. The test therefore asserts
  on the `max_polls` path — **not** the `TimeoutError` branch (reports.py:173–180) — despite its
  name. Error-payload content is never checked.
- **Rewrite:**
  - Make `get_status` return a non-terminal `IN_PROGRESS` status (already does) **and** make the
    poll loop genuinely exceed the 0.5s deadline. Two viable techniques:
    - (a) Patch `asyncio.sleep` with a **real** delay (e.g. `await asyncio.sleep(1.0)` per poll)
      so 1 poll > 0.5s deadline → genuine `asyncio.WaitFor`/`TimeoutError` in `worker`.
    - (b) Inject a `wait_task` double that `await asyncio.sleep(5)` so the outer `wait_for` times
      out at 0.5s, proving the `except TimeoutError` branch.
  - **Assert the error payload** contains `"Timeout after"` (reports.py:179:
    `task.error = f"Timeout after {timeout}s"`), not `"Poll deadline exceeded"`.
  - Keep the `qsize() == 0` and `errors/*.json5` existence assertions (queue draining + artifact).

---

## Phase 4 — Update tests with weakened/missing assertions (test quality)

### 4.1 Block 1 — strengthen/split `test_cli_init` + cover untested CLI paths (B1-2, B1-4)

- **File:** `tests/test_app.py`
- **B1-2:** Rename `test_cli_init` → `test_cli_init_help` (it only checks `--help`). Add a new test
  `test_cli_init_invokes_init_project` that invokes `["init"]` via `CliRunner` with
  `mocker.patch("mko_get_mediascope_data.app.init_project", return_value=tmp_path)` and asserts the
  stdout contains `"Settings initialized:"` (matches app.py:37 `[green]✅ Settings initialized:`) and
  exit code 0.
- **B1-4:** Add `CliRunner`-based tests for each untested public surface. Mock
  `get_app_service` so no real Mediascope credentials are needed:
  - `test_cli_run_invokes_report` — invoke `["run", <report_path>]`, assert "Running report" output
    and that the mocked `run_report` was called with the path.
  - `test_cli_run_reports_missing_file` — invoke `["run", nonexistent]`, assert non-zero exit.
  - `test_cli_list_reports` — invoke `["list-reports"]` with a mocked service whose
    `app_paths.reports` contains known files, assert the bullet lines.
  - `test_run_report_async_public_api` — assert `run_report_async("missing")` raises
    `FileNotFoundError` (app.py:85–86).
  - `test_run_report_sync_wrapper` — assert `run_report` calls `asyncio.run(run_report_async(...))`
    (mock `run_report_async`).
  - `test_initialize_settings_wrapper` — assert `initialize_settings(force=True)` returns the path
    from `init_project(force=True)`.
  - `test_sys_stdout_reconfigure_on_run` — (app.py:51–52) assert `sys.stdout.reconfigure` is called
    when `sys.stdout` is a `TextIOWrapper` (mock `get_app_service().run_report` to no-op).

### 4.2 Block 2 — fix magic number + regex + remove internal spying + strengthen assertions (B2-1…B2-7)

- **File:** `tests/test_models.py`
- **B2-1:** Rewrite `test_bundled_templates_kitId_conformance` to parse each JSON5 with
  `json5.loads` and recursively traverse the dict/list tree collecting every `kitId` key. Validate
  each value against `{1, 2, 3, 4, 5, 6, 7, 9, 10}`. (Current regex `kitId\s*:\s*(\d+)` matches
  only **3** of **26** occurrences — the unquoted `kitId:` shorthand in `defaults/tv_reg/crosstab.json5`
  — silently passing the other **23** quoted `"kitId": N` values.) Keep the `TOP_NAT_TV_ADVERTISERS`
  `kitId == 1` assertion (B2-1 tail) — it is correct.
- **B2-2:** Replace magic `total_entries == 32` (line 123) with a derived constant:
  - Add module-level `EXPECTED_TEMPLATE_ENTRIES = 32` (or, better, derive: walk
    `APP_PATHS.app_settings_dir / "reports"` and count entries via `json5.loads`) so the test fails
    loudly when templates are added/removed rather than silently.
- **B2-5:** Strengthen `test_report_settings_valid` (lines 21–31) — assert all fields with defaults:
  `report_type`, `report_subtype` (default `default_value`), `data_lang == "ru"`, `compression ==
  {"method": "gzip"}`, `multiple_files == True`, `check_done == True`, `ad_filter is None`,
  `relative_path == Path("tv/test")`, `period.date_filter is None`, `period.last_time.unit_count == 1`.
- **B2-6:** Strengthen `test_period_model_rejects_both` — add `match="Specify either"` to the
  `pytest.raises(ValidationError)` (line 35 currently has no `match`).
- **B2-3:** Remove the `mocker.patch("mko_get_mediascope_data.core.utils.en_to_ru")` spy in
  `test_merge_data_settings_en_does_not_translate` (line 50). The behavioral assertion
  `result["slices"] == ["EName_col"]` (line 67) already proves no translation occurred; the spy
  tests implementation detail. Remove the `mock_en_to_ru.assert_not_called()` line and the patch.
- **B2-4:** Rewrite `test_merge_data_settings_ru_with_none_slices` (lines 70–89) to use the **real**
  loading path (`AppService.get_default_data_settings`) rather than a synthetic
  `DataDefaults(DATA_DEFAULTS={"slices": None})` that bypasses `services.py:140` guard. Either
  construct a `DataDefaults` from a real file with `slices: null` and `data_lang="ru"`, or drop the
  test if the `None` path is confirmed unreachable (the guard `if slices is not None` prevents
  `en_to_ru(None)` from ever running).
- **B2-7 (gaps):** Add focused unit tests:
  - `test_report_settings_rejects_invalid_enum` — `media="INVALID"` raises `ValidationError`.
  - `test_period_model_single_date_filter_rejected` — `date_filter` of length 1 raises.
  - `test_en_to_ru_direct` — `en_to_ru(["EName_col"])` → `["Name_col"]`; `en_to_ru(None)` → `[]`.
  - `test_app_settings_normalize_path` — `AppSettings(export_folder="~/x")` resolves to an
    expanded `Path`.
  - `test_coerce_none_to_empty_edge` — `Data(options=None, statistics=None, slices=None)` all → `[]`.
  - `test_data_defaults_extra_fields_allowed` — a subtype overlay (`TOP_NAT_TV_ADVERTISERS`) is
    accessible via attribute on `DataDefaults`.

### 4.3 Block 3 — strengthen `test_init_produces_readable_credential_file` + cover init_service (B3-1, B3-3…B3-7)

- **File:** `tests/test_init_credentials.py`
- **B3-1:** After `init_project()`, open `mediascope.json` with `json.load()` and assert it parses
  and contains the expected credential keys (`username`, `passw`, `client_id`, `user_agent`).
- **B3-3:** Add tests for `_migrate_old_credential_name` (init_service.py:60–90) — all 3 branches:
  - Early return (line 72–73): legacy file absent → no rename, no warning.
  - Warn + no rename (lines 75–83): both `mediascope.json` and `mediascope_RENAME_.json` exist →
    warning logged, legacy file left in place.
  - Rename (line 85): legacy exists, primary absent → file renamed to `mediascope.json`.
  - Use `tmp_path` + `shutil.copytree` of the settings templates; patch `APP_PATHS`.
- **B3-4:** Add `test_init_force_overwrites_existing_file` — pre-create a file in the user dir,
  call `init_project(force=True)`, assert the file was overwritten (the skip branch at
  init_service.py:53–54 is `not force`).
- **B3-5:** Capture the return value of `init_project()` and assert `result == tmp_path / "settings"`
  (== `APP_PATHS.user_settings_dir`).
- **B3-6:** Bulk-assert all 25 copied template files exist after init (enumerate the bundled
  `settings/` tree; 24 are currently unchecked — only `connections/mediascope.json` is verified).
- **B3-7:** Add `test_copy_file_atomic_cleans_up_temp_on_failure` — mock `shutil.copy2` to raise,
  assert the temp file (`tempfile.mkstemp`) is unlinked and the target is untouched.

### 4.4 Block 4 — rewrite fragile semaphore test + fix thread leak + translate strings + remove decorators (B4-4…B4-11)

- **File:** `tests/test_network.py`
- **B4-5:** Rewrite `test_call_respects_semaphore` (lines 107–134) to be **async-native** — drop
  `threading`/`time.sleep`. Use `asyncio.gather` of N concurrent `client.call(...)` calls against a
  `max_concurrent_requests=2` client; track the peak number of concurrently-acquired semaphore
  slots by incrementing a counter inside the wrapped function and asserting `max(current) <= 2`.
  (Threading + `time.sleep(0.05)` is racy and measures OS scheduling, not semaphore semantics.)
- **B4-6:** Rewrite `test_call_raises_timeout_after_call_timeout` (lines 200–224) — replace the
  `time.sleep(2)` blocking function (which leaks the thread past test end) with a mock that raises
  `TimeoutError`/`asyncio.TimeoutError`. E.g. set `client._call_timeout = 0.5`, `max_attempts=1`,
  and mock `_invoke` to `await asyncio.sleep(2)` then return — `wait_for` still cancels the
  coroutine cleanly (no OS thread left running).
- **B4-7:** Remove the `# noqa: F401` on the `MediascopeApiError` import (line 11) — it is dead
  until G7 is added in Phase 5; removing it now avoids the "imported for documentation" charade.
  (Re-add with a real `G7` test.)
- **B4-8:** Replace Russian literals at lines 236 (`"Слишком много запросов"`) and 253/270
  (`"Ошибка сервера"`) with English (`"Too many requests"`, `"Server error"`). AGENTS.md §1:
  English only.
- **B4-9:** Strip the 14 redundant `@pytest.mark.asyncio` decorators (pyproject.toml sets
  `--asyncio-mode=auto`; see §7.25). Note: Block 7/8 tests already omit them — `test_reports_lifecycle.py`
  uses bare `async def` (the auto mode handles it). Confirm parity.
- **B4-10:** After each retry test, assert on the **patched `asyncio.sleep`** (currently
  `AsyncMock`). Add `mock_sleep = mocker.patch("asyncio.sleep", new_callable=AsyncMock)` then
  `assert mock_sleep.call_count >= max_attempts - 1` and verify backoff growth: for a 3-attempt run,
  capture `sleep.call_args_list` and assert the 2nd delay `> ` the 1st (exponential growth,
  network.py:167 — `backoff = _sleep_time * 1.2**(attempt-1)`).
- **B4-11 (missing coverage — 8 gaps):** Add the focused tests enumerated in §5.1.

### 4.5 Block 5 — strengthen round-trip assertions (B5-2…B5-5)

- **File:** `tests/test_tasks.py`
- **B5-2:** In `test_to_file_writes_valid_json5`, assert **all 8** `dump_settings` keys in the
  round-tripped JSON5: `name`, `report_type`, `report_subtype`, `target`, `error`, `log_error`,
  `interval`, `settings` (currently asserts 5).
- **B5-3:** Add raw-byte assertions for formatting options (`indent=2`, `ensure_ascii=False`,
  `quote_keys=True`, `trailing_commas=False`) — read the file text and assert e.g. `"` quoting of
  keys, no trailing commas, 2-space indentation. (These are normalized away by `json5.load`.)
- **B5-4:** Replace `json5.load(json5_file.open(encoding="utf-8"))` (line 42) with a `with` block
  to prevent the file-handle leak:
  ```python
  with json5_file.open(encoding="utf-8") as f:
      data = json5.load(f)
  ```
- **B5-5:** Audit `tasks.py:38` (`interval = cast(..., list(self.interval))`) — the `cast`/`list()`
  is flagged redundant since `json5.dump` serializes tuples as arrays. This is a **production**
  note (tasks.py:38) for the implementor's backlog; the test does not need to assert on it. Document
  the finding but do not add a test.

### 4.6 Block 8 — fix vacuous/wrong assertions in services tests (B8-1, B8-2, B8-3, B8-5, B8-6, B8-7)

- **File:** `tests/test_services.py`
- **B8-1:** `test_reload_config_invalidates_export_folder` (lines 193–201) — change
  `assert reloaded == original` to `assert reloaded is not original` (the identity check is the
  meaningful one; value-equality is trivially true for an unchanged JSON5). Ideally: mutate
  `app_config.json5` between the two accesses and assert the resolved `export_folder` actually
  changes.
- **B8-2:** Fix `test_run_report_with_real_models` (lines 43–106) — class-level
  `mocker.patch("...NetworkClient.call", side_effect=...)` means **all** `NetworkClient` instances
  in the process share the patch (fragile; `__name__`-based dispatch at line 82 fails on `MagicMock`
  children which have no `__name__`). Rewrite to **inject a mock `NetworkClient`** instance into the
  report instead of patching the class. Use an explicit dispatch on `func.__name__` with a real
  function name (or a `Mock` with `__name__` set). Fix the docstring (line 54 says "only
  MediaVortexTask doubled" — `NetworkClient.call` is also mocked today).
- **B8-3:** Fix `_compute_result_df` (lines 21–40) to include the `frequencyDistInterval` branch
  mirroring `set_output_columns` (reports.py:293–294). Extract a shared helper
  (`_expected_output_columns(...)`) to `conftest.py` — see Phase 6.1.
- **B8-5:** Rename `test_get_default_data_settings_missing_file` →
  `test_bundled_defaults_templates_exist_and_parse` (it tests existence/parsing, not the
  missing-file path). Replace the 6 hardcoded `(media, report_type)` combos (lines 231–238) with
  `itertools.product(MediaType, ReportType)` iterated against real `DataDefaults(**data)`. Replace
  the vacuous `assert defaults.DATA_DEFAULTS is not None` (line 249) with
  `assert "slices" in defaults.DATA_DEFAULTS` (a real structural check).
- **B8-6:** `test_reload_config_reapplies_logging` (lines 186–190) — assert
  `mock_dict_config.assert_called_once_with(app_config_after_reload.logging_settings.model_dump())`,
  i.e. pin the **config dict** passed (not merely the call count).
- **B8-7:** `test_app_config_is_cached` (lines 158–162) — add a content assertion:
  `local_app_service.app_config.app_settings.export_folder == Path("output")` so the test verifies
  app config values, not just that `cached_property` returns the same object.

### 4.7 Block 2 — strengthen `test_period_model_rejects_both` (already partially in 4.2)

Folded into Phase 4.2 above (B2-6).

---

## Phase 5 — Add missing critical coverage (8 new tests + 13 reporting branches)

These close the gaps enumerated in audit §5. Prioritize the Network G-gaps and the
`multiple_files=False` reporting branch (which is the crash path from §4).

### 5.1 Network & retry gaps (8 new tests)

- **File:** `tests/test_network.py`
- Add the 8 gaps from §5.1 of the audit:
  - **G1 `test_backoff_grows_exponentially`** — patch `asyncio.sleep`, trigger
    `ConnectTimeout` over 3 attempts, assert the 2 captured `sleep()` durations satisfy
    `d[1] > d[0]` (network.py:166–169: `backoff = sleep_time * 1.2**(attempt-1)`).
  - **G2 `test_initial_jitter_in_range`** — assert the first `sleep()` before any attempt is in
    `[sleep_time*0.5, sleep_time*2]` (network.py:103).
  - **G3 `test_file_not_found_error_not_retried`** — a func raising `FileNotFoundError` is
    re-raised immediately (call_count == 1); it is not in `RETRIABLE_EXCEPTIONS` (network.py:126–131).
  - **G4 `test_semaphore_released_after_error`** — after a retriable exception exhausts retries,
    acquire the semaphore again (non-blocking) to prove it was released (network.py:106–171).
  - **G5 `test_call_timeout_none_invokes_raw`** — `NetworkClient(call_timeout=None)` wraps `_invoke`
    without `wait_for` (network.py:77–79); assert the func returns its value with no
    `TimeoutError` wrapper (patch `asyncio.wait_for` to assert it is **not** called).
  - **G6 `test_invoke_applies_timeout_and_thread`** — unit-test `_invoke` directly: a blocking func
    is wrapped in `asyncio.to_thread` and bounded by `call_timeout` (network.py:74–79).
  - **G7 `test_non_5xx_mediascope_api_error_raises`** — `MediascopeApiError` with a non-5xx code
    (e.g. 403) is re-raised immediately (network.py:146–156). Re-add the `MediascopeApiError` import
    (removed in 4.7) as a real consumer here.
  - **G8 `test_success_after_transient_retry`** — func raises `ConnectTimeout` once then returns;
    assert `MaxRetriesExceeded` is **not** raised and the success value is returned (network.py:109–171
    partial-recovery path).

### 5.2 Reporting pipeline gaps (13 new tests / branches)

- **File:** `tests/test_reports_lifecycle.py`
- Add focused tests for the 13 uncovered branches in §5.2:
  - **TimeoutError handler** (reports.py:173–180) — covered by the rewritten B7-1 (Phase 3.5).
  - **TVRegTaskStrategy factory** (reports.py:40–41) — assert `ReportFactory(tv_reg_settings).task_strategy`
    is `TVRegTaskStrategy`.
  - **TVRegTaskStrategy full region logic** — non-skip test asserting `company_filter` construction,
    `id==99` skip, and multi-region iteration (Phase 3.3 adds this).
  - **DictDataStrategy factory** (reports.py:47–50) — assert `ReportFactory(dynamics_dict_subtype).data_strategy`
    is `DictDataStrategy`; add error-path tests for missing columns (reports.py:619–623 returns `None`)
    and empty `slices` (reports.py:616 returns `None`).
  - **poll_deadline guard** (reports.py:383–387) — drive `wait_task` with `poll_deadline` set below
    the sleep so the wall-clock branch trips (task.error contains `"Poll deadline exceeded"`).
  - **`send_task` is-False branch** (reports.py:352) — `builder` returns `False`; assert
    `task.status is False` and `task.error == "Task.key is not received from API."` (task never sent).
  - **`extract_data` False/None branches** (reports.py:422–441) — three sub-tests:
    `get_result` returns `False`, `result2table` returns `None`, `prepare_data` returns `None`.
  - **`frequency_dist_conditions`** (reports.py:293–294) — assert `output_columns["statistics"]`
    contains `"frequencyDistInterval"` when `frequency_dist_conditions` is set; assert
    `pivot_df_frequency` is invoked.
  - **`multiple_files=False`** (reports.py:336–338) — the crash path from §4. Add a lifecycle test
    with `multiple_files=False`, assert `to_file` writes a date-serialized error artifact when a
    task fails (regression guard for Phase 0). **[BLOCKED on Phase 0.]**
  - **`last_time` path** (reports.py:309–317) — assert `get_last_period` is called when
    `date_filter is None` (mock `get_available_period` + `get_last_period`).
  - **`wait_task` status-False** (reports.py:394–399) — `get_status` returns `False` (API failure);
    assert `task.error == "Max retries exceeded"` and `task.status is False`.

### 5.3 CLI & orchestrator gaps (10 new tests)

- **File:** `tests/test_app.py` + `tests/test_services.py`
- Add:
  - **`get_app_service` singleton** (services.py:220–227) — first call constructs; second call returns
    the cached `_app_service`. Test in `test_services.py` with mocked `APP_PATHS`.
  - **`prepare_app` filename handler** (services.py:60–64) — a handler dict with `"filename"` key has
    its path resolved and parent created (mock `dictConfig`, assert `handler["filename"]` is a `Path`
    under the resolved base).
  - **Multi-entry `run_report` loop** (services.py:167–178) — a JSON5 with 2 report entries yields 2
    export paths (mock the inner report creation).
  - **`get_report_settings` dict branch** (services.py:92–93) — a JSON5 that parses to a **dict**
    (not a list) is normalized via `list(report_configs.values())`.
  - **Subtype overlay in `merge_data_settings`** (services.py:130–132) — a `DataDefaults` with a
    subtype-named attribute is merged into `data_settings`.
  - **`AppSettings.normalize_path`** (models.py:33–36) — `"~/foo"` expands via `expanduser()`.
  - The 6 CLI tests from Phase 4.1 (B1-4).

---

## Phase 6 — Test infrastructure (cross-cutting, do early to reduce churn)

### 6.1 Consolidate shared helpers into `tests/conftest.py`

- **File:** `tests/conftest.py`
- **Add `hermetic_app_service` fixture** — promote the body of `_build_hermetic_app_service`
  (test_reports_lifecycle.py:59–67) and `local_app_service` (test_services.py:142–155) into a single
  shared fixture. Both are functionally identical (write `log_config.json5` + `app_config.json5` to
  `tmp_path/settings`, copy bundled `defaults/`, construct `AppService`). Consumers: Block 7 (~12
  tests) and Block 8 (~6 tests).
  - **Dependency:** do this *before* refactoring Block 7 and Block 8 tests so both can switch to the
    shared fixture without divergent copies.
- **Add `_expected_output_columns(report)` helper** — single source of truth mirroring
  `set_output_columns` (reports.py:282–296) **including** the `frequencyDistInterval` branch
  (reports.py:293–294). Replace `_build_result_df` (test_reports_lifecycle.py:70–84) and
  `_compute_result_df` (test_services.py:21–40).
  - **Dependency:** fix B7-3/B8-3 (missing `frequencyDistInterval`) by routing both through this
    helper.
- **Optionally add `standard_report_settings` fixture** — a `ReportSettings` built from the
    `tv_json5_content` fixture's entry, providing correct `MediaType`/`ReportType`/`ReportSubtype`
    enum wiring. Reduces the wrong-enum hazard in Block 6 (B6-1, B6-4).

### 6.2 Delete dead infrastructure (after Phase 1 removes consumers)

- **File:** `tests/conftest.py`, `tests/test_network.py`
- Remove `mock_network_client` fixture (Phase 1.2 already covers this).
- Remove the `MediascopeApiError` import (Phase 4.7) — re-added as a real import in the G7 test
  (Phase 5.1, G7).

---

## Phase 7 — Doc-only maintenance (no code)

### 7.1 Maintain PIPE line-reference comments

- **File:** `tests/test_reports.py`
- `test_default_data_strategy_prepare_logs_task_name` (B6-3, line 159) carries a
  `# PIPE-008` comment. If `set_output_columns` or `DefaultDataStrategy.prepare` line numbers shift
  during the Phase 0/5 work, keep the cross-references accurate. No logic change required — verify
  only.

### 7.2 Refresh the AGENTS.md test-command accuracy (informational)

- **File:** `AGENTS.md` / `docs/`
- Confirm the documented test commands (`uv run pytest`, `--asyncio-mode=auto`, `make` wrappers) and
  the `markers: slow` definition remain accurate after the `@pytest.mark.asyncio` cleanup in
  Phase 4.9. (No change expected; verification only.)

---

## Dependency graph

```
Phase 0 [PROD-FIX] (tasks.py date serializer)
  ├─ BLOCKS ─ Phase 3.4  (B5-1 rewrite, date interval)
  └─ BLOCKS ┘ Phase 5.2  (multiple_files=False lifecycle test, date-interval error artifact)

Phase 1 (delete T1/T2/T3/T7 + dead fixture + dead import)
  └─ Phase 6.2 depends on T2 deletion to remove `mock_network_client` fixture

Phase 2 (fix enums in mock_report; delete test_default_task_strategy)
  └─ BLOCKS ─ Phase 3.3 (B6-4 rewrite needs correct enums / enriched fixture)

Phase 1.4 (keep 1 lazy-import test) + Phase 6.2 (dead import removal)
  └─ independent of 2.1 but both feed Phase 3/5 Block-6 correctness

Phase 6.1 (consolidate hermetic_app_service + _expected_output_columns)
  └─ BLOCKS ─ Phase 4.6 (B8-3 _compute_result_df) and any Phase-5 reporting tests that
              build result DataFrames

Phase 3.5 (rewrite B7-1 timeout test)
  └─ BLOCKS ─ Phase 5.2 TimeoutError handler gap (same branch)
```

**Critical ordering constraints (must not be reordered):**

1. **Phase 0 before Phase 3.4 / Phase 5.2 (`multiple_files=False`).** Without the `date` serializer
   fix, the rewritten `test_to_file_writes_valid_json5` crashes and the new lifecycle test for
   `multiple_files=False` cannot pass.
2. **Phase 1.1 (delete T2) before Phase 6.2 (remove `mock_network_client` fixture).** Removing the
   fixture while T2 still references it breaks collection.
3. **Phase 2.1 (enrich `mock_report` + correct enums) before Phase 3.3.** The B6-4 rewrite depends on
   the corrected fixture/enums.
4. **Phase 6.1 (consolidate helpers) before Phase 4.6 (B8-3) and Phase 5.2 reporting-dataframe
   tests.** Both `_build_result_df` and `_compute_result_df` must route through the shared
   `_expected_output_columns` to pick up `frequencyDistInterval`.
5. **Phase 2.2 (delete `test_default_task_strategy`) can run in parallel with Phase 3.1 (B1-1
   rewrite)** — no shared dependencies.

---

## Recommended implementation order (prioritized)

| Priority | Phase | Item | File(s) | Rationale (4-factor) |
|----------|-------|------|---------|----------------------|
| **1** | 0 | `[PROD-FIX]` date serializer | `src/.../tasks.py` | **Correctness 10/10** — prevents silent failure masking; blocks 2 test items |
| **2** | 1 | Delete T1/T2/T3/T7 + dead import + `mock_network_client` fixture | `tests/test_network.py`, `tests/conftest.py` | **Regression risk 9/10** — removes 8–26s real sleep + false-confidence retry test; low effort |
| **3** | 1.4 | Delete 2 redundant lazy-import tests | `tests/test_app.py` | Correctness 3/10, effort 1/10 |
| **4** | 6.1 | Consolidate `hermetic_app_service` + `_expected_output_columns` | `tests/conftest.py` | Unblocks Phase 4.6 + 5.2; medium effort but reduces churn across 18 tests |
| **5** | 3.1 | Rewrite B1-1 (sys.modules assertion) | `tests/test_app.py` | False-confidence removal, low effort |
| **6** | 4.4 | Rewrite fragile semaphore test + fix thread leak | `tests/test_network.py` | Regression risk (thread leak) 6/10; medium effort |
| **7** | 2.1–2.2 | Fix enums in `mock_report` + delete `test_default_task_strategy` | `tests/test_reports.py` | Correctness 8/10 (wrong-enum hazard); prerequisite for 3.3 |
| **8** | 4.2 | Fix kitId regex + magic number + remove en_to_ru spy | `tests/test_models.py` | Test quality (23 kitIds were silently unchecked), low effort |
| **9** | 4.3 | Strengthen credential test + add init_service branch coverage | `tests/test_init_credentials.py` | Coverage gap (init_service untested), medium effort |
| **10** | 3.3 | Rewrite B6-4 with correct enums + non-skip test | `tests/test_reports.py` | Depends on #7; closes TVReg region-logic gap |
| **11** | 4.5 | Strengthen tasks round-trip (8 keys, formatting, `with` block) | `tests/test_tasks.py` | Low effort, improves precision |
| **12** | 3.4 | **Conditional:** rewrite B5-1 (date interval) | `tests/test_tasks.py` | **Requires #1 (Phase 0) confirmed** |
| **13** | 4.1 | Split/strengthen CLI init test + 6 untested CLI paths | `tests/test_app.py` | Covers B1-4 untested public API |
| **14** | 4.11 | Fix B8-1/B8-2/B8-5/B8-6/B8-7 assertions | `tests/test_services.py` | Test-quality (vacuous assertions), medium effort |
| **15** | 3.5 | Rewrite B7-1 timeout test (assert payload) | `tests/test_reports_lifecycle.py` | Correctness 8/10 (misleading name); depends on #4 |
| **16** | 4.9 | Translate Russian strings + strip `@pytest.mark.asyncio` | `tests/test_network.py` | Style (English-only rule), low effort, mechanical |
| **17** | 4.10 | Add backoff assertions to existing retry tests | `tests/test_network.py` | Closes B4-10 (backoff timing unverified), low effort |
| **18** | 5.1 | Add 8 Network G-gap tests | `tests/test_network.py` | Coverage 9/10, medium effort |
| **19** | 5.3 | Add CLI/orchestrator gap tests (singleton, multi-entry, etc.) | `tests/test_app.py`, `tests/test_services.py` | Coverage 8/10, medium effort |
| **20** | 5.2 | Add reporting branch tests (13 gaps) | `tests/test_reports_lifecycle.py` | Depends on #4 (shared helper); coverage 9/10; `multiple_files=False` sub-item blocked on #1 |
| **21** | 7.1 | PIPE line-reference comment audit | `tests/test_reports.py` | Doc only, trivial |
| **22** | 7.2 | Command-reference accuracy check | `AGENTS.md`, `docs/` | Doc only, trivial |

> Items marked with a single `|` in the table above (16–22) are lower-priority polish/coverage and
> may be deferred to a follow-up sprint without blocking the correctness fixes (1–15). The
> **[PROD-FIX] (#1)**, the **test-deletion/slow-sleep removal (#2)**, and the **wrong-enum +
> vacuous-assertion fixes (#3, 7, 11)** are the highest-value, lowest-regret changes.

---

## Validation checkpoint after each phase

Run from the project root (`C:\py_dev\mko_get_mediascope_data`) using the canonical toolchain
(`uv run`; see AGENTS.md).

| After phase | Command | Gate |
|-------------|---------|------|
| **Phase 0** (prod fix) | `uv run ruff check src tests` + `uv run mypy src/mko_get_mediascope_data/core/tasks.py` | Lint + typecheck clean; revert-fix repro shows `TypeError` |
| **Phase 1** (deletions) | `uv run pytest tests/test_network.py tests/test_app.py tests/test_init_credentials.py -q` | Same 69→(69−5) count; no collection errors from removed fixture |
| **Phase 1.2** (fixture removal) | `uv run pytest tests/ --collect-only -q \| rg mock_network_client` | **Zero** references to `mock_network_client` remain |
| **Phase 2** (enums) | `uv run pytest tests/test_reports.py -q` | All remaining Block 6 tests pass with corrected enums |
| **Phase 3** (rewrites) | `uv run pytest tests/test_app.py tests/test_tasks.py tests/test_reports.py tests/test_reports_lifecycle.py -q` | Rewrites pass; B5-1 fails-without-fix (Phase 0) — verify this **before** the fix |
| **Phase 4** (updates) | `uv run pytest tests/test_models.py tests/test_init_credentials.py tests/test_network.py -q` | Updated assertions hold; Russian strings gone; no `@pytest.mark.asyncio` in test_network.py |
| **Phase 5** (new coverage) | `uv run pytest tests/ -q` | Full suite green; coverage gate for `network.py`/`reports.py` improved (informational) |
| **Phase 6** (infra) | `uv run pytest tests/ --collect-only -q` | No duplicate-symbol / fixture collisions from conftest promotion |
| **Full gate** (final) | `uv run ruff check src tests` + `uv run mypy src tests` + `uv run pytest` | All three green (mirrors CI in `.github/workflows/ci.yml`) |

**Mandatory before merge:** `uv run pytest` (full suite) — CI runs
`uv run ruff check src tests` then `uv run pytest` (`.github/workflows/ci.yml`). A
`--cov` report is informational only (no threshold configured in `pyproject.toml`).

### Per-change verification of the [PROD-FIX] regression guard

- Before applying Phase 0: confirm the **rewritten** `test_to_file_writes_valid_json5` (date-typed
  interval) **fails** with `TypeError: Object of type date is not JSON serializable` — this proves
  the test can catch the bug.
- After applying Phase 0: confirm the same test **passes** and that a revert of the fix makes it fail
  again. This is the only end-to-end regression guard for the silent-failure-masking bug in §4.

---

## Risk register

| Risk | Likelihood | Impact | Mitigation in plan |
|------|-----------|--------|--------------------|
| Date-serialization crash still latent after rewrite (B5-1 passes on string interval) | High (pre-fix) | Critical — silent task-failure masking | Phase 3.4 explicitly requires `date`-typed interval; Phase 0 fix + revert-test before merge |
| Removing `mock_network_client` fixture breaks an unknown consumer | Low (verified: only T2) | Medium — collection error | Phase 1.2 gate: grep for `mock_network_client` in `tests/` returns 0 before removal |
| Backdrop test (B7-1) still doesn't hit the `TimeoutError` branch after rewrite | Medium | High — misleading coverage | Phase 3.5 asserts error payload contains `"Timeout after"` (the `except TimeoutError` message at reports.py:179) |
| Shared `hermetic_app_service` fixture diverges from one of its sources | Low | Medium — subtle env difference | Phase 6.1: diff the two originals; keep the fixture identical to the current `local_app_service` (test_services.py:142–155) which is the more recently-maintained copy |
| `_expected_output_columns` still omits `frequencyDistInterval` | Medium | Medium — `KeyError` under frequency templates | Phase 6.1: mirror reports.py:293–294 exactly; add a unit test asserting `frequencyDistInterval` appears when `frequency_dist_conditions` is set |
| Wrong-enum hazard recurs in new Block 6 tests | Medium | High — tests pass on broken impl | Phase 2.1: use the `enriched mock_report` fixture + `standard_report_settings`; Phase 2.2 deletes the worst offender |
| `asyncio.sleep` mock assertions are brittle if backoff formula changes | Low | Low — false positives on refactor | Phase 4.10 / 5.1(G1): assert *growth* (`d[1] > d[0]`) not exact values; pin only the growth direction |

---

## Summary of scope (what changes, by file)

| File | Delete | Rewrite | Update | Add |
|------|--------|---------|--------|-----|
| `tests/test_app.py` | 2 lazy-import tests | B1-1 bootstrap | B1-2 split CLI init | 6 CLI/public-API tests |
| `tests/test_models.py` | — | — | kitId regex, magic num, en_to_ru spy, B2-5/2-6, B2-7 gaps | 6 model unit tests |
| `tests/test_init_credentials.py` | redundant `exists()` (L22) | — | credential parse (B3-1), return value (B3-5), bulk files (B3-6) | `_migrate` (3 branches), `_copy_file_atomic` (B3-7), `force=True` (B3-4) |
| `tests/test_network.py` | T1/T2/T3/T7 + dead import | semaphore (T6), T11 thread leak | Russian strings, `@pytest.mark.asyncio`, backoff asserts | 8 Network G-gaps |
| `tests/test_tasks.py` | — | B5-1 (date interval) | 8-key round-trip, formatting, `with` block (B5-2–5) | — |
| `tests/test_reports.py` | `test_default_task_strategy` | B6-2 dict→Data, B6-4 TV_REG enums | enrich `mock_report` fixture | TVReg non-skip test |
| `tests/test_reports_lifecycle.py` | — | B7-1 timeout payload | — | 12 reporting-branch tests (1 blocked) |
| `tests/test_services.py` | — | — | B8-1/2/3/5/6/7 assertions | 4 orchestrator gap tests |
| `tests/conftest.py` | `mock_network_client` fixture | — | — | `hermetic_app_service`, `_expected_output_columns`, (opt.) `standard_report_settings` |

> **Production code touched:** only the Phase 0 `[PROD-FIX]` in `src/mko_get_mediascope_data/core/tasks.py`
> (the `json5.dump` `default=` serializer). **No other production file is modified by this plan.**
