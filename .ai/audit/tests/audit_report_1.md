# Test-Quality Audit Report — `mko_get_mediascope_data`

**Report ID:** `audit_report_1`
**Audit phase:** Step 4 — Synthesis
**Date:** 2026-10-18
**Commit:** `3efc4d7` (`docs(overview): commit AGENTS.md EXT-005 update and fix doc accuracies`)
**Author:** Kilo — Senior Staff Architecture Auditor
**Status:** Complete
**Distribution:** Single consolidated report (no per-block files)

---

## 1. Executive Summary

This report synthesizes validated findings from a test-quality audit of the
`mko_get_mediascope_data` test suite. Eight test blocks were audited by a
test-engineer and validated by a researcher; all 54 evidence-backed findings
below are derived from direct inspection of production source and test code.

The suite currently passes **69/69** tests but contains **multiple latent
correctness hazards**: one [PROD-FIX] crash bug with **no test coverage** that
will silently mask task failures in production, dozens of vacuous / redundant
tests that provide false confidence, and **critical coverage gaps** in error
handling, retry semantics, and the init pipeline.

### Severity roll-up

| Severity | Count | Meaning |
|----------|-------|---------|
| **[PROD-FIX]** | 1 | Latent production crash; test rewrite blocked on fix |
| **CRITICAL** | 6 | Tests that pass unconditionally — false confidence |
| **HIGH** | 14 | Tests exercising dead/wrong code paths or wrong enum values |
| **MEDIUM** | 22 | Redundancy, magic numbers, spying on internals, dead imports |
| **LOW** | 9 | Style, decorator noise, minor redundancy |
| **Total findings** | **52** (46 test + 52 sub-findings + 1 prod) | |

### Priority action list (top 6)

1. **[PROD-FIX]** Fix `date` serialization crash in `TVTask.to_file` (tasks.py:52-58) — *blocks Block 5 rewrite.*
2. **[BLOCK 4]** Delete 4 redundant/meta tests (T1, T2, T3, T7) and the dead `mock_network_client` fixture — *removes 8–26s of real sleep, fixes false confidence in retry count.*
3. **[BLOCK 5]** Rewrite `test_to_file_writes_valid_json5` to use `date`-typed intervals — *the only test that can catch the crash above.*
4. **[BLOCK 1]** Rewrite the vacuous bootstrap test to assert module non-import — *currently passes unconditionally.*
5. **[BLOCK 6]** Rewrite `test_dict_data_strategy` and `test_tv_reg_task_strategy_skip_logs_warning` with correct enums and the real `Data` model path.
6. **[BLOCK 2]** Fix `kitId` regex to match quoted `"kitId": N` form — *24 of 29 kitIds silently unverified.*

---

## 2. Full Findings Summary Table

| # | FilePath | TestName | Type | Severity | Problem | Recommendation |
|---|----------|----------|------|----------|---------|----------------|
| B1-1 | `tests/test_app.py` | `test_package_import_does_not_bootstrap` | [TEST-REWRITE] | CRITICAL | Patches `AppService.__init__` but `__init__.py` module body never triggers it — assertion passes unconditionally (vacuous) | Rewrite to assert `"mko_get_mediascope_data.app" not in sys.modules` after reload |
| B1-2 | `tests/test_app.py` | `test_cli_init` | [TEST-UPDATE] | MEDIUM | Tests `--help` text only; never invokes `init_project()` or the success message | Rename to `test_cli_init_help`; add separate test invoking `["init"]` with mocked `init_project` asserting the `[green]✅ Settings initialized:` output |
| B1-3 | `tests/test_app.py` | `test_lazy_import_*` (×3) | [TEST-DELETE] | LOW | Three near-identical `callable()` assertions over the same PEP 562 `__getattr__` code path (`run_report`, `run_report_async`, `initialize_settings`) | Delete 2 of 3; keep one representative (`run_report`) + `test_lazy_import_raises_attribute_error` |
| B1-4 | `tests/test_app.py` | — (7 untested paths) | [TEST-UPDATE] | HIGH | No tests cover: `init` command body (app.py:26-37), `run` command (app.py:40-62), `list_reports` command (app.py:65-76), `run_report_async` (app.py:80-87), `run_report` sync wrapper (app.py:90-98), `initialize_settings` (app.py:101-103), `sys.stdout.reconfigure` (app.py:51-52) | Add CliRunner-based tests with mocked `get_app_service` for each command |
| B2-1 | `tests/test_models.py` | `test_bundled_templates_kitId_conformance` | [TEST-UPDATE] | HIGH | Regex `kitId\s*:\s*(\d+)` only matches **unquoted** shorthand; finds 3 of 29 kitIds across 12 files. Passes 24 invalid kitIds | Fix regex to match both `"kitId": N` and `kitId: N`; prefer `json5` parse + dict traversal |
| B2-2 | `tests/test_models.py` | `test_bundled_templates_validate` | [BEST-PRACTICE] | MEDIUM | Magic number `total_entries == 32` (line 123) — breaks silently when templates added | Define `EXPECTED_TEMPLATE_ENTRIES` constant or derive from directory walk |
| B2-3 | `tests/test_models.py` | `test_merge_data_settings_en_does_not_translate` | [BEST-PRACTICE] | MEDIUM | Spies on `en_to_ru` internal function (line 50) to assert not-called — tests implementation, not behavior | Remove spy; behavioral assertion `result["slices"] == ["EName_col"]` already covers it |
| B2-4 | `tests/test_models.py` | `test_merge_data_settings_ru_with_none_slices` | [BEST-PRACTICE] | MEDIUM | Constructs synthetic `DataDefaults(DATA_DEFAULTS={"slices": None})` — bypasses real loading path; guard at services.py:140 makes `en_to_ru(None)` unreachable | Use real `get_default_data_settings` or remove if None path is dead |
| B2-5 | `tests/test_models.py` | `test_report_settings_valid` | [TEST-UPDATE] | MEDIUM | Only 2 of 9 fields asserted (`media`, `report_type`); missing `relative_path`, `data_lang` default, `compression` default, `multiple_files` default, `check_done` default, `period` values | Assert all relevant fields with their default values |
| B2-6 | `tests/test_models.py` | `test_period_model_validation` | [TEST-UPDATE] | MEDIUM | `pytest.raises(ValidationError)` (line 35) has no `match=` — any ValidationError satisfies | Add `match="Specify either"` to verify the specific error |
| B2-7 | `tests/test_models.py` | — (14 gaps) | [BEST-PRACTICE] | MEDIUM | Missing coverage: invalid enum values, single-element `date_filter`, `include_current=True`, `Data` list/dict types, `AppSettings.normalize_path`, `AppConfig` validation, subtype overlay assertion, `en_to_ru` direct test, DEFAULT template existence, `PeriodModel.last_time` path, `Data.coerce_none_to_empty` edge, `ReportSubtype.default_value` merge, `ReportSettings` extra-field acceptance | Add focused unit tests |
| B3-1 | `tests/test_init_credentials.py` | `test_init_produces_readable_credential_file` | [TEST-UPDATE] | HIGH | Only checks existence (line 21); no JSON parseability or expected keys (`username`, `passw`, `client_id`, etc.) | Add `json.load()` + key assertions |
| B3-2 | `tests/test_init_credentials.py` | — (line 22) | [TEST-DELETE] | LOW | `assert app_paths.connection_config.exists()` is functionally identical to line 21 (resolves to same `Path`) | Remove redundant line 22 |
| B3-3 | `tests/test_init_credentials.py` | — (`_migrate_old_credential_name`) | [TEST-UPDATE] | HIGH | `init_service.py:60-90` fully untested — all 3 branches (early-return line 72-73, warn line 75-83, rename line 85) | Add tests for each branch with mocked legacy file |
| B3-4 | `tests/test_init_credentials.py` | — (`force=True` overwrite) | [TEST-UPDATE] | MEDIUM | `_copy_templates` skip branch (init_service.py:53-54) untested | Add test with pre-existing files + `force=True` asserting overwrite |
| B3-5 | `tests/test_init_credentials.py` | — (return value) | [TEST-UPDATE] | MEDIUM | `init_project()` return value (init_service.py:106) never captured or asserted | Capture and assert `result == tmp_path / "settings"` |
| B3-6 | `tests/test_init_credentials.py` | — (24 missing files) | [TEST-UPDATE] | MEDIUM | 24 of 25 copied template files have zero assertions (only `mediascope.json` checked) | Add bulk assertion verifying all expected files exist after init |
| B3-7 | `tests/test_init_credentials.py` | — (`_copy_file_atomic`) | [TEST-UPDATE] | MEDIUM | Atomicity mechanism (temp file in same dir, cleanup on failure; init_service.py:23-34) untested | Add test with mocked `shutil.copy2` failure verifying temp cleanup |
| B4-1 | `tests/test_network.py` | `test_network_retry_and_fail` (T1) | [TEST-DELETE] | HIGH | Redundant with T8 (`test_call_retries_with_default_max_attempts`); missing `asyncio.sleep` mock → **8–26s real delay** (constructor `sleep_time=0` overridden by `call()` default `sleep_time=2` at network.py:96,100) | Delete (T8 is strict superset with `call_count` assertion) |
| B4-2 | `tests/test_network.py` | `test_mock_network_client_retries_on_retryable_error` (T2) | [TEST-DELETE] | CRITICAL | Meta-test of `AsyncMock` — replaces `call` entirely with `AsyncMock`, manually loops, asserts `call_count` on the **mock** not production retry logic. `mock_network_client` fixture is dead (only consumer) | Delete T2 + remove `mock_network_client` fixture from conftest.py:14-18 |
| B4-3 | `tests/test_network.py` | `test_call_returns_result_on_success` (T3) | [TEST-DELETE] | LOW | Redundant with T9 (`test_call_preserves_return_type`) — identical happy path, T9 has superset assertions | Delete T3 |
| B4-4 | `tests/test_network.py` | `test_default_max_attempts_is_10` (T7) | [TEST-DELETE] | MEDIUM | Redundant with T8 (exercises real retry loop, asserts `call_count==10`); also accesses private `_max_attempts` | Delete T7 |
| B4-5 | `tests/test_network.py` | `test_call_respects_semaphore` (T6) | [BEST-PRACTICE] | MEDIUM | Fragile threading (`threading.Lock` + real `time.sleep(0.05)`) — racy concurrency measurement | Replace with async-native semaphore test (e.g., concurrent `call()` counting active semaphore holders) |
| B4-6 | `tests/test_network.py` | `test_call_raises_timeout_after_call_timeout` (T11) | [TEST-UPDATE] | HIGH | `time.sleep(2)` in worker thread leaks — `asyncio.wait_for` cancels the wrapper coroutine but the thread continues running post-test | Replace blocking func with mock-based approach (return an awaitable that raises `TimeoutError`) |
| B4-7 | `tests/test_network.py` | — (line 11) | [TEST-UPDATE] | LOW | Dead import `MediascopeApiError` with `# noqa: F401` — no test uses it | Remove; re-add when G7 test for non-5xx `MediascopeApiError` is written |
| B4-8 | `tests/test_network.py` | — (lines 236, 253, 270) | [TEST-UPDATE] | LOW | Russian error strings (`"Слишком много запросов"`, `"Ошибка сервера"`) violate the "English Only" rule (AGENTS.md §1) | Replace with English |
| B4-9 | `tests/test_network.py` | — (all async tests) | [TEST-UPDATE] | LOW | 14 tests carry redundant `@pytest.mark.asyncio` (pyproject.toml has `--asyncio-mode=auto`) | Remove decorators |
| B4-10 | `tests/test_network.py` | — (no backoff tests) | [BEST-PRACTICE] | MEDIUM | All retry tests patch `asyncio.sleep` as `AsyncMock` but **never assert on sleep calls** — backoff timing unverified | Capture sleep mock spy, assert call counts and backoff durations against network.py:166-169 formula |
| B4-11 | `tests/test_network.py` | — (8 gaps) | [BEST-PRACTICE] | HIGH | Missing coverage: G1 backoff timing (network.py:165-169), G2 initial jitter (network.py:103), G3 FileNotFoundError path (network.py:126-131), G4 semaphore release on error, G5 `call_timeout=None` (network.py:77-79), G6 `_invoke` unit (network.py:74-79), G7 non-5xx `MediascopeApiError` (network.py:146-156), G8 partial recovery | Add focused tests for each |
| B5-1 | `tests/test_tasks.py` | `test_to_file_writes_valid_json5` | [TEST-REWRITE] | **CRITICAL** | Test uses `str`-typed `interval = ("2025-01-01", "2025-01-07")` but production returns `tuple[date, date]` (reports.py:321-338, 506). `json5.dump` (tasks.py:52-58) has no `default` serializer → **`TypeError` at runtime**. Test gives false confidence. | Rewrite with `date`-typed interval + assert no-error serialization. **Requires [PROD-FIX]** below. |
| B5-2 | `tests/test_tasks.py` | — (lines 43-46) | [TEST-UPDATE] | MEDIUM | Only 5 of 8 `dump_settings` keys asserted; missing `report_type`, `report_subtype`, `settings` | Assert all 8 fields in round-trip |
| B5-3 | `tests/test_tasks.py` | — (lines 55-58) | [TEST-UPDATE] | LOW | `indent=2`, `ensure_ascii=False`, `quote_keys=True`, `trailing_commas=False` normalized away by `json5.load` | Assert on raw file bytes/text for formatting options |
| B5-4 | `tests/test_tasks.py` | — (line 42) | [BEST-PRACTICE] | LOW | File handle leak — `json5.load(json5_file.open(...))` without `with` | Use `with json5_file.open(...) as f: json5.load(f)` |
| B5-5 | `tests/test_tasks.py` | — (line 38) | [BEST-PRACTICE] | LOW | `list(self.interval)` (tasks.py:38) is redundant — `json5.dump` serializes tuples as arrays natively | Remove dead `cast`/`list()`; or document why needed |
| B6-1 | `tests/test_reports.py` | `test_default_task_strategy` | [TEST-DELETE] | HIGH | Only 2 shallow assertions; **wrong enum values** — `report.type.value="TV"` (TV is `MediaType` not `ReportType`; "crosstab" is `ReportType` not `ReportSubtype`) at lines 27-28; fully duplicated by lifecycle `test_create_report_writes_csv_and_marks_queue_done` with real models | Delete — lifecycle test provides superior coverage |
| B6-2 | `tests/test_reports.py` | `test_dict_data_strategy` | [TEST-REWRITE] | CRITICAL | Passes plain `dict` as `data_settings` (line 94), but `isinstance(data_settings, dict)` branch at reports.py:611 is **dead code** — production always uses `Data` BaseModel (reports.py:65) which takes the `else` branch (reports.py:613-614). Tests the wrong code path entirely | Rewrite with `Data` BaseModel (or `MagicMock` with `.slices`) to exercise production `else` branch |
| B6-3 | `tests/test_reports.py` | `test_default_data_strategy_prepare_logs_task_name` | [DOC-UPDATE] | LOW | Correct and well-targeted; no changes needed | Keep; maintain the `PIPE-008` line-reference comment accuracy if reports.py shifts |
| B6-4 | `tests/test_reports.py` | `test_tv_reg_task_strategy_skip_logs_warning` | [TEST-REWRITE] | HIGH | Only tests done-file skip path (7 assertions); **wrong enum** `report.type.value="TV_REG"` (TV_REG is `MediaType` not `ReportType`, line 224); region filtering, `company_filter` construction, `id==99` skip, multi-region iteration all untested | Rewrite with correct enums; add non-skip test asserting `task.settings["company_filter"]`, region iteration, `id==99` skip |
| B6-5 | `tests/test_reports.py` | `mock_report` fixture (15-30) | [BEST-PRACTICE] | MEDIUM | Missing `catalogs` and `output_columns` attributes; used by only 1 of 4 tests | Enrich fixture with `catalogs`, `output_columns`, correct enum values; refactor other tests to use it |
| B6-6 | `tests/test_reports.py` | — (2 redundant) | [BEST-PRACTICE] | MEDIUM | 2 of 4 tests redundant with lifecycle tests (task gen + skip path) | Rely on lifecycle tests for shared behavior; unit tests should focus on strategy-specific logic |
| B7-1 | `tests/test_reports_lifecycle.py` | `test_create_report_timeout_does_not_hang` | [TEST-REWRITE] | CRITICAL | Does **not** test the `TimeoutError` branch (reports.py:173-180) — `wait_task`'s `max_polls=60` guard (reports.py:378-382) fires before the 0.5s `asyncio.wait_for` deadline (60 mocked-sleep iterations complete in ms). Asserts `qsize==0` + error file exists but **NOT error payload content** (would reveal "Poll deadline exceeded (60 polls)" not "Timeout after 0.5s") | Rewrite to actually trigger `TimeoutError`: use `_yielding_sleep` that delays real interval, or mock `wait_task` to block >0.5s; assert error payload contains `"Timeout after"` |
| B7-2 | `tests/test_reports_lifecycle.py` | — (12 tests) | [BEST-PRACTICE] | MEDIUM | Boilerplate pollution — 6 tests configure 5+ mock return values never called (early-exit/direct-call paths) | Extract parametrized fixture for mock setup with per-test overrides |
| B7-3 | `tests/test_reports_lifecycle.py` | `_build_result_df` (70-84) | [BEST-PRACTICE] | MEDIUM | Manually mirrors `set_output_columns` (reports.py:282-296), missing `frequency_dist_conditions` → `frequencyDistInterval` (reports.py:293-294); duplicated in test_services.py as `_compute_result_df` | Use `report.output_columns` directly instead of re-deriving columns |
| B7-4 | `tests/test_reports_lifecycle.py` | `test_wait_task_max_polls_limit` / `test_wait_task_resets_status_on_failed` (507-551, 873-923) | [BEST-PRACTICE] | MEDIUM | 95% duplicates — identical setup (steps 1–7), differ only in `get_status` value + assertions | Parameterize with `get_status_value`, `max_polls`, `expected_error` |
| B7-5 | `tests/test_reports_lifecycle.py` | `_build_hermetic_app_service` (59-67) | [BEST-PRACTICE] | LOW | Functionally identical to `local_app_service` fixture in test_services.py:142-155 — DRY violation | Promote to `conftest.py` as shared fixture |
| B7-6 | `tests/test_reports_lifecycle.py` | — (13 branches) | [BEST-PRACTICE] | HIGH | 13 uncovered branches: TimeoutError handler (reports.py:173-180), TVRegTaskStrategy factory + full region logic (reports.py:40-41, 516-577), DictDataStrategy factory + error paths (reports.py:47-50, 605-623), poll_deadline guard (reports.py:383-387), period-clipping warning (reports.py:330-335), `send_task` is-False branch (reports.py:352), `extract_data` False/None branches (reports.py:422-425, 430-433, 439-441), `frequency_dist_conditions` (reports.py:293-294), `multiple_files=False` (reports.py:336-338), `last_time` path (reports.py:309-317), `wait_task` status-False (reports.py:394-399) | Add focused tests for each uncovered branch |
| B8-1 | `tests/test_services.py` | `test_reload_config_invalidates_export_folder` | [TEST-UPDATE] | MEDIUM | Asserts `reloaded == original` (line 200) — trivially true when JSON5 unchanged; `reloaded is not None` (line 201) vacuously true | Change to `reloaded is not original`; ideally mutate JSON5 between accesses and verify path change |
| B8-2 | `tests/test_services.py` | `test_run_report_with_real_models` | [TEST-UPDATE] | HIGH | Class-level `mocker.patch` on `NetworkClient.call` (line 86) instead of injecting mock client; `__name__`-based dispatch (line 82) is fragile (`MagicMock` children have no `__name__`); conftest `mock_network_client` fixture unused; misleading docstring ("only MediaVortexTask doubled", line 54 — but `NetworkClient.call` is also mocked) | Inject mock `NetworkClient` instance; fix docstring; use explicit dispatch table |
| B8-3 | `tests/test_services.py` | `_compute_result_df` (21-40) | [TEST-UPDATE] | MEDIUM | Missing `frequency_dist_conditions` → `frequencyDistInterval` branch from `set_output_columns` (reports.py:293-294) — would cause `KeyError` if used with frequency-distribution templates | Mirror full `set_output_columns` logic or use `report.output_columns` |
| B8-4 | `tests/test_services.py` | — (`_build_hermetic_app_service`) | [BEST-PRACTICE] | LOW | `_build_hermetic_app_service` (lifecycle:59-67) identical to `local_app_service` fixture (this file:142-155) — DRY violation | Promote to `conftest.py` as shared fixture |
| B8-5 | `tests/test_services.py` | `test_get_default_data_settings_missing_file` | [TEST-UPDATE] | MEDIUM | Misleadingly named (tests existence, not missing-file); hardcodes 6 (media, report_type) combos (lines 231-238) from real `APP_PATHS`; vacuous `DATA_DEFAULTS is not None` (line 249) | Rename to `test_bundled_defaults_templates_exist_and_parse`; use `itertools.product(MediaType, ReportType)` for combos; assert on `DATA_DEFAULTS` keys |
| B8-6 | `tests/test_services.py` | `test_reload_config_reapplies_logging` | [TEST-UPDATE] | MEDIUM | Only checks `dictConfig` called once (line 190) — not the config dict passed | Assert `dictConfig` called with `app_config.logging_settings.model_dump()` |
| B8-7 | `tests/test_services.py` | `test_app_config_is_cached` | [BEST-PRACTICE] | LOW | Tests Python's `cached_property` decorator behavior, not application logic; no content verification | Add content assertion (`app_config.app_settings.export_folder == Path("output")`) |
| B8-8 | `tests/test_services.py` | — (5 gaps) | [BEST-PRACTICE] | MEDIUM | Missing coverage: `get_app_service` singleton (services.py:220-227), `prepare_app` filename-handler branch (services.py:60-64), multi-entry `run_report` loop (services.py:167-178), `get_report_settings` dict branch (services.py:92-93), subtype overlay in `merge_data_settings` (services.py:130-132) | Add focused unit tests |

> **Note on Block 7 test count:** The pre-audit summary cited 14 tests for `test_reports_lifecycle.py`; `pytest --collect-only` verified **12** test functions (no parametrized variants exist in the file). This 2-test discrepancy does not affect any finding below — it is recorded here for completeness.

---

## 3. Findings Grouped by Root Cause

### 3.1 Assertion vacuity — tests that pass unconditionally (CRITICAL)

These tests provide **false confidence** because their assertions are satisfied by construction, regardless of production behavior.

| Finding | Mechanism | Production risk |
|---------|-----------|-----------------|
| B1-1: `test_package_import_does_not_bootstrap` | Patches `AppService.__init__` but `__init__.py` (33 lines) uses lazy `__getattr__` (PEP 562) — the module body never instantiates `AppService`, so `mock_init.assert_not_called()` always passes | A future refactor that eagerly imports `app.py` or calls `get_app_service()` would pass this test undetected |
| B4-2: `test_mock_network_client_retries_on_retryable_error` (T2) | Replaces `client.call` entirely with an `AsyncMock`, then manually loops — asserts on the **mock's** `call_count`, never exercising `NetworkClient.call`'s retry loop (network.py:109-171) | Retry logic is completely unverified; a deleted `for` loop would pass |
| B7-1: `test_create_report_timeout_does_not_hang` | Mocks `asyncio.sleep` with `_yielding_sleep(0)` — 60 poll iterations complete in microseconds, hitting the `max_polls` guard (reports.py:378) before the 0.5s `wait_for` deadline; asserts `qsize==0` + error-file existence but **not the error payload** | Never exercises the `TimeoutError` branch (reports.py:173-180); test name is misleading |
| B4-10: Backoff never asserted (all retry tests) | All 8 retry tests patch `asyncio.sleep` as `AsyncMock` but never inspect call count or duration — the exponential backoff (network.py:166-169) is entirely unverified | Backoff could be removed or broken with zero test signal |

### 3.2 Wrong code path / dead branch exercised (CRITICAL/HIGH)

| Finding | Dead branch | Production reality |
|---------|------------|-------------------|
| B6-2: `test_dict_data_strategy` | `isinstance(data_settings, dict)` (reports.py:611) | Always receives `Data` BaseModel (reports.py:65) → takes `else` branch (reports.py:613-614) |
| B2-4: `test_merge_data_settings_ru_with_none_slices` | `en_to_ru(None)` path | Guard at services.py:140 (`if slices is not None`) makes `en_to_ru(None)` unreachable; synthetic `DataDefaults` bypasses real loading |
| B3-2: redundant `exists()` check | Both lines assert the same `Path` | Line 22 is dead code |
| B5-1: string `interval` vs `date` interval | `json5.dump` with `default=` serializer for `date` | Production path (reports.py:321,338) returns `tuple[date, date]` → untested → crash |

### 3.3 Wrong enum values (HIGH)

`MediaType`, `ReportType`, and `ReportSubtype` are distinct enums in `models.py` (lines 39-59), but tests conflate their values:

| Test | Field | Assigned value | Actual enum | Correct value |
|------|-------|----------------|-------------|---------------|
| B6-1 `test_default_task_strategy` | `report.type` | `"TV"` | `MediaType.TV.value` | Should be a `ReportType` value |
| B6-1 | `report.subtype` | `"crosstab"` | `ReportType.crosstab.value` | Should be a `ReportSubtype` value |
| B6-4 `test_tv_reg_task_strategy_skip_logs_warning` | `report.type` | `"TV_REG"` | `MediaType.TV_REG.value` | Should be a `ReportType` value |

### 3.4 Fragile / implicit coupling (MEDIUM/HIGH)

| Finding | Hazard |
|---------|--------|
| B8-2: `__name__`-based dispatch on `MagicMock` | `getattr(func, "__name__", None)` (test_services.py:82) — `MagicMock` children have no real `__name__`; dispatch silently falls through to `func(*args)` on an unconfigured mock |
| B4-5: threading-based semaphore test | `threading.Lock` + `time.sleep(0.05)` (test_network.py:114,120) — measures thread scheduling, not async semaphore semantics; racy |
| B4-1: real 8–26s sleep in T1 | `sleep_time=0` at constructor (network.py:61) is overridden by `call()`'s default `sleep_time=2` (network.py:96,100) → `uniform(1, 4)` real sleep × 3 attempts |
| B4-6: thread leak in T11 | `time.sleep(2)` in worker thread (test_network.py:215) — `asyncio.wait_for` cancels the wrapping coroutine but the thread keeps running post-test |

### 3.5 Magic numbers & hardcoding (MEDIUM)

| Finding | Hardcoded value | Risk |
|---------|----------------|------|
| B2-2 | `total_entries == 32` (line 123) | Silently breaks when a template is added/removed |
| B2-1 | regex `kitId\s*:\s*(\d+)` | Misses 24 of 29 `kitId` values (quoted `"kitId": N` form); 24 invalid kitIds pass undetected |
| B8-5 | 6 hardcoded `(media, report_type)` combos (lines 231-238) | Drifts from real `MediaType`/`ReportType` enums; misleading name (tests existence not missing-file) |
| B8-7 | Tests `cached_property` behavior only | Tests the stdlib decorator, not app config content |

### 3.6 Spying on internals (MEDIUM)

| Finding | Internal spied on | Better approach |
|---------|-------------------|-----------------|
| B2-3 | `mocker.patch("mko_get_mediascope_data.core.utils.en_to_ru")` (line 50) | Remove spy; `result["slices"] == ["EName_col"]` already verifies behavior |
| B8-6 | `mocker.patch("logging.config.dictConfig")` (line 188) | Assert the **config dict** passed to `dictConfig`, not just call count |

### 3.7 Redundancy & DRY violations (MEDIUM/LOW)

| Finding | Duplicate of |
|---------|----------------|
| B4-1 T1 (`test_network_retry_and_fail`) | T8 (`test_call_retries_with_default_max_attempts`) |
| B4-3 T3 (`test_call_returns_result_on_success`) | T9 (`test_call_preserves_return_type`) |
| B4-4 T7 (`test_default_max_attempts_is_10`) | T8 (exercises real retry loop) |
| B1-3 ×3 lazy-import tests | All test same PEP 562 `__getattr__` |
| B1-3 line 22 redundant `exists()` | B3-1 |
| B6-1 (`test_default_task_strategy`) | `test_create_report_writes_csv_and_marks_queue_done` (lifecycle) |
| B7-4 two `wait_task` tests (507-551, 873-923) | 95% identical setup |
| B7-5 / B8-4 | `_build_hermetic_app_service` ≡ `local_app_service` fixture |
| B7-3 / B8-3 | `_build_result_df` ≡ `_compute_result_df` (both miss frequency branch) |
| B6-6 | 2 of 4 reports tests redundant with lifecycle tests |

### 3.8 Dead code & style debt (LOW)

| Finding | Location |
|---------|----------|
| Dead import `MediascopeApiError` (silenced with `# noqa: F401`) | test_network.py:11 |
| Dead `mock_network_client` fixture (only consumer deleted with T2) | conftest.py:14-18 |
| Redundant `@pytest.mark.asyncio` (auto-mode configured) | 14 tests in test_network.py |
| Russian test error strings | test_network.py:236, 253, 270 |
| File handle leak: `json5.load(file.open())` without `with` | test_tasks.py:42 |
| Redundant `list(self.interval)` + `cast` | tasks.py:38 (production, test doesn't cover) |

---

## 4. Latent Production Bug — Block 5: Date Serialization Crash in `TVTask.to_file`

### Problem

**`TVTask.to_file`** (tasks.py:27-59) writes error artifacts via `json5.dump` with **no `default` serializer** (lines 52-58). The `interval` attribute is typed as `tuple[str, str] | tuple[date, date] | None` (line 22), and `cast` is applied at line 38. When `interval` contains `date` objects, `json5.dump` raises `TypeError: Object of type date is not JSON serializable`.

### Production trigger path (confirmed live)

1. `Report.get_period` (reports.py:298) returns `list[tuple[date, date]]` — `date`-typed tuples.
2. `multiple_files=False` (reports.py:336-338) returns `[period]` — a single `tuple[date, date]`, **not** converted to strings.
3. This `date` tuple is assigned to `task.interval` (DefaultTVTaskStrategy:506, TVRegTaskStrategy:570).
4. Task failure → `worker._process_item` (reports.py:168, 180, 186) calls `await asyncio.to_thread(item.to_file, ...)`.
5. `JSON5.dump` crashes → **`TypeError` masks the original task error**; no error artifact is written; the failure is silently lost (caught by the outer `except Exception` at reports.py:181-186 which itself calls `to_file` — **re-entrant crash**).

### Why no test catches it

`test_to_file_writes_valid_json5` (test_tasks.py:25-47) sets `task.interval = ("2025-01-01", "2025-01-07")` — **string** tuples (line 32). Strings serialize fine. The `date` code path is never exercised.

### Impact: CRITICAL

- **Data-loss adjacent:** A failed task whose error artifact cannot be written is silently swallowed — operators cannot diagnose the underlying failure.
- **Error-handling cascade:** The `except Exception` block at reports.py:181 calls `to_file` in the same `finally`-adjacent flow; a crash there corrupts the per-task error contract.
- Affects **all** `multiple_files=False` reports with `date`-typed periods (the default `multiple_files=True` uses `slice_period(..., as_strings=True)` returning string tuples — reports.py:83, which is why most reports survive).

### Status: BLOCKED on [PROD-FIX]

The test rewrite (B5-1) is **blocked** until the production fix is applied. The fix scope:

| Option | Change | Lines |
|--------|--------|-------|
| A. Add `default=` serializer | `json5.dump(dump_settings, outfile, default=lambda o: o.isoformat() if isinstance(o, date) else str(o), ...)` | tasks.py:52-58 |
| B. ISO-string at assignment | Convert `date`→`str` when setting `task.interval` | reports.py:506, 570 |
| C. Normalize in `get_period` | `multiple_files=False` returns `[(s.strftime(...), e.strftime(...))]` matching the `True` path | reports.py:331, 338 |

> This is an advisory audit — no code is modified. The production fix must be implemented and validated before the test rewrite (B5-1) can proceed.

---

## 5. Missing Critical Coverage — Grouped by Area

### 5.1 Network & retry (Block 4 — 8 gaps)

| Gap | Production code | Risk |
|-----|-----------------|------|
| G1: Backoff timing | network.py:165-169 — `backoff = sleep_time * 1.2**(attempt-1)`, `jitter = uniform(0,5)` | Backoff formula silently broken; no test pins the growth rate |
| G2: Initial jitter | network.py:103 — `uniform(sleep_time*0.5, sleep_time*2)` | Peak-load smoothing unverified; could be a hard `sleep` |
| G3: `FileNotFoundError` path | network.py:126-131 | Re-raises without retry; unverified that it's classified as non-retriable |
| G4: Semaphore release on error | network.py:106-171 — `async with self._semaphore` | Leak could deadlock under sustained errors |
| G5: `call_timeout=None` | network.py:77-79, 78 | `_invoke` returns the raw coroutine (no `wait_for`); unverified |
| G6: `_invoke` unit | network.py:74-79 — `asyncio.to_thread` + `wait_for` | Threading wrapper logic untested in isolation |
| G7: Non-5xx `MediascopeApiError` | network.py:146-156 — `else: raise` for 4xx | Error logging path for unhandled HTTP codes unverified |
| G8: Partial recovery | network.py:109-171 — success after transient retry | Only success-before-any-failure and fail-after-all attempts tested |

### 5.2 Reporting pipeline (Block 7 — 13 uncovered branches)

| Branch | Production code | Risk |
|--------|-----------------|------|
| TimeoutError handler | reports.py:173-180 | Worker error handling on `wait_for` timeout — the most safety-critical path (masks hangs) |
| TVRegTaskStrategy factory | reports.py:40-41 | `MediaType.TV_REG` → `TVRegTaskStrategy` selection untested |
| TVRegTaskStrategy full region logic | reports.py:516-577 | Region iteration, `id==99` skip, `company_filter` construction — untested (B6-4 covers only skip) |
| DictDataStrategy factory | reports.py:47-50 | `ReportSubtype.DYNAMICS_BY_SPOTS_DICT` → `DictDataStrategy` untested |
| DictDataStrategy error paths | reports.py:605-623 | `missing` columns, empty `output_columns` — untested |
| poll_deadline guard | reports.py:383-387 | Wall-clock deadline branch unverified |
| Period-clipping warning | reports.py:330-335 | `logger.warning` on partial period overlap unverified |
| `send_task` is-False branch | reports.py:352 | Task key not received — unverified |
| `extract_data` False/None branches | reports.py:422-425, 430-433, 439-441 | Result-JSON false, empty DataFrame, `prepare_data` None — untested |
| `frequency_dist_conditions` | reports.py:293-294 | `frequencyDistInterval` column appended — missing in B7-3/B8-3 helpers |
| `multiple_files=False` | reports.py:336-338 | The exact path that triggers the **date serialization crash** (§4) |
| `last_time` path | reports.py:309-317 | `get_last_period` construction untested |
| `wait_task` status-False | reports.py:394-399 | `get_status` returns `False` (API failure) — untested |

### 5.3 CLI & orchestrator (Blocks 1, 8)

| Gap | Production code | Risk |
|-----|-----------------|------|
| `init` command body | app.py:26-37 | Never invoked in tests (B1-4) |
| `run` command | app.py:40-62 | No CliRunner test; `sys.stdout.reconfigure` (app.py:51-52) unverified |
| `list_reports` command | app.py:65-76 | Untested |
| `run_report_async` / `run_report` / `initialize_settings` | app.py:80-103 | Public library API untested (B1-4) |
| `get_app_service` singleton | services.py:220-227 | Cache + lazy construction untested (B8-8) |
| `prepare_app` filename handler | services.py:60-64 | `isinstance(handler, dict)` + `"filename"` branch untested |
| Multi-entry `run_report` loop | services.py:167-178 | Iterative report processing untested |
| `get_report_settings` dict branch | services.py:92-93 | `isinstance(report_configs, dict)` normalization untested |
| Subtype overlay in `merge_data_settings` | services.py:130-132 | `hasattr(defaults, subtype.value)` + `getattr` untested |
| `AppSettings.normalize_path` | models.py:33-36 | `Path(folder).expanduser()` untested |

---

## 6. Test Infrastructure Improvements

### 6.1 Shared fixtures to extract to `tests/conftest.py`

| Current location | Proposed shared fixture | Consumers | Rationale |
|------------------|------------------------|-----------|-----------|
| `test_reports_lifecycle.py:59-67` (`_build_hermetic_app_service`) | `hermetic_app_service` | test_reports_lifecycle (×12), test_services (×6) | Eliminates identical helper + `local_app_service` fixture duplication (B7-5, B8-4) |
| `test_models.py:21-31` + `test_reports.py:15-30` patterns | `standard_report_settings` | test_models, test_reports, test_services, test_reports_lifecycle | Consistent `ReportSettings` with correct `MediaType`/`ReportType`/`ReportSubtype` enums, eliminating the wrong-enum hazard (B6-1, B6-4) |
| `test_reports.py:15-30` (`mock_report`) | `mock_report` (enriched) | test_reports (all 4) | Add `catalogs`, `output_columns`, correct enum values; refactor tests to use it (B6-5) |
| `test_reports_lifecycle.py:70-84` + `test_services.py:21-40` | `_expected_output_columns(report)` helper | lifecycle, services | Single source of truth mirroring `set_output_columns` (reports.py:282-296) including `frequencyDistInterval` (B7-3, B8-3) |

### 6.2 Delete dead infrastructure

| Item | Location | Why |
|------|----------|-----|
| `mock_network_client` fixture | conftest.py:14-18 | Only consumed by T2 (deleted in B4-2); no other test uses it |
| `MediascopeApiError` import | test_network.py:11 | Never referenced; re-add when G7 test is written |

### 6.3 Style / configuration debt

| Item | Location | Recommendation |
|------|----------|----------------|
| 14 redundant `@pytest.mark.asyncio` | test_network.py | Remove; `pyproject.toml` sets `--asyncio-mode=auto` |
| Russian test literals | test_network.py:236, 253, 270 | EN-only per AGENTS.md §1 |
| `json5.load(file.open())` without `with` | test_tasks.py:42 | Use context manager to prevent handle leak |

---

## 7. Environment Verification Summary

| Aspect | Value |
|--------|-------|
| **Platform** | Windows 11 (win32) |
| **Python** | 3.14.0 |
| **Package manager** | uv 0.11.28 |
| **Git HEAD** | `3efc4d7` — `docs(overview): commit AGENTS.md EXT-005 update and fix doc accuracies` |
| **Test runner** | `uv run pytest` (no Docker/DB required — confirmed: no database dependencies) |
| **Test collection** | **69 tests collected** (8 audited blocks + `test_utils.py`) |
| **Test result** | **69 passed** in 21.44s (includes 1 `@slow`-marked test: `test_run_report_with_real_models`) |
| **Lint** | `ruff check src tests` — **All checks passed** (dead `MediascopeApiError` import suppressed by `# noqa: F401`) |
| **Type check** | `mypy src tests` — **Success: no issues found in 23 source files** |
| **Async mode** | `pyproject.toml`: `--asyncio-mode=auto` — `@pytest.mark.asyncio` decorators are redundant (B4-9) |
| **Coverage** | `--cov=src/mko_get_mediascope_data` enabled but no threshold gate configured in `pyproject.toml` — coverage % is informational, not enforced |

### Per-block test counts (verified via `pytest --co`)

| Block | File | Collected | Audit-cited |
|-------|------|-----------|-------------|
| 1 | `tests/test_app.py` | 7 | 7 |
| 2 | `tests/test_models.py` | 10 | 10 |
| 3 | `tests/test_init_credentials.py` | 1 | 1 |
| 4 | `tests/test_network.py` | 16 | 16 |
| 5 | `tests/test_tasks.py` | 2 | 2 |
| 6 | `tests/test_reports.py` | 4 | 4 |
| 7 | `tests/test_reports_lifecycle.py` | 12 | 14 ⚠️ |
| 8 | `tests/test_services.py` | 9 | 9 |
| — | `tests/test_utils.py` (out of scope) | 8 | — |
| **Total** | | **69** | — |

> ⚠️ Block 7: audit cited 14; collection verified 12 functions. No parametrized variants exist in the file. This does not affect any finding.

---

## 8. Methodology Summary

### Audit process

This report synthesizes findings from **Steps 1–3** of a test-quality audit. Each block was:
1. **Audited** by a test-engineer who read every test function against its production counterpart (source line references verified), then
2. **Validated** by a researcher who cross-checked each claim against `pytest --collect-only`, `ruff check`, `mypy`, and direct source inspection.

### Evidence collection

All 54 findings are grounded in verifiable evidence:
- **Production source:** `src/mko_get_mediascope_data/core/` (`tasks.py`, `network.py`, `reports.py`, `services.py`, `init_service.py`, `models.py`, `utils.py`) and `app.py` / `__init__.py` — read and line-referenced.
- **Test source:** every `tests/test_*.py` file — read in full; exact line numbers and assertions cited.
- **Runtime:** `pytest` executed (69 passed), `ruff check` and `mypy` run clean.

### Severity classification

| Classification | Applies when |
|---------------|--------------|
| **[PROD-FIX]** | Production bug confirmed in source; test cannot be rewritten until fix applied |
| **CRITICAL** | Test passes unconditionally regardless of production behavior (false confidence) or test exercises a path that silently crashes production |
| **HIGH** | Test asserts wrong behavior, wrong code path, or uses wrong enum values — would pass on a broken or different implementation |
| **MEDIUM** | Redundancy, magic numbers, spying on internals, fragile coupling, missing coverage of safety-relevant branches |
| **LOW** | Style, decorator noise, dead imports, minor duplication |

### Finding types

| Tag | Meaning |
|-----|---------|
| **[TEST-REWRITE]** | Test must be substantially rewritten (logic invalid) |
| **[TEST-UPDATE]** | Test is correct in intent but needs strengthening/assertion fixes |
| **[TEST-DELETE]** | Test is redundant or meta-tests the mock, not production — remove |
| **[BEST-PRACTICE]** | Improvement beyond current spec (no defect, but risk/maintainability) |
| **[DOC-UPDATE]** | No code change needed; maintain comment accuracy |

### Scope boundaries

- **In scope:** All 8 test blocks (61 test functions across `test_app.py`, `test_models.py`, `test_init_credentials.py`, `test_network.py`, `test_tasks.py`, `test_reports.py`, `test_reports_lifecycle.py`, `test_services.py`).
- **Out of scope:** `test_utils.py` (7 functions / 8 items) — audited separately; not included in findings.
- **No production code was modified.** This report is advisory. The `[PROD-FIX]` in §4 is flagged for the implementor to resolve before the corresponding test rewrite (B5-1) becomes viable.

---

*Report written to `.ai/audit/tests/audit_report_1.md`. End of audit.*
