---
plan_id: "02-test-coverage-fixes"
source_audit: ".ai/audit/99-validation/11-test-coverage-validated-findings.md"
source_code_ref: "fa74b86"
phase: "11-test-coverage"
status: active
agent_role: "tech-lead"
findings_covered: ["TST-001", "TST-002", "TST-003", "TST-004", "TST-005", "TST-006", "TST-007"]
---

# Execution Plan — Test Coverage Fixes (TST-001..007)

## Plan Overview and Scope

This plan decomposes seven validated audit findings (1 CRITICAL, 3 HIGH, 1 MEDIUM, 2 LOW)
from the **11-test-coverage** phase into four dependency-aware execution blocks.

### Scope

**In scope:**
- `.github/workflows/ci.yml` (new) — CI workflow (TST-004)
- `.pre-commit-config.yaml` (new) — pre-commit hooks (TST-004)
- `Makefile.ps1` (new) — PowerShell task runner (TST-004)
- `pyproject.toml` — mypy scope, pytest-cov, coverage config (TST-005)
- `.kilo/rules/commands.md` — fix typecheck command reference (TST-005)
- `tests/conftest.py` — remove unused `dummy_df` local copy, use fixtures (TST-006, TST-007)
- `tests/test_services.py` — hermetic fixture, un-mock `create_report`, remove duplicate `dummy_df` (TST-001, TST-002, TST-003, TST-007)
- `tests/test_network.py` — add success/4xx/401/semaphore test cases (TST-003)
- `tests/test_models.py` — use enum references, add `data_lang="en"` test (TST-007)
- `tests/test_reports_lifecycle.py` (new) — real pipeline lifecycle test (TST-001, TST-003)

**Out of scope:**
- Production code pipeline fixes (PIPE-001, PIPE-002, PIPE-003) — owned by Phase 04
- Production code changes — all fixes are test infrastructure, CI config, and dev tooling
- Template repairs (W-01) — separate work item in delivery plan

## Constraints

1. One Implementor at a time; max 2 parallel subagents; no background launches
2. Production code is king — tests are adapted to exercise real code, never distort production code
3. English-only comments, logs, docstrings
4. No `print()` in core modules — use `logging.getLogger(__name__)`
5. Pydantic v2 models for all settings; Enums for fixed values

## Key Risks

1. **PIPE-003 crash** (reports.py:241): `get_period` accesses `self.report_settings.period.last_time.time_unit`
   unconditionally — crashes when `last_time` is None even if `date_filter` is set. The existing
   `tv_yaml_content` fixture has `last_time: null`. The lifecycle test fixture must set `last_time`
   to avoid this production-code path. This production bug is owned by Phase 04, not Phase 11.

2. **Worker hang risk** (reports.py:134-153): `worker()` has no try/finally — if `send_task` or
   `extract_data` raises, `task_done()` is skipped and `queue.join()` hangs. Tests must configure
   the doubled `MediaVortexTask` so no exceptions occur.

3. **Asyncio.sleep delays** (reports.py:278, network.py:69): `wait_task` sleeps 2s; `NetworkClient.call`
   has initial jitter sleep. Tests use `sleep_time=0` for NetworkClient and mock `asyncio.sleep`.

4. **mypy scope expansion**: Adding `tests` to mypy scope will surface new diagnostics in test code.
   Any new errors must be fixed before considering the task complete.

## Execution Blocks

### Block A — TST-004: Load Merge Gate (CI + pre-commit + Makefile)
**Priority:** HIGH (must precede trusting TST-005 metrics)
**Risk:** LOW (new files only, no existing test breaks)
**Agent:** Implementor

**Actions:**
1. Create `.github/workflows/ci.yml` — runs `ruff check src tests` + `uv run pytest` on push/PR
2. Create `.pre-commit-config.yaml` — ruff, mypy, pytest hooks
3. Create `Makefile.ps1` — test/lint/typecheck targets using `uv run`

**Acceptance criteria:**
- CI workflow file exists and runs ruff + pytest
- pre-commit config exists with ruff and mypy hooks
- Makefile.ps1 has test, lint, and typecheck targets

### Block B — TST-006 + TST-007: Test Hygiene Cleanup
**Priority:** MEDIUM
**Risk:** LOW (test-only changes)
**Agent:** Implementor

**Actions:**
1. TST-006: Add a test that uses `mock_network_client` fixture, or remove it
2. TST-006: Add a test that uses `tmp_report_dir` fixture, or remove it
3. TST-006: Use `@pytest.mark.slow` on an appropriate test (or remove the marker registration)
4. TST-007: Remove duplicate `dummy_df` local copy in `tests/test_services.py` (lines 23-25),
   use the `conftest.py` fixture instead
5. TST-007: Use `MediaType.TV`, `ReportType.crosstab`, etc. enum references instead of bare literals
   in `tests/test_models.py` and `tv_yaml_content` fixture
6. TST-007: Add `test_data_lang_en_does_not_translate` test

**Acceptance criteria:**
- No dead fixtures: all three (`mock_network_client`, `tmp_report_dir`, `slow` marker) are used
- No duplicate `dummy_df` definition
- Enum values referenced in tests instead of bare string literals
- `data_lang="en"` path tested

### Block C — TST-005: Fix mypy Scope + Add Coverage Gate
**Priority:** MEDIUM
**Risk:** MEDIUM (mypy on tests surfaces new errors — must fix all)
**Agent:** Implementor

**Actions:**
1. Add `files = ["src", "tests"]` to `[tool.mypy]` in `pyproject.toml`
2. Add `pytest-cov` to dev dependencies in `pyproject.toml`
3. Add `--cov=src/mko_get_mediascope_data` to pytest `addopts` in `pyproject.toml`
4. Add `[tool.coverage.run]` and `[tool.coverage.report]` sections to `pyproject.toml`
5. Add stub packages to dev deps: `types-PyYAML`, `types-python-dateutil`
6. Fix `.kilo/rules/commands.md` line 35: `basedpyright` → `mypy`
7. Fix all new mypy errors in test files

**Acceptance criteria:**
- `mypy src tests` runs (and is as close to clean as possible; no NEW errors beyond the
  known production-code baseline)
- `pytest-cov` is installed and `--cov` is in addopts
- `.kilo/rules/commands.md` references `mypy`, not `basedpyright`

### Block D — TST-002 + TST-001 + TST-003: Hermetic Suite + Real Pipeline Tests
**Priority:** HIGH
**Risk:** HIGH (complex, pipeline internals, mock setup)
**Agent:** Planner → Implementor

**Sub-tasks:**
1. **D1 (TST-002):** Rewrite `test_run_report_with_real_models` to use `local_app_service` fixture
   - Copy template defaults to tmp settings dir
   - Use `local_app_service.run_report(test_yaml)` instead of `get_app_service()`
   - Still un-mock `create_report` (TST-001/003) — the test now tests the real pipeline hermetically

2. **D2 (TST-001/003):** Create `tests/test_reports_lifecycle.py`:
   - `test_create_report_writes_csv_and_marks_queue_done` — exercises real `create_report`
     against a doubled `MediaVortexTask`, asserts CSV files exist and `task_done` accounting
   - Use a test fixture with `last_time` set (avoids PIPE-003 crash)
   - Double `MediaVortexTask.cats.get_availability_period`, `build_task`, `send_task`,
     `get_status`, `get_result`, `result2table` with proper return values
   - Mock `asyncio.sleep` to avoid delays
   - Assert `<export_path>/**/*.csv.gz` files exist

3. **D3 (TST-003):** Add network tests to `tests/test_network.py`:
   - `test_call_returns_result_on_success` — success return path
   - `test_call_raises_immediately_on_bad_request` — BadRequestError
   - `test_call_raises_immediately_on_auth_error` — AuthorizationError
   - `test_call_respects_semaphore` — concurrency limit (optional if feasible)

**Acceptance criteria:**
- `test_run_report_with_real_models` uses `local_app_service`, no real user-dir writes
- `test_create_report_writes_csv_and_marks_queue_done` creates real CSV files
- Network client tests cover success + error paths (not just retry exhaustion)
- All 23 (currently) + new tests pass

## Rollout Order
1. Block A (TST-004) — load a gate
2. Block B (TST-006 + TST-007) — test hygiene
3. Block C (TST-005) — mypy scope + coverage (after gate loaded)
4. Block D (TST-002 + TST-001 + TST-003) — hermetic + real pipeline

## Post-Implementation
5. Documentation update (Doc-specialist) — AGENTS.md fixture list, README CI badge
6. Final validation (Validator)

## Completion Log

| Block | Commit | Findings addressed | Status |
|-------|--------|-------------------|--------|
| A | `1d9b4af` | TST-004 (CI + pre-commit + Makefile.ps1) | ✅ Complete |
| B | `f810a96` | TST-006 (unused fixtures), TST-007 (dedup, enums) | ✅ Complete |
| C | `85c21d8` | TST-005 (mypy scope, coverage, dev deps) | ✅ Complete |
| D0 | `664e8f3` | PIPE-003 fixture fix (`tv_yaml_content` last_time) | ✅ Complete |
| D1 | `664e8f3` | TST-002 (hermetic), TST-001 (un-mock create_report) | ✅ Complete |
| D2 | `664e8f3` | TST-001/003 (real pipeline lifecycle test) | ✅ Complete |
| D3 | `664e8f3` | TST-003 (network client tests: success/4xx/401/semaphore) | ✅ Complete |
| D-fix | `2ba7523` | mypy unused-ignore cleanup, types-requests dep | ✅ Complete |
| Docs | `b98d31e` | AGENTS.md, README.md, current-behavior.md | ✅ Complete |

**Test count:** 23 → 31 (8 new tests)
**Coverage:** 68% total, reports.py 74%, services.py 85%
**mypy:** 60 src errors (pre-existing baseline), 0 test errors

## Post-Implementation Checklist
5. Documentation update (Doc-specialist) — AGENTS.md fixture list, README CI badge ✅
6. Final validation (Validator) ✅

## Validator Verdict: **PASS**
All 7 findings remediated, 31/31 tests pass, ruff clean, mypy 0 test errors, no production code changed.

---

## Task Descriptions

### Task D1: Hermetic test_run_report_with_real_models

**Files:** `tests/test_services.py`

**Targets:** `test_run_report_with_real_models` test function

**Description:**
Rewrite `test_run_report_with_real_models` to:
- Use the `local_app_service` fixture instead of `get_app_service()`
- Un-mock `create_report` (remove the `mocker.patch` for `TVMediaReport.create_report`)
- Keep `MediaVortexTask` doubled (mock `NetworkClient.call` with proper side_effect)
- Copy template defaults to the tmp settings dir so `get_default_data_settings` works
- Mock `asyncio.sleep` to avoid the 2s delay in `wait_task`
- Use a test fixture with `last_time` set to avoid the PIPE-003 crash
- Assert on actual CSV output files, not just path strings

**Acceptance criteria:**
- Test uses `local_app_service` fixture
- `create_report` is NOT mocked — real pipeline runs
- No writes to real user config dir
- Asserts CSV files exist in export path

### Task D2: test_reports_lifecycle.py — real pipeline lifecycle test

**Files:** `tests/test_reports_lifecycle.py` (new)

**Description:**
Create a new test file that exercises the real `create_report` pipeline against a doubled
`MediaVortexTask`:
- `test_create_report_writes_csv_and_marks_queue_done` — creates a `TVMediaReport` with a
  doubled `MediaVortexTask`, runs `create_report()`, asserts CSV files are written to the
  export path and `task_done` is called on the task queue
- Doubled `MediaVortexTask` methods configured:
  - `cats.get_availability_period` → DataFrame with matching `id`
  - `build_task` → dict (task JSON, not False)
  - `send_task` → non-empty string key
  - `get_status` → `{"taskStatus": "DONE"}`
  - `get_result` → dict (result JSON, not False)
  - `result2table` → DataFrame with matching columns
- Use `NetworkClient(sleep_time=0, max_attempts=1)`
- Mock `asyncio.sleep` to avoid delays

**Acceptance criteria:**
- CSV files exist after `create_report()` completes
- `task_done` accounting verified (queue.join completes)
- No real API calls, no real filesystem pollution

### Task D3: Network client tests

**Files:** `tests/test_network.py`

**Description:**
Add tests for `NetworkClient.call`:
- `test_call_returns_result_on_success` — verifies the success return path
  (`asyncio.to_thread(func, *args, **kwargs)` returns the function's result)
- `test_call_raises_immediately_on_bad_request` — `BadRequestError` is re-raised immediately
- `test_call_raises_immediately_on_auth_error` — `AuthorizationError` is re-raised immediately

**Acceptance criteria:**
- Each test exercises a distinct path through `NetworkClient.call`
- No test depends on real network calls
- All tests complete quickly (use `sleep_time=0`)
