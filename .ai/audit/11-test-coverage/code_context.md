---
phase: "11"
phase_name: "test-coverage"
audit_date: "2026-10-16"
commit: "335a11e"
---

# Code Context — Verification of Findings TST-000, TST-007 (remaining), TST-008–TST-012

## Summary

All verification conducted against HEAD (`335a11e`). Test suite: **36 passed** in 20.67s. mypy: **60 errors in 6 src files** (checked 23 files incl. tests). Coverage: **70%**.

**Key discrepancy:** the findings.md re-audit status table (lines 586–598) claims several findings are OPEN or PARTIALLY RESOLVED, but the re-audit text itself is stale relative to the current worktree in three respects:

1. **TST-011 is actually RESOLVED** — `test_merge_data_settings_en_does_not_translate` exists at `tests/test_models.py:40-62` and directly tests `data_lang="en"`, but TST-011 remains listed as OPEN.
2. **TST-007's `data_lang="en"` sub-claim is STALE** — the same test resolves it; only the `id=="1"` literal and bare-literal-in-`tv_yaml_content` sub-claims remain OPEN.
3. **TST-005's "0 test files in scope" and "basedpyright" claims are STALE** — `pyproject.toml:137` sets `files = ["src", "tests"]`; `.kilo/rules/commands.md:35` now says `mypy`, not `basedpyright` (though `basedpyright` persists in `.kilo/commands/implement/*.md` and `.kilo/commands/debug/bugfix.md`).

Additionally, **`test_count` in the findings header (line 10) says 35 but actual is 36** (`test_slice_period` is parametrized ×2; `test_init_credentials.py` was added at HEAD).

---

## TST-000 — Meta: TST-007 namespace collision

**Status: OPEN (confirmed).**

`docs/00-overview/current-behavior.md:197` labels the crosstab-template YAML fix as `TST-007`:
```text
| `defaults/tv/crosstab.yaml` | **Fixed** — ... parses correctly at HEAD (TST-007) |
```
`.ai/audit/11-test-coverage/findings.md:51` labels the duplicate-fixture-data finding as `TST-007`:
```text
| TST-007 | LOW | `tests/conftest.py::dummy_df` | Duplicated and restated fixture values ... |
```
These are two different findings sharing one ID. The collision is also present at `current-behavior.md:198`, `:202`, `:296`.

The findings.md re-audit (line 586) claims to avoid collision by minting `TST-008…012`, but it does not renumber the docs-side `TST-007`, so the namespace remains poisoned.

**Evidence (grep):**
```text
docs/00-overview/current-behavior.md:197  | TST-007 | crosstab-template YAML fix
findings.md:51                              | TST-007 | duplicate dummy_df fixture
```

---

## TST-007 — Duplicated and restated fixture values (remaining sub-claims)

**Status: PARTIALLY RESOLVED.** Two of four sub-claims are resolved; two remain OPEN.

| Sub-claim | findings.md says | Current code | Verdict |
|---|---|---|---|
| (1) Two `dummy_df` defs (conftest + test_services local copy) | UNRESOLVED | `test_services.py` no longer has a local `dummy_df`; it uses the `dummy_df` fixture (test_services.py:49). Only `conftest.py:17-20` defines it. | **RESOLVED** |
| (2) `id=="1"` hard-codes the `kitId` default | UNRESOLVED | `conftest.py:19`: `{"id": ["1"], ...}`. `reports.py:189`: `self.base_id = self.data_settings.options.get("kitId", 1)`. Still coupled. | **STILL OPEN** |
| (3) Bare-literal enum values in `tv_yaml_content` and `test_models` | UNRESOLVED | `test_models.py::test_report_settings_valid` (lines 18-29) now uses `MediaType.TV`, `ReportType.crosstab`, `ReportSubtype.TOP_NAT_TV_ADVERTISERS`. **But** `conftest.py:34-36` (`tv_yaml_content`) still uses `"crosstab"`, `"DYNAMICS_BY_SPOTS"`, `"TV"` as bare string literals. | **PARTIALLY OPEN** (test_models fixed; tv_yaml_content not) |
| (4) `data_lang="en"` path untested | UNRESOLVED | `test_models.py:40-62`: `test_merge_data_settings_en_does_not_translate` — creates `ReportSettings(data_lang="en")`, calls `merge_data_settings`, asserts `en_to_ru` is NOT called. Present and passing. | **RESOLVED** (stale in findings.md) |

**Evidence:**
```text
conftest.py:17-20: dummy_df() -> {"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}  [bare literal, kitId-coupled]
conftest.py:34:         "report_type": "crosstab",                    [bare literal]
conftest.py:35:         "report_subtype": "DYNAMICS_BY_SPOTS",        [bare literal]
conftest.py:36:         "media": "TV",                                [bare literal]
conftest.py:38:         "data_lang": "ru",                              [hard-coded ru]
test_models.py:20:  "media": MediaType.TV,                             [enum ref — FIXED]
test_models.py:21:  "report_type": ReportType.crosstab,                 [enum ref — FIXED]
test_models.py:22:  "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS,  [enum ref — FIXED]
test_models.py:40:  def test_merge_data_settings_en_does_not_translate(mocker):  [en path — TESTED]
reports.py:189: self.base_id = self.data_settings.options.get("kitId", 1)  [default == "1"]
```

---

## TST-008 — Done-file skip branch is unreachable by tests

**Status: OPEN (confirmed).**

`test_reports_lifecycle.py:131` hard-codes `check_done=False`:
```python
report = TVMediaReport(
    ...
    check_done=False,   # line 131
)
```
With `check_done=False`, `Report.__init__` skips `get_done_files()` (reports.py:72-73), so `done_files = {}` and the skip branches never execute.

Both skip branches are present and untested:
- **`DefaultTVTaskStrategy.generate` (reports.py:378-380):**
  ```python
  if task.name in report.done_files:
      logger.warning(f"File '{task.name}' already exists in folder, skipping.")
      continue
  ```
- **`TVRegTaskStrategy.generate` (reports.py:430-431):**
  ```python
  if task.name in report.done_files:
      continue
  ```

Mutation M10 (`continue` → `yield None` at line 378) was NOT DETECTED — all 35/36 tests pass. A `yield None` would send `None` into `worker` (reports.py:142 `item = await self.task_queue.get()`), then `await self.send_task(item)` would fail with `AttributeError` on `None` — but the suite never enters this branch.

**Evidence:**
```text
test_reports_lifecycle.py:131: check_done=False
grep "check_done=True" tests/  ->  (no matches)
mutation M10 (continue -> yield None at reports.py:378): 35 passed (NOT DETECTED)
```

---

## TST-009 — Production `max_attempts=10` default is never observed

**Status: OPEN (confirmed).**

`NetworkClient.__init__` defaults `max_attempts` to 10 (`network.py:53`). Every test instantiates `NetworkClient` with an **explicit** `max_attempts`:

| File | Line | Call |
|---|---|---|
| `conftest.py` | 11 | `NetworkClient(max_attempts=2, sleep_time=0)` |
| `test_network.py` | 16 | `NetworkClient(max_attempts=3, sleep_time=0)` |
| `test_network.py` | 56 | `NetworkClient(max_attempts=3, sleep_time=0)` |
| `test_network.py` | 69 | `NetworkClient(max_attempts=3, sleep_time=0)` |
| `test_network.py` | 86 | `NetworkClient(max_attempts=3, sleep_time=0)` |
| `test_network.py` | 103 | `NetworkClient(max_concurrent_requests=2, max_attempts=1, sleep_time=0)` |
| `test_reports_lifecycle.py` | 111 | `NetworkClient(sleep_time=0, max_attempts=1)` |

**No test calls `NetworkClient()` without explicit `max_attempts`.** (Production code at `services.py:152` does use `NetworkClient()` — but that's not a test.)

Mutation M13 (default 10 → 1) was NOT DETECTED — all 36 tests pass.

**Evidence:**
```text
grep "NetworkClient(" tests/  ->  every call specifies max_attempts explicitly
network.py:53: max_attempts: int = 10   [production default, never tested]
mutation M13 (max_attempts 10 -> 1): 36 passed (NOT DETECTED)
```

---

## TST-010 — No coverage `--cov-fail-under` gate

**Status: OPEN (confirmed).**

`pyproject.toml` addopts (lines 150-158) include `--cov` and `--cov-report=term-missing` but **no `--cov-fail-under`**:
```toml
addopts = [
    "--import-mode=importlib",
    "-ra",
    "-v",
    "--strict-markers",
    "--asyncio-mode=auto",
    "--cov=src/mko_get_mediascope_data",
    "--cov-report=term-missing"
]
```

`grep "cov-fail-under" pyproject.toml` → no matches. pytest exits 0 regardless of coverage %.

**Evidence:**
```text
pyproject.toml:150-158: addopts has --cov and --cov-report, no --cov-fail-under
grep "cov-fail-under" pyproject.toml  ->  (no matches)
pytest coverage: TOTAL 846 stmts, 253 miss, 70%  -> exit 0
```

---

## TST-011 — `data_lang="en"` branch is untested

**Status: RESOLVED (findings.md is STALE — lists this as OPEN).**

The test `test_merge_data_settings_en_does_not_translate` exists at `tests/test_models.py:40-62`:
```python
def test_merge_data_settings_en_does_not_translate(mocker):
    mock_en_to_ru = mocker.patch("mko_get_mediascope_data.core.utils.en_to_ru")
    defaults = DataDefaults(DATA_DEFAULTS={"slices": ["EName_col"]})
    report_settings = ReportSettings(
        ...
        data_lang="en",
        ...
    )
    result = AppService.merge_data_settings(defaults, report_settings)
    mock_en_to_ru.assert_not_called()
    assert result["slices"] == ["EName_col"]
```

This test:
1. Creates `ReportSettings` with `data_lang="en"` (not the default `"ru"`)
2. Calls `AppService.merge_data_settings()` 
3. Asserts `en_to_ru` is NOT called (the `en` path skips translation)
4. Asserts slices remain untranslated

The production code at `services.py:107-108`:
```python
if report_settings.data_lang == "ru":
    data_settings["slices"] = utils.en_to_ru(data_settings["slices"])
```

This test directly covers the `en` path (the `if` is False, so `en_to_ru` is skipped). TST-011 is **resolved** in the current worktree.

**Discrepancy:** The findings.md re-audit (lines 666-681) lists TST-011 as a NEW OPEN finding, claiming `grep 'data_lang.*en' tests/ -> (no matches)`. This is incorrect — the grep would match `test_models.py:53: data_lang="en"`. The test was added in commit `f810a96` ("test(tst): wire unused fixtures and dedup test data (TST-006, TST-007)") and is present at HEAD.

**Evidence:**
```text
test_models.py:40:  def test_merge_data_settings_en_does_not_translate(mocker):
test_models.py:53:         data_lang="en",
test_models.py:59:     # en_to_ru must NOT be called when data_lang is 'en'
test_models.py:60:     mock_en_to_ru.assert_not_called()
services.py:107: if report_settings.data_lang == "ru":
services.py:108:     data_settings["slices"] = utils.en_to_ru(data_settings["slices"])
grep 'data_lang.*en' tests/  ->  test_models.py:41, test_models.py:53, test_models.py:59  [3 matches, NOT zero]
```

---

## TST-012 — Worker loop has no timeout or exception isolation

**Status: OPEN (confirmed).**

`MediaReport.worker()` at `reports.py:134-153`:
```python
async def worker(self):
    """managing task queue sequence: ..."""
    while True:
        item = await self.task_queue.get()
        await self.send_task(item)          # line 143 — no try/except
        if item.status is True:
            await self.wait_task(item)      # line 145 — no try/except
        if item.status is True:
            await self.extract_data(item)   # line 147 — no try/except
        if item.status is False:
            logger.warning(f"Calculation: '{item.name}' will be skipped.")
            await asyncio.to_thread(item.to_yaml, self.export_path)
        else:
            logger.info(f"Task {item.name} is ready.")
        self.task_queue.task_done()         # line 153
```

**No timeout:** `while True` loop with no `asyncio.wait_for` or deadline. If `send_task`, `wait_task`, or `extract_data` hangs (e.g. `wait_task`'s own inner poll loop at reports.py:277-293 has no timeout), the worker never exits.

**No exception isolation:** Any exception from `send_task`/`wait_task`/`extract_data`/`to_yaml` propagates uncaught, killing the worker coroutine. Since `processing_tasks` (reports.py:155-162) creates `max_tasks=100` workers but only cancels them after `task_queue.join()`, a single dead worker can leave the queue non-empty forever.

**No guard around `task_done()`:** If any step raises before line 153, `task_done()` is never called, so `task_queue.join()` never returns → **indefinite hang**. Mutation M11 (removing `task_done()`) caused the suite to hang past 240s.

**Evidence:**
```text
reports.py:134-153: worker() — while True: ... await self.send_task(item) ... await self.wait_task(item) ... await self.extract_data(item) ... self.task_queue.task_done()
reports.py:277-293: wait_task inner loop — while True: await asyncio.sleep(2); status = await call(get_status, ...)  [no deadline, no break on timeout]
reports.py:155-162: processing_tasks — await self.task_queue.join()  [no timeout, no error recovery]
mutation M11 (task_done() removed): suite HUNG > 240s, killed via taskkill
_probe_mutations.py: M11 excluded from the probe script (only M10, M13, M16, M17 present)
```

---

## Discrepancies Between findings.md and Current Code

| Finding | findings.md claims | Actual current code | Type |
|---|---|---|---|
| TST-005 | "0 test files in scope" for mypy | `pyproject.toml:137`: `files = ["src", "tests"]`; mypy checked 23 files incl. 9 test files | STALE |
| TST-005 | "basedpyright still absent" (referring to `.kilo/rules/commands.md:35`) | `commands.md:35` now says `uv run mypy <path>`, not `basedpyright` | STALE |
| TST-005 | "mypy still red (60 errors in 7 files)" | mypy: 60 errors in **6** files (checked 23) — one file lost its errors between audit write and HEAD | MINOR |
| TST-006 | "`tmp_report_dir`... never invoked" | `test_utils.py:37`: `test_csv_to_file_writes_to_report_dir(tmp_path, tmp_report_dir)` uses the fixture | STALE |
| TST-007 | "`data_lang="en"` path still untested" | `test_models.py:40-62`: `test_merge_data_settings_en_does_not_translate` exists and passes | STALE |
| TST-007 | "two `dummy_df` defs" (conftest + test_services local) | test_services.py local copy removed; only conftest.py:17 defines it | STALE |
| TST-007 | "`test_models.test_report_settings_valid` restate enum values as bare literals" | test_models.py:20-22 now use `MediaType.TV`, `ReportType.crosstab`, `ReportSubtype.TOP_NAT_TV_ADVERTISERS` | STALE |
| TST-011 | "`data_lang="en"` branch is untested" | `test_models.py:40-62` tests it directly | STALE (finding is RESOLVED) |
| Header | `test_count: 35` | 36 tests collected (parametrized `test_slice_period` ×2; `test_init_credentials.py` added at HEAD) | MINOR |
| system-architecture.md:244 | "23 run" / "mypy red (56 errors)" | 36 tests run; mypy: 60 errors | STALE (not in findings.md scope but adjacent) |

---

## TST-005 — PARTIALLY RESOLVED (with stale sub-claims)

**Status: PARTIALLY RESOLVED** (findings.md status is accurate in direction but stale on specifics).

| Sub-claim | Current state |
|---|---|
| `pytest-cov` absent | **FIXED** — `pytest-cov>=6.0.0` in dev deps (pyproject.toml:180); `--cov` in addopts; coverage output shows 70% |
| No `--cov-fail-under` gate | **STILL OPEN** — this is now TST-010 |
| mypy scope excludes tests | **FIXED** — `pyproject.toml:137`: `files = ["src", "tests"]` |
| mypy still red | **TRUE** — 60 errors in 6 src files (was 61 in 7 files) |
| `basedpyright` in commands.md | **STALE** — `commands.md:35` says `mypy`; but `basedpyright` still referenced in `.kilo/commands/implement/implement-plan.md:129`, `implement-task.md:106`, `implement-multiple-tasks.md:63`, `debug/bugfix.md:52` |
| CI does not run mypy | **TRUE** — `ci.yml` runs only ruff + pytest, not mypy |

**Evidence:**
```text
pyproject.toml:137: files = ["src", "tests"]
pyproject.toml:156-157: --cov=src/mko_get_mediascope_data --cov-report=term-missing  (no --cov-fail-under)
pyproject.toml:180: pytest-cov>=6.0.0  [in dev deps]
.kilo/rules/commands.md:35: | Typecheck | `uv run mypy <path>` |  [NOT basedpyright]
.github/workflows/ci.yml: only "Lint (ruff)" + "Test (pytest)" steps; no mypy step
uv run mypy: Found 60 errors in 6 files (checked 23 source files)  [was 61 in 7]
```

---

## TST-004 — PARTIALLY RESOLVED (confirmed)

**Status: PARTIALLY RESOLVED** (findings.md is accurate).

| Gate | Current state |
|---|---|
| `.github/workflows/ci.yml` | **EXISTS** — runs `ruff check src tests` + `uv run pytest` on push/PR. No mypy step. |
| `.pre-commit-config.yaml` | **EXISTS** — ruff + ruff-format hooks only (no mypy, no pytest) |
| `Makefile.ps1` | **EXISTS** — lint, test, typecheck, format, all targets |
| Git hooks | 0 active (all `.sample`) |

**Evidence:**
```text
Test-Path .github/workflows/ci.yml  ->  True
Test-Path .pre-commit-config.yaml   ->  True
Test-Path Makefile.ps1            ->  True
pre-commit hooks: ruff, ruff-format  (no mypy, no pytest)
CI jobs: ruff + pytest only  (no mypy)
```
