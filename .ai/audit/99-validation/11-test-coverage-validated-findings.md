---
phase: "11"
phase_name: "test-coverage"
id_prefix: "TST"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/11-audit-test-coverage.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Test Coverage

## Summary

Seven findings (1 CRITICAL, 3 HIGH, 1 MEDIUM, 2 LOW). All seven anchors resolve to
the cited location, all seven disambiguator tokens exist inside their primary anchors,
and all seven claims reproduce under the re-execution performed at `d093d9c` (HEAD).
**7 confirmed, 0 rejected, 0 re-graded, 0 unsettled.**

No finding was downgraded, re-typed, or withdrawn. The green test status (12 passed) and
green ruff ("All checks passed!") are confirmed methodology, never verdicts, and do not
substantiate the findings they are cited alongside. The sole imprecision found is a
cross-reference error inside the ID-Conflict Note (phase 09's prefix is `EXT`, not
`API-`; phase 10 `QLT-` is omitted) — recorded as VAL-001; it does not affect any finding's
verdict.

## Scope

**Examined:** `core/reports.py` (`TVMediaReport.create_report`),
`core/services.py` (module-scope `app_service` singleton + module-scope `dictConfig`),
`core/models.py` (enums), `core/network.py` (`NetworkClient.call`), `core/tasks.py`,
`core/utils.py` (`yaml_to_dict`, `csv_to_file`), `core/paths.py`, `app.py`,
`__init__.py` (0-byte), `pyproject.toml`
(`[tool.ruff.lint]`, `[tool.mypy]`, `[tool.pytest.ini_options]`), `.kilo/rules/commands.md`,
`AGENTS.md`, `tests/{conftest,test_app,test_models,test_network,test_reports,test_services,test_utils}.py`,
`.ai/audit/{01,04,09}-*/findings.md`, `.ai/audit/99-validation/{01,04}-*`,
`.kilo/commands/audit/phases/{11,01}-audit-*.md`, `.ai/audit/templates/audit-findings.md`.

**Runtime probes performed:**
- `uv run pytest` → `collected 12 items / 12 passed in 19.37s` (matches findings)
- `uv run ruff check src tests` → `All checks passed!` (matches findings)
- `uv run mypy src` → `Found 61 errors in 7 files (checked 14 source files)`; mypy 1.19.1
- `import pytest_cov` → `ModuleNotFoundError`; `import basedpyright` → `ModuleNotFoundError`
- `platformdirs.user_config_dir('mko_get_mediascope_data')` → real user dir (re-polluted)
- FS-pollution re-probe after a fresh `pytest` run: `log.log` size, `settings/` children,
  empty `output/finance/raw_data`
- `git rev-parse --short HEAD` → `d093d9c`; `git ls-files src` → 36; `git ls-files tests` → 7
- `Test-Path .github / Makefile / Makefile.ps1 / .pre-commit-config.yaml` → all `False`
- `Get-ChildItem .git/hooks | ? Name -notmatch '.sample'` → 0 active hooks
- grep over `tests/` for `mock_network_client`, `tmp_report_dir`, `@pytest.mark.slow`,
  `MediaVortexTask`, `create_report`/`NetworkClient.call`, `dummy_df`

**Not verified:** live Mediascope `MediaVortexTask` API calls (no credentials); a
clean-environment run with an uninitialized user config dir (the import-time
`TypeError` — ENT-003 — is established by phase 01 and its mechanism confirmed here by
source read of `yaml_to_dict` returning `None`); Jupyter `sys.stdout.reconfigure` on
`ipykernel.iostream.OutStream` (inferred from the unconditional call site).

---

## Findings

| ID      | Disposition | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|---------------------------|-----------------|
| TST-001 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL     | yes             |
| TST-002 | confirmed   | HIGH       | HIGH vs. HIGH             | yes             |
| TST-003 | confirmed   | HIGH       | HIGH vs. HIGH             | yes             |
| TST-004 | confirmed   | HIGH       | HIGH vs. HIGH             | yes             |
| TST-005 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM         | yes             |
| TST-006 | confirmed   | LOW        | LOW vs. LOW               | yes             |
| TST-007 | confirmed   | LOW        | LOW vs. LOW               | yes             |

### TST-001 — confirmed — [CRITICAL] `create_report` + `NetworkClient.call` mocked; no CSV/output assertion

**Anchor resolved:** `core/reports.py::TVMediaReport.create_report` — method at line 164;
body `self.period = await self.get_period()` (166), `await self.processing_tasks()` (167).
The disambiguator `await self.processing_tasks()` is at line 167. In `tests/test_services.py`
the matching `mocker.patch` is at lines 41-50 (NetworkClient.call at 41-44, create_report
at 47-50); the only assertions are at lines 56-58.

**Claim tested:** `create_report` is the entry to `get_period` → `processing_tasks` →
`worker` → `send_task`/`wait_task`/`extract_data` → `prepare_data` → `csv_to_file` →
`task_done`. The only end-to-end test replaces `create_report` with an `AsyncMock` no-op and
`NetworkClient.call` with a stub, then asserts only on the returned export path — never on a
CSV file existing, a `task_done` accounting, a skip, or a crash.

**Evidence (source — test doubles):**
```text
tests/test_services.py:41-44:
    mocker.patch(
        "mko_get_mediascope_data.core.network.NetworkClient.call",
        side_effect=mock_call_side_effect,
    )
    mocker.patch(
        "mko_get_mediascope_data.core.reports.TVMediaReport.create_report",
        new_callable=AsyncMock,
    )
```
*(supports: "create_report and NetworkClient.call are both replaced in the only end-to-end test")*

**Evidence (assertion surface):**
```text
tests/test_services.py:56-58:
    assert len(export_paths) == 1
    assert export_paths[0].name == "raw_data"      # only checks the path string
    assert export_paths[0].parts[-2] == "finance"  # only checks the path string
```

**Evidence (probe — no output produced):** After the run, `output/finance/raw_data` exists
but is empty (created by `services.py:126 ensure_dir`, not by `csv_to_file`), so zero
`.csv.gz` files are written while the test passes. (`create_report` is a no-op `AsyncMock`.)

**Rubric grading:** Phase 11 CRITICAL = "the suite's green is affirmatively false today… a
decision the system acts on whose effect no assertion anywhere can observe, where the
unobserved effect is one a user or a seller receives." The CSV the seller receives is never
asserted. Matches CRITICAL. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.create_report` / the worker
pipeline in `core/reports.py::MediaReport.worker`) and tests
(`tests/test_services.py::test_run_report_with_real_models`). Confirmed via grep that
`create_report` and `NetworkClient.call` are patched in exactly one test file
(`tests/test_services.py`) and nowhere else.

**Recommendation carry-out:** Fix names existing targets — un-mock `create_report` and run
the real pipeline against a doubled `MediaVortexTask`; add
`test_create_report_writes_csv_and_marks_queue_done`. Targets resolvable. ✓

### TST-002 — confirmed — [HIGH] Suite is not hermetic: import-time singleton + `dictConfig` in the real user-config dir

**Anchor resolved:** `core/services.py::app_service` — module-scope singleton at line 163:
`app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))`.
Module-scope `dictConfig` at line 166. Disambiguator `app_service = AppService(...)` matches.
`AppService.__init__` (lines 30-33) calls `self.prepare_app()`; `app_config`
cached_property (lines 35-42) reads the two YAML files from the *real*
`platformdirs.user_config_dir` and validates them; `prepare_app` (lines 44-49) calls
`ensure_file_parent`, creating directories; `APP_PATHS.user_dir` resolves via
`paths.py:8` `USER_DIR = Path(user_config_dir(APP_NAME))`.

**Claim tested:** Any test importing `mko_get_mediascope_data.app` (e.g. `test_app.py:3`
→ `app.py:14 from .core.services import app_service`) or `core.services`
(`test_services.py:6`) executes the singleton at module scope, doing real FS I/O and
`dictConfig` against the real user dir. The suite's green therefore requires ambient
initialized config; in a clean tree the import raises the `TypeError` (ENT-003).

**Evidence (probe — FS pollution after one `pytest` run):**
```text
USER_CONFIG_DIR = C:\Users\Om\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data
USER_DIR exists: True
  log.log (file)   6691 bytes   # grew from 5557 -> appended every pytest run
  settings (dir)
  output (dir)
SETTINGS children: ['app_config.yaml', 'connections', 'defaults', 'log_config.yaml', 'reports']
output/finance/raw_data exists: True
output/finance/raw_data children: []   # empty, created by services.py:126 ensure_dir
```
*(supports: "log.log + settings + empty output dir appear in the real user tree from a single pytest run")*

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/services.py:163: app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))
src/mko_get_mediascope_data/core/services.py:164: log_config = app_service.app_config.logging_settings
src/mko_get_mediascope_data/core/services.py:166: logging.config.dictConfig(log_config.model_dump())
```

**Rubric grading:** Phase 11 HIGH = "a property the harness depends on that nothing holds
it to… state carried forward between runs… process-wide state a test shares with
production code." The singleton is process-wide, `log.log` is appended every run, and
`check_done=True` reads stale real output. Matches HIGH (not CRITICAL: the green is
contaminated rather than affirmatively false). No re-grade.

**Load-bearing artefact:** Code (`core/services.py` module scope) plus the `platformdirs`
user-config dir (environment). Corroborated by phase 01 ENT-002 (same singleton, same
import-time side effect) — cross-referenced, not re-filed.

**Recommendation carry-out:** Targets `core/services.py` (defer singleton) + `tests/conftest.py`
(hermetic fixture). Both exist and are stable. ✓

### TST-003 — confirmed — [HIGH] Doubles replace the network + report-execution boundaries the only end-to-end test purports to cover

**Anchor resolved:** `tests/test_services.py::test_run_report_with_real_models`. The
disambiguator `mock_report_service.cats.get_availability_period = MagicMock(...)` is at
line 28. `mock_report_service` is constructed at line 26 (`# Fake MediaVortexTask`, line 25).

**Claim tested:** The doubles inventory removes exactly the boundaries the test claims to
cover: `NetworkClient.call` (retry/backoff/semaphore pipeline in `core/network.py`) at
test_services.py:41-44, and `TVMediaReport.create_report` (the entire worker→CSV pipeline)
at test_services.py:47-50. The real `MediaVortexTask` is never constructed (`get_connection`
is stubbed at test_services.py:36-39). `tests/test_network.py` observes only the retry-exhaustion
path (`ConnectTimeout` → `RuntimeError`); the success-return, semaphore, jitter, and
4xx-immediate-raise paths are never asserted.

**Evidence (doubles present, real connection absent):**
```text
tests/test_services.py:41-44  -> mocker.patch("...NetworkClient.call", side_effect=...)
tests/test_services.py:47-50  -> mocker.patch("...TVMediaReport.create_report", new_callable=AsyncMock)
tests/test_services.py:26      -> mock_report_service = MagicMock()   # # Fake MediaVortexTask
tests/test_services.py:36-39   -> async def mock_call_side_effect: if func.__name__=="get_connection": return mock_report_service
tests/test_reports.py:23        -> report.network_client.call = AsyncMock(return_value=dummy_df)
grep "MediaVortexTask" in tests/ -> 1 match (comment only: test_services.py:25: # Fake MediaVortexTask)
```

**Rubric grading:** Phase 11 HIGH = "a gap that will be mistaken for coverage and is silent
today: a decision exercised only through a stand-in for the thing it decides, a boundary
asserted from one side only." Subject-under-test doubled. Matches HIGH. No re-grade.

**Load-bearing artefact:** Code (`core/network.py::NetworkClient.call`,
`core/reports.py::TVMediaReport.create_report`) and tests. Shares root cause with TST-001.

**Recommendation carry-out:** Split the doubles: keep `MediaVortexTask` doubled, run the
real `NetworkClient`/`create_report`. Targets `tests/test_network.py` +
`tests/test_reports_lifecycle.py` (new) — resolvable. ✓

### TST-004 — confirmed — [HIGH] No merge gate is loaded

**Anchor resolved:** `pyproject.toml::tool.ruff.lint` — `[tool.ruff.lint]` at line 106,
`select = ["E","F","I","B","UP"]` at lines 107-113. Disambiguator `select = [...]` matches.

**Claim tested:** Static analysis, types, and tests are *declared* in `pyproject.toml`
(`[tool.ruff.lint]`, `[tool.mypy]`, `[tool.pytest.ini_options]`) but nothing loads them on a
merge: there is no `.github/` directory, no `Makefile`/`Makefile.ps1`, no
`.pre-commit-config.yaml`, and every entry in `.git/hooks` is a `.sample` (0 active hooks).
So ruff/mypy/pytest are green only if a human runs them locally; a red commit is not stopped.

**Evidence (probe — gate-loading inventory):**
```text
.github exists: False
Makefile exists: False
Makefile.ps1 exists: False
.pre-commit-config.yaml exists: False
active (non-.sample) git hooks: 0
pyproject declared: [tool.ruff.lint] select=[E,F,I,B,UP]; [tool.mypy]; [tool.pytest.ini_options] testpaths=["tests"]
```
*(supports: "every gate is declared in config but none is loaded by CI/hook/Makefile")*

**Rubric grading:** Phase 11 HIGH = "a gate declared and never loaded." Exact match.
No re-grade.

**Load-bearing artefact:** Config (`pyproject.toml`) vs. the absence of enforcement. The
ruff/mypy/pytest *config* is honest; only the *enforcement* is missing.

**Recommendation carry-out:** Create `.github/workflows/ci.yml` + `.pre-commit-config.yaml`
+ `Makefile.ps1` targets running `ruff check src tests` and `uv run pytest`. Targets are
proposed new files (their absence is the finding); commands resolvable. ✓

### TST-005 — confirmed — [MEDIUM] mypy red over accepted-noise class; test tree outside scope; no coverage gate; typecheck command mis-installed

**Anchor resolved:** `pyproject.toml::tool.mypy` — `[tool.mypy]` at line 135,
`disallow_any_generics = true` at line 142. Disambiguator matches.

**Claim tested (four sub-claims, all reproduced):**
1. `uv run mypy src` exits 1: `Found 61 errors in 7 files (checked 14 source files)` — **0 test
   files** in scope (mypy is invoked with `src` only; no `files = ["src","tests"]`).
2. `pytest-cov` is not declared and not installed: `import pytest_cov` →
   `ModuleNotFoundError: No module named 'pytest_cov'`; no `[tool.coverage]`, no `--cov` in
   `addopts` (pyproject.toml:150-156). Coverage is never measured.
3. `basedpyright` is not installed/configured: `import basedpyright` →
   `ModuleNotFoundError`; no `[tool.basedpyright]` in pyproject; not in dev deps
   (`pyproject.toml:166-177` lists black/isort/pytest/pytest-asyncio/pytest-mock/ruff/pandas-stubs/mypy/twine).
4. `.kilo/rules/commands.md:35` declares `uv run basedpyright <path>` as the typecheck
   command, which fails if followed — a doc-vs-config drift. (Note: `AGENTS.md:17`
   instead names `uv run mypy <path>`, so the two rule documents already disagree; mypy
   is the de-facto tool.)

**Evidence (probe):**
```text
$ uv run mypy src
Found 61 errors in 7 files (checked 14 source files)   # 0 test files checked; mypy 1.19.1

$ uv run python -c "import pytest_cov"
ModuleNotFoundError: No module named 'pytest_cov'

$ uv run python -c "import basedpyright"
ModuleNotFoundError: No module named 'basedpyright'
```

**Evidence (source — cited genuine diagnostics):**
```text
src/mko_get_mediascope_data/core/utils.py:25: error: Incompatible types in assignment (expression has type "date", variable has type "str")  [assignment]
src/mko_get_mediascope_data/core/reports.py:241: error: Item "None" of "LastTimeModel | None" has no attribute "time_unit"  [union-attr]
src/mko_get_mediascope_data/app.py:17: error: Item "TextIO" of "TextIO | Any" has no attribute "reconfigure"  [union-attr]
```
*(reports.py:241 is the type-level analogue of PIPE-003's `get_period` crash — cross-ref valid)*

**Rubric grading:** Phase 11 MEDIUM = "a gate loaded and red over a known accepted-noise
class" + "a measurement whose effective scope is not the scope it is read for." mypy is
loaded (installed) and red; its scope (`src`) excludes the whole test tree; the noise is
config-driven (`import-untyped` from missing stubs, `type-arg` from
`disallow_any_generics`). The diagnostic count is not the finding — the scope/loaded-state is.
Matches MEDIUM. No re-grade.

**Load-bearing artefact:** Config (`pyproject.toml [tool.mypy]`, `dev` deps) and rules
(`.kilo/rules/commands.md:35`). The `basedpyright`/`mypy` doc drift is a test-layer
contract (TST scope) because it governs whether the gate is correctly invoked.

**Recommendation carry-out:** Targets `pyproject.toml` + `.kilo/rules/commands.md`, plus
adding stub types + `pytest-cov` dev deps. All target files exist; fixes resolvable. ✓

### TST-006 — confirmed — [LOW] Declared-but-dead test-layer contracts

**Anchor resolved:** `tests/conftest.py::mock_network_client` — fixture at line 10.
Disambiguator `def mock_network_client():` matches. `tmp_report_dir` at line 24.

**Claim tested:** Three declared contracts are defined and never used:
`mock_network_client` (conftest.py:10-13), `tmp_report_dir` (conftest.py:23-27) are never
requested as a fixture argument by any test; the `slow` marker (pyproject.toml:161-163,
`"slow: marks tests as slow..."`) is registered but `@pytest.mark.slow` appears nowhere in
`tests/`. `AGENTS.md:71` lists `mock_network_client`, `dummy_df`, `tv_yaml_content`,
`tmp_report_dir` as the fixtures — the project self-description advertises helpers that no
test consumes.

**Evidence (probe — usage):**
```text
conftest.py:10: def mock_network_client():         ...  (used by: NONE)
conftest.py:24: def tmp_report_dir(tmp_path):      ...  (used by: NONE)
conftest.py:17: def dummy_df():                    ...  (used by: test_reports.py::mock_report)
pyproject markers: ["slow: marks tests as slow (use -m slow to run)"]
@pytest.mark.slow in tests/ -> (no matches; only @pytest.mark.asyncio / @parametrize present)
AGENTS.md:71: "Test fixtures live in tests/conftest.py: mock_network_client, dummy_df, tv_yaml_content, tmp_report_dir"
```

**Rubric grading:** Phase 11 LOW = "self-description and hygiene with no runtime
consequence today… a marker registered and inert." Exact match. No re-grade.

**Load-bearing artefact:** Tests (`tests/conftest.py`) + docs (`AGENTS.md:71`) + config
(`pyproject.toml::tool.pytest.ini_options.markers`). The doc over-describes the helpers.

**Recommendation carry-out:** Targets `tests/conftest.py` / `pyproject.toml` /
`tests/test_*`. Resolvable. ✓

### TST-007 — confirmed — [LOW] Duplicated fixture data and restated enum values

**Anchor resolved:** `tests/conftest.py::dummy_df` — fixture at line 17 (body lines 18-20).
Disambiguator `{"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}`
matches conftest.py:18-20.

**Claim tested (four sub-claims, all confirmed):**
1. `dummy_df` is defined in `conftest.py:17-20` **and** re-declared as a local in
   `test_services.py:21-23` with identical content — two maintenance paths that can drift.
2. Both hard-code `id: "1"`, which only lines up because `TVMediaReport.base_id` falls back
   to `data_settings.options.get("kitId", 1)` (= `1`) at `core/reports.py:189`, and
   `get_available_period` (reports.py:201) compares `df["id"] == str(self.base_id)`. The only
   consumer is `mock_report_service.cats.get_availability_period`, but since `create_report`
   is mocked (TST-001) that path never executes against the dummy — so a non-default
   `kitId` mismatch is unobserved.
3. `tv_yaml_content` (conftest.py:31-50) and `test_models.py::test_report_settings_valid`
   restate enum members as bare literals (`"TV"`, `"crosstab"`,
   `"DYNAMICS_BY_SPOTS"`, `"TOP_NAT_TV_ADVERTISERS"`) instead of `MediaType.TV` etc.
   (models.py:39-58).
4. `data_lang` is fixed at `"ru"` everywhere (conftest `tv_yaml_content: "ru"`;
   `test_models` omits it, defaulting to `models.py:92` `data_lang = "ru"`), so the `en`
   branch guarded by `en_to_ru` in `services.py:90-91` is never exercised.

**Evidence (source):**
```text
conftest.py:17: def dummy_df():
conftest.py:18-20: return pd.DataFrame({"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]})
test_services.py:21-23: dummy_df = pd.DataFrame({"id": ["1"], ...})   # identical local copy
reports.py:189: self.base_id = self.data_settings.options.get("kitId", 1)
reports.py:201: df.loc[df["id"] == str(self.base_id)[["periodFrom", "periodTo"]]
models.py:92: data_lang: Literal["ru", "en"] = "ru"
services.py:90-91: if report_settings.data_lang == "ru": data_settings["slices"] = utils.en_to_ru(...)
```

**Rubric grading:** Phase 11 LOW = "a duplicated helper with no divergence yet." The two
`dummy_df` copies currently agree; the restated-literal and `data_lang="ru"` drifts are
latent. Matches LOW. No re-grade.

**Load-bearing artefact:** Tests (`tests/conftest.py`, `tests/test_services.py`,
`tests/test_models.py`). The `id=="1"` coupling is a fixture-data contract with no divergence
yet, so no production-code change is required to validate it.

**Recommendation carry-out:** Targets `tests/conftest.py` + `tests/test_models.py`.
Resolvable. ✓

---

## Validation-Level Findings (VAL-)

| ID      | Severity | Disposition | Description |
|---------|----------|-------------|-------------|
| VAL-001 | LOW      | confirmed   | ID-Conflict Note mislabels phase 09's prefix as `API-`; actual prefix is `EXT` (`09-external-api/findings.md:4`); phase 10 (`QLT-`) is omitted from the enumerated existing-prefix list |

### VAL-001 — [LOW] ID-Conflict Note misstates an existing prefix namespace

**Template mandate:** Phase 11 §(ID-Conflict) requires checking for a `TST-` collision and
reporting rather than minting a second namespace; the prefix enumeration is supporting
evidence.

**Observed:** The findings' ID-Conflict Note (findings.md:394-396) states:
"The existing prefix namespaces in `.ai/audit` are `ENT- (phase 01), CFG- (phase 02),
PIPE- (phase 04), ASY- (phase 03), API- (phase 09), TPL- (phase 05)`". But:
- `09-external-api/findings.md:4` declares `id_prefix: "EXT"`, **not** `API-`.
- Phase 10 (`10-code-quality/findings.md:4`) declares `id_prefix: "QLT-"`, which is omitted
  from the list.

**Impact:** None on any finding's verdict — `EXT` and `QLT` still do not collide with `TST-`,
so the no-collision conclusion holds. The error is confined to the audit's supporting
cross-reference text.

**Cause:** A stale/incorrect enumeration of sibling prefixes in the ID-Conflict Note.

**Disposition:** confirmed. Does not affect any finding. Recorded separately because it is a
defect in the audit artefact, not in the audited system.

---

## Cross-Phase Analysis

### Ownership check

| Concern | Phase that owns it | TST filing | Verdict |
|---|---|---|---|
| Singleton `app_service` at import (FS I/O + `dictConfig`) | ENT-002 (entry-architecture) | TST-002 (test suite depends on it) | cross-reference, no conflict |
| `yaml_to_dict` returns `None` → `TypeError`; `init` unreachable | ENT-003 (entry-architecture) | cited by TST-002 as the clean-env failure mode | cross-reference, no conflict |
| `get_period` crash at reports.py:241 (`last_time.time_unit`) | PIPE-003 (data-pipeline) | TST-005 cites the same mypy diagnostic | cross-reference, no conflict |
| `create_report`/worker/`csv_to_file` coverage gap | PIPE-001/002/004/006/007 (data-pipeline) | TST-001 records the coverage gap that lets them ship green | cross-reference, no conflict |
| `print()` in `core/` | ENT-005 (entry-architecture) | — (not re-claimed by TST) | no conflict |
| mypy diagnostics in production code | phase 10 (code-quality) | TST-005 scopes only to "test tree outside mypy + mypy red + coverage gate" | no conflict; TST does not populate production diagnostics |

No finding is filed under a phase that does not own it. TST-002 does **not** re-claim
ENT-002's singleton bug as a production defect — it owns the *test-harness* dependency on
that import-time behaviour, which is phase 11's concern ("a property the harness depends on
that nothing holds it to"). The two coexist (cf. the ENT-002 validated report's cross-phase
table for `print()`).

### Opposite conclusions

None. No two phases reach opposing verdicts on the same subject. The PIPE-004 validated
report already cross-references ENT-002/ENT-003 (same singleton / same `TypeError`) and
records no conflict; TST-002's findings are consistent with both.

### Namespace integrity (block 7)

Six shapes per prefix, per the phase-99 ruling:
- **Declared:** phase file `11-audit-test-coverage.md` Report Output → `TST-` — ✓ matches
  findings front matter `id_prefix: "TST"`.
- **Minted:** `TST-001`…`TST-007` — a single, contiguous set in one namespace. ✓
- **In-source markers:** none (findings are audit artefacts, not shipped code). ✓
- **Compound form:** not applicable. ✓
- **Phase-prefix list:** `ENT` (01), `CFG` (02), `ASY` (03), `PIPE` (04), `TPL` (05), `EXT` (09),
  `QLT` (10), `TST` (11) — verified by grepping `id_prefix:` across `.ai/audit/`.
- **Reuse across phases:** each phase uses exactly one prefix; no phase reuses another's. ✓

**Ruling:** The phase 11 run reuses the declared `TST-` namespace (no second namespace
minted). A full-tree `TST-` search returns matches *only* inside
`.ai/audit/11-test-coverage/findings.md` (the run's own IDs) and the phase-file instruction
line (`.kilo/commands/audit/phases/11-audit-test-coverage.md:128`) — **no** pre-existing
`TST-` identifiers in `src/`, `tests/`, `docs/`, or sibling audit reports. No collision.
(VAL-001 records a drift in the *enumerated list* of sibling prefixes, not a collision.)

## Rollout Safety Assessment

**Test-impact (per finding the fix turns red):**
- TST-001 fix (un-mock `create_report`) will turn `tests/test_services.py::test_run_report_with_real_models`
  red until the real pipeline is driven by a doubled `MediaVortexTask` — the findings name
  this test as the intended casualty. `test_csv_to_file` (single write → 1 file) and
  `test_default_task_strategy` are unaffected. ✓
- TST-002 fix (defer singleton / hermetic fixture) must keep `test_services` and `test_app`
  green, since both import `app_service`. The findings prescribe a lazy accessor /
  `tmp_path`-rooted `PathResolver` — compatible with both. ✓
- TST-004 fix (add CI): creates new files; no existing test breaks. ✓
- TST-005 fix (mypy on `src tests`): will surface fresh diagnostics in test code — intended,
  not a regression. ✓
- TST-006/TST-007 fixes: test-only; no production-code change. ✓

**Rollout ordering:** TST-004 (load a gate) should precede trusting TST-005's number;
TST-001/TST-003 share the "subject-under-test doubled" root cause and are fixed together
(un-mock `create_report`/`NetworkClient.call`, keep only `MediaVortexTask` doubled).

**Merge candidates:** TST-001 + TST-003 (both are the `create_report` + `NetworkClient.call`
doubling in `test_services.py`); TST-002 + TST-003 (both flow from the singleton/hermeticity
rewrite).

**No fix has been applied** — all seven findings remain in the `d093d9c` tree unmodified.

## Evidence Index

| Probe | Result | Supports |
|---|---|---|
| `uv run pytest` | `collected 12 items / 12 passed in 19.37s` | TST-001 (green despite mocked pipeline), TST-003 (only end-to-end test mocks subject) |
| `uv run ruff check src tests` | `All checks passed!` | TST-004 (ruff green but not gate-loaded) |
| `uv run mypy src` | `Found 61 errors in 7 files (checked 14 source files)`; mypy 1.19.1 | TST-005 (red + 0 test files in scope) |
| `import pytest_cov` | `ModuleNotFoundError: No module named 'pytest_cov'` | TST-005 (no coverage gate) |
| `import basedpyright` | `ModuleNotFoundError: No module named 'basedpyright'` | TST-005 (command mis-installed) |
| `grep create_report\|NetworkClient.call\|get_connection tests/` | matches only in `test_services.py` (41-48) | TST-001, TST-003 (doubling) |
| `grep MediaVortexTask tests/` | 1 match (comment `test_services.py:25`) | TST-003 (real client never constructed) |
| `grep mock_network_client\|tmp_report_dir tests/` | conftest.py:10, conftest.py:24 only | TST-006 (unused fixtures) |
| `grep @pytest.mark.slow tests/` | no matches (only `@pytest.mark.asyncio`/`@parametrize`) | TST-006 (inert marker) |
| `grep dummy_df tests/` | conftest.py:17 def + test_services.py:21 local copy | TST-007 (duplicate helper) |
| FS re-probe after `pytest` | `log.log` 6691 B (appended); `settings/` children present; `output/finance/raw_data` empty | TST-002 (non-hermetic, real user dir) |
| `Test-Path .github\|Makefile\|Makefile.ps1\|.pre-commit-config.yaml` | all `False` | TST-004 (no merge gate) |
| `Get-ChildItem .git/hooks` non-`.sample` | 0 items | TST-004 (0 active hooks) |
| `git rev-parse --short HEAD` | `d093d9c` | matches `code_ref` (all findings) |
| `git ls-files src` / `tests` | 36 / 7 | GATES (all cited paths tracked) |
| `grep id_prefix .ai/audit/*/findings.md` | ENT, CFG, ASY, PIPE, TPL, EXT, QLT, TST | VAL-001 / namespace integrity |

---

## Artefact & Methodology Integrity

**Block 8 — template as controlled artefact.** The audited front matter
(`.ai/audit/templates/audit-findings.md`) mandates: `phase`, `phase_name`, `id_prefix`,
`mode`, `report_status`, `severity_taxonomy`, `code_ref`. The input findings set all six
(plus `id_prefix: "TST"` matches the phase file's Report Output) — honest and present.
Per-finding mandated fields (Anchor, Disambiguator, Checked, Why this band, Problem, Repro,
Impact, Root cause, Fix, Verify, Related) are present in all seven — no field omitted or
renamed. The `severity_taxonomy` pointer
`.kilo/commands/audit/phases/11-audit-test-coverage.md#severity-taxonomy` resolves to a file
that exists and carries a `## Severity Taxonomy` heading. The `code_ref` (`d093d9c`) matches
`git rev-parse --short HEAD`. No template defect found.

**Block 9 — input rests on its own enumerated checks?** No. Phase 11's phase file enumerates
eight audit *blocks* (topics), not a numbered checklist gated as support. Each finding's
support is an independent observation — a reproduced `pytest` run, a `mypy` run, a grep, a
code read, or an FS-pollution probe — never "checklist X passed." No substantive finding
rests on a self-named phase check; the "GATES compliance check" at findings.md:556-564 is
methodology (anchor/symbol resolution), not a finding's support.

**Block 10 — the validated report as an artefact.** The disposition tally
(7 confirmed) agrees with the per-finding verdicts in the Findings table; no verdict is
withheld. Identifiers `TST-001`…`TST-007` are preserved, never renumbered. The audited
namespace (`TST`) and the validation namespace (`VAL`) never share a table — VAL-001 occupies
its own section with its own severity scale. Every claim that could not be settled in this
environment (live API calls; clean-env import-time `TypeError`; Jupyter `OutStream`) is
listed under **Not verified** with the reason. Confirmed unresolvable: 0; unsettled in this
report: 0.
```
