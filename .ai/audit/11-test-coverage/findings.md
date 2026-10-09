---
phase: "11"
phase_name: "test-coverage"
id_prefix: "TST"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/11-audit-test-coverage.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Test Coverage

## Summary

Seven findings across all four severity bands. The highest is CRITICAL: the only
end-to-end test stubs the entire report-execution pipeline (`create_report`) and the
network client together, so the suite is green while it cannot notice that a report
produces no CSV, hangs, or crashes on the period it is given. Three HIGHs follow: the
suite is not hermetic (it imports a singleton that writes into the real user-config
directory and depends on ambient initialized config), the doubles it does use remove the
boundaries the tests claim to cover, and no merge gate is actually loaded. The root
cause for every finding is one: the test layer doubles everything interesting and then
asserts only on the path the double returns.

## Scope

**Examined:** `tests/{conftest,test_app,test_models,test_network,test_reports,test_services,test_utils}.py`,
`src/mko_get_mediascope_data/{app.py,core/{models,network,reports,services,tasks,utils,paths,init_service,errors,logger}.py`,
`pyproject.toml` (`[tool.pytest.ini_options]`, `[tool.mypy]`, `[tool.ruff.lint]`),
`src/mko_get_mediascope_data/settings/{app_config,log_config,defaults/**}.yaml`,
`.kilo/rules/commands.md`, `AGENTS.md`, `tests/conftest.py` fixtures, git-tracked
symbol inventory (`git ls-files src` → 36 files), `.git/hooks` (all `.sample`),
`.ai/context/test-command-patterns.md`.

**Tools:** `uv run pytest` (12 collected), `uv run ruff check src tests`,
`uv run mypy src`, `uv run python` runtime probes, `git ls-files`, Grep/AST (content +
symbol search), `platformdirs.user_config_dir` probe, `Get-ChildItem` (git hooks).

**Not verified:** live Mediascope `MediaVortexTask` calls (no credentials — never
asserted by any test); a clean-environment run with an uninitialized user config dir
(`init` must run first, which is itself gated by the import-time singleton — ENT-003);
cross-OS `sys.stdout.reconfigure` behaviour in IPython (inferred from call site).

<!-- GATES — git ls-files confirms every cited src/ and tests/ path is tracked; `.github`,
Makefile, Makefile.ps1, .pre-commit-config.yaml are absent (see TST-004 evidence). -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TST-001 | CRITICAL | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.create_report` | Report-execution pipeline and its CSV/output effect are reached by zero assertions (create_report mocked) |
| TST-002 | HIGH | `src/mko_get_mediascope_data/core/services.py::app_service` | Suite is not hermetic: imports do FS I/O + dictConfig in the real user-config dir; green depends on ambient initialized config |
| TST-003 | HIGH | `tests/test_services.py::test_run_report_with_real_models` | Doubles remove the report-execution + network boundaries the only end-to-end test purports to cover |
| TST-004 | HIGH | `pyproject.toml::tool.ruff.lint` | No merge gate is loaded: ruff/mypy/pytest declared in pyproject but no CI/Makefile/hook enforces them |
| TST-005 | MEDIUM | `pyproject.toml::tool.mypy` | mypy is red (61 diagnostics), its scope excludes the test tree, no coverage gate exists, and the documented typecheck command is mis-installed |
| TST-006 | LOW | `tests/conftest.py::mock_network_client` | Declared-but-dead test-layer contracts: `mock_network_client` and `tmp_report_dir` fixtures and the `slow` marker are never invoked/used |
| TST-007 | LOW | `tests/conftest.py::dummy_df` | Duplicated and restated fixture values: two `dummy_df` defs, `id=="1"` mirrors the `kitId` default, enum values restated as bare literals |

#### TST-001 — [CRITICAL] Report-execution pipeline and its CSV/output effect are reached by zero assertions (create_report mocked)

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.create_report`
**Disambiguator:** `await self.processing_tasks()`
**Checked:** ran (pytest 12 passed) + static (code read + grep)
**Why this band:** Matches the CRITICAL criterion — "a decision the system acts on whose
effect no assertion anywhere can observe, where the unobserved effect is one a user or a
seller receives" (the CSV files / completion status sellers receive).
**Problem:** `create_report` (reports.py:164-167) is the entry to `get_period` →
`processing_tasks` → the `worker` loop (`send_task`/`wait_task`/`extract_data` →
`prepare_data` → `csv_to_file` → `task_done`) and every one of its branches. The only
end-to-end test patches it to a no-op: `mocker.patch("...TVMediaReport.create_report",
new_callable=AsyncMock)` (test_services.py:47-50), and simultaneously patches
`NetworkClient.call` (test_services.py:41-44) to a stub that returns a `MagicMock`
connection. So the suite asserts only that an export *path* is returned, never that a CSV
is written, a task is marked done, a skip is recorded, or a crash is surfaced. The suite
would stay green for PIPE-001 (silent CSV
skip), PIPE-002 (worker death / hang), PIPE-004 (zero-task silent success),
PIPE-006 (unbounded poll), PIPE-007 (no error YAML) — none of which it can notice.
**Repro:** Run `tests/test_services.py::test_run_report_with_real_models`; it invokes
`app_service.run_report(test_yaml)` with `create_report` replaced by an `AsyncMock` no-op and
`NetworkClient.call` replaced by a stub returning a `MagicMock` connection. Its only
assertions — `export_paths[0].name == "raw_data"` and `parts[-2] == "finance"` — pass
without a single `csv_to_file` or `task_done` having run. (Verified: after the run,
`…/output/finance/raw_data` exists but is empty.)
**Impact:** Sellers believe a report succeeded (app.py prints "✅ Отчёт успешно
завершён") while zero CSVs are produced, the worker hangs on a bad task, or a period
crash aborts with no file. The green suite hides every one of these.
**Root cause:** The test boundary doubles the subject under test (`create_report` +
`NetworkClient.call`) instead of one of its external collaborators, so the property
being tested (output produced) is unobservable by construction.
**Fix:** Exercise the real `create_report` against a faithfully-doubled
`MediaVortexTask`/`NetworkClient` (real strategies, real tasks, real `worker`) and assert
on `<export_path>/**/*.csv.gz` existing and `task_done` accounting; convert the path-only
assertions in `test_services.py` into output assertions. Target: add
`tests/test_reports_lifecycle.py`; extend `tests/test_services.py`.
**Verify:** existing `tests/test_services.py::test_run_report_with_real_models` (insufficient —
mocked `create_report`); new test needed: `test_create_report_writes_csv_and_marks_queue_done`
**Related:** PIPE-001, PIPE-002, PIPE-004, PIPE-006, PIPE-007 (phase 04 — not re-filed; this
finding is the coverage gap that lets them ship green)
**Evidence — create_report + NetworkClient.call patched in test_services:**
```text
tests/test_services.py:41-50:
    mocker.patch(
        "mko_get_mediascope_data.core.network.NetworkClient.call",
        side_effect=mock_call_side_effect,
    )
    mocker.patch(
        "mko_get_mediascope_data.core.reports.TVMediaReport.create_report",
        new_callable=AsyncMock,
    )
```

#### TST-002 — [HIGH] Suite is not hermetic: imports do FS I/O + dictConfig in the real user-config dir; green depends on ambient initialized config

**Anchor:** `src/mko_get_mediascope_data/core/services.py::app_service`
**Disambiguator:** `app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(...))`
**Checked:** ran (platformdirs probe)
**Why this band:** "a property the harness depends on that nothing holds it to" + "state
carried forward between runs" + process-wide state a test shares with production code.
**Problem:** Any test module that imports `mko_get_mediascope_data.app` (e.g. `test_app`)
or `core.services` (`test_services`) — and pytest collects both in one process — executes
`services.py:163` — `app_service = AppService(...)` — at module scope. `__init__` runs
`prepare_app()` (services.py:44-49), which accesses the `cached_property` `app_config`
(services.py:35-42) — reading `app_paths.log_config` and `app_paths.app_config` (two
YAML files under the real `platformdirs.user_config_dir`) and validating them with
Pydantic — and calls `ensure_file_parent`, creating directories under the real user
dir. The `logging.config.dictConfig(...)` call follows at module scope (services.py:166),
attaching a `RotatingFileHandler` writing `log.log` to the real user dir. Consequently the suite's green requires the user to have
already run `init` to materialise `~/.…/settings/{app_config.yaml,log_config.yaml,
defaults/}` (confirmed present); in a clean checkout the import raises
`TypeError: ... argument after ** must be a mapping, not NoneType` (ENT-003) and collection
fails. Additionally `test_services` resolves `export_folder` to the real
`~/…/output/` and calls `ensure_dir`, creating `output/finance/raw_data` (empty) in the
real user tree, and `check_done=True` reads whatever `.csv.gz` files a *previous* real run
left there.
**Repro:** `uv run python -c "import platformdirs;from pathlib import Path;print(platformdirs.user_config_dir('mko_get_mediascope_data'))"`
→ `C:\Users\Om\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data`. After
`uv run pytest`, that dir contains `log.log` (appended by the run's `dictConfig`),
`settings/{app_config.yaml,log_config.yaml,defaults,reports,connections}` (read from a
prior `init`) and `output/finance/raw_data` (empty — created by the run's `ensure_dir`),
all touching the real home directory.
**Impact:** A red run cannot be reproduced without also reproducing the operator's home
directory; `check_done` carry-forward makes task-skip behaviour depend on stale real
output; the root logger / file handler is mutated process-wide and shared by every test.
**Root cause:** Module-scope singleton instantiation with no lazy/factory indirection and no
hermetic fixture that redirects `AppPaths.user_dir` to `tmp_path`.
**Fix:** Provide a hermetic `AppService` fixture that injects a `PathResolver` rooted at
`tmp_path` and that does not call `dictConfig` at import; make the library entry
(`run_report`/`run_report_async`) construct `AppService` lazily inside the command, not at
module scope. Target: `tests/conftest.py` (new fixture) + `core/services.py` (defer
singleton).
**Verify:** new test needed: `test_suite_runs_against_tmp_user_dir_without_real_init`
**Related:** ENT-002, ENT-003 (phase 01)
**Evidence — user-config dir populated by a single pytest run:**
```text
USER_CONFIG_DIR = C:\Users\Om\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data
SETTINGS children: ['app_config.yaml', 'connections', 'defaults', 'log_config.yaml', 'reports']
output/finance/raw_data exists: True   (children: []  -> empty, created by test_services)
```

#### TST-003 — [HIGH] Doubles remove the report-execution + network boundaries the only end-to-end test purports to cover

**Anchor:** `tests/test_services.py::test_run_report_with_real_models`
**Disambiguator:** `mock_report_service.cats.get_availability_period = MagicMock(...)`
**Checked:** ran (grep over tests) + static (code read)
**Why this band:** "a decision exercised only through a stand-in for the thing it decides"
+ "boundary asserted from one side only."
**Problem:** The test doubles inventory (Appendix C) replaces — for the only end-to-end
test — the two boundaries that *are* the subject: `NetworkClient.call` (the retry/backoff/
semaphore pipeline in `core/network.py`) and `TVMediaReport.create_report` (the entire
worker→CSV pipeline). What each double removes and that no test observes anywhere:
(1) `NetworkClient.call` retry-vs-no-retry discrimination — `test_network.py` observes
only the retry-exhaustion path (`ConnectTimeout` → `RuntimeError`); the success-return
path, the `Semaphore` concurrency limit, the initial jitter sleep, and the immediate-raise
paths for `BadRequestError`/`AuthorizationError`/`FileNotFoundError`/`HTTPError 4xx` are
never asserted; (2) `create_report`/worker/CSV/task_done — zero assertions (TST-001); (3)
`MediaVortexTask` (the real Mediascope API client) — replaced by a `MagicMock`
(`mock_report_service`) whose `.cats.get_availability_period` returns a fabricated
`dummy_df`, removing the real availability-row contract (the code does
`df.loc[df["id"]==str(self.base_id)][["periodFrom","periodTo"]]...[0]` — an absent row is
an unobserved `IndexError`).
**Repro:** `grep -rn "create_report\|NetworkClient.call" tests/` → patched in
`test_services.py`; `grep -rn "MediaVortexTask" tests/` → one match only, a comment
(`test_services.py:25: # Fake MediaVortexTask`); the real client is never constructed
(`get_connection` is stubbed inside the patched `call` side-effect at test_services.py:36-39).
**Impact:** The retry contract, the worker contract, and the API-availability contract are
each exercised only through stand-ins, so a regression in any of them is invisible.
**Root cause:** The test strategy doubles the boundary the product exists to exercise
(network + execution) rather than doubling a leaf collaborator and letting the subject run.
**Fix:** Split the doubles: keep `MediaVortexTask` doubled (external, correct) but run the
real `NetworkClient` against a local HTTP stub for the retry cases; run the real
`create_report` against a doubled `MediaVortexTask` and assert on files/`task_done`. Target:
`tests/test_network.py` (add success/4xx/401/semaphore cases) + `tests/test_reports_lifecycle.py`.
**Verify:** existing `tests/test_network.py::test_network_retry_and_fail` (retry-exhaust
only); new test needed: `test_call_raises_immediately_on_bad_request`
**Related:** TST-001 (same root cause: subject-under-test doubled)
**Evidence — doubles present, real connection absent in tests:**
```text
tests/test_services.py:41-44  -> mocker.patch("...NetworkClient.call", side_effect=...)
tests/test_services.py:47-50  -> mocker.patch("...TVMediaReport.create_report", new_callable=AsyncMock)
tests/test_reports.py:23      -> report.network_client.call = AsyncMock(return_value=dummy_df)
grep "MediaVortexTask" in tests/ -> 1 match (comment only: test_services.py:25: # Fake MediaVortexTask)
```

#### TST-004 — [HIGH] No merge gate is loaded: ruff/mypy/pytest declared in pyproject but no CI/Makefile/hook enforces them

**Anchor:** `pyproject.toml::tool.ruff.lint`
**Disambiguator:** `select = ["E","F","I","B","UP"]`
**Checked:** ran (`git ls-files` / `Test-Path` / `Get-ChildItem`)
**Why this band:** "a gate declared and never loaded."
**Problem:** Static analysis, types, and tests are all *declared* in `pyproject.toml`
(`[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`) but nothing loads them on a
merge: there is no `.github/` directory, no `Makefile`/`Makefile.ps1`, no
`.pre-commit-config.yaml`, and every entry in `.git/hooks` is a `.sample` (0 active hooks).
So a commit that turns ruff red, that breaks tests, or that ships mypy's 61 diagnostics is
not stopped by automation — "loaded" only if a human runs `uv run <tool>` locally.
**Repro:** `Test-Path .github / Makefile / Makefile.ps1 / .pre-commit-config.yaml` → all
`False`; `Get-ChildItem .git/hooks | ? Name -notmatch '.sample'` → 0 items.
**Impact:** Quality is opt-in per developer; the green status the team trusts is not
guaranteed on CI and is not reproducible as a gate.
**Root cause:** No CI workflow or hook was ever wired; the tooling config is local-only.
**Fix:** Add a minimal GitHub Actions workflow (or Makefile) that runs
`ruff check src tests` and `uv run pytest` (and optionally `mypy src`) on push/PR, and add
a `pre-commit` config for `ruff`/`mypy`/`pytest --co`. Target: `.github/workflows/ci.yml`
+ `.pre-commit-config.yaml` + `Makefile.ps1` targets.
**Verify:** new test needed: `test_merge_gate_runs_ruff_and_pytest` (a CI smoke)
**Related:** TST-005 (mypy red), TST-001 (tests don't guard the pipeline)
**Evidence — gate-loading inventory:**
```text
.github exists: False
Makefile exists: False
Makefile.ps1 exists: False
.pre-commit-config.yaml exists: False
active (non-.sample) git hooks: 0
pyproject declared: [tool.ruff.lint] select=[E,F,I,B,UP]; [tool.mypy]; [tool.pytest.ini_options] testpaths=["tests"]
```

#### TST-005 — [MEDIUM] mypy is red (61 diagnostics), its scope excludes the test tree, no coverage gate exists, and the documented typecheck command is mis-installed

**Anchor:** `pyproject.toml::tool.mypy`
**Disambiguator:** `disallow_any_generics = true`
**Checked:** ran (`uv run mypy src`, `import pytest_cov`)
**Why this band:** "a measurement whose effective scope is not the scope it is read for"
(mypy run scope) and "a gate … over a known accepted-noise class" (mypy diagnostics are a
mix of config-driven noise and genuine errors, but the count is not the finding — the
scope/loaded-state is).
**Problem:** `uv run mypy src` exits 1 with "Found 61 errors in 7 files (checked 14 source
files)" — `src` only, so **the entire test tree is outside the type-check scope**. The
noise is partly config-driven: `import-untyped` for `yaml`/`requests`/`dateutil` (no stub
packages in dev deps — only `pandas-stubs` is declared) and `type-arg` (ungenericified
`dict`/`list`, driven by `disallow_any_generics = true`); the remainder are genuine logic
diagnostics, e.g. `core/utils.py:25` reassigning a `str` param to a `date`, and
`core/reports.py:241` `Item "None" of "LastTimeModel | None" has no attribute "time_unit"`
(the exact line PIPE-003 reports as a crash — unguarded in the type check). Separately,
there is **no coverage gate**: `pytest-cov` is not installed and `pyproject.toml` defines
no `[tool.coverage]` or `--cov` addopts, so "line coverage" is never produced or measured.
Finally, `.kilo/rules/commands.md:35` declares `uv run basedpyright <path>` as the
typecheck command, but `basedpyright` is neither installed nor configured (no
`[tool.basedpyright]`, no dev dep) — `mypy` is the de-facto tool, so the documented
typecheck command fails if a developer follows it.
**Repro:** `uv run mypy src` → EXIT 1, "Found 61 errors"; `uv run python -c "import
pytest_cov"` → `ModuleNotFoundError: No module named 'pytest_cov'`; `uv run python -c
"import basedpyright"` → `ModuleNotFoundError`.
**Impact:** Type regressions in production code ship silently (no gate); test code is
never type-checked; coverage is never measured, so the "12 tests pass" signal has no
denominator; the typecheck instruction in the rules does not work.
**Root cause:** mypy scope pinned to `src` (no `files`/test inclusion), stub packages not
declared, `pytest-cov` not declared, and `basedpyright` referenced in rules without being
set up.
**Fix:** Set `[tool.mypy] files = ["src", "tests"]` (or add a `make typecheck` that runs
`mypy src tests`), add `types-PyYAML`/`types-requests`/`types-python-dateutil` to dev deps
(or relax `disallow_any_generics`), add `pytest-cov` + a `[tool.coverage]` + `--cov` in
addopts, and align the typecheck command in `.kilo/rules/commands.md` to `mypy`. Target:
`pyproject.toml` + `.kilo/rules/commands.md`.
**Verify:** new test needed: `test_mypy_covers_tests_and_passes` (run `mypy src tests` in CI)
**Related:** TST-004 (no gate loaded)
**Evidence — mypy (abbreviated):**
```text
$ uv run mypy src
src\mko_get_mediascope_data\core\utils.py:25: error: Incompatible types in assignment (expression has type "date", variable has type "str")  [assignment]
src\mko_get_mediascope_data\core\reports.py:241: error: Item "None" of "LastTimeModel | None" has no attribute "time_unit"  [union-attr]
src\mko_get_mediascope_data\app.py:17: error: Item "TextIO" of "TextIO | Any" has no attribute "reconfigure"  [union-attr]
Found 61 errors in 7 files (checked 14 source files)   # <-- 14 src files, 0 test files
```

#### TST-006 — [LOW] Declared-but-dead test-layer contracts: `mock_network_client`, `tmp_report_dir`, and the `slow` marker are never invoked

**Anchor:** `tests/conftest.py::mock_network_client`
**Disambiguator:** `def mock_network_client():`
**Checked:** ran (grep over `tests/`)
**Why this band:** "a helper nothing calls is a question of purpose" + "a marker registered
and inert"; no runtime consequence today.
**Problem:** Three declared test-layer contracts are defined and never used: the
`mock_network_client` fixture (conftest.py:9-13) and `tmp_report_dir` fixture
(conftest.py:23-27) are never requested as a fixture argument by any test function (the only
references are their own definitions and the fixture list in `AGENTS.md:71`); the `slow`
marker (pyproject `[tool.pytest.ini_options] markers`: `"slow: marks tests as slow"`) is
registered but `@pytest.mark.slow` appears nowhere in `tests/`. A registered-but-unused
marker and two unused fixtures make the test layer's self-description ("here is how you
mock the network / stub a report dir / mark slow tests") contradict what the tests actually
do.
**Repro:** `grep -rn "mock_network_client\|tmp_report_dir\|pytest.mark.slow" tests/` →
match only in `conftest.py` (definitions) for the fixtures; `grep -rn "@pytest.mark.slow"
tests/` → no matches.
**Impact:** Low — dead config; a contributor trusting `AGENTS.md`'s fixture list will reach
for helpers that don't exist in any test, and `slow` gives false confidence a slow-suite
filter exists.
**Root cause:** Fixtures/markers added speculatively and never wired.
**Fix:** Either add a test that consumes each fixture/marker, or remove the unused
declarations (and the `AGENTS.md` list entry that references them). Target:
`tests/conftest.py` + `pyproject.toml::tool.pytest.ini_options.markers` + `tests/test_*`
(add usage).
**Verify:** existing `tests/test_network.py::test_network_retry_and_fail` builds its own
`NetworkClient` (ignores `mock_network_client`); new test needed:
`test_network_retry_uses_fixture` (or delete the fixture)
**Related:** TST-007 (fixture data contract)
**Evidence — fixture/marker usage:**
```text
conftest.py:10: def mock_network_client():   ...  (used by: NONE)
conftest.py:24: def tmp_report_dir(tmp_path): ...  (used by: NONE)
conftest.py:17: def dummy_df():             ...  (used by: test_reports.py::mock_report)
pyproject markers: ["slow: marks tests as slow (use -m slow to run)"]
@test pytest.mark.slow in tests/ -> (no matches)
```

#### TST-007 — [LOW] Duplicated and restated fixture values: two `dummy_df` defs; `id=="1"` mirrors the `kitId` default; enum values restated as bare literals

**Anchor:** `tests/conftest.py::dummy_df`
**Disambiguator:** `{"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}`
**Checked:** ran (code read + grep)
**Why this band:** Fixture-data contract with no runtime consequence today but two homes for
one value, and a helper absorbing a production default.
**Problem:** (1) `dummy_df` is defined in `conftest.py:17-20` *and* re-declared as a local
in `test_services.py:21-23` with identical content — a duplicated helper with divergent
maintenance paths (the two copies could drift). (2) Both hard-code `id: "1"`, which is
only correct because `TVMediaReport.base_id` falls back to `data_settings.options.get(
"kitId", 1)` = `1` (reports.py:189); the test therefore only ever validates the
default-kitId path and cannot detect a non-default `kitId` mismatch — the availability-row
lookup (`df.loc[df["id"]==str(self.base_id)]`) is exercised against a fabricated row whose
key only lines up with the default. (3) `tv_yaml_content` (conftest.py:31-50) and
`test_models.test_report_settings_valid` restate the `MediaType`/`ReportType`/
`ReportSubtype` enum values as bare literals (`"TV"`, `"crosstab"`,
`"TOP_NAT_TV_ADVERTISERS"`, `"DYNAMICS_BY_SPOTS"`) instead of referencing the enums, so a
rename of an enum member would leave the tests green while production breaks. (4)
`data_lang` is fixed at `"ru"` everywhere, so the `en` path in `merge_data_settings`
(services.py:90-91 `en_to_ru` guard) is never exercised.
**Repro:** `grep -rn 'id.*1\|kitId' tests/conftest.py tests/test_services.py` → `id "1"`
literal; `grep -n 'options.get("kitId", 1)' core/reports.py` → reports.py:189.
**Impact:** Low today, but the duplicated `dummy_df` and bare-literal enums are silent
drift risks; the kitId/id coupling masks a real availability-row miss; `data_lang="en"` is
untested.
**Root cause:** Fixture values authored as literals instead of derived from the production
defaults/enum; `dummy_df` copy-pasted rather than shared.
**Fix:** Have `dummy_df` build the `id` from the same `kitId` default the product uses (or
parametrise it), reference `MediaType.TV` etc. instead of literals, and add a
`data_lang="en"` case. Target: `tests/conftest.py` + `tests/test_models.py`.
**Verify:** existing `tests/test_models.py::test_report_settings_valid` (uses literals);
new test needed: `test_data_lang_en_does_not_translate`
**Related:** TST-006 (unused fixtures); PIPE-005 (availability-row contract)
**Evidence — two dummy_df defs + kitId default:**
```text
conftest.py:17: def dummy_df():
conftest.py:18-20:   return pd.DataFrame({"id": ["1"], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]})
test_services.py:21-23: dummy_df = pd.DataFrame({"id": ["1"], ...})   # identical local copy
reports.py:189: self.base_id = self.data_settings.options.get("kitId", 1)   # default == "1"
```

## Fix Order

- **Dependencies:** TST-004 (load a gate) should precede trusting TST-005's number; TST-003
  and TST-001 share the root cause (the subject under test is the thing doubled) and are
  fixed together — splitting the doubles so the real `create_report`/`NetworkClient` run
  while only the external `MediaVortexTask` is stubs.
- **Merge candidates:** TST-001 + TST-003 (both are the "create_report + call are mocked"
  doubling in `test_services`); TST-002 + TST-003 (both depend on the singleton/hermeticity
  rewrite).
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** TST-001 deliberately does NOT re-file PIPE-001/002/004/006/007
  (owned by phase 04); it records that phase 04's defects survive green. TST-002/TST-007
  cross-reference ENT-002/003 (phase 01). TST-005's `reports.py:241` diagnostic is the
  type-level view of PIPE-003; both stand.
- **Rollout risk:** Fixing TST-001 by un-mocking `create_report` will turn
  `test_services.py::test_run_report_with_real_models` red until the real pipeline is
  driven by a doubled `MediaVortexTask` — that test is the intended casualty and must stay
  green only after the new `test_reports_lifecycle.py` asserts on output. Fixing TST-002
  by deferring the singleton must keep `test_services` green (it imports `app_service`).

## ID-Conflict Note

The phase file warns the `TST-` prefix is "already in use in shipped source and operations
documentation from a prior cycle, with at least one identifier already resolving to two
different findings; check for a collision before minting an ID and report it rather than
creating a second namespace." A full-tree search for `TST-` (Grep over `src/`, `tests/`,
`.ai/audit/`, `docs/`, `.kilo/`) returned **no** prior `TST-` identifiers — the only hit is
the phase file's own instruction. The existing prefix namespaces in `.ai/audit` are `ENT-`
(phase 01), `CFG-` (phase 02), `PIPE-` (phase 04), `ASY-` (phase 03), `API-` (phase 09),
`TPL-` (phase 05) — none collide with `TST-`. No collision was found; IDs minted here
(`TST-001`…`TST-007`) use a single namespace.

## Appendices

### Appendix A — Decision set and per-decision assertion verdict (block 1)

| Production decision | Asserted? | Covering test | Notes |
|---|---|---|---|
| `ReportSettings` rejects media/report_type/data_lang not in enum | partial | test_models (`test_report_settings_valid` only valid case) | no invalid-media/invalid-lang rejection test |
| `PeriodModel`: both empty rejected | yes | test_models | `PeriodModel()` raises |
| `PeriodModel`: date_filter length≠2 rejected | **no** | — | — |
| `get_period`: date_filter-only (`last_time=null`) | **no** | — | crashes PIPE-003; fixture uses this exact config |
| `get_period`: out-of-range clamp + warn | **no** | — | PIPE-005 |
| `ReportFactory.task_strategy`: TV_REG→TVRegTaskStrategy | **no** | — | only TV path tested (via mock_report, not factory) |
| `ReportFactory.data_strategy`: DICT dispatch | partial | test_reports (`DictDataStrategy` tested, not the factory branch) | `"DICT" in subtype.value` untested |
| `worker`: `task_done` on success | **no** | — | create_report mocked |
| `worker`: exception isolation around task body | **no** | — | PIPE-002 |
| `csv_to_file`: one write → file exists | yes | test_utils (`test_csv_to_file`) | mode `"x"` swallow (PIPE-001) not tested |
| `extract_data`: empty df → status=False, log_error | **no** | — | PIPE-009 |
| `DefaultTVTaskStrategy`: done_files skip logs warning | partial | test_reports (`test_default_task_strategy`, but `done_files={}`) | skip branch not exercised |
| `TVRegTaskStrategy`: done_files skip (no log) | **no** | — | PIPE-011 |

One decision whose effect no assertion can observe: `worker` calls `task_done()` and
`csv_to_file` writes the seller-visible CSV (reports.py:141-153, 325-332). One test whose
assertions survive the decision being inverted: `test_services.py::test_run_report_with_real_models`
passes with `create_report` mocked to a no-op `AsyncMock` (no CSV written, no `task_done`
called) — its only assertions (`export_paths[0].name == "raw_data"`,
`parts[-2] == "finance"`) hold against the mock's return value, not against any output.

### Appendix B — Gate-loading / harness facts (blocks 5–7)

```text
# pytest (declared + green locally)
configfile: pyproject.toml   addopts: --import-mode=importlib -ra -v --strict-markers --asyncio-mode=auto
markers: ["slow: marks tests as slow (use -m slow to run)"]   # registered, never used
plugins: asyncio-1.3.0, mock-3.15.1
collected 12 items -> 12 passed in 13.20s
pythonpath = ["src"]   testpaths = ["tests"]   python_files = ["tests.py","test_*.py"]

# ruff (declared + green locally)
Select: E, F, I, B, UP  (ignore E501, I001)   ->  "All checks passed!"

# mypy (declared + RED; scope = src only)
[tool.mypy] follow_imports=silent ignore_missing_imports=true disallow_any_generics=true
  check_untyped_defs=true no_implicit_reexport=true strict_optional=true
  ->  "Found 61 errors in 7 files (checked 14 source files)"   # 0 test files checked
  noise class: [type-arg] (disallow_any_generics) + [import-untyped] (yaml/requests/dateutil stubs absent)
  genuine logic: utils.py:25 date<str reassign; reports.py:241 None.time_unit (PIPE-003); app.py:17 TextIO.reconfigure (ENT-001)

# tracked sources (git ls-files)
git ls-files src  -> 36 files
git ls-files tests -> 7 files (conftest.py + test_app/test_models/test_network/test_reports/test_services/test_utils.py)

# coverage (NOT declared; not installed)
import pytest_cov -> ModuleNotFoundError: No module named 'pytest_cov'   -> no --cov, no [tool.coverage]

# CI / make / hook (NONE)
.github = False ; Makefile = False ; Makefile.ps1 = False ; .pre-commit-config.yaml = False
active (non-.sample) git hooks = 0

# typecheck command declared vs installed
.kilo/rules/commands.md:35  -> "uv run basedpyright <path>"   (de-facto canonical)
basedpyright installed/configured? -> ModuleNotFoundError: No module named 'basedpyright'
mypy (dev dep, pyproject [tool.mypy]) -> 1.19.1   -> red
```

### Appendix C — Double inventory and the property each removes (block 2)

| Double (what is replaced) | Replaces | Property removed that NO test observes |
|---|---|---|
| `tests/test_services.py:41` `NetworkClient.call` patched (side_effect) | real retry/backoff/semaphore call pipeline | success-return path, `Semaphore` cap, initial jitter sleep, no-retry on `BadRequestError`/`AuthorizationError`/`FileNotFoundError`/`HTTPError-4xx`; only retry-exhaust observed (test_network) |
| `tests/test_services.py:47` `TVMediaReport.create_report` → `AsyncMock` | entire worker→send→wait→extract→prepare→csv→task_done | CSV written, `task_done()` accounting, status propagation, error YAML emission |
| `tests/test_services.py:26` `mock_report_service` `MediaVortexTask` | real Mediascope API client | real availability-row contract (`df.loc[df["id"]==str(base_id)][0]` → IndexError if absent) |
| `tests/test_reports.py:11` `mock_report` `MagicMock` | real `Report`/`Data`/`TVMediaReport` | `DefaultTVTaskStrategy` works against a real `Data` (mock `model_dump()` returns `{}`) |
| `tests/test_models.py` literals vs enums | `MediaType`/`ReportType`/`ReportSubtype` values | enum-member rename drift (tests assert `"TV"`/`"crosstab"`, not `MediaType.TV`) |

One property observed by no test: the successful `NetworkClient.call` return path
(`asyncio.to_thread(func, *args)` returning the function's result) — `test_services` patches
`call` itself; `test_network` only covers retry-exhaustion. One stand-in for a shared
resource: the module-level `app_service` singleton (`services.py:163`) held process-wide and
shared by every test; the real `~/.config/.../output/` tree + root logger are the underlying
shared resources it points at.

### Appendix D — Fixture data contract (block 3)

Defaults manufactured by the helper layer and whether the product can produce each:
- `dummy_df` → `{"id":["1"],"periodFrom":["2025-01-01"],"periodTo":["2025-12-31"]}`.
  Producible IFF `data_settings.options.kitId` defaults to `1` (services.py:189). For any
  non-default `kitId` the produced availability row would not match `id=="1"` → unobserved
  `IndexError` in `get_available_period`. Call-site count for the unproducible case: 0 tests
  cross that boundary (the only consumer is `test_services`, which patches `call` so
  `get_available_period` never executes against the dummy).
- `tv_yaml_content` `data_lang: "ru"` (default) — `en` branch (services.py:90-91
  `en_to_ru`) never exercised.
- `mock_report.data_settings.model_dump.return_value = {}` absorbs the `Data` schema
  (`options`/`slices`/`statistics` lists) — no test must satisfy it (block 3 "constraint
  the helper absorbs").
- Restated fixed values: `"TV"`, `"crosstab"`, `"TOP_NAT_TV_ADVERTISERS"`,
  `"DYNAMICS_BY_SPOTS"` restate `MediaType.TV.value`/`ReportType.crosstab.value`/enum
  values (models.py:40-58) as bare literals in `tv_yaml_content` and
  `test_models.test_report_settings_valid`.
- Two helper universes for the availability dummy agree in value (no divergence yet) but
  exist as two definitions (conftest `dummy_df` + test_services local `dummy_df`); the only
  assertion testing that value is `test_reports::test_default_task_strategy` (via
  `mock_report.network_client.call = AsyncMock(return_value=dummy_df)`), so both copies are
  tested against the same fixture — the risk is the silent-duplication drift, not today's
   behaviour.

---

## Verification Pass — Re-execution at code_ref d093d9c (HEAD)

Re-ran the suite and gate tooling at commit `d093d9c` (current HEAD) to confirm all
seven findings remain accurate. Three precision-only corrections were applied during
this pass (see below); no finding was re-graded, withdrawn, or had its consequence
altered.

### Corrections applied (locator precision only)

1. **TST-002 — `dictConfig` attribution:** `prepare_app()` (services.py:44-49) reads config
   via the `app_config` cached_property (services.py:35-42) and calls `ensure_file_parent`;
   the `logging.config.dictConfig(...)` call is at module scope (services.py:166), not
   inside `prepare_app`. Consequence unchanged.
2. **TST-003 — `MediaVortexTask` grep:** one match exists in `tests/`
   (`test_services.py:25: # Fake MediaVortexTask` — a comment). The real client is still
   never constructed; `get_connection` is stubbed at test_services.py:36-39.
3. **TST-007 — `base_id` locator:** `self.base_id = self.data_settings.options.get("kitId", 1)`
   is at **reports.py:189**, not `services.py:189` (services.py is 166 lines).
   Grep verified: `reports.py:189`.

### Runtime evidence (reproduced)

```text
$ uv run pytest
collected 12 items
12 passed in 19.25s

$ uv run ruff check src tests
All checks passed!

$ uv run mypy src
Found 61 errors in 7 files (checked 14 source files)   # 0 test files in scope; mypy 1.19.1

$ uv run python -c "import pytest_cov"
ModuleNotFoundError: No module named 'pytest_cov'

$ uv run python -c "import basedpyright"
ModuleNotFoundError: No module named 'basedpyright'
```

### FS-pollution re-probe (TST-002)

```text
USER_DIR = C:\Users\Om\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data
log.log exists: True   size: 5557 bytes   # grew from 5179 — appended every pytest run
settings children: ['app_config.yaml', 'connections', 'defaults', 'log_config.yaml', 'reports']
output/finance/raw_data exists: True   children: []   # empty, created by test_services
```

### GATES compliance check (phase §GATES)

1. Every cited `src/` and `tests/` path appears in `git ls-files` (36 src, 7 tests).
2. Every symbol resolves: `create_report` (reports.py:164, inherited by TVMediaReport),
   `app_service` (services.py:163), `base_id` (reports.py:189), `mock_network_client`
   (conftest.py:10), `dummy_df` (conftest.py:17), `tool.ruff.lint` (pyproject.toml:106),
   `tool.mypy` (pyproject.toml:135).
3. No `:<digits>` in any Anchor/Disambiguator field — all use `path::Symbol` format.
4. Every "we ran X" claim above has pasted output in a fence.

### Dispositions — all **confirmed** unchanged at d093d9c

| ID | Severity | Verdict |
|---|---|---|
| TST-001 | CRITICAL | confirmed — `create_report` + `NetworkClient.call` mocked in test_services.py:41-50; only path assertions (lines 56-58) |
| TST-002 | HIGH | confirmed — module-scope singleton (services.py:163) + module-scope `dictConfig` (services.py:166); log.log 5557 B + empty output dir |
| TST-003 | HIGH | confirmed — `test_run_report_with_real_models` doubles both subject boundaries |
| TST-004 | HIGH | confirmed — no .github/Makefile/Makefile.ps1/.pre-commit-config.yaml; 0 active hooks |
| TST-005 | MEDIUM | confirmed — mypy 1.19.1 red (61 in 7 src files, 0 tests); pytest-cov/basedpyright absent from dev deps |
| TST-006 | LOW | confirmed — `mock_network_client`, `tmp_report_dir`, `slow` marker never invoked |
| TST-007 | LOW | confirmed — duplicate `dummy_df` (conftest.py:17 + test_services.py:21); `id=="1"` couples to reports.py:189 `kitId` default |
