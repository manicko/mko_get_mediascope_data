---
phase: "12"
phase_name: "production-ops"
id_prefix: "OPS"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/12-audit-production-ops.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Production Operations, Security & Observability

## Summary

Six findings. Highest severity CRITICAL. Two CRITICALs undermine the release path itself: the project publishes a wheel that is **non-functional for new users** (its bundled `settings/` templates are omitted from the artifact, so `init` is a no-op and the console entry point cannot even be imported on a fresh install), and **nothing gates a release** (no CI/CD exists; `mypy` is red with 61 errors and is never enforced before `uv build`+`twine upload`). The remaining findings are an observability pipeline that fails to record terminal failure states, inaccurate README runbook claims, and version↔commit untraceability.

## Scope

**Examined:** packaging/build (`pyproject.toml`, `MANIFEST.in`, setuptools backend), controls (`ruff`/`mypy`/`pytest` invoked via `uv run`), release path (`uv build` → `dist/`, `twine` upload, no CI), observability (`settings/log_config.yaml`, `core/services.py` logging/`print`, `core/network.py` retry exhaustion, runtime `log.log`), README runbook, version control traceability (`git tag`/`describe`, `uv.lock`).
**Tools:** `ruff 0.15.1`, `mypy 1.19.1`, `pytest 9.0.2`, `uv`, `git`.
**Not verified:** the actually-published TestPyPI artifact (only a local `uv build` from committed config — the build is deterministic from `pyproject.toml`/`MANIFEST.in`, so locally-built artifacts are representative); CI behavior (none exists to verify). Bare `uv run -m`/`uv run python` invocations were intermittently affected by a sandbox path-resolution quirk on the `src/` overlay (while `uv run pytest` — the project's documented command — runs clean); this is an environmental artifact, not a code defect, and is **not** filed.

> ID-prefix note (`OPS-`): the phase file states the `OPS-` prefix is "already in use in shipped source, tests, and workflows from a prior cycle." A repo-wide `git grep "OPS-"` returns **zero** matches in tracked source/tests/workflows — the only hit is the phase instruction itself (line 169). There is **no collision**; `OPS-` is safe to mint. Prior phases used `ENT`/`CFG`/`ASY`/`PIPE`/`TPL`/`EXT`/`QLT`/`TST`/`VAL`.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| OPS-001 | CRITICAL | `pyproject.toml::tool.mypy` | No CI/CD gate; release ships manually with mypy RED (61 errors) |
| OPS-002 | CRITICAL | `MANIFEST.in::"recursive-include mko_get_mediascope_data *.json *.yaml"` | Published wheel omits `settings/` package data → fresh-install import crashes |
| OPS-004 | HIGH | `core/network.py::NetworkClient.call` | Retry exhaustion & worker-task failure are raised/swallowed without a terminal log or counter |
| OPS-003 | MEDIUM | `settings/log_config.yaml::loggers` | Configured named loggers are dead (`app`/`__main__`/`tests`); `core/services.py` progress uses `print()`, bypassing the log file |
| OPS-005 | MEDIUM | `README.md::"uv run -m mko_get_mediascope_data run settings/reports/nat_tv_brands_last.yaml"` | README runbook path has no match at repo root; `run` takes one file, docs claim "one or more" |
| OPS-006 | MEDIUM | `pyproject.toml::project.version` | Hardcoded version, zero git tags; no version↔commit traceability, deps declared as unpinned `>=` ranges |

#### OPS-001 — [CRITICAL] No CI/CD gate; release ships manually with mypy RED (61 errors)

**Anchor:** `pyproject.toml::tool.mypy`
**Disambiguator:** `mypy`
**Checked:** ran
**Why this band:** CRITICAL — "a control believed to be active that is not, on the path that decides whether a change ships or a release publishes, so that nothing distinguishes a vetted subject from an unvetted one." mypy is documented (`AGENTS.md` "Typecheck: uv run mypy <path>") and configured (`[tool.mypy]`) but is RED, and there is no CI on the publish path.
**Problem:** No `.github/`, `Makefile`, `Makefile.ps1`, or `.pre-commit-config.yaml` is committed. Release is a manual `uv build` + `twine upload` (dev deps include `twine>=5.0.0`). The documented controls are never invoked automatically: `uv run mypy src/` is red (61 errors in 7 source files; the test tree is excluded by the `mypy src/` invocation), `ruff` is green, and `pytest` is green only against ambient user config (see TST-002). A maintainer can publish regardless.
**Repro:**
```
$ uv run mypy src/
Found 61 errors in 7 files (checked 14 source files)
$ git ls-files | grep -E 'github|Makefile|pre-commit'   # (no matches)
$ uv run ruff check src/ tests/
All checks passed!
$ uv run pytest
12 passed in 20.49s
```
**Impact:** Maintainers publish wheels that fail type-checking; consumers/reviewers have no automated signal that a release was vetted.
**Root cause:** No CI configuration committed; release process is entirely manual.
**Fix:** Add `.github/workflows/ci.yml` (and `.pre-commit-config.yaml`) gating `ruff`, `mypy`, `pytest` on push/PR and blocking `twine upload`. Effort: medium.
**Verify:** new test needed: CI runs `mypy` over `src/` and `tests/` (close scope gap). Related: TST-004, TST-005, QLT-014.

#### OPS-002 — [CRITICAL] Published wheel omits `settings/` package data; fresh-install import crashes

**Anchor:** `MANIFEST.in::"recursive-include mko_get_mediascope_data *.json *.yaml"`
**Disambiguator:** `mko_get_mediascope_data`
**Checked:** ran
**Why this band:** CRITICAL — a release-shipping artifact is non-functional for its documented "install → init → run" flow; nothing on the publish path catches it (no CI; see OPS-001).
**Problem:** `MANIFEST.in` uses `recursive-include mko_get_medioscope_data *.json *.yaml`, but the package lives under `src/mko_get_medioscope_data/`, so the include matches nothing (build warns "no files found matching '*.yaml' under directory 'mko_get_mediascope_data'"). `include-package-data = true` does not compensate: `settings/` has no `__init__.py`, so under src-layout setuptools does not treat its contents as package data. The published wheel therefore contains **only `*.py` + dist-info — no `settings/`**. `init_project` copies from `APP_PATHS.app_settings_dir` (absent in the wheel) → copies nothing. Worse, `services.py:163` builds the `app_service` singleton at import time, which loads `log_config.yaml`/`app_config.yaml` from the user dir; for a fresh user these are absent → `yaml_to_dict` returns `None` → `LoggingSettings(**None)` → `TypeError` before `init` can run.
**Repro:**
```
$ uv build
warning: no files found matching '*.yaml' under directory 'mko_get_mediascope_data'
Successfully built mko_get_mediascope_data-1.0.8-py3-none-any.whl
$ uv pip install --python <fresh-venv> --no-deps dist/mko_get_mediascope_data-1.0.8-py3-none-any.whl
Installed 1 package in 90ms
$ <fresh-venv>/python -c "import mko_get_mediascope_data.core.paths as p,\
from pathlib import Path; d=Path(p.__file__).resolve().parent.parent;\
print((d/'settings').exists())"
False
$ <fresh-venv>/python -c "import platformdirs,tempfile;\
platformdirs.user_config_dir=lambda *a,**k:tempfile.mkdtemp();\
import mko_get_medioscope_data.app"
No such file or directory ... settings\log_config.yaml [Errno 2]
IMPORT FAILED: TypeError :: mko_get_mediascope_data.core.models.LoggingSettings()
  argument after ** must be a mapping, not NoneType
```
**Impact:** New users cannot install-and-run; the `mko-get-mediascope` entry point raises on a clean install; `init` is a silent no-op.
**Root cause:** packaging manifest path is wrong for src-layout; no build/CI check verifies wheel contents.
**Fix:** declare data correctly — add `[tool.setuptools.package-data]` with `mko_get_mediascope_data = ["settings/**/*.yaml", "settings/**/*.json"]` and fix `MANIFEST.in` to `recursive-include src/mko_get_mediascope_data *.yaml *.json`; add a CI step asserting `settings/log_config.yaml` is present in the built wheel. Effort: small.
**Verify:** new test needed: assert `settings/log_config.yaml` shipped in `uv build` wheel (block-4 gate). Related: ENT-002, ENT-003, CFG-003.

#### OPS-004 — [HIGH] Retry exhaustion & worker-task failure are raised/swallowed without a terminal log or counter

**Anchor:** `core/network.py::NetworkClient.call`
**Disambiguator:** `retry limit exceeded`
**Checked:** ran
**Why this band:** HIGH — "a failure state with no counter and no log, so a quiet tool is indistinguishable from a working one."
**Problem:** The terminal outcome of the retry control is raised as an unlogged `RuntimeError("Network retry limit exceeded")` (`network.py:126`) — per-attempt timeouts are logged as warnings, but the exhaustion that decides the task is dead is not recorded. `MediaReport.worker` (`reports.py:141-153`) has no try/except, so an exception kills the worker before `task_done()` (ASY-001); `processing_tasks` cancels workers without awaiting/gathering, so un-retrieved task exceptions surface only via asyncio's default stderr handler, never the dictConfig file.
**Repro:** runtime `log.log` shows retry cycles ending at "attempt 3/3: timeout" with no subsequent record for the terminal exhaustion (the `RuntimeError` is raised, not logged):
```
09-10-26 12:39:15 AM - WARNING - Network error (ConnectTimeout) attempt 3/3: timeout - mko_get_medioscope_data.core.network
09-10-26 12:39:21 AM - WARNING - Network error (ConnectTimeout) attempt 3/3: timeout - mko_get_medioscope_data.core.network
...no ERROR/exhaustion line follows; the run stalls or terminates silently
```
**Impact:** A run that exhausts retries or fails a task is indistinguishable from a slow/in-progress run in the configured logs; operators must infer failure from missing output.
**Root cause:** exhaustion and task body are outside the logging layer; no counters/metrics (EXT-007).
**Fix:** log ERROR before raising (`logger.error("Network retry limit exceeded after %d attempts", self._max_attempts)`); wrap `worker` body in try/except/finally logging + guaranteeing `task_done()`; add counters (EXT-007). Effort: medium.
**Verify:** new test needed: `NetworkClient.call` logs ERROR on exhaustion before raising; `worker` logs task failures and still calls `task_done`. Related: EXT-007, ASY-001, PIPE-002.

#### OPS-003 — [MEDIUM] Configured named loggers are dead; `core/services.py` progress bypasses the log file via `print()`

**Anchor:** `settings/log_config.yaml::loggers`
**Disambiguator:** `loggers`
**Checked:** ran (static + runtime log.log)
**Why this band:** MEDIUM — "degraded operability with a workaround"; report-lifecycle progress is observable on stdout but absent from the configured log file.
**Problem:** `log_config.yaml` declares named loggers `__main__`, `tests`, `app`, but no module registers a logger with those names — `core/*.py` all use `getLogger(__name__)` → `mko_get_medioscope_data.core.{network,reports,tasks,utils}`; `app.py`/`services.py` define no `app` logger (grep for `getLogger` in `src/mko_get_medioscope_data` returns only `tasks.py:7`, `utils.py:15`, `network.py:21`, `reports.py:24`). So the three named-logger configs never fire (real loggers fall through to root). Additionally `AppService.run_report` emits start/complete/progress markers with `print()` (`services.py:117,124,154`), which never reach `log.log` or the configured formatter. (Mirrors ENT-005 / CFG-010.)
**Repro:** run a report; `log.log` contains retry/error lines but the "Обработка стартована/завершена" markers are absent (those are `print()` → stdout only).
**Impact:** Operators/monitoring that tail the configured `log.log` miss report lifecycle events; the named-logger config is dead weight that masks the real logger names.
**Root cause:** logger names in `log_config.yaml` never aligned to actual module `__name__`; `print()` used for progress in a core module.
**Fix:** align `log_config.yaml` named loggers to the real module loggers (or rely on root), and replace `print()` in `core/services.py` with `logger.info(...)`. Effort: small.
**Verify:** existing `test_services.py::test_run_report_with_real_models`; new test: progress messages route to `mko_get_mediascope_data.core.services` logger. Related: ENT-005, QLT-017.

#### OPS-005 — [MEDIUM] README runbook invocation is inaccurate and non-reproducible from a clean checkout

**Anchor:** `README.md::"uv run -m mko_get_mediascope_data run settings/reports/nat_tv_brands_last.yaml"`
**Disambiguator:** `nat_tv_brands_last.yaml`
**Checked:** ran
**Why this band:** MEDIUM — "a documented procedure or claim that is inaccurate, but whose consequence an operator can work around."
**Problem:** README Quick Start step 3 and the Examples pass `settings/reports/nat_tv_brands_last.yaml` — a repo-root-relative path for which no `settings/` exists at the repo root (reports live under `src/mko_get_medioscope_data/settings/reports/` or, after `init`, under `~/.config/mko_get_medioscope_data/settings/reports/`). `run`'s Typer argument is declared `exists=True` (`app.py:42`), so the documented invocation is rejected. README also says "Run one or more reports from YAML file(s)" but `run --help` takes a single required `REPORT FILE`.
**Repro:**
```
$ Test-Path "settings/reports/nat_tv_brands_last.yaml"   # from repo root
False
$ uv run -m mko_get_medioscope_data run --help
Arguments: *    report      FILE  ... [required]   # single file, not "one or more"
```
**Impact:** A user following the README aborts at step 3; the "one or more files" claim misleads.
**Root cause:** README path assumed an in-tree/CWD context and was never validated against a clean checkout.
**Fix:** README should use the absolute user-config path (e.g. `~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.yaml`) and state `run` takes one file; add a CI/doc check invoking the documented command after `init`. Effort: trivial.
**Verify:** new test needed: README Quick Start `run` command succeeds from a fresh `init` in a clean checkout. Related: ENT-004.

#### OPS-006 — [MEDIUM] No version↔commit traceability; deps unpinned in the published artifact

**Anchor:** `pyproject.toml::project.version`
**Disambiguator:** `version`
**Checked:** ran
**Why this band:** MEDIUM — "degraded operability with a workaround"; rollback to a pinned version fetches the immutable wheel, but identity/traceability is unverifiable.
**Problem:** `version = "1.0.8"` is hardcoded; `project.dynamic` is unset; `git tag` is empty and `git describe --tags --always` returns only a commit hash (`d093d9c`). Each release is a manual `uv build`+`twine upload` with no record of which commit shipped. Published `METADATA` declares `Requires-Dist: …>=X.Y.Z` (lower bounds only); `uv.lock` (188 401 bytes) pins transitive deps but is dev-only and not shipped, so end-user `uv pip install` resolution can pull versions newer than the maintainer's locked environment tested.
**Repro:**
```
$ git tag                          # (empty)
$ git describe --tags --always
d093d9c
$ uv run python - <<'PY'
import tomllib
d = tomllib.load(open('pyproject.toml','rb'))
print('version =', d['project']['version'])
print('dynamic =', d['project'].get('dynamic'))
PY
version = 1.0.8
dynamic = None
```
**Impact:** Rollbacks are unverifiable against the forward path; dependency drift can reintroduce defects the maintainer's locked dev env would not surface.
**Root cause:** No VCS-based versioning or release tagging; no pinned runtime constraints for published artifacts.
**Fix:** adopt dynamic versioning from git tags (`setuptools-scm`/`hatch-vcs`) and tag releases; pin critical runtime deps or ship a constraints file; add a release job that tags then publishes. Effort: medium.
**Verify:** new test needed: CI fails when `git describe --tags` is empty at tag-time / when `uv.lock` is stale vs `pyproject.toml`. Related: OPS-001.

## Fix Order

- **Dependencies:** OPS-002 (packaging) and OPS-001 (CI gate) are independent; notably, the CI gate from OPS-001 would have caught OPS-002's empty `settings/` in the wheel if it existed. OPS-006 (versioning), OPS-003/OPS-004 (observability), OPS-005 (docs) are independent of one another.
- **Merge candidates:** OPS-001 + OPS-002 (both symptoms of "no build/CI check on the publish path"); OPS-003 + OPS-004 (observability).
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** OPS-002's root cause (`services.py:163` singleton import-time config load) overlaps ENT-002/ENT-003 (bootstrap); OPS-003/OPS-004 overlap QLT-017 (empty `core/logger.py`), EXT-007 (no counters), ASY-001/PIPE-002 (worker exceptions). These remain owned by their phases; OPS findings cite them as related, not replacements.
- **Rollout risk:** OPS-002 + OPS-001 fixes must keep `uv run pytest` green (12 passing); OPS-003's `print()`→`logger` change must keep `test_app.py`/`test_services.py` green. None of the fixes alter report-extraction math (phases 01-11).

## Appendices

### Appendix A — Control outputs (Phase 12, block 1)

```text
$ uv run ruff --version            -> ruff 0.15.1
$ uv run ruff check src/ tests/
All checks passed!
$ uv run mypy --version            -> mypy 1.19.1 (compiled: yes)
$ uv run mypy src/
Found 61 errors in 7 files (checked 14 source files)
$ uv run pytest
12 passed in 20.49s
```
Declared scopes: ruff `[tool.ruff.lint]` (src/tests); mypy `[tool.mypy]` (no `files` → invoked as `mypy src/`, **excludes tests**); pytest `[tool.pytest.ini_options]` (`testpaths=["tests"]`). A repo-wide `git ls-files` finds **no** `.github/`, `Makefile`, `Makefile.ps1`, or `.pre-commit-config.yaml` — i.e. no secret scanner and no merge gate exist.

### Appendix B — Built wheel omits `settings/` (Phase 12, block 4)

```text
$ uv run python - <<'PY'
import zipfile, glob
w = sorted(glob.glob('dist/*.whl'))[-1]
with zipfile.ZipFile(w) as z:
    for n in sorted(z.namelist()): print(n)
PY
mko_get_mediascope_data/__init__.py
mko_get_mediascope_data/__main__.py
mko_get_mediascope_data/app.py
mko_get_mediascope_data/core/__init__.py
... (errors.py, init_service.py, logger.py, models.py, network.py, paths.py,
      reports.py, services.py, tasks.py, utils.py)
mko_get_mediascope_data-1.0.8.dist-info/...   (METADATA, WHEEL, RECORD, entry_points.txt, top_level.txt)
```
No `settings/` entries. Build warnings: `warning: no files found matching '*.json' under directory 'mko_get_medioscope_data'` / `…'*.yaml'…`.

### Appendix C — Fresh-install import crash (Phase 12, block 4/7)

```text
$ uv pip install --python <fresh-venv> --no-deps dist/mko_get_medioscope_data-1.0.8-py3-none-any.whl
Installed 1 package in 90ms
$ <fresh-venv>/python -c "import platformdirs,tempfile;\
platformdirs.user_config_dir=lambda *a,**k:tempfile.mkdtemp();\
import mko_get_medioscope_data.app"
No such file or directory ... settings\log_config.yaml [Errno 2] ...
IMPORT FAILED: TypeError :: mko_get_mediascope_data.core.models.LoggingSettings()
  argument after ** must be a mapping, not NoneType
```
The `app_service` singleton (`services.py:163`) loads `log_config.yaml`/`app_config.yaml` from the (absent) user config at import; `utils.yaml_to_dict` swallows `FileNotFoundError` and returns `None`; `LoggingSettings(**None)` raises before `init` can run.

### Appendix D — Version traceability (Phase 12, block 7)

```text
$ git tag                          # (empty — no tags)
$ git describe --tags --always
d093d9c
$ uv run python - <<'PY'
import tomllib
d = tomllib.load(open('pyproject.toml','rb'))
print('version =', d['project']['version'])
print('dynamic =', d['project'].get('dynamic'))
PY
version = 1.0.8
dynamic = None
```
Published `METADATA`: `Version: 1.0.8`, `Requires-Dist: …>=X.Y.Z` (lower bounds). `uv.lock` pins (188 401 bytes) but is dev-only.

### Appendix E — `init` on an existing user directory (Phase 12, block 7)

`_copy_templates` (`init_service.py:23`) skips existing files when `force=False`; repeated `uv run -m mko_get_medioscope_data init` is idempotent (both runs print the settings path, no overwrites). However the copy is **non-atomic** (per-file `shutil.copy2`, no temp+rename) — an interrupted `init` leaves partial files (CFG-008). The bundled settings template filename `connections/mediascope_RENAME_.json` does not match the path the code reads (`connections/mediascope.json`, `paths.py:59`) — so even a successful `init` leaves credentials unusable (CFG-003, CFG-004).
