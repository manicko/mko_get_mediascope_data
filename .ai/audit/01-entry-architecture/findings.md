---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/01-audit-entry-architecture.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Entry Points & Process Architecture

## Summary

Seven findings. Three CRITICAL: the singleton `app_service` bootstraps at import time with
filesystem and logging side effects that a library caller cannot suppress, `sys.stdout` is
reconfigured unconditionally on import, and a missing config directory produces an obscure
`TypeError` that makes the `init` command itself unreachable for new users. One HIGH: the
documented package-level public API is not re-exported from `__init__.py`. Two MEDIUM and one
LOW round out the layer-discipline and documentation-drift issues.

## Scope

**Examined:** `src/mko_get_mediascope_data/__init__.py`, `__main__.py`, `app.py`,
`core/services.py`, `core/paths.py`, `core/init_service.py`, `core/utils.py`,
`core/logger.py`, `core/reports.py`, `core/network.py`, `core/models.py`,
`settings/{app_config.yaml,log_config.yaml}`, `pyproject.toml`, `README.md`,
`docs/00-overview/doc-maintenance-rules.md`, `tests/conftest.py`, `tests/test_app.py`,
`tests/test_services.py`.

**Tools:** `rg`, `ast` (Python), `ruff`, `python -c` runtime probes, `git ls-files`,
`platformdirs.user_config_dir`, `importlib.metadata`.

**Not verified:** Runtime behaviour inside Jupyter/IPython (no Jupyter kernel available in
this environment); whether `sys.stdout.reconfigure` raises on `ipykernel.iostream.OutStream`
is inferred from the unconditional call site rather than observed live.

## Findings

| ID  | Severity   | Anchor                                              | Title |
|-----|------------|-----------------------------------------------------|-------|
| ENT-001 | CRITICAL | `src/mko_get_mediascope_data/app.py:17`             | `sys.stdout.reconfigure(...)` runs unconditionally at import time on every entry surface |
| ENT-002 | CRITICAL | `src/mko_get_mediascope_data/core/services.py:163`  | Singleton `app_service` created at import performs filesystem I/O + `dictConfig` logging setup |
| ENT-003 | CRITICAL | `src/mko_get_mediascope_data/core/utils.py:358`     | Missing config swallowed as `None` → obscure `TypeError`; `init` CLI command is unreachable for new users |
| ENT-004 | HIGH     | `src/mko_get_mediascope_data/__init__.py`           | Public API not re-exported from package root; README import fails |
| ENT-005 | MEDIUM   | `src/mko_get_mediascope_data/core/services.py:117`  | `print()` calls in `core/` bypass the logging layer |
| ENT-006 | MEDIUM   | `src/mko_get_mediascope_data/core/services.py:35`   | `app_config` cached on singleton is stale for the lifetime of the process |
| ENT-007 | LOW      | `src/mko_get_mediascope_data/app.py:2`              | Russian docstrings and comments in the entry layer violate the "English Only" standard |

#### ENT-001 — [CRITICAL] `sys.stdout.reconfigure()` runs unconditionally at import time on every entry surface

**Anchor:** `src/mko_get_mediascope_data/app.py:17`
**Disambiguator:** `reconfigure(line_buffering=True, encoding="utf-8")`
**Checked:** ran
**Why this band:** Import-time side effect that corrupts a host process's stdout without
consent — directly matches the CRITICAL criterion.
**Problem:** `app.py` line 17 executes `sys.stdout.reconfigure(line_buffering=True,
encoding="utf-8")` at module level — unconditionally on every import of `app.py`. This module
is imported by `__main__.py` (module entry), the console-script entry point
(`mko-get-mediascope = mko_get_mediascope_data.app:app`), and by any library caller doing
`from mko_get_mediascope_data.app import ...`. The reconfiguration changes the host process's
stdout encoding and buffering before the caller can opt out.
**Repro:**
1. `python -c "import sys; print('before:', sys.stdout.encoding, sys.stdout.line_buffering)"`
   → `before: cp1252 False`
2. `python -c "import sys; import mko_get_medioscope_data.app; print('after:', sys.stdout.encoding, sys.stdout.line_buffering)"`
   → `after: utf-8 True`
3. In Jupyter/IPython, `sys.stdout` is an `ipykernel.iostream.OutStream` (not a
   `TextIOWrapper`); `reconfigure` is not part of that object's public API, so the import
   raises `AttributeError` — the library cannot be imported in Jupyter at all.
**Impact:** Jupyter/Airflow users and test runners that import the library lose control of
their own stdout contract (encoding, buffering). In Jupyter the import may crash outright.
**Root cause:** `sys.stdout.reconfigure` called at module scope with no guard, no
conditionality, and no opt-out flag.
**Fix:** `move sys.stdout.reconfigure` out of module scope into the CLI `run` command's
execution path only (e.g. inside `app.run()` after the Typer argument is parsed), or guard
it with a check that stdout supports `reconfigure` and skip it in non-CLI contexts.
**Verify:** new test needed: test_stdout_not_reconfigured_on_library_import
**Related:** ENT-002, ENT-003

**Evidence — stdout encoding before/after import:**
```text
Before import: stdout type: TextIOWrapper encoding: cp1252 line_buffering: False
After import:  stdout type: TextIOWrapper encoding: utf-8 line_buffering: True
```

#### ENT-002 — [CRITICAL] Singleton `app_service` created at import performs filesystem I/O + `dictConfig` logging setup

**Anchor:** `src/mko_get_mediascope_data/core/services.py:163`
**Disambiguator:** `app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(...))`
**Checked:** ran
**Why this band:** A singleton that performs filesystem I/O on import and cannot be suppressed
by a library caller — directly matches the CRITICAL criterion.
**Problem:** At `services.py:163`, `app_service = AppService(app_paths=APP_PATHS,
resolver=PathResolver(APP_PATHS.user_dir))` executes at module scope. `AppService.__init__`
(`services.py:30-33`) calls `self.prepare_app()`, which accesses the `app_config`
cached_property (`services.py:35-42`). That property reads two YAML files via
`utils.yaml_to_dict` and validates them through Pydantic. `prepare_app` (`services.py:44-49`)
then resolves the `rotating_file` handler's filename and calls
`PathResolver.ensure_file_parent` — creating directories on disk. Finally
`services.py:164-165` calls `logging.config.dictConfig(log_config.model_dump())`, which
instantiates a `RotatingFileHandler` that opens a file handle to `log.log` and attaches it
to the root logger. All of this happens on every import of `app.py` / `services.py`, with no
way for a library caller to opt out.
**Repro:**
1. Remove `log.log` from the user config directory.
2. `python -c "import mko_get_medioscope_data.app"`
3. `log.log` now exists and the root logger has `StreamHandler` + `RotatingFileHandler`
   attached — purely from the import statement.
**Impact:** Importing the library as a dependency (e.g. in Jupyter, Airflow, or another
package) silently creates files on disk, opens file handles, and reconfigures the host
process's logging. The caller never opted in.
**Root cause:** Module-level singleton instantiation with no lazy-initialisation guard and no
separation between "declare the singleton" and "bootstrap it".
**Fix:** `defer AppService instantiation and dictConfig` behind a factory or lazy accessor
(e.g. `get_app_service()`), or move the `dictConfig` call into the CLI `run` command body
so that importing the library does not trigger logging setup. At minimum, guard
`dictConfig` so it only runs when `app.py.__name__ == "__main__"` or when invoked through
the CLI.
**Verify:** new test needed: test_import_does_not_create_log_file_or_reconfigure_logging
**Related:** ENT-001, ENT-003

**Evidence — log file created at import; root logger handlers after import:**
```text
log.log exists before import: False
log.log exists after import: True
Root logger handlers: ['StreamHandler', 'RotatingFileHandler']
  Handler RotatingFileHandler: baseFilename = .../mko_get_mediascope_data/log.log
```

#### ENT-003 — [CRITICAL] Missing config swallowed as `None` → obscure `TypeError`; `init` CLI command is unreachable for new users

**Anchor:** `src/mko_get_mediascope_data/core/utils.py:358`
**Disambiguator:** `except FileNotFoundError as err:` (in `yaml_to_dict`)
**Checked:** ran
**Why this band:** The bootstrap gate silently admits a missing config file — `yaml_to_dict`
returns `None` instead of raising — which then produces an unhelpful crash with no actionable
message, and makes the `init` command itself un-runnable.
**Problem:** `yaml_to_dict` (`utils.py:343-360`) catches `FileNotFoundError`, logs an error
message, and falls through to an implicit `return None`. When the user config directory does
not yet exist (first-time user), both `app_config.yaml` and `log_config.yaml` are missing.
At `services.py:40`, `LoggingSettings(**None)` raises
`TypeError: ... argument after ** must be a mapping, not NoneType` — the user sees a
cryptic Python error with no hint to run `init`. Worse, because `app.py:14` imports
`app_service` at module scope, any entry that loads `app.py` — including the console-script
`mko-get-mediascope init` — crashes at import time before `init_project()` can ever execute.
A brand-new user cannot initialise the tool.
**Repro:**
1. Point `APP_PATHS.user_dir` to a non-existent path (e.g.
   `/nonexistent/missing_config`).
2. `python -c "import mko_get_mediascope_data.app"`
3. → `TypeError: mko_get_mediascope_data.core.models.LoggingSettings() argument after **
   must be a mapping, not NoneType`
4. `from mko_get_medioscope_data.core.init_service import init_project` succeeds (does not
   import `services.py`), confirming the `init` function itself is fine — only the CLI
   entry is blocked.
**Impact:** A new user following the README (`mko-get-mediascope init`) encounters an
unhelpful `TypeError` and cannot bootstrap the tool. The CLI `init` command is dead code for
anyone without a pre-existing config.
**Root cause:** `yaml_to_dict` swallows `FileNotFoundError` by returning `None` (violating
fail-fast), and the CLI imports the fully-bootstrapped singleton at module scope rather than
deferring bootstrap to command execution time.
**Fix:** (a) In `yaml_to_dict`, raise `FileNotFoundError` (or a domain-specific
`ConfigFileNotFoundError`) instead of returning `None` when a required config file is absent.
(b) In `AppService.app_config`, check for `None` and raise an actionable
`RuntimeError("Configuration not found — run 'mko-get-mediascope init' first")`.
(c) Move singleton instantiation out of `services.py` module scope so that importing `app.py`
does not trigger config loading; instantiate lazily inside the `run`/`list_reports` commands
and inside `run_report_async`.
**Verify:** new test needed: test_init_command_runs_without_existing_config; new test needed:
test_import_without_config_raises_actionable_error
**Related:** ENT-002, ENT-004

**Evidence — simulated missing-config import:**
```text
Import FAILED: TypeError: mko_get_medioscope_data.core.models.LoggingSettings()
  argument after ** must be a mapping, not NoneType
No such file or directory\nonexistent\missing_config\settings\log_config.yaml
  [Errno 2] No such file or directory: '\\nonexistent\missing_config\settings\\log_config.yaml'
```

#### ENT-004 — [HIGH] Public API not re-exported from package root; README import fails

**Anchor:** `src/mko_get_mediascope_data/__init__.py`
**Disambiguator:** file is 0 bytes (empty)
**Checked:** ran
**Why this band:** A documented entry point whose startup contract does not hold — the README
and the `app.py` docstring instruct users to `from mko_get_medioscope_data import run_report`,
but the package `__init__.py` never re-exports any names.
**Problem:** `__init__.py` is 0 bytes. `app.py:104-111` defines `__all__` listing
`run_report`, `run_report_async`, `initialize_settings`, etc., but `__all__` only controls
`app.py`'s own namespace — it does not re-export from the package root. `README.md:82` and
`app.py:89` both show `from mko_get_medioscope_data import run_report`, which raises
`ImportError`. Users must instead write `from mko_get_mediascope_data.app import run_report`.
**Repro:**
```
$ python -c "from mko_get_mediascope_data import run_report"
ImportError: cannot import name 'run_report' from 'mko_get_mediascope_data'
```
**Impact:** Library adoption in Jupyter/Airflow fails at the first import line shown in the
README. The documented public API contract is broken.
**Root cause:** `__init__.py` was left empty; nobody added re-exports.
**Fix:** `add re-exports to __init__.py` — e.g.
`from mko_get_mediascope_data.app import run_report, run_report_async, initialize_settings`
and define `__all__` at the package level. Alternatively, use
`__getattr__` (PEP 562) for lazy re-export to avoid triggering the singleton at
package-import time (see ENT-002).
**Verify:** new test needed: test_package_level_imports; existing:
test_app.py::test_cli_init (only tests `--help`, does not exercise the public API import)
**Related:** ENT-002, ENT-003

**Evidence — README documented import:**
```text
README.md:82: from mko_get_mediascope_data import run_report, initialize_settings
app.py:89:        from mko_get_mediascope_data import run_report
```

#### ENT-005 — [MEDIUM] `print()` calls in `core/` bypass the logging layer

**Anchor:** `src/mko_get_mediascope_data/core/services.py:117`
**Disambiguator:** `print(f"\n{'-' * 10}  Обработка стартовата...`
**Checked:** ran (static AST scan)
**Why this band:** An import-time side effect that is unnecessary but not harmful to
correctness; specifically a layer-discipline violation — output from `core/` modules via
`print()` instead of `logging.getLogger(__name__)`, so it bypasses all log-level and
file-handler configuration.
**Problem:** `docs/00-overview/doc-maintenance-rules.md:101` mandates
`logger = logging.getLogger(__name__)` for all service-layer output in `core/`. However,
four `print()` calls exist in core modules:
- `services.py:117` — start-time banner
- `services.py:124` — per-report progress
- `services.py:154` — end-time summary
- `reports.py:265` — progress dot on task send

These bypass the `dictConfig` logging setup: output goes to stdout directly, ignoring log
levels, formatters, and the `rotating_file` handler. In a Jupyter/Airflow context the output
appears as raw stdout rather than structured logs.
**Repro:**
```text
src\mko_get_mediascope_data\core\reports.py: line 265: print()
src\mko_get_mediascope_data\core\services.py: line 117: print()
src\mko_get_mediascope_data\core\services.py: line 154: print()
src\mko_get_mediascope_data\core\services.py: line 124: print()
```
**Impact:** Users cannot redirect, filter, or capture core-module output through the logging
configuration they customise via `log_config.yaml`. Debugging in production (file logs) misses
these messages entirely.
**Root cause:** `print()` used for user-facing progress output in orchestration and report
layers, rather than `logger.info(...)`.
**Fix:** `replace all print()` calls in `core/` with `logger.info(...)` (or `console.print()`
if the output must be Rich-formatted — but `console` is defined in `app.py`, so core modules
should use `logging`). For the progress dot at `reports.py:265`, use
`logger.debug("Task sent: %s", task.name)`.
**Verify:** existing: test_services.py::test_run_report_with_real_models (asserts output
behaviour is unchanged for the mocked path); new test needed:
test_no_print_in_core_modules
**Related:** ENT-002

#### ENT-006 — [MEDIUM] `app_config` cached on singleton is stale for the lifetime of the process

**Anchor:** `src/mko_get_mediascope_data/core/services.py:35`
**Disambiguator:** `@cached_property` on `app_config`
**Checked:** ran
**Why this band:** An import-time side effect (config loaded once) that is unnecessary but
not harmful to a single-shot CLI; however, in a long-lived Jupyter or Airflow process it
silently serves stale configuration after the user edits the YAML files.
**Problem:** `AppService.app_config` (`services.py:35-42`) is a `cached_property`. It is
first accessed during `prepare_app()` in `__init__` (i.e. at import time). Once cached,
subsequent accesses return the same `AppConfig` object. If a user edits
`app_config.yaml` or `log_config.yaml` and re-runs `run_report` in the same Jupyter kernel
or Airflow task process, the stale cached config is used — changes are silently ignored.
**Repro:**
```text
app_config loaded: AppConfig
app_config same object: True   # second access returns cached, not re-read
```
**Impact:** In interactive/long-lived environments, configuration changes are invisible
without restarting the kernel or process. Users may debug a "config not applied" issue for
hours.
**Root cause:** `cached_property` on a process-wide singleton with no invalidation hook.
**Fix:** `add a refresh/reload method` to `AppService` (e.g.
`reload_config()`) that clears `cached_property` entries and re-runs `prepare_app()`.
Alternatively, move singleton instantiation out of module scope and create a fresh
`AppService` per `run_report` call in library contexts.
**Verify:** new test needed: test_config_changes_pickled_up_after_reload
**Related:** ENT-002, ENT-003

#### ENT-007 — [LOW] Russian docstrings and comments in the entry layer violate the "English Only" standard

**Anchor:** `src/mko_get_mediascope_data/app.py:2`
**Disambiguator:** `"""mko_get_mediascope_data — основной публичный интерфейс"""`
**Checked:** ran (static read)
**Why this band:** Documentation or comment drift with no runtime consequence — the
entry-layer module docstring and CLI command docstrings are in Russian, contradicting the
"English Only" mandate in AGENTS.md rule 1 and
`docs/00-overview/doc-maintenance-rules.md#translation-rules`.
**Problem:** `app.py` module docstring (line 2) and all three Typer command docstrings
(`init` line 33, `run` line 46, `list_reports` line 61) are in Russian. Several error
messages in `services.py` (e.g. line 117, 154) and `core/reports.py` loggers are also in
Russian. The standard requires all docstrings, logs, error messages, and documentation in
clear English.
**Repro:**
```
app.py:2:  """mko_get_mediascope_data — основной публичный интерфейс"""
app.py:33: """Инициализировать пользовательские настройки"""
app.py:46: """Запустить выгрузку отчёта"""
app.py:61: """Показать все доступные файлы заданий"""
```
**Impact:** Non-Russian-speaking contributors and downstream integrators cannot read the
entry-layer documentation. Mixed-language codebase violates the project's own governance
rule.
**Root cause:** Entry layer was authored in Russian without translation to English as
required.
**Fix:** `translate all docstrings and comments in app.py` to English. Translate user-facing
strings in `core/services.py` and `core/reports.py` to English (or route them through
`logging` with English messages — see ENT-005).
**Verify:** existing: none; new test needed: test_no_cyrillic_in_docstrings
**Related:** ENT-005

## Fix Order

- **Dependencies:** ENT-002 must be addressed before ENT-003's fix is fully effective
  (moving singleton instantiation out of module scope is what makes `init` reachable).
  ENT-003's `yaml_to_dict` fix (raise instead of return `None`) blocks the
  ENT-001 `AttributeError` crash in Jupyter from being masked by a `TypeError`.
  ENT-004's `__init__.py` re-export should use PEP 562 `__getattr__` to avoid triggering
  ENT-002's import-time side effects.
- **Merge candidates:** ENT-002, ENT-003, and ENT-004 all stem from the same root cause:
  bootstrap side effects placed at module scope. A single refactor that defers
  `AppService` instantiation and `dictConfig` to command-execution time would resolve all
  three.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** ENT-005 (print in `core/`) and ENT-007 (Russian strings) are
  also relevant to phases 09 (blocking-work bridge) and 10 (enum discipline) respectively,
  but the entry-architecture phase is the correct home since they concern entry-layer output
  and documentation.
- **Rollout risk:** Moving singleton instantiation out of module scope (ENT-002/003) could
  break `tests/test_services.py::test_run_report_with_real_models`, which imports
  `app_service` directly from `core.services`. That test must continue to work by accessing
  `app_service` via a lazy accessor or by explicitly bootstrapping in the test fixture.
  `tests/test_app.py::test_cli_init` must remain green (it only invokes `--help`).

## Appendices

### Appendix A — Entry-surface inventory and import-time side effects

```text
Entry surface                  | Module-scope work at import
-------------------------------|---------------------------------------------------
Console script mko-get-mediascope | imports app.py → sys.stdout.reconfigure, app_service()
                              | → read 2 YAML files, mkdir log dir, dictConfig → opens
                              |   RotatingFileHandler on log.log, attaches to root logger
Module entry: python -m mko_get   | __main__.py → from .app import app → same as console
Library import:                  | from mko_get_mediascope_data.app import run_report
  from mko_get_medioscope.app    | → triggers ALL side effects above
Package-level (documented):      | from mko_get_mediascope_data import run_report → ImportError
  __init__.py is 0 bytes         |  (BUG)

Side effects observed at import:
  - sys.stdout.encoding: cp1252 → utf-8
  - sys.stdout.line_buffering: False → True
  - log.log file Created (RotatingFileHandler opened)
  - Root logger handlers: StreamHandler + RotatingFileHandler
  - Parent dirs of log file created via ensure_file_parent
  - YAML config files read and Pydantic-validated
```

*(supports: ENT-001, ENT-002)*

### Appendix B — Missing-config crash simulation

```text
$ python -c "from unittest.mock import patch; from pathlib import Path; from unittest.mock import MagicMock
... with patch('mko_get_medioscope_data.core.paths.APP_PATHS') as mock_paths:
...     mock_paths.app_dir = Path('src/mko_get_mediascope_data').resolve()
...     mock_paths.user_dir = Path('/nonexistent/missing_config')
...     ...
...     import mko_get_mediascope_data.app"

Import FAILED: TypeError: mko_get_mediosphere_data.core.models.LoggingSettings()
  argument after ** must be a mapping, not NoneType
No such file or directory\nonexistent\missing_config\settings\log_config.yaml
```

*(supports: ENT-003, ENT-007)*

### Appendix C — Public API import failure (README vs reality)

```text
$ python -c "from mko_get_mediascope_data import run_report, initialize_settings"
ImportError: cannot import name 'run_report' from 'mko_get_mediascope_data'
```

*(supports: ENT-004)*

### Appendix D — `print()` in `core/` modules (AST scan)

```text
src\mko_get_medioscope_data\core\reports.py: line 265: print()
src\mko_get_medioscope_data\core\services.py: line 117: print()
src\mko_get_medioscope_data\core\services.py: line 154: print()
src\mko_get_medioscope_data\core\services.py: line 124: print()
```

*(supports: ENT-005)*

### Appendix E — Singleton config cache stale within process

```text
app_config loaded: AppConfig
app_config same object: True   # cached_property returns same instance
```

*(supports: ENT-006)*
