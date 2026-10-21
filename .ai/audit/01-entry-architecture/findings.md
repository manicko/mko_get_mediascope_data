---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/01-entry-architecture/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Entry Points & Process Architecture

## Summary

Two findings, one HIGH and one MEDIUM. The documented synchronous notebook example cannot start in a running event loop. Users with missing settings also receive a raw startup failure without the available recovery instruction.

## Scope

**Examined:** All six phase blocks: console script, module entry, CLI commands, package re-exports and public wrappers; deferred service bootstrap; stdout/logging setup; settings path resolution and initialization; import direction; event-loop boundaries and shutdown ownership. Source inspected: package `__init__.py`, `__main__.py`, `app.py`, and core `services.py`, `paths.py`, `init_service.py`, `utils.py`, `reports.py`; package logging template; `pyproject.toml`, CI workflow, `tests/test_app.py`, README and current-behavior/system-architecture documentation.

**Evidence scope:** Current dirty working tree on 2026-10-21, not pristine HEAD. Pre-existing modifications include package `app_config.json5`, current-behavior and async-concurrency documentation; deleted prior reports and an untracked settings-wiring plan were preserved. Initial snapshot: `.ai/tmp/audit-2026-10-21/initial-hashes.json` and `initial-git-status.txt`. Historical ENT-001–007 were inspected only for ID provenance; new IDs start at ENT-008.

**Tools:** PowerShell read-only inspection, `rg`, `git ls-files`/`show`/`status`, existing venv Python, isolated `CliRunner` and CPython runtime simulation. No dependency synchronization or application/test changes.

**Not verified:** Real Jupyter/IPython kernel, live API, end-to-end export, full lint/type/test suite. Native Windows event-loop creation stalled in Python 3.14's socketpair setup inside the sandbox before application execution; that probe was interrupted. An approved rerun outside the sandbox verified ENT-008 with a real running asyncio loop. Worker internals belong to phase 03; vendor transport closure belongs to phase 09.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| ENT-008 | HIGH | `src/mko_get_mediascope_data/app.py::run_report` | Documented notebook wrapper rejects a running host loop |
| ENT-009 | MEDIUM | `src/mko_get_mediascope_data/app.py::run` | Missing settings bypass the CLI error boundary and recovery guidance |

#### ENT-008 — [HIGH] Documented notebook wrapper rejects a running host loop

**Anchor:** `src/mko_get_mediascope_data/app.py::run_report`
**Disambiguator:** `asyncio.run(run_report_async(report_path, progress=progress))`
**Checked:** ran
**Why this band:** The documented entry startup contract fails for a host with an active event loop. This is HIGH, not CRITICAL: import remains possible and the separate async wrapper provides a usable route.
**Problem:** `run_report` describes itself as a synchronous wrapper for Jupyter and includes a notebook example calling it directly. Its unconditional `asyncio.run` fails when that host already runs an event loop, before report validation or execution.
**Repro:** Execute the Appendix C probe. It creates a real asyncio loop, invokes the package's public synchronous wrapper from an async function, and captures both the RuntimeError and unawaited-coroutine warning. No service or API call occurs. Appendix A also contains an earlier explicitly simulated reproduction.
**Impact:** Notebook users following the advertised wrapper cannot start the export and receive an unrelated coroutine warning.
**Root cause:** The synchronous wrapper's documented host contract does not account for ownership of an already running loop.
**Fix:** Align `run_report`'s supported-host contract and notebook example with the existing `run_report_async` entry point; give running-loop callers an actionable diagnostic without abandoning a created coroutine. Benefit: users can select the working entry point; effort small; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_run_report_in_running_host_loop_has_actionable_contract
**Related:** None.

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_host_loop.py`** *(supports: actual public wrapper failure inside a real running asyncio loop)*:
```text
actual_running_loop_exception: asyncio.run() cannot be called from a running event loop
actual_running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```

#### ENT-009 — [MEDIUM] Missing settings bypass the CLI error boundary and recovery guidance

**Anchor:** `src/mko_get_mediascope_data/app.py::run`
**Anchor:** `src/mko_get_mediascope_data/app.py::list_reports`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.app_config`
**Disambiguator:** `app_service = get_app_service()`
**Checked:** ran
**Why this band:** A recoverable configuration-bootstrap diagnostic failure, rated MEDIUM by its limited effect. It is not HIGH: initialization remains usable and valid configured runs are not prevented; it is not CRITICAL: no missing settings are silently accepted.
**Problem:** `run` constructs the service before entering its exception handler. Required settings load failures therefore bypass its normal error presentation. `list_reports` likewise bootstraps without contextual handling. The resulting missing-file message does not tell a first-time user to run `mko-get-mediascope init`, although that command works without settings.
**Repro:** Patch only `services.APP_PATHS` to a nonexistent scratch user directory, pass an existing scratch report to `CliRunner` and invoke `run` and `list-reports`. Both exit 1 with `FileNotFoundError`; the output and exception contain no `init` hint. Invoke `init` with a separate scratch destination to demonstrate the available recovery.
**Impact:** New users or users with removed settings receive a low-level path error instead of the recovery action, and direct CLI execution can expose a traceback before its normal error boundary.
**Root cause:** The entry command error boundary begins after configuration bootstrap; the shared settings-file exception lacks initialization context.
**Fix:** Include `run` bootstrap failures and `list_reports` startup failures in the entry layer's actionable configuration diagnostics, naming `init` as recovery for missing required settings. Benefit: users can recover without inspecting source; effort small; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_cli_missing_settings_explains_init_recovery
**Related:** None.

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_probe.py`** *(supports: missing-config failure and available recovery)*:
```text
missing_config_exit: 1
missing_config_exception: FileNotFoundError [Errno 2] No such file or directory: 'C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5'
missing_config_init_hint: False
missing_list_exit: 1
missing_list_exception: FileNotFoundError
isolated_init_exit: 0
isolated_init_app_config_exists: True
isolated_init_log_config_exists: True
```

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 1; MEDIUM 1; LOW 0.
- **Dependencies:** None. Review ENT-008's public host contract before updating notebook examples.
- **Merge candidates:** None; host-loop ownership and configuration diagnostics have separate causes.
- **Conflicting evidence:** Historical eager-import claims are obsolete and were not carried forward.
- **Cross-phase conflicts:** No worker-pool or vendor resource findings are filed here. ENT-009 concerns the startup diagnostic rather than settings values or credential provenance.
- **Rollout risk:** ENT-008 must preserve ordinary synchronous execution and async callers; keep `tests/test_app.py::test_run_report_sync_wrapper` and the public import identity tests meaningful. ENT-009 must preserve parser errors, nonzero execution failures and settings-free `init`; keep `tests/test_app.py::test_cli_run_missing_file` and `test_cli_init_invokes_init_project` meaningful.

## Appendices

### Appendix A — Complete bounded runtime probe output

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_probe.py` (exit 0).
The script's maintenance purpose is isolated entry-contract reproduction; it writes only inside the assigned scratch directory and never calls the vendor API. `_set_running_loop(object())` simulates the CPython guard condition, not a real running Jupyter kernel. Settings are copied to a separate isolated destination solely to exercise `init`.

```text
import_state_unchanged: True
singleton_after_import: None
missing_config_exit: 1
missing_config_exception: FileNotFoundError [Errno 2] No such file or directory: 'C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5'
missing_config_cli_output: "No such file or directory: C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5 — [Errno 2] No such file or directory: 'C:\\\\py_dev\\\\mko_get_mediascope_data\\\\.ai\\\\tmp\\\\audit-2026-10-21\\\\entry-missing\\\\settings\\\\log_config.json5'\n"
missing_config_init_hint: False
missing_list_exit: 1
missing_list_exception: FileNotFoundError
isolated_init_exit: 0
isolated_init_app_config_exists: True
isolated_init_log_config_exists: True
running_loop_exception: asyncio.run() cannot be called from a running event loop
running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```
*(supports: ENT-008 and ENT-009; the first two lines describe probe setup, not separate findings.)*

### Appendix B — Surface and bootstrap map used for scope coverage

| Surface | Import/deferred boundary | Configuration source |
|---|---|---|
| Console script `mko-get-mediascope` | `pyproject.toml` points at `app:app`; command invocation is deferred | `run` and `list_reports` reach `get_app_service` |
| `python -m mko_get_mediascope_data` | `__main__` imports the Typer app, invokes under main guard | Same service/bootstrap as console script |
| Package public names | `__getattr__` loads `app.py` on named access | Wrapper invocation reaches service; package import does not bootstrap it |
| `run_report` | Creates one `asyncio.run` boundary per call | Delegates to async wrapper |
| `run_report_async` | Checks report path, then awaits service | User settings resolved via `APP_PATHS` |
| `initialize_settings` / CLI `init` | Calls `init_project` without service construction | Copies package templates to platformdirs user settings directory |

Static call chain: `get_app_service` → `AppService.__init__` → `prepare_app` → cached `app_config` → JSON5 logging/application settings → Pydantic models → handler-parent creation → `dictConfig`. Relative log/export paths resolve against `APP_PATHS.user_dir`; required settings are under its `settings` child. Stdout reconfiguration occurs inside CLI `run`, not in module scope. Runtime claims about imports are limited to the observed stdout/stderr/root-handler identity and empty singleton, not a full I/O interception audit.

Dependency direction inspected: entry → init/service/progress/models/tasks/utils; service → paths/models/network/reports/utils; reports → network/tasks/models/progress/utils. No core back-import of the entry module was found. CLI presentation and report domain work remain separate. Both lint and mypy configuration cover `src`/`tests`; CI runs both gates. These are coverage notes, not findings.

Shutdown scope: service configuration is cached process-wide, but each report constructs its own `NetworkClient` and `TVMediaReport` with queue state. The normal `processing_tasks` queue-join boundary cancels and gathers workers in `finally`; pre-join worker lifetime/cancellation semantics are reserved for phase 03. No runtime cross-run resource-leak claim is made here.

### Appendix C — Real host-loop reproduction

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_host_loop.py` (approved outside-sandbox execution; exit 0).

```text
actual_running_loop_exception: asyncio.run() cannot be called from a running event loop
actual_running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```
*(supports: ENT-008. This is a real asyncio host-loop reproduction, not execution of a Jupyter kernel.)*
