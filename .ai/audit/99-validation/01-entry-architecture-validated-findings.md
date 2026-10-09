---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/01-audit-entry-architecture.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Entry Points & Process Architecture

## Summary

Seven findings (3 CRITICAL, 1 HIGH, 2 MEDIUM, 1 LOW). All seven anchors resolve to
the cited location and all seven claims reproduce under runtime probe or static
inspection. **7 confirmed, 0 rejected, 0 re-graded, 0 unsettled.**

No finding was downgraded, re-typed, or withdrawn. The sole imprecision — ENT-003's
secondary reference to `services.py:40` (the `AppSettings` line) instead of the actual
crash site `services.py:38` (the `LoggingSettings` line) — does not alter the claim's
truth: `yaml_to_dict` returns `None` for a missing file and `LoggingSettings(**None)`
raises `TypeError`.

Two validation-level findings (VAL-001, VAL-002) are recorded in a separate section:
anchor-format drift and the ENT-003 line-number imprecision.

## Scope

**Examined:** `src/mko_get_mediascope_data/__init__.py`, `__main__.py`, `app.py`,
`core/services.py`, `core/paths.py`, `core/init_service.py`, `core/utils.py`,
`core/models.py`, `core/reports.py`, `core/logger.py`, `core/network.py`,
`settings/app_config.yaml`, `settings/log_config.yaml`, `pyproject.toml`,
`README.md`, `docs/00-overview/doc-maintenance-rules.md`, `.kilo/commands/audit/phases/01-audit-entry-architecture.md`,
`.ai/audit/templates/audit-findings.md`, `.ai/audit/02-config-secrets/findings.md`,
`tests/test_app.py`, `tests/test_services.py`, `tests/conftest.py`.

**Runtime probes performed:**
- `python -c "..."` before/after `import mko_get_mediascope_data.app` — stdout encoding/line_buffering
- `from mko_get_mediascope_data import run_report` — ImportError
- `yaml_to_dict('/nonexistent/...')` — returns `None`
- `LoggingSettings(**None)` — TypeError reproduced
- Root logger handler inspection before/after import
- AST scan for `print()` across all `core/` modules
- AST scan for Cyrillic characters in `app.py` docstrings/commands

**Not verified:** Runtime behavior inside Jupyter/IPython — no kernel available.
The `ipykernel.iostream.OutStream` `AttributeError` claim (ENT-001) is inferred from
the unconditional call site; the inference is substantiated (OutStream has no
`reconfigure` method) but not observed live.

**Unsettled claims:** ENT-001's Jupyter `AttributeError` consequence — inferred, not
observed. All other claims are directly reproduced.

---

## Findings

| ID      | Disposition  | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|--------------------------|-----------------|
| ENT-001 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes             |
| ENT-002 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes             |
| ENT-003 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes*            |
| ENT-004 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| ENT-005 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM        | yes             |
| ENT-006 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM        | yes             |
| ENT-007 | confirmed   | LOW        | LOW vs. LOW              | yes             |

\* ENT-003's primary anchor `utils.py:358` resolves to `except FileNotFoundError as err:`;
its secondary reference "services.py:40" points to the `AppSettings` line, but the
actual crash site is `services.py:38` (`LoggingSettings(**None)`). Does not affect disposition.

### ENT-001 — confirmed — [CRITICAL] `sys.stdout.reconfigure()` at module scope

**Anchor resolved:** `src/mko_get_medioscope_data/app.py:17` → line 17 is
`sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")` at module scope
(not inside any function/class).

**Claim tested:** Importing `app.py` reconfigures `sys.stdout` unconditionally on every
import path (CLI, console-script, library import).

**Evidence (runtime probe):**
```text
Before import: stdout type: TextIOWrapper  encoding: cp1252  line_buffering: False
After import:  stdout type: TextIOWrapper  encoding: utf-8   line_buffering: True
```
Console-script entry point confirmed at `pyproject.toml:44`:
`mko-get-mediascope = "mko_get_mediascope_data.app:app"`.
`__main__.py:1` → `from .app import app`.

**Asserted cause tested separately:** The reconfiguration is at module scope with no
guard, no conditionality, and no opt-out flag. In Jupyter/IPython, `sys.stdout` is
`ipykernel.iostream.OutStream` which lacks `reconfigure` → `AttributeError`. Not
observed live (no kernel); inference is substantiated (OutStream API does not expose
`reconfigure`).

**Rubric grading:** Phase 01 CRITICAL = "an import-time side effect that corrupts a
host process's stdout or logging without consent." The claim matches exactly.
CRITICAL carried = CRITICAL on rubric. No re-grade.

**Load-bearing artefact:** Code (`app.py:17`). Documentation (`app.py` docstring
example, README usage) depends on the code behavior.

**Recommendation carry-out:** Fix names `app.run()` as the target. Moving the call
into the CLI `run` command body is a well-defined target. ✓

---

### ENT-002 — confirmed — [CRITICAL] Singleton `app_service` bootstraps at import

**Anchor resolved:** `src/mko_get_mediascope_data/core/services.py:163` → line 163:
`app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))`.

**Claim tested:** Importing `services.py` (transitively via `app.py`) instantiates
`AppService` at module scope, which calls `prepare_app()` → accesses `app_config`
(cached_property) → reads two YAML files → Pydantic validation → `ensure_file_parent`
creates directories → `dictConfig` opens `RotatingFileHandler` on `log.log`.

**Evidence (runtime probe):**
```text
Handlers before import: []
Handlers after import:  ['StreamHandler', 'RotatingFileHandler']
  StreamHandler:    stream=<TextIOWrapper name='<stdout>' mode='w' encoding='utf-8'>
  RotatingFileHandler: baseFilename=.../mko_get_mediascope_data/log.log
log.log exists after import: True
```
Call graph verified from source:
- `services.py:30-33` `__init__` → `self.prepare_app()`
- `services.py:35-42` `app_config` (cached_property) → `yaml_to_dict` on `log_config` + `app_config`
- `services.py:44-49` `prepare_app()` → `ensure_file_parent` (mkdir)
- `services.py:164` → `dictConfig(log_config.model_dump())`

**Asserted cause tested separately:** The `__init__` calls `self.prepare_app()` which
triggers `self.app_config` access. `prepare_app` mutates handler dicts via
`resolve` + `ensure_file_parent`. `dictConfig` at module scope instantiates
`RotatingFileHandler`. All verified against source.

**Rubric grading:** Phase 01 CRITICAL = "a singleton that performs network or
filesystem I/O on import and cannot be suppressed by a library caller." Matches exactly.
CRITICAL carried = CRITICAL on rubric. No re-grade.

**Load-bearing artefact:** Code (`services.py:163` + `dictConfig` at line 166).

**Recommendation carry-out:** "defer behind a factory or lazy accessor
(`get_app_service()`)" — target `services.py` module scope is stable. ✓

---

### ENT-003 — confirmed — [CRITICAL] Missing config swallowed as `None` → obscure `TypeError`

**Anchor resolved (primary):** `src/mko_get_mediascope_data/core/utils.py:358` →
`except FileNotFoundError as err:` inside `yaml_to_dict`.

**Anchor resolved (secondary):** The finding states "At services.py:40,
`LoggingSettings(**None)`" — line 40 is actually `app_settings = AppSettings(...)`
(the AppSettings call). The actual `LoggingSettings` construction spans lines 37-39,
with the `yaml_to_dict` call at line 38. Because `log_config.yaml` is read first
(line 38), the `TypeError` occurs at `LoggingSettings`, not `AppSettings` (line 40).
This is a line-number imprecision, not a claim falsification.

**Claim tested:** `yaml_to_dict` catches `FileNotFoundError`, logs an error, and
falls through to implicit `return None`. When config files are missing,
`LoggingSettings(**None)` raises `TypeError`. The CLI `init` command is unreachable
because `app.py:14` imports `app_service` at module scope, which triggers the crash
before `init_project()` can execute.

**Evidence (runtime probes):**
```text
yaml_to_dict('/nonexistent/missing_config/settings/log_config.yaml') → None
LoggingSettings(**None) → TypeError: argument after ** must be a mapping, not NoneType

# Confirmed independently:
init_project (init_service.py) does NOT import services.py.
app.py imports app_service at module scope (line 14: from .core.services import app_service)
→ crashes at import → CLI init unreachable.
```

**Asserted cause tested separately:** `yaml_to_dict` function body (utils.py:353-359)
has no `return` in either `except` block — falls through to implicit `return None`.
Return type annotation is `dict[str, Any] | None`, confirming the design admits
`None`. `AppService.app_config` (services.py:37-38) passes this directly to
`LoggingSettings(**...)`. All verified against source.

**Rubric grading:** Phase 01 CRITICAL = "a bootstrap gate that silently admits a
missing config and proceeds with hidden fallback values." The `None` return is the
silent admission; the subsequent `TypeError` is the unhelpful crash. Root cause
verified. CRITICAL carried = CRITICAL on rubric. No re-grade.

**Load-bearing artefact:** Code (`utils.py:yaml_to_dict` and `services.py:AppService.app_config`).

**Recommendation carry-out:** Three sub-fixes all name concrete targets:
(a) `utils.py:yaml_to_dict` — raise instead of return None ✓
(b) `services.py:AppService.app_config` — check for None, raise actionable error ✓
(c) `services.py:163` — move singleton out of module scope ✓
All targets exist and are stable.

---

### ENT-004 — confirmed — [HIGH] Public API not re-exported from package root

**Anchor resolved:** `src/mko_get_mediascope_data/__init__.py` → file is 0 bytes.

**Claim tested:** `__init__.py` is empty. `app.py:104-111` defines `__all__` but this
only governs `app.py`'s namespace, not the package root. `README.md:82` and
`app.py:89` both show `from mko_get_mediascope_data import run_report`, which raises
`ImportError`.

**Evidence (runtime probe):**
```text
__init__.py size: 0 bytes
app.__all__ = ['run_report', 'run_report_async', 'initialize_settings', 'init',
'run', 'list_reports', 'app']

$ python -c "from mko_get_mediascope_data import run_report, initialize_settings"
ImportError: cannot import name 'run_report' from 'mko_get_mediascope_data'
```

**Asserted cause tested separately:** `__init__.py` has no import statements. The
`__all__` list is defined in `app.py` (line 104), not `__init__.py`. Python does not
inherit `__all__` across modules. Verified by source inspection.

**Rubric grading:** Phase 01 HIGH = "a startup contract that does not hold."
The README documents `from mko_get_mediascope_data import run_report` but the
contract is broken. HIGH carried = HIGH on rubric. No re-grade.

**Load-bearing artefact:** `__init__.py` (empty) is the defect; documentation
(README, app.py docstring) depends on the re-export existing.

**Recommendation carry-out:** "add re-exports to `__init__.py`" — target exists.
PEP 562 `__getattr__` alternative is also valid. ✓

---

### ENT-005 — confirmed — [MEDIUM] `print()` calls in `core/` bypass the logging layer

**Anchor resolved:** Four call sites confirmed by AST scan:
- `core/services.py:117` → `print(f"\n{'-' * 10}  Обработка стартовала:...")`
- `core/services.py:124` → `print(f"\nОтчёт {idx} → {report_settings.report_subtype}\n")`
- `core/services.py:154` → `print(f"\n{'-' * 10}  Обработка завершена: ...")`
- `core/reports.py:265` → `print("=", end="", flush=True)`

**Claim tested:** Four `print()` calls exist in `core/` modules, bypassing the
`logging.getLogger(__name__)` pattern mandated by
`docs/00-overview/doc-maintenance-rules.md:101`.

**Evidence (AST scan across all core/ modules):**
```text
src/mko_get_mediascope_data/core/services.py: line 117: print()
src/mko_get_mediascope_data/core/services.py: line 124: print()
src/mko_get_mediascope_data/core/services.py: line 154: print()
src/mko_get_mediascope_data/core/reports.py: line 265: print()
```
`utils.py` and `network.py` correctly use `logger = logging.getLogger(__name__)`.
`reports.py:24` and `services.py` do not define a module-level logger.

**Asserted cause tested separately:** The `print()` calls are in `run_report()`
(services.py:110) and `send_task()` (reports.py:252). Neither is at import time;
both are runtime calls. They bypass the `dictConfig` setup from ENT-002. Verified
by source inspection.

**Rubric grading:** Phase 01 MEDIUM = "an import-time side effect that is
unnecessary but not harmful." The print() calls are not import-time but are
unnecessary layer-discipline violations. MEDIUM is the closest fit — not CRITICAL
(no stdout/logging corruption at import), not HIGH (no broken startup contract),
not LOW (has runtime consequence: output bypasses configured log handlers).
MEDIUM carried = MEDIUM on rubric. No re-grade.

**Load-bearing artefact:** Code (`services.py` and `reports.py`).
`doc-maintenance-rules.md:101` is the governing standard.

**Recommendation carry-out:** "replace all print() calls in core/ with logger.info()"
— targets `services.py` and `reports.py`. ✓

---

### ENT-006 — confirmed — [MEDIUM] `app_config` cached on singleton is stale

**Anchor resolved:** `src/mko_get_mediascope_data/core/services.py:35` →
`@cached_property` decorator on `app_config` (line 35), property body at lines 36-42.

**Claim tested:** `app_config` is a `cached_property`. It is first accessed during
`prepare_app()` in `__init__` (line 33), i.e. at import time via the module-level
singleton. Once cached on the instance, subsequent accesses return the same object.
Since `app_service` is a process-wide singleton, config edits in a long-lived
process (Jupyter/Airflow) are silently ignored.

**Evidence (source inspection):**
```text
services.py:35:     @cached_property
services.py:36:     def app_config(self) -> AppConfig:
services.py:44:     def prepare_app(self):
services.py:45:         for handler in self.app_config.logging_settings.handlers...
services.py:163: app_service = AppService(app_paths=APP_PATHS, ...)
```
`AppService.__init__` (line 30-33) calls `self.prepare_app()` → accesses
`self.app_config` → triggers the read + cache. `cached_property` stores the result
in `self.__dict__["app_config"]`. Subsequent `app_service.app_config` returns the
cached object. Python `cached_property` semantics verified.

**Asserted cause tested separately:** `@cached_property` has no invalidation hook.
The singleton is process-global. No `reload_config()` method exists. Verified by
source inspection — search for "reload" returns no results.

**Rubric grading:** Phase 01 MEDIUM = "an import-time side effect that is
unnecessary but not harmless." The caching occurs at import time (via `__init__`
→ `prepare_app` → `app_config` access). In single-shot CLI it's harmless; in
long-lived processes it serves stale config. MEDIUM carried = MEDIUM on rubric.
No re-grade.

**Load-bearing artefact:** Code (`services.py:35`).

**Recommendation carry-out:** "add a `reload_config()` method to `AppService`" —
target `AppService` class exists and is stable. ✓

---

### ENT-007 — confirmed — [LOW] Russian docstrings and comments violate English-only standard

**Anchor resolved:** `src/mko_get_medioscope_data/app.py:2` → module docstring:
`"""mko_get_medioscope_data — основной публичный интерфейс"""`.

**Claim tested:** Entry-layer docstrings, Typer command docstrings, `help=` strings,
and `console.print()` messages in `app.py` are in Russian. Error messages in
`core/services.py` (lines 117, 124, 154) and loggers in `core/reports.py` are also
in Russian. This violates AGENTS.md rule 1 and
`docs/00-overview/doc-maintenance-rules.md` (Translation Rules).

**Evidence (AST scan for Cyrillic in app.py):**
```text
Module docstring: 'mko_get_medioscope_data — основной публичный интерфейс'
  init:            'Инициализировать пользовательские настройки'
  run:             'Запустить выгрузку отчёта'
  list_reports:    'Показать все доступные файлы заданий'
  run_report:     'Синхронная обёртка для Jupyter / Airflow.'
  initialize_settings: 'Обёртка для Jupyter/Airflow'
```
Additional Russian strings in `app.py`: Typer `help` strings (lines 22, 30, 42),
`console.print()` messages (lines 35, 48, 52, 55, 67), `FileNotFoundError` message
(line 80). In `core/services.py` (lines 118, 124, 155: "Обработка стартовата(з)а",
"Отчёт"). In `core/reports.py` (lines 149, 152, 349, 355, 379, 453, 466).
In `core/utils.py` (lines 55, 251). In `core/models.py` (lines 75, 78).
In `core/network.py:1` (module docstring).

**Asserted cause tested separately:** Source files were authored in Russian without
translation. `doc-maintenance-rules.md:94-96` mandates "All documentation must be
in English" and "Do not commit non-English content." AGENTS.md rule 1 mandates
"English Only." Verified by source inspection.

**Rubric grading:** Phase 01 LOW = "documentation or comment drift about which
surfaces exist or which config they load, with no runtime consequence." The Russian
strings are documentation/comment drift with no runtime consequence. MEDIUM would
apply if runtime behavior were affected; it is not. LOW carried = LOW on rubric.
No re-grade.

**Load-bearing artefact:** Documentation (`app.py` docstrings, `core/` log messages).
AGENTS.md and `doc-maintenance-rules.md` are the governing standards.

**Recommendation carry-out:** "translate all docstrings and comments in app.py to
English" — target `app.py` exists. ✓

---

## Validation-Level Findings (VAL-)

| ID      | Severity | Disposition | Description |
|---------|----------|-------------|-------------|
| VAL-001 | LOW      | confirmed   | Anchor format drift: findings use `path:line` format; template mandates `path::Symbol` |
| VAL-002 | LOW      | confirmed   | ENT-003 secondary anchor `services.py:40` imprecise (actual crash at `services.py:38`) |

### VAL-001 — [LOW] Anchor format drift from controlled template

**Template mandate:** `.ai/audit/templates/audit-findings.md:40-43` —
"Anchor rule: `path::Symbol` — the file plus the class/function/method carrying the
defect." Line numbers belong only inside pasted tool output.

**Observed:** The findings file's index table uses `path:line` format for all seven
anchors (e.g., `src/mko_get_medioscope_data/app.py:17`). All line numbers resolve
correctly to the cited content. No anchor points at a non-existent line.

**Impact:** Format drift only — all anchors are mechanically verifiable. Does not
affect finding accuracy or disposition.

**Disposition:** confirmed. Recommendation: update anchors to `path::Symbol` format
(e.g., `src/mko_get_medioscope_data/app.py::sys.stdout.reconfigure` at module scope)
in the next raw-phase revision.

### VAL-002 — [LOW] ENT-003 secondary anchor line-number imprecision

**Template mandate:** Anchors must resolve to the correct symbol; "a reference into
lines that do not exist is a wrong approval by default."

**Observed:** ENT-003 states "At `services.py:40`, `LoggingSettings(**None)`."
Line 40 is `app_settings = AppSettings(**utils.yaml_to_dict(self.app_paths.app_config))`
— the `AppSettings` call, not `LoggingSettings`. The `LoggingSettings` construction
is at lines 37-39, with the `yaml_to_dict` call at line 38. The crash occurs at
`LoggingSettings` because `log_config.yaml` is read first (line 38).

**Impact:** The behavioral claim and root cause are correct. The line number is off
by 2. Since the crash was directly reproduced (`LoggingSettings(**None)` → `TypeError`),
this is a documentation imprecision, not a finding falsification.

**Disposition:** confirmed. No finding re-grade needed.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 01 finding | Phase 02 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| yaml_to_dict None return | ENT-003 (CRITICAL) | CFG-005 (MEDIUM) | `utils.py:yaml_to_dict` catches `FileNotFoundError`, falls through to `return None` | both confirmed; same root cause |
| print() in core/ | ENT-005 (MEDIUM) | CFG-010 (LOW) | `print()` calls in `core/services.py` and `core/reports.py` bypass logging | both confirmed; same root cause |

**Severity divergence rationale:** The two phases use different rubrics. Phase 01
(entry-architecture) grades `print()` as MEDIUM (layer-discipline violation at
entry layer); Phase 02 (config-secrets) grades it as LOW (minor issue in config
context). Both are correct under their respective taxonomies. These are
**cross-references**, not conflicts — neither phase reaches an opposite conclusion
about the same fact.

No cross-phase conflicts detected. No finding was withdrawn, downgraded, or re-graded
due to cross-phase interaction.

---

## Rollout Safety Assessment

**Risk: Moving singleton out of module scope (ENT-002/003/004 fixes)**
- `tests/test_services.py:6` imports `from mko_get_mediascope_data.core.services import app_service`
  directly and calls `app_service.run_report(test_yaml)` at line 53.
- `tests/test_app.py:3` imports `from mko_get_mediascope_data.app import app`, which
  triggers the full module body including `from .core.services import app_service`.
- Both tests depend on the module-scope singleton. A fix that removes module-scope
  instantiation must provide a lazy accessor or test fixture workaround.

**Risk: `__init__.py` re-exports (ENT-004 fix)**
- A naive `from .app import run_report` in `__init__.py` would trigger the ENT-002
  import-time side effects. The finding correctly recommends PEP 562 `__getattr__`
  for lazy re-export to avoid this coupling.

**Risk: stdout.reconfigure move (ENT-001 fix)**
- Moving the call into the CLI `run` command body only affects CLI invocations;
  library callers would no longer have stdout reconfigured. No existing test
  asserts stdout encoding/line_buffering behavior.

**Dependencies identified in source:**
- ENT-002 must be addressed before ENT-003's `init` reachability is fully resolved
  (moving singleton out of module scope is what unblocks `init`).
- ENT-003's `yaml_to_dict` fix (raise instead of `None`) blocks ENT-001's Jupyter
  `AttributeError` from being masked by a `TypeError` — config loading (line 14
  import) precedes stdout reconfigure (line 17), so a clear config error surfaces
  first.
- ENT-004's re-export should use `__getattr__` to avoid triggering ENT-002's
  import-time side effects.

**Rollout risk: none of the fixes have been applied.** All seven findings remain
in the current `code_ref: d093d9c` tree unmodified.

---

## Evidence Index

| Evidence | Supports |
|---|---|
| Runtime probe: stdout encoding cp1252→utf-8, line_buffering False→True on import | ENT-001 |
| Runtime probe: root logger gains `RotatingFileHandler` on `log.log` at import | ENT-002 |
| Runtime probe: `yaml_to_dict('/nonexistent')` → `None` | ENT-003 |
| Runtime probe: `LoggingSettings(**None)` → `TypeError` | ENT-003 |
| Runtime probe: `from mko_get_mediascope_data import run_report` → `ImportError` | ENT-004 |
| Runtime probe: `__init__.py` is 0 bytes | ENT-004 |
| AST scan: four `print()` calls in `core/services.py` and `core/reports.py` | ENT-005 |
| Source inspection: `@cached_property` on `app_config`, first accessed in `__init__` | ENT-006 |
| AST scan: Cyrillic in `app.py` module docstring and all command docstrings | ENT-007 |
| `pyproject.toml:44`: console-script entry point `mko_get_mediascope_data.app:app` | ENT-001, ENT-002 |
| `app.py:14`: `from .core.services import app_service` at module scope | ENT-002, ENT-003 |
| `doc-maintenance-rules.md:101`: logging mandate for `core/` | ENT-005 |
| `AGENTS.md` rule 1: English Only mandate | ENT-007 |
