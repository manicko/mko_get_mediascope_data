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

<!-- JSON5-MIGRATION TODO: This validated-findings doc references pre-migration YAML
  symbols (yaml_to_dict) that no longer exist after the JSON5 settings templates
  migration. Anchors need re-verification in a future audit pass; this does NOT block
  the implementation PR. Verdicts remain factually accurate as historical records of
  the code_ref (d093d9c). -->

# Audit Findings — Validation Report: Entry Points & Process Architecture

## Summary

Seven findings (3 CRITICAL, 1 HIGH, 2 MEDIUM, 1 LOW at `code_ref d093d9c`). All
seven anchors resolved to their cited symbols at `code_ref d093d9c`; ENT-001 and
ENT-006 are **fixed at HEAD** (ENT-001's mechanism was relocated into the `run`
command; ENT-006's `reload_config()` was added in commit `755523e`), and five claims
(ENT-002, ENT-003, ENT-004, ENT-005, ENT-007) are rejected as falsified — see
the re-validation note. **2 fixed (ENT-001, ENT-006), 5 rejected
as falsified at HEAD (ENT-002–005, ENT-007), 0 re-graded, 0 unsettled.**

The 2026-10-15 re-validation pass found that post-`code_ref` remediation falsified ENT-002 and ENT-003; both are rejected with the current-tree truth recorded. The originally recorded imprecision (ENT-003's secondary `services.py:40` reference) is moot: that mechanism no longer exists.

Two validation-level findings (VAL-001, VAL-002) are recorded in a separate section:
anchor-format drift and the ENT-003 line-number imprecision.

> **Re-validation revision (2026-10-15, HEAD `335a11e`; code identical to baseline
> `b04d177`).** Post-`code_ref` remediation changed the code this report validated.
> Two findings no longer reproduce at HEAD and are **rejected as falsified**:
> ENT-002 — the module-scope `app_service` singleton was replaced by a lazy
> `get_app_service()` factory (commit `706eed0`; tests migrated in `63bb1c0`), so
> importing `services.py` no longer instantiates `AppService`, reads no YAML, and
> opens no log handler; and ENT-003 — `yaml_to_dict` (`core/utils.py:343-367`) now
> returns `dict[str, Any]` and raises `FileNotFoundError`/`YAMLError` with logging
> instead of returning `None`, `app_config` (`core/services.py:37-55`) carries
> explicit None-guards, and the `init` command (app.py:26-32) calls `init_project`
> directly and never constructs `AppService`. The remaining five findings were not
> re-examined in that pass; a second re-validation pass (2026-10-15) re-examined
> them against HEAD and additionally **rejected ENT-001, ENT-004, ENT-005 and
> ENT-007 as falsified** (see the marked sections). ENT-006 was confirmed in that
> pass but **fixed at HEAD** in commit `755523e` — see the marked section below.
> All seven findings were remediated at HEAD.

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
- `from mko_get_mediascope_data import run_report` — `ImportError` at `code_ref d093d9c`; importable via PEP 562 at HEAD (ENT-004 rejected)
- `yaml_to_dict('/nonexistent/...')` — returns `None` at `code_ref d093d9c`; raises `FileNotFoundError` at HEAD (ENT-003 rejected)
- `LoggingSettings(**None)` — `TypeError` reproduced at `code_ref d093d9c`; not reachable at HEAD
- Root logger handler inspection before/after import
- AST scan for `print()` across all `core/` modules
- AST scan for Cyrillic characters in `app.py` docstrings/commands — 45 hits at `code_ref d093d9c`; 0 hits at HEAD (ENT-007 rejected)

**Not verified:** Runtime behavior inside Jupyter/IPython — no kernel available.
The `ipykernel.iostream.OutStream` `AttributeError` claim (ENT-001) is inferred from
the unconditional call site; the inference is substantiated (OutStream has no
`reconfigure` method) but not observed live.

**Unsettled claims:** ENT-001's Jupyter `AttributeError` consequence — inferred, not
observed. All other claims are directly reproduced.

## Conformance

**Validated against:** commit `d093d9c` (Phase 01 baseline)
**HEAD at re-validation:** `335a11e` (commit `e8fd1867` code-identical to `b04d177`)
**Survival at HEAD:** 2 fixed (ENT-001, ENT-006), 5 rejected as falsified at HEAD (ENT-002–005, ENT-007), 0 re-graded, 0 unsettled
**Correction type:** no-op — re-validation notes and corrected Summary are already inline; this block records the conformance verdict for cross-file tooling.

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). Correction applied: ENT-001 re-labelled from "falsified" to
**fixed at HEAD**, because the mechanism was relocated rather than merely
absent — `sys.stdout.reconfigure` moved into the `run` command body
(`src/mko_get_mediascope_data/app.py:45`, commit `706eed0`), so library
importers are unaffected and the CLI path still sets UTF-8 line-buffered
stdout. Mapping: ENT-001 → D-05.

---

## Findings

| ID      | Disposition  | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|--------------------------|-----------------|
| ENT-001 | fixed at HEAD (claim falsified at code_ref) | CRITICAL (at code_ref) | CRITICAL vs. fixed at HEAD | yes† |
| ENT-002 | rejected (falsified at HEAD) | CRITICAL (at code_ref) | CRITICAL vs. falsified at HEAD | yes† |
| ENT-003 | rejected (falsified at HEAD) | CRITICAL (at code_ref) | CRITICAL vs. falsified at HEAD | yes† |
| ENT-004 | rejected (falsified at HEAD) | HIGH (at code_ref) | HIGH vs. falsified at HEAD | yes† |
| ENT-005 | rejected (falsified at HEAD) | MEDIUM (at code_ref) | MEDIUM vs. falsified at HEAD | yes† |
| ENT-006 | fixed at HEAD (claim falsified at code_ref) | MEDIUM (at code_ref) | MEDIUM vs. fixed at HEAD | yes† |
| ENT-007 | rejected (falsified at HEAD) | LOW (at code_ref) | LOW vs. falsified at HEAD | yes† |

\* ENT-001/002/003/004/005/006/007: anchors resolved at code_ref d093d9c; the claims are
falsified at HEAD by the remediation commits that followed the validation:
`706eed0` (deferred singleton via `get_app_service()`; `yaml_to_dict` raises instead
of returning `None`; `sys.stdout.reconfigure` moved into the `run` command),
`63bb1c0` (tests migrated to the factory), `934cf9c0` (PEP 562 lazy re-exports in
`__init__.py`), `606c899` (`print()` replaced with `logger.info()`), `b04d177`
(Russian strings translated to English), and `755523e` (`reload_config()` added to
`AppService`, falsifying ENT-006's "no invalidation hook" assertion). See the re-validation
note and the marked sections below. All seven findings were remediated at HEAD.

### ENT-001 — fixed at HEAD — [CRITICAL at code_ref] `sys.stdout.reconfigure()` at module scope

**FIXED AT HEAD `335a11e` (the code_ref claim is falsified).** The reconfigure
call is no longer at
module scope: it now lives inside the `run` command body (`app.py:45`,
`sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")`), moved there in
commit `706eed0`. Importing `app.py` (CLI, console-script, or library) no longer
reconfigures `sys.stdout` on any import path, and the Jupyter/IPython
`AttributeError` path (an `OutStream` without `reconfigure`) is unreachable —
library callers are no longer affected. The claim, probe output, and grading
below are preserved as the `code_ref d093d9c` record and do not reproduce at HEAD.

**Anchor resolved:** `src/mko_get_mediascope_data/app.py:17` → line 17 is
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

### ENT-002 — rejected (falsified at HEAD) — [CRITICAL at code_ref] Singleton `app_service` bootstraps at import

**REJECTED — falsified at HEAD `335a11e`.** The anchor `services.py:163 →
app_service = AppService(...)` no longer exists: the module-scope singleton was
replaced by a lazy factory (`_app_service: AppService | None = None` plus
`get_app_service()`, at the tail of `core/services.py` — verified at lines 188-199),
landed in commit `706eed0` (tests migrated in the same commit). Importing
`services.py` (transitively via `app.py`, whose line 14 now imports
`get_app_service`) no longer instantiates `AppService`, performs no YAML I/O, and
opens no log handler; that work now happens on the first `get_app_service()` call.
The claim, probe output, and grading below are preserved as the `code_ref d093d9c`
record and do not reproduce at HEAD.

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

### ENT-003 — rejected (falsified at HEAD) — [CRITICAL at code_ref] Missing config swallowed as `None` → obscure `TypeError`

**REJECTED — falsified at HEAD `335a11e`.** `yaml_to_dict` (`core/utils.py:343-367`)
now returns `dict[str, Any]` — body `return yaml.safe_load(cfg) or {}` (line 361) —
and raises with `logger.error` logging on `yaml.YAMLError` (362-364) and
`FileNotFoundError` (365-367); it never returns `None`. `app_config`
(`core/services.py:37-55`) additionally carries explicit None-guards that raise
actionable `FileNotFoundError` messages (40-44, 48-52 — unreachable dead code at
HEAD). With the singleton deferred (commit `706eed0`), a missing config file now
fails at the first `get_app_service()` call with a logged, typed `FileNotFoundError`
— not an import-time `TypeError` — and the `init` command (app.py:26-32) calls
`init_project` directly without constructing `AppService`, so it remains reachable.
The claim, probes, and grading below are preserved as the `code_ref d093d9c`
record and do not reproduce at HEAD.

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

### ENT-004 — rejected (falsified at HEAD) — [HIGH at code_ref] Public API not re-exported from package root

**REJECTED — falsified at HEAD `335a11e`.** `__init__.py` is no longer empty: it
is 32 lines declaring `__all__` (`run_report`, `run_report_async`,
`initialize_settings`, `init`, `run`, `list_reports`, `app`) and a PEP 562
`__getattr__` lazy re-export that imports names from the `app` submodule on first
access — landed in commit `934cf9c0`. The documented contract
`from mko_get_mediascope_data import run_report` now works, and importing the
package does not eagerly execute `app.py`'s module body. The claim, probe output,
and grading below are preserved as the `code_ref d093d9c` record and do not
reproduce at HEAD.

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

### ENT-005 — rejected (falsified at HEAD) — [MEDIUM at code_ref] `print()` calls in `core/` bypass the logging layer

**REJECTED — falsified at HEAD `335a11e`.** No active `print()` calls remain in
`src/`: the four active call sites (`core/services.py:117/124/154`,
`core/reports.py:265`) were replaced with the logging layer in commit `606c899`,
and both modules now define `logger = logging.getLogger(__name__)`. The remaining
`print(` occurrences in `core/reports.py` (lines 58, 102, 257, 290, 333) are
commented-out debug lines — hygiene drift, not layer-discipline violations, and
`app.py`'s `console.print()` (Rich) is the allowed CLI surface. The claim, AST
output, and grading below are preserved as the `code_ref d093d9c` record and do
not reproduce at HEAD.

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

### ENT-006 — fixed at HEAD (claim falsified at code_ref) — [MEDIUM at code_ref] `app_config` cached on singleton is stale

**FIXED AT HEAD `335a11e` (the code_ref claim is falsified).** Remediation commit
`755523e` (*feat(core): add reload_config() to AppService for dynamic config reload
(ENT-006)*) added a `reload_config()` method to `AppService`
(`core/services.py:66-76`) that invalidates the `@cached_property` caches for both
`app_config` and `export_folder` and re-applies `dictConfig`. The original asserted
cause — "no `reload_config()` method exists" — was verified by source inspection at
`code_ref d093d9c` and is now falsified: the method exists and is covered by 5 tests
(`tests/test_services.py:102-145`). Additionally, the module-scope singleton was
deferred to `get_app_service()` (commit `706eed0`), so `app_config` is no longer
accessed at import time. The `@cached_property` decorator itself remains, but the
staleness concern is remediated by the explicit invalidation hook. The claim,
evidence, and grading below are preserved as the `code_ref d093d9c` record and do
not reproduce at HEAD.

**Anchor resolved:** `src/mko_get_mediascope_data/core/services.py:35` →
`@cached_property` decorator on `app_config` (line 35 at `code_ref`; line 37 at HEAD),
property body at lines 36-42 (code_ref) / 38-55 (HEAD). The `@cached_property` remains
at HEAD; `reload_config()` at lines 66-76 provides the invalidation hook that was absent
at `code_ref`.

**Claim tested:** `app_config` is a `cached_property`. It is first accessed during
`prepare_app()` in `__init__` (line 33 at `code_ref`), i.e. at import time via the
module-level singleton. Once cached on the instance, subsequent accesses return the
same object. Since `app_service` is a process-wide singleton, config edits in a
long-lived process (Jupyter/Airflow) are silently ignored. At HEAD, the singleton is
deferred behind `get_app_service()` (commit `706eed0`) and `reload_config()` (commit
`755523e`) allows explicit cache invalidation, so config edits are no longer silently
ignored.

**Evidence (source inspection, at `code_ref d093d9c`):**
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

At HEAD: `reload_config()` (`services.py:66-76`) pops `app_config` and
`export_folder` from `self.__dict__` and calls `prepare_app()`, so the next
`app_config` access re-reads the YAML files. Five tests cover this
(`tests/test_services.py:102-145`).

**Asserted cause tested separately:** `@cached_property` has no invalidation hook.
The singleton is process-global. No `reload_config()` method exists. Verified by
source inspection — search for "reload" returns no results. At HEAD,
`reload_config()` exists (`services.py:66-76`) and the singleton is deferred behind
`get_app_service()` (`services.py:188-199`), falsifying both sub-claims.

**Rubric grading:** Phase 01 MEDIUM = "an import-time side effect that is
unnecessary but not harmless." The caching occurs at import time (via `__init__`
→ `prepare_app` → `app_config` access). In single-shot CLI it's harmless; in
long-lived processes it serves stale config. MEDIUM carried = MEDIUM on rubric.
No re-grade.

**Load-bearing artefact:** Code (`services.py:35`).

**Recommendation carry-out:** "add a `reload_config()` method to `AppService`" —
target `AppService` class exists and is stable. ✓

---

### ENT-007 — rejected (falsified at HEAD) — [LOW at code_ref] Russian docstrings and comments violate English-only standard

**REJECTED — falsified at HEAD `335a11e`.** A Cyrillic grep
(`[А-Яа-яЁё]`) over the whole `src/mko_get_mediascope_data/` tree returns **no
matches**: the entry-layer docstrings, Typer `help=` strings, and
`console.print()` messages in `app.py` were translated to English in commit
`b04d177` (ENT-007), together with the `core/` modules cited below. AGENTS.md
rule 1 and `doc-maintenance-rules.md` are satisfied at HEAD. The claim, scan
output, and grading below are preserved as the `code_ref d093d9c` record and do
not reproduce at HEAD.

**Anchor resolved:** `src/mko_get_mediascope_data/app.py:2` → module docstring:
`"""mko_get_mediascope_data — основной публичный интерфейс"""`.

**Claim tested:** Entry-layer docstrings, Typer command docstrings, `help=` strings,
and `console.print()` messages in `app.py` are in Russian. Error messages in
`core/services.py` (lines 117, 124, 154) and loggers in `core/reports.py` are also
in Russian. This violates AGENTS.md rule 1 and
`docs/00-overview/doc-maintenance-rules.md` (Translation Rules).

**Evidence (AST scan for Cyrillic in app.py):**
```text
Module docstring: 'mko_get_mediascope_data — основной публичный интерфейс'
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
anchors (e.g., `src/mko_get_mediascope_data/app.py:17`). All line numbers resolve
correctly to the cited content. No anchor points at a non-existent line.

**Impact:** Format drift only — all anchors are mechanically verifiable. Does not
affect finding accuracy or disposition.

**Disposition:** confirmed. Recommendation: update anchors to `path::Symbol` format
(e.g., `src/mko_get_mediascope_data/app.py::sys.stdout.reconfigure` at module scope)
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

**Disposition:** confirmed. No finding re-grade needed. Superseded at HEAD — `app_config` moved to `core/services.py:37-55` (`LoggingSettings` construction at line 45) and ENT-003's mechanism is rejected; see the re-validation note.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 01 finding | Phase 02 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| yaml_to_dict None return | ENT-003 (CRITICAL) | CFG-005 (MEDIUM) | `utils.py:yaml_to_dict` catches `FileNotFoundError`, falls through to `return None` | cross-reference; both rejected at HEAD — `yaml_to_dict` raises (utils.py:343-367) |
| print() in core/ | ENT-005 (MEDIUM) | CFG-010 (LOW) | `print()` calls in `core/services.py` and `core/reports.py` bypass logging | cross-reference; both rejected at HEAD — `print()` replaced with `logger.info()` (commit 606c899) |

**Severity divergence rationale:** The two phases use different rubrics. Phase 01
(entry-architecture) graded `print()` as MEDIUM (layer-discipline at entry layer)
and Phase 02 (config-secrets) as LOW (minor issue in config context); both gradings
were anchored at `code_ref d093d9c`, and at HEAD both findings are rejected because
the `print()` calls were replaced with `logger.info()` (commit `606c899`). These
are **cross-references**, not conflicts — neither phase reaches an opposite conclusion
about the same fact.

No cross-phase conflicts detected. No finding was withdrawn, downgraded, or re-graded
due to cross-phase interaction; the ENT-003/CFG-005 pair is jointly rejected at HEAD by the `yaml_to_dict` remediation.

---

## Rollout Safety Assessment

**Post-validation remediation applied at HEAD:** the singleton was moved out of module scope (commit `706eed0`, which deferred the singleton via `get_app_service()` and made `yaml_to_dict` raise instead of returning `None`) and tests were migrated to the factory in commit `63bb1c0`. The same remediation sweep also moved `sys.stdout.reconfigure` into the `run` command (commit `706eed0`, ENT-001), added PEP 562 lazy re-exports to `__init__.py` (commit `934cf9c0`, ENT-004), replaced the `print()` calls in `core/` with `logger.info()` (commit `606c899`, ENT-005/CFG-010), and translated all Russian strings to English (commit `b04d177`, ENT-007). The risks, dependencies, and the closing statement below are preserved as the `code_ref d093d9c` record.
- `tests/test_services.py:8` imports `from mko_get_mediascope_data.core.services import AppService, get_app_service` (migrated to the factory per commit `63bb1c0`); `tests/test_services.py:55` calls `get_app_service().run_report(test_yaml)`.
- `tests/test_app.py:6` imports `from mko_get_mediascope_data.app import app`, which
  imports `get_app_service` (a function) at module scope — no bootstrap at import.
- Tests were migrated to the `get_app_service()` factory (commit `63bb1c0`); they no
  longer depend on a module-scope singleton. The lazy accessor is in use.

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

**Rollout risk (as of `code_ref: d093d9c`): none of the fixes had been applied.** At HEAD the ENT-002/ENT-003 fixes are applied (commit `706eed0` deferred the singleton and made `yaml_to_dict` raise); the finding texts above remain the validation-time record.

---

## Evidence Index

| Evidence | Supports |
|---|---|
| Runtime probe: stdout encoding cp1252→utf-8, line_buffering False→True on import (at `code_ref d093d9c`; reconfigure moved into the `run` command at HEAD) | ENT-001 |
| Runtime probe: root logger gains `RotatingFileHandler` on `log.log` at import (at `code_ref d093d9c`; deferred at HEAD — first `get_app_service()` call) | ENT-002 |
| Runtime probe: `yaml_to_dict('/nonexistent')` → `None` (at `code_ref d093d9c`; raises `FileNotFoundError` at HEAD) | ENT-003 |
| Runtime probe: `LoggingSettings(**None)` → `TypeError` (at `code_ref d093d9c`; not reachable at HEAD) | ENT-003 |
| Runtime probe: `from mko_get_mediascope_data import run_report` → `ImportError` (at `code_ref d093d9c`; importable via PEP 562 at HEAD) | ENT-004 |
| Runtime probe: `__init__.py` is 0 bytes (at `code_ref d093d9c`; 32-line lazy re-export at HEAD) | ENT-004 |
| AST scan: four `print()` calls in `core/services.py` and `core/reports.py` (at `code_ref d093d9c`; 0 active at HEAD — commit 606c899) | ENT-005 |
| Source inspection: `@cached_property` on `app_config`, first accessed in `__init__` (at `code_ref d093d9c`; `reload_config()` exists at HEAD — services.py:66-76) | ENT-006 |
| AST scan: Cyrillic in `app.py` module docstring and all command docstrings (at `code_ref d093d9c`; no Cyrillic in `src/` at HEAD — commit b04d177) | ENT-007 |
| `pyproject.toml:44`: console-script entry point `mko_get_mediascope_data.app:app` | ENT-001, ENT-002 |
| `app.py:14`: `from .core.services import app_service` at module scope (code_ref; HEAD imports the `get_app_service` factory) | ENT-002, ENT-003 |
| `doc-maintenance-rules.md:101`: logging mandate for `core/` | ENT-005 |
| `AGENTS.md` rule 1: English Only mandate | ENT-007 |
