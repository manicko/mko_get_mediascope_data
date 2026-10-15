---
plan_id: "01-entry-architecture-fixes"
source_audit: ".ai/audit/99-validation/01-entry-architecture-validated-findings.md"
source_code_ref: "d093d9c"
phase: "01-entry-architecture"
status: draft
agent_role: "planner"
findings_covered: ["ENT-001", "ENT-002", "ENT-003", "ENT-004", "ENT-005", "ENT-006", "ENT-007"]
---

# Execution Plan — Entry Architecture Fixes

## 1. Plan Overview and Scope

### Purpose

This plan decomposes seven validated audit findings (3 CRITICAL, 1 HIGH, 2 MEDIUM, 1 LOW)
from the **01-entry-architecture** phase into five dependency-aware execution blocks. Each
block groups findings that share file targets and have clear inter-dependencies, minimizing
intermediate broken states during rollout.

### Scope

**In scope** — all seven findings (ENT-001 through ENT-007) and the test files coupled to
the module-scope singleton:

- `src/mko_get_mediascope_data/app.py`
- `src/mko_get_mediascope_data/core/services.py`
- `src/mko_get_mediascope_data/core/utils.py`
- `src/mko_get_mediascope_data/core/reports.py`
- `src/mko_get_mediascope_data/core/models.py`
- `src/mko_get_mediascope_data/core/network.py`
- `src/mko_get_mediascope_data/core/tasks.py`
- `src/mko_get_mediascope_data/__init__.py`
- `tests/test_services.py`
- `tests/test_app.py`

**Out of scope** — `core/logger.py` (confirmed empty placeholder, not directly referenced
by any finding), `init_service.py` (not modified by any finding), `core/errors.py`,
`core/paths.py`, `core/tasks.py` is only touched by ENT-007 translation if Russian strings
are found.

### Corrected plan drift (from validated code_context)

The validated findings report (ENT-005, p. 255) states that *both* `services.py` and
`reports.py` lack a module-level logger. **This is incorrect for `reports.py`**: it defines
`logger = logging.getLogger(__name__)` at line 24. Only `services.py` lacks a module-level
logger. All planning below reflects this correction:

- ENT-005's `reports.py` work is limited to replacing the single `print()` call at line 265
  with `logger.info()` — no logger needs to be added.
- ENT-005's `services.py` work requires **adding** a module-level logger and replacing
  three `print()` calls at lines 117, 124, and 154.

### Key constraints

1. **One Implementor at a time** — `.kilo/commands/../commands.md` mandates max 1
   implementor agent. Blocks execute sequentially, not in parallel.
2. **No background agents** — agents must not be launched in background.
3. **Semantic targeting** — all edits target symbols/functions/classes, never line numbers.
4. **Production code is king** — if tests conflict with architecture, fix/remove tests;
   never distort production code for tests.
5. **`core/logger.py` is empty** (1 line, whitespace only) — modules use
   `logging.getLogger(__name__)` directly. No finding requires populating it.

---

## 2. Dependency Map

```text
ENT-002 (defer singleton)                    [CRITICAL — services.py]
  ├── blocks ENT-004 (lazy re-export must avoid triggering import side-effects)
  ├── enables ENT-003's init-reachability fix (init command becomes reachable)
  └── makes ENT-006's staleness a concern (singleton is process-global)
ENT-003 (yaml_to_dict raises)                [CRITICAL — utils.py + services.py]
  └── surfaces clear error before ENT-001's stdout reconfigure (import ordering)
ENT-001 (stdout.reconfigure move)            [CRITICAL — app.py]
ENT-006 (reload_config)                      [MEDIUM — services.py] ← depends on ENT-002
ENT-004 (lazy re-export)                     [HIGH — __init__.py] ← depends on ENT-002
ENT-005 (print → logger)                     [MEDIUM — services.py, reports.py]
ENT-007 (Russian → English)                  [LOW — app.py, reports.py, models.py,
                                                utils.py, network.py, tasks.py]
  └── entangled with ENT-005 (services.py print() messages are Russian)
```

### Runtime call-site map for `yaml_to_dict` (ENT-003)

| Location | Context | Current behavior on missing file |
|----------|---------|----------------------------------|
| `services.py:38` | `AppService.app_config` (cached_property) — import-time via singleton | `None` → `TypeError` at `LoggingSettings(**None)` |
| `services.py:40` | `AppService.app_config` (cached_property) — import-time via singleton | `None` → (unreached, crash at line 38 first) |
| `services.py:60` | `AppService.get_report_settings` — runtime | `None` → `yield ReportSettings(**None)` → `TypeError` |
| `services.py:74` | `AppService.get_default_data_settings` — runtime | `None` → `DataDefaults(**None)` → `TypeError` |

After ENT-002 defers the singleton, the lines 38/40 crash is deferred to lazy
access. After ENT-003, `yaml_to_dict` raises `FileNotFoundError` at all four sites,
producing an actionable error immediately.

### Test coupling risk (from validated findings, p. 429-434)

- `tests/test_services.py:6` — `from mko_get_mediascope_data.core.services import app_service`
  then `app_service.run_report(test_yaml)` at line 53. This import triggers the full
  singleton bootstrap including `dictConfig`.
- `tests/test_app.py:3` — `from mko_get_mediascope_data.app import app`, which triggers
  `app.py` module body including `from .core.services import app_service` at line 14.
- `tests/test_app.py:9` — `assert "Инициализировать" in result.stdout` — a Russian
  string assertion that will break after ENT-007 translation.

Both tests must be adapted as part of Block A (test_services.py) and Block D/E (test_app.py).

---

## 3. Per-Block Breakdown

### Block A — Critical Bootstrap Decoupling

| Field | Value |
|-------|-------|
| **Block ID** | A |
| **Findings** | ENT-002 (CRITICAL), ENT-003 (CRITICAL), ENT-001 (CRITICAL) |
| **Files** | `core/services.py`, `core/utils.py`, `app.py`, `tests/test_services.py`, `tests/test_app.py` |
| **Severity** | CRITICAL (all three) |
| **blocked_by** | None — this is the root block |
| **Required agents** | Auditor (verify anchors), Planner (task breakdown), Implementor |
| **Goes direct to Implementor?** | No — Planner needed for detailed task_description |

#### Scope

Three interlocking CRITICAL findings, all rooted in import-time side effects:

1. **ENT-002** — Remove module-scope `app_service = AppService(...)` at
   `services.py:163-166`. Add a lazy accessor `get_app_service() -> AppService` that
   instantiates on first call and caches the instance. Remove the module-scope
   `dictConfig` call at `services.py:166` — it should be invoked inside
   `AppService.__init__` or `prepare_app()` (already is, via `app_config` access).
   Update `app.py` line 14 to import `get_app_service` instead of `app_service`,
   and update all call sites in `app.py` (`run` at line 51, `list_reports` at line 62,
   `run_report_async` at line 81).

2. **ENT-003** — In `utils.py:yaml_to_dict`, replace the silent `return None` fallthrough
   with a re-raised `FileNotFoundError` (or a wrapped, more descriptive error). Add a
   defensive `None` guard in `AppService.app_config` (services.py:35-42) that raises
   an actionable error if `yaml_to_dict` ever returns `None` (e.g., for a YAML file
   whose top-level node is `null`). This guard covers the runtime call sites at
   services.py:60 and services.py:74 as well.

3. **ENT-001** — Move `sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")`
   from `app.py:17` (module scope) into the body of the CLI `run` command function
   (around line 46-48, before the report execution).

#### Sub-ordering within block

```
Step 1: ENT-002 — Defer singleton
  - Add get_app_service() to services.py
  - Update app.py imports and call sites
  - Adapt tests/test_services.py:6-53
  → After step 1: importing app.py or services.py triggers NO AppService bootstrap.

Step 2: ENT-003 — yaml_to_dict raises + None guard
  - Modify utils.py:yaml_to_dict to raise on missing file
  - Add None guard in AppService.app_config
  → After step 2: missing config files produce clear FileNotFoundError at all call sites.

Step 3: ENT-001 — Move stdout.reconfigure
  - Move line 17 into run() command body
  → After step 3: importing app.py does not reconfigure stdout.
```

#### Implementation approaches (decision points)

**ENT-002 caching mechanism** — two viable implementations for `get_app_service()`:

- **Option A (module-level variable + None sentinel)** — explicit, simple, allows
  `reload_config()` to reset the cache by setting the variable to `None`. No decorator
  magic. This is the most maintainable approach given ENT-006 also needs cache
  invalidation.
- **Option B (`functools.lru_cache(maxsize=1)`)** — concise but requires
  `get_app_service.cache_clear()` for reload, which couples Block A with Block B's
  implementation.

**ENT-003 error type** — two viable approaches:

- **Option A (re-raise `FileNotFoundError`)** — preserves the original traceback and
  exception type. Minimal change: remove the `except FileNotFoundError` block or
  re-raise with `raise` after logging.
- **Option B (wrap in a domain-specific error)** — e.g., raise a `ConfigError` with
  a descriptive message. Requires defining a new exception class (none currently exist
  in `core/errors.py`).

**ENT-001 placement** — two viable targets within `run()`:

- **Option A (top of `run()` body)** — execute before any logic; cleanest separation.
- **Option B (inside the `try` block)** — executes only if no early return; but `run`
  has no early return currently, so this is equivalent.

> **Planner recommendation**: Option A for all three decisions. Module-level variable
> for `get_app_service()` (simplest, most compatible with ENT-006). Re-raise
> `FileNotFoundError` (no new exception class needed). Move `stdout.reconfigure` to
> the top of `run()` body.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| **Test coupling** — `test_services.py:6` imports `app_service` directly | High | Update test to call `get_app_service()` instead. The test at line 53 (`app_service.run_report(test_yaml)`) must become `get_app_service().run_report(test_yaml)`. |
| **Test coupling** — `test_app.py:3` imports `app` which triggers `run` command help assertion | Medium | `test_app.py:9` asserts Russian `"Инициализировать"` — this is a Block D/E concern (ENT-007). The import itself still works after Block A. |
| **`get_app_service()` changes the API surface** — `app_service` name is removed | Medium | Verify no other code references `app_service` directly. Grep confirms only `app.py` (lines 14, 51, 62, 81) and `tests/test_services.py:6` reference it. |
| **`dictConfig` moved** — currently at module scope (line 166); after ENT-002 it runs inside `__init__` | Low | `dictConfig` is already called in `prepare_app()` indirectly via `app_config` access in `__init__`. Verify logging still works after the change. |
| **`yaml_to_dict` raising breaks runtime call sites** (services.py:60, 74) | Low | Both call sites are inside methods that already have `try/except` or are expected to fail loudly on missing config. The current `return None` causes a `TypeError` anyway — raising `FileNotFoundError` is strictly better. |
| **`__main__.py` entry point** — `from .app import app` at line 1 | Low | Still works — `app` is defined in `app.py` regardless of singleton deferral. |

#### Verification

- `python -c "import mko_get_mediascope_data.app"` — should NOT instantiate AppService,
  should NOT reconfigure stdout, should NOT create log files.
- `python -c "from mko_get_medioscope_data.core.services import get_app_service"` —
  should not bootstrap.
- `get_app_service()` called twice returns the same instance (singleton preserved).
- `yaml_to_dict("/nonexistent/file.yaml")` raises `FileNotFoundError`, not returns `None`.
- `mko-get-mediascope init --help` works (CLI `init` is reachable after singleton deferral).
- `mko-get-mediascope run --help` works.
- `python -m mko_get_medioscope_data init` executes without crash.

---

### Block B — Config Cache Invalidation

| Field | Value |
|-------|-------|
| **Block ID** | B |
| **Findings** | ENT-006 (MEDIUM) |
| **Files** | `core/services.py` |
| **Severity** | MEDIUM |
| **blocked_by** | Block A (ENT-002) — staleness only matters because singleton is process-global |
| **Required agents** | Implementor |
| **Goes direct to Implementor?** | Yes — audit recommendation is specific and implementation is standard |

#### Scope

Add a `reload_config()` method to `AppService` that invalidates the `@cached_property`
`app_config` (services.py:35-42) so that subsequent accesses re-read the YAML files.
The standard mechanism is `del self.__dict__["app_config"]` (or use
`self.__dict__.pop("app_config", None)` for safety). After invalidation, the next
`self.app_config` access re-runs the property body, and `prepare_app()` must be called
again to re-apply `dictConfig` and re-resolve handler paths.

#### Implementation approach

```python
def reload_config(self) -> None:
    """Invalidate the cached app_config and re-apply configuration."""
    self.__dict__.pop("app_config", None)
    self.__dict__.pop("export_folder", None)
    self.prepare_app()
```

Both `app_config` and `export_folder` are `cached_property` and depend on config;
invalidate both. `prepare_app()` re-reads `app_config` (re-triggering the property)
and re-applies handler path resolution.

After Block A, `prepare_app()` is called in `__init__`. The `dictConfig` call at
line 166 (module scope) is part of Block A's removal — it should move into
`__init__` or `prepare_app()`. This means `reload_config()` must also re-apply
`dictConfig`. **Decision**: call `dictConfig` inside `prepare_app()` so that both
initial bootstrap and reload apply logging configuration consistently.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| **`dictConfig` currently at module scope (line 166)** — must be relocated as part of Block A or this block | Medium | Coordinate with Block A: the `dictConfig` call should live in `__init__` or `prepare_app()`. If Block A already moves it, Block B just calls `prepare_app()` in `reload_config()`. |
| **Other cached_properties affected** — `export_folder` (line 51) also caches | Low | Invalidate both in `reload_config()`. |
| **`reload_config()` called before first `app_config` access** — `pop` with default handles this | None | Use `self.__dict__.pop("key", None)` pattern. |

#### Verification

- `svc = get_app_service(); svc.app_config is svc.app_config` → True (cached).
- `svc.reload_config(); svc.app_config is not original_config` → True (cache invalidated).
- Logging handlers are re-applied after `reload_config()`.
- No errors when `reload_config()` is called before any `app_config` access.

---

### Block C — Lazy Package Re-Export

| Field | Value |
|-------|-------|
| **Block ID** | C |
| **Findings** | ENT-004 (HIGH) |
| **Files** | `__init__.py` |
| **Severity** | HIGH |
| **blocked_by** | Block A (ENT-002) — lazy `__getattr__` avoids triggering import-time side-effects |
| **Required agents** | Implementor |
| **Goes direct to Implementor?** | Yes — the audit recommends PEP 562 `__getattr__` and lists the exact symbols |

#### Scope

`__init__.py` is currently 0 bytes. Add a PEP 562 module-level `__getattr__` that
lazily resolves the public API names from `app.py`. This avoids eagerly importing
`app.py` (which, even after Block A, still imports `get_app_service` and defines the
Typer `app` object) at package import time.

The `__all__` list is defined in `app.py` (lines 104-112):
```python
__all__ = ["run_report", "run_report_async", "initialize_settings", "init", "run", "list_reports", "app"]
```

#### Implementation approach

```python
from typing import Any

__all__ = [
    "run_report",
    "run_report_async",
    "initialize_settings",
    "init",
    "run",
    "list_reports",
    "app",
]

def __getattr__(name: str) -> Any:
    if name in __all__:
        from .app import app as _app  # noqa: F401 — deferred import
        # Re-export from app module
        import importlib
        module = importlib.import_module("mko_get_mediascope_data.app")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

**Simpler variant** — since `app.py` is a single module, the `__getattr__` can
directly import from it:

```python
def __getattr__(name: str) -> Any:
    if name in __all__:
        from .app import app as _app_module
        # Map name to the attribute in app
        ...
```

> **Planner recommendation**: Use `importlib.import_module` inside `__getattr__` for
> clean, lazy, cached resolution. Cache the resolved value in `globals()` so the import
> only happens once per name.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| **Naive `from .app import *` would trigger import side-effects** | Addressed by design | PEP 562 `__getattr__` defers all imports until attribute access. |
| **`app.py` imports trigger `get_app_service()`?** | No — after Block A, `app.py` imports `get_app_service` (a function), not the instance. No bootstrap at import. | Block A must complete first (dependency). |
| **`__getattr__` raising `AttributeError` for unknown names** | Standard Python behavior | `from package import nonexistent` raises `ImportError`, which is correct. |
| **`dir(package)` doesn't show `__all__` names** | Known PEP 562 limitation | Acceptable — IDE/tab-completion support is secondary to correctness. |

#### Verification

- `python -c "from mko_get_mediascope_data import run_report"` succeeds (no ImportError).
- `python -c "from mko_get_mediascope_data import initialize_settings"` succeeds.
- `python -c "import mko_get_mediascope_data"` does NOT trigger `get_app_service()` bootstrap.
- `python -c "from mko_get_medioscope_data import nonexistent"` raises `ImportError`.

---

### Block D — Logging Discipline

| Field | Value |
|-------|-------|
| **Block ID** | D |
| **Findings** | ENT-005 (MEDIUM) + partial ENT-007 (services.py print messages are Russian) |
| **Files** | `core/services.py`, `core/reports.py` |
| **Severity** | MEDIUM |
| **blocked_by** | None (ENT-005 is independent; ENT-007 entanglement is internal to this block) |
| **Required agents** | Implementor |
| **Goes direct to Implementor?** | Yes — audit enumerates exact call sites; approach is unambiguous |

#### Scope

**ENT-005** — Replace `print()` calls in `core/` modules with `logger.info()`:

1. `services.py` — Add `logger = logging.getLogger(__name__)` at module level (currently absent).
   Replace three `print()` calls:
   - Line 117: `print(f"\n{'-' * 10}  Обработка стартовала: {start_time} {'-' * 10}\n", flush=True)`
   - Line 124: `print(f"\nОтчёт {idx} → {report_settings.report_subtype}\n")`
   - Line 154: `print(f"\n{'-' * 10}  Обработка завершена: {end_time}. Общее время: {end_time - start_time} {'-' * 10}\n", flush=True)`

2. `reports.py` — Logger already exists (line 24). Replace one `print()` call:
   - Line 265: `print("=", end="", flush=True)`

**ENT-007 (partial)** — The three `print()` messages in `services.py` contain Russian
text ("Обработка стартовала", "Отчёт", "Обработка завершена", "Общее время"). These
must be translated to English as part of the `print() → logger.info()` replacement.

#### Entanglement note

Since Block D replaces the `services.py` print() calls (which are the only Russian
strings in that file), the Russian-to-English translation of `services.py` is fully
covered by this block. Block E does **not** need to revisit `services.py`.

For `reports.py`, the `print("=")` at line 265 has no Russian text — pure mechanism change.
The Russian `logger` calls at lines 149, 152, 379, 453, 466 are handled in Block E.

#### Logger semantics for `print("=", end="", flush=True)`

The `reports.py:265` call prints a single `=` character as a progress indicator with
no newline, flushed immediately. Replacing with `logger.info("=")` changes behavior:
the log handler will add its own formatting (timestamp, level, logger name) and
newline. **This is intentional per the audit recommendation** — progress indication
should go through the logging layer, not raw stdout. The Rich `Console` in `app.py`
remains the channel for user-facing output.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| **`services.py` has no module-level logger** — must add one | Low | Add `logger = logging.getLogger(__name__)` following the pattern in `utils.py:15` and `reports.py:24`. |
| **Removing `flush=True`** changes buffering behavior | Low | `logger.info()` output is flushed by the handler (StreamHandler flushes after each emit). No functional impact. |
| **Test assertion on output** — no tests assert on these print messages | None | Grep tests/ for "Обработка" or "Отчёт" — no matches found. |
| **ENT-007 in test_app.py:9** — `assert "Инициализировать" in result.stdout` | Medium | This is a test adaptation required. See Block E for the test_app.py Russian assertion fix. |

#### Verification

- AST scan for `print(` across all `core/` modules — zero matches.
- `services.py` has `logger = logging.getLogger(__name__)`.
- `ruff check` passes (no unused `print` references).

---

### Block E — Russian → English Translation

| Field | Value |
|-------|-------|
| **Block ID** | E |
| **Findings** | ENT-007 (LOW) |
| **Files** | `app.py`, `core/reports.py`, `core/models.py`, `core/utils.py`, `core/network.py`, `core/tasks.py`, `tests/test_app.py` |
| **Severity** | LOW |
| **blocked_by** | None (independent, but must come after Block D to avoid double-editing `services.py`) |
| **Required agents** | Implementor |
| **Goes direct to Implementor?** | Yes — mechanical translation, audit enumerates all strings |

#### Scope

Translate all Russian/Cyrillic text in docstrings, comments, log messages, error
strings, Typer `help` strings, and `console.print()` messages to clear English.

**Note**: `services.py` is excluded — Block D already translated its three Russian
print messages during the `print() → logger.info()` replacement.

**Note**: `core/logger.py` is not listed as Russian — it is empty.

#### Per-file Russian string inventory (after Block D exclusions)

**`app.py`** (highest user impact — CLI help text and console output):
- Module docstring (line 2): `основной публичный интерфейс`
- Typer `help` (line 22): `Инструмент выгрузки данных из Mediascope (TV / TV_REG)`
- `help` (line 30): `Перезаписать существующие файлы`
- Docstring (line 33): `Инициализировать пользовательские настройки`
- `console.print` (line 35): `Настройки инициализированы`
- `help` (line 42): `Путь к yaml-файлу задания`
- Docstring (line 46): `Запустить выгрузку отчёта`
- `console.print` (lines 48, 52, 55): `Запуск отчёта`, `Отчёт успешно завершён`, `Ошибка`
- Docstring (line 61): `Показать все доступные файлы заданий`
- `console.print` (line 67): `Нет файлов в reports/`
- Docstring (line 76): `Асинхронная версия для async-окружений`
- `raise` (line 80): `Файл не найден`
- Docstrings (lines 86, 96): `Синхронная обёобтка`, `Обёртка для Jupyter/Airflow`

**`core/reports.py`** (logger messages only — no print after Block D):
- Logger warning (line 149): `Расчёт: ... будет пропущен`
- Logger info (line 152): `Задача ... готова`
- Comments (lines 187, 290-291, 300, 333): `общие поля`, `Получаем результат`, etc.
- Docstrings (lines 349, 355): `Генерирует задачи`, `Подготавливает DataFrame`
- Logger warning (line 379): `Файл ... уже есть в папке, пропускаем`
- Logger error (line 453): `Ошибка при обработке данных`
- Logger error (line 466): `Отсутствуют колонки`

**`core/models.py`**:
- `ValueError` (lines 75, 78): `Нужно указать либо date_filter, либо last_time`,
  `date_filter должен содержать 2 даты`

**`core/utils.py`**:
- Docstring (line 37): `Генерирует последовательные интервалы...`
- Docstring (line 92): `Возвращает начало и конец периода...`
- Docstring (lines 134-135): `Возвращает полное расширение файла...`
- `raise ValueError` (line 55): `Неизвестная частота`
- Logger exception (line 251): `Ошибка при обработке частот`

**`core/network.py`**:
- Module docstring (line 1): `Network handler — общие сетевые вызовы...`
- Comments (lines 46, 81): `Готово к расширению`, `401 — это известная клиентская ошибка`

**`core/tasks.py`**:
- Logger info (line 44): `Сохраняю настройки задачи`

**`tests/test_app.py`**:
- Assertion (line 9): `assert "Инициализировать" in result.stdout` → must become
  `assert "Initialize" in result.stdout`

#### Implementation approach

This is a mechanical translation task. No design decisions. The Implementor should:

1. Translate all Russian strings to English, preserving:
   - f-string structure (e.g., `f"Report {idx} → {report_settings.report_subtype}"`)
   - Rich markup tags (e.g., `[green]`, `[blue]`, `[red]`, `[yellow]`)
   - Variable names and technical terms (e.g., `yaml-файлу` → `YAML file`)
2. For CLI-facing messages (console.print in app.py), use concise, user-friendly English.
3. For error messages, use clear, actionable English.
4. For docstrings, use standard Python docstring English.
5. Run `ruff check` and `ruff format` to ensure no line-length issues from translation
   (ruff ignores E501, but `ruff format` may rewrap).

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| **Rich markup tags embedded in Russian text** | Low | Tags like `[green]✅ ...[/green]` are format-agnostic; translate only the inner text. |
| **f-string variable names in Russian messages** | Low | Preserve all `{variable}` placeholders; only translate surrounding text. |
| **test_app.py:9 assertion** breaks after translation | Medium | Update assertion to match English output. This is a test adaptation, not production code distortion. |
| **Line length changes** from translation | Low | `ruff format` handles wrapping. E501 is ignored by ruff config. |

#### Verification

- `grep` for Cyrillic characters in all source files — zero matches.
- `test_app.py` assertion updated and passes.
- `ruff check` passes on all modified files.
- CLI `--help` output is in English.

---

## 4. Overall Execution Sequence

```text
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  BLOCK A  (CRITICAL)                                        │
│  ENT-002: Defer singleton → get_app_service()    [services.py]│
│  ENT-003: yaml_to_dict raises + None guard        [utils.py  │
│           + services.py]                                     │
│  ENT-001: Move stdout.reconfigure into run()        [app.py]  │
│  └─ step 1: defer singleton + update app.py calls          │
│  └─ step 2: yaml_to_dict → raise + None guard              │
│  └─ step 3: move stdout.reconfigure into run()             │
│  └─ adapt test_services.py                                   │
│                                                             │
│         ↓                                                   │
│                                                             │
│  BLOCK B  (MEDIUM)                                          │
│  ENT-006: Add reload_config() to AppService     [services.py]│
│  └─ invalidate cached_property + re-apply dictConfig       │
│                                                             │
│         ↓                                                   │
│                                                             │
│  BLOCK C  (HIGH)                                            │
│  ENT-004: PEP 562 __getattr__ in __init__.py               │
│                                                             │
│         ↓                                                   │
│                                                             │
│  BLOCK D  (MEDIUM)                                          │
│  ENT-005: print() → logger.info()        [services.py]      │
│  + partial ENT-007: translate services.py  [reports.py]     │
│  print messages to English                                 │
│                                                             │
│         ↓                                                   │
│                                                             │
│  BLOCK E  (LOW)                                             │
│  ENT-007: Russian → English across remaining files            │
│  (app.py, reports.py, models.py, utils.py,                  │
│   network.py, tasks.py, test_app.py)                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Rationale for ordering

1. **Block A first** — The three CRITICAL findings form the foundation. All other
   blocks either depend on Block A (Blocks B, C) or modify files that Block A touches
   (Block D modifies `services.py`). Deferring the singleton unblocks `init` reachability
   and removes the import-time crash chain.

2. **Block B before Block C** — ENT-006's `reload_config()` needs to integrate with
   the `get_app_service()` caching mechanism from Block A. Doing it immediately after
   ensures the caching pattern is fresh. It touches only `services.py`.

3. **Block C after Block A** — ENT-004's PEP 562 `__getattr__` must be lazy to avoid
   triggering the import side-effects that Block A fixes. Cannot be done in parallel
   with Block A (depends on `get_app_service()` existing in `services.py`).

4. **Block D after Block A** — modifies `services.py` which Block A restructures
   significantly (removes module-scope singleton). Must wait for Block A to complete
   to avoid merge conflicts.

5. **Block E last** — depends on Block D for `services.py` translation (Block D handles
   the print→logger translation of the only Russian strings in services.py). All other
   files in Block E are independent. Running last avoids double-editing `reports.py`
   (Block D touches one line, Block E touches multiple lines).

### Parallelizable work

Given the one-implementor constraint, blocks are strictly sequential. However, within
Block E, the file translations are independent of each other and *could* be parallelized
if multiple implementors were available:
- `app.py` translation is independent of `models.py` translation
- `network.py` translation is independent of `tasks.py` translation
- `core/utils.py` translation is independent of `core/reports.py` translation

---

## 5. Open Design Questions Requiring Researcher Input

The following decisions have viable alternatives. The audit provides a recommended
direction in each case, but a Researcher could produce a comparison matrix if
stakeholders want to evaluate trade-offs before committing.

### RQ-1: `get_app_service()` caching mechanism (Block A, ENT-002)

**The decision**: After deferring the singleton, how should `get_app_service()` cache
the instance?

- **Option A (module-level variable)** — explicit `if _instance is None` pattern.
  Pros: simple, readable, easily invalidated by `reload_config()`. Cons: uses `global`
  keyword.
- **Option B (`functools.lru_cache(maxsize=1)`**) — decorator-based. Pros: idiomatic,
  thread-safe-ish. Cons: `reload_config()` must call `get_app_service.cache_clear()`,
  creating coupling between Block A and Block B.
- **Option C (module-level `__getattr__`)** — make `services.py` itself lazy via PEP 562,
  so `from ...services import app_service` works but defers instantiation.
  Pros: backward-compatible with existing `app_service` imports. Cons: still triggers
  bootstrap on first access (just deferred, not suppressible by library callers —
  the exact problem the audit flags).

**Audit recommendation**: "defer behind a factory or lazy accessor
(`get_app_service()`)" — implies Option A or B, not Option C.

**Researcher recommendation**: Compare Options A and B specifically for their
`reload_config()` integration. The audit does not resolve which caching mechanism
pairs best with ENT-006.

### RQ-2: `__init__.py` re-export strategy (Block C, ENT-004)

**The decision**: PEP 562 `__getattr__` (lazy) vs direct `from .app import *` (eager).

- **Option A (PEP 562 `__getattr__)** — lazy, defers all imports. Pros: no import side
  effects, only resolves accessed names. Cons: `dir(package)` doesn't show all names.
- **Option B (direct imports)** — `from .app import run_report, run_report_async, ...`.
  Pros: simple, IDE-visible, standard. Cons: triggers `app.py` module body on `import
  mko_get_mediascope_data` — after Block A this only imports `get_app_service` (no
  bootstrap), but still loads Rich, Typer, etc.

**Audit recommendation**: "PEP 562 `__getattr__` alternative is also valid" +
"re-export must be lazy to avoid triggering import side-effects" — leans toward
Option A.

**Researcher recommendation**: Confirm whether post-Block-A direct imports are safe
enough to justify the simpler Option B. The audit's recommendation favors Option A
regardless, so this may be a no-op research call.

### RQ-3: `yaml_to_dict` error handling (Block A, ENT-003)

**The decision**: How should `yaml_to_dict` signal a missing file?

- **Option A (re-raise `FileNotFoundError`)** — preserve original exception, minimal
  change. Cons: caller sees `FileNotFoundError` which may be too generic for config
  issues.
- **Option B (wrap in domain error)** — raise a `ConfigLoadError` (new class in
  `core/errors.py`). Pros: semantically meaningful. Cons: requires new exception class,
  and existing callers may catch `FileNotFoundError` (services.py:62 catches `Exception`).
- **Option C (return `{}`)** — the docstring already says "or an empty dict if the file
  does not exist" (incorrectly — it actually returns `None`). Returning `{}` would
  avoid the TypeError but masks the error. The audit explicitly rejects this: "raise
  instead of return None."

**Audit recommendation**: "raise instead of return None" — Option A or B.

**Researcher recommendation**: Not needed — the audit is sufficiently specific. Option A
(re-raise) is the simplest correct approach. No new exception class needed.

### RQ-4: `print("=", end="", flush=True)` replacement (Block D, ENT-005)

**The decision**: The reports.py:265 `print("=", end="", flush=True)` is a progress
indicator. `logger.info("=")` changes output semantics (adds timestamp/level prefix,
forces newline).

- **Option A (`logger.info("=")`)** — follows audit recommendation literally.
- **Option B (`logger.debug(f"Task {task.name} submitted")`) — richer message.
  Pros: more informative. Cons: deviates from "replace print with logger.info" directive.
- **Option C (`rich.progress`)** — use Rich progress bar per `doc-maintenance-rules.md:103`.
  Pros: proper progress UI. Cons: architectural scope creep beyond ENT-005.

**Audit recommendation**: "replace all print() calls in core/ with logger.info()" —
Option A.

**Researcher recommendation**: Not needed. Option A is the specified approach.

### Summary: Researcher assignment

| RQ | Block | Finding | Researcher needed? |
|----|-------|---------|-------------------|
| RQ-1 | A | ENT-002 | **Recommended** — caching mechanism choice affects Block B |
| RQ-2 | C | ENT-004 | Optional — audit already favors PEP 562 |
| RQ-3 | A | ENT-003 | No — audit is specific |
| RQ-4 | D | ENT-005 | No — audit is specific |

> **If no Researcher is assigned**: Proceed with Planner-recommended options:
> A (module-level variable) for RQ-1, PEP 562 `__getattr__` for RQ-2, re-raise
> `FileNotFoundError` for RQ-3, `logger.info("=")` for RQ-4. All findings are
> detailed enough to go directly to Implementor after this plan.

---

## 6. Test Adaptation Requirements

Two test files have coupling to the module-scope singleton and Russian strings:

### `tests/test_services.py` (coupled to ENT-002)

- **Line 6**: `from mko_get_mediascope_data.core.services import app_service`
  → must become `from mko_get_mediascope_data.core.services import get_app_service`
- **Line 53**: `app_service.run_report(test_yaml)` → must become
  `get_app_service().run_report(test_yaml)`

This adaptation is part of Block A (Step 1). The test must be updated atomically with
the `get_app_service()` introduction — if the Implementor changes `services.py` but
forgets `test_services.py`, the test will fail with `ImportError`.

### `tests/test_app.py` (coupled to ENT-007)

- **Line 9**: `assert "Инициализировать" in result.stdout`
  → must become `assert "Initialize" in result.stdout` (or the chosen English translation)

This adaptation is part of Block E. The test verifies CLI help output — after translating
the `init` command docstring, the Russian word disappears from `--help` output.

---

## 7. Verification Matrix

| Finding | Block | Verification step | When to run |
|---------|-------|------------------|-------------|
| ENT-001 | A | `python -c "import app; ..."` — stdout NOT reconfigured | After Block A |
| ENT-002 | A | `get_app_service()` returns cached singleton; import doesn't bootstrap | After Block A |
| ENT-003 | A | `yaml_to_dict("/nonexistent")` raises `FileNotFoundError` | After Block A |
| ENT-004 | C | `from mko_get_medioscope_data import run_report` succeeds | After Block C |
| ENT-005 | D | AST scan: `print(` not found in `core/` | After Block D |
| ENT-006 | B | `reload_config()` invalidates cache, re-applies logging | After Block B |
| ENT-007 | E | `grep` for Cyrillic in `src/` — zero matches | After Block E |

### Regression check (after all blocks)

1. `uv run pytest` — all existing tests pass (with adapted imports/assertions).
2. `uv run ruff check src/` — no lint errors.
3. `python -c "import mko_get_medioscope_data"` — no side effects.
4. `python -c "from mko_get_medioscope_data import run_report"` — works.
5. `python -m mko_get_medioscope_data init --help` — English output, exit 0.
6. `python -m mko_get_medioscope_data --help` — English output, exit 0.

---

## 8. Block-to-Task Mapping

The following table maps blocks to implementation tasks for task-tracking purposes.
Each block produces one or more atomic tasks that can be tracked independently.

| Block | Task ID | Task title | Primary file(s) | Required agents |
|-------|---------|------------|-----------------|-----------------|
| A-1 | ENT-002 | Defer singleton behind `get_app_service()` | `core/services.py`, `app.py` | Implementor |
| A-2 | ENT-002-test | Adapt test_services.py for get_app_service() | `tests/test_services.py` | Implementor |
| A-3 | ENT-003 | `yaml_to_dict` raises on missing file + None guard | `core/utils.py`, `core/services.py` | Implementor |
| A-4 | ENT-001 | Move stdout.reconfigure into run() command body | `app.py` | Implementor |
| B | ENT-006 | Add reload_config() to AppService | `core/services.py` | Implementor |
| C | ENT-004 | Add PEP 562 __getattr__ to __init__.py | `__init__.py` | Implementor |
| D | ENT-005 | Replace print() → logger.info() in core/ | `core/services.py`, `core/reports.py` | Implementor |
| E | ENT-007 | Translate Russian → English across all source | Multiple | Implementor |
| E-test | ENT-007 | Adapt test_app.py Russian assertion | `tests/test_app.py` | Implementor |

---

*End of plan — 218 lines*
