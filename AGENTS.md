---
name: Agent Guidelines
description: Mandatory rules and context for the project
alwaysApply: true
---

# Agent Guidelines

**`mko_get_mediascope_data`** — a CLI tool and Python library for loading data from the Mediascope API (TV / TV_REG). Sellers configure report definitions in JSON5; the tool fetches data asynchronously, processes it with pandas, and exports CSV files. Usable as a CLI (`mko-get-mediascope`) or imported as a library in Jupyter / Airflow.

## Quick Reference

- **Package Manager:** `uv` (Python)
- **CLI entry point:** `uv run -m mko_get_mediascope_data`
- **Lint:** `uv run ruff check <path>` · **Auto-fix:** `uv run ruff check --fix <path>` · **pre-commit:** `pre-commit run --all-files` (`.pre-commit-config.yaml`)
- **Format:** `uv run ruff format <path>`
- **Typecheck:** `uv run mypy <path>` · mypy checks both `src` and `tests` (configured in `pyproject.toml`)
- **Test:** `uv run pytest` (coverage reported by default via `--cov`) · **`slow` marker:** `uv run pytest -m slow` (run only slow tests); `uv run pytest -m "not slow"` (skip slow tests)
- **Add dep:** `uv add <package>` · **Add dev dep:** `uv add --dev <package>`

On Windows, `Makefile.ps1` wraps the same toolchains:
`.\Makefile.ps1 test` · `.\Makefile.ps1 lint` · `.\Makefile.ps1 typecheck` · `.\Makefile.ps1 format` · `.\Makefile.ps1 all` (lint + test).

**CI:** every push and PR runs `uv run ruff check src tests` then `uv run pytest` (`.github/workflows/ci.yml`).

## Core Context

- **Stack:** Python 3.12+ (3.14 in classifiers) · `uv` · Typer CLI · Rich console · Pydantic v2 (models/validation/DTO) · pandas (data processing) · `mediascope-api-lib` (API client) · json5 (config) · asyncio (concurrent report tasks).
- **No database, no Docker, no web framework.** This is a standalone async data-fetcher.
- **Two entry points:** CLI (`app.py`) — Typer app with `init`, `run`, `list-reports` commands; Library — public functions `run_report`, `run_report_async`, `initialize_settings`.
- **Fixed values:** use `Enum` — the project uses `MediaType`, `ReportType`, `ReportSubtype` in `core/models.py`.
- **Config:** user settings live in `~/.config/mko_get_mediascope_data/settings/` (copied from package templates via `init` command). Settings are Pydantic-validated.

## Architecture

```
src/mko_get_mediascope_data/
├── __init__.py              # package init — lazy public re-exports via PEP 562 __getattr__
├── __main__.py              # module entry: `python -m mko_get_mediascope_data`
├── app.py                   # CLI (Typer) + public API (run_report, etc.)
├── core/
│   ├── paths.py             # PathResolver + AppPaths — config dir resolution
│   ├── models.py            # Pydantic v2 models + Enums (MediaType, ReportType, ReportSubtype)
│   ├── init_service.py      # Copies template settings to user config dir
│   ├── services.py          # AppService orchestrator (lazy singleton via get_app_service())
│   ├── reports.py           # Report classes + Strategy pattern (task/data strategies)
│   ├── tasks.py             # TVTask — async task representation
│   ├── network.py           # NetworkClient — async retry with backoff + semaphore
│   ├── utils.py             # Helpers: date/period logic, JSON5/CSV I/O
│   └── errors.py            # NoReportFoundError, WrongDataSettings
└── settings/                # Bundled default templates (app_config.json5, log_config.json5, reports/, etc.)
```

### Key patterns

- **Strategy pattern** (`core/reports.py`): `ReportFactory` selects a `TaskGenerationStrategy` and `DataPreparationStrategy` based on `MediaType`/`ReportSubtype`. New report types add a strategy subclass.
- **Async pipeline** (`core/network.py`): `NetworkClient.call` wraps every API call with retries, exponential backoff + jitter, a per-call `asyncio.wait_for` timeout (`call_timeout`, default 300s), and concurrency limiting via `asyncio.Semaphore`. Transient errors (timeouts, connection errors, HTTP 429) are retried; other client errors (4xx) raise immediately; 502/503/504 also retry at the vendor transport layer (`urllib3.Retry`). The task-submission POST is sent once (`retryable=False`) to prevent duplicate task creation.
- **Orchestration** (`core/services.py`): `AppService.run_report` loads JSON5 settings, validates via Pydantic, builds `Data` settings from defaults, creates a `TVMediaReport`, and drives `create_report()`.

## Test Environment

Tests use `pytest` with `pytest-asyncio` (mode `auto`) and `pytest-mock`. No Docker or database is required.

```bash
uv run pytest                  # run all tests
uv run pytest tests/test_utils.py                   # single file
uv run pytest -k test_str_to_date                    # single test
uv run pytest --asyncio-mode=auto                  # explicit asyncio mode
```

Configuration is in `pyproject.toml`:
- `pythonpath = ["src"]`
- `testpaths = ["tests"]`
- `addopts`: `--import-mode=importlib -ra -v --strict-markers --asyncio-mode=auto --cov=src/mko_get_mediascope_data --cov-report=term-missing`
- `markers`: `slow` (use `-m slow` to run only slow tests; `-m "not slow"` to skip)

Test fixtures live in `tests/conftest.py`: `mock_network_client`, `dummy_df`,
`tv_json5_content`, `tmp_report_dir`. `tests/test_services.py` defines the
`local_app_service` fixture (hermetic `AppService` backed by `tmp_path`).

## Coding Standards

1. **English Only** — All comments, logs, docstrings, error messages, and documentation must be in clear English.
2. **Production Code is King** — If tests conflict with architecture or business logic, fix or remove the tests. Never distort production code for tests.
3. **Strict Separation of Concerns** — Enforce clear boundaries between UI (CLI), business logic (services, reports, tasks), and data layers (models, network, utils).
4. **Single Responsibility** — Modules, services, and functions must be small and focused on one thing.
5. **Avoid Overengineering** — Prefer simple, obvious, and maintainable solutions over complex abstractions.
6. **Composition Over Inheritance** — Prefer composition; the Strategy pattern in `reports.py` exemplifies this.
7. **Follow Existing Patterns** — Respect and follow established patterns and conventions in the codebase. Do not introduce new abstractions without strong justification.
8. **Meaningful & Consistent Naming** — Use clear, concise, and consistent names for files, functions, and variables.
9. **Type Safety Everywhere** — Use strict type hints on all functions. Pydantic v2 for all models and validation. Do not introduce `Any` or untyped equivalents.
10. **Enums for All Constants** — Fixed value sets and settings use `Enum` or `StrEnum`, declared separately in `core/models.py`, rather than raw string/dict/list vocabularies. Ordinary data collections remain collections.
11. **Pydantic v2** — All config/settings/models use Pydantic v2 (`BaseModel`, `Field`, `field_validator`, `model_validator`). Django ORM is not used.
12. **No `print()` Statements** — Use `logging.getLogger(__name__)` in `core/` modules. CLI user-facing output uses `rich.console.Console.print` in `app.py`.
13. **Small Modules and Functions** — Short, focused files and functions are easier to maintain.
14. **Keep Documentation Updated** — Architecture decisions and setup instructions must stay current.
15. **Internationalization Is Part of Done** — Account for internationalization in user-visible changes while retaining the project's English-only text requirement. Do not introduce a new translation framework without a requirement.
16. **Workspace Cleanliness** — Before creating a file, establish its place in the task/module architecture and its operational or maintenance purpose. If neither exists, write only to `.ai/tmp/`.

## Codex Roles and Workflows

- Preserve `.kilo/` and every nested file. Kilo Code and Codex must remain usable in parallel. Work on the Codex configuration under `.codex/` and `.agents/skills/`; do not redirect edits into the Kilo originals.
- Roles describe the executor in `.codex/agents/*.toml`. Skills describe procedures in `.agents/skills/*/SKILL.md`. Read the selected Skill and its required references before execution; do not replace a detailed workflow with a generic checklist.
- Delegate when the user or an applicable Skill explicitly calls for specialized agents. Follow the Skill's role sequence, gates, artifacts, and user decisions. For an ordinary task without such instructions, work directly.
- Maximum **2 active subagents across the whole task tree**, and **1 implementor at a time**. The parent counts toward neither limit. Nested coordinators must share this budget; execute sequentially if slots are unavailable. Never fire and forget: wait for every delegated result before finishing. Prefer resuming an existing agent after an interruption.
- The main agent owns routing, output checks, and final completeness. Send each executor a scoped task, source Skill path, relevant context, output path, constraints, and acceptance criteria. Preserve workflow prompts marked `original_prompt`; add context without weakening them.
- Use `tech-lead` for coordination; `analyst` for business specifications; `researcher` for evidence and technical options; `planner` for dependency-safe plans; `auditor` for assigned audit work; `validator` for independent verification; `implementor` for scoped changes; `test-engineer` for test analysis; `docs-specialist` for documentation. Audit phases and their executors are listed in [.codex/audit-phases.md](.codex/audit-phases.md).
- Role boundaries and migrated command/tool policies are in [.codex/agent-permissions.md](.codex/agent-permissions.md). Read the relevant policy when assigning or assuming a role. These are behavioral restrictions, not a separate sandbox; inherited host permissions and higher-priority instructions still apply. Do not use shell commands or another tool to bypass a role's edit restriction.
- Do not invent missing tools, roles, templates, or decisions. Report unavailable delegation or blocked prerequisites and preserve partial results. A serial fallback is acceptable only when independence is not required; label it and never claim independent validation.
- Honor existing user authorization. Ask only for unresolved required input or a workflow's actual pending approval; do not ask again for information or approval already supplied. If a gate blocks progress, identify the exact Skill and gate.
- Keep application code, dependencies, build/deployment settings, and Kilo files unchanged during AI-configuration migration or maintenance. Broader changes require a separate applicable user task.

## Toolchain Details

- Windows 11: use PowerShell and `uv`; `make` requires an additional GNU Make/WSL installation. `.\Makefile.ps1` defaults to `all` (lint, then tests); it also accepts `test`, `lint`, `typecheck`, and `format`.
- Lint: `uv run ruff check src tests`; types: `uv run mypy src tests`; tests: `uv run pytest`. CI runs lint and tests on every push/PR. Pytest enforces `--cov-fail-under=70` and includes slow tests by default.
- `ruff check --fix` is the fix/import-sorting command (subject to the actual enabled rules); `ruff format` changes formatting only. `.\Makefile.ps1 format` currently calls `ruff check --fix src tests`. Do not auto-fix application code during configuration-only work.
- Add dependencies through `uv add <package>` or `uv add --dev <package>` only within an authorized dependency-change task. Do not install unrelated Docker, database, or frontend tooling for this package.

## Detailed Instructions

For specific guidelines, see:
- [Doc maintenance rules](docs/00-overview/doc-maintenance-rules.md)
- [Codex migration, source mapping, invocation examples, and limitations](docs/codex-migration.md)
