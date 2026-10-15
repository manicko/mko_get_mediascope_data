---
name: Agent Guidelines
description: Mandatory rules and context for the project
alwaysApply: true
---

# Agent Guidelines

**`mko_get_mediascope_data`** — a CLI tool and Python library for loading data from the Mediascope API (TV / TV_REG). Sellers configure report definitions in YAML; the tool fetches data asynchronously, processes it with pandas, and exports CSV files. Usable as a CLI (`mko-get-mediascope`) or imported as a library in Jupyter / Airflow.

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

- **Stack:** Python 3.12+ (3.14 in classifiers) · `uv` · Typer CLI · Rich console · Pydantic v2 (models/validation/DTO) · pandas (data processing) · `mediascope-api-lib` (API client) · PyYAML (config) · asyncio (concurrent report tasks).
- **No database, no Docker, no web framework.** This is a standalone async data-fetcher.
- **Two entry points:** CLI (`app.py`) — Typer app with `init`, `run`, `list_reports` commands; Library — public functions `run_report`, `run_report_async`, `initialize_settings`.
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
│   ├── utils.py             # Helpers: date/period logic, YAML/CSV I/O
│   ├── errors.py            # NoReportFoundError, WrongDataSettings
│   └── logger.py            # (placeholder)
└── settings/                # Bundled default templates (app_config.yaml, log_config.yaml, reports/, etc.)
```

### Key patterns

- **Strategy pattern** (`core/reports.py`): `ReportFactory` selects a `TaskGenerationStrategy` and `DataPreparationStrategy` based on `MediaType`/`ReportSubtype`. New report types add a strategy subclass.
- **Async pipeline** (`core/network.py`): `NetworkClient.call` wraps every API call with retries, exponential backoff + jitter, and concurrency limiting via `asyncio.Semaphore`. Transient errors (timeouts, connection errors) are retried; client errors (4xx) raise immediately.
- **Orchestration** (`core/services.py`): `AppService.run_report` loads YAML settings, validates via Pydantic, builds `Data` settings from defaults, creates a `TVMediaReport`, and drives `create_report()`.

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

Test fixtures live in `tests/conftest.py`: `mock_network_client`, `dummy_df`, `tv_yaml_content`, `tmp_report_dir`.

## Coding Standards

1. **English Only** — All comments, logs, docstrings, error messages, and documentation must be in clear English.
2. **Production Code is King** — If tests conflict with architecture or business logic, fix or remove the tests. Never distort production code for tests.
3. **Strict Separation of Concerns** — Enforce clear boundaries between UI (CLI), business logic (services, reports, tasks), and data layers (models, network, utils).
4. **Single Responsibility** — Modules, services, and functions must be small and focused on one thing.
5. **Avoid Overengineering** — Prefer simple, obvious, and maintainable solutions over complex abstractions.
6. **Composition Over Inheritance** — Prefer composition; the Strategy pattern in `reports.py` exemplifies this.
7. **Follow Existing Patterns** — Respect and follow established patterns and conventions in the codebase.
8. **Meaningful & Consistent Naming** — Use clear, concise, and consistent names for files, functions, and variables.
9. **Type Safety Everywhere** — Use strict type hints on all functions. Pydantic v2 for all models and validation. Avoid `Any` where possible.
10. **Enums for All Constants** — All fixed values use `Enum` (in `core/models.py`).
11. **Pydantic v2** — All config/settings/models use Pydantic v2 (`BaseModel`, `Field`, `field_validator`, `model_validator`). Django ORM is not used.
12. **No `print()` Statements** — Use `logging.getLogger(__name__)` in `core/` modules. CLI user-facing output uses `rich.console.Console.print` in `app.py`.
13. **Small Modules and Functions** — Short, focused files and functions are easier to maintain.
14. **Keep Documentation Updated** — Architecture decisions and setup instructions must stay current.

## Detailed Instructions

For specific guidelines, see:
- [Doc maintenance rules](docs/00-overview/doc-maintenance-rules.md)
