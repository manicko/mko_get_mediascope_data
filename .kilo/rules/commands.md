# Subagents
Max allowed parallel subagents = 2
Only 1 implementor agent is allowed at a time.
Do not launch agents in background.
If stop or break prefer resume old session not launching new agent.

# Project Commands

**Env:** Windows 11 · `uv` (Python) · no Docker, no database, no web framework.
This is a standalone async data-fetcher package.

## Quick start

**This environment is Windows 11** — `make` requires WSL or a GNU Make install.
Use `.\Makefile.ps1` in PowerShell 7+:

| Task | Command |
|---|---|
| Lint + test (default) | `.\Makefile.ps1` or `.\Makefile.ps1 all` |
| Run tests | `.\Makefile.ps1 test` |
| Lint | `.\Makefile.ps1 lint` |
| Typecheck | `.\Makefile.ps1 typecheck` |
| Auto-fix + format | `.\Makefile.ps1 format` |

## CLI entry point

```bash
uv run -m mko_get_mediascope_data    # run as module
mko-get-mediascope                   # installed script (after uv sync)
```

CLI commands: `init`, `run`, `list-reports`.

## Python toolchain (local, PowerShell)

| Task | Command |
|---|---|
| Lint | `uv run ruff check src tests` |
| Auto-fix (incl. import sorting, I001) | `uv run ruff check --fix src tests` |
| Format | `uv run ruff format src tests` |
| Typecheck | `uv run mypy src tests` |
| Add dep | `uv add <pkg>` / `uv add --dev <pkg>`
| Run tests | `uv run pytest` |
| Run slow tests only | `uv run pytest -m slow` |
| Skip slow tests | `uv run pytest -m "not slow"` |

> `ruff check --fix` handles import sorting (I001). `ruff format` only reformats (line wraps, quotes) and does **NOT** sort imports — `.\Makefile.ps1 format` runs `ruff check --fix src tests`, not `ruff format`.

## Tests

Tests run locally via `uv run pytest` — no Docker or database is required.
The project uses `pytest` with `pytest-asyncio` (mode `auto`) and `pytest-mock`.

`pyproject.toml` configures `--asyncio-mode=auto`, `--cov=src/mko_get_mediascope_data`,
`--cov-report=term-missing`, `--cov-fail-under=70`, and a `slow` marker.

| Task | Command | When |
|---|---|---|
| Run all tests | `uv run pytest` | Default dev iteration |
| Run slow tests only | `uv run pytest -m slow` | Long-running/integration tests |
| Skip slow tests | `uv run pytest -m "not slow"` | Fast feedback |
| Run a single test / file | `uv run pytest tests/test_utils.py` | Targeted debugging |
| Run a single test by name | `uv run pytest -k test_str_to_date` | Targeted debugging |
| Typecheck | `uv run mypy src tests` | Type errors in src and tests |

**Test fixtures** — source of truth: `tests/conftest.py` (`mock_network_client`, `dummy_df`,
`tv_json5_content`, `tmp_report_dir`). `tests/test_services.py` defines the
`local_app_service` fixture (hermetic `AppService` backed by `tmp_path`).

## CI

Every push and PR runs `uv run ruff check src tests` then `uv run pytest`.
