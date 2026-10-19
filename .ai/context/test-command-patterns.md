---
id: test-command-patterns
domain: commands
tags:
  - testing
  - commands
related:
  - pyproject.toml
---

# Test Command Patterns (canonical reference)

Grounded in committed configuration (`pyproject.toml`, `Makefile.ps1`,
`AGENTS.md`). Use these shapes so every test run is reproducible
and matches CI.

| Concern | Correct (canonical) | Incorrect / fragile (anti-pattern) |
|---|---|---|
| Test isolation | `uv run pytest` (all tests) | Running individual files outside the `src` pythonpath — `pythonpath = ["src"]` is set in `pyproject.toml` |
| Slow tests | `uv run pytest -m slow` (run only slow) | `uv run pytest -m "not slow"` (skip slow) — both work, but `slow` is the canonical marker |
| Single file | `uv run pytest tests/test_utils.py` | `uv run python -m pytest` without `uv run` — may use wrong interpreter |
| Single test | `uv run pytest -k test_str_to_date` | `uv run pytest tests/test_utils.py::test_str_to_date` — works but overly verbose |
| Coverage | Automatic via `--cov=src/mko_get_mediascope_data` (set in `pyproject.toml`) | Manually passing `--cov` flags — redundant |

## Equivalent direct invocation

`.\Makefile.ps1 test` (Windows PowerShell) is equivalent to:

```bash
uv run pytest
```

## Skipping slow tests

By default, `uv run pytest` runs all tests including those marked `slow`.
To skip slow tests for fast feedback:

```bash
uv run pytest -m "not slow"
```

## Test configuration

`pyproject.toml` configures:
- `--asyncio-mode=auto` (pytest-asyncio)
- `--cov=src/mko_get_mediascope_data`
- `--cov-report=term-missing`
- `--cov-fail-under=70`
- `pythonpath = ["src"]`
- `testpaths = ["tests"]`
- `slow` marker (for long-running tests)

## Test fixtures

Source of truth: `tests/conftest.py` (`mock_network_client`, `dummy_df`,
`tv_json5_content`, `tmp_report_dir`). `tests/test_services.py` defines the
`local_app_service` fixture (hermetic `AppService` backed by `tmp_path`).
