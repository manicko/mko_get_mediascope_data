
# MEDIASCOPE DATA LOADER

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue)](https://www.python.org)
[![uv](https://img.shields.io/badge/uv-managed-4B0082)](https://docs.astral.sh/uv/)
[![CI](https://github.com/manicko/mko_get_mediascope_data/actions/workflows/ci.yml/badge.svg)](https://github.com/manicko/mko_get_mediascope_data/actions/workflows/ci.yml)

**Fast, reliable and asynchronous data loader for Mediascope API (TV / TV_REG).**

## 🚀 Quick Start

### Install from GitHub

```bash
# 1. Install the package
pip install git+https://github.com/manicko/mko_get_mediascope_data.git

# 2. Initialize user settings (creates everything in your home folder)
uv run -m mko_get_mediascope_data init

# 3. Run a report
uv run -m mko_get_mediascope_data run reports/nat_tv_brands_last.json5
```

### For developers (editable installation)

```bash
git clone https://github.com/manicko/mko_get_mediascope_data.git
cd mko_get_mediascope_data

uv pip install -e .
uv run -m mko_get_mediascope_data init
uv run -m mko_get_mediascope_data run reports/nat_tv_brands_last.json5
```

## 📁 Configuration

### 1. Initialize settings
```bash
uv run -m mko_get_mediascope_data init
```
This command creates the full configuration structure in your user directory:

```
~/.config/mko_get_mediascope_data/settings/
├── app_config.json5
├── log_config.json5
├── connections/mediascope.json
├── defaults/
└── reports/
```

### 2. Mediascope API credentials
Edit `settings/connections/mediascope.json` with your credentials (see official [Mediascope API documentation](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib)).

### 3. Report settings
Copy any file from `settings/reports/` as a template and adjust it to your needs.  
All parameters follow the standard **Mediascope API** format.

> **Migrating from YAML to JSON5:** Settings templates were migrated from YAML
> to JSON5. Re-run `uv run -m mko_get_mediascope_data init --force` to refresh
> your templates. Hand-authored `.yaml` report files must be renamed to `.json5`
> (the content format is otherwise unchanged). Error artifacts (`errors/*.json5`)
> and configuration files (`app_config`, `log_config`) are also now JSON5.

## 📋 CLI Commands

```bash
uv run -m mko_get_mediascope_data --help
```

**Available commands:**
- `init`          — Initialize user settings and templates
- `run`           — Run one or more reports from JSON5 file(s)
- `list-reports`  — List all available report files

**Examples:**
```bash
# Run single report
uv run -m mko_get_mediascope_data run reports/nat_tv_tvreport.json5
```

## 🔌 Public API (Jupyter, Airflow, scripts)

```python
from mko_get_mediascope_data import run_report, initialize_settings

# Initialize settings (once)
initialize_settings(force=True)

# Run report
run_report("settings/reports/nat_tv_brands_last.json5")
```

## 🛠️ Requirements
- Python 3.12+
- Recommended: [uv](https://docs.astral.sh/uv/) package manager

## 📖 Documentation

Project documentation (start here):

| Document | Read it if you want to know… |
|---|---|
| [Product Overview](docs/00-overview/product-overview.md) | what the tool is for, who uses it, and how it is expected to behave |
| [Current Behavior](docs/00-overview/current-behavior.md) | exactly what the CLI and library do today — inputs, outputs, errors (verified against the code) |
| [Data Flow](docs/00-overview/data-flow-architecture.md) | how settings, period, and API data move and are transformed |
| [System Architecture](docs/00-overview/system-architecture.md) | components, call direction, diagrams, and trade-offs |
| [Open Questions](docs/00-overview/open-questions.md) | open product questions, decisions, and known defects |
| [Settings Layers](docs/00-overview/settings-layers-architecture.md) | the DEFAULT/CLIENT settings merge in depth |
| [Async & Concurrency](docs/00-overview/async-concurrency-architecture.md) | the two-level concurrency model in depth |

The CLI command surface (`init`, `run`, `list-reports`) is verified against
the Typer app in [`src/mko_get_mediascope_data/app.py`](src/mko_get_mediascope_data/app.py).
The validation status of bundled report templates and full behavior details
are documented in
[Current Behavior](docs/00-overview/current-behavior.md).

External:
- Official Mediascope API: [mediascope-api-lib](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib)
- Doc maintenance rules: [docs/00-overview/doc-maintenance-rules.md](docs/00-overview/doc-maintenance-rules.md)

