
# MEDIASCOPE DATA LOADER

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue)](https://www.python.org)
[![uv](https://img.shields.io/badge/uv-managed-4B0082)](https://docs.astral.sh/uv/)

**Fast, reliable and asynchronous data loader for Mediascope API (TV / TV_REG).**

## 🚀 Quick Start

### Install from Test PyPI (recommended for users)

```bash
# 1. Install the package
uv pip install -i https://test.pypi.org/simple/ mko-get-mediascope-data

# 2. Initialize user settings (creates everything in your home folder)
uv run -m mko_get_mediascope_data init

# 3. Run a report
uv run -m mko_get_mediascope_data run settings/reports/nat_tv_brands_last.yaml
```

### For developers (editable installation)

```bash
git clone https://github.com/yourusername/mko_get_mediascope_data.git
cd mko_get_mediascope_data

uv pip install -e .
uv run -m mko_get_mediascope_data init
uv run -m mko_get_mediascope_data run settings/reports/nat_tv_brands_last.yaml
```

## 📁 Configuration

### 1. Initialize settings
```bash
uv run -m mko_get_mediascope_data init
```
This command creates the full configuration structure in your user directory:

```
~/.config/mko_get_mediascope_data/settings/
├── app_config.yaml
├── log_config.yaml
├── connections/mediascope.json
├── defaults/
└── reports/
```

### 2. Mediascope API credentials
Edit `settings/connections/mediascope.json` with your credentials (see official [Mediascope API documentation](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib)).

### 3. Report settings
Copy any file from `settings/reports/` as a template and adjust it to your needs.  
All parameters follow the standard **Mediascope API** format.

## 📋 CLI Commands

```bash
uv run -m mko_get_mediascope_data --help
```

**Available commands:**
- `init`          — Initialize user settings and templates
- `run`           — Run one or more reports from YAML file(s)
- `list_reports`  — List all available report files

**Examples:**
```bash
# Run single report
uv run -m mko_get_mediascope_data run settings/reports/nat_tv_brands_last.yaml

# Run with minimal output
uv run -m mko_get_mediascope_data run my_report.yaml --no-verbose
```

## 🔌 Public API (Jupyter, Airflow, scripts)

```python
from mko_get_mediascope_data import run_report, initialize_settings

# Initialize settings (once)
initialize_settings(force=True)

# Run report
run_report("settings/reports/nat_tv_brands_last.yaml")
```

## 🛠️ Requirements
- Python 3.12+
- Recommended: [uv](https://docs.astral.sh/uv/) package manager

## 📖 Documentation
- Official Mediascope API: [mediascope-api-lib](https://github.com/MEDIASCOPE-JSC/mediascope-api-lib)

