
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

# 3. List available reports and run a report
uv run -m mko_get_mediascope_data list-reports
uv run -m mko_get_mediascope_data run ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5
```

### For developers (editable installation)

```bash
git clone https://github.com/manicko/mko_get_mediascope_data.git
cd mko_get_mediascope_data

uv pip install -e .
uv run -m mko_get_mediascope_data init
uv run -m mko_get_mediascope_data run ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5
```

## 📁 Configuration

### 1. Initialize settings
```bash
uv run -m mko_get_mediascope_data init
```
This command creates the full configuration structure in the platform-specific
user configuration directory selected by `platformdirs` (there is **no single
universal path**):

| Platform | Settings directory |
|---|---|
| Linux | `~/.config/mko_get_mediascope_data/settings/` |
| macOS | `~/Library/Application Support/mko_get_mediascope_data/settings/` |
| Windows | `%APPDATA%\mko_get_mediascope_data\settings\` |

The tree below is the Linux example:

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

### 4. Migrating a hand-authored report from YAML to JSON5
Renaming a `.yaml` file to `.json5` is only sufficient when its content is
already JSON5-compatible. Ordinary YAML block sequences and mappings (indented
`-` list items and `key: value` lines) are **not** valid JSON5, so such files
must be converted by hand. Only hand-authored reports need conversion; bundled
templates are shipped in JSON5. To refresh the bundled templates, run
`uv run -m mko_get_mediascope_data init --force`; it re-copies the package
templates and **overwrites files of the same name**, so back up any report you
edited yourself before running it. There is no automatic migration feature.

For example, this YAML report:

```yaml
- report_type: crosstab
  report_subtype: DYNAMICS_BY_SPOTS
  media: TV
  relative_path: finance/raw_data
  period:
    date_filter:
      - "2021-01-01"
      - "2024-11-30"
    time_unit: m
```

is written like this in JSON5:

```json5
[
  {
    "report_type": "crosstab",
    "report_subtype": "DYNAMICS_BY_SPOTS",
    "media": "TV",
    "relative_path": "finance/raw_data",
    "period": {
      "date_filter": [
        "2021-01-01",
        "2024-11-30",
      ],
      "time_unit": "m",
    },
  },
]
```

## 📋 CLI Commands

```bash
uv run -m mko_get_mediascope_data --help
```

**Available commands:**
- `init`          — Initialize user settings and templates
- `run`           — Run a report from a JSON5 file
- `list-reports`  — List all available report files

**Examples:**
```bash
# Run single report (default verbosity with progress bars)
uv run -m mko_get_mediascope_data run ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_tvreport.json5

# Run with verbose logging and progress bars
uv run -m mko_get_mediascope_data run --verbosity VERBOSE ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5

# Run quietly (no progress bars, warnings only)
uv run -m mko_get_mediascope_data run --verbosity QUIET ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5

# Run to a custom export directory for this run only (overrides configured export_folder)
uv run -m mko_get_mediascope_data run --export-folder ./exports ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5

# Run to an absolute or home directory path
uv run -m mko_get_mediascope_data run --export-folder ~/data/reports ~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5
```

## 🔌 Public API (scripts, Airflow, notebooks)

Choose the entry point that matches the host environment:

- **Synchronous scripts and Airflow tasks** — `run_report(...)` blocks until the
  report finishes. It must **not** be called from within a running event loop.
- **Jupyter / IPython notebooks and other active-loop hosts with default
  user settings** — `await run_report_async(...)` runs inside the existing event
  loop (native `await`; no `nest_asyncio`).

```python
from mko_get_mediascope_data import run_report, run_report_async, initialize_settings

# Initialize settings (once)
initialize_settings(force=True)

# Synchronous script or Airflow task
run_report(
    "~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5"
)
```

```python
# Jupyter / IPython notebook or any host with a running event loop
from mko_get_mediascope_data import run_report_async

await run_report_async(
    "~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5"
)
```

**Per-run export folder override (optional):** Both `run_report(report_path, progress=None, *, export_folder=None)` and `run_report_async(report_path, progress=None, *, export_folder=None)` accept an optional keyword-only `export_folder` (str | Path). When provided, it replaces the configured `export_folder` **for that run only** (no config/global mutation; precedence over configured value). Each report entry’s output is written to `<effective_base>/<relative_path>` (the base is replaced while `relative_path` is preserved). Relative override paths are resolved/absolutized anchored to the **current working directory (CWD)**; `~` is supported. Empty/blank strings are rejected with `ValueError`. `None` means use the configured export folder. The returned export paths reflect the actual destinations used for the run. 

```python
from mko_get_mediascope_data import run_report

# Export to a custom directory for this run only
run_report(
    "settings/reports/nat_tv_brands_last.json5",
    export_folder="./exports",
)
```

### Jupyter notebook example (delivered, CR-3 Alternative A — caveated)

A repo-shipped example notebook is provided at
[`examples/mediascope_notebook_workflow.ipynb`](examples/mediascope_notebook_workflow.ipynb)
for **local JupyterLab / classic Jupyter** (Python 3.12+). It demonstrates a
portable-workspace workflow using **internal dependency-injection (DI) workspace
bootstrap** (not a stable public notebook API):

- **Install from Git (selectable ref, pinned default):** the notebook shows a
  user-selectable `REPO_URL`/`GIT_REF` and defaults to the pinned delivery ref
  `0743c3d21ed5ac6291ab0a96dfab26db0664c05f` (the repo has no tags at delivery
  time; a branch/tag override can be used when appropriate).
- **Configurable folders (notebook cell variables):** `WORKSPACE_ROOT`,
  `SETTINGS_WORKSPACE` (settings/config workspace), `REPORTS_DIR` (reports source
  path), `EXPORT_DIR` (output/export), and `REPORT_PATH`.
- **Alt A execution path:** bootstrap/validate a workspace-local settings tree
  from bundled templates (non-destructive by default; excludes
  `connections/mediascope.json` placeholder credentials from blanket copy), set
  `app_config.export_folder` to the absolute export directory **before**
  `AppService` construction, construct
  `AppPaths(user_dir=WORKSPACE_ROOT, app_dir=core.paths.APP_DIR, app_name=APP_NAME)`,
  `PathResolver(WORKSPACE_ROOT)`, and `AppService(...)`, then run:
  ```python
  export_paths = await service.run_report(
      Path(REPORT_PATH), progress=NullProgressReporter()
  )
  ```
  Do **not** use the public `run_report_async` singleton wrapper for this Alt A
  workspace workflow. `run_report_async` / `run_report` remain supported for
  default user-settings hosts (scripts, Airflow, active-loop notebooks using the
  user config directory).
- **Credentials (CR-7):** external/user-managed at a configurable
  `CREDENTIALS_PATH` (portable default: user-config
  `connections/mediascope.json`); never embed or print secrets. Workspace
  credential copy is explicit opt-in only, never overwrites existing credentials,
  and never promotes the bundled placeholder.
- **Outputs:** clear export listing/count/sizes; `run_report` may skip entries
  (warned `NoReportFoundError`) — do not claim success from path existence alone.
- **Caveats (CR-8):** uses internal modules (`core.paths`, `core.services`,
  `core.progress`), not a stable public notebook API; `AppService.prepare_app()`
  applies global `logging.config.dictConfig()` and resolves log handler paths
  (restart the kernel to reset state in long-lived sessions); bypasses
  singleton/public wrappers (`init`/`list-reports` remain user-dir fixed; public
  `run_report*`/`initialize_settings` unchanged); requires a complete valid
  settings tree; `get_connection()` missing-file errors may mention `init`
  generically — the Alt A remedy is workspace bootstrap/explicit credential copy.
- **Real E2E evidence (CR-5):** real execution requires authorized local
  JupyterLab/classic plus valid external credentials/network. Current sanitized
  evidence is **BLOCKED** with no success claim:
  [`.ai/evidence/01-jupyter-notebook-e2e.md`](.ai/evidence/01-jupyter-notebook-e2e.md).
  Spec reference: [`.ai/problems/02_jupyter_notebook_launch_spec.md`](.ai/problems/02_jupyter_notebook_launch_spec.md).

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

