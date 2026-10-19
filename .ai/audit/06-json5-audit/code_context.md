# Code Context — JSON5 Settings Templates Migration

## Status Summary

**Overall state: Implementation complete in production code (committed). Tests updated (uncommitted). Documentation pending.**

### Git History (commits already applied)

```
6b44fa8 feat(config): add json5 dependency, remove PyYAML
b2ff6e4 refactor(config): replace yaml_to_dict with json5_to_dict
a596d86 chore(templates): migrate settings templates from YAML to JSON5
00e87f6 refactor(cli): update list-reports and run help to JSON5
52f1c64 refactor(tasks): convert error artifacts to JSON5, rename to_yaml to to_file
```

### Quality Gates

| Check | Result |
|---|---|
| `uv run ruff check src/ tests/` | PASSED — all checks passed |
| `uv run mypy src/ tests/` | PASSED — no issues found in 23 source files |
| `uv run pytest` | PASSED — 52 tests passed in 23s |

### What is Already Implemented (committed)

1. **Dependency & Packaging** (Block 1 / T-02):
   - `pyproject.toml`: `json5>=0.17.3` added; `PyYAML` removed from dependencies; `types-PyYAML` removed from dev deps
   - `MANIFEST.in`: `recursive-include mko_get_mediascope_data *.json *.json5`
   - `.pre-commit-config.yaml`: `json5` added to mypy `additional_dependencies`; `PyYAML` removed

2. **JSON5 Reader + Path Resolution** (Block 2 / T-01 + T-04 + T-09):
   - `core/utils.py`: `import json5`; `yaml_to_dict()` → `json5_to_dict()` using `json5.load()`
   - `core/paths.py`: `app_config.yaml` → `app_config.json5`; `log_config.yaml` → `log_config.json5`
   - `core/services.py`: All 4 call sites `utils.yaml_to_dict` → `utils.json5_to_dict`; `.with_suffix(".yaml")` → `.with_suffix(".json5")`; docstrings "YAML" → "JSON5"
   - `core/utils.py`: `list_files_in_directory` default `extensions` → `("json5","json")`; `dir_content_to_dict` default `suffix` → `"json5"`

3. **Template Conversion** (Block 3 / T-03):
   - 24 `.yaml` files → 24 `.json5` files (2 top-level + 6 defaults + 16 reports = 32 entries)
   - No `.yaml` files remain under `settings/`
   - `settings/connections/mediascope.json` stays strict JSON (not converted)

4. **CLI Surface** (Block 4 / T-05):
   - `app.py`: `help` text "YAML" → "JSON5"; `list_reports` `extensions=("yaml",)` → `("json5",)`; example path `.yaml` → `.json5`

5. **Error Artifacts** (Block 5 / T-06):
   - `core/tasks.py`: `to_yaml()` → `to_file()`; `yaml.safe_dump` → `json5.dumps`
   - `core/reports.py`: All 3 call sites `item.to_yaml` → `item.to_file`

### What is Pending (uncommitted)

1. **Comment/docstring fixes**:
   - `core/errors.py`: "YAML" → "JSON5" in `WrongDataSettings` docstring
   - `core/models.py`: "YAML ``Null`` values" → "JSON5 ``null`` values" in `coerce_none_to_empty` docstring

2. **Test updates** (Block 6 / T-07):
   - `tests/conftest.py`: fixture `tv_yaml_content` → `tv_json5_content`
   - `tests/test_models.py`: `import yaml` → `import json5`; `rglob("*.yaml")` → `rglob("*.json5")`; `yaml.safe_load` → `json5.loads`; docstring YAML → JSON5
   - `tests/test_services.py`: `import yaml` → `import json5`; `_LOG_CONFIG_YAML` → `_LOG_CONFIG_JSON5` (with format conversion from YAML to JSON5 syntax); `_APP_CONFIG_YAML` → `_APP_CONFIG_JSON5`; `.yaml` → `.json5` file paths; `yaml.safe_load` → `json5.loads`
   - `tests/test_reports_lifecycle.py`: Same pattern of updates (import, fixture names, file extensions, log/config format conversions, glob patterns)
   - `uv.lock`: Updated for json5 dependency

### What Still Needs to be Done

1. **Commit pending test changes**: The uncommitted changes in test files need to be committed. All tests pass with these changes.

2. **Documentation updates** (Block 7 / T-08):
   - `README.md`: Lines 22, 33, 46, 47, 68, 74, 86 still reference `.yaml` / "YAML file(s)"
   - `AGENTS.md`: Lines 9, 28, 49, 51, 58 reference "YAML" / "PyYAML" / `.yaml`
   - `docs/00-overview/data-flow-architecture.md`: Lines 37-39, 48-49, 71, 184, 242, 222-227 reference YAML / `.yaml`
   - `docs/00-overview/settings-layers-architecture.md`: Multiple YAML references
   - `docs/00-overview/current-behavior.md`: Many YAML references
   - `docs/00-overview/system-architecture.md`: Multiple YAML references
   - `docs/00-overview/open-questions.md`: YAML references
   - `docs/00-overview/product-overview.md`: YAML references

3. **Audit file TODO markers** (Q-3/Q-4):
   - `.ai/audit/05-config-templates/_validate.py`: Uses `yaml.safe_load` + `glob("**/*.yaml")`
   - `.ai/audit/99-validation/*.md`: Multiple findings reference stale `yaml_to_dict`, `to_yaml`, `*.yaml`, `yaml.safe_load`, `PyYAML`
   - `.ai/audit/05-config-templates/findings.md`: References to YAML format

### Architecture & Dependencies

- **Stack**: Python 3.12+ (3.14 in classifiers) · uv · Typer CLI · Rich console · Pydantic v2 · pandas · mediascope-api-lib · json5 · asyncio
- **No database, no Docker, no web framework.** Standalone async data-fetcher.
- **Entry points**: CLI (`app.py` with `init`, `run`, `list-reports`); Library (`run_report`, `run_report_async`, `initialize_settings`)
- **Fixed values**: `MediaType`, `ReportType`, `ReportSubtype` enums in `core/models.py`
- **Config**: User settings in `~/.config/mko_get_mediascope_data/settings/` (copied via `init` command)

### Key Patterns Preserved

- **Strategy pattern** (`core/reports.py`): `ReportFactory` selects strategies based on `MediaType`/`ReportSubtype`
- **Async pipeline** (`core/network.py`): `NetworkClient.call` with retries, backoff, semaphore
- **Orchestration** (`core/services.py`): `AppService.run_report` loads JSON5 settings, validates via Pydantic

### Discrepancies Between Spec and Current State

| Spec Reference | Current State | Status |
|---|---|---|
| TPL-004 brace defect | Already repaired at HEAD | Resolved, no action needed |
| C-3: `test_tasks.py` missing | Already updated (to_yaml → to_file) | Resolved in commit `52f1c64` |
| C-4: `reports.py` call sites | Already updated `item.to_yaml` → `item.to_file` | Resolved in commit `52f1c64` |
| C-9: `ext://sys.stdout` unquoted | Test fixture `_LOG_CONFIG_JSON5` now uses JSON5 object syntax with quoted strings | Handled in uncommitted changes |
| PyYAML removal | Fully removed from pyproject.toml, pre-commit, and all src/ | Complete |
