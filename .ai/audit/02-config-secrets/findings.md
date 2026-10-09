---
phase: "02"
phase_name: "config-secrets"
id_prefix: "CFG"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/02-audit-config-secrets.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Configuration & Secrets Management

## Summary

10 findings (3 HIGH, 4 MEDIUM, 3 LOW). The highest is HIGH: every shipped report
template except one fails Pydantic validation, the `tv_reg` defaults template is
malformed YAML, and the credential template is copied to a filename the code never
reads. All three entry surfaces share a single eagerly-built `AppService` singleton
that crashes at import if `init` hasn't run, because `yaml_to_dict` returns `None`
instead of raising.

## Scope

**Examined:** `core/paths.py`, `core/init_service.py`, `core/models.py`,
`core/services.py`, `core/network.py`, `core/utils.py`, `core/tasks.py`,
`core/reports.py`, `app.py`, `__main__.py`, all `settings/` templates, and the
vendored `mediascope_api` library (`core/net.py`, `core/utils.py`, `core/errors.py`).
**Tools:** `uv run python` (Pydantic/YAML validation, init simulation), `rg`
(field consumption).
**Not verified:** live API calls (no credentials); log output during a real run
(error-handler source reviewed only).

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| CFG-001 | HIGH | `settings/reports/nat_tv_brands_last.yaml::period_type` | Templates use stale field names and enum values rejected by Pydantic |
| CFG-002 | HIGH | `settings/defaults/tv_reg/crosstab.yaml::1` | Malformed YAML (unbalanced braces) — TV_REG reports always crash |
| CFG-003 | HIGH | `core/paths.py::AppPaths.connection_config` | Credential template name never matches the path the code reads |
| CFG-004 | MEDIUM | `core/services.py::AppService.get_connection` | No credential guard — missing file / placeholders reach API silently |
| CFG-005 | MEDIUM | `core/utils.py::yaml_to_dict` | Returns `None` on error → cascading `TypeError` at import and report time |
| CFG-006 | MEDIUM | `core/services.py::AppService.get_default_data_settings` | Missing defaults for `simple` and `tv_reg/timeband` — valid combos crash |
| CFG-007 | MEDIUM | `core/services.py::AppService.merge_data_settings` | 3 ReportSubtype values have no matching defaults key — silent skip |
| CFG-008 | LOW | `core/init_service.py::_copy_templates` | Non-atomic template copy — interrupted `init` leaves partial files |
| CFG-009 | LOW | `pyproject.toml::project.dependencies` | `pydantic-settings` declared but never imported — no env controls |
| CFG-010 | LOW | `core/services.py::AppService.run_report` | `print()` in core modules — bypasses logging config |

<!-- Findings body exceeds 100 lines; see Appendices for full evidence. -->

#### CFG-001 — [HIGH] Templates use stale field names / enum values rejected by Pydantic

**Checked:** ran · **Verify:** new test needed: `test_bundled_templates_validate`

30 of 32 bundled report definitions fail `ReportSettings(**rep)`. Two drifts:
(a) `last_time` uses `period_type`/`period_num` but `LastTimeModel` (models.py:61)
requires `time_unit`/`unit_count` with `extra="forbid"`; (b) 28 definitions use
`report_type: TV_CROSSTAB`/`TV_DICT_CROSSTAB` but the enum (models.py:44) only
accepts `crosstab`/`timeband`/`simple`. Templates with `TV_CROSSTAB` also omit
required `media` and `relative_path`, using unconsumed `folder`/`category_name`.

**Repro:** `ReportSettings(report_type='crosstab', media='TV', relative_path='x', period={'date_filter':['a','b'],'last_time':{'period_type':'m','period_num':'1','include_current':False}})` → `ValidationError: unit_count missing, period_type forbidden, period_num forbidden`

**Fix:** Rename `period_type→time_unit`, `period_num→unit_count`; replace
`TV_CROSSTAB→crosstab`, `TV_DICT_CROSSTAB→crosstab`; add `media`/`relative_path`.
**Related:** CFG-007

#### CFG-002 — [HIGH] Malformed YAML in `tv_reg/crosstab.yaml`

**Checked:** ran · **Verify:** new test needed: `test_tv_reg_defaults_parse`

13 `{` vs 12 `}` — missing `}` to close `DATA_DEFAULTS` on line 31.
`yaml_to_dict` catches the `YAMLError`, logs it, returns `None`;
`DataDefaults(**None)` → `TypeError`. Every TV_REG crosstab report crashes.

**Repro:** `yaml.safe_load(open('…/tv_reg/crosstab.yaml'))` → `ScannerError: expected ',' or '}', but got '<stream end>' at line 162, column 2`

**Fix:** Add missing `}` after line 30. **Related:** CFG-005

#### CFG-003 — [HIGH] Credential file name mismatch

**Checked:** ran · **Verify:** new test needed: `test_init_produces_readable_credential_file`

Template `settings/connections/mediascope_RENAME_.json` is copied by `init` to
the user dir, but `AppPaths.connection_config` (paths.py:59) resolves
`connections/mediascope.json`. After `init`, the expected file does not exist.
No guard tells the user to rename it.

**Repro (simulated init):** `init_project()` → file at
`…/connections/mediascope_RENAME_.json` (exists); code reads
`…/connections/mediascope.json` (missing → `FileNotFoundError`).

**Fix:** Rename template to `mediascope.json`, or update `connection_config`
to resolve the `_RENAME_` path; add existence guard in `get_connection`.

#### CFG-004 — [MEDIUM] No credential guard before API connection

**Checked:** ran · **Verify:** new test needed: `test_get_connection_raises_on_missing_credential`

`get_connection` (services.py:106) passes `settings_filename` to `MediaVortexTask`
without checking existence or placeholder values. `FileNotFoundError` is caught by
`NetworkClient.call` (network.py:88) and logged with the file path, but the error
is library-level, not actionable from project code. Placeholder values
(`"you username"`, `"00000000-…"`) pass through undetected.

**Fix:** Add pre-check in `get_connection` — verify file exists, raise
project-specific error naming the expected path.

#### CFG-005 — [MEDIUM] `yaml_to_dict` returns `None` on error

**Checked:** ran · **Verify:** existing `test_utils.py` only tests happy paths; new test needed

`yaml_to_dict` (utils.py:343) catches `YAMLError`/`FileNotFoundError`, logs,
falls through to **implicit `return None`**. Callers do
`AppSettings(**utils.yaml_to_dict(...))` → `TypeError`. Because `app_service`
is a module-level singleton (services.py:163), **importing the package before
`init` crashes**. Only `YAMLError` and `FileNotFoundError` are caught;
`UnicodeDecodeError`, `PermissionError` are not.

**Repro:** `yaml_to_dict('/nonexistent')` → `None`; `AppSettings(**None)` →
`TypeError: argument after ** must be a mapping, not NoneType`

**Fix:** Return `{}` on error (or raise typed error); broaden `except` to `OSError`.

#### CFG-006 — [MEDIUM] Missing defaults templates for valid enum combos

**Checked:** ran · **Verify:** new test needed: `test_get_default_data_settings_missing_file`

`get_default_data_settings` (services.py:66) builds `defaults/{media}/{type}.yaml`.
Missing: `TV/simple`, `TV_REG/timeband`, `TV_REG/simple`. Missing file →
`yaml_to_dict` returns `None` → `DataDefaults(**None)` → `TypeError`.

**Fix:** Ship the 3 missing templates or raise a specific error.

#### CFG-007 — [MEDIUM] 3 ReportSubtype values have no matching defaults key

**Checked:** ran · **Verify:** new test needed: `test_data_defaults_covers_all_subtypes`

`merge_data_settings` (services.py:83) does `hasattr(defaults, subtype.value)`.
`NAT_TV_CHANNELS_ATV`, `NAT_TV_CHANNELS_BA`, `NAT_TV_CHANNELS_SOC_DEM`
have no key in any defaults template → override skipped → `statistics` stays
`None` → `Data(statistics=None)` fails (`dict|list` type doesn't allow `None`).

**Fix:** Add subtype blocks to defaults, or guard `Data` against `None`.

#### CFG-008 — [LOW] Non-atomic template copy

**Checked:** static · **Verify:** not checked (theoretical)

`_copy_templates` (init_service.py:27) uses `shutil.copy2` per file.
Interrupted `init` leaves a partial config tree. `force=False` preserves
user edits (correct), but there is no temp-write + atomic-rename.

**Fix:** Write to temp file, `os.replace` on success.

#### CFG-009 — [LOW] `pydantic-settings` declared but unused

**Checked:** static · **Verify:** not checked

`pyproject.toml:21` lists `pydantic-settings>=2.12.0`; `rg` finds zero imports in
`src/`. No env-overridable controls exist for concurrency, retry count, export
path, or credential path.

**Fix:** Implement `BaseSettings` for env overrides, or remove the dependency.

#### CFG-010 — [LOW] `print()` in core modules

**Checked:** ran (static) · **Verify:** existing tests don't assert output; static check only

AGENTS.md: "No `print()` Statements — use `logging` in `core/`". Violations:
services.py:117, services.py:124, services.py:154, reports.py:265.

**Fix:** Replace with `logger.info()`.

## Fix Order

- **Dependencies:** CFG-002 must be fixed before CFG-006 can be validated for TV_REG.
  CFG-001 must be resolved before CFG-007 can be tested end-to-end.
- **Merge candidates:** CFG-005 and CFG-006 share the `None`-return root cause.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** CFG-002/006 templates are read by phase 03 (async
  pipeline); changes must not alter Strategy dispatch.
- **Rollout risk:** CFG-003 rename may invalidate users who already renamed
  `mediascope_RENAME_.json` → `mediascope.json` manually. `test_app.py::test_cli_init`
  asserts `--help` only and stays green.

## Appendices

### Appendix A — validation results for all 32 bundled report definitions

```text
gap_resurs/nat_tv_brands.yaml         [0] FAIL (7)  sovcombank/nat_tv_by_spots.yaml    [0] FAIL (6)
gap_resurs/nat_tv_brands.yaml         [1] FAIL (7)  sovcombank/nat_tv_by_spots.yaml    [1] FAIL (6)
gap_resurs/reg_tv_by_spots.yaml       [0] FAIL (7)  sovcombank/nat_tv_by_spots.yaml    [2] FAIL (7)
gap_resurs/reg_tv_by_spots.yaml       [1] FAIL (7)  sovcombank/nat_tv_finance_q.yaml   [0] FAIL (6)
gap_resurs/reg_tv_by_spots.yaml       [2] FAIL (7)  sovcombank/reg_tv_by_spots.yaml    [0] FAIL (6)
last_category/nat_tv_brands_last.yaml [0] FAIL (6)  sovcombank/tv_msk_new_creatives.yaml       [0] FAIL (6)
last_category/nat_tv_brands_last.yaml [1] FAIL (7)  sovcombank/tv_msk_new_creatives_short.yaml [0] FAIL (6)
last_category/nat_tv_brands_snack.yaml [0] FAIL (6)
last_category/nat_tv_brands_snack.yaml [1] FAIL (7)
last_category/nat_tv_brands_weekly_reach.yaml [0] FAIL (6)
nat_tv_brands_last.yaml [0] FAIL (3)  test.yaml [0] FAIL (6)
nat_tv_brands_tefal.yaml [0] PASS
nat_tv_brands_tefal.yaml [1] PASS
nat_tv_tvreport.yaml [0..6] FAIL (3 each)

Error breakdown:
  7 errors = TV_CROSSTAB + missing media + missing relative_path + 3 last_time errors
  6 errors = TV_CROSSTAB + missing media + missing relative_path + 2 last_time errors
  3 errors = valid report_type + 3 last_time errors (period_type/period_num/forbidden + unit_count missing)

TOTAL: 2 PASS, 30 FAIL
```

### Appendix B — defaults template existence by MediaType × ReportType

```text
                crosstab     timeband     simple
  TV:           exists ✓     exists ✓     MISSING ✗
  TV_REG:       malformed ✗  MISSING ✗    MISSING ✗
```

### Appendix C — DataDefaults attribute coverage for ReportSubtype

```text
  DYNAMICS_BY_SPOTS           hasattr=True   (tv/crosstab.yaml, tv_reg/crosstab.yaml)
  DYNAMICS_BY_SPOTS_DICT      hasattr=True
  TOP_NAT_TV_ADVERTISERS      hasattr=True
  TOP_NAT_TV_PROGRAMS         hasattr=True
  NAT_TV_CHANNELS_ATV         hasattr=False  ← CFG-007
  NAT_TV_CHANNELS_BA          hasattr=False  ← CFG-007
  NAT_TV_CHANNELS_SOC_DEM     hasattr=False  ← CFG-007
```

### Appendix D — credential template vs. expected path

```text
Package template:  settings/connections/mediascope_RENAME_.json
  keys: username, passw, client_id, client_secret, auth_server, root_url
  placeholders: "you username", "you password", "00000000-…"
Code expects:    settings/connections/mediascope.json  (AppPaths.connection_config, paths.py:59)
After init:      mediascope_RENAME_.json exists; mediascope.json MISSING
```
