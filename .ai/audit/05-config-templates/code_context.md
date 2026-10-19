# Code Context — Phase 05: Config Template Validation

**Status:** Assessed against HEAD `3dfffe6` (commit lineage well past the plan's `d093d9c`/`b04d177` baseline).

## Executive Summary

The six findings (TPL-001–TPL-006) were anchored to a pre-migration YAML codebase (`d093d9c`/`b04d177`). Between that baseline and the current HEAD, a **YAML→JSON5 migration** was committed (`b2ff6e4`, `a596d86`, `00e87f6`, `52f1c64`), plus targeted remediation commits (`183aed6` schema evolution, `335a11e` missing-defaults creation, `4877882` model validators). All 52 tests pass. The findings map to current code as follows:

| Finding | Original Severity | Current Status | Code changes needed? |
|---|---|---|---|
| TPL-001 | CRITICAL | **RESOLVED** — all 32 template entries validate | No |
| TPL-002 | LOW | **STILL APPLICABLE** — `extra="allow"` intended; typo risk remains | No (doc-only) |
| TPL-003 | HIGH | **PARTIALLY RESOLVED** — `coerce_none_to_empty` covers `en` path; `ru` path (`en_to_ru(None)`) still crashes in code | Yes (2-line guard) |
| TPL-004 | HIGH | **RESOLVED** — JSON5 files all parse; brace defects eliminated by conversion | No |
| TPL-005 | MEDIUM | **PARTIALLY RESOLVED** — missing files created; no code-level existence check | Yes (existence → `WrongDataSettings`) |
| TPL-006 | LOW | **STILL APPLICABLE** — `kitId: 7` (undefined) persists in defaults + templates; comment drift | Template-only |

---

## 1. Current Architecture & File Formats

### YAML → JSON5 Migration (COMPLETE)

The plan file was pinned to `d093d9c`/`b04d177` where settings were YAML. At current HEAD:

| Aspect | Before (plan baseline) | After (current HEAD) |
|---|---|---|
| Reader function | `core/utils.py::yaml_to_dict` | `core/utils.py::json5_to_dict` (line 365) |
| Defaults suffix | `.with_suffix(".yaml")` | `.with_suffix(".json5")` |
| Report templates | 16 `.yaml` files | 16 `.json5` files |
| Defaults templates | 3 `.yaml` files | 6 `.json5` files |
| PyYAML dependency | present | **removed** from `pyproject.toml` |
| json5 dependency | absent | `json5>=0.17.3` (pyproject.toml:17) |
| `list_files_in_directory` ext | `("csv", "txt")` | `("json5", "json")` |
| Error artifacts | `to_yaml()` + `yaml.dump` | `to_file()` + `json5.dumps` (tasks.py:52, reports.py:3 calls) |
| `log_config` / `app_config` | `.yaml` | `.json5` (paths.py:50, 58) |

**Migration commits (in order):**
- `b2ff6e4` — replace `yaml_to_dict` with `json5_to_dict`
- `6b44fa8` — add json5 dep, remove PyYAML
- `a596d86` — migrate all 24 settings templates from YAML→JSON5
- `00e87f6` — update CLI (`list-reports`, help text)
- `52f1c64` — convert error artifacts to JSON5, rename `to_yaml`→`to_file`

**No `.yaml` files remain under `src/mko_get_mediascope_data/settings/`.** PyYAML is completely absent from `src/`, `tests/`, and `pyproject.toml`. The `05-config-templates/_validate.py` reproduction script (line 25, 34, 42, 63, 79, 115, 117) and the `findings.md` / `05-config-templates/findings.md` still reference YAML — these are stale.

### Schema Evolution (post-baseline)

Commit `183aed6` moved `time_unit` from `LastTimeModel` to `PeriodModel`. Current model definitions (`core/models.py`):

- **`LastTimeModel`** (models.py:61-64): `model_config = ConfigDict(extra="forbid")`; fields: `include_current: bool = False`, `unit_count: PositiveInt`. No `time_unit`.
- **`PeriodModel`** (models.py:67-84): `model_config = ConfigDict(extra="forbid")`; fields: `date_filter: list[date] | None`, `last_time: LastTimeModel | None`, `time_unit: Literal["d","w","m","y"] = "w"`. Validator enforces mutual exclusivity of `date_filter` and `last_time`; requires exactly one.
- **`ReportSettings`** (models.py:87-115): `model_config = ConfigDict(extra="allow")` (commented at :87-92 explaining the two-layer architecture); declared fields: `check_done`, `report_type`, `report_subtype`, `media`, `compression`, `data_lang`, `relative_path`, `multiple_files`, `ad_filter`, `target_audiences`, `period`.
- **`DataDefaults`** (models.py:128-130): `extra="allow"`; field: `DATA_DEFAULTS: dict[str, Any]`; subtype blocks absorbed as extras.
- **`Data`** (models.py:133-148): `extra="allow"`; declared fields: `options`, `statistics`, `slices`. **NEW (4877882, :139-148):** `coerce_none_to_empty` field validator converts `None` → `[]` for `options`/`statistics`/`slices` (partially addresses TPL-003 for the `en` path).

### File Inventory (current)

**Report templates** (16 `.json5` files, 32 entries):

| File | Entries | Media | Report Types |
|---|---|---|---|
| `reports/test.json5` | 1 | TV | crosstab |
| `reports/gap_resurs/nat_tv_brands.json5` | 2 | TV | crosstab |
| `reports/gap_resurs/reg_tv_by_spots.json5` | 3 | TV_REG | crosstab |
| `reports/last_category/nat_tv_brands_last.json5` | 2 | TV | crosstab |
| `reports/last_category/nat_tv_brands_snack.json5` | 2 | TV | crosstab |
| `reports/last_category/nat_tv_brands_weekly_reach.json5` | 1 | TV | crosstab |
| `reports/nat_tv_brands_tefal.json5` | 2 | TV | crosstab |
| `reports/nat_tv_brands_last.json5` | 2 | TV | crosstab |
| `reports/nat_tv_tvreport.json5` | 7 | TV | crosstab ×5, timeband ×2 |
| `reports/sovcombank/nat_tv_brands_finance.json5` | 2 | TV | crosstab |
| `reports/sovcombank/nat_tv_brands_finance2.json5` | 2 | TV | crosstab |
| `reports/sovcombank/nat_tv_by_spots.json5` | 3 | TV | crosstab |
| `reports/sovcombank/nat_tv_finance_q.json5` | 1 | TV | crosstab |
| `reports/sovcombank/reg_tv_by_spots.json5` | 1 | TV_REG | crosstab |
| `reports/sovcombank/tv_msk_new_creatives.json5` | 1 | TV | crosstab |
| `reports/sovcombank/tv_msk_new_creatives_short.json5` | 1 | TV | crosstab |

**Defaults templates** (6 `.json5` files):

| File | Media | ReportType | Subtype blocks | `slices` default |
|---|---|---|---|---|
| `defaults/tv/crosstab.json5` | TV | crosstab | 7 (all enum subtypes) | `null` (line 14) |
| `defaults/tv/timeband.json5` | TV | timeband | 0 | `null` (line 12) |
| `defaults/tv/simple.json5` | TV | simple | 0 | `null` (line 12) |
| `defaults/tv_reg/crosstab.json5` | TV_REG | crosstab | 2 (DYNAMICS_BY_SPOTS, _DICT) | `["regionName"]` (line 15-17) |
| `defaults/tv_reg/timeband.json5` | TV_REG | timeband | 0 | `null` (line 15) |
| `defaults/tv_reg/simple.json5` | TV_REG | simple | 0 | `null` (line 15) |

**Note:** The original 3 defaults files are now 6 — the 3 missing combinations (`tv/simple`, `tv_reg/timeband`, `tv_reg/simple`) were created in `335a11e`.

### Test Suite (52 tests, all passing)

| Test file | Tests | Relevant to TPL findings |
|---|---|---|
| `test_app.py` | 7 | CLI surface (JSON5) |
| `test_models.py` | 7 | `test_report_settings_valid`, `test_bundled_templates_validate`, `test_data_defaults_covers_all_subtypes`, `test_data_coerces_none_to_empty`, `test_period_model_validation`, `test_merge_data_settings_en_does_not_translate`, `test_period_model_rejects_both_date_filter_and_last_time` |
| `test_services.py` | 8 | `test_run_report_with_real_models` (slow), `test_get_default_data_settings_missing_file`, config-cache tests, `test_get_connection_raises_on_missing_credential` |
| `test_utils.py` | 6 | `str_to_date`, `slice_period`, `csv_to_file`, `get_files_suffix` — **does NOT test `en_to_ru` or `json5_to_dict`** |
| `test_network.py` | 8 | NetworkClient retry logic |
| `test_reports.py` | 4 | Report strategies |
| `test_reports_lifecycle.py` | 9 | Full pipeline lifecycle |
| `test_init_credentials.py` | 1 | Init/credential copy |
| `test_tasks.py` | 2 | TVTask |

---

## 2. Finding-by-Finding Assessment

### TPL-001 — [CRITICAL] Bundled report templates fail `ReportSettings` validation

**Status: RESOLVED**

The original finding: 30/32 template entries fail `ReportSettings(**rep)` due to:
(a) `last_time` uses `period_type`/`period_num` while `LastTimeModel` requires `time_unit`/`unit_count`
(b) `report_type` uses stale values `TV_CROSSTAB`, `TV_DICT_CROSSTAB`, `REG_TV_CROSSTAB`, `REG_TV_DICT_CROSSTAB`
(c) Legacy templates omit required `media` and `relative_path` fields

**Current code evidence:**

- **Schema (`core/models.py`):** `time_unit` now lives on `PeriodModel` (line 71), not `LastTimeModel` (line 61-64). `LastTimeModel` has `extra="forbid"` (line 62) and fields `include_current` + `unit_count`. `ReportSettings` requires `media: MediaType` (line 97) and `relative_path: Path` (line 102). `ReportType` enum (line 44-47) has only `crosstab`/`timeband`/`simple`.

- **Templates:** All 16 report templates migrated to JSON5 now use correct field names. Every entry has `media`, `relative_path`, `period.time_unit`, and `report_type` values matching the `ReportType` enum. Examples:
  - `nat_tv_brands_tefal.json5`: `report_type: "crosstab"`, `media: "TV"`, `relative_path: "tefal/raw_data"`, `period.time_unit: "m"`
  - `nat_tv_tvreport.json5`: 7 entries, all with `media: "TV"`, `relative_path`, `period.last_time.unit_count`, `period.time_unit`
  - `last_category/nat_tv_brands_weekly_reach.json5`: `kitId: 7` in `options` (see TPL-006)

- **Runtime probe (verified):** `ReportSettings(**entry)` on all 32 entries → **32 passed, 0 failed**.

- **Tests added:** `test_bundled_templates_validate` (test_models.py:67-96) asserts 32/32 entries validate. `test_data_defaults_covers_all_subtypes` (test_models.py:99-115) asserts all 7 `ReportSubtype` values have matching defaults keys.

**What changed:** The `183aed6` commit moved `time_unit` to `PeriodModel` and updated all templates. The JSON5 migration (`a596d86`) renamed all fields to match the new schema. The `4877882` commit added `coerce_none_to_empty` and the mutual-exclusivity validator.

**Remaining residual (TPL-001 → TPL-006 carryover):** The `nat_tv_tvreport.json5` entry[0] (TOP_NAT_TV_ADVERTISERS) has no `options` block (VAL-003 structural observation). It inherits the default's `options` which has `issueType: "AD"` + `standRtg` but no `kitId` — so `reports.py:263` (`options.get("kitId", 1)`) defaults to 1. The resulting kitId=1 coincides with what the template would intend, but the `options` block is silently absent. This is masked because `extra="allow"` absorbs it without error.

**Conclusion:** TPL-001 is **resolved**. No code or template changes needed for the core finding. The kitId reconciliation residual is tracked under TPL-006 below.

---

### TPL-002 — [LOW] Typo'd API-parameter keys pass through `Data` unvalidated

**Status: STILL APPLICABLE (LOW, as re-graded)**

**Current code evidence:**

- `Data.model_config = ConfigDict(extra="allow")` (models.py:134). Comment at models.py:87-92 on `ReportSettings` explicitly documents this as the intended two-layer transport. The `settings-layers-architecture.md` doc (lines 106-151) documents the full consumption trace: `merge_data_settings` → `Data(**data_settings)` → `model_dump()` → `TVTask.settings` → `build_task(report_type, **task.settings)` → vendor API JSON.

- `reports.py:259-261` reads `options.get("kitId", 1)` — the silent kitId fallback for typo'd `options` keys.
- `reports.py:492` (`DefaultTVTaskStrategy`) and `reports.py:515` (`TVRegTaskStrategy`) call `report.data_settings.model_dump()` which includes all 12 extra API-parameter keys.
- `reports.py:350-352` calls `self.builder` (=`MediaVortexTask.build_task`) with `**task.settings`.

**Residual risks (unchanged from plan):**
1. A typo'd top-level key in a template (e.g. `ad_filterr` instead of `ad_filter`) is silently absorbed by `extra="allow"`, forwarded to `build_task`, and dropped by the vendor's function signature — no error raised.
2. A typo'd key inside `options` (e.g. `kitid` instead of `kitId`) is forwarded verbatim to the API, while `reports.py:263` falls back to kitId=1 silently.

**Tests:** No test in `test_utils.py` or `test_models.py` asserts `extra="allow"` behavior on `Data`. The plan recommends adding a test that constructs a `Data` with a typo'd `options` key and asserts no silent kitId fallback — this test does NOT exist.

**Conclusion:** STILL APPLICABLE at LOW. The pass-through is intended (do NOT change to `extra="forbid"` — it would break the two-layer overlay). The recommendation stands: template conformance ensures parameter names match API names; consider adding declared fields for known API parameters if stricter typing is desired.

---

### TPL-003 — [HIGH] `en_to_ru(None)` crash when a report subtype has no defaults block

**Status: PARTIALLY RESOLVED (code-level gap remains)**

**Current code evidence:**

- **`en_to_ru`** (utils.py:223-233): STILL has no `None` guard. `len(slices_en)` on `None` → `TypeError` → caught by `except (TypeError, AttributeError)` → logged → re-raised. No change since the plan.

```python
def en_to_ru(slices_en: list[Any]) -> list[Any]:
    try:
        slices_ru: list[list[Any]] = [[] for _ in range(len(slices_en))]
        for i, param in enumerate(slices_en):
            slices_ru[i] = param.replace("EName", "Name")
        return slices_ru
    except (TypeError, AttributeError) as err:
        logger.error(f"Element must be a string, got {type(slices_en)}")
        raise err
```

- **`merge_data_settings`** (services.py:131): STILL calls `utils.en_to_ru(data_settings["slices"])` without any `None` guard:
```python
if report_settings.data_lang == "ru":
    data_settings["slices"] = utils.en_to_ru(data_settings["slices"])
```

- **Partial fix applied (4877882):** `Data.coerce_none_to_empty` (models.py:139-148) converts `None` → `[]` at `Data` construction time. This fixes the `data_lang="en"` path where `None` slices reach `Data(**data_settings)` without going through `en_to_ru`. **But the `en_to_ru` call happens in `merge_data_settings` (services.py:131) BEFORE `Data` is constructed (services.py:138)** — so the crash path is still alive for `data_lang="ru"`.

**Runtime probe (verified):**
- `en_to_ru(None)` → `TypeError: object of type 'NoneType' has no len()` (STILL CRASHES)
- Timeband defaults (`defaults/tv/timeband.json5`) has `DATA_DEFAULTS.slices: null` (line 12) and ZERO subtype blocks.
- A timeband report with `data_lang="ru"` and `report_subtype=NAT_TV_CHANNELS_ATV` (not in timeband defaults) → `merge_data_settings` crashes with `TypeError`.
- Same report with `data_lang="en"` → `coerce_none_to_empty` converts `None`→`[]` → `Data.slices = []` (WORKS).

**Why the crash doesn't trigger with current bundled templates:** Every `data_lang="ru"` template in the repo either (a) uses `crosstab` with a subtype that has its own `slices` block in `defaults/tv/crosstab.json5` (all 7 subtypes have slices), or (b) provides its own `slices` list in the template (e.g. `nat_tv_tvreport.json5` timeband entries). But the code-level vulnerability remains for any user-authored template that doesn't provide `slices` with `data_lang="ru"` on a non-crosstab report type.

**Tests:** 
- `test_data_defaults_covers_all_subtypes` (test_models.py:99-115) verifies all 7 subtypes have defaults in `tv/crosstab.json5` — this mitigates the crash for crosstab reports with known subtypes.
- NO test exercises `en_to_ru(None)` or `data_lang="ru"` with `None` slices.

**Fix needed (code):** Add a `None` guard in `merge_data_settings` before the `en_to_ru` call:
```python
if report_settings.data_lang == "ru":
    slices = data_settings.get("slices")
    if slices is not None:
        data_settings["slices"] = utils.en_to_ru(slices)
```
Or alternatively, guard in `en_to_ru` itself (`if slices_en is None: return []`).

**Fix needed (template):** Change `DATA_DEFAULTS.slices` from `null` to `[]` in `defaults/tv/timeband.json5` and `defaults/tv/simple.json5` (and `tv_reg/timeband.json5`, `tv_reg/simple.json5`).

**Conclusion:** TPL-003 is **partially resolved**. The `en` path is fixed by `coerce_none_to_empty`; the `ru` path crash in `en_to_ru`/`merge_data_settings` is NOT fixed. The test `test_data_defaults_covers_all_subtypes` provides indirect mitigation for crosstab but not for timeband/simple reports.

---

### TPL-004 — [HIGH] Malformed YAML/JSON5 in defaults

**Status: RESOLVED**

The original finding: `defaults/tv_reg/crosstab.yaml` had unbalanced flow-mapping braces (13 open / 12 close). Additionally (DISC-07), `defaults/tv/crosstab.yaml` had a `TOP_NAT_TV_PROGRAMS.slices` sequence closed with `}` instead of `]` (regression from commit `b04d177`).

**Current code evidence:**

- All 6 JSON5 defaults files parse correctly (runtime probe verified):
  ```
  tv/crosstab.json5: OK, keys= ['DATA_DEFAULTS', 'DYNAMICS_BY_SPOTS', 'DYNAMICS_BY_SPOTS_DICT', 'TOP_NAT_TV_ADVERTISERS', 'TOP_NAT_TV_PROGRAMS', 'NAT_TV_CHANNELS_ATV', 'NAT_TV_CHANNELS_BA', 'NAT_TV_CHANNELS_SOC_DEM']
  tv/simple.json5: OK, keys= ['DATA_DEFAULTS']
  tv/timeband.json5: OK, keys= ['DATA_DEFAULTS']
  tv_reg/crosstab.json5: OK, keys= ['DATA_DEFAULTS', 'DYNAMICS_BY_SPOTS', 'DYNAMICS_BY_SPOTS_DICT']
  tv_reg/simple.json5: OK, keys= ['DATA_DEFAULTS']
  tv_reg/timeband.json5: OK, keys= ['DATA_DEFAULTS']
  ```

- `defaults/tv/crosstab.json5` now has all 7 subtype blocks (the plan noted `NAT_TV_CHANNELS_ATV/BA/SOC_DEM` were absent in the old YAML — commit `335a11e` added them).
- `defaults/tv_reg/crosstab.json5` has correct brace structure — `DATA_DEFAULTS` closes at line 32, then subtype blocks follow at the top level.
- `json5_to_dict` (utils.py:365-389) raises `ValueError` for invalid JSON5 and `FileNotFoundError` for missing files — no implicit `None` return.

- `data-flow-architecture.md` (line 341) documents this as RESOLVED: "Both bundled crosstab DEFAULT templates were malformed YAML [pre-JSON5 migration]... Fixed at HEAD — templates now parse at HEAD."

**Tests:** `test_get_default_data_settings_missing_file` (test_services.py:223-249) loads all 6 defaults files and validates each as `DataDefaults`.

**Conclusion:** TPL-004 is **resolved** by the JSON5 migration. The structural defects (unbalanced braces, wrong closing characters) were naturally eliminated by the clean JSON5 conversion. The `335a11e` commit also added the missing subtype blocks to `tv/crosstab.json5`. No further action needed.

---

### TPL-005 — [MEDIUM] `get_default_data_settings` has no existence check

**Status: PARTIALLY RESOLVED (template side fixed, code side not)**

**Current code evidence:**

- **Template side RESOLVED:** All 6 defaults files now exist. The 3 missing files (`tv/simple.json5`, `tv_reg/timeband.json5`, `tv_reg/simple.json5`) were created in `335a11e`. Test `test_get_default_data_settings_missing_file` (test_services.py:223-249) verifies all 6 `(media, report_type)` combinations load:
```python
for media, report_type in combos:
    defaults_path = APP_PATHS.app_settings_dir / "defaults" / media.value.lower() / f"{report_type.value}.json5"
    assert defaults_path.exists(), f"Missing defaults template: {defaults_path}"
    data = json5.loads(defaults_path.read_text(encoding="utf-8"))
    defaults = DataDefaults(**data)
```

- **Code side NOT FIXED:** `get_default_data_settings` (services.py:100-114) still has **no `path.exists()` check**. It builds the path and calls `json5_to_dict` directly:
```python
def get_default_data_settings(self, report_settings: ReportSettings) -> DataDefaults:
    data_default_settings_path = Path(
        self.app_paths.defaults_dir,
        report_settings.media.value.lower(),
        report_settings.report_type.value.lower(),
    ).with_suffix(".json5")
    data_defaults_dict = utils.json5_to_dict(data_default_settings_path)
    if not isinstance(data_defaults_dict, dict):
        raise TypeError(...)
    return DataDefaults(**data_defaults_dict)
```

- The plan recommended: "check `path.exists()` before calling `json5_to_dict`; if absent, raise `WrongDataSettings`." This is **NOT implemented.**
- `WrongDataSettings` (errors.py:20-26) exists but its own docstring says: "Reserved for future use... Currently unused."
- **`json5_to_dict`** (utils.py:365-389) was changed to **raise** `FileNotFoundError` instead of returning `None` (commit `b2ff6e4`). So the failure surface is now a clear `FileNotFoundError` with a log message, rather than the opaque `TypeError` described in the plan. This is an improvement but doesn't use the domain-specific `WrongDataSettings` exception.
- `paths.py:61-81` `connection_config` property DOES have an existence check that raises `FileNotFoundError` with a helpful "Run `init`" message — but `get_default_data_settings` does NOT follow the same pattern.

**Tests:** No test covers the missing-defaults-file path in `get_default_data_settings`. The `test_get_default_data_settings_missing_file` test only verifies that all 6 EXISTING files load — it does not test the missing-file error path.

**Fix needed (code):** Add `path.exists()` check in `get_default_data_settings`; raise `WrongDataSettings` with a clear `(media, report_type)` message instead of letting `FileNotFoundError` propagate.

**Conclusion:** TPL-005 is **partially resolved**. The missing-file templates were created, but the code-level existence check with `WrongDataSettings` was NOT implemented. The crash is now a clear `FileNotFoundError` (improved from `TypeError`) but still not the domain-specific exception.

---

### TPL-006 — [LOW, folded into TPL-001] `kitId` value/comment drift between defaults and templates

**Status: STILL APPLICABLE**

The plan rejected TPL-006 as a merge-semantics finding (shallow overlay is intended) and re-typed the kitId drift residual into TPL-001.

**Current code evidence — kitId=7 (UNDEFINED in legend) still present:**

The legend comment in `defaults/tv/crosstab.json5` (line 58) and `defaults/tv_reg/crosstab.json5` (line 71, 97):
```
// dataset id (1-Russia all, 2-Russia 100+, 3-Cities, 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow)
```
Value `7` is **undefined**. Yet kitId=7 persists:

**In defaults (`defaults/tv/crosstab.json5`):**
- Line 126: `TOP_NAT_TV_PROGRAMS.options.kitId = 7` — comment says "TV Index Russia all" (WRONG — 1 is Russia all, 7 is undefined)
- Line 164: `NAT_TV_CHANNELS_BA.options.kitId = 7` — comment says "TV Index Russia all" (WRONG)

**In templates:**
- `reports/nat_tv_tvreport.json5` line 174: `NAT_TV_CHANNELS_BA` entry has `kitId: 7` (same as default — doesn't correct the drift)
- `reports/last_category/nat_tv_brands_weekly_reach.json5` line 27: `DYNAMICS_BY_SPOTS` entry has `kitId: 7` — overrides the default's kitId=1 (the "wrong direction" noted in the plan: template sets 7 over default's 1 for the same subtype)

**Partially correct in templates:**
- `nat_tv_tvreport.json5` line 53: `TOP_NAT_TV_PROGRAMS` has `kitId: 1` — correctly overrides default's 7
- `nat_tv_tvreport.json5` entries [2]-[6]: kitId=1 (correct for Russia all)

**Fixed items (TPL-006 resolution applied):**
- `TOP_NAT_TV_ADVERTISERS` in `defaults/tv/crosstab.json5` (lines 99-119): NO `kitId`, NO duplicate `issueType` — the old YAML had a duplicate `issueType` key; this is fixed in JSON5. ✓
- `DATA_DEFAULTS.options` is `null` (line 17) — not a dict, correctly. ✓ (The plan noted the original claim about `DATA_DEFAULTS.options.kitId=1` was incorrect.)

**Comment drift also present in `defaults/tv_reg/crosstab.json5`:**
- Line 73: `DYNAMICS_BY_SPOTS.options.kitId = 3` — comment says "TV Index Russia all" (WRONG — should say "TV Reg" / "Cities"; kitId=3 = Cities per the legend)
- Line 99: `DYNAMICS_BY_SPOTS_DICT.options.kitId = 3` — same wrong comment

**Tests:** No test asserts kitId values or merge behavior. The only validating template (`nat_tv_brands_tefal.json5`) declares no `options` block and inherits `DYNAMICS_BY_SPOTS` kitId=1.

**Status:** STILL APPLICABLE. kitId=7 (undefined) persists in both defaults and one template. Comment drift (kitId=3 labeled "Russia all") also persists. The `nat_tv_brands_weekly_reach.json5` template overrides kitId=7 over default=1 — the wrong direction per the plan's analysis.

---

### VAL-003 Residual (structural issue in `nat_tv_tvreport.json5`)

**Status: PRESENT but masked**

`nat_tv_tvreport.json5` entry[0] (TOP_NAT_TV_ADVERTISERS) has NO `options` block. The `options` block at lines 52-59 belongs to entry[1] (TOP_NAT_TV_PROGRAMS). Entry[0] inherits the default's `TOP_NAT_TV_ADVERTISERS.options` which has `issueType: "AD"` + `standRtg` but no `kitId`. At `reports.py:263`, `options.get("kitId", 1)` defaults to 1. Since kitId=1 (Russia all) is likely the intended value for TOP_NAT_TV_ADVERTISERS, this happens to produce the correct result by accident. But the template's intended `options` is silently absent.

This was documented as VAL-003 in the plan ("should be filed as a separate finding if TPL-001 is resolved"). Since TPL-001 IS resolved, this structural issue is now unmasked (though harmless due to the silent default).

---

## 3. Dependencies Between Fixes (Plan's Rollout Safety Assessment — Current Status)

| Dependency | Original concern | Current status |
|---|---|---|
| TPL-004 must land before TPL-003 TV_REG testing | `tv_reg/crosstab.yaml` ParserError blocked TV_REG defaults | **Resolved** (JSON5 migration fixed both files) |
| TPL-004 must land before TPL-005 TV_REG/timeband testable | Malformed crosstab prevented TV_REG defaults tests | **Resolved** (all 6 files exist and parse) |
| TPL-001 migration must include kitId reconciliation | Once field names are migrated, kitId divergences become active | **At risk** — TPL-001 resolved but kitId=7 drift NOT reconciled |
| TPL-003 must land before CFG-007 reachable | `en_to_ru(None)` crashes before `Data` construction | **Not addressed** — `en_to_ru(None)` still crashes for `data_lang="ru"` |
| TPL-002 `extra="allow"` must NOT be narrowed | Would break two-layer overlay | **Acknowledged** — code comment + architecture doc confirm |

---

## 4. Code Changes vs. Template-Only Changes

| Finding | Code change needed? | Template change needed? |
|---|---|---|
| TPL-001 | No (resolved) | No (resolved) — kitId carryover to TPL-006 |
| TPL-002 | No (`extra="allow"` is intended) | No (template conformance) |
| TPL-003 | **YES** — guard `en_to_ru(None)` in `merge_data_settings` (services.py:130-131) or `en_to_ru` itself (utils.py:223-233) | Change `DATA_DEFAULTS.slices` from `null` to `[]` in timeband/simple defaults files |
| TPL-004 | No (resolved) | No (resolved) |
| TPL-005 | **YES** — add `path.exists()` + raise `WrongDataSettings` in `get_default_data_settings` (services.py:100-114) | No (all files exist) |
| TPL-006 | No (merged into TPL-001 template conformance) | **YES** — reconcile kitId=7 → valid value (1); fix comments on kitId=3 entries; add `options` to `nat_tv_tvreport.json5` entry[0] if intended |

---

## 5. Tests That Must Stay Green

All 52 tests pass currently. The following tests are directly relevant to Phase 05 findings:

| Test | File:line | Asserts | Must-stay-green constraint |
|---|---|---|---|
| `test_report_settings_valid` | test_models.py:20 | `time_unit`/`unit_count` correct keys; `ReportSettings` construction succeeds | Any model/schema change must not break this |
| `test_bundled_templates_validate` | test_models.py:67 | All 32 entries pass `ReportSettings.model_validate` | TPL-001 fix must keep 32/32 passing |
| `test_data_defaults_covers_all_subtypes` | test_models.py:99 | All 7 subtypes present in `tv/crosstab.json5` | TPL-006 kitId reconciliation must not remove subtype blocks |
| `test_data_coerces_none_to_empty` | test_models.py:118 | `Data(options=None, statistics=None, slices=None)` → `[]`, `[]`, `[]` | `coerce_none_to_empty` validator must stay |
| `test_merge_data_settings_en_does_not_translate` | test_models.py:42 | `en_to_ru` NOT called for `data_lang="en"` | TPL-003 `en_to_ru` guard must not call it for `en` |
| `test_get_default_data_settings_missing_file` | test_services.py:223 | All 6 `(media, report_type)` combos load | TPL-005 must keep all 6 files resolvable |
| `test_period_model_validation` | test_models.py:33 | `LastTimeModel(unit_count=4)` + mutual exclusivity | Schema changes must preserve |
| `test_period_model_rejects_both_date_filter_and_last_time` | test_models.py:126 | Both fields → `ValidationError` | Must stay green |
| `test_run_report_with_real_models` | test_services.py:43 (slow) | Full pipeline with `tv_json5_content` fixture (correct `media`/`report_type`/`relative_path`; `last_time: null`) | Template/schema changes must not break |

**Note:** `test_utils.py` does NOT test `en_to_ru` or `json5_to_dict` directly. The plan recommended adding a test for `en_to_ru(None)` and a test for the missing-defaults-file path — neither exists.

---

## 6. Stale Documentation

Several docs still reference the pre-migration YAML state:

| File | Line(s) | Stale content |
|---|---|---|
| `.ai/audit/05-config-templates/findings.md` | 25, 31, 36, 38-40, 48, 52 | References `*.yaml`, `period_type`/`period_num`, `TV_CROSSTAB`, `d093d9c` |
| `.ai/audit/05-config-templates/_validate.py` | 3-6, 16, 25, 33-34, 42, 63, 79, 115, 117 | Uses `yaml.safe_load`, `glob("**/*.yaml")`, `yaml_to_dict` |
| `.ai/audit/99-validation/05-config-templates-validated-findings.md` | 13-17 (header), 58, 204 | References `yaml_to_dict`, `*.yaml`, `yaml.safe_load` — has migration TODO marker |
| `.ai/audit/06-json5-audit/code_context.md` | 4 (status) | Says "Tests updated (uncommitted)" — tests ARE committed; "Documentation pending" — docs ARE updated in main docs |
| `docs/00-overview/settings-layers-architecture.md` | 57, 67 | Line 57: still says "duplicate `issueType` at L107" for TOP_NAT_TV_ADVERTISERS (fixed in JSON5); line 67: says `nat_tv_brands_tefal` is "the only validating template" (now all 32 validate) |
| `docs/00-overview/settings-layers-architecture.md` | Line 57 | References YAML line numbers (L59, L87, L107, L128, L150, L166, L181) |
| `core/models.py` `coerce_none_to_empty` docstring | 144 | Says "YAML ``Null`` values" and "``Null`` entries" — should say "JSON5 ``null`` values" (line 142 already says "JSON5" but line 146 says "YAML Null") |
| `.ai/audit/02-config-secrets/findings.md` | 28, 67, 112-114, 128, 191-204 | References `*.yaml`, `yaml_to_dict`, stale template validation failure counts (6-7 per entry) |
| `.ai/audit/10-code-quality/findings.md` | 49, 66, 71-72, 83-85, 89, 96 | References `yaml_to_dict` (function no longer exists) |

---

## 7. Architecture & Dependencies

### Current Architecture (unchanged in shape, evolved in format)

```
src/mko_get_mediascope_data/
├── __init__.py          # PEP 562 lazy re-exports (run_report, run_report_async, etc.)
├── __main__.py          # `python -m mko_get_mediascope_data` → Typer app
├── app.py               # CLI (init, run, list-reports) + public API
├── core/
│   ├── paths.py         # PathResolver + AppPaths (config dir resolution; JSON5 paths)
│   ├── models.py        # Pydantic v2 models + Enums (MediaType, ReportType, ReportSubtype)
│   ├── init_service.py  # Atomic template copy to user config dir; legacy credential rename
│   ├── services.py      # AppService (get_report_settings, get_default_data_settings, merge_data_settings, get_data_settings, run_report)
│   ├── reports.py       # Report classes + Strategy pattern (TaskGenerationStrategy, DataPreparationStrategy)
│   ├── tasks.py         # TVTask (async task rep; to_file() writes JSON5 error artifacts)
│   ├── network.py       # NetworkClient (async retry + backoff + semaphore)
│   ├── utils.py         # Helpers: date/period logic, json5_to_dict, en_to_ru, csv I/O
│   └── errors.py        # NoReportFoundError, MaxRetriesExceeded, WrongDataSettings (unused)
└── settings/            # Bundled JSON5 templates (app_config, log_config, connections, defaults/, reports/)
```

### Key Call Chain (merge path)

1. `AppService.get_report_settings(report_settings_path)` (services.py:85) — loads JSON5 via `json5_to_dict`, constructs `ReportSettings(**rep)` at line 95.
2. `AppService.get_default_data_settings(report_settings)` (services.py:100) — builds `defaults/{media}/{report_type}.json5` path, loads via `json5_to_dict`, constructs `DataDefaults(**data)`.
3. `AppService.merge_data_settings(defaults, report_settings)` (services.py:116) — shallow top-level overlay:
   - L120: `data_settings = defaults.DATA_DEFAULTS.copy()`
   - L122-124: overlay subtype block (`dict.update`)
   - L126-128: overlay report settings keys (only those present in `data_settings`)
   - L130-131: `en_to_ru(data_settings["slices"])` if `data_lang=="ru"` — **crash point for TPL-003**
4. `AppService.get_data_settings(report_settings)` (services.py:135) — calls above 3, then `Data(**data_settings)` (services.py:138) — `coerce_none_to_empty` converts remaining `None`→`[]`.
5. `TVMediaReport.__init__` (reports.py:233) — reads `options.get("kitId", 1)` at line 263.
6. `DefaultTVTaskStrategy.generate` (reports.py:487) — `report.data_settings.model_dump()` → `TVTask.settings`.
7. `TVMediaReport.send_task` (reports.py:343) — `build_task(report_type, **task.settings)` → vendor API JSON.

### Dependencies

- `json5>=0.17.3` (pyproject.toml:17) — settings file loading
- `pydantic>=2.12.5` — all models/validation
- `mediascope-api-lib>=1.1.15` — vendor API client (`MediaVortexTask`)
- `pandas>=2.2.0` — DataFrame processing
- `platformdirs>=4.7.0` — user config dir resolution (`paths.py:4, 9`)
- `uv` — package manager
- No database, no Docker, no web framework

### External Dependencies (vendor boundary)

`media_vortex` / `mediascope_api.mediavortex.tasks.MediaVortexTask` — `build_task(report_type, **task.settings)` reads each named parameter (`statistics`, `ad_filter`→`adFilter`, `slices`, `options`, `sortings`, `frequencyDistConditions`, `break_filter`→`breakFilter`, etc.) into the API JSON request body. This is the consumer of the `extra="allow"` pass-through.

---

## 8. Recommendations (Priority / Effort)

| # | What | Why | Effort | Priority | Type |
|---|---|---|---|---|---|
| 1 | Guard `en_to_ru(None)` in `merge_data_settings` (services.py:131) — skip call if `slices` is None or non-list | Eliminates the only remaining `TypeError` crash path for `data_lang="ru"` reports | Trivial | Recommended | TPL-003 |
| 2 | Alternatively, guard in `en_to_ru` itself (utils.py:223) — `if slices_en is None: return []` | Single-point fix covering all call paths | Trivial | — | TPL-003 |
| 3 | Add `path.exists()` check in `get_default_data_settings` (services.py:103) — raise `WrongDataSettings` | Converts `FileNotFoundError` into domain-specific exception with clear `(media, report_type)` message; enables graceful handling | Small | Recommended | TPL-005 |
| 4 | Reconcile kitId=7 → valid value (1) in `defaults/tv/crosstab.json5` (TOP_NAT_TV_PROGRAMS L126, NAT_TV_CHANNELS_BA L164) | kitId 7 is undefined in the legend; reports against wrong dataset risk | Trivial | Recommended | TPL-006 |
| 5 | Reconcile kitId=7 → 1 in `reports/last_category/nat_tv_brands_weekly_reach.json5` L27 and `reports/nat_tv_tvreport.json5` L174 | Template overrides default's kitId=1 with undefined 7 (wrong direction) | Trivial | Recommended | TPL-006 |
| 6 | Fix comment drift: kitId=3 labeled "TV Index Russia all" in `defaults/tv_reg/crosstab.json5` (L73, L99) — should be "TV Reg"/"Cities" | Prevents operator confusion about dataset selection | Trivial | Advisory | TPL-006 |
| 7 | Add `options` block to `nat_tv_tvreport.json5` entry[0] (TOP_NAT_TV_ADVERTISERS) if intended, or confirm silence is deliberate | Currently relies on silent kitId=1 default from `reports.py:263` | Trivial | Advisory | VAL-003 |
| 8 | Change `DATA_DEFAULTS.slices` from `null` to `[]` in `tv/timeband.json5`, `tv/simple.json5`, `tv_reg/timeband.json5`, `tv_reg/simple.json5` | Eliminates `None` at source; defense-in-depth alongside the `en_to_ru` guard | Trivial | Recommended | TPL-003 |
| 9 | Add test for `en_to_ru(None)` → returns `[]` (not crashes) after guard added | Prevents regression of the TPL-003 fix | Small | Recommended | TPL-003 |
| 10 | Add test for `get_default_data_settings` with non-existent file → `WrongDataSettings` | Prevents regression of TPL-005 fix | Small | Recommended | TPL-005 |
| 11 | Fix stale docstring in `coerce_none_to_empty` (models.py:146) — "YAML ``Null``" → "JSON5 ``null``" | Consistency with current format | Trivial | Advisory | Doc |
| 12 | Update stale audit docs: `05-config-templates/findings.md`, `_validate.py`, `02-config-secrets/findings.md`, `10-code-quality/findings.md` | References to `yaml_to_dict`, `*.yaml` are dead anchors; future auditors will be misled | Medium | Advisory | Doc |

**Do NOT change:** `extra="allow"` on `Data`/`DataDefaults`/`ReportSettings` (TPL-002) — it is the intended transport for the two-layer merge. Do NOT implement a deep merge (TPL-006 rejected) — shallow top-level overlay is the documented architecture.

---

## 9. Key Code Locations Reference

| Symbol | File | Line(s) |
|---|---|---|
| `LastTimeModel` (`extra="forbid"`) | `core/models.py` | 61-64 |
| `PeriodModel` (`time_unit`, `extra="forbid"`) | `core/models.py` | 67-84 |
| `ReportSettings` (`extra="allow"`, `media`, `relative_path`) | `core/models.py` | 87-115 |
| `DataDefaults` (`extra="allow"`) | `core/models.py` | 128-130 |
| `Data` (`extra="allow"`, `coerce_none_to_empty`) | `core/models.py` | 133-148 |
| `en_to_ru` (no None guard) | `core/utils.py` | 223-233 |
| `json5_to_dict` (raises FileNotFoundError/ValueError) | `core/utils.py` | 365-389 |
| `get_report_settings` | `core/services.py` | 85-98 |
| `get_default_data_settings` (no exists() check) | `core/services.py` | 100-114 |
| `merge_data_settings` (en_to_ru call, no guard) | `core/services.py` | 116-133 |
| `get_data_settings` | `core/services.py` | 135-138 |
| `data_lang=="ru"` → `en_to_ru` call | `core/services.py` | 130-131 |
| `WrongDataSettings` (defined, unused) | `core/errors.py` | 20-26 |
| `options.get("kitId", 1)` (silent fallback) | `core/reports.py` | 262-263 |
| `build_task(report_type, **task.settings)` | `core/reports.py` | 350-352 |
| `connection_config` (has exists() check) | `core/paths.py` | 61-81 |
| `test_bundled_templates_validate` | `tests/test_models.py` | 67-96 |
| `test_data_defaults_covers_all_subtypes` | `tests/test_models.py` | 99-115 |
| `test_data_coerces_none_to_empty` | `tests/test_models.py` | 118-123 |
| `test_merge_data_settings_en_does_not_translate` | `tests/test_models.py` | 42-64 |
| `test_get_default_data_settings_missing_file` | `tests/test_services.py` | 223-249 |
| `test_run_report_with_real_models` (slow) | `tests/test_services.py` | 43-106 |

---

## 10. Conclusion

**The YAML→JSON5 migration and targeted remediation commits have resolved TPL-001, TPL-004, and the template side of TPL-005.** However, two findings still have unresolved code-level gaps:

1. **TPL-003 (HIGH)** — The `en_to_ru(None)` crash in `merge_data_settings` (services.py:131) is still reachable for `data_lang="ru"` reports on timeband/simple defaults (which have `slices: null` and no subtype blocks). The `coerce_none_to_empty` validator only covers the `data_lang="en"` path. A 2-line guard is needed.

2. **TPL-005 (MEDIUM)** — All 6 defaults files now exist, but `get_default_data_settings` still has no `path.exists()` check and `WrongDataSettings` remains unused. A clear `FileNotFoundError` is raised instead (improved from the original `TypeError`), but the domain-specific exception is not used.

3. **TPL-006 (LOW)** — kitId=7 (undefined in the legend) persists in `defaults/tv/crosstab.json5` (TOP_NAT_TV_PROGRAMS, NAT_TV_CHANNELS_BA) and in two templates (`nat_tv_brands_weekly_reach.json5`, `nat_tv_tvreport.json5`). Comment drift (kitId=3 labeled "Russia all") also persists.

4. **TPL-002 (LOW)** — Still applicable; the `extra="allow"` pass-through is intended and documented. Typo risk is real but mitigated by template conformance.

The plan file itself (and other pre-migration audit docs) contain stale YAML references but carry a migration TODO marker acknowledging this. A follow-up audit pass to re-anchor findings to the JSON5 codebase is recommended.
