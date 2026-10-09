---
phase: "05"
phase_name: config-templates
id_prefix: "TPL"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Config Template Validation & Spec Conformance

## Summary

Five classes of defects across the bundled YAML templates and their consuming Pydantic models. The highest severity is CRITICAL: essentially every shipped report-definition example is non-runnable because its field names no longer match the models that load them. The broadest business consequence is silent wrong-data risk on Mediascope API parameters the `Data` model neither declares nor validates (e.g. `kitId`, `date_filter`), which can produce a report against the wrong dataset with no validation failure.

## Scope

**Examined:** `src/mko_get_mediascope_data/settings/reports/**` (16 YAML files, 32 entries), `src/mko_get_mediascope_data/settings/defaults/**` (3 YAML files), `src/mko_get_mediascope_data/core/models.py`, `src/mko_get_mediascope_data/core/services.py` (load + merge), `src/mko_get_mediascope_data/core/reports.py` (reads from `Data`), `src/mko_get_mediascope_data/core/utils.py` (`en_to_ru`, `yaml_to_dict`), `src/mko_get_mediascope_data/core/paths.py`.
**Tools:** `uv run python` validation reproduction, `grep`, `git ls-files`.
**Not verified:** live Mediascope API behavior and credential sourcing (shipped `connections/mediascope_RENAME_.json` vs `core/paths.py::AppPaths.connection_config` expecting `mediascope.json` — owned by phase 02); end-to-end runs against live credentials.

## Findings

**Checked key — per-model `extra`:** `LastTimeModel`=`'forbid'`, `PeriodModel`=`None` (Pydantic default → ignore), `ReportSettings`=`'allow'`, `DataDefaults`=`'allow'`, `Data`=`'allow'`.

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TPL-001 | CRITICAL | `src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings` | 30/32 bundled reports fail ReportSettings validation due to stale field names |
| TPL-002 | HIGH | `src/mko_get_mediascope_data/core/models.py::Data` | Data model declares 3 of 15 merged keys; 12 API params are untyped extras |
| TPL-003 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings` | en_to_ru(None) TypeError aborts ru reports whose subtype lacks a defaults block |
| TPL-004 | HIGH | `src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml::"DATA_DEFAULTS"` | Malformed YAML (unclosed flow mapping) in required TV_REG default |
| TPL-005 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings` | Default path built with no existence check; tv_reg/timeband.yaml absent |
| TPL-006 | LOW | `src/mko_get_mediascope_data/settings/defaults/tv/crosstab.yaml::"kitId"` | kitId duplicated between defaults and templates with divergent values, resolved by shallow full-replace merge |

#### TPL-001 — [CRITICAL] 30/32 bundled reports fail ReportSettings validation due to stale field names

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings`
**Disambiguator:** `"ReportSettings(**rep)"`
**Checked:** ran
**Why this band:** "a report template that fails validation so the report cannot run at all, while a shipped example asserts it should work" (phase CRITICAL). Sixteen shipped template files assert they should run and `run_report` aborts on every one of the 30 affected entries.
**Problem:** Templates were not migrated to the refactored models. Three independent drifts each independently break validation: (a) `last_time` uses `period_type`/`period_num` while `LastTimeModel` (extra=`'forbid'`) requires `time_unit`/`unit_count`; (b) `report_type` uses `TV_CROSSTAB`/`REG_TV_CROSSTAB`/`TV_DICT_CROSSTAB`/`REG_TV_DICT_CROSSTAB`, none of which are `ReportType` enum values (`crosstab`/`timeband`/`simple`); (c) legacy templates omit the required `media` and `relative_path` fields, carrying `folder`/`category_name` instead. `get_report_settings` calls `ReportSettings(**rep)` with no lenient fallback, so the error propagates and `run_report` aborts.
**Repro:** `uv run python .ai/audit/05-config-templates/_validate.py` (Block 1: every bundled report entry validated as `ReportSettings`). Swapping only the `last_time` keys to `time_unit`/`unit_count` makes the same entry validate, confirming field-name drift.
**Impact:** Any operator running `mko run <template>.yaml` gets a `ValidationError` and an aborted run; the bundled report examples are non-functional.
**Root cause:** Model refactor (added required `media`/`relative_path`, a strict `ReportType` enum, and `LastTimeModel(time_unit, unit_count)` + `extra='forbid'`) landed without a corresponding template migration; `tests/test_models.py` already encodes the *correct* keys, proving the templates — not the tests — are stale.
**Fix:** Migrate the 16 template files to the model: `last_time.period_type`→`time_unit`, `last_time.period_num`→`unit_count`, `report_type: TV_CROSSTAB`→`crosstab`, `folder`→`relative_path`, and add `media`. Target: `src/mko_get_mediascope_data/settings/reports/**/*.yaml` field keys (templates, not code; tests already assert the target shape). Recommended — unblocks all examples.
**Verify:** new test needed: `test_bundled_reports_validate` (assert every `settings/reports/**/*.yaml` entry constructs a `ReportSettings` without error).
**Related:** TPL-002, TPL-005

**Evidence — `uv run python .ai/audit/05-config-templates/_validate.py`** *(supports: "30/32 entries fail validation")*:
```text
BLOCK 1: Report templates -> ReportSettings
  total entries validated: 32
  failed: 30
  passed: 2
  FAIL reports\nat_tv_brands_last.yaml[0]: period.last_time.unit_count: missing; period.last_time.period_type: extra_forbidden; period.last_time.period_num: extra_forbidden
  FAIL reports\gap_resurs\nat_tv_by_spots.yaml[0]: report_type: enum; media: missing; relative_path: missing; period.last_time.unit_count: missing; period.last_time.period_type: extra_forbidden; period.last_time.period_num: extra_forbidden
  FAIL reports\gap_resurs\reg_tv_by_spots.yaml[0]: report_type: enum; report_subtype: enum; media: missing; relative_path: missing; period.last_time.unit_count: missing; period.last_time.period_type: extra_forbidden; period.last_time.period_num: extra_forbidden
  FAIL reports\test.yaml[0]: report_type: enum; media: missing; relative_path: missing; period.last_time.unit_count: missing; period.last_time.period_type: extra_forbidden; period.last_time.period_num: extra_forbidden
```

#### TPL-002 — [HIGH] Data model declares 3 of 15 merged keys; 12 API params are untyped extras

**Anchor:** `src/mko_get_mediascope_data/core/models.py::Data`
**Disambiguator:** `"options"`
**Checked:** ran
**Why this band:** "extra='allow' silently absorbing a typoed field name that the code never reads, so a misconfigured report runs with defaults the operator did not intend" (phase HIGH). `Data` validates no field the API actually consumes.
**Problem:** `Data` declares only `options`, `statistics`, `slices` and sets `extra='allow'`. `merge_data_settings` hands it a 15-key dict; the remaining 12 keys — `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`, `location_filter`, `basedemo_filter`, `targetdemo_filter`, `program_filter`, `break_filter`, `ad_filter`, `sortings`, `frequency_dist_conditions` — become untyped `__pydantic_extra__` with no presence/type/content check. A typo on any passes `Data(**data_settings)` silently. Concretely, `TVMediaReport` reads `kitId` via `self.data_settings.options.get("kitId", 1)`; a misspelled `kitid` falls back to `1` (Russia-all) instead of the intended dataset with no error. `ReportSettings` (`extra='allow'`) likewise absorbs the orphaned template keys `category_name`, `folder`, `period_type`, `period_num` without complaint (they are never referenced in `core/`).
**Repro:** merge `nat_tv_brands_tefal.yaml[0]` (the only template that validates) → `Data(**merged)`; inspect `data.model_dump()`.
**Impact:** Misspelled API params produce reports against the wrong slice/filter/dataset with no validation failure — silent wrong data.
**Root cause:** `Data`/`ReportSettings`/`DataDefaults` were given `extra='allow'` to tolerate the legacy template shape, but the legacy keys were never pruned to a typed contract, so the model validates nothing the API consumes.
**Fix:** Tighten `extra` to a typed contract — add typed fields for every consumed API key and forbid untyped extras — or, minimally, enforce an explicit consumed-key allow-list in the strategy. Target symbols: `src/mko_get_mediascope_data/core/models.py::Data` (add fields / set `extra='forbid'`) and `src/mko_get_mediascope_data/core/reports.py` strategy tasks. Recommended.
**Verify:** new test needed: `test_data_rejects_unknown_keys` (or a consumed-key allow-list assertion).
**Related:** TPL-001, TPL-003

**Evidence — `Data(**merged).model_dump()` / `m.model_config.get("extra")`** *(supports: "3 declared of 15 keys")*:
```text
BLOCK4 merge->Data for nat_tv_brands_tefal[0]:
   merged keys: ['ad_filter','basedemo_filter','break_filter','company_filter',
     'date_filter','daytype_filter','frequency_dist_conditions','location_filter',
     'options','program_filter','slices','sortings','statistics','targetdemo_filter','weekday_filter']
   Data fields + extras count: 15
   declared Data fields: 3 (options, statistics, slices)
LastTimeModel: extra='forbid'
PeriodModel: extra=None
ReportSettings: extra='allow'
DataDefaults: extra='allow'
Data: extra='allow'
```

#### TPL-003 — [HIGH] en_to_ru(None) TypeError aborts ru reports whose subtype lacks a defaults block

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
**Disambiguator:** `en_to_ru`
**Checked:** ran
**Why this band:** a template that passes validation but crashes in the merge with no runtime fallback (phase HIGH: "a template that passes lenient parsing but fails strict validation with no runtime fallback" — here the failure is a crash on otherwise-valid input).
**Problem:** For any `data_lang == 'ru'` report, `merge_data_settings` unconditionally calls `utils.en_to_ru(data_settings["slices"])`. `DATA_DEFAULTS.slices` is `Null`, and `en_to_ru` does `[[] for _ in range(len(slices_en))]` — `len(None)` raises `TypeError`. A subtype absent from the defaults file (no override block) leaves `slices` as `None`, so the call crashes. The shipped subtype set is `{DYNAMICS_BY_SPOTS, DYNAMICS_BY_SPOTS_DICT, TOP_NAT_TV_ADVERTISERS, TOP_NAT_TV_PROGRAMS}`; any other subtype (`NAT_TV_CHANNELS_ATV/BA/SOC_DEM`, etc.) with `data_lang: ru` triggers it. There is no guard and no fallback. Currently masked by TPL-001 (those ru templates fail ReportSettings first), but it fires as soon as the drift is repaired.
**Repro:** build `ReportSettings(media=TV, report_type=crosstab, report_subtype=NAT_TV_CHANNELS_ATV, data_lang=ru, ...)` (valid enum/symbols) then call `svc.get_data_settings(rs)`.
**Impact:** `get_data_settings` → `run_report` aborts with an opaque `TypeError` for whole families of TV+ru reports.
**Root cause:** `DATA_DEFAULTS.slices` defaults to `Null` (not a list), and `en_to_ru` assumes a list; `merge_data_settings` never guards the `None` case.
**Fix:** Default `DATA_DEFAULTS.slices` to `[]` in `src/mko_get_mediascope_data/settings/defaults/tv{_reg}/*.yaml`, and guard the call in `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings` (`if data_settings.get("slices"):`) before invoking `en_to_ru`. Recommended.
**Verify:** new test needed: `test_get_data_settings_survives_missing_subtype_slices`.
**Related:** TPL-002, TPL-005

**Evidence — `svc.get_data_settings(probe)` with `data_lang="ru"` and subtype `NAT_TV_CHANNELS_ATV`** *(supports: "en_to_ru(None) TypeError")*:
```text
  en_to_ru(None) raises: TypeError: object of type 'NoneType' has no len()
  get_data_settings(NAT_TV_CHANNELS_ATV/ru) crashes: TypeError: object of type 'NoneType' has no len()
  subtype blocks present in tv/crosstab.yaml: ['DYNAMICS_BY_SPOTS', 'DYNAMICS_BY_SPOTS_DICT', 'TOP_NAT_TV_ADVERTISERS', 'TOP_NAT_TV_PROGRAMS']
```

#### TPL-004 — [HIGH] Malformed YAML (unclosed flow mapping) in required TV_REG default

**Anchor:** `src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml::"DATA_DEFAULTS"`
**Disambiguator:** `"DATA_DEFAULTS"`
**Checked:** ran
**Why this band:** a shipped default template that is not even parseable, with no runtime fallback (phase HIGH; the file is required for an entire media-type report family).
**Problem:** `defaults/tv_reg/crosstab.yaml` is a flow mapping whose `DATA_DEFAULTS` block is opened but never closed before the subtype blocks `DYNAMICS_BY_SPOTS_M`/`_D`/`_DICT`, so those subtype blocks are nested inside `DATA_DEFAULTS` and the outermost `{` is left unclosed at EOF. `yaml.safe_load` raises `ParserError`; `utils.yaml_to_dict` catches `yaml.YAMLError` and returns `None`; `DataDefaults(**None)` then raises `TypeError`. This is the default loaded for every TV_REG+crosstab report. Masked today only because TV_REG report templates already fail `ReportSettings` (TPL-001).
**Repro:** `uv run python .ai/audit/05-config-templates/_validate.py` (Block 2: each default validated as `DataDefaults`).
**Impact:** Any TV_REG+crosstab report cannot load its defaults and aborts in `get_default_data_settings`/`get_data_settings`.
**Root cause:** The `DATA_DEFAULTS` closing brace was dropped when the subtype blocks were appended — the sibling `tv/crosstab.yaml` closes `DATA_DEFAULTS` before its first subtype, but `tv_reg/crosstab.yaml` does not.
**Fix:** Add the missing `}` to close `DATA_DEFAULTS` before the `DYNAMICS_BY_SPOTS_M` subtype block in `src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml` (mirror the brace structure of `tv/crosstab.yaml`). Target: that YAML file.
**Verify:** new test needed: `test_tv_reg_defaults_load` (assert `DataDefaults` validates `defaults/tv_reg/crosstab.yaml`).
**Related:** TPL-001, TPL-005

**Evidence — `DataDefaults(**load_yaml("defaults/tv_reg/crosstab.yaml"))`** *(supports: "ParserError at EOF")*:
```text
  OK   defaults\tv\crosstab.yaml
  OK   defaults\tv\timeband.yaml
  FAIL defaults\tv_reg\crosstab.yaml: ParserError: while parsing a flow mapping
    in "...defaults\ttv_reg\crosstab.yaml", line 1, column 1
  expected ',' or '}', but got '<stream end>'
    in "...defaults\ttv_reg\crosstab.yaml", line 162, column 2
```

#### TPL-005 — [MEDIUM] Default path built with no existence check (latent crash)

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings`
**Disambiguator:** `defaults_dir`
**Checked:** ran
**Why this band:** a latent crash on a configuration that has no consumer today but no guard against it (phase MEDIUM).
**Problem:** `get_default_data_settings` builds `{defaults_dir}/{media.value.lower()}/{report_type.value.lower()}.yaml` (via `AppPaths.defaults_dir` / `media.value` / `report_type.value`) and passes it straight to `utils.yaml_to_dict` + `DataDefaults(**...)` with no existence check. Shipped defaults cover only `{tv,crosstab}`, `{tv,timeband}`, `{tv_reg,crosstab}`. There is no `tv_reg/timeband.yaml`; a TV_REG+timeband report reads a non-existent file, `yaml_to_dict` returns `None`, and `DataDefaults(**None)` raises `TypeError`. Latent: no shipped template targets TV_REG+timeband, and existing TV_REG templates are blocked upstream by TPL-001. (See also the credential-file naming owned by phase 02, filed below.)
**Repro:** `git ls-files src/mko_get_mediascope_data/settings/defaults/` to enumerate shipped defaults; observe none matches `tv_reg/timeband.yaml`.
**Impact:** Adding a TV_REG+timeband report crashes with an opaque `TypeError` instead of a clear "missing defaults" message.
**Root cause:** The defaults directory is keyed off enum values with no fallback/defaulting when the file is absent.
**Fix:** In `src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings`, guard the path: if the default file is missing, raise the already-defined `src/mko_get_mediascope_data/core/errors.py::WrongDataSettings` instead of `DataDefaults(**None)`; and ship `defaults/tv_reg/timeband.yaml`.
**Verify:** new test needed: `test_missing_default_raises_clear_error`.
**Related:** TPL-003, TPL-004

**Evidence — `git ls-files src/mko_get_mediascope_data/settings/defaults/`** *(supports: "tv_reg/timeband.yaml absent")*:
```text
shipped defaults: ['tv\\crosstab.yaml', 'tv\\timeband.yaml', 'tv_reg\\crosstab.yaml']
  defaults\tv_reg\timeband.yaml exists=False
```

#### TPL-006 — [LOW] kitId duplicated with divergent values, resolved by shallow full-replace merge

**Anchor:** `src/mko_get_mediascope_data/settings/defaults/tv/crosstab.yaml::"kitId"`
**Disambiguator:** `"kitId"`
**Checked:** ran
**Why this band:** documentation/duplication drift with no runtime consequence today (phase LOW: "a default file with no consumer" / comment drift).
**Problem:** `kitId` is duplicated across the defaults subtype blocks and the report templates. For subtype `TOP_NAT_TV_PROGRAMS` the default block sets `kitId: 7` while the matching report entry in `nat_tv_tvreport.yaml` sets `kitId: 1`. `merge_data_settings` overlays report fields with a shallow assignment, so the report-level `options` dict fully replaces the subtype default `options` (no deep merge) — the default `kitId: 7` is silently discarded and the report's `kitId: 1` wins. Both values are annotated `#TV Index Russia all`, contradicting the in-file dataset legend (`1`=Russia all, `7` undefined). No runtime consequence today: the overriding template fails `ReportSettings` (TPL-001), but it is a latent wrong-dataset risk and a maintenance hazard once the drift is repaired.
**Repro:** merge `nat_tv_tvreport.yaml[1]` through `merge_data_settings` → result `options.kitId == 1`, default `kitId == 7` discarded (observed for the analogous tefal[0] merge where the default's kitId surfaces unchanged). The shallow `data_settings[k] = v` assignment in `merge_data_settings` is the mechanism.
**Impact:** A maintainer editing only the defaults file believes they control `kitId`, while the report template silently overrides it; a typo in a partial `options` block also drops every other default option key (`issueType`, `standRtg`, …) without warning.
**Root cause:** `kitId` is a magic number duplicated in defaults and templates with no single source of truth, combined with a shallow (not recursive) overlay.
**Fix:** Treat `kitId` as a typed constant on the defaults `DataDefaults`/`Data` model and/or switch the `options`/`slices`/`statistics` overlay to a deep merge with an explicit conflict policy; at minimum document that report-level `options` fully replaces (not merges) the default. Target: `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`.
**Verify:** new test needed: `test_options_overlay_is_explicit` documenting the full-replace semantics.
**Related:** TPL-002, TPL-001

**Evidence — kitId values per file + merge assignment** *(supports: "default kitId 7 vs report kitId 1; shallow replace")*:
```text
tv/crosstab.yaml:  'kitId': 1,  #TOP_NAT_TV_ADVERTISERS block (line 61)
                   "kitId": 7,  #TOP_NAT_TV_ADVERTISERS block (line 107)
                   "kitId": 7,  #TOP_NAT_TV_PROGRAMS block (line 129)
nat_tv_tvreport.yaml[1]: report_subtype=TOP_NAT_TV_PROGRAMS; "kitId": 1 (report override)
services.py merge loop:  data_settings[k] = v   # shallow replace, NOT a deep merge
merged for tefal[0]: options.kitId = 1  # surfaced from the default subtype block (report had no options)
```

## Fix Order

- **Dependencies:** TPL-001 must be fixed before TPL-002/TPL-003/TPL-004 runtime risk becomes reachable on the affected templates (otherwise they abort upstream at `get_report_settings`). TPL-001 and TPL-004 are otherwise independent.
- **Merge candidates:** None share a single root cause; each touches a distinct defect.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** Credential-file naming (`connections/mediascope_RENAME_.json` vs `AppPaths.connection_config`'s `mediascope.json`) belongs to phase 02; filed there, not here.
- **Rollout risk:** TPL-001 template migration changes no code path that `tests/test_models.py` / `tests/test_services.py` exercises (they use the inline `tv_yaml_content` fixture, which already uses the *correct* `time_unit`/`unit_count` keys), so fixing templates aligns production with existing tests rather than breaking them. TPL-004's brace fix must preserve the `tv/crosstab.yaml` structure exactly (mirror it).

## Appendices

### Appendix A — Validation reproduction (Block 1 + Block 2)

```text
BLOCK 1: Report templates -> ReportSettings
  total entries validated: 32
  failed: 30
  passed: 2
  [8 templates] period.last_time.unit_count: missing; period.last_time.period_type: extra_forbidden; period.last_time.period_num: extra_forbidden
      e.g. reports\nat_tv_brands_last.yaml[0]
      e.g. reports\nat_tv_tvreport.yaml[0]
  [13 templates] report_type: enum; media: missing; relative_path: missing; ...
  [2 templates] report_type: enum; report_subtype: enum; ...
  [7 templates] report_type: enum; media: missing; relative_path: missing; target_audiences.None.[key]: string_type; ...

BLOCK 2: Default data templates -> DataDefaults
  OK   defaults\tv\crosstab.yaml
  OK   defaults\tv\timeband.yaml
  yaml.parser.ParserError: expected ',' or '}', but got '<stream end>'
    in "...defaults\ttv_reg\crosstab.yaml", line 162, column 2
```

### Appendix B — Merged Data vs declared (Block 5 evidence)

```text
LastTimeModel: extra='forbid'
PeriodModel: extra=None  (Pydantic default => ignore)
ReportSettings: extra='allow'
DataDefaults: extra='allow'
Data: extra='allow'
```
`Data` declared fields = {`options`,`statistics`,`slices`}; consumed API keys (`date_filter`, `weekday_filter`, `company_filter`, `ad_filter`, `sortings`, …) live only as untyped extras. Template keys consumed nowhere in `core/`: `category_name`, `period_type`, `period_num` (grep: 0 hits each in `core/*.py`); `folder` appears only as an unrelated parameter name in `core/tasks.py::TVTask.to_yaml`, never as a consumed `ReportSettings`/`Data` attribute.

### Appendix C — Advisory (not filed): full-replace merge semantics

`merge_data_settings` does `data_settings[k] = v` (shallow) for `options`/`slices`/`statistics`, not a deep merge: a report-level partial `options`/`slices` block fully replaces the subtype default. No shipped template currently supplies a *partial* override (they specify full blocks or none), so this is latent; flag before loosening `extra` on `Data` (TPL-002), else deep-merge semantics must be added. This is the mechanism behind TPL-006.

## Glossary of anchors
- `src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings` — strict `ReportSettings(**rep)` loader, no fallback
- `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings` — defaults+subtype+report overlay, calls `en_to_ru`
- `src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings` — builds default path, no existence guard
- `src/mko_get_mediascope_data/core/models.py::Data` — `extra='allow'`, declares 3 of 15 keys
- `src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml::"DATA_DEFAULTS"` — malformed YAML
- `src/mko_get_mediascope_data/settings/defaults/tv/crosstab.yaml::"kitId"` — duplicated kitId
- `src/mko_get_mediascope_data/core/errors.py::WrongDataSettings` — available clear-error type for TPL-005
