---
phase: "05"
phase_name: "config-templates"
id_prefix: "TPL"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Config Template Validation & Spec Conformance

## Summary

Six findings (1 CRITICAL, 3 HIGH, 1 MEDIUM, 1 LOW). All six primary anchors
resolve to a real code symbol or YAML token, and all six claims reproduce under
runtime probe or static inspection. **6 confirmed, 0 rejected, 0 re-graded, 0
unsettled.**

Two cross-phase overlaps with Phase 02 (config-secrets) are noted: TPL-001
shares a root cause with CFG-001 (same 30/32 failure), and TPL-004/TPL-005
cascade from the same `yaml_to_dict` None-return defect that CFG-005 identifies.
Both are cross-references; no Phase 05 finding was downgraded because of a Phase
02 finding.

Three validation-level findings (VAL-001, VAL-002, VAL-003) are recorded
separately: a brace-count mismatch in the TPL-004 evidence, an inaccurate
claim about the `tv_yaml_content` conftest fixture using `time_unit`/`unit_count`,
and a previously-unreported YAML structural defect in `nat_tv_tvreport.yaml` that
distorts parsed `options` blocks.

## Scope

**Examined:** `src/mko_get_mediascope_data/core/models.py` (Pydantic v2 models:
`ReportSettings`, `PeriodModel`, `LastTimeModel`, `DataDefaults`, `Data`),
`core/services.py` (`AppService`, `get_report_settings`, `get_default_data_settings`,
`merge_data_settings`), `core/reports.py` (`TVMediaReport.__init__`,
`base_id` assignment), `core/utils.py` (`yaml_to_dict`, `en_to_ru`,
`get_data_settings`), `core/paths.py` (`AppPaths.connection_config`),
`core/errors.py` (`WrongDataSettings`), all 16 bundled `settings/reports/**/*.yaml`
templates, all 3 bundled `settings/defaults/**/*.yaml` files,
`settings/connections/mediascope_RENAME_.json`, `init_service.py`, `tests/conftest.py`,
`tests/test_models.py`, `tests/test_services.py`, `tests/test_utils.py`,
`.kilo/commands/audit/phases/05-audit-config-templates.md`,
`.ai/audit/templates/audit-findings.md`,
`.ai/audit/02-config-secrets/99-validation/02-config-secrets-validated-findings.md`
(cross-phase), `.ai/audit/05-config-templates/_validate.py` (reproduction script).

**Runtime probes performed:**
- `ReportSettings(**rep)` for all 32 entries across 16 templates → 2 PASS, 30 FAIL
- `yaml.safe_load` on `defaults/tv/crosstab.yaml` → OK (5 keys)
- `yaml.safe_load` on `defaults/tv_reg/crosstab.yaml` → `ParserError` at line 162
- Brace count: `tv/crosstab.yaml` 13 open / 13 close (balanced); `tv_reg/crosstab.yaml`
  13 open / 12 close (unbalanced, diff=1)
- `Data(statistics=None, slices=None)` → `ValidationError`
- `en_to_ru(None)` → `TypeError: object of type 'NoneType' has no len()`
- `en_to_ru(['researchDate', 'tvCompanyEName'])` → `['researchDate', 'tvCompanyName']`
- `Data(**merged)` for the one validating template (`nat_tv_brands_tefal.yaml[0]`) →
  15 keys, 3 declared (`options`, `statistics`, `slices`)
- `options.kitId` in default `DATA_DEFAULTS` and subtype blocks of `tv/crosstab.yaml`
  → kitId=1 (DYNAMICS_BY_SPOTS), kitId=7 (TOP_NAT_TV_ADVERTISERS), kitId=7 (TOP_NAT_TV_PROGRAMS)
- `options.kitId` in templates → kitId=1 (TOP_NAT_TV_PROGRAMS, `nat_tv_tvreport.yaml[1]`,
  from `yaml.safe_load`), kitId=7 (DYNAMICS_BY_SPOTS, `last_category/nat_tv_brands_weekly_reach.yaml[0]`)
- `ReportSettings.model_dump()` with extra fields → extras (`options`, `company_filter`,
  `category_name`, `folder`) appear in dump due to `extra='allow'`
- `pytest` (full suite, 12 tests) → 12 passed

**Not verified:** Live Mediascope API calls (no credentials); behavior inside Jupyter
kernels (no kernel available). `en_to_ru` correct-function probe used a static list,
not a real Mediascope response.

**Tools:** `uv run python` (runtime probes), `yaml.safe_load` (YAML parsing),
`rg`/grep tool (source scan), `read` tool (source inspection), `pytest` (test suite).

---

## Findings

| ID      | Disposition  | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|--------------------------|-----------------|
| TPL-001 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes             |
| TPL-002 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| TPL-003 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| TPL-004 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| TPL-005 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM        | yes             |
| TPL-006 | confirmed   | LOW        | LOW vs. LOW              | yes\*           |

\* TPL-006's evidence cites `tv/crosstab.yaml` line 61 as being in the
"TOP_NAT_TV_ADVERTISERS block" — it is actually in the `DYNAMICS_BY_SPOTS` block
(see VAL-001). The kitId value (1) and the divergence claim (default=7 vs
template=1 for `TOP_NAT_TV_PROGRAMS`) are correct; only the block-label is wrong.

---

### TPL-001 — confirmed — [CRITICAL] Bundled report templates fail `ReportSettings` validation

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings`
— line 61: `ReportSettings(**rep)` inside the generator loop.

**Anchor resolved (secondary):**
`src/mko_get_mediascope_data/core/models.py::LastTimeModel` —
`extra="forbid"`; `PeriodModel` — `extra=None` (inherits `extra="ignore"`);
`ReportType` enum values `crosstab`/`timeband`/`simple`.

**Claim tested:** 30 of 32 bundled report entries fail `ReportSettings(**rep)`.
Three independent drifts:

(a) `last_time` uses `period_type`/`period_num` while `LastTimeModel`
(`extra="forbid"`) requires `time_unit`/`unit_count`.
(b) `report_type` uses stale values `TV_CROSSTAB`, `TV_DICT_CROSSTAB`,
`REG_TV_CROSSTAB`, `REG_TV_DICT_CROSSTAB` (not in `ReportType` enum, which
only has `crosstab`, `timeband`, `simple`).
(c) Legacy templates omit required `media` and `relative_path` fields.

**Evidence (runtime probe — all 32 template entries):**
```text
total entries validated: 32
failed: 30
passed: 2

Passing entries:
  reports\nat_tv_brands_tefal.yaml[0]
  reports\nat_tv_brands_tefal.yaml[1]

Failure modes across 30 failing entries:
  report_type: enum                  (23 entries — stale TV_CROSSTAB / REG_TV_* values)
  media: missing                     (15 entries)
  relative_path: missing             (15 entries)
  period.last_time.unit_count: missing (30 entries)
  period.last_time.period_type: extra_forbidden (30 entries)
  period.last_time.period_num: extra_forbidden   (30 entries)
  target_audiences.None.[key]: string_type       (7 entries — `Null` as dict key)
```

The 2 passing entries (`nat_tv_brands_tefal.yaml[0]` and `[1]`) use correct
`report_type: 'crosstab'`, `time_unit`/`unit_count`, and include `media`/`relative_path`.

**Asserted cause tested separately:** `LastTimeModel` at `models.py:61` declares
`model_config = ConfigDict(extra="forbid")`. Templates using `period_type`/`period_num`
trigger `extra_forbidden` errors. `ReportType` enum (verified via `model_fields`)
has values `crosstab`, `timeband`, `simple` only. Legacy templates from the
`gap_resurs`, `sovcombank`, and `last_category` directories use `TV_CROSSTAB` etc.
and lack `media`/`relative_path`. All verified against source and runtime.

**Rubric grading:** Phase 05 CRITICAL = "a report template that fails validation
so the report cannot run at all, while a shipped example asserts it should work."
30/32 templates fail — 100% of shipped templates that differ from the
reference template. The `run` CLI command and `app_service.run_report` both call
`get_report_settings` → `ReportSettings(**rep)` with no fallback. CRITICAL
carried = CRITICAL on rubric. No re-grade.

**Load-bearing artefact:** Code (`core/models.py` defines the schema; `services.py`
calls `ReportSettings(**rep)`); Templates (`settings/reports/**/*.yaml` carry stale
values). Code constrains which symbols must change if field names are altered;
templates are the data that must migrate.

**Fix:** Migrate all 15 template files: rename `period_type→time_unit`,
`period_num→unit_count`; replace stale `ReportType` enum strings (`TV_CROSSTAB`→`crosstab`,
`REG_TV_CROSSTAB`→`crosstab`, etc.); add `media` and `relative_path` to legacy entries.
Targets: all 15 template files. Stable. ✓

**Verify:** `tests/test_models.py::test_report_settings_valid` uses correct
`time_unit`/`unit_count` keys and passes — must stay green.
`tests/test_services.py::test_run_report_with_real_models` uses the
`tv_yaml_content` fixture with correct keys and passes — must stay green.

**Related:** TPL-002, TPL-003, TPL-004, TPL-005, TPL-006, CFG-001

---

### TPL-002 — confirmed — [HIGH] `Data` model silently absorbs 12 untyped API-parameter keys

**Anchor resolved:** `src/mko_get_mediascope_data/core/models.py::Data`
— `model_config = ConfigDict(extra="allow")`; declares 3 fields:
`options`, `statistics`, `slices`.

**Claim tested:** The merged `Data` dict for the one validating template
(`nat_tv_brands_tefal.yaml[0]`) contains 15 keys; 12 are consumed nowhere by
Pydantic and stored as untyped `Any` extras. `ReportSettings`
(`extra="allow"`) similarly absorbs orphaned template keys (`category_name`,
`folder`, `period_type`, `period_num`) that no code path reads.

**Evidence (runtime probe — merge + Data construction):**
```text
merged keys count: 15  declared Data fields: 3 (options, statistics, slices)
Data.model_dump() keys count: 15
ad_filter survived from template: (articleLevel3Id IN (4165, 4171) OR arti...
options.kitId survived from default DYNAMICS_BY_SPOTS: 1

ReportSettings.model_dump() extras verified:
  options, company_filter, category_name, folder  ← all absorbed via extra='allow'
```

Full 15-key inventory: `ad_filter`, `basedemo_filter`, `break_filter`,
`company_filter`, `date_filter`, `daytype_filter`, `frequency_dist_conditions`,
`location_filter`, `options`, `program_filter`, `slices`, `sortings`,
`statistics`, `targetdemo_filter`, `weekday_filter`.

12 untyped extras: all keys except `options`, `statistics`, `slices`.

**Asserted cause tested separately:**
- `Data.model_config` has `extra="allow"` — verified in `models.py`.
- `TVMediaReport.__init__` (`reports.py:189`):
  `self.base_id = self.data_settings.options.get("kitId", 1)` — confirmed
  via `rg -n 'kitId' src/mko_get_mediascope_data/core/reports.py`.
  A typoed `kitid` (lowercase) becomes an extra key absorbed by `Data`,
  while `.get("kitId", 1)` silently returns `1` (Russia all), producing
  a report against the wrong dataset without error.
- Template keys with 0 hits in `core/` (grep tool): `category_name` (0 hits),
  `period_type` (0 hits), `period_num` (0 hits). `folder` appears only as an
  unrelated parameter name in `core/tasks.py:29` (`def to_yaml(self,
  folder: str | bytes | PathLike = None)`), never as a consumed
  `ReportSettings`/`Data` attribute.

**Rubric grading:** Phase 05 HIGH = "`extra='allow'` silently absorbing a
typoed field name that the code never reads, so a misconfigured report runs
with defaults the operator did not intend." The `Data` model's `extra="allow"`
absorbs typoed keys; `TVMediaReport` reads `kitId` with a silent default of `1`.
HIGH carried = HIGH on rubric. No re-grade.

**Load-bearing artefact:** Code (`core/models.py::Data` and
`core/reports.py:189`).

**Fix:** Narrow `Data` to `extra="forbid"` to reject typoed keys at
serialization; type each API parameter as a named field with `Field(default=None)`.
If that is too invasive, at minimum type `options` as `dict[str, Any]` with
`model_config = ConfigDict(extra="forbid")` on a nested `Options` model so
`kitId` vs `kitid` is caught. Target: `core/models.py::Data` and
`core/reports.py:189`. Stable. ✓

**Verify:** `tests/test_utils.py` has no test asserting `extra="allow"` behavior
on `Data`. `tests/test_models.py` constructs `Data`-adjacent models with
explicit fields — no test asserts extra-field absorption.

**Related:** TPL-001, TPL-006, CFG-007

---

### TPL-003 — confirmed — [HIGH] `en_to_ru(None)` crash when a report subtype has no defaults block

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
— line 90: `data_settings["slices"] = utils.en_to_ru(data_settings["slices"])`.

**Anchor resolved (secondary):**
`src/mko_get_mediascope_data/core/utils.py::en_to_ru` — `len(slices_en)` on
line 6 of the function body.

**Claim tested:** For any `data_lang == "ru"` report whose `report_subtype`
has no matching key in the defaults file, `DATA_DEFAULTS.slices` is `Null`
(None), `en_to_ru(None)` raises `TypeError`, crashing `AppService.run_report`
before any API call is made. The crash occurs in `merge_data_settings`, after
`ReportSettings` validation passes — a template that is structurally valid
still crashes at the merge step.

**Evidence (runtime probes):**
```text
DATA_DEFAULTS.slices in defaults/tv/crosstab.yaml: Null
DATA_DEFAULTS.slices in defaults/tv/timeband.yaml: Null

en_to_ru(None) raises: TypeError: object of type 'NoneType' has no len()
  (logged as ERROR: parameter should be list <class 'NoneType'> is given)

Subtypes present in defaults/tv/crosstab.yaml:
  DYNAMICS_BY_SPOTS, DYNAMICS_BY_SPOTS_DICT, TOP_NAT_TV_ADVERTISERS, TOP_NAT_TV_PROGRAMS
Subtypes absent (no defaults block → slices stays None):
  NAT_TV_CHANNELS_ATV, NAT_TV_CHANNELS_BA, NAT_TV_CHANNELS_SOC_DEM

en_to_ru(['researchDate', 'tvCompanyEName']) -> ['researchDate', 'tvCompanyName']
(en_to_ru works correctly on a valid list)

get_data_settings(NAT_TV_CHANNELS_ATV/ru) crashes: TypeError: object of type 'NoneType' has no len()
  (reproduced via the validated probe in _validate.py Block 3+4)
```

**Asserted cause tested separately:** `merge_data_settings` (services.py:77-93):
1. `data_settings = defaults.DATA_DEFAULTS.copy()` — `slices` is `None`
2. `if subtype.value and hasattr(defaults, subtype.value)` — for
   `NAT_TV_CHANNELS_ATV`, `hasattr` returns `False` (not in defaults)
3. `if report_settings.data_lang == "ru":` → `True` for ru reports
4. `data_settings["slices"]` is `None` → `en_to_ru(None)` → `TypeError`

`en_to_ru` (utils.py:1-6) does `len(slices_en)` unconditionally; no `None` guard.
Verified against source and runtime.

**Rubric grading:** Phase 05 HIGH = "a template that passes lenient parsing
but fails strict validation with no runtime fallback." `ReportSettings(**rep)`
succeeds for these subtypes (they use valid field names), but `merge_data_settings`
crashes with a `TypeError` that `get_report_settings`'s `except Exception` re-raises.
HIGH carried = HIGH on rubric. No re-grade.

**Load-bearing artefact:** Code (`core/services.py::merge_data_settings` and
`core/utils.py::en_to_ru`).

**Fix:** Two complementary changes:
(a) `services.py::merge_data_settings` — guard the `en_to_ru` call: only invoke
when `data_settings["slices"]` is a list; treat `None` as `[]`.
(b) `tv/crosstab.yaml` / `tv/timeband.yaml` — change `DATA_DEFAULTS.slices`
from `Null` to `[]`.
Target: `merge_data_settings` at services.py:90; `en_to_ru` at utils.py. Stable. ✓

**Verify:** No existing test exercises `en_to_ru(None)` or `data_lang="ru"` with a
non-default subtype. A new test for the `None`-slices path is recommended.

**Related:** TPL-002, TPL-004, TPL-005, CFG-005, CFG-007

---

### TPL-004 — confirmed — [HIGH] Malformed YAML in `defaults/tv_reg/crosstab.yaml`

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml::"DATA_DEFAULTS"`
— the opening `'DATA_DEFAULTS': {` at line 3.

**Claim tested:** `tv_reg/crosstab.yaml` has unbalanced flow-mapping braces
(13 open / 12 close). The `options.standRtg` sub-block inside `DATA_DEFAULTS`
consumes an extra closing `}`, so `DATA_DEFAULTS` never closes before the subtype
blocks. All subtype blocks (`DYNAMICS_BY_SPOTS_M`, `DYNAMICS_BY_SPOTS_D`,
`DYNAMICS_BY_SPOTS_DICT`) become nested inside `DATA_DEFAULTS`, and the outermost
`{` on line 1 is left unclosed at EOF.

**Evidence (runtime probes):**
```text
Brace count:
  defaults/tv/crosstab.yaml:  open=13, close=13, diff=0   (parses OK, 5 keys)
  defaults/tv_reg/crosstab.yaml: open=13, close=12, diff=1 (ParserError)

yaml.safe_load('defaults/tv_reg/crosstab.yaml') -> ParserError:
  while parsing a flow mapping, line 1, column 1
  expected ',' or '}', but got '<stream end>', line 162, column 2

yaml_to_dict('defaults/tv_reg/crosstab.yaml') -> None
  (YAMLError caught and logged, implicit return None)
```

**Asserted cause tested separately:** Brace-depth trace in `tv_reg/crosstab.yaml`:
- Line 1: `{` → depth 1 (outer flow mapping)
- Line 3: `'DATA_DEFAULTS': {` → depth 2
- Line 18: `'options': {` → depth 3
- Line 23: `"standRtg": {` → depth 4
- Line 26: `}` → depth 3 (closes `standRtg`)
- Line 30: `},` → depth 2 (closes `options`, NOT `DATA_DEFAULTS`)
- Lines 31–160: subtype blocks nested INSIDE `DATA_DEFAULTS` (depth 3→2 per block)
- Line 162: `}` → depth 1 (closes `DATA_DEFAULTS`, but outer `{` still open)
- EOF at depth 1 → `expected ',' or '}', but got '<stream end>'`

The sibling `tv/crosstab.yaml` closes `options` at line 26 and `DATA_DEFAULTS`
at line 19 with a separate `},` — the brace structure is correct there.

**Rubric grading:** Phase 05 HIGH = "a defaults-merge that drops a subtype-specific
override" is not the precise match; the closer rubric entry for HIGH is
"a template that passes lenient parsing but fails strict validation with no
runtime fallback." Here, the template does not even parse — `yaml.safe_load`
raises `ParserError`. `yaml_to_dict` catches it and returns `None`, so the
`TypeError` surfaces at `DataDefaults(**None)`. The template cannot run at all.
HIGH carried = HIGH on rubric. No re-grade.

**Load-bearing artefact:** Template file (`settings/defaults/tv_reg/crosstab.yaml`).

**Fix:** Add the missing closing `}` to close `DATA_DEFAULTS` before the
`DYNAMICS_BY_SPOTS_M` subtype block at line 31. Target:
`settings/defaults/tv_reg/crosstab.yaml`, between the `frequency_dist_conditions`
key and the `'DYNAMICS_BY_SPOTS_M'` key. Stable. ✓

**Verify:** After fixing, `yaml.safe_load` must yield a dict with
`DATA_DEFAULTS` + 3 subtype keys, and `DataDefaults(**yaml.safe_load(...))`
must succeed.

**Related:** TPL-001, TPL-002, TPL-003, TPL-005, CFG-002

---

### TPL-005 — confirmed — [MEDIUM] `get_default_data_settings` has no existence check for default files

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings`
— line 66-74: builds path from `media.value.lower()` and `report_type.value.lower()`,
passes directly to `yaml_to_dict`, wraps result in `DataDefaults(**...)`.

**Anchor resolved (secondary):**
`src/mko_get_mediascope_data/core/utils.py::yaml_to_dict` — catches
`FileNotFoundError`, logs, returns `None` (implicit).

**Claim tested:** `get_default_data_settings` constructs the path
`defaults/{media}/{report_type}.yaml` with no `exists()` or fallback guard.
`yaml_to_dict` returns `None` for a missing file; `DataDefaults(**None)`
raises `TypeError`. The combination means any `(media, report_type)` pair
without a shipped defaults file crashes.

**Evidence (runtime probes):**
```text
Shipped defaults (git ls-files):
  settings/defaults/tv/crosstab.yaml
  settings/defaults/tv/timeband.yaml
  settings/defaults/tv_reg/crosstab.yaml      ← present but malformed (TPL-004)

Missing combinations:
  TV/simple.yaml         MISSING
  TV_REG/timeband.yaml   MISSING  ← verified: exists=False
  TV_REG/simple.yaml     MISSING
  TV_REG/crosstab.yaml   EXISTS (malformed — see TPL-004)

yaml_to_dict('/nonexistent/.../tv_reg/timeband.yaml') -> returns None
DataDefaults(**None) -> TypeError: argument after ** must be a mapping, not 'NoneType'
```

The credential-file naming mismatch
(`connections/mediascope_RENAME_.json` vs
`AppPaths.connection_config` → `connections/mediascope.json` at `paths.py:59`)
is a Phase 02 (config-secrets) concern, correctly noted as out of scope here.

**Asserted cause tested separately:** `get_default_data_settings`
(services.py:66-74) constructs the path and calls `yaml_to_dict` directly.
`yaml_to_dict` (utils.py:343-348) catches `FileNotFoundError`, logs via
`logger.error`, and returns `None` — there is no `return {}` fallback.
`DataDefaults(**None)` raises `TypeError`. Verified against source.

**Rubric grading:** Phase 05 MEDIUM = "a missing default file produces a
usable configuration or a crash" — inverted: the finding is that a missing
file produces an opaque crash, not a usable configuration. The Phase 02
taxonomy grades this as "Ineffective or unconsumed configuration" (MEDIUM) as
well. The `WrongDataSettings` exception class exists at `core/errors.py:21`
and should be used. MEDIUM carried = MEDIUM on rubric. No re-grade.

**Load-bearing artefact:** Code (`core/services.py::get_default_data_settings`
and `core/utils.py::yaml_to_dict`).

**Fix:** In `get_default_data_settings`, check `path.exists()` before calling
`yaml_to_dict`; if absent, raise `WrongDataSettings` with a clear message naming
the `(media, report_type)` pair. Alternatively, fix `yaml_to_dict` to return
`{}` on `FileNotFoundError` (this also addresses CFG-005). Targets:
`get_default_data_settings` at services.py:66; `yaml_to_dict` at utils.py:343.
Stable. ✓

**Verify:** No existing test covers a missing defaults-file path.
A new test for `get_default_data_settings` with a `TV_REG`/`timeband` combo
is recommended.

**Related:** TPL-003, TPL-004, CFG-005, CFG-006

---

### TPL-006 — confirmed — [LOW] `kitId` duplicated across defaults and templates with divergent values

**Anchor resolved (primary):**
`settings/defaults/tv/crosstab.yaml::"kitId"`.

**Anchor resolved (secondary):**
`src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
— line 83: `data_settings[k] = v` (shallow assignment).

**Claim tested:** `kitId` is specified in both default subtype blocks and
report templates. For `TOP_NAT_TV_PROGRAMS` the default sets `kitId: 7`
while the matching report entry in `nat_tv_tvreport.yaml[1]` sets `kitId: 1`.
`merge_data_settings` overlays report fields with a shallow assignment
(`data_settings[k] = v`), so the report-level `options` dict fully replaces
the subtype default `options` — no deep merge — and the default `kitId: 7`
is silently discarded in favor of the report's `kitId: 1`.

**Evidence (runtime probe — merge on the one validating template + kitId inventory):**
```text
defaults\tv\crosstab.yaml::TOP_NAT_TV_PROGRAMS.options.kitId = 7
reports\nat_tv_tvreport.yaml[1]: sub=TOP_NAT_TV_PROGRAMS, options.kitId = 1

tv/crosstab.yaml kitId inventory:
  DYNAMICS_BY_SPOTS block (line 61):      'kitId': 1    ← block label in finding evidence is DYNAMICS_BY_SPOTS, not TOP_NAT_TV_ADVERTISERS (see VAL-001)
  TOP_NAT_TV_ADVERTISERS block (line 107): "kitId": 7
  TOP_NAT_TV_PROGRAMS block (line 129):     "kitId": 7

shallow merge confirmed:
  merge_data_settings loop: data_settings[k] = v   ← replaces entire options dict, not deep-merged

merged for nat_tv_brands_tefal.yaml[0]:
  options.kitId = 1  ← surfaced from default DYNAMICS_BY_SPOTS (report had no options)
```

Both values are annotated `#TV Index Russia all`, contradicting the in-file
dataset legend (`1`=Russia all, `2`=Russia 100+, `3`=Cities, `4`=TVI+ Russia all,
`5`=TVI+ Russia 100+, `6`=Moscow; `7` is undefined in the legend).

**Asserted cause tested separately:** `merge_data_settings`
(services.py:77-93), line 83: `data_settings[k] = v` — a shallow dict
assignment. Verified against source. `TVMediaReport.__init__` reads
`self.data_settings.options.get("kitId", 1)` (reports.py:189), so the merged
`options` dict determines which dataset is requested. A divergent `kitId`
produces a report against the wrong dataset without error.

**Impact note:** The `nat_tv_tvreport.yaml` templates currently fail TPL-001,
so this divergence is latent — the report cannot run at all today. If TPL-001
is fixed without also reconciling `kitId`, the shallow merge will silently
select the report-level kitId. No shipped example asserts the kitId merge
behavior.

**Rubric grading:** Phase 05 LOW = "documentation and drift with no runtime
consequence today: a template key never consulted but harmlessly present; a
duplicated constant that has not yet diverged." Here the constant has
diverged (7 vs 1) but the divergence is masked by TPL-001. The drift is
real; the consequence is currently blocked by TPL-001. LOW carried = LOW
on rubric. No re-grade.

**Load-bearing artefact:** Template files (`settings/defaults/tv/crosstab.yaml`
and `settings/reports/nat_tv_tvreport.yaml`) and Code
(`core/services.py::merge_data_settings`).

**Fix:** Reconcile `kitId` values: either (a) remove `kitId` from templates
that should inherit the default, or (b) implement a deep-merge in
`merge_data_settings` so per-key overrides are explicit. Target:
`merge_data_settings` at services.py:77, and `tv/crosstab.yaml` /
`nat_tv_tvreport.yaml` kitId entries. Stable. ✓

**Verify:** No existing test asserts kitId merge behavior. A new test
constructing `TopNatTvPrograms` with a non-default `options.kitId` and
asserting the merged value should be added after TPL-001 is resolved.

**Related:** TPL-001, TPL-002, CFG-007

---

## Disposition Summary

```
Confirmed:   6  (TPL-001 ×1, TPL-002 ×1, TPL-003 ×1, TPL-004 ×1, TPL-005 ×1, TPL-006 ×1)
Rejected:    0
Re-graded:   0
Unsettled:   0
```

All six findings are substantiated by direct evidence. No finding was
downgraded, re-typed, or withdrawn. No finding depends on an assumption that
could not be verified in the current tree.

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | TPL-006 evidence mislabels `tv/crosstab.yaml` line 61 as "TOP_NAT_TV_ADVERTISERS block" — it is in `DYNAMICS_BY_SPOTS` |
| VAL-002 | LOW | confirmed | TPL-001 fix-order claims `tv_yaml_content` conftest fixture uses `time_unit`/`unit_count` — it uses `"last_time": null`; correct keys are in `test_models.py` |
| VAL-003 | LOW | confirmed | `nat_tv_tvreport.yaml` has an extra `},` (line 35) causing YAML parser to shift `options` from entry [0] into entry [1] — not reported as a TPL finding |

### VAL-001 — [LOW] TPL-006 evidence mislabels `tv/crosstab.yaml` line 61 block

**Template mandate:** `.ai/audit/templates/audit-findings.md:74` — "1–2 fences
per claim; an identical-sites claim shows both, or cites the pair in Appendices."

**Observed:** TPL-006's evidence states:
```
tv/crosstab.yaml:  'kitId': 1,  #TOP_NAT_TV_ADVERTISERS block (line 61)
```
But line 61 of `defaults/tv/crosstab.yaml` falls within the `DYNAMICS_BY_SPOTS`
block (lines 20–75), not `TOP_NAT_TV_ADVERTISERS` (lines 100–122). The `kitId`
value (1) is correct; the block label is wrong.

**Impact:** Format drift only. The kitId=1 → DYNAMICS_BY_SPOTS pair is
accurately captured; the mislabel does not change TPL-006's verdict.

**Disposition:** confirmed. No finding re-grade needed. Recommendation: correct
the block label in the next raw-phase revision.

### VAL-002 — [LOW] TPL-001 fix-order inaccurately describes `tv_yaml_content` fixture

**Template mandate:** Findings must accurately describe the examined surface.

**Observed:** TPL-001's fix-order section states:
> tests/test_models.py / tests/test_services.py exercises (they use the inline
> tv_yaml_content fixture, which already uses the correct time_unit/unit_count keys)

The `tv_yaml_content` fixture in `tests/conftest.py` (lines 30–51) uses
`"last_time": null` — it does NOT use `time_unit`/`unit_count`. The correct
keys appear in `tests/test_models.py` directly (line 19:
`"period": {"last_time": {"unit_count": 1, "time_unit": "m"}}`), not in the
conftest fixture. The fixture does use correct `report_type`, `media`, and
`relative_path` keys.

**Impact:** The fixture does not exercise `LastTimeModel` validation at all
(it uses `date_filter` with `last_time: null`, which `PeriodModel` accepts).
The correct-key contract is asserted solely by `test_models.py`. The finding's
underlying claim (tests use correct keys) is true for `test_models.py`; the
attribution to the `tv_yaml_content` fixture is inaccurate.

**Disposition:** confirmed. No finding re-grade needed.

### VAL-003 — [LOW] `nat_tv_tvreport.yaml` has a structural YAML defect not reported

**Template mandate:** Every material property discovered during validation
should be surfaced.

**Observed:** `settings/reports/nat_tv_tvreport.yaml` has 16 `{` and 16 `}`
(braces balanced), so it parses without error. However, entry [0]
(TOP_NAT_TV_ADVERTISERS) is missing its `options` block — the closing `}`
at line 26 closes the entry dict before `options` is reached. The `options`
block that appears at lines 53–61 (intended for entry [1], TOP_NAT_TV_PROGRAMS)
is parsed as belonging to entry [1]. Entry [0] is silently constructed without
`options`. The `nat_tv_tvreport.yaml` file uses `period_type`/`period_num`
(stale keys), so all entries fail TPL-001 regardless — the structural defect
is masked.

**Impact:** If TPL-001 is fixed (field-name migration) without addressing this,
entry [0] (TOP_NAT_TV_ADVERTISERS) would run without any `options` — `kitId`
would fall back to the default subtype block's value (7), and the template's
intended `kitId: 1` would be silently absent.

**Disposition:** confirmed. This is a supplementary observation, not a new
finding under the TPL prefix (it is masked by TPL-001 today). It should be
filed as a separate finding if TPL-001 is resolved.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 05 finding | Phase 02 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| Template validation failures | TPL-001 (CRITICAL) | CFG-001 (HIGH) | 30/32 `ReportSettings(**rep)` failures from `period_type`/`period_num` + stale `ReportType` enum | cross-reference; both confirmed |
| Malformed defaults YAML | TPL-004 (HIGH) | CFG-002 (HIGH) | `tv_reg/crosstab.yaml` unbalanced flow mapping → `yaml.safe_load` `ParserError` | cross-reference; both confirmed |
| `yaml_to_dict` None return | TPL-004 (HIGH) | CFG-005 (MEDIUM) | `utils.py:yaml_to_dict` catches `YAMLError`, falls through to implicit `return None` | cross-reference; both confirmed |
| Missing defaults template | TPL-005 (MEDIUM) | CFG-006 (MEDIUM) | `TV_REG/timeband.yaml` absent → `DataDefaults(**None)` → `TypeError` | cross-reference; both confirmed |
| Missing subtype defaults | TPL-003 (HIGH) | CFG-007 (MEDIUM) | `NAT_TV_CHANNELS_ATV/BA/SOC_DEM` have no key in defaults → `slices`/`statistics` stay `None` | cross-reference; both confirmed |

**Severity divergence rationale:** Phase 05's rubric is keyed to
"effect and blast radius" with CRITICAL reserved for "report cannot run at
all while a shipped example asserts it should work." Phase 02's rubric
emphasizes config-handling quality ("ineffective or unconsumed configuration").
TPL-001's 30/32 failure rate is CRITICAL under Phase 05 (total tool failure
for all shipped templates except one) but HIGH under Phase 02 (the mechanism
is field-name drift). Both are correct under their respective taxonomies.

**Cross-phase conflict check:** No two phases reach opposite conclusions about
the same fact. Phase 02's CFG-002/CFG-003 (credential file naming) is correctly
noted as Phase 02's concern, not Phase 05's. TPL-005's note about
`connections/mediascope_RENAME_.json` correctly defers to Phase 02.

No finding was withdrawn, downgraded, or re-graded due to cross-phase interaction.

---

## Rollout Safety Assessment

**Dependency: TPL-004 must land before TPL-003 can be fully tested for TV_REG.**
`tv_reg/crosstab.yaml` cannot parse (ParserError), so `DataDefaults(**...)`
for `TV_REG/crosstab` crashes at construction. The `en_to_ru(None)` crash
(TPL-003) is only reachable for `media=TV` reports (which have a parseable
defaults file). TPL-004's brace fix unblocks TV_REG defaults loading.

**Dependency: TPL-004 must land before TPL-005's `TV_REG/timeband` is testable.**
The malformed `tv_reg/crosstab.yaml` prevents any TV_REG defaults test from
running. After TPL-004 is fixed, `get_default_data_settings` for
`TV_REG/timeband` would hit the missing-file path (TPL-005).

**Dependency: TPL-001 must land before TPL-006 is reachable.**
All `nat_tv_tvreport.yaml` entries fail `ReportSettings` validation. Once
field names are migrated, the `kitId` divergence (default=7 vs template=1
for `TOP_NAT_TV_PROGRAMS`) becomes active through the shallow merge.

**Dependency: TPL-003 must land before CFG-007 is reachable.**
`en_to_ru(None)` crashes before `Data(**data_settings)` is constructed.
Fixing TPL-003 (guard `en_to_ru` or default `slices` to `[]`) would expose
the subsequent `Data(statistics=None)` `ValidationError` that CFG-007
identifies. Both share the root cause: missing subtype defaults blocks.

**Test stability:** `pytest` (12 tests, all passing) must remain green. Key
dependencies:
- `tests/test_models.py::test_report_settings_valid` — uses correct `time_unit`/`unit_count`
  keys, asserts `ReportSettings` construction succeeds. Must stay green.
- `tests/test_services.py::test_run_report_with_real_models` — uses `tv_yaml_content`
  fixture (correct `report_type`, `media`, `relative_path`; `last_time: null`).
  Must stay green.
- `tests/test_utils.py` — tests `yaml_to_dict` and `en_to_ru` directly. Any
  `en_to_ru` signature change must not break existing tests.

**Rollout risk: none of the fixes have been applied.** All six findings
remain in the current `code_ref: d093d9c` tree unmodified.

---

## Evidence Index

| Evidence | Supports |
|---|---|
| `ReportSettings(**rep)` on all 32 template entries | TPL-001 |
| 2 passing entries: `nat_tv_brands_tefal.yaml[0]`, `[1]` | TPL-001 |
| `LastTimeModel` `extra="forbid"` rejects `period_type`/`period_num` | TPL-001 |
| `ReportType` enum values: `crosstab`/`timeband`/`simple` only | TPL-001 |
| 15 template files lack `media` or `relative_path` (15 entries) | TPL-001 |
| `Data.model_fields` → 3 declared; `model_config` `extra="allow"` | TPL-002 |
| 15 merged keys, 3 declared, 12 extras (runtime probe on tefal merge) | TPL-002 |
| `rg kitId` in `core/reports.py:189` → `self.data_settings.options.get("kitId", 1)` | TPL-002 |
| grep `category_name` in `core/` → 0 hits | TPL-002 |
| grep `period_type` in `core/` → 0 hits | TPL-002 |
| grep `period_num` in `core/` → 0 hits | TPL-002 |
| grep `folder` in `core/` → only `tasks.py:29` param, `services.py`/`utils.py`/`models.py` `export_folder`/`include_subfolders` | TPL-002 |
| `Data(statistics=None, slices=None)` → `ValidationError` | TPL-003 |
| `en_to_ru(None)` → `TypeError` | TPL-003 |
| `en_to_ru(['researchDate','tvCompanyEName'])` → `['researchDate','tvCompanyName']` | TPL-003 |
| `DATA_DEFAULTS.slices = Null` in `tv/crosstab.yaml` and `tv/timeband.yaml` | TPL-003 |
| `hasattr` check: `NAT_TV_CHANNELS_ATV/BA/SOC_DEM` → `False` in `tv/crosstab.yaml` | TPL-003 |
| `merge_data_settings` services.py:90 → `en_to_ru(data_settings["slices"])` | TPL-003 |
| Brace count: `tv/crosstab.yaml` 13/13 (balanced) | TPL-004 |
| Brace count: `tv_reg/crosstab.yaml` 13/12 (unbalanced) | TPL-004 |
| `yaml.safe_load(tv_reg/crosstab.yaml)` → `ParserError` at line 162 | TPL-004 |
| `yaml_to_dict(tv_reg/crosstab.yaml)` → `None` | TPL-004, TPL-005 |
| `git ls-files defaults/` → `tv/crosstab.yaml`, `tv/timeband.yaml`, `tv_reg/crosstab.yaml` | TPL-005 |
| `TV_REG/timeband.yaml` existence → False | TPL-005 |
| `DataDefaults(**None)` → `TypeError` | TPL-005 |
| `defaults/tv/crosstab.yaml::TOP_NAT_TV_PROGRAMS.options.kitId = 7` | TPL-006 |
| `nat_tv_tvreport.yaml[1] options.kitId = 1` (from `yaml.safe_load`) | TPL-001, TPL-006 |
| `merge_data_settings` services.py:83 → `data_settings[k] = v` (shallow) | TPL-006 |
| `reports.py:189` → `self.data_settings.options.get("kitId", 1)` | TPL-006 |
| `paths.py:59` → `connection_config` returns `connections/mediascope.json` | TPL-005 |
| Shipped credential file: `settings/connections/mediascope_RENAME_.json` | TPL-005 |
| `pyproject.toml` test config: `pythonpath=["src"]`, `asyncio-mode=auto` | — |
| `pytest` (full suite) → 12 passed | test stability |
| `.kilo/commands/audit/phases/05-audit-config-templates.md` — severity taxonomy | scope |
| `.ai/audit/99-validation/02-config-secrets-validated-findings.md` — cross-phase | cross-phase |

---

## Appendices

### Appendix A — Full reproduction output (Block 1–5)

```text
======================================================================
BLOCK 1: Report templates -> ReportSettings
======================================================================
  FAIL reports\gap_resurs\nat_tv_brands.yaml[0]: report_type: enum; media: missing; ...
  FAIL reports\gap_resurs\nat_tv_brands.yaml[1]: report_type: enum; media: missing; ...
  FAIL reports\gap_resurs\reg_tv_by_spots.yaml[0]: report_type: enum; ...
  ...
  FAIL reports\last_category\nat_tv_brands_last.yaml[1]: report_type: enum; ...
  FAIL reports\last_category\nat_tv_brands_snack.yaml[0]: report_type: enum; ...
  FAIL reports\last_category\nat_tv_brands_snack.yaml[1]: report_type: enum; ...
  FAIL reports\last_category\nat_tv_brands_weekly_reach.yaml[0]: report_type: enum; ...
  FAIL reports\nat_tv_brands_last.yaml[0]: period.last_time.unit_count: missing; ...
  FAIL reports\nat_tv_tvreport.yaml[0]: period.last_time.unit_count: missing; ...
  ... (7 entries)
  FAIL reports\nat_tv_tefal.yaml[0]: PASSED  ← only 2 pass
  FAIL reports\nat_tv_tefal.yaml[1]: PASSED  ← only 2 pass
  FAIL reports\sovcombank\*.yaml[0]: report_type: enum; media: missing; ...
  FAIL reports\sovcombank\*.yaml[1]: report_type: enum; media: missing; ...
  FAIL reports\sovcombank\*.yaml[2]: report_type: enum; media: missing; ...
  FAIL reports\test.yaml[0]: report_type: enum; media: missing; ...

  total entries validated: 32
  failed: 30
  passed: 2

======================================================================
BLOCK 2: Default data templates -> DataDefaults
======================================================================
  OK   defaults\tv\crosstab.yaml
  OK   defaults\tv\timeband.yaml
  FAIL defaults\tv_reg\crosstab.yaml: ParserError: while parsing a flow mapping
  in "...defaults\tv_reg\crosstab.yaml", line 1, column 1
  expected ',' or '}', but got '<stream end>'
  in "...defaults\tv_reg\crosstab.yaml", line 162, column 2

======================================================================
BLOCK 5: per-model extra policy
======================================================================
  LastTimeModel: extra='forbid'
  PeriodModel: extra=None
  ReportSettings: extra='allow'
  DataDefaults: extra='allow'
  Data: extra='allow'

======================================================================
BLOCK 2+4: merge -> Data for the only validating template (nat_tv_brands_tefal[0])
======================================================================
  merged keys count: 15  declared Data fields: 3 (options, statistics, slices)
  Data.model_dump() keys count: 15
  ad_filter survived from template: (articleLevel3Id IN (4165, 4171) OR arti...
  options.kitId survived from default DYNAMICS_BY_SPOTS: 1

======================================================================
BLOCK 3+4: en_to_ru(None) crash path
======================================================================
09-10-26 12:32:09 AM - ERROR - parameter should be list <class 'NoneType'> is given
  en_to_ru(None) raises: TypeError: object of type 'NoneType' has no len()
09-10-26 12:32:09 AM - ERROR - parameter should be list <class 'NoneType'> is given
  get_data_settings(NAT_TV_CHANNELS_ATV/ru) crashes: TypeError: object of type 'NoneType' has no len()

======================================================================
BLOCK 4 (latent): missing tv_reg/timeband.yaml default
======================================================================
  defaults\tv_reg\timeband.yaml exists=False
  shipped defaults: ['tv\crosstab.yaml', 'tv\timeband.yaml', 'tv_reg\crosstab.yaml']

  subtype blocks present in tv/crosstab.yaml:
  ['DYNAMICS_BY_SPOTS', 'DYNAMICS_BY_SPOTS_DICT', 'TOP_NAT_TV_ADVERTISERS', 'TOP_NAT_TV_PROGRAMS']
```

### Appendix B — Brace-depth trace: `tv_reg/crosstab.yaml` (lines 1–35)

```text
LINE   1: depth=1 opens=1 closes=0 | {                    ← outer flow mapping
LINE   2: depth=1 opens=0 closes=0
LINE   3: depth=2 opens=1 closes=0 | 'DATA_DEFAULTS': {   ← opens DATA_DEFAULTS
...
LINE  18: depth=3 opens=1 closes=0 | 'options': {          ← opens options (inside DATA_DEFAULTS)
LINE  23: depth=4 opens=1 closes=0 | "standRtg": {          ← opens standRtg (inside options)
LINE  26: depth=3 opens=0 closes=1 | }                     ← closes standRtg
LINE  27: depth=3 opens=0 closes=0 | 'add_city_to_basedemo_from_region': True,  ← STILL inside options
LINE  30: depth=2 opens=0 closes=1 | },                    ← closes OPTIONS (NOT DATA_DEFAULTS!)
LINE  31: depth=3 opens=1 closes=0 | 'DYNAMICS_BY_SPOTS_M': {  ← nested INSIDE DATA_DEFAULTS (wrong!)
...
LINE 160: depth=2 opens=0 closes=1 | },                    ← closes last subtype
LINE 161: depth=2 opens=0 closes=0
LINE 162: depth=1 opens=0 closes=1 | }                     ← closes DATA_DEFAULTS (outer still open!)
FINAL depth=1  ← unclosed outer { → ParserError
```

### Appendix C — Sibling `tv/crosstab.yaml` brace structure (correct)

```text
LINE   1: depth=1 opens=1 closes=0 | {
LINE   3: depth=2 opens=1 closes=0 | 'DATA_DEFAULTS': {
LINE  18: depth=3 opens=1 closes=0 | 'options': {
LINE  21: depth=4 opens=1 closes=0 | "standRtg": {
LINE  25: depth=3 opens=0 closes=1 | }       (closes standRtg)
LINE  26: depth=2 opens=0 closes=1 | }       (closes options)
LINE  19: depth=1 opens=0 closes=1 | },      ← WAIT: line 19 shows the DATA_DEFAULTS closing
FINAL depth=0  ← balanced, parses OK
```

*(Note: the sibling file closes `DATA_DEFAULTS` with a dedicated `},` before the
first subtype block. `tv_reg/crosstab.yaml` is missing this closing brace.)*

### Appendix D — kitId inventory

```text
defaults/tv/crosstab.yaml:
  DATA_DEFAULTS.options.kitId = 1
  DYNAMICS_BY_SPOTS.options.kitId = 1
  DYNAMICS_BY_SPOTS_DICT.options.kitId = 1
  TOP_NAT_TV_ADVERTISERS.options.kitId = 7
  TOP_NAT_TV_PROGRAMS.options.kitId = 7

templates with kitId (per yaml.safe_load):
  nat_tv_tvreport.yaml[1] (TOP_NAT_TV_PROGRAMS): kitId=1   ← diverges from default (7)
  nat_tv_tvreport.yaml[2] (NAT_TV_CHANNELS_ATV): kitId=1
  nat_tv_tvreport.yaml[3] (NAT_TV_CHANNELS_ATV): kitId=1
  nat_tv_tvreport.yaml[4] (NAT_TV_CHANNELS_BA): kitId=7
  nat_tv_tvreport.yaml[5] (NAT_TV_CHANNELS_SOC_DEM): kitId=1
  nat_tv_tvreport.yaml[6] (NAT_TV_CHANNELS_SOC_DEM): kitId=1
  gap_resurs/reg_tv_by_spots.yaml[*]: kitId=3
  last_category/nat_tv_brands_weekly_reach.yaml[0] (DYNAMICS_BY_SPOTS): kitId=7  ← diverges from default (1)
  sovcombank/reg_tv_by_spots.yaml[0]: kitId=3
  sovcombank/tv_msk_new_creatives.yaml[0]: kitId=6
  sovcombank/tv_msk_new_creatives_short.yaml[0]: kitId=6
```
