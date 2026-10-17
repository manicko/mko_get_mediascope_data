---
phase: "05"
phase_name: "config-templates"
id_prefix: "TPL"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/05-audit-config-templates.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
validation_revision: "2026-10-14"
---

<!-- JSON5-MIGRATION TODO: This validated-findings doc references pre-migration YAML
  symbols (yaml_to_dict, *.yaml, yaml.safe_load) that no longer exist after the JSON5
  settings templates migration. Anchors need re-verification in a future audit pass;
  this does NOT block the implementation PR. Verdicts remain factually accurate as
  historical records of the code_ref (d093d9c). -->

# Audit Findings — Validation Report: Config Template Validation & Spec Conformance

## Summary

Six findings (1 CRITICAL, 2 HIGH, 1 MEDIUM, 1 LOW; one further finding, TPL-006, is rejected as a merge-semantics finding and re-typed into TPL-001 as LOW kitId drift). All six primary anchors
resolve to a real code symbol or YAML token, and all claims were tested by runtime probe or static inspection; TPL-006's central claim is rejected (see the re-validation note). **5 confirmed (one re-graded), 1 rejected as a merge-defect and re-typed into TPL-001, 0 unsettled.**

Two cross-phase overlaps with Phase 02 (config-secrets) are noted: TPL-001
shares a root cause with CFG-001 (same 30/32 failure), and TPL-004/TPL-005
cascade from the same `yaml_to_dict` error-path defect that CFG-005 identifies (mechanism stale at HEAD — see Current-Tree Accuracy Addendum).
Both are cross-references; no Phase 05 finding was downgraded because of a Phase
02 finding.

Three validation-level findings (VAL-001, VAL-002, VAL-003) are recorded
separately: a block-label mislabel in the TPL-006 kitId evidence (VAL-001, superseded by the 2026-10-14 revision), an inaccurate
claim about the `tv_yaml_content` conftest fixture using `time_unit`/`unit_count`,
and a previously-unreported YAML structural defect in `nat_tv_tvreport.yaml` that
distorts parsed `options` blocks.

> **Re-validation revision (2026-10-14, verified against code baseline `b04d177`; the current HEAD `8ed385f` is a docs-only commit on top of it, so every code line reference in this revision remains valid).** The project owner
> identified an architectural error in the original validation: TPL-006 treated
> the intended two-layer settings architecture as a merge defect. This revision
> re-adjudicates TPL-006 and TPL-002 against that architecture — the DEFAULT
> layer (`settings/defaults/**/*.yaml` → `DataDefaults`) is the broad base
> carrying the full parameter set plus per-subtype blocks; the CLIENT layer
> (`settings/reports/**/*.yaml` → `ReportSettings`) carries deltas; and
> `AppService.merge_data_settings` (`core/services.py:104-121`) overlays
> matching top-level keys wholesale — client values win where specified,
> default values remain where the client is silent. Architecture is primary:
> stale templates are repaired to conform, never the code or models. The
> architecture reference for this revision is
> [`docs/00-overview/settings-layers-architecture.md`](../../../docs/00-overview/settings-layers-architecture.md).

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
- `yaml.safe_load` on `defaults/tv/crosstab.yaml` → OK (5 keys) at `code_ref d093d9c`; `ParserError` at line 138 at HEAD (see Current-Tree Accuracy Addendum)
- `yaml.safe_load` on `defaults/tv_reg/crosstab.yaml` → `ParserError` at line 162
- Brace count: `tv/crosstab.yaml` 13 open / 13 close (balanced); `tv_reg/crosstab.yaml`
  13 open / 12 close (unbalanced, diff=1)
- `Data(statistics=None, slices=None)` → `ValidationError`
- `en_to_ru(None)` → `TypeError: object of type 'NoneType' has no len()`
- `en_to_ru(['researchDate', 'tvCompanyEName'])` → `['researchDate', 'tvCompanyName']`
- `Data(**merged)` for the one validating template (`nat_tv_brands_tefal.yaml[0]`) →
  15 keys, 3 declared (`options`, `statistics`, `slices`)
- `options.kitId` in subtype blocks of `tv/crosstab.yaml` → kitId=1 (DYNAMICS_BY_SPOTS L61,
  DYNAMICS_BY_SPOTS_DICT L89), kitId=7 (TOP_NAT_TV_PROGRAMS L129); `DATA_DEFAULTS.options`
  is `Null` (L17) and `TOP_NAT_TV_ADVERTISERS` declares no kitId (duplicate `issueType`
  at L107-108) — corrected in the 2026-10-14 revision
- `options.kitId` in templates → kitId=1 (TOP_NAT_TV_PROGRAMS, `nat_tv_tvreport.yaml[1]`,
  from `yaml.safe_load`), kitId=7 (DYNAMICS_BY_SPOTS, `last_category/nat_tv_brands_weekly_reach.yaml[0]`)
- `ReportSettings.model_dump()` for the only validating template (`nat_tv_brands_tefal.yaml[0]`) → no extras; `extra='allow'` absorbs template-defined API-parameter keys, which are forwarded into the API JSON by `build_task` (see revised TPL-002)
- `pytest` (full suite, 23 tests) → 23 passed

**Not verified:** Live Mediascope API calls (no credentials); behavior inside Jupyter
kernels (no kernel available). `en_to_ru` correct-function probe used a static list,
not a real Mediascope response.

**Tools:** `uv run python` (runtime probes), `yaml.safe_load` (YAML parsing),
`rg`/grep tool (source scan), `read` tool (source inspection), `pytest` (test suite).

## Conformance

**Validated against:** commit `d093d9c` (Phase 05 baseline)
**HEAD at re-validation:** `8ed385f` (docs-only commit on top of `e8fd1867`, which is code-identical to `b04d177`)
**Survival at HEAD:** 5 confirmed (one re-graded), 1 rejected/re-typed (TPL-006). All anchors resolve; TPL-001/TPL-004/TPL-005 mechanism notes updated at HEAD via the Current-Tree Accuracy Addendum.
**Correction type:** pytest count 12→23, HEAD `e8fd1867`→`8ed385f`.

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). All findings re-verified at HEAD by probe (2 of 32 bundled entries
validate; both crosstab DEFAULT templates raise `ParserError`; `tv/simple`,
`tv_reg/timeband`, `tv_reg/simple` are absent). TPL-003's section already
carries the two-path mechanism (`en_to_ru(None)` `TypeError` for
`data_lang: "ru"`; `Data(**merged)` `ValidationError` for `data_lang: "en"`),
re-probed at HEAD. Added: **DISC-07** — `defaults/tv/crosstab.yaml` parses
correctly at commit `3408747` and fails from commit `b04d177` (the i18n
translation commit) onward with `yaml.safe_load` `ParserError` at line 138 (the
`TOP_NAT_TV_PROGRAMS.slices` sequence is closed with `}` instead of `]`), so
the TV defaults outage is a regression introduced by that commit, not a
long-standing defect (probe: `.ai/tmp/doc_probe_git_defaults.py`). Mapping:
TPL-001/002/004/005 → D-06 / D-11 (repair all bundled templates; nothing
deleted; templates intentionally incomplete), gap G-05, work item W-01;
TPL-006 → D-20 (no `kitId` meaning asserted; the owner will supply the current
API mapping; DISC-11).

---

## Findings

| ID      | Disposition  | Severity   | Band (rubric) vs. Carried | Anchor resolves |
|---------|-------------|------------|--------------------------|-----------------|
| TPL-001 | confirmed   | CRITICAL   | CRITICAL vs. CRITICAL    | yes             |
| TPL-002 | confirmed   | LOW        | HIGH vs. LOW             | yes             |
| TPL-003 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| TPL-004 | confirmed   | HIGH       | HIGH vs. HIGH            | yes             |
| TPL-005 | confirmed   | MEDIUM     | MEDIUM vs. MEDIUM        | yes             |
| TPL-006 | re-typed (folded into TPL-001) | LOW (drift, masked by TPL-001) | — | yes\* |

\* TPL-006's kitId inventory was inaccurate and its central claim was
mis-framed: `DATA_DEFAULTS.options` is `Null` (no kitId), `TOP_NAT_TV_ADVERTISERS`
has no kitId (duplicate `issueType` at L107-108), and the shallow top-level
override in `merge_data_settings` is the intended architecture, not a defect.
A second divergence the original finding missed:
`last_category/nat_tv_brands_weekly_reach.yaml[0]` (DYNAMICS_BY_SPOTS) kitId=7
vs default kitId=1. TPL-006 is re-typed into TPL-001 as kitId-value
reconciliation; see the revised TPL-006 section and Appendix D.

---

### TPL-001 — confirmed — [CRITICAL] Bundled report templates fail `ReportSettings` validation

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings`
— line 88: `ReportSettings(**rep)` inside the generator loop.

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

Failure modes across 30 failing entries (reproduced at HEAD b04d177):
  report_type: enum                  (22 entries — stale TV_CROSSTAB / REG_TV_* values)
  media: missing                     (22 entries)
  relative_path: missing             (22 entries)
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
Reconcile `kitId` values in `options` blocks against the in-file legend (`defaults/tv/crosstab.yaml`: `TOP_NAT_TV_PROGRAMS` L129 kitId `7→1`; remove the duplicate `issueType` in `TOP_NAT_TV_ADVERTISERS` L107-108) and in `settings/reports/last_category/nat_tv_brands_weekly_reach.yaml` L31 (`7→1`) — this covers the TPL-006 residual. Every `options` block declared in a template must be complete (kitId + issueType + standRtg where the default block carries them). Targets: all 15 template files. Stable. ✓

**Verify:** `tests/test_models.py::test_report_settings_valid` uses correct
`time_unit`/`unit_count` keys and passes — must stay green.
`tests/test_services.py::test_run_report_with_real_models` uses the
`tv_yaml_content` fixture with correct keys and passes — must stay green.

**Related:** TPL-002, TPL-003, TPL-004, TPL-005, TPL-006, CFG-001

---

### TPL-002 — re-graded — [LOW] Typo'd API-parameter keys pass through `Data` unvalidated (pass-through itself is intended)

**Anchor resolved:** `src/mko_get_mediascope_data/core/models.py::Data`
— `model_config = ConfigDict(extra="allow")`; declares 3 fields:
`options`, `statistics`, `slices`.

**Claim re-framed:** The original finding claimed that 12 merged keys are
"consumed nowhere by Pydantic and stored as untyped `Any` extras". The first
half is inaccurate: all 12 extra keys (`ad_filter`, `basedemo_filter`, `break_filter`,
`company_filter`, `date_filter`, `daytype_filter`, `frequency_dist_conditions`,
`location_filter`, `program_filter`, `sortings`, `targetdemo_filter`,
`weekday_filter`) are **consumed downstream**.
The merged dict becomes `Data` (`services.py:122-125`), the task strategies
re-dump it (`reports.py:365`/`386`), it becomes `TVTask.settings` (`tasks.py:19`),
and `send_task` forwards it as `build_task(report_type, **task.settings)`
(`reports.py:258-259`). The vendor `MediaVortexTask.build_task` (library
`mediavortex/tasks.py:50`) reads each named parameter into the API JSON
(`statistics` L215, `ad_filter`→`adFilter` L230, `slices` L240, `options`
L248, `sortings` L244, `frequency_dist_conditions`→`frequencyDistConditions`
L268, and every `*_filter` L219-238). `extra="allow"` on `Data`/`DataDefaults`/`ReportSettings`
(`models.py:84`/`120`/`125`) is therefore the **intended pass-through
transport** for template-defined API parameters — the defaults layer defines
the full parameter set and the client layer overrides individual keys
(`merge_data_settings`, `services.py:104-121`) — not a silent absorber.

**Residual (real, LOW):** two validation gaps remain in the pass-through:
(a) a typo'd top-level key (e.g. `ad_filterr`) is silently dropped by
`build_task`, so the report runs without the intended filter and no error is
raised; (b) a typo'd key inside the untyped `options` dict (e.g. `kitid`
instead of `kitId`) is forwarded verbatim to the API, while
`TVMediaReport.__init__` reads `self.data_settings.options.get("kitId", 1)`
(`reports.py:189`) and silently falls back to kitId=1 (Russia all) — a report
against the wrong dataset without error.

**Genuinely-orphaned keys (ReportSettings level, never reach `Data`):**
`category_name`, `folder`, `period_type`, `period_num` appear in some stale
templates, are never placed into the merged `Data` dict and never forwarded.
Their presence is template staleness (TPL-001), not a `Data` defect.

**Evidence:** consumption trace above. Probe at HEAD `b04d177`: all 15 merged
`Data` keys for the only validating template (`nat_tv_brands_tefal.yaml[0]`)
are named parameters of `build_task` (verified `True`). The original claim
that `options`, `company_filter`, `category_name`, `folder` appear as extras in
`ReportSettings.model_dump()` is inaccurate for that template: it declares
none of them — its `options.kitId=1` is inherited from the `DYNAMICS_BY_SPOTS`
defaults block, not absorbed as a `ReportSettings` extra.

**Rubric grading:** Phase 05 HIGH = "`extra='allow'` silently absorbing a
typoed field name that the code never reads, so a misconfigured report runs
with defaults the operator did not intend." does not apply as originally scoped —
the keys are read and forwarded into the API request. The residual typo risk
belongs to the LOW band (drift with no current runtime consequence: typo'd
keys can only appear in templates that fail TPL-001 validation today).
HIGH carried → LOW on re-grade.

**Load-bearing artefact:** Templates (`settings/reports/**/*.yaml`,
`settings/defaults/**/*.yaml`) and the vendor boundary
(`MediaVortexTask.build_task`).

**Fix (template conformance — no code change):** keep `extra="allow"` as the
intended transport. Narrowing `Data` or `ReportSettings` to `extra="forbid"`
would break the two-layer overlay, because the client layer relies on
template-defined API-parameter keys (`options`, `slices`, `statistics`,
`*_filter`, …) being absorbed and forwarded. The remedy is template
conformance (TPL-001): parameter names in templates must exactly match the
API parameter names consumed by `build_task`. If typing is ever desired, add
the known API parameters as declared fields on `Data` rather than forbidding
extras.

**Verify:** `tests/test_utils.py` has no test asserting `extra="allow"`
behavior on `Data`; `tests/test_models.py` constructs adjacent models with
explicit fields. No test asserts the typo-propagation paths; a test
constructing `TopNatTvPrograms` with a typo'd `options` key and asserting no
silent `kitId` fallback is recommended after TPL-001 is resolved.

**Related:** TPL-001, TPL-006, CFG-007

---

### TPL-003 — confirmed — [HIGH] `en_to_ru(None)` crash when a report subtype has no defaults block

**Anchor resolved (primary):**
`src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
— line 118: `data_settings["slices"] = utils.en_to_ru(data_settings["slices"])`.

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

HEAD update (2026-10-15, re-probed): `yaml_to_dict` no longer returns None -
it raises (commit `706eed0` fixed the mechanism; see Phase 01 ENT-003), so the
current-tree failure for BOTH files is a raised `ParserError` from
`get_default_data_settings`, not a silent None. `defaults/tv/crosstab.yaml` also
fails at HEAD (see DISC-07 below).
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

**DISC-07 (added 2026-10-15, probe-verified):** that last sentence is stale at
HEAD. `defaults/tv/crosstab.yaml` parses correctly at commit `3408747` (5 keys:
`DATA_DEFAULTS`, `DYNAMICS_BY_SPOTS`, `DYNAMICS_BY_SPOTS_DICT`,
`TOP_NAT_TV_ADVERTISERS`, `TOP_NAT_TV_PROGRAMS`) and breaks from commit
`b04d177` (the i18n comment-translation commit) onward:
`yaml.safe_load` raises `ParserError` at line 138, where the
`TOP_NAT_TV_PROGRAMS.slices` sequence is closed with `}` instead of `]`.
Consequence at HEAD: **both** crosstab DEFAULT templates are malformed, so even
a fully valid crosstab report entry cannot run. Probe:
`.ai/tmp/doc_probe_git_defaults.py`. Mapping: gap G-05, work item W-01
(template repair).

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
(services.py:93-101) constructs the path and calls `yaml_to_dict` directly, with no existence check or fallback. At `code_ref d093d9c`, `yaml_to_dict` (utils.py:343-348) caught the error, logged via `logger.error`, and returned `None`, so `DataDefaults(**None)` raised `TypeError`; at HEAD `b04d177` `yaml_to_dict` (utils.py:343-367) raises `FileNotFoundError`/`ParserError` instead — the crash surface moved but the missing-file path is still an opaque error rather than `WrongDataSettings` (mechanism stale at HEAD; see Current-Tree Accuracy Addendum).

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
the `(media, report_type)` pair. The `yaml_to_dict` side of this fix is already superseded — at HEAD `b04d177` `yaml_to_dict` raises instead of returning `None` (utils.py:343-367) — so only the existence check remains. Targets: `get_default_data_settings` at services.py:93-101. Stable. ✓

**Verify:** No existing test covers a missing defaults-file path.
A new test for `get_default_data_settings` with a `TV_REG`/`timeband` combo
is recommended.

**Related:** TPL-003, TPL-004, CFG-005, CFG-006

---

### TPL-006 — re-typed (folded into TPL-001) — [LOW] `kitId` value/comment drift between defaults and templates

**Anchor resolved (primary):**
`settings/defaults/tv/crosstab.yaml::"kitId"` — L129 `TOP_NAT_TV_PROGRAMS` kitId=7.

**Anchor resolved (secondary):**
`src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
(L104-121) — L115 `data_settings[k] = v` (shallow top-level overlay, **by design**).

**Claim rejected:** TPL-006 frames the shallow per-key overlay in
`merge_data_settings` as a merge defect ("no deep merge; default kitId
silently discarded"). This contradicts the intended two-layer architecture:
the DEFAULT layer (`settings/defaults/{media}/{report_type}.yaml` →
`DataDefaults`) supplies the broad base — the full API-parameter set plus
per-subtype blocks — and the CLIENT layer (`settings/reports/**/*.yaml` →
`ReportSettings`) supplies deltas. `merge_data_settings` overlays matching
top-level keys wholesale: a client value replaces the default value for that
key; a key the client does not specify keeps its default. `extra="allow"` on
`Data`/`DataDefaults`/`ReportSettings` (`models.py:84`/`120`/`125`) is the
intended pass-through for template-defined API parameters — every such
parameter is consumed downstream by `MediaVortexTask.build_task` (library
`mediavortex/tasks.py:50`), which reads each named parameter into the API JSON
(`statistics` L215, `ad_filter`→`adFilter` L230, `slices` L240, `options`
L248, `sortings` L244, `frequency_dist_conditions`→`frequencyDistConditions`
L268, and every `*_filter` L219-238). Implementing a deep merge, or removing
`kitId` from templates, would change or bypass the intended contract.

**Real residual (template/default data, not merge):** `kitId` values and
comments are inconsistent with the in-file legend (`1`=Russia all … `6`=Moscow;
`7` is undefined):
- `TOP_NAT_TV_PROGRAMS` default kitId=7 (L129, commented "Russia all") is
  stale — `7` is undefined in the legend. `nat_tv_tvreport.yaml[1]` kitId=1
  (L54) is correct; the shallow override correctly selects it.
- `TOP_NAT_TV_ADVERTISERS` (defaults L100-122) has **no kitId** — a duplicate
  `issueType` key (L107 `PROGRAM`, L108 `AD`; the later value wins). The
  original "kitId: 7 here" claim was incorrect.
- `DATA_DEFAULTS.options` is `Null` (L17) — the original
  "DATA_DEFAULTS.options.kitId=1" claim was incorrect; kitId comes from the
  subtype blocks.
- Missed by the original finding: `last_category/nat_tv_brands_weekly_reach.yaml[0]`
  (DYNAMICS_BY_SPOTS) kitId=7 (L31) overrides the default kitId=1 (L61) — the
  wrong direction.

This divergence is masked today: all `nat_tv_tvreport.yaml` entries fail
TPL-001, so no divergent `kitId` reaches the API. Per the architecture-first
rule, stale templates and defaults are repaired, not the merge.

**Disposition:** REJECTED as a merge-semantics finding. The residual kitId
value/comment drift is LOW and **folded into TPL-001** (reconcile kitId to the
legend; remove the duplicate `issueType`; align comments). No code, model, or
merge change is warranted.

**Fix (template/default data only):** (a) `defaults/tv/crosstab.yaml` — align
kitId values to the legend and remove the duplicate `issueType` in
`TOP_NAT_TV_ADVERTISERS`; (b) every `reports/**` template that declares an
`options` block must supply a complete block (kitId + issueType + standRtg
where the default block carries them) — no shipped template relies on partial
keys inherited from the default. Targets: `settings/defaults/tv/crosstab.yaml`,
`settings/reports/nat_tv_tvreport.yaml`,
`settings/reports/last_category/nat_tv_brands_weekly_reach.yaml`.

**Verify:** No test asserts kitId merge behavior; the only validating template
(`nat_tv_brands_tefal.yaml[0]`) declares no `options` and inherits
`DYNAMICS_BY_SPOTS` kitId=1.

**Related:** folded into TPL-001; TPL-002 (the `extra="allow"` pass-through is
intended); CFG-007 (same kitId/None drift root cause).

---

## Disposition Summary

```
Confirmed:   5  (TPL-001, TPL-003, TPL-004, TPL-005 at carried severity; TPL-002 confirmed and re-graded HIGH -> LOW)
Rejected:    1  (TPL-006 as a merge-semantics finding — shallow top-level override is intended architecture)
Re-typed:    1  (TPL-006 residual — kitId value/comment drift, LOW, folded into TPL-001)
Re-graded:   1  (TPL-002 HIGH -> LOW — pass-through consumption verified via MediaVortexTask.build_task)
Unsettled:   0
```

Five findings are substantiated by direct evidence. TPL-002 is confirmed but re-graded (HIGH → LOW) after the downstream consumption trace. TPL-006 is rejected as a merge-semantics finding and re-typed into TPL-001: the shallow top-level override in `merge_data_settings` is the intended two-layer architecture, not a defect.

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | TPL-006 evidence mislabels `tv/crosstab.yaml` line 61 as "TOP_NAT_TV_ADVERTISERS block" — it is in `DYNAMICS_BY_SPOTS` |
| VAL-002 | LOW | confirmed | TPL-001 fix-order claims `tv_yaml_content` conftest fixture uses `time_unit`/`unit_count` — it uses `"last_time": null`; correct keys are in `test_models.py` |
| VAL-003 | LOW | confirmed | `nat_tv_tvreport.yaml` entry [0] (TOP_NAT_TV_ADVERTISERS) is closed at line 26 without an `options` block while the `options` block at lines 53-61 is parsed as entry [1]'s — not reported as a TPL finding (the original `},`-shift line reference no longer matches at HEAD; the observable state is unchanged) |

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
the block label in the next raw-phase revision. Superseded — the corrected kitId inventory appears in the revised TPL-006 section and Appendix D of the 2026-10-14 revision.

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
would fall back to `1` (the `.get` default in `reports.py:189`), not 7, because the `TOP_NAT_TV_ADVERTISERS` defaults block defines no `kitId` (it has a duplicate `issueType` key instead). The template's intended `kitId: 1` would be silently absent — though the resulting value coincides with the correct one.

**Disposition:** confirmed. This is a supplementary observation, not a new
finding under the TPL prefix (it is masked by TPL-001 today). It should be
filed as a separate finding if TPL-001 is resolved.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 05 finding | Phase 02 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| Template validation failures | TPL-001 (CRITICAL) | CFG-001 (HIGH) | 30/32 `ReportSettings(**rep)` failures from `period_type`/`period_num` + stale `ReportType` enum | cross-reference; both confirmed |
| Malformed defaults YAML | TPL-004 (HIGH) | CFG-002 (HIGH) | `tv_reg/crosstab.yaml` unbalanced flow mapping → `yaml.safe_load` `ParserError` | cross-reference; both confirmed |
| `yaml_to_dict` None return | TPL-004 (HIGH) | CFG-005 (MEDIUM) | `utils.py:yaml_to_dict` catches `YAMLError`, falls through to implicit `return None` | cross-reference; both confirmed (mechanism stale at HEAD b04d177 — see Current-Tree Accuracy Addendum) |
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

**Dependency: TPL-001's migration must include kitId reconciliation (covers TPL-006).**
Once field names are migrated, the previously-masked kitId divergences become
active through the shallow override. TPL-006's proposed *merge* change is NOT a
dependency — it is rejected. The migration step must assert that every
`options` block is complete and every kitId matches the legend (1..6; the
undefined value 7 removed or justified).

**Dependency: TPL-003 must land before CFG-007 is reachable.**
`en_to_ru(None)` crashes before `Data(**data_settings)` is constructed.
Fixing TPL-003 (guard `en_to_ru` or default `slices` to `[]`) would expose
the subsequent `Data(statistics=None)` `ValidationError` that CFG-007
identifies. Both share the root cause: missing subtype defaults blocks.

**Test stability:** `pytest` (23 tests, all passing) must remain green. Key
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

## Current-Tree Accuracy Addendum (HEAD `b04d177`)

The validated evidence above was pinned to `code_ref: d093d9c` (an ancestor of
the current HEAD `8ed385f`; `git merge-base --is-ancestor d093d9c HEAD` → true).
Throughout this revision, 'HEAD `b04d177`' denotes the commit at which the
re-validation probes were executed; the current HEAD `e8fd1867` is a docs-only
commit on top of `b04d177`, so all code line references remain valid. During the
2026-10-14 re-validation the following current-tree facts were established. They do not
change any finding's verdict; they correct the *mechanisms* some findings
cite:

1. **`yaml_to_dict` no longer returns `None`.** `core/utils.py:343-367` —
   signature `-> dict[str, Any]`, body `return yaml.safe_load(cfg) or {}`,
   `except YAMLError / FileNotFoundError: logger.error(...); raise`. Probe at
   `b04d177`: a missing file raises `FileNotFoundError`; a malformed file
   raises `ParserError`. The "falls through to implicit `return None`"
   mechanism cited by TPL-004, TPL-005 and by cross-phase findings CFG-005 /
   ENT-003 / QLT-005 does not reproduce; failures now surface as
   `FileNotFoundError`/`ParserError` from `get_default_data_settings`
   (`services.py:101`).
2. **`defaults/tv/crosstab.yaml` is malformed at HEAD (L138).** The
   `TOP_NAT_TV_PROGRAMS` `slices` block sequence is closed by `}` instead of
   `]`. `yaml.safe_load` raises `ParserError` at L138. Consequences: Appendix
   C's "parses OK" claim is false at HEAD; the only `TV` defaults file cannot
   load, so every TV-media report crashes at
   `DataDefaults(**utils.yaml_to_dict(...))` (`services.py:101`); and the
   original TPL-006 Block-2 merge probe (`options.kitId=1 from default
   `DYNAMICS_BY_SPOTS`) is not reproducible at HEAD. This is a sibling of
   TPL-004 (template defect — repair the YAML), not a TPL-006 matter.
3. Line references captured at `d093d9c` (`services.py:77-93`, `:83`, `:90`)
   correspond to `services.py:104-121`, `:115`, `:118` at HEAD.

## Evidence Index

| Evidence | Supports |
|---|---|
| `ReportSettings(**rep)` on all 32 template entries | TPL-001 |
| 2 passing entries: `nat_tv_brands_tefal.yaml[0]`, `[1]` | TPL-001 |
| `LastTimeModel` `extra="forbid"` rejects `period_type`/`period_num` | TPL-001 |
| `ReportType` enum values: `crosstab`/`timeband`/`simple` only | TPL-001 |
| 22 template entries across the 15 stale files lack `media` or `relative_path` | TPL-001 |
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
| `merge_data_settings` services.py:118 → `en_to_ru(data_settings["slices"])` | TPL-003 |
| Brace count: `tv/crosstab.yaml` 13/13 (balanced) | TPL-004 |
| Brace count: `tv_reg/crosstab.yaml` 13/12 (unbalanced) | TPL-004 |
| `yaml.safe_load(tv_reg/crosstab.yaml)` → `ParserError` at line 162 | TPL-004 |
| `yaml_to_dict(tv_reg/crosstab.yaml)` → `None` (mechanism stale at HEAD b04d177 — see Current-Tree Accuracy Addendum) | TPL-004, TPL-005 |
| `git ls-files defaults/` → `tv/crosstab.yaml`, `tv/timeband.yaml`, `tv_reg/crosstab.yaml` | TPL-005 |
| `TV_REG/timeband.yaml` existence → False | TPL-005 |
| `DataDefaults(**None)` → `TypeError` (mechanism stale at HEAD b04d177 — see Current-Tree Accuracy Addendum) | TPL-005 |
| `defaults/tv/crosstab.yaml::TOP_NAT_TV_PROGRAMS.options.kitId = 7` | TPL-006 |
| `nat_tv_tvreport.yaml[1] options.kitId = 1` (from `yaml.safe_load`) | TPL-001, TPL-006 |
| `merge_data_settings` services.py:115 → `data_settings[k] = v` (shallow, by-design override) | TPL-006 |
| `reports.py:189` → `self.data_settings.options.get("kitId", 1)` | TPL-006 |
| `paths.py:59` → `connection_config` returns `connections/mediascope.json` | TPL-005 |
| Shipped credential file: `settings/connections/mediascope_RENAME_.json` | TPL-005 |
| `pyproject.toml` test config: `pythonpath=["src"]`, `asyncio-mode=auto` | — |
| `pytest` (full suite) → 23 passed | test stability |
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
  OK   defaults\tv\crosstab.yaml   (at code_ref d093d9c; ParserError at L138 at HEAD — see Appendix C)
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

The sibling `defaults/tv/crosstab.yaml` has balanced braces (13 open / 13 close)
but is **structurally invalid**: the `TOP_NAT_TV_PROGRAMS.slices` flow sequence
(`'slices': [` at L132) is closed by `}` at L138 instead of `]` — the
closing brace terminates the `TOP_NAT_TV_PROGRAMS` mapping, leaving the
sequence unclosed.

**DATA_DEFAULTS.options** (L17): `'options': Null` — no kitId; kitId values
enter exclusively through the per-subtype blocks (`DYNAMICS_BY_SPOTS` L61
kitId=1, `DYNAMICS_BY_SPOTS_DICT` L89 kitId=1, `TOP_NAT_TV_PROGRAMS` L129
kitId=7).

```text
brace count:  open=13  close=13  (balanced)
bracket count: open=1   close=0   (unbalanced — [ at L132 never closed with ])
yaml.safe_load('defaults/tv/crosstab.yaml') → ParserError:
  while parsing a flow node, expected the node content, but found '}'
  at line 138, column 3
```

**Consequence:** `tv/crosstab.yaml` is the only `TV` defaults file; with it
malformed, every `media=TV` report crashes at
`DataDefaults(**utils.yaml_to_dict(data_default_settings_path))`
(`services.py:101`). The raw-phase "parses OK, 5 keys" claim (Block 2 output)
held at `code_ref d093d9c` but is false at HEAD `b04d177`. See the
**Current-Tree Accuracy Addendum** for full probe details.

### Appendix D — kitId inventory

```text
defaults/tv/crosstab.yaml:
  DATA_DEFAULTS.options: Null                    (no kitId — L17)
  DYNAMICS_BY_SPOTS.options.kitId = 1            (L61)
  DYNAMICS_BY_SPOTS_DICT.options.kitId = 1       (L89)
  TOP_NAT_TV_ADVERTISERS.options: NO kitId       (L107-108 duplicate issueType PROGRAM/AD)
  TOP_NAT_TV_PROGRAMS.options.kitId = 7          (L129)  <- legend-invalid (7 undefined; comment "Russia all" wrong)

templates with kitId (per yaml.safe_load):
  nat_tv_tvreport.yaml[1] (TOP_NAT_TV_PROGRAMS):      kitId=1   <- diverges from default (7); template is correct per legend
  nat_tv_tvreport.yaml[2] (NAT_TV_CHANNELS_ATV):     kitId=1
  nat_tv_tvreport.yaml[3] (NAT_TV_CHANNELS_ATV):     kitId=1
  nat_tv_tvreport.yaml[4] (NAT_TV_CHANNELS_BA):      kitId=7
  nat_tv_tvreport.yaml[5] (NAT_TV_CHANNELS_SOC_DEM): kitId=1
  nat_tv_tvreport.yaml[6] (NAT_TV_CHANNELS_SOC_DEM): kitId=1
  gap_resurs/reg_tv_by_spots.yaml[0]:                kitId=3
  last_category/nat_tv_brands_weekly_reach.yaml[0] (DYNAMICS_BY_SPOTS): kitId=7   <- diverges from default (1); template is STALE (7 undefined)
  sovcombank/reg_tv_by_spots.yaml[0]:                kitId=3
  sovcombank/tv_msk_new_creatives.yaml[0]:           kitId=6
  sovcombank/tv_msk_new_creatives_short.yaml[0]:     kitId=6
```
