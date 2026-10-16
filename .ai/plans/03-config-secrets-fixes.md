---
plan_id: "03-config-secrets-fixes"
source_audit: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_code_context: ".ai/tmp/code_context_config_secrets.md"
source_code_ref: "b98d31e"
validation_report_head_ref: "8ed385f"
phase: "02-config-secrets"
status: "draft"
agent_role: "planner"
findings_covered: ["CFG-001", "CFG-003", "CFG-004", "CFG-006", "CFG-007", "CFG-008", "CFG-009", "VAL-001", "VAL-002"]
findings_rejected: ["CFG-002", "CFG-005", "CFG-010"]
---

# Execution Plan — Config & Secrets Remediation (CFG-001 through CFG-009, VAL-001–002)

## 1. Plan Overview and Scope

### Purpose

Decompose the validated config-secrets audit findings into dependency-aware
execution blocks with concrete, testable changes. Each block maps to atomic
Implementor tasks using semantic code anchors (symbols, modules, template
files — never line numbers).

### HEAD State Summary

The validator re-validated findings against actual HEAD `b98d31e` (the
validation report's referenced HEAD `8ed385f` is 8 commits behind). All seven
phase-01 entry-architecture findings are already remediated at HEAD (see
`.ai/plans/01-entry-architecture-fixes.md`). The config-secrets phase has:

- **3 findings REJECTED (falsified at actual HEAD):**
  - CFG-002 — malformed YAML in `tv_reg/crosstab.yaml` was fixed in commit `664e8f3`; braces now balanced (13/13), `yaml.safe_load` succeeds.
  - CFG-005 — `yaml_to_dict` already raises `FileNotFoundError`/`YAMLError` instead of returning `None`.
  - CFG-010 — no active `print()` calls remain in `core/` (replaced by `logger.info()` in `606c899`).

- **9 findings CONFIRMED and requiring action:**
  - CFG-001 (HIGH), CFG-003 (HIGH), CFG-004 (MEDIUM), CFG-006 (LOW),
    CFG-007 (MEDIUM), CFG-008 (LOW), CFG-009 (LOW), VAL-001 (LOW), VAL-002 (LOW)

- **Residual issues** (discovered by auditor, no finding ID):
  - Dead `None`-guards in `AppService.app_config` (`core/services.py:40-44, 48-52`) — unreachable since `yaml_to_dict` never returns `None`.
  - `WrongDataSettings` defined in `core/errors.py:5` but never imported anywhere.
  - Validation report cites non-existent commit `63bb1c0` (RISK-3).

### In Scope

| Finding | Files / Symbols |
|---------|----------------|
| CFG-001 | `settings/reports/**/*.yaml` (15 files, 32 entries; 30 fail) |
| CFG-003 | `settings/connections/mediascope_RENAME_.json`, `core/paths.py::AppPaths.connection_config` |
| CFG-004 | `core/services.py::AppService.get_connection` |
| CFG-006 | `settings/defaults/{tv, tv_reg}/{crosstab, timeband, simple}.yaml` — 3 missing templates |
| CFG-007 | `settings/defaults/tv/crosstab.yaml`, `settings/defaults/tv_reg/crosstab.yaml`, `core/services.py::merge_data_settings`, `core/models.py::Data` |
| CFG-008 | `core/init_service.py::_copy_templates` |
| CFG-009 | `pyproject.toml::project.dependencies`, `core/network.py::NetworkClient.__init__` |
| VAL-001 | `.ai/audit/99-validation/02-config-secrets-validated-findings.md` (Scope section) |
| VAL-002 | `.ai/audit/99-validation/02-config-secrets-validated-findings.md` (CFG-002 anchor) |
| Dead code | `core/services.py::AppService.app_config` (None-guards), `core/errors.py::WrongDataSettings` |
| RISK-3 | `.ai/audit/99-validation/02-config-secrets-validated-findings.md` (commit `63bb1c0` citation) |

### Out of Scope

- Phase-01 findings (all remediated at HEAD)
- Pipeline/production-code bugs (PIPE-001 through PIPE-011, owned by Phase 04)
- Template-format conformance gaps (TPL-001 through TPL-006, owned by Phase 05)
- Live API integration (no credentials available)
- Any changes to phase-01 or pre-existing audit phase files (raw findings in `.ai/audit/02-config-secrets/`)

### Key Constraints

1. **One Implementor at a time** — `.kilo/commands/../commands.md` mandates max 1
   implementor agent. Blocks execute sequentially.
2. **No background agents** — agents must not be launched in background.
3. **Semantic targeting** — all edits target symbols/functions/classes/template files,
   never line numbers.
4. **Production code is king** — if tests conflict with architecture, fix/remove tests;
   never distort production code for tests.
5. **Pydantic v2** — all models use Pydantic v2; Enums for all fixed values.
6. **English-only** — all comments, logs, docstrings in English.
7. **Settings architecture** — `.ai/tmp/code_context_config_secrets.md` and
   `docs/00-overview/settings-layers-architecture.md` establish that template
   repairs are template-first: "repair the template, never the code" when a template
   lags behind the Pydantic models.

---

## 2. Dependency Map

```text
CFG-003 (credential rename)            [HIGH — settings/connections, paths.py]
  └── CFG-004 (credential guard)       [MEDIUM — services.py:get_connection]

CFG-001 (template field/enum repair)   [HIGH — 15 report template files]
  └── CFG-007 (subtype defaults + TV_REG stale keys)  [MEDIUM — defaults templates, services.py]

CFG-006 (missing defaults templates)   [LOW — 3 new template files]

CFG-008 (atomic template copy)         [LOW — init_service.py:_copy_templates]

CFG-009 (pydantic-settings)            [LOW — pyproject.toml, network.py]

Dead code cleanup (None-guards,          [residual — services.py, errors.py]
  WrongDataSettings)

VAL-001 / VAL-002 / RISK-3              [LOW — validation report doc-only]

Final validation                        [Validator — full regression]
```

### Critical ordering constraints

1. **CFG-001 before CFG-007** — Report templates must pass `ReportSettings(**rep)`
   validation before the data-settings merge chain (`get_default_data_settings` →
   `merge_data_settings` → `Data(**data_settings)`) can be exercised end-to-end.
   The validation report's Rollout Safety Assessment states this explicitly.

2. **CFG-002 already resolved at HEAD** — The malformed YAML in `tv_reg/crosstab.yaml`
   was fixed in commit `664e8f3` (braces balanced 13/13). CFG-006's missing
   `TV_REG/timeband` and `TV_REG/simple` templates can now be tested independently
   without the YAML parse failure blocking them.

3. **CFG-003 before CFG-004** — The credential guard in `get_connection` must
   check the correct filename. If CFG-003 renames the template to `mediascope.json`,
   CFG-004's guard checks `mediascope.json`. If CFG-003 adds a migration guard
   (checks both filenames), CFG-004 must coordinate.

4. **CFG-007 RISK-2 reaches post-CFG-001** — TV_REG report templates currently
   fail CFG-001 validation (stale `report_type` enum values) before reaching
   `merge_data_settings`. Once CFG-001 is fixed, TV_REG templates using
   `DYNAMICS_BY_SPOTS` as `report_subtype` will hit the stale-key-name defect
   (`DYNAMICS_BY_SPOTS_M` in `tv_reg/crosstab.yaml` → `hasattr` returns `False`).
   This is the latent RISK-2 that must be resolved as part of CFG-007.

---

## 3. Execution Blocks

### Block 1 — VAL-001/VAL-002/RISK-3: Validation Report Corrections (doc-level, LOW)

| Field | Value |
|-------|-------|
| **Block ID** | 1 |
| **Findings** | VAL-001, VAL-002, RISK-3 |
| **Files** | `.ai/audit/99-validation/02-config-secrets-validated-findings.md` |
| **Severity** | LOW |
| **blocked_by** | None |
| **Required agents** | Doc-specialist |
| **Goes direct to Implementor?** | No — documentation correction only |

#### Scope

**VAL-001** — The Scope section (line 48-56 of the validation report) states:
> "the vendored `mediascope_api` library (`core/net.py`, `core/utils.py`, `core/errors.py`)"

Three corrections needed:
1. `core/net.py` does not exist — the actual module is `core/network.py`
2. `core/utils.py` is the project's own utility module, not vendored
3. `core/errors.py` defines two project exception classes, not vendored code

The actual `mediascope_api` library is a pip-installed dependency
(`pyproject.toml:17`), imported as `from mediascope_api.mediavortex.tasks import MediaVortexTask`
(services.py:7, reports.py:9) and `from mediascope_api.core.errors import AuthorizationError, BadRequestError`
(network.py:10).

**VAL-002** — CFG-002's anchor `settings/defaults/tv_reg/crosstab.yaml::1` uses a
bare line number. Since CFG-002 is falsified at actual HEAD, this anchor is moot.
Add a re-validation note recording CFG-002 as falsified (following the same pattern
used for CFG-005 and CFG-010 in the validation report).

**RISK-3** — The validation report (line 625) cites commit `63bb1c0` migrating tests
to `get_app_service()`. This commit does not exist (`git log --all` shows no match).
The actual `test_services.py` uses the `local_app_service` fixture. Correct the
citation.

#### Implementation approach

Pure documentation correction — no code changes. Follow the pattern already
established in the validation report for CFG-005 and CFG-010 (re-validation revision
note + updated Evidence Index entries).

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Over-editing the validation report beyond the 3 corrections | Low | Only touch the Scope section, VAL-002 section, and the RISK-3 citation |
| The validation report's HEAD reference (`8ed385f`) is outdated | Medium | Add a note that actual HEAD is `b98d31e`, with the same `git diff` evidence already in the code_context |

#### Verification

- The Scope section names `core/network.py` (not `core/net.py`)
- VAL-001 section notes `mediascope_api` is a pip dependency, not vendored
- RISK-3 citation corrected to reference the actual test approach (`local_app_service` fixture)
- VAL-002 records CFG-002 as falsified at actual HEAD

---

### Block 2 — CFG-003: Credential File Name Alignment (HIGH)

| Field | Value |
|-------|-------|
| **Block ID** | 2 |
| **Finding** | CFG-003 — template `mediascope_RENAME_.json` vs code's `mediascope.json` |
| **Files** | `settings/connections/mediascope_RENAME_.json`, `core/paths.py::AppPaths.connection_config` |
| **Severity** | HIGH (credential context elevates config-name drift) |
| **blocked_by** | None |
| **Required agents** | **Researcher** (migration safety decision) |
| **Goes direct to Implementor?** | No — multiple viable approaches with operational risk |

#### Scope

The template `settings/connections/mediascope_RENAME_.json` is copied verbatim
by `_copy_templates` to the user's `connections/` dir. After `init`, the file
lands as `mediascope_RENAME_.json`. But `AppPaths.connection_config`
(paths.py:58-59) hardcodes `connections/mediascope.json`. The `_RENAME_` suffix
is never resolved, so the credential file is effectively invisible to the code.

#### Implementation paths (NOT PRE-SELECTED — technical uncertainty exists)

**Path A — Rename template (simplest):**
Rename `mediascope_RENAME_.json` → `mediascope.json` in the package settings.
After `init`, the file lands at `connections/mediascope.json` — matching the code.
- *Pros:* Zero code change; minimal; matches existing `_copy_templates` behavior.
- *Cons:* Users who already renamed the file to `mediascope.json` will not get
  the update (the existing-file guard at init_service.py:23-24 skips it); users
  who haven't renamed will lose the visual reminder to rename.

**Path B — Update code to resolve `_RENAME_` path:**
Add a property or fallback in `AppPaths` or `get_connection` that checks for
`mediascope_RENAME_.json` when `mediascope.json` is absent.
- *Pros:* Backward-compatible with existing user setups.
- *Cons:* Permanently carries the `_RENAME_` cruft; the template still ships
  with a confusing name.

**Path C — Migration guard (recommended by validation report §Rollout Safety):**
Keep the template renamed to `mediascope.json` (Path A), but add a migration
check in `get_connection` or `init_project` that detects the old
`mediascope_RENAME_.json` filename in the user dir, warns the user, and either
auto-renames or raises an actionable error.
- *Pros:* Safe rollout for existing users; clean template name going forward.
- *Cons:* Most code change; adds a migration path that must be maintained.

> **Planner note:** Path C is recommended by the validation report's Rollout Safety
> Assessment (§Rollout Safety, lines 614-619), which explicitly flags that "renaming
> the template may invalidate users who already manually renamed the file." However,
> Path A is operationally simpler. **This decision is deferred to the Researcher.**
> The Implementor task (T-CFG-003) will be blocked until the path is chosen.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Renaming the template breaks users who already renamed | High | Path C migration guard; warn on old filename |
| No test asserts post-init credential filename | Medium | Add `test_init_produces_readable_credential_file` (see Block 10) |
| `_copy_templates` (CFG-008) is non-atomic | Low | Address in Block 6; does not affect correctness |

#### Verification

- After `init`, `connections/mediascope.json` exists in the user settings dir
- `AppPaths.connection_config` resolves to a file that exists
- A test asserts the post-init credential filename matches what the code reads

---

### Block 3 — CFG-001: Repair All Report Templates (HIGH)

| Field | Value |
|-------|-------|
| **Block ID** | 3 |
| **Finding** | CFG-001 — 15 template files with stale field names and enum values |
| **Files** | `settings/reports/**/*.yaml` (15 files) |
| **Severity** | HIGH (100% of shipped templates fail `ReportSettings(**rep)`) |
| **blocked_by** | None |
| **Required agents** | None — goes **direct to Implementor** |
| **Goes direct to Implementor?** | Yes — unambiguous mechanical fix |

#### Scope

30 of 32 bundled report entries fail `ReportSettings(**rep)`. The reference
template `settings/reports/nat_tv_brands_tefal.yaml` (entries [0] and [1] both
PASS) defines the correct pattern. Every affected file needs the same transforms.

**Defect inventory (applied per-entry):**

| Defect | Old value | New value | Model rejection |
|--------|-----------|-----------|-----------------|
| `report_type` enum | `TV_CROSSTAB` | `crosstab` | `ReportType` enum (`core/models.py:44-47`) |
| `report_type` enum | `TV_DICT_CROSSTAB` | `crosstab` | Same enum |
| `report_type` enum | `REG_TV_CROSSTAB` | `crosstab` | Same enum |
| `report_type` enum | `REG_TV_DICT_CROSSTAB` | `crosstab` | Same enum |
| `period.last_time` field | `period_type` | `time_unit` | `LastTimeModel` has `extra="forbid"` (`models.py:62`) |
| `period.last_time` field | `period_num` | `unit_count` | Same model; `PositiveInt` type |
| `period.last_time.unit_count` | `'1'` (string) | `1` (int) | `PositiveInt` — string rejected |
| `period.last_time.unit_count` | `Null` (when last_time is null) | valid `LastTimeModel` | `last_time` is optional, but when present must validate |
| Missing field | no `media` key | `media: MediaType` | `ReportSettings.media` is required (`models.py:88`) |
| Missing field | no `relative_path` key | `relative_path: Path` | `ReportSettings.relative_path` is required (`models.py:93`) |
| Stale field | `folder` key | → `relative_path` (value preserved) | `ReportSettings` uses `relative_path`; `folder` absorbed as extra, never consumed by core |
| Stale field | `category_name` key | remove (absorbed as extra, never consumed) | Not used by `merge_data_settings` or downstream |

**Affected files (15 of 16 in `settings/reports/`; `nat_tv_brands_tefal.yaml` passes):**

| File | Entries | Defects |
|------|---------|---------|
| `gap_resurs/nat_tv_brands.yaml` | 2 | TV_CROSSTAB, TV_DICT_CROSSTAB; period_type/num; folder/category_name; no media |
| `gap_resurs/reg_tv_by_spots.yaml` | 3 | REG_TV_CROSSTAB ×2, REG_TV_DICT_CROSSTAB; period_type/num; folder/category_name; no media |
| `last_category/nat_tv_brands_last.yaml` | 2 | TV_CROSSTAB, TV_DICT_CROSSTAB; period_type/num; folder/category_name; no media |
| `last_category/nat_tv_brands_snack.yaml` | 2 | same pattern |
| `last_category/nat_tv_brands_weekly_reach.yaml` | 1 | TV_CROSSTAB; period_type/num; folder/category_name; no media |
| `nat_tv_brands_last.yaml` | 1 | already has `media`/`relative_path`/`crosstab`; only period_type/num needs fixing |
| `nat_tv_tvreport.yaml` | 6 | period_type/num; some entries already have media/relative_path; NAT_TV_CHANNELS_* subtypes (CFG-007 linkage) |
| `sovcombank/nat_tv_brands_finance.yaml` | 2 | same pattern |
| `sovcombank/nat_tv_brands_finance2.yaml` | 2 | same pattern |
| `sovcombank/nat_tv_by_spots.yaml` | 3 | same pattern |
| `sovcombank/nat_tv_finance_q.yaml` | 1 | same pattern; `slices: []` empty list |
| `sovcombank/reg_tv_by_spots.yaml` | 1 | REG_TV_CROSSTAB; period_type/num; folder/category_name; no media |
| `sovcombank/tv_msk_new_creatives.yaml` | 1 | TV_CROSSTAB; period_type/num; folder/category_name; no media |
| `sovcombank/tv_msk_new_creatives_short.yaml` | 1 | same pattern |
| `test.yaml` | 1 | TV_CROSSTAB; period_type/num; folder/category_name; no media; `ad_filter: Null` |

#### Template conformance rules (from `settings-layers-architecture.md:150-161`)

1. **Template-first:** repair the template to conform to the code, never the code to the template.
2. **CLIENT carries deltas:** client templates provide only the deltas they intend to override for keys that exist in `DATA_DEFAULTS`.
3. **`folder`/`category_name` removal:** these are legacy keys never consumed by `core/` (grep confirms zero references). They were silently absorbed by `extra="allow"` on `ReportSettings`. The `relative_path` value replaces `folder`; `category_name` is dropped.
4. **Reference:** `nat_tv_brands_tefal.yaml` is the canonical conformant template.

#### Implementation approach

This is a mechanical, template-first repair. No code changes. Each file gets:
1. `report_type` enum value updated to lowercase `crosstab`/`timeband`/`simple`
2. `last_time` sub-dict keys renamed: `period_type` → `time_unit`, `period_num` → `unit_count`
3. `period_num` value coerced from string to int (e.g., `'1'` → `1`)
4. `media` field added (if missing) — derived from the file path: TV templates get `media: TV`, TV_REG templates get `media: TV_REG`
5. `folder` → `relative_path` (value preserved); `category_name` removed

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| `period_num` as string `'1'` vs int `1` — Pydantic `PositiveInt` rejects strings | Medium | Coerce all `period_num` values to bare ints in the YAML |
| Some templates use `data_lang: 'ru'` — these exercise `en_to_ru` (CFG-007 path) | Medium | Fix CFG-007 (Block 5) before running these end-to-end |
| `target_audiences: { Null: Null }` — Pydantic `extra="allow"` absorbs it | Low | Leave as-is (absorbed, not consumed); focus on the required fields |
| `ad_filter: Null` — `ReportSettings.ad_filter` is `str \| None = None` | Low | `Null` is valid; no change needed |
| Renaming `folder` → `relative_path` changes the semantics of the value | Low | The value is the output sub-directory; behavior is identical |

#### Verification

- `ReportSettings(**rep)` passes for all 32 entries (2 already pass, 30 to be fixed)
- A new test `test_bundled_templates_validate` iterates all entries and asserts validity
- `yaml.safe_load` succeeds on all 15 modified files
- No behavioral change to the 2 passing entries in `nat_tv_brands_tefal.yaml`

---

### Block 4 — CFG-004: Credential Existence Guard (MEDIUM)

| Field | Value |
|-------|-------|
| **Block ID** | 4 |
| **Finding** | CFG-004 — `get_connection` passes credential path without existence check |
| **Files** | `core/services.py::AppService.get_connection` |
| **Severity** | MEDIUM |
| **blocked_by** | Block 2 (CFG-003) — must know the correct credential filename |
| **Required agents** | None — goes **direct to Implementor** |
| **Goes direct to Implementor?** | Yes — specific, narrow recommendation |

#### Scope

`AppService.get_connection` (services.py:127-135) passes
`self.app_paths.connection_config` directly to `MediaVortexTask` without checking
file existence. Placeholder values (`"you username"`, `"00000000-..."`) pass
through undetected. `NetworkClient.call` (network.py:88) catches
`FileNotFoundError` but this is library-level, not actionable from project code.

**Fix:** Add a pre-check in `get_connection` — verify the credential file exists
before passing it to `MediaVortexTask`. Raise a project-specific error if absent.

#### Implementation approach

```python
def get_connection(self, media_type: MediaType) -> MediaVortexTask:
    ...
    cred_path = self.app_paths.connection_config
    if not cred_path.exists():
        raise FileNotFoundError(
            f"Mediascope credentials not found at {cred_path}. "
            f"Run 'mko-get-mediascope init' to create the settings directory."
        )
    return connection_service(
        settings_filename=cred_path,
        check_version=False,
    )
```

The error message should guide the user to run `init` if they haven't initialized.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Raising `FileNotFoundError` duplicates what `NetworkClient.call` catches | Low | The project-level guard produces a clearer, actionable message before the MediaVortexTask constructor is even called |
| No test for this guard | Medium | Add `test_get_connection_raises_on_missing_credential` (see Block 10) |

#### Verification

- `get_connection` raises `FileNotFoundError` with an actionable message when
  the credential file is absent
- `get_connection` succeeds when the file exists (happy path preserved)
- No regression in `test_network.py` or `test_services.py` tests

---

### Block 5 — CFG-006: Create Missing Defaults Templates (LOW)

| Field | Value |
|-------|-------|
| **Block ID** | 5 |
| **Finding** | CFG-006 — `TV/simple`, `TV_REG/timeband`, `TV_REG/simple` templates missing |
| **Files** | `settings/defaults/tv/simple.yaml`, `settings/defaults/tv_reg/timeband.yaml`, `settings/defaults/tv_reg/simple.yaml` |
| **Severity** | LOW |
| **blocked_by** | None (CFG-002 YAML already fixed at HEAD) |
| **Required agents** | None — goes **direct to Implementor** |
| **Goes direct to Implementor?** | Yes — mechanical template creation |

#### Scope

`get_default_data_settings` (services.py:93-101) builds path
`defaults/{media.value.lower()}/{report_type.value.lower()}.yaml`. Missing files:

| Template | MediaType | ReportType |
|----------|-----------|-----------|
| `tv/simple.yaml` | TV | simple |
| `tv_reg/timeband.yaml` | TV_REG | timeband |
| `tv_reg/simple.yaml` | TV_REG | simple |

Currently `yaml_to_dict` raises `FileNotFoundError` for any of these combinations,
aborting the run.

#### Implementation approach

Create 3 new template files following the pattern of existing defaults templates
(`tv/crosstab.yaml`, `tv/timeband.yaml`, `tv_reg/crosstab.yaml`):
- `DATA_DEFAULTS` block with the API parameter keys (matching the pattern of the
  corresponding existing template)
- No subtype blocks needed for these — subtypes are overlaid per-subtype in the
  corresponding `report_type` template

For `tv/simple.yaml`: base it on `tv/timeband.yaml` (which has 12 keys including
`time_filter`), simplified for the "simple" report type. The `simple` type is a
flat statistics report without timeband slicing.

For `tv_reg/timeband.yaml` and `tv_reg/simple.yaml`: base them on `tv_reg/crosstab.yaml`'s
`DATA_DEFAULTS` structure (15 keys including `add_city_to_basedemo_from_region`).

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Template content for `simple` type is not well-documented | Medium | Use the existing `DATA_DEFAULTS` keys from the `crosstab`/`timeband` template as the base; the Mediascope API expects these parameters |
| No test for missing-template error path | Medium | Add `test_get_default_data_settings_missing_file` (see Block 10) |

#### Verification

- `yaml.safe_load` succeeds on all 3 new files
- `get_default_data_settings` resolves the path for `(TV, simple)`, `(TV_REG, timeband)`,
  `(TV_REG, simple)` without `FileNotFoundError`
- `DataDefaults(**yaml_to_dict(path))` succeeds for all 3

---

### Block 6 — CFG-007: Subtype Defaults Coverage + TV_REG Stale Keys (MEDIUM)

| Field | Value |
|-------|-------|
| **Block ID** | 6 |
| **Finding** | CFG-007 — 3 subtypes have no defaults key; TV_REG stale key names |
| **Files** | `settings/defaults/tv/crosstab.yaml`, `settings/defaults/tv_reg/crosstab.yaml`, `core/services.py::merge_data_settings`, `core/models.py::Data` |
| **Severity** | MEDIUM |
| **blocked_by** | Block 3 (CFG-001) — templates must pass validation before merge chain testing |
| **Required agents** | **Researcher** (design decision: data fix vs. code guard) |
| **Goes direct to Implementor?** | No — multiple viable approaches with different maintainability implications |

#### Scope

Two related defects (RISK-2 from the auditor's code_context):

**6a. Missing subtype blocks in `tv/crosstab.yaml`:**
`merge_data_settings` (services.py:110-111) checks
`hasattr(defaults, subtype.value)`. Three subtypes have no matching block:
- `NAT_TV_CHANNELS_ATV`
- `NAT_TV_CHANNELS_BA`
- `NAT_TV_CHANNELS_SOC_DEM`

When no block matches, `DATA_DEFAULTS.slices` stays `Null` (None in Python). For
`data_lang="ru"`, `en_to_ru(None)` (utils.py:203-211) raises `TypeError`
(services.py:118). For `data_lang="en"`, `Data(**data_settings)` raises
`ValidationError` — `options`/`statistics`/`slices` are `None` but typed as
`dict | list` with `default_factory=list` (factory only applies when key is absent,
not when explicitly `None`).

**6b. TV_REG stale key names in `tv_reg/crosstab.yaml`:**
The top-level keys are `DYNAMICS_BY_SPOTS_M` and `DYNAMICS_BY_SPOTS_D` (not
`DYNAMICS_BY_SPOTS` and `DYNAMICS_BY_SPOTS_DICT`). For a TV_REG report with
`report_subtype: DYNAMICS_BY_SPOTS`, `hasattr(defaults, "DYNAMICS_BY_SPOTS")`
returns `False` — the override is silently skipped. This is currently unreachable
because all TV_REG templates fail CFG-001 first, but becomes live once CFG-001
is fixed.

#### Implementation paths (NOT PRE-SELECTED — technical uncertainty exists)

**Path A — Root-cause data fix (add subtype blocks):**
Add `NAT_TV_CHANNELS_ATV`, `NAT_TV_CHANNELS_BA`, `NAT_TV_CHANNELS_SOC_DEM` blocks
to `tv/crosstab.yaml` with appropriate `slices`, `statistics`, and `options`.
Rename `DYNAMICS_BY_SPOTS_M` → `DYNAMICS_BY_SPOTS` and `DYNAMICS_BY_SPOTS_D` →
`DYNAMICS_BY_SPOTS_DICT` in `tv_reg/crosstab.yaml` (merging with the existing
`DYNAMICS_BY_SPOTS_DICT` block if needed).
- *Pros:* Root-cause fix; no code change; follows the template-first principle.
- *Cons:* Requires domain knowledge to determine correct `slices`/`statistics`/`options`
  for NAT_TV_CHANNELS_* subtypes; the `_M`/`_D` rename loses the month/day distinction.

**Path B — Defensive code guard (guard `Data` against `None`):**
Modify `Data` model to accept `None` and coerce it to `[]` (or modify
`merge_data_settings` to filter `None` values before constructing `Data`).
- *Pros:* Catches all `None`-propagation bugs at the model boundary.
- *Cons:* Masks the root cause (missing config); could hide future template defects;
  violates the template-first principle.

**Path C — Defensive code guard (guard `en_to_ru` against `None`):**
Add a type check at the top of `en_to_ru`: if `slices_en` is `None`, return `[]`.
- *Pros:* Minimal, narrow fix for the `data_lang="ru"` path.
- *Cons:* Only addresses one of the two-path failure; doesn't fix `data_lang="en"`
  path; masks the missing-subtype-block defect.

**Path D — Hybrid:** Add subtype blocks (Path A) AND add a defensive guard in
`merge_data_settings` or `Data` to treat `None` as `[]` — belt-and-suspenders.
- *Pros:* Root-cause fix + defensive safety net.
- *Cons:* More code change; the guard may give false confidence.

> **Planner note:** The validation report recommends "add subtype blocks to defaults,
> **or** guard `Data` against `None`." Path A is the template-first approach (matching
> `settings-layers-architecture.md:157`). However, the `_M`/`_D` → enum rename in
> Path A raises a question: `DYNAMICS_BY_SPOTS_M` and `DYNAMICS_BY_SPOTS_D` have
> different `slices` content (`researchMonth` vs `researchDate`). Renaming both
> to `DYNAMICS_BY_SPOTS` would collide with the already-present `DYNAMICS_BY_SPOTS`
> intent. **This ambiguity requires Researcher input.** The Implementor task
> (T-CFG-007) will be blocked until the approach is chosen.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| `DYNAMICS_BY_SPOTS_M`/`_D` have distinct content — renaming to enum values collides | High | Researcher must decide: merge into existing blocks, extend the enum, or add a mapping layer |
| `NAT_TV_CHANNELS_*` subtype blocks need domain-specific values | Medium | The report templates that use these subtypes (e.g. `nat_tv_tvreport.yaml`) specify their own `slices`/`statistics`/`options` — these could be used as the basis for DEFAULT blocks |
| `en_to_ru(None)` TypeError is opaque ("object of type 'NoneType' has no len()") | Medium | Add a project-specific error (WrongDataSettings from `core/errors.py:5-6`) to provide context |
| `WrongDataSettings` is defined but never imported | Low | Either import and use it (Path B/D), or document why it remains unused |

#### Verification

- `hasattr(defaults, subtype.value)` returns `True` for all 7 ReportSubtype values
- `Data(statistics=None)` no longer occurs (subtype blocks provide real values)
- `en_to_ru(None)` is no longer reached for these subtypes
- A test `test_data_defaults_covers_all_subtypes` iterates all subtypes and asserts
  non-None `slices`/`statistics` after merge

---

### Block 7 — CFG-008: Atomic Template Copy (LOW)

| Field | Value |
|-------|-------|
| **Block ID** | 7 |
| **Finding** | CFG-008 — `_copy_templates` uses `shutil.copy2`, non-atomic |
| **Files** | `core/init_service.py::_copy_templates` |
| **Severity** | LOW |
| **blocked_by** | None |
| **Required agents** | None — goes **direct to Implementor** |
| **Goes direct to Implementor?** | Yes — specific recommendation (temp-write + os.replace) |

#### Scope

`_copy_templates` (init_service.py:7-27) iterates `source_dir.rglob("*")` and
copies each file via `shutil.copy2(item, target_path)` (line 27). An interrupted
`init` leaves a partial config tree with no rollback.

#### Implementation approach

Replace the per-file `shutil.copy2` with an atomic write pattern:
1. Write to a temporary file in the same directory (`tempfile.NamedTemporaryFile`
   or a sibling temp file)
2. `os.replace(temp_path, target_path)` on success — atomic on POSIX and Windows

```python
import os
import tempfile

def _copy_file_atomic(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(target.parent), prefix=".tmp_")
    try:
        os.close(fd)
        shutil.copy2(source, tmp_path)
        os.replace(tmp_path, target)
    except Exception:
        os.unlink(tmp_path) if os.path.exists(tmp_path) else None
        raise
```

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| `os.replace` semantics differ across filesystems | Low | `tempfile.mkstemp(dir=target.parent)` ensures same filesystem |
| Existing tests assert on init behavior — none do | None | `test_app.py::test_cli_init` only checks `--help` output |
| Adding `tempfile` import to init_service | None | Standard library, no version constraint |

#### Verification

- `init_project()` produces the same file tree as before (byte-identical content)
- `ruff check src/mko_get_mediascope_data/core/init_service.py` passes
- `test_cli_init` still passes

---

### Block 8 — CFG-009: pydantic-settings Utilization (LOW)

| Field | Value |
|-------|-------|
| **Block ID** | 8 |
| **Finding** | CFG-009 — `pydantic-settings` declared but never imported |
| **Files** | `pyproject.toml:21`, `core/network.py::NetworkClient.__init__` |
| **Severity** | LOW |
| **blocked_by** | None |
| **Required agents** | **Researcher** (architectural decision) |
| **Goes direct to Implementor?** | No — genuine design choice with downstream impact |

#### Scope

`pydantic-settings>=2.12.0` is listed in `pyproject.toml:21` but never imported
in `src/`. `NetworkClient.__init__` (network.py:49-53) hardcodes
`max_concurrent_requests=5`, `sleep_time=2`, `max_attempts=10`. No `os.getenv()`
calls exist in `core/`.

#### Implementation paths (NOT PRE-SELECTED — architectural uncertainty exists)

**Path A — Implement `BaseSettings` for env overrides:**
Create a `NetworkSettings(BaseSettings)` model in a new or existing module,
with fields mapped to environment variables:
```
MEDIOSCOPE_MAX_CONCURRENT_REQUESTS (default 5)
MEDIOSCOPE_SLEEP_TIME (default 2)
MEDIOSCOPE_MAX_ATTEMPTS (default 10)
```
`NetworkClient.__init__` reads from the settings model.
- *Pros:* Additive; uses the already-declared dependency; enables runtime
  configurability for deployment (Airflow, Docker); future-proofs the config layer.
- *Cons:* Introduces a new pattern (`BaseSettings`) not used elsewhere in the project;
  adds a new settings-loading code path that must be tested; requires choosing
  env-var naming convention and defaults.

**Path B — Remove the dependency:**
Remove `pydantic-settings>=2.12.0` from `pyproject.toml:21`.
- *Pros:* Minimal; removes a dead dependency; no new code.
- *Cons:* Loses the option of env-based overrides; if any downstream consumer
  relies on `pydantic_settings` being transitively installed, removal could break them
  (unlikely but unverified); the validation report frames this as an "undocumented
  switch" that suggests the intent was to use it.

**Path C — Narrow use: credential path override only:**
Implement `BaseSettings` only for the credential path override (CFG-003/004
integration), not for NetworkClient params.
- *Pros:* Minimal scope; addresses the credential-handling concerns from CFG-003/004.
- *Cons:* Half-measure; doesn't utilize the full `pydantic-settings` capability;
  leaves NetworkClient params hardcoded.

> **Planner note:** Path A is the more architecturally sound choice (the dependency
> was declared with intent), but it introduces the only `BaseSettings` in the project.
> Path B is the leanest. Path C is a compromise. **The decision is deferred to the
> Researcher** — the Implementor task (T-CFG-009) depends on the chosen path.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Removing `pydantic-settings` breaks downstream transitive importers | Low | `pydantic` itself depends on `pydantic-settings`; removal from *this* project's deps only affects projects that import `mko_get_mediascope_data` and rely on `pydantic_settings` being installed transitively — unlikely but unverified |
| `BaseSettings` env vars read at import time can surprise in tests | Medium | Only read inside `NetworkClient.__init__` or a lazy factory, not at module scope |
| Naming convention for env vars is unspecified | Medium | Researcher to define: `MEDIOSCOPE_*` prefix is consistent with the project's domain |

#### Verification

- Path A: `NetworkClient()` with `MEDIOSCOPE_MAX_ATTEMPTS=3` env set → `_max_attempts == 3`
- Path A: Without env var → defaults preserved (5, 2, 10)
- Path B: `pydantic-settings` removed from `pyproject.toml`; `uv run pytest` still passes
- `ruff check` and `mypy` pass on any new code

---

### Block 9 — Dead Code Cleanup + WrongDataSettings (residual, LOW)

| Field | Value |
|-------|-------|
| **Block ID** | 9 |
| **Findings** | Residual: dead None-guards (CFG-005), unused `WrongDataSettings` |
| **Files** | `core/services.py::AppService.app_config`, `core/errors.py::WrongDataSettings` |
| **Severity** | LOW (residual cleanup) |
| **blocked_by** | None |
| **Required agents** | None — goes **direct to Implementor** |
| **Goes direct to Implementor?** | Yes — mechanical, well-scoped |

#### Scope

**9a. Dead None-guards** — Since `yaml_to_dict` (utils.py:343-367) always returns
`dict` (never `None`), the guards at `services.py:40-44` and `services.py:48-52`
are unreachable:

```python
# services.py:37-55 (current)
@cached_property
def app_config(self) -> AppConfig:
    logging_settings_dict = utils.yaml_to_dict(self.app_paths.log_config)
    if logging_settings_dict is None:          # ← DEAD CODE
        raise FileNotFoundError(...)
    ...
    app_settings_dict = utils.yaml_to_dict(self.app_paths.app_config)
    if app_settings_dict is None:              # ← DEAD CODE
        raise FileNotFoundError(...)
```

**9b. `WrongDataSettings`** — Defined in `core/errors.py:5-6` but never
imported anywhere in `src/`. It could be used in CFG-007's `merge_data_settings`
(error reporting for missing defaults) or left as-is.

#### Implementation approach

**9a. Remove dead guards** — Delete the two `if ... is None: raise FileNotFoundError(...)`
blocks in `AppService.app_config`. The `yaml_to_dict` function already raises
`FileNotFoundError`/`YAMLError` with logging; the callers get a clear error directly.

**9b. `WrongDataSettings` decision** — Two options:
- **Option A:** Leave `WrongDataSettings` defined and unused (it's a small class,
  2 lines). Document in the plan that it's intentionally retained for future use
  (e.g., CFG-007 error reporting if the Researcher chooses the defensive-guard path).
- **Option B:** If the Researcher (Block 6) chooses to use `WrongDataSettings` for
  CFG-007's missing-subtype error, import and raise it in `merge_data_settings`.

> **Planner note:** No decision needed yet for Block 9 alone. If Block 6 (CFG-007)
> chooses a defensive code guard, `WrongDataSettings` may be wired in. This block
> handles only the dead-code removal (9a), which is unambiguous.

#### Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Removing the guard means `yaml_to_dict` exceptions propagate directly | Low | `yaml_to_dict` already raises with logging — the guard was a second layer for a `None` that can't happen |
| `WrongDataSettings` remaining unused triggers a lints warning | None | `ruff` does not flag unused classes in `core/errors.py`; it's a public module-level symbol |

#### Verification

- AST scan or `ruff` finds no `is None` check on `yaml_to_dict` return values in
  `AppService.app_config`
- Existing tests (`test_app_config_is_cached`, `test_reload_config_*`) still pass
  — they use the `local_app_service` fixture with valid config files

---

## 4. Execution Sequence

```text
┌─────────────────────────────────────────────────────────────┐
│  BLOCK 1  (LOW, doc-only)                                   │
│  VAL-001/VAL-002/RISK-3: Correct validation report           │
│  Scope text, false commit citation                          │
│  Required: Doc-specialist                                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  BLOCK 2  (HIGH)  ──→  BLOCK 4  (MEDIUM)                    │
│  CFG-003: credential rename                    CFG-004:      │
│  Researcher decision needed                  cred guard    │
│  Required: Researcher → Implementor            Required:     │
│                                               Implementor    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  BLOCK 3  (HIGH)  ──→  BLOCK 6  (MEDIUM)                    │
│  CFG-001: repair 15 templates             CFG-007:          │
│  Implementor (direct)                  subtype defaults    │
│                                          Researcher decision │
├─────────────────────────────────────────────────────────────┤
│  BLOCK 5  (LOW)                                             │
│  CFG-006: 3 missing defaults templates                     │
│  Implementor (direct)                                       │
├─────────────────────────────────────────────────────────────┤
│  BLOCK 7  (LOW)                                             │
│  CFG-008: atomic template copy                              │
│  Implementor (direct)                                       │
├─────────────────────────────────────────────────────────────┤
│  BLOCK 8  (LOW)                                             │
│  CFG-009: pydantic-settings                                 │
│  Researcher decision → Implementor                          │
├─────────────────────────────────────────────────────────────┤
│  BLOCK 9  (LOW, residual)                                   │
│  Dead None-guards removal + WrongDataSettings decision     │
│  Implementor (direct)                                       │
├─────────────────────────────────────────────────────────────┤
│  BLOCK 10 (final)                                           │
│  Verification & regression — Validator                      │
└─────────────────────────────────────────────────────────────┘
```

### Rationale for ordering

1. **Block 1 first** — Pure documentation correction, no code changes, zero risk
   to implementation. Sets the validation report straight before implementation
   begins.

2. **Block 2 (CFG-003) before Block 4 (CFG-004)** — The credential guard must
   check the correct filename. If the template is renamed (Path A/C), the guard
   checks `mediascope.json`. This is the key dependency in the credential pair.

3. **Block 3 (CFG-001) before Block 6 (CFG-007)** — Templates must pass
   `ReportSettings(**rep)` validation before the data-settings merge chain can
   be exercised end-to-end. This is explicitly stated in the validation report's
   Rollout Safety Assessment.

4. **Blocks 5, 7, 9** — Independent of each other and of the credential/template
   pairs. Ordered for logical grouping (defaults → copy → settings → cleanup).

5. **Block 10 last** — Validator performs independent regression review.

6. **Single-implementor constraint** — All Implementor blocks execute sequentially.
   Researcher-decision blocks (2, 6, 8) must resolve before their corresponding
   Implementor tasks.

### Parallelizable work

Given the one-implementor constraint, all blocks are sequential. However, the
dependency graph shows these blocks are mutually independent and *could* run
in parallel if multiple implementors were available:
- Block 1 (doc) is fully independent
- Block 5 (CFG-006) is fully independent
- Block 7 (CFG-008) is fully independent
- Block 9 (dead code) is fully independent
- Block 8 (CFG-009) is independent (but needs Researcher first)

---

## 5. Implementation Path Alternatives Summary

The following blocks have genuine technical uncertainty and require Researcher
input before the Implementor can proceed:

| Block | Finding | Alternatives | Decision required |
|-------|---------|-------------|-------------------|
| 2 | CFG-003 | A: rename template; B: update code path; C: migration guard | Migration safety for existing users |
| 6 | CFG-007 | A: add subtype blocks; B: guard Data vs None; C: guard en_to_ru; D: hybrid | Data fix vs. code guard; _M/_D key rename strategy |
| 8 | CFG-009 | A: implement BaseSettings; B: remove dependency; C: credential path only | Architectural: introduce BaseSettings pattern or remove dep |

Blocks that go **direct to Implementor** (no uncertainty):
- Block 3 (CFG-001): mechanical template repair, reference template exists
- Block 4 (CFG-004): specific narrow recommendation, unambiguous
- Block 5 (CFG-006): create 3 template files following existing patterns
- Block 7 (CFG-008): specific recommendation, standard library pattern
- Block 9 (9a): remove dead code, no decision needed

---

## 6. Test Requirements

The validation report identified these missing tests (code_context §5.5):

| Finding | Test recommended | Test exists? | Task block |
|---------|-------------------|-------------|------------|
| CFG-001 | `test_bundled_templates_validate` | ❌ No | Block 3 |
| CFG-003 | `test_init_produces_readable_credential_file` | ❌ No | Block 2/4 |
| CFG-004 | `test_get_connection_raises_on_missing_credential` | ❌ No | Block 4 |
| CFG-006 | `test_get_default_data_settings_missing_file` | ❌ No | Block 5 |
| CFG-007 | `test_data_defaults_covers_all_subtypes` | ❌ No | Block 6 |
| CFG-008 | (static theory) | ❌ No | Block 7 |
| CFG-009 | (static) | ❌ No | Block 8 |

### Existing tests that constrain implementation

- `tests/test_services.py` — `local_app_service` fixture copies bundled defaults
  via `shutil.copytree` (line 139). CFG-008's atomic copy change must not break this.
- `tests/test_app.py::test_cli_init` — asserts `"Initialize" in result.stdout`
  (already English at HEAD; passes). CFG-003's template rename doesn't affect CLI help.
- `tests/test_services.py::test_run_report_with_real_models` (slow) — exercises
  the full pipeline with TV media, `DYNAMICS_BY_SPOTS` subtype. CFG-001's template
  repair for `nat_tv_tvreport.yaml` will add `NAT_TV_CHANNELS_*` subtypes that
  need CFG-007 defaults coverage.

### Test strategy

- **Inline verification** for simple changes (CFG-004, CFG-008): implementor
  runs existing + new tests before marking complete (per task template: simple tasks
  verify inline).
- **Verification task** for multi-block work (CFG-001 + CFG-007): separate
  verification task after both blocks complete.

---

## 7. Documentation Updates

Per `docs/00-overview/doc-maintenance-rules.md`, update docs for schema changes
and security changes. The following doc updates are warranted:

| Doc | Change needed | Trigger |
|-----|--------------|---------|
| `docs/00-overview/current-behavior.md` §Configuration (line 84) | Add note about `mediascope.json` credential filename (CFG-003) | Credential path change |
| `docs/00-overview/settings-layers-architecture.md` §Bundled DEFAULT files (line 55-59) | Add 3 new template entries (`tv/simple`, `tv_reg/timeband`, `tv_reg/simple`); note `tv_reg/crosstab.yaml` key names after CFG-007 fix | New templates + key rename |
| `docs/00-overview/current-behavior.md` §Inputs (line 195-209) | Update defaults template matrix to show 6/6 templates present | CFG-006 |

> **Note:** CFG-001 (template repairs) is a bug fix — `doc-maintenance-rules.md:34`
> says "Do NOT update for: bug fixes." So no doc update is needed for CFG-001.
> CFG-006 (new shipped config files) is a feature addition → doc update warranted.
> CFG-003 (credential filename) is a security-relevant change → doc update warranted.
> CFG-009 (if Path A: new env vars) is a configuration change → doc update warranted.

---

## 8. Block-to-Task Mapping

| Block | Task ID | Task title | Primary file(s) | Required agents | Direct to Implementor? |
|-------|---------|------------|-----------------|-----------------|------------------------|
| 1 | T-VAL-001 | Correct validation report scope + false commit citation | validation report (md) | Doc-specialist | No |
| 2 | T-CFG-003 | Fix credential file name (after Researcher decision) | settings/connections, paths.py | Researcher → Implementor | No (Researcher needed) |
| 3 | T-CFG-001 | Repair 15 report template files | settings/reports/**/*.yaml | Implementor | Yes |
| 3 | T-CFG-001-test | Add bundled template validation test | tests/test_models.py | Implementor | Yes |
| 4 | T-CFG-004 | Add credential existence guard in get_connection | core/services.py:AppService.get_connection | Implementor | Yes |
| 5 | T-CFG-006 | Create 3 missing defaults templates | settings/defaults/ | Implementor | Yes |
| 6 | T-CFG-007 | Add subtype defaults + fix TV_REG stale keys (after Researcher decision) | settings/defaults/, services.py, models.py | Researcher → Implementor | No (Researcher needed) |
| 7 | T-CFG-008 | Atomic template copy | core/init_service.py::_copy_templates | Implementor | Yes |
| 8 | T-CFG-009 | Resolve pydantic-settings (after Researcher decision) | pyproject.toml, network.py | Researcher → Implementor | No (Researcher needed) |
| 9 | T-DEADCODE | Remove dead None-guards + WrongDataSettings decision | core/services.py, core/errors.py | Implementor | Yes (for 9a) |
| — | V-REGRESSION | Final verification & regression | full suite | Validator | No |

---

## 9. Task Descriptions (YAML)

### T-CFG-001-repair-templates.yaml

```yaml
id: task_CFG_001_repair_templates

title: Repair all 15 report template files to conform to ReportSettings schema

priority: high

depends_on: []

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-001 — confirmed — [HIGH] Templates use stale field names"

description: >
  30 of 32 bundled report entries fail ReportSettings(**rep) validation.
  The reference template nat_tv_brands_tefal.yaml (2 entries, both PASS)
  defines the correct pattern. Apply the same field/enum corrections to all
  15 affected template files so all 32 entries validate.

goals:
  - All 32 bundled report entries pass ReportSettings(**rep)
  - Template-first repair: conform templates to models, never change the code
  - Preserve all existing data (ad_filter, target_audiences, slices, statistics, etc.)
  - Follow settings-layers-architecture.md conformance rules

files:
  - path: src/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.yaml
    targets:
      - type: yaml_file
    changes:
      - action: rename_key
        description: "period_type → time_unit in last_time block"
        target: "period.last_time.period_type"
      - action: rename_key
        description: "period_num → unit_count in last_time block"
        target: "period.last_time.period_num"
      - action: coerce_value
        description: "period_num string '1' → int 1 (PositiveInt)"
        target: "period.last_time.unit_count"

  - path: src/mko_get_mediascope_data/settings/reports/gap_resurs/nat_tv_brands.yaml
    targets:
      - type: yaml_file
    changes:
      - action: rename_key
        description: "TV_CROSSTAB → crosstab (report_type)"
        target: "report_type"
        old_value: "TV_CROSSTAB"
        new_value: "crosstab"
      - action: rename_key
        description: "TV_DICT_CROSSTAB → crosstab (report_type)"
        target: "report_type"
        old_value: "TV_DICT_CROSSTAB"
        new_value: "crosstab"
      - action: rename_key
        description: "period_type → time_unit"
        target: "period.last_time.period_type"
      - action: rename_key
        description: "period_num → unit_count"
        target: "period.last_time.period_num"
      - action: coerce_value
        description: "period_num string → int"
        target: "period.last_time.unit_count"
      - action: rename_key
        description: "folder → relative_path"
        target: "folder"
        new_key: "relative_path"
      - action: remove_key
        description: "category_name (not consumed by core, extra='allow' absorbs it)"
        target: "category_name"
      - action: add_field
        description: "media: TV (required by ReportSettings)"
        target: "media"
        value: "TV"

  # ... repeat for all 15 files with their specific defect patterns

  - path: src/mko_get_mediascope_data/settings/reports/sovcombank/nat_tv_by_spots.yaml
    targets:
      - type: yaml_file
    changes:
      - action: rename_key
        description: "TV_DICT_CROSSTAB → crosstab (report_type)"
        target: "report_type"
        old_value: "TV_DICT_CROSSTAB"
        new_value: "crosstab"
      # ... same transform set applies

  - path: src/mko_get_mediascope_data/settings/reports/sovcombank/reg_tv_by_spots.yaml
    targets:
      - type: yaml_file
    changes:
      - action: rename_key
        description: "REG_TV_CROSSTAB → crosstab (report_type)"
        target: "report_type"
        old_value: "REG_TV_CROSSTAB"
        new_value: "crosstab"
      - action: add_field
        description: "media: TV_REG (required by ReportSettings)"
        target: "media"
        value: "TV_REG"
      # ... same transform set applies

# ── Transform rules (applied uniformly to all 15 files) ──
constraints:
  - "Apply to ALL entries in ALL 15 template files (not just the first entry)"
  - "Transform 1: report_type values TV_CROSSTAB/TV_DICT_CROSSTAB/REG_TV_CROSSTAB/REG_TV_DICT_CROSSTAB → crosstab"
  - "Transform 2: last_time.period_type → last_time.time_unit"
  - "Transform 3: last_time.period_num → last_time.unit_count (coerce string to int)"
  - "Transform 4: Add media field if missing (TV for non-REG files, TV_REG for REG_ files)"
  - "Transform 5: Rename folder → relative_path (preserve value)"
  - "Transform 6: Remove category_name (absorbed as extra, never consumed by core)"
  - "Do NOT touch nat_tv_brands_tefal.yaml (already passes)"
  - "Do NOT change any data values (slices, statistics, options, company_filter, etc.)"
  - "Reference template: settings/reports/nat_tv_brands_tefal.yaml"

verification_steps:
  - command: "uv run pytest tests/test_models.py::test_bundled_templates_validate -v"
    expected: "1 passed"
  - command: "uv run ruff check src/mko_get_mediascope_data/settings/reports/"
    expected: "All checks passed!" (or N/A for YAML)
  - command: "python -c \"from yaml import safe_load; from pathlib import Path; ...validate all 32 entries...\""
    expected: "All 32 entries pass ReportSettings validation"

acceptance_criteria:
  - All 15 template files modified
  - All 32 entries pass ReportSettings(**rep) validation
  - nat_tv_brands_tefal.yaml unchanged (2 entries already pass)
  - All data values preserved (only field/enum names changed)
  - media field present on all entries
  - relative_path field present on all entries
```

### T-CFG-001-test.yaml

```yaml
id: task_CFG_001_test

title: Add test_bundled_templates_validate — assert all 32 entries pass ReportSettings

priority: high

depends_on:
  - task_CFG_001_repair_templates

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-001 — test coverage gap"

description: >
  No test asserts bundled template validity. Create a test that iterates all
  .yaml files in settings/reports/, loads each entry as ReportSettings,
  and fails if any entry raises ValidationError. This prevents future template
  drift from regressing CFG-001.

goals:
  - All 32 entries validated against ReportSettings
  - Test fails fast on any template drift
  - Test is hermetic (reads from package templates, not user dir)

files:
  - path: tests/test_models.py
    targets:
      - type: test_function
        name: test_bundled_templates_validate
    changes:
      - action: add_function
        description: >
          Iterates settings/reports/**/*.yaml, loads each entry, validates
          with ReportSettings(**entry). Asserts all 32 entries pass.

verification_steps:
  - command: "uv run pytest tests/test_models.py::test_bundled_templates_validate -v"
    expected: "1 passed"
  - command: "uv run pytest -m 'not slow' -v"
    expected: "All tests pass (existing + new)"

acceptance_criteria:
  - Test iterates all .yaml files in settings/reports/
  - All 32 entries pass ReportSettings validation
  - Test runs as part of the default (non-slow) suite
```

### T-CFG-003.yaml

```yaml
id: task_CFG_003_credential_filename

title: Fix credential file name mismatch (mediascope_RENAME_.json → mediascope.json)

priority: high

depends_on:
  - task_VAL_001_002_doc_corrections

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-003 — confirmed — [HIGH] Credential file name mismatch"
source_note: "BLOCKED: requires Researcher decision on Path A/B/C"

description: >
  The template settings/connections/mediascope_RENAME_.json is copied verbatim
  by _copy_templates, but AppPaths.connection_config (paths.py:58-59) reads
  connections/mediascope.json. After init, the expected file does not exist.

goals:
  - Credential file is readable after init
  - Existing users who renamed are not broken
  - Path is verifiable by test

files:
  # Depends on Researcher decision — Path A or C:
  - path: src/mko_get_mediascope_data/settings/connections/mediascope_RENAME_.json
    targets:
      - type: file
    changes:
      - action: rename
        description: >
          Rename template to mediascope.json (Path A/C).
          If Path C (migration guard) is chosen, also modify:
        condition: "Path A or C chosen by Researcher"

  # If Path B or C chosen, also modify:
  - path: src/mko_get_mediascope_data/core/paths.py
    targets:
      - type: property
        name: connection_config
    changes:
      - action: add_fallback
        description: >
          Add fallback to check both mediascope.json and mediascope_RENAME_.json
          (Path C)
        condition: "Path B or C chosen by Researcher"

  # If Path C chosen, also modify:
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: method
        name: get_connection
    changes:
      - action: add_migration_check
        description: >
          If mediascope.json absent but mediascope_RENAME_.json exists,
          log deprecation warning and use the _RENAME_ variant
        condition: "Path C chosen by Researcher"

  # Test:
  - path: tests/test_init.py
    targets:
      - type: test_function
        name: test_init_produces_readable_credential_file
    changes:
      - action: add_function
        description: >
          After init_project(), assert connections/mediascope.json exists
          in the user settings dir.

constraints:
  - "BLOCKED until Researcher selects Path A, B, or C for CFG-003"
  - "Path A: rename only — simplest, no code change"
  - "Path B: code fallback — backward compatible, carries cruft"
  - "Path C: rename + migration guard — safest, most code"
  - "If Path C: get_connection (CFG-004) should integrate with the migration check"

verification_steps:
  - command: "uv run pytest tests/ -k 'init' -v"
    expected: "test_init_produces_readable_credential_file passes"
  - command: "python -c \"from mko_get_mediascope_data.core.paths import AppPaths, PathResolver; ... assert connection_config resolves to a .json that exists after init\""
    expected: "No FileNotFoundError"

acceptance_criteria:
  - After init_project(), AppPaths.connection_config resolves to an existing file
  - Path-specific criteria per Researcher decision (see constraints)
```

### T-CFG-004.yaml

```yaml
id: task_CFG_004_credential_guard

title: Add credential existence guard in AppService.get_connection

priority: medium

depends_on:
  - task_CFG_003_credential_filename

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-004 — confirmed — [MEDIUM] No credential guard before API connection"

description: >
  AppService.get_connection (services.py:127-135) passes the credential
  path directly to MediaVortexTask without checking existence or placeholder
  values. Add a pre-check that verifies the file exists and raises an
  actionable error if not.

goals:
  - Fail fast with a clear, actionable error when credentials are missing
  - Do not change the happy path
  - Error message guides the user to run 'mko-get-mediascope init'

files:
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: method
        name: get_connection
    changes:
      - action: add_guard
        description: >
          Add Path.exists() check on self.app_paths.connection_config before
          passing to MediaVortexTask. Raise FileNotFoundError with actionable
          message if the file is missing.
        code_hint: |
          cred_path = self.app_paths.connection_config
          if not cred_path.exists():
              raise FileNotFoundError(
                  f"Mediascope credentials not found at '{cred_path}'. "
                  f"Run 'mko-get-mediascope init' to create the settings directory."
              )

  - path: tests/test_services.py
    targets:
      - type: test_function
        name: test_get_connection_raises_on_missing_credential
    changes:
      - action: add_function
        description: >
          Assert that get_connection raises FileNotFoundError when the
          credential file does not exist.

verification_steps:
  - command: "uv run pytest tests/test_services.py::test_get_connection_raises_on_missing_credential -v"
    expected: "1 passed"
  - command: "uv run ruff check src/mko_get_mediascope_data/core/services.py"
    expected: "All checks passed!"

acceptance_criteria:
  - get_connection raises FileNotFoundError with actionable message when credential file is missing
  - get_connection still succeeds when credential file exists
  - No regression in existing tests
```

### T-CFG-006.yaml

```yaml
id: task_CFG_006_missing_defaults

title: Create 3 missing defaults templates (TV/simple, TV_REG/timeband, TV_REG/simple)

priority: medium

depends_on: []

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-006 — confirmed (re-graded) — [LOW]"

description: >
  get_default_data_settings builds path defaults/{media}/{report_type}.yaml.
  Missing: TV/simple, TV_REG/timeband, TV_REG/simple. Missing files cause
  FileNotFoundError from yaml_to_dict. Create the 3 missing template files
  following the structure of existing defaults templates.

goals:
  - All 6 (media, report_type) combinations have defaults templates
  - New templates parse with yaml.safe_load
  - New templates load as DataDefaults(**dict) without error

files:
  - path: src/mko_get_mediascope_data/settings/defaults/tv/simple.yaml
    targets:
      - type: file
        name: new_defaults_template
    changes:
      - action: create
        description: >
          Create new YAML template following the pattern of tv/timeband.yaml
          (DATA_DEFAULTS with standard API parameter keys). The 'simple' report
          type is a flat statistics report without timeband or crosstab slicing.

  - path: src/mko_get_mediascope_data/settings/defaults/tv_reg/timeband.yaml
    targets:
      - type: file
        name: new_defaults_template
    changes:
      - action: create
        description: >
          Create new YAML template following the pattern of tv/timeband.yaml
          but with TV_REG-specific defaults (kitId=3, add_city_to_basedemo_from_region=True,
          based on the tv_reg/crosstab.yaml DATA_DEFAULTS structure).

  - path: src/mko_get_mediascope_data/settings/defaults/tv_reg/simple.yaml
    targets:
      - type: file
        name: new_defaults_template
    changes:
      - action: create
        description: >
          Create new YAML template following the pattern of tv_reg/crosstab.yaml
          DATA_DEFAULTS (TV_REG-specific keys) adapted for the 'simple' report type.

  - path: tests/test_services.py
    targets:
      - type: test_function
        name: test_get_default_data_settings_missing_file
    changes:
      - action: add_function
        description: >
          Assert that get_default_data_settings no longer raises FileNotFoundError
          for (TV, simple), (TV_REG, timeband), (TV_REG, simple).

verification_steps:
  - command: "python -c \"import yaml; from pathlib import Path; ... verify all 6 combos exist and parse\""
    expected: "6/6 templates parse OK; 6/6 DataDefaults load OK"
  - command: "uv run pytest tests/test_services.py -k 'default_data_settings' -v"
    expected: "1 passed"

acceptance_criteria:
  - 3 new template files created in settings/defaults/
  - All parse with yaml.safe_load (no ParserError)
  - All load as DataDefaults(**yaml_to_dict(path)) without error
  - get_default_data_settings resolves all 6 (media, type) combinations without FileNotFoundError
```

### T-CFG-007.yaml

```yaml
id: task_CFG_007_subtype_defaults

title: Add missing subtype defaults blocks + fix TV_REG stale key names

priority: medium

depends_on:
  - task_CFG_001_repair_templates

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-007 — confirmed — [MEDIUM] 3 ReportSubtype values have no matching defaults key"
source_note: "BLOCKED: requires Researcher decision on Path A/B/C/D"

description: >
  merge_data_settings (services.py:110-111) checks hasattr(defaults, subtype.value).
  Three subtypes (NAT_TV_CHANNELS_ATV, NAT_TV_CHANNELS_BA, NAT_TV_CHANNELS_SOC_DEM)
  have no defaults block in tv/crosstab.yaml → slices/stats/options stay None →
  Data(**data_settings) raises ValidationError. Additionally, tv_reg/crosstab.yaml
  uses stale key names DYNAMICS_BY_SPOTS_M/_D that don't match the ReportSubtype
  enum values.

goals:
  - All 7 ReportSubtype values have matching defaults keys
  - TV_REG stale key names resolved to match enum
  - No None propagation into Data model for covered subtypes

files:
  # Depends on Researcher decision:

  # Path A (data fix): add subtype blocks to defaults templates
  - path: src/mko_get_mediascope_data/settings/defaults/tv/crosstab.yaml
    targets:
      - type: yaml_file
    changes:
      - action: add_key
        description: >
          Add NAT_TV_CHANNELS_ATV, NAT_TV_CHANNELS_BA, NAT_TV_CHANNELS_SOC_DEM
          subtype blocks with appropriate slices/statistics/options
        condition: "Path A chosen"

  - path: src/mko_get_mediascope_data/settings/defaults/tv_reg/crosstab.yaml
    targets:
      - type: yaml_file
    changes:
      - action: rename_key
        description: "DYNAMICS_BY_SPOTS_M → DYNAMICS_BY_SPOTS"
        condition: "Path A chosen"
      - action: rename_key
        description: "DYNAMICS_BY_SPOTS_D → DYNAMICS_BY_SPOTS_DICT (merge with existing)"
        condition: "Path A chosen"

  # Path B/C (code guard): guard Data or en_to_ru against None
  - path: src/mko_get_mediascope_data/core/models.py
    targets:
      - type: class
        name: Data
    changes:
      - action: add_validator
        description: >
          Add model_validator that coerces None to [] for options/statistics/slices
        condition: "Path B chosen"

  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: method
        name: merge_data_settings
    changes:
      - action: add_guard
        description: >
          Guard slices before en_to_ru call: if None, skip translation or
          raise WrongDataSettings with context
        condition: "Path C chosen"

  # Test (all paths):
  - path: tests/test_models.py
    targets:
      - type: test_function
        name: test_data_defaults_covers_all_subtypes
    changes:
      - action: add_function
        description: >
          Iterate all 7 ReportSubtype values, load the corresponding defaults
          template, and assert hasattr(defaults, subtype.value) is True
          for subtypes that should have coverage.

constraints:
  - "BLOCKED until Researcher selects Path A (data fix), B (guard Data), C (guard en_to_ru), or D (hybrid)"
  - "If Path A: may extend ReportSubtype enum if _M/_D variants need preserving"
  - "If Path B or C: import and use WrongDataSettings from core/errors.py for better error context (ties to Block 9)"
  - "Must not break existing DataDefaults/ReportSubtype behavior"

verification_steps:
  - command: "uv run pytest tests/test_models.py::test_data_defaults_covers_all_subtypes -v"
    expected: "1 passed"
  - command: "python -c \"from mko_get_mediascope_data.core.models import ReportSubtype; ... assert all 7 subtypes have hasattr on loaded defaults\""
    expected: "7/7 True (after Path A fix)"

acceptance_criteria:
  - All 7 ReportSubtype values have matching defaults keys (hasattr=True)
  - TV_REG stale key names resolved to match enum values
  - No None propagation into Data model for subtypes with defaults
  - test_data_defaults_covers_all_subtypes passes
```

### T-CFG-008.yaml

```yaml
id: task_CFG_008_atomic_copy

title: Make _copy_templates atomic with temp-write + os.replace

priority: low

depends_on: []

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-008 — confirmed — [LOW] Non-atomic template copy"

description: >
  _copy_templates (init_service.py:7-27) uses shutil.copy2 per file with no
  temp-write + atomic-rename pattern. An interrupted init leaves a partial
  config tree. Replace with tempfile + os.replace.

goals:
  - Each file copy is atomic (temp file written, then os.replace'd)
  - Same end-state as before (byte-identical files)
  - Interrupted copy does not leave partial files

files:
  - path: src/mko_get_mediascope_data/core/init_service.py
    targets:
      - type: function
        name: _copy_templates
    changes:
      - action: replace_body
        description: >
          Replace shutil.copy2(item, target_path) with atomic write pattern:
          write to temp file in target parent dir, then os.replace.

verification_steps:
  - command: "uv run pytest tests/test_app.py::test_cli_init -v"
    expected: "1 passed (init still works)"
  - command: "uv run ruff check src/mko_get_mediascope_data/core/init_service.py"
    expected: "All checks passed!"
  - command: "python -c \"from mko_get_mediascope_data.core.init_service import _copy_templates; ... verify atomic write\""
    expected: "Files match original, no partial files on interruption"

acceptance_criteria:
  - _copy_templates uses tempfile + os.replace instead of shutil.copy2 for file copies
  - Directory creation still uses mkdir(parents=True, exist_ok=True) (not per-file)
  - Output file tree is byte-identical to before
  - test_cli_init passes
```

### T-CFG-009.yaml

```yaml
id: task_CFG_009_pydantic_settings

title: Resolve unused pydantic-settings dependency

priority: low

depends_on: []

source_reference: ".ai/audit/99-validation/02-config-secrets-validated-findings.md"
source_section: "CFG-009 — confirmed — [LOW] pydantic-settings declared but unused"
source_note: "BLOCKED: requires Researcher decision on Path A/B/C"

description: >
  pydantic-settings>=2.12.0 is declared in pyproject.toml:21 but never imported.
  NetworkClient.__init__ (network.py:49-53) hardcodes max_concurrent_requests=5,
  sleep_time=2, max_attempts=10. No os.getenv() calls exist in core/.

goals:
  - Resolve the dependency's purpose: either utilize it or remove it
  - Do not break existing NetworkClient behavior
  - If BaseSettings: enable env-var overrides for operational params

files:
  # Path A (implement BaseSettings):
  - path: src/mko_get_mediascope_data/core/network.py
    targets:
      - type: class
        name: NetworkClient
    changes:
      - action: add_settings
        description: >
          Add NetworkSettings(BaseSettings) model with env-var-mapped fields,
          inject into NetworkClient.__init__
        condition: "Path A chosen"

  - path: pyproject.toml
    targets:
      - type: table
        name: project.dependencies
    changes: []
      # pydantic-settings stays (already declared)

  # Path B (remove dependency):
  - path: pyproject.toml
    targets:
      - type: table
        name: project.dependencies
    changes:
      - action: remove_value
        description: "Remove pydantic-settings>=2.12.0 from dependencies"
        condition: "Path B chosen"

  # Test (Path A):
  - path: tests/test_network.py
    targets:
      - type: test_function
        name: test_network_client_reads_env_overrides
    changes:
      - action: add_function
        description: >
          With MEDIOSCOPE_MAX_ATTEMPTS=3 env set, NetworkClient()._max_attempts == 3
        condition: "Path A chosen"

constraints:
  - "BLOCKED until Researcher selects Path A (BaseSettings), B (remove dep), or C (credential only)"
  - "Path A: env var naming convention MEDIOSCOPE_* (Researcher to confirm)"
  - "Path B: verify no downstream consumer relies on pydantic_settings transitively"
  - "NetworkClient callers (services.py:156) pass no kwargs — default behavior must be preserved"

verification_steps:
  # Path A:
  - command: "MEDIOSCOPE_MAX_ATTEMPTS=3 uv run pytest tests/test_network.py::test_network_client_reads_env_overrides -v"
    expected: "1 passed"
    condition: "Path A chosen"
  # Path B:
  - command: "uv run ruff check src tests && uv run pytest tests/test_network.py -v"
    expected: "All pass"
    condition: "Path B chosen"

acceptance_criteria:
  - Path A: NetworkClient params are env-overridable via BaseSettings; defaults preserved
  - Path B: pydantic-settings removed from deps; all tests pass
  - No behavioral regression in existing NetworkClient tests
```

### T-DEADCODE.yaml

```yaml
id: task_DEADCODE_cleanup

title: Remove dead None-guards in AppService.app_config

priority: low

depends_on: []

source_reference: ".ai/tmp/code_context_config_secrets.md"
source_section: "Section 2 — Residual dead code from ENT-003/CFG-005 remediation"

description: >
  Since yaml_to_dict (utils.py:343-367) always returns dict (never None),
  the None-guards in AppService.app_config (services.py:40-44, 48-52) are
  unreachable dead code. Remove them.

goals:
  - Remove the two if ... is None: raise FileNotFoundError(...) blocks
  - No behavioral change (yaml_to_dict already raises on missing/malformed files)
  - No test regression

files:
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: property
        name: app_config
    changes:
      - action: remove_block
        description: >
          Remove the None-check guard for logging_settings_dict (lines 40-44)
          and app_settings_dict (lines 48-52). yaml_to_dict raises FileNotFoundError
          or YAMLError directly; the guards were for the old None-return behavior.

verification_steps:
  - command: "uv run pytest tests/test_services.py -k 'app_config' -v"
    expected: "2 passed (test_app_config_is_cached, test_reload_config_invalidates_cache)"
  - command: "uv run ruff check src/mko_get_mediascope_data/core/services.py"
    expected: "All checks passed!"
  - command: "grep -n 'is None' src/mko_get_mediascope_data/core/services.py"
    expected: "No None-guards on yaml_to_dict return values in app_config"

acceptance_criteria:
  - app_config no longer contains if ... is None guards
  - Existing app_config tests pass
  - yaml_to_dict errors propagate directly (FileNotFoundError at utils.py:365-367)
```

### V-REGRESSION.yaml

```yaml
id: verify_config_secrets_regression

title: Final verification — full test suite + lint + template validation

priority: high

type: verification

depends_on:
  - task_CFG_001_repair_templates
  - task_CFG_001_test
  - task_CFG_003_credential_filename
  - task_CFG_004_credential_guard
  - task_CFG_006_missing_defaults
  - task_CFG_007_subtype_defaults
  - task_CFG_008_atomic_copy
  - task_CFG_009_pydantic_settings
  - task_DEADCODE_cleanup

verifies:
  - task_CFG_001_repair_templates
  - task_CFG_001_test
  - task_CFG_003_credential_filename
  - task_CFG_004_credential_guard
  - task_CFG_006_missing_defaults
  - task_CFG_007_subtype_defaults
  - task_CFG_008_atomic_copy
  - task_CFG_009_pydantic_settings
  - task_DEADCODE_cleanup

verification_steps:
  - build: "N/A (no build step — uv sync)"
  - test: "uv run pytest -v"
    expected: "All tests pass (existing + new), 0 failures"
  - smoke_check: "uv run ruff check src tests"
    expected: "All checks passed!"
  - smoke_check: "python -c \"from mko_get_mediascope_data.core.models import ReportSettings; ... validate all 32 templates\""
    expected: "32/32 PASS"
  - smoke_check: "python -c \"# verify all 6 defaults templates exist and parse\""
    expected: "6/6 OK"
  - smoke_check: "python -c \"# verify connection_config resolves to existing file after init\""
    expected: "No FileNotFoundError"

pass_criteria:
  - All tests pass (existing 31 + new tests for CFG-001/004/006/007/009)
  - ruff check clean on src and tests
  - All 32 report templates validate against ReportSettings
  - All 6 defaults templates parse and load as DataDefaults
  - No dead None-guards remain in AppService.app_config
  - mypy introduces no NEW errors beyond the pre-existing 56-error baseline

failure_action: "Return failing tasks to Implementor for rework; Validator does not approve"

# ── Rollback (if needed) ──
rollback_notes: >
  Each block is atomic in git (one commit per block recommended). If any block
  fails verification, revert that block's commit and re-plan. The template files
  (CFG-001) are the highest-risk change — keep a git stash of the originals
  before editing.
```

---

## 10. Verification Matrix

| Finding | Block | Verification step | When to run |
|---------|-------|-------------------|-------------|
| CF-001 | 3 | `test_bundled_templates_validate` — all 32 entries pass | After Block 3 |
| CFG-002 | — | Already falsified at HEAD (fixed in `664e8f3`) | N/A |
| CFG-003 | 2 | `test_init_produces_readable_credential_file` — credential file exists after init | After Block 2 |
| CFG-004 | 4 | `test_get_connection_raises_on_missing_credential` — FileNotFoundError with actionable message | After Block 4 |
| CFG-005 | — | Already falsified at HEAD | N/A |
| CFG-006 | 5 | All 6 defaults templates exist and parse; no FileNotFoundError | After Block 5 |
| CFG-007 | 6 | `test_data_defaults_covers_all_subtypes` — 7/7 hasattr True | After Block 6 |
| CFG-008 | 7 | `_copy_templates` uses tempfile + os.replace; init still works | After Block 7 |
| CFG-009 | 8 | Path A: env override works; Path B: dep removed, tests pass | After Block 8 |
| CFG-010 | — | Already falsified at HEAD | N/A |
| VAL-001 | 1 | Scope text corrected in validation report | After Block 1 |
| Dead code | 9 | No None-guards on yaml_to_dict returns in app_config | After Block 9 |

### Regression check (after all blocks)

1. `uv run pytest -v` — all tests pass (existing 31 + new tests)
2. `uv run ruff check src tests` — no lint errors
3. `uv run mypy src tests` — no NEW errors beyond the 56-error pre-existing baseline
   (mypy is not a CI gate, but should not regress)
4. All 32 report templates validate against `ReportSettings(**rep)`
5. All 6 defaults templates parse and load as `DataDefaults(**dict)`
6. No files written to the real user config dir during tests (hermetic)
7. `from mko_get_medioscope_data import run_report` still works (ENT-004 preserved)
8. `import mko_get_mediascope_data` does not bootstrap AppService (ENT-002 preserved)

---

## 11. Risks

| Risk | Finding | Assessment | Mitigation |
|------|---------|-----------|------------|
| Template repair changes output semantics for users | CFG-001 | Low | `folder` → `relative_path` preserves the value; `category_name` was never consumed |
| CFG-003 rename breaks existing renamed users | CFG-003 | High | Path C migration guard (Researcher decision); warn on old filename |
| TV_REG stale keys (_M/_D) lose semantic distinction on rename | CFG-007 | High | Researcher must decide: merge, extend enum, or mapping layer |
| pydantic-settings removal breaks transitive importers | CFG-009 | Low | Path A (implement BaseSettings) avoids removal; Path B requires verification |
| Template repair + defaults changes interact in merge_data_settings | CFG-007 | Medium | Test the full merge chain after both Block 3 and Block 6 complete |
| Dead code removal exposes yaml_to_dict exceptions | Dead code | Low | yaml_to_dict already raises with logging; the guards were belt-and-suspenders for an impossible None |
| Worker hang (PIPE-002) masks template/defaults bugs | CFG-007 | Low | Not in scope — owned by Phase 04; tests use proper mock values |
| mypy baseline (56 errors) may grow with new code | All | Low | New code should be mypy-clean; baseline errors are pre-existing |

---

## 12. Agent Assignment Summary

| Block | Finding(s) | Complexity | Researcher needed? | Auditor needed? | Validator needed? | Direct to Implementor? |
|-------|-----------|------------|-------------------|-----------------|-------------------|----------------------|
| 1 | VAL-001, VAL-002, RISK-3 | Low | No | No | No (doc-only) | No — Doc-specialist |
| 2 | CFG-003 | Medium | **Yes** (Path A/B/C) | No | No | No |
| 3 | CFG-001 | High (mechanical) | No | No | No | **Yes** |
| 4 | CFG-004 | Low | No | No | No | **Yes** |
| 5 | CFG-006 | Low (mechanical) | No | No | No | **Yes** |
| 6 | CFG-007 | Medium | **Yes** (Path A/B/C/D) | No | No | No |
| 7 | CFG-008 | Low | No | No | No | **Yes** |
| 8 | CFG-009 | Low | **Yes** (Path A/B/C) | No | No | No |
| 9 | Dead code | Low | No | No | No | **Yes** (9a only) |
| 10 | Regression | — | No | No | **Yes** | No |

### Decision gates

1. **Block 2 (CFG-003)** — Researcher must select Path A/B/C before Implementor
   can proceed. The credential filename determines what CFG-004's guard checks.

2. **Block 6 (CFG-007)** — Researcher must select Path A/B/C/D before Implementor.
   The `_M`/`_D` → enum rename question requires domain input.

3. **Block 8 (CFG-009)** — Researcher must select Path A/B/C before Implementor.
   This is the only block touching the dependency declaration surface.

4. **Block 9 (9b)** — The `WrongDataSettings` decision is coupled to Block 6's
   path choice: if Path B/C is chosen for CFG-007, `WrongDataSettings` gets imported
   and used there. If Path A is chosen, `WrongDataSettings` remains unused and
   the Implementor should decide whether to remove it or leave it.

### Final note on CFG-002

CFG-002's YAML defect (malformed `tv_reg/crosstab.yaml`) was **already fixed at
actual HEAD** in commit `664e8f3`. The validation report is stale — it was written
against HEAD `8ed385f` (before the fix). The validation report must be updated
(Block 1) to record CFG-002 as **falsified at actual HEAD**, following the same
precedent as CFG-005 and CFG-010. No code or template change is needed for CFG-002
itself.

However, **RISK-2** (TV_REG stale key names `DYNAMICS_BY_SPOTS_M`/`_D`) is a
separate, still-valid defect that lives within CFG-007's scope. It is currently
unreachable (all TV_REG templates fail CFG-001 first) but becomes live once
CFG-001 is fixed. This is handled in Block 6.

---

*End of execution plan.*
