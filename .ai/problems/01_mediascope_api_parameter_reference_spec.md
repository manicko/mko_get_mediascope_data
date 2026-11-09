# Specification: Mediascope TVI API Parameter Reference Documentation

**Spec ID:** 01  
**Problem Name:** Mediascope API Parameter Reference Documentation  
**Status:** Final (Approved with Recommendations Applied)  
**Date:** 2026-10-28  
**Analyst:** Senior Product Analyst  
**Source Problem:** `.ai/plans/problems/Problem01.md`

---

## 1. Problem Statement

The project `mko_get_mediascope_data` ships default report templates in `src/mko_get_mediascope_data/settings/defaults/{tv,tv_reg}/{crosstab,simple,timeband}.json5` that describe **not all** Mediascope TVI API parameters. Sellers configuring report definitions in JSON5 need a comprehensive, systematically organized reference of all API parameters, grouped by report type, to correctly assemble report configurations.

A new documentation file must be created in the `settings/defaults/` folder (or a designated documentation location) that systematizes all Mediascope TVI API parameters — common parameters (shared across all report types) documented once, and report-type-specific parameters (slices, statistics, type-specific filters) documented per report type — with no duplication. The file must also be deployed to users automatically via the existing `init` command flow.

The document must be sourced from the official Mediascope vendor documentation (3 URLs) and cross-checked against the project's own template files to ensure no undocumented or outdated parameters are silently used.

---

## 2. Confirmed Requirements

| # | Requirement | Source |
|---|-------------|--------|
| R-01 | Create a new documentation file systematizing all Mediascope TVI API parameters, organized by report type | Problem01.md |
| R-02 | Avoid parameter duplication: common parameters documented once in a shared section; type-specific parameters only in their applicable report-type sections | PO Decision Q2 |
| R-03 | Cover ALL API parameters from vendor docs (exhaustive reference), not just parameters our tool uses | PO Decision Q3 |
| R-04 | Do NOT include a comparison matrix of "what we have vs. API" — template parsing is only for verifying no undocumented/outdated parameters are used | PO Decision Q2 |
| R-05 | Our taxonomy (crosstab/timeband/simple) is close to Mediascope's native taxonomy — use our enum names as primary organization | PO Decision Q4 |
| R-06 | Static snapshot with "last verified" date per section (no live sync automation) | PO Decision Q5 |
| R-07 | Preferred format: Markdown (.md); fallback: JSON5 if Markdown is not feasible | Problem01.md |
| R-08 | Document language: English | Problem01.md |
| R-09 | The file must be deployed to users via the existing `init` command | Problem01.md |
| R-10 | The document must cross-reference our JSON5 template files (which template uses which parameters) | Implied by PO Q2 + Q6 |
| R-11 | Audience: Both sellers (configuring JSON5) and developers (maintaining the tool) | PO Decision Q1 |

---

## 3. Product Owner Decisions (Collected Answers)

| Question | Decision | Rationale (from PO) |
|----------|----------|---------------------|
| Q1: Document audience | **(C)** Both sellers and developers | Sellers need to know which parameters to configure; developers need to understand parameter mapping |
| Q2: Systematization approach | Reference catalog with **common + per-type structure** (not comparison matrix) | Parameters are grouped by: common (shared by all report types), then per-report-type sections with type-specific params. Template parsing is ONLY to verify no undocumented/outdated params are used |
| Q3: Coverage target | **(A)** All API parameters (exhaustive reference) | The tool supports all API parameters; the doc should cover all of them |
| Q4: Taxonomy | **(C)** Maps our code constructs to vendor terminology, taxonomy close to Mediascope | Our ReportType enum (crosstab/timeband/simple) aligns with vendor task types |
| Q5: Update model | **(C)** Static snapshot + verification metadata | "Last verified" date per section; no live sync automation |
| Q6: Template relationship | **(C)** Standalone reference, cross-referencing JSON5 template filenames | Keep documentation decoupled from config logic |

---

## 4. Conceptual Development Tasks

### Task 1: Collect Mediascope Vendor Documentation

**Purpose:** Gather exhaustive parameter documentation from the Mediascope vendor sources to build the authoritative reference.

**Expected Outcome:** Structured extraction of all TVI API parameters, statistics, slices, filters, options, and constraints organized by report type, with Russian-to-English translations of descriptions and preservation of original parameter names.

**Dependencies:** None (external research task)

**Assigned to:** 2 Researcher agents (redundant extraction with cross-verification)

**Sources:**
- URL 1: TVI API statistics — `https://docs.mediascope.net/xwiki/bin/view/Main/TV%20Index%20API/TVI%20API%3A%20статистики/`
- URL 2: TVI API filters/slices (Timeband) — `https://docs.mediascope.net/xwiki/bin/view/Main/TV%20Index%20API/TVI%20API%3A%20фильтры%20и%20срезы/Отчет%20Timeband/`
- URL 3: Mediascope Jupyter notebook — `https://github.com/MEDIASCOPE-JSC/mediascope-jupyter/blob/main/tv_index/_help.ipynb`
- Bonus: Installed vendor library `mediascope-api-lib` source code (`tasks.py`, `checks.py`, `catalogs.py`)

**Deliverables (from completed research):**
- `.ai/tmp/research_vendor_1.json` — Verification of 3 key claims (CONFIRMED): filter support matrix, statistics completeness, sortings JSON shape
- `.ai/tmp/research_vendor_2.json` — Comprehensive vendor extraction (527+ lines): 6 report types, 47 parameters, filter support matrix, statistics dictionaries, options, sortings, reach_conditions, frequency_dist_conditions, scales, custom variables, city dataset features, validation behavior

### Task 2: Analyze Project Template Files

**Purpose:** Extract all parameters used in the project's own JSON5 template and report files, and cross-check them against the vendor documentation to identify undocumented, outdated, or misconfigured parameters.

**Expected Outcome:** Complete mapping of which parameters each template file uses, TV vs TV_REG differences, and a list of template issues (duplicates, unsupported filters, outdated values) that require correction.

**Dependencies:** Task 1 (for cross-checking parameters)

**Assigned to:** 1 Researcher agent

**Deliverable:** `.ai/tmp/research_templates.json` — Analysis of 6 default template files + 17 report definition files, with 11 common parameters, 7 TV-vs-TV_REG differences, 12 undocumented parameters, 7 potential template issues, 5 unused defaults, and 22 vendor-documented parameters not used in templates.

### Task 3: Systematize Findings and Design Document Structure

**Purpose:** Consolidate research from Tasks 1 and 2 into a coherent document structure that organizes all parameters without duplication, following API documentation best practices.

**Expected Outcome:** Complete document structure specification with section outline, duplication strategy (common vs per-type), parameter placement table, markdown table templates, content gap analysis, and template verification checklist.

**Dependencies:** Tasks 1 and 2

**Assigned to:** 1 Auditor agent

**Deliverable:** `.ai/tmp/document_structure.md` — 527-line document structure specification with 9 major sections: proposed outline, duplication strategy, parameter placement table (47 parameters catalogued), markdown table templates, content gaps, best practices, template verification checklist (7 checks), quick reference table, and appendices.

### Task 4: Validate Document Structure

**Purpose:** Independently verify the document structure specification for completeness, consistency, and accuracy against all research sources and actual source code/template files.

**Expected Outcome:** Validation report identifying issues, with pass/fail per check and required fixes.

**Dependencies:** Task 3

**Assigned to:** 1 Validator agent

**Deliverable:** `.ai/tmp/validation_report.md` — 296-line validation report. Final assessment: **APPROVED_WITH_RECOMMENDATIONS**. 10 issues identified across 7 checks; 6 required fixes + 4 advisory recommendations.

### Task 5: Generate the Documentation File

**Purpose:** Create the actual Markdown parameter reference file based on the validated document structure, incorporating all research findings and applying the required fixes from validation.

**Expected Outcome:** A `.md` file placed in the settings folder that will be automatically deployed during `init`.

**Dependencies:** Tasks 1 (completed), 3 (completed with fixes applied), 4 (completed)

**Status:** Pending — this is the **implementation task** (defined in spec, not yet executed)

### Task 6: Integrate with Init Command

**Purpose:** Ensure the new documentation file is automatically copied to the user's config directory during `init`.

**Expected Outcome:** The existing `_copy_templates` function in `core/init_service.py` already handles this via `rglob("*")` — no code changes needed if the file is placed under `settings/`.

**Dependencies:** Task 5

**Status:** Verified (no code change needed) — see Section 10 (Constraints)

---

## 5. Research Summary

### 5.1 Vendor Research (Task 1)

**Report Types Identified:** 6 report types in the vendor API:

| Report Type | API Endpoint | Library Builder | Supported by Our Tool? |
|-------------|-------------|-----------------|----------------------|
| Timeband | `/task/timeband` | `build_timeband_task` | ✅ Yes (ReportType.timeband) |
| Simple | `/task/simple` | `build_simple_task` | ✅ Yes (ReportType.simple) |
| Crosstab | `/task/crosstab` | `build_crosstab_task` | ✅ Yes (ReportType.crosstab) |
| Consumption Target | `/task/consumption-target` | `build_consumption_target_task` | ❌ No |
| Duplication Timeband | `/task/duplication-timeband` | `build_duplication_timeband_task` | ❌ No |
| Respondent Analysis | `/task/respondent-analysis` | No builder method exists | ❌ No |

**Parameter Categories (47 parameters total):**
- **Filters (20):** `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`, `region_filter` (deprecated), `time_filter`, `location_filter`, `basedemo_filter`, `targetdemo_filter`, `respondent_filter`, `platform_filter`, `playbacktype_filter`, `bigtv_filter`, `program_filter`, `break_filter`, `ad_filter`, `subject_filter`, `duration_filter`, `duplication_company_filter`, `duplication_time_filter`
- **Non-filter task parameters (15):** `slices`, `statistics`, `scales`, `sortings`, `options`, `reach_conditions`, `frequency_dist_conditions`, `consumption_target_options`, 6× `custom_*_variable_id`, `task_name`
- **Client-side options (2):** `add_city_to_basedemo_from_region`, `add_city_to_targetdemo_from_region`
- **Project-level parameters (10):** `media`, `report_type`, `report_subtype`, `data_lang`, `relative_path`, `target_audiences`, `compression`, `check_done`, `multiple_files`, `period`/`last_time`

**Duplication Strategy (post-validation fix):**
- **"Common" = parameters accepted by ALL 5 documented vendor report types** (timeband, simple, crosstab, consumption-target, duplication-timeband as defined by `filter_support_matrix_by_report` in research_vendor_2.json lines 342–367)
- **8 common filters:** `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`, `region_filter` (deprecated/no-op), `location_filter`, `basedemo_filter`, `targetdemo_filter`
- **Common non-filter parameters:** `options`, `slices`, `statistics`, `task_name` (from `common_parameters_across_all_reports`, research_vendor_2.json lines 370–385)
- **`reach_conditions`** — supported by 5 types (NOT respondent-analysis, which is undocumented); documented in a dedicated section, listed per-type where applicable
- **`scales`** — conditionally supported (consumption-target + duplication-timeband per research_vendor_2.json lines 260, 284); documented in dedicated section, cross-referenced from applicable types
- **Respondent Analysis:** Marked as [UNDOCUMENTED] — not assumed to support any parameters since the vendor sources provide no filter/statistics/slice data for it

### 5.2 Template Analysis (Task 2)

**Key findings:**
- 11 common parameters in `DATA_DEFAULTS` across all 6 template files
- 7 TV-vs-TV_REG differences (most significant: `add_city_to_*` params, `options` defaults, `time_filter` TV-only)
- 12 project-level parameters not in vendor docs (correctly classified as N/A)
- 7 potential template issues requiring PO attention (see Section 8 — Template Verification Checklist)
- 5 truly unused default parameters: `weekday_filter`, `daytype_filter`, `location_filter`, `program_filter`, `sortings`

**File sharing observation:**
- `tv/simple.json5` and `tv/timeband.json5` are byte-identical (888 bytes each)
- `tv_reg/simple.json5` and `tv_reg/timeband.json5` are byte-identical (888 bytes each)

### 5.3 Validation Results (Task 4)

**Final Assessment: APPROVED_WITH_RECOMMENDATIONS**

The document structure is fundamentally sound (Section 1 outline and Section 9 appendices passed), but 10 issues were identified across checks 2–6. All 6 required fixes are documented in Section 8 below.

### 5.4 Validation Required Fixes (Applied to This Spec)

The following 6 fixes from the validator have been incorporated into the requirements/constraints below:

| Fix # | Issue | Correction Applied |
|-------|-------|-------------------|
| 1 | `company_filter` CODE citation wrong | Corrected: `company_filter` set programmatically at `reports.py:829` (TVRegTaskStrategy only), NOT lines 787/833 (which set `basedemo_filter`) |
| 2 | `company_filter` RF citation inaccurate | Removed `nat_tv_brands_*` — these files inherit `company_filter` from template subtypes, do NOT set it directly |
| 3 | TMPL-03 contradicts vendor | **`frequency_dist_conditions` IS supported by Simple** per vendor (`["simple", "crosstab"]`, research_vendor_2.json line 363). The Simple template should NOT have it removed. Only the **timeband** template should lack it. |
| 4 | Section 8 lists `custom_time_variable_id` for Simple | **Removed** — `custom_time_variable_id` is Crosstab-only (Section 3 row 32, research_vendor_2.json line 220) |
| 5 | Section 5 missing 5 content gaps | Added: docs 404 for Crosstab/CT/DT, dead pageId link, region_filter no-op, multiple_files classification, inconsistent JSON5 quoting |
| 6 | Statistics counts wrong | Corrected: Timeband **29** (not 27), Simple **53** (not 49), Crosstab **74** (not ~80) |

---

## 6. Assumptions

| # | Assumption | Rationale |
|---|-----------|-----------|
| A-01 | The existing `_copy_templates` function handles automatic deployment | It uses `source_dir.rglob("*")` to recursively copy all files from `src/.../settings/` to `~/.config/mko_get_mediascope_data/settings/`, so any new file placed under `settings/` (or `settings/defaults/`) will be automatically deployed | A-02 | The document covers all 5 documented vendor report types, plus notes Vendor-only types (Consumption Target, Duplication Timeband, Respondent Analysis) as out-of-scope for template creation but in-scope for reference | PO Q4: taxonomy close to Mediascope; R-03: cover all API parameters |
| A-03 | Statistics slice names follow `en_to_ru` convention (replace "EName" with "Name" for Russian) | Verified in `core/utils.py:341-351` — the tool translates `EName` → `Name` when `data_lang="ru"` |
| A-04 | The `kitId` option determines which statistics/slices/filters are valid | Verified in vendor library `catalogs.py` — `cats.get_*_unit(kit_id)` fetches kit-dependent allow-lists at runtime |
| A-05 | The Big TV (7-kit) statistics and filters are out of scope | The vendor docs child page "4.1. Статистики набора данных Big TV (7 кит)" was not one of the assigned URLs and was not fetched |

---

## 7. Constraints

| # | Constraint | Source |
|---|-----------|--------|
| C-01 | English-only documentation content | AGENTS.md §1, Problem01.md |
| C-02 | Preferred format: Markdown (.md) | Problem01.md |
| C-03 | No new dependencies unless authorized | AGENTS.md: "Add dependencies through `uv add` only within an authorized dependency-change task" |
| C-04 | No code changes required for init deployment | `init_service.py:_copy_templates` already copies all files recursively via `rglob("*")` |
| C-05 | Statistics/slices are kit-dependent at runtime | Vendor library fetches allow-lists via `cats.get_*_unit(kit_id)` — cannot be statically enumerated for all kits |
| C-06 | Must not introduce `print()` statements or new abstractions | AGENTS.md §6, §7 |
| C-07 | Workspace cleanliness — file must have a clear place in module architecture | AGENTS.md §17 |

---

## 8. Template Verification Checklist

Derived from `research_templates.json` → `potential_template_issues`. The Product Owner must resolve these items. This checklist serves BOTH as a specification deliverable AND as a template cleanup guide for the PO.

| # | Check ID | Description | Files Affected | Vendor Reference | Pass/Fail Criterion | Status |
|---|----------|-------------|----------------|------------------|---------------------|--------|
| 1 | TMPL-01 | `time_filter` is NOT supported by the `simple` report type | `defaults/tv/simple.json5`, `defaults/tv/timeband.json5` | Vendor: `build_simple_task` signature has no `time_filter` param. Verified: `research_vendor_1.json` lines 13–16 | `time_filter` key does NOT appear in `tv/simple.json5` DATA_DEFAULTS | **FAIL** — currently present (line 8) |
| 2 | TMPL-02 | `time_filter` gap in TV_REG timeband — decide if time-of-day analysis is needed | `defaults/tv_reg/timeband.json5`, `defaults/tv_reg/simple.json5` | Vendor: timeband supports `time_filter` (filter matrix) | **Binary decision required from PO**: Add `time_filter` to `tv_reg/timeband.json5` DATA_DEFAULTS, OR explicitly document it as an unsupported feature gap | **OPEN** — PO decision needed |
| 3 | TMPL-03 | Timeband must NOT have program/break/ad filters or `frequency_dist_conditions` | `defaults/tv_reg/timeband.json5` | Vendor: filter_support_matrix — `program_filter`/`break_filter`/`ad_filter` NOT supported by timeband; `frequency_dist_conditions` supported by simple+crosstab only | `program_filter`, `break_filter`, `ad_filter` keys absent from `tv_reg/timeband.json5`; `frequency_dist_conditions` absent from timeband (✅ may be present in simple) | **FAIL** — all 4 present in `tv_reg/timeband.json5` |
| 4 | TMPL-04 | No duplicate slices per subtype; at most 1 time slice per task | `defaults/tv/crosstab.json5` | Vendor: "at most 1 time slice per task" | Each slice name appears at most once per subtype; `researchDate`/`researchMonth`/`researchWeek` appears at most once per subtype | **FAIL** — `articleList4EName` x2 in DYNAMICS_BY_SPOTS; `researchMonth` x2 in TOP_NAT_TV_PROGRAMS |
| 5 | TMPL-05 | No duplicate IDs in filter expressions | `defaults/tv/crosstab.json5`; `reports/nat_tv_tvreport.json5` | `research_templates.json` lines 849, 873, 901 | No duplicate IDs in any `IN (...)` list | **FAIL** — `tvNetId IN (...348,348,348...)` in 4 locations |
| 6 | TMPL-06 | `issueType` only in Crosstab options | `defaults/tv_reg/timeband.json5`, `defaults/tv_reg/simple.json5` | Vendor: `issueType` is Crosstab-specific (research_vendor_2.json line 497) | `issueType` key absent from non-Crosstab DATA_DEFAULTS options | **FAIL** — present at line 22 in both files |
| 7 | TMPL-07 | Template file consistency after any split | `tv/simple.json5 ↔ tv/timeband.json5`; `tv_reg/` pair | If files split: simple excludes `time_filter`; timeband retains it | All remaining keys correct after split; no shared-file contamination of unsupported params | **N/A** — applies only if TMPL-01 is resolved by splitting files |

**Note on TMPL-03 fix:** The validator originally stated `frequency_dist_conditions` should be removed from the Simple template. This was corrected: `frequency_dist_conditions` IS supported by Simple per vendor docs (`["simple", "crosstab"]`). The Simple template may correctly include it; only the **timeband** template should not have it.

### Additional Template Issue (from Content Gap 5.5)

| # | Check ID | Description | Files | Vendor Reference | Criterion | Status |
|---|----------|-------------|-------|------------------|-----------|--------|
| 8 | TMPL-08 | Consistent JSON5 key quoting | `defaults/tv_reg/crosstab.json5` | `research_templates.json` lines 563–568 | All keys consistently quoted (or consistently unquoted) across all template files | **FAIL** — `tv_reg/crosstab.json5` uses unquoted keys while `tv/crosstab.json5` uses quoted keys |

---

## 9. Document Structure Specification

The following structure is defined in `.ai/tmp/document_structure.md` (Sections 1–9) with the 6 validation fixes applied.

### 9.1 Proposed Outline

```
H1: Mediascope TVI API Parameter Reference
├── H2: Common Parameters (All Report Types)
│   └── 8 universal filters + options/slices/statistics/task_name (each documented ONCE)
├── H2: Filter Support Matrix
│   └── Table: filter × report type → ✅/❌
├── H2: Per-Report-Type Sections
│   ├── H3: Report Metadata (api_task_type, endpoint, builder, applies_to, options_defaults)
│   ├── H3: Type-Specific Filters
│   ├── H3: Statistics
│   ├── H3: Slices
│   └── H3: Options & Constraints
├── H2: Statistics Dictionary (global, alphabetical)
├── H2: Slices Dictionary (cross-reference by category)
├── H2: Options Dictionary (kitId, totalType, standRtg, issueType, etc.)
├── H2: Filter Element Reference (date, channel, location, demo, program, break, ad, platform, BigTV, dictionaries)
├── H2: Filter Operators & Expression Syntax (IN/NIN/EQ/NEQ/LT/LTE/GT/GTE, AND/OR)
├── H2: Sortings
├── H2: Reach Conditions
├── H2: Frequency Distribution Conditions
├── H2: Scales
├── H2: Custom Variable Identifiers (6 params)
├── H2: City Dataset Features (kitId=3, TV_REG only)
├── H2: Consumption Target Options
├── H2: Project-Level Parameters (NOT TVI API — media, report_type, data_lang, etc.)
├── H2: Task Lifecycle (build → send → wait → get_result → result2table)
├── H2: Validation Behavior (check_task, check_units_in_task, kit-dependent allow-lists)
├── H2: Report Templates Cross-Reference
├── H2: Content Gaps & Undocumented Parameters
└── H2: Metadata (sources, research date, methodology)
```

### 9.2 Duplication Strategy (Validated)

**Definition of "Common":** Parameters with documented support by ALL 5 documented vendor report types (timeband, simple, crosstab, consumption-target, duplication-timeband), as verified by `filter_support_matrix_by_report` (research_vendor_2.json lines 342–367) and `supported_by` lists for non-filter parameters.

| Placement | Rule |
|-----------|------|
| **Common section** | Parameter is documented as supported by all 5 report types. Full description appears exactly once. |
| **Per-report-type section** | Parameter supported by SOME but not all types. Documented once in applicable section(s) with "Also supported by: [list]" cross-reference. |
| **Common badge in per-type sections** | Parameters also in Common are listed with `[COMMON]` badge + link to `#common-parameters`. No description duplicated. |
| **Dedicated global sections** | Statistics, slices, options, element references — each documented ONCE in a global dictionary; per-type sections list applicable items with links. |
| **N/A (project-level)** | Parameters NOT sent to the TVI API (`media`, `report_type`, `data_lang`, etc.) — documented in separate "Project-Level Parameters" section. |

### 9.3 Statistics Counts (Corrected from Validation)

| Report Type | Count | Source |
|-------------|-------|--------|
| Timeband | 29 | research_vendor_2.json lines 102–130 |
| Simple | 53 | research_vendor_2.json lines 149–201 |
| Crosstab | 74 (39 Sum + 35 Avg) | research_vendor_2.json lines 223–227 |
| Consumption Target | 4 | research_vendor_2.json lines 249–253 |
| Duplication Timeband | 4 | research_vendor_2.json lines 275–279 |
| Respondent Analysis | Undocumented | Marked [UNDOCUMENTED] |

### 9.4 Content Gaps (Complete)

Incorporating all validator-required fixes:

| # | Gap | Source |
|---|-----|--------|
| 5.1 | `scales`, `drfd`, `reachN` — undocumented in notebook/docs, only in `checks._check_scales` | research_vendor_2.json discrepancies, item 2 |
| 5.2 | `custom_*_variable_id` (6 params) — listed in `build_task` signature, no notebook examples | research_vendor_2.json discrepancies, item 5 |
| 5.3 | `subject_filter`, `duration_filter`, `respondent_filter`, `platform_filter`, `playbacktype_filter`, `bigtv_filter` — in `build_task` signature but not in notebook | research_vendor_2.json discrepancies, item 5 |
| 5.4 | `duplication_company_filter`, `duplication_time_filter` — listed in `build_task` but not in notebook | research_vendor_2.json discrepancies, item 5 |
| 5.5 | Docs filter/slice pages for Crosstab, Consumption Target, Duplication Timeband are 404/unreachable | research_vendor_2.json discrepancies, item 6 |
| 5.6 | `pageId=78807053` ("справка по параметрам") is a dead/empty link | research_vendor_2.json discrepancies, item 7 |
| 5.7 | `region_filter` accepted by API client but is a no-op at runtime (prints deprecation notice) | research_vendor_2.json discrepancies, item 4 |
| 5.8 | `multiple_files` is a project-level parameter, not a vendor API parameter | research_vendor_2.json discrepancies, item 8 |
| 5.9 | `WAgeAverage` present in URL-1 Timeband list but absent from notebook | research_vendor_2.json discrepancies, item 9 |
| 5.10 | Big TV (7-kit) statistics not fetched (child page 4.1 not assigned) | research_vendor_2.json discrepancies, item 10 |
| 5.11 | Respondent Analysis filter/slice/statistics entirely undocumented | research_vendor_2.json discrepancies, item not explicitly numbered |
| 5.12 | Project-level parameters not in vendor docs: `data_lang`, `relative_path`, `target_audiences`, `compression`, `check_done`, `media`, `report_type`, `report_subtype`, `multiple_files`, `period`/`last_time` | research_templates.json `undocumented_parameters_vs_vendor` |
| 5.13 | Inconsistent JSON5 key quoting in `tv_reg/crosstab.json5` (unquoted keys vs. quoted in `tv/crosstab.json5`) | research_templates.json potential_template_issues |
| 5.14 | `sortings` JSON shape: vendor doc shows `{sortingUnits: [{unit, direction}]}` but `_help.ipynb` doesn't show the wrapper | Verified via `core/tasks.py:200-214`, `tasks.py:559-573` (CONFIRMED) |
| 5.15 | `time_filter` in `simple` template is unsupported by vendor | Verified via `build_simple_task` signature (CONFIRMED) |
| 5.16 | `issueType` in TV_REG non-Crosstab DATA_DEFAULTS is Crosstab-specific | research_vendor_2.json line 497 |

**Statistics verification (from research_vendor_1.json):**
| Check | Verified At | Result | Confidence |
|-------|-------------|--------|------------|
| `time_filter` support matrix | `tasks.py` builder signatures | CONFIRMED | HIGH |
| Statistics completeness (RtgPerSum, ATV, Reach000 in URL-1) | `checks.py` + URL-1 | CONFIRMED | HIGH |
| Sortings JSON shape | `core/tasks.py:200-214`, `tasks.py:559-573` | CONFIRMED | HIGH |

### 9.5 Corrected Quick Reference Table (Post-Validation)

| Report Type | Report-Specific Filters | Custom Variables | Sortings? | Frequency Dist? |
|-------------|------------------------|-------------------|-----------|------------------|
| **Timeband** | `time_filter` ✅ | None | ✅ | — |
| **Simple** | `program_filter`, `break_filter`, `ad_filter`, `subject_filter` ✅ | `custom_time_variable_id` ✅ (Crosstab only — NOT Simple) | ✅ | ✅ |
| **Crosstab** | `program_filter`, `break_filter`, `ad_filter`, `subject_filter` ✅ | `custom_time_variable_id` ✅ | ✅ | ✅ |
| **Consumption Target** | `time_filter`, `program_filter`, `break_filter`, `ad_filter`, `subject_filter`, `duration_filter` ✅ | `custom_demo/company/respondent_variable_id` ✅ | ❌ | — |
| **Duplication Timeband** | `time_filter`, `duration_filter`, `duplication_company_filter`, `duplication_time_filter` ✅ | `custom_duplication_time/company_variable_id` ✅ | ❌ | — |
| **Respondent Analysis** | [UNDOCUMENTED] | [UNDOCUMENTED] | [UNDOCUMENTED] | [UNDOCUMENTED] |

> **Note:** The `custom_time_variable_id` entry has been corrected — it is **Crosstab-only**, not supported by Simple. This was an internal inconsistency identified by the validator (Check 6) and is now resolved.

---

## 10. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| R-01 | Vendor docs pages may return 404 or change content | Medium | Medium | Document source URLs with last-verified dates; include vendor library source as authoritative fallback |
| R-02 | Statistics/slices are kit-dependent (runtime-fetching) | High | Medium | Document kit-dependence clearly; provide kitId reference table; note that per-kit allow-lists come from `cats.get_*_unit(kit_id)` at runtime |
| R-03 | Three report types (Consumption Target, Duplication Timeband, Respondent Analysis) are vendor-supported but NOT supported by our tool | High | Low | Clearly mark these as out-of-scope for template creation but in-scope for reference documentation |
| R-04 | `region_filter` is a no-op (deprecated) but still accepted by the API client | Low | Medium | Document as deprecated with clear "use `company_filter` with `regionId` instead" guidance |
| R-05 | `scales`, `drfd`, `reachN` are undocumented in vendor docs/notebook | Low | Low | Flag as [UNDOCUMENTED — evidence from library source only]; note limited understanding |
| R-06 | Shared template files (tv/simple == tv/timeband; tv_reg/simple == tv_reg/timeband) cause parameter contamination | Medium | High | TMPL-01 and TMPL-03 must be resolved by PO before documentation is published; document the issue clearly |
| R-07 | Statistics count discrepancies across sources | Low | Low | Corrected counts documented in Section 9.3; all future counts reference research_vendor_2.json |
| R-08 | File placement ambiguity — "which folder" for the doc file | Low | Low | Resolved: see Section 12 (File Placement Decision) below |

---

## 11. Open Questions

| # | Question | Status |
|---|----------|--------|
| Q-01 | **TMPL-02:** Should TV_REG timeband add `time_filter` to enable time-of-day analysis? | **OPEN** — PO decision required |
| Q-02 | Should the document also cover the 3 vendor report types we DON'T support (Consumption Target, Duplication Timeband, Respondent Analysis) as reference-only? | **Open** — PO Q2 said "systematize by report type" covering all API parameters (R-8), but our tool only supports 3 types. The document structure includes all 6 for completeness, but the PO should confirm this is desired. |
| Q-03 | What is the exact filename for the documentation file? | **Resolved:** `tvi_api_parameter_reference.md` (per Section 12) |

---

## 12. File Placement Decision

**Resolved Decision:** Place the documentation file at `src/mko_get_mediascope_data/settings/defaults/report_parameters_reference.md`

**Rationale:**
- The original request said "in this folder" referring to `settings/defaults/tv/`, but the document covers ALL report types (crosstab, simple, timeband) across both TV and TV_REG media types
- Placing it under `settings/defaults/` (one level above `tv/` and `tv_reg/`) is the logical parent — it's where default template documentation belongs
- The `_copy_templates` function in `init_service.py` uses `rglob("*")` which recursively copies **all** files from `app_settings_dir` (`src/.../settings/`) to the user config directory — **no code changes needed** for deployment
- The file will be automatically deployed during `init` alongside the existing template files

**Alternative considered:** `settings/report_parameters_reference.md` (at `settings/` root) — rejected because `defaults/` is the semantically correct location for documentation about default parameters

---

## 13. Out of Scope

| Item | Reason |
|------|--------|
| Fixing existing template issues (TMPL-01 through TMPL-08) | These are identified in the checklist (Section 8) but are **template fixes**, not documentation. The PO may choose to address them separately. The documentation should still accurately reflect the vendor's intended parameter behavior (not the current template bugs). |
| Adding support for Consumption Target / Duplication Timeband / Respondent Analysis report types to the tool | Our `ReportType` enum only has `crosstab`, `timeband`, `simple`. These vendor types are documented as reference only. |
| Live/automatic sync with vendor docs | PO Q5: static snapshot with "last verified" metadata only |
| JSON5 format for the documentation | PO preferred MD; JSON5 only if MD is "сложно реализовать" (hard to implement). Markdown is straightforward. |
| Big TV (7-kit) statistics/filter enumeration | Not one of the 3 assigned URLs; child page "4.1. Статистики набора данных Big TV" was not fetched |
| `respondent-analysis` parameter enumeration | Entirely undocumented in vendor sources; vendor library has no `build_*` convenience method for it |
| `scales`/`drfd`/`reachN` deep-dive | Undocumented in vendor docs and notebook; only evidence is `checks._check_scales`; insufficient for full documentation |

---

## 14. Definition of Ready

The following criteria must be met for the documentation file to be considered ready for deployment:

| # | Criteria | How to Verify |
|---|----------|---------------|
| DR-01 | File exists at `.ai/tmp/document_structure.md` and passes validation (APPROVED_WITH_RECOMMENDATIONS with all 6 required fixes applied) | Review validation_report.md Sections and this spec |
| DR-02 | Document covers all vendor parameters (filters, statistics, slices, options, sortings, reach_conditions, frequency_dist_conditions, scales, custom variables, city features, consumption_target_options) | Cross-check against research_vendor_2.json parameter_to_json_key and all report_types entries |
| DR-03 | No duplication: common parameters documented once; per-type parameters only in applicable sections | Review duplication strategy (Section 9.2) |
| DR-04 | All 6 validation required fixes applied (company_filter citation, RF citation, TMPL-03 frequency_dist, custom_time_variable_id, content gaps, statistics counts) | Verify each fix is addressed in Section 9 |
| DR-05 | Template verification checklist (TMPL-01 through TMPL-08) is documented with clear pass/fail criteria | Section 8 |
| DR-06 | File is in Markdown format with English prose | Format check |
| DR-07 | File placement at `settings/defaults/report_parameters_reference.md` will be auto-deployed by `init` | Verify `_copy_templates` uses `rglob("*")` — confirmed in `init_service.py:45` |
| DR-08 | "Last verified" metadata included per section | Document formatting requirement |
| DR-09 | Statistics counts are correct (Timeband: 29, Simple: 53, Crosstab: 74) | Section 9.3 |
| DR-10 | Cross-references to template files are accurate (correct file names, correct parameter usage) | Spot-check against actual template files |

---

## 15. Implementation Notes (For the Implementor)

These notes are informational for whoever creates the final `.md` file. They capture the key decisions the analyst has already resolved.

1. **No code changes needed for init integration** — `_copy_templates` in `init_service.py` already copies all files recursively. The doc file goes to `settings/defaults/` and is automatically deployed.

2. **Statistics/slices are kit-dependent** — The vendor library fetches allow-lists at runtime via `cats.get_*_unit(kit_id)`. The doc should state kitId=1 (TV Index All Russia) is the default used in examples, and provide the kit ID reference table (Section 9.1 of `document_structure.md`).

3. **`en_to_ru` translation** — When `data_lang="ru"` (the default), the tool translates slice names by replacing `EName` → `Name`. The doc should note that slices can be specified in `EName` form and will be auto-translated.

4. **Template file sharing** — The doc's "Report Templates Cross-Reference" section should clearly note that `tv/simple.json5` == `tv/timeband.json5` and `tv_reg/simple.json5` == `tv_reg/timeband.json5` are identical, because this causes the parameter contamination issues documented in TMPL-01, TMPL-02, TMPL-03, and TMPL-07.

5. **`company_filter` code reference** — The only programmatic setting of `company_filter` is in `TVRegTaskStrategy.generate()` at `reports.py:829` (line 829: `settings["company_filter"] = company_filter.format(reg_id=reg_id)`). The `DefaultTVTaskStrategy` does NOT set `company_filter` from code — it's always from templates/report files.

6. **File name** — `tvi_api_parameter_reference.md`

---

## 16. Research Artifacts

| Artifact | Path | Purpose |
|----------|------|---------|
| Vendor research (run 1) | `.ai/tmp/research_vendor_1.json` | Verification of 3 key claims (CONFIRMED) |
| Vendor research (run 2) | `.ai/tmp/research_vendor_2.json` | Full vendor parameter extraction (527 lines) |
| Template research | `.ai/tmp/research_templates.json` | Project template parameter analysis |
| Document structure | `.ai/tmp/document_structure.md` | 527-line structure specification |
| Validation report | `.ai/tmp/validation_report.md` | 296-line validation with 6 required fixes |
| This specification | `.ai/problems/01_mediascope_api_parameter_reference_spec.md` | Final analytical specification |

---

*Specification compiled from research conducted by 2 Researcher agents, 1 Researcher agent (templates), 1 Auditor agent, and 1 Validator agent. All research artifacts are preserved in `.ai/tmp/` for traceability.*