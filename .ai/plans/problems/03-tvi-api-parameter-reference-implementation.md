---
plan_id: "03-tvi-api-parameter-reference-implementation"
source_spec: ".ai/problems/01_mediascope_api_parameter_reference_spec.md"
source_structure: ".ai/tmp/document_structure.md"
source_validation: ".ai/tmp/validation_report.md"
source_research_vendor: ".ai/tmp/research_vendor_2.json"
source_research_templates: ".ai/tmp/research_templates.json"
status: active
phase: implementation
---

# Execution Plan: TVI API Parameter Reference Documentation

## Overview

Create a comprehensive Markdown documentation file that exhaustively documents all
Mediascope TVI API parameters, organized by report type, with cross-references to
the project's JSON5 template files. The file is placed in `settings/defaults/`
and automatically deployed to users via the existing `init` command
(`_copy_templates` uses `rglob("*")`, confirmed by Auditor — **no code changes
needed** for deployment).

## Filename Resolution

The spec has an inconsistency (Section 11 Q-03 says `tvi_api_parameter_reference.md`;
Section 12 / DR-07 says `report_parameters_reference.md`). Resolution:
**`tvi_api_parameter_reference.md`** — this name is used in 3 of 4 spec locations
(Section 11 Q-03, Section 15 item 6, document_structure.md line 8) vs. 2 locations
using the alternative. The document_structure.md target output and the
Implementation Notes (Section 15) are the most implementation-specific references.

**Placement:** `src/mko_get_mediascope_data/settings/defaults/tvi_api_parameter_reference.md`

## Execution Blocks

### Block 1: Generate the Documentation File (Task 5)

**Agent:** Implementor

**Scope:**
Create `src/mko_get_mediascope_data/settings/defaults/tvi_api_parameter_reference.md`
following the structure defined in `.ai/tmp/document_structure.md` (Sections 1-9),
incorporating all research data from `.ai/tmp/research_vendor_2.json` and
`.ai/tmp/research_templates.json`, and applying all 6 required fixes from
`validation_report.md`.

**Required Validation Fixes (from validation_report.md):**
1. `company_filter` CODE citation → `reports.py:829` (not 787/833 which set basedemo_filter)
2. `company_filter` RF citation → remove `nat_tv_brands_*` (inherit from template, don't set directly)
3. TMPL-03 → `frequency_dist_conditions` IS supported by Simple (vendor: `["simple", "crosstab"]`); only timeband should lack it
4. Section 8 → remove `custom_time_variable_id` from Simple row (Crosstab-only per vendor docs)
5. Section 5 → add 5 missing content gaps (docs 404, dead pageId, region_filter no-op, multiple_files classification, JSON5 quoting)
6. Statistics counts → Timeband 29, Simple 53, Crosstab 74

**Document Structure (from document_structure.md):**
1. Common Parameters (8 universal filters + common non-filter params)
2. Filter Support Matrix
3. Per-Report-Type Sections (5 types: timeband, simple, crosstab, consumption-target, duplication-timeband)
4. Statistics Dictionary (global, alphabetical)
5. Slices Dictionary
6. Options Dictionary
7. Filter Element Reference
8. Filter Operators & Expression Syntax
9. Sortings
10. Reach Conditions
11. Frequency Distribution Conditions
12. Scales
13. Custom Variable Identifiers
14. City Dataset Features
15. Consumption Target Options
16. Project-Level Parameters
17. Task Lifecycle
18. Validation Behavior
19. Report Templates Cross-Reference
20. Content Gaps & Undocumented Parameters
21. Metadata

**Acceptance Criteria:**
- File exists at `src/mko_get_mediascope_data/settings/defaults/tvi_api_parameter_reference.md`
- Covers ALL vendor parameters (exhaustive reference per R-03)
- No parameter duplication (common documented once, per-type only in applicable sections)
- All 6 validation fixes applied
- Statistics counts correct (Timeband 29, Simple 53, Crosstab 74)
- Template cross-references accurate (correct filenames, correct parameter usage)
- "Last verified" metadata per section
- Markdown format, English prose
- File is auto-deployable via `init` command (no code changes needed — verified)

### Block 2: Verify Init Integration (Task 6) — COMPLETED

**Status:** Verified by Auditor. `_copy_templates` at `init_service.py:45` uses
`source_dir.rglob("*")` to recursively copy all files from
`src/mko_get_mediascope_data/settings/` to the user's config directory.
Placing a `.md` file under `settings/defaults/` will be automatically deployed
during `init`. **No code changes required.**

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Filename ambiguity in spec | Medium | Low | Resolved: use `tvi_api_parameter_reference.md` (3/4 spec references) |
| Sortings line reference ambiguous (core/tasks.py vs vendor tasks.py) | Low | Low | Document cites vendor library paths, not project paths |
| Statistics are kit-dependent at runtime | High | Low | Document clearly with kitId reference table |
| 3 vendor report types not supported by our tool | High | Low | Document as reference-only, mark unsupported types |
| region_filter is a no-op | Low | Low | Document as deprecated with migration guidance |

## Dependencies

- `.ai/tmp/research_vendor_2.json` — vendor parameter data (complete)
- `.ai/tmp/research_vendor_1.json` — verification of 3 key claims (complete)
- `.ai/tmp/research_templates.json` — template parameter analysis (complete)
- `.ai/tmp/document_structure.md` — document structure spec (complete)
- `.ai/tmp/validation_report.md` — validation with required fixes (complete)

No external dependencies. No new packages required.
