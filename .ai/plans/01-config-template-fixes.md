# Execution Plan — Phase 05 Config Template Fixes

## Source

- **Plan file:** `.ai/audit/99-validation/05-config-templates-validated-findings.md`
- **Code context:** `.ai/audit/05-config-templates/code_context.md`
- **Architecture reference:** `docs/00-overview/settings-layers-architecture.md`

## Current State (HEAD `3dfffe6`)

The codebase has undergone a YAML→JSON5 migration since the plan was written.
TPL-001 (CRITICAL), TPL-004 (HIGH), and the template side of TPL-005 are **resolved**.
Two findings still have unresolved code-level gaps, two have template-level residuals.

### Finding Status Summary

| Finding | Severity | Status | Action Needed |
|---------|----------|--------|---------------|
| TPL-001 | CRITICAL | RESOLVED | None |
| TPL-002 | LOW | STILL APPLICABLE | Doc/test only — `extra="allow"` is intended |
| TPL-003 | HIGH | PARTIALLY RESOLVED | **Code fix** + template null→[] + tests |
| TPL-004 | HIGH | RESOLVED | None |
| TPL-005 | MEDIUM | PARTIALLY RESOLVED | **Code fix** — add existence check + `WrongDataSettings` |
| TPL-006 | LOW | STILL APPLICABLE | **Template fix** — kitId=7→1 + comment drift |

---

## Work Blocks

### Block A — TPL-003: `en_to_ru(None)` crash guard (HIGH)

**Goal:** Eliminate the `TypeError` crash path for `data_lang="ru"` reports
when `slices` is `None` (defaults to `null` in timeband/simple defaults files).

**Dependencies:** None. Independent.

**Code change:** `core/services.py` — `merge_data_settings` (line 130-131)
Guard the `en_to_ru` call so it skips when `slices` is `None`. The downstream
`Data.coerce_none_to_empty` validator will still convert `None`→`[]` at
`Data` construction time.

```python
# BEFORE:
if report_settings.data_lang == "ru":
    data_settings["slices"] = utils.en_to_ru(data_settings["slices"])

# AFTER:
if report_settings.data_lang == "ru":
    slices = data_settings.get("slices")
    if slices is not None:
        data_settings["slices"] = utils.en_to_ru(slices)
```

**Templates changes** (defense-in-depth): Change `DATA_DEFAULTS."slices": null`
→ `"slices": []` in:
- `settings/defaults/tv/timeband.json5` (line 12)
- `settings/defaults/tv/simple.json5` (line 12)
- `settings/defaults/tv_reg/timeband.json5` (line 15)
- `settings/defaults/tv_reg/simple.json5` (line 15)

**Tests to add:**
- `test_en_to_ru_handles_none` — `en_to_ru(None)` returns `[]` (not crash)
- `test_merge_data_settings_ru_with_none_slices` — `merge_data_settings`
  with `data_lang="ru"` and `slices: None` does not crash

**Tests to keep green:**
- `test_merge_data_settings_en_does_not_translate` (test_models.py:42)
- `test_data_coerces_none_to_empty` (test_models.py:118)

**Risk:** Low. 2-line code guard; no API surface change.

---

### Block B — TPL-005: WrongDataSettings for missing defaults file (MEDIUM)

**Goal:** Convert the opaque `FileNotFoundError` from `get_default_data_settings`
into the domain-specific `WrongDataSettings` exception with a clear message.

**Dependencies:** None. Independent.

**Code change:** `core/services.py` — `get_default_data_settings` (lines 100-114)
Add `path.exists()` check before calling `json5_to_dict`; raise `WrongDataSettings`
if the file does not exist.

**Code change:** `core/errors.py` — `WrongDataSettings` (lines 20-26)
Update docstring: remove "Reserved for future use" / "Currently unused"
language; document that it is now raised by `get_default_data_settings`.

**Tests to add:**
- `test_get_default_data_settings_missing_defaults_raises_wrong_data_settings`
  — using `local_app_service` (hermetic) with a non-existent report type

**Tests to keep green:**
- `test_get_default_data_settings_missing_file` (test_services.py:223) —
  verifies all 6 existing combos load

**Risk:** Low. Adds a guard; `WrongDataSettings` subclasses `Exception`.

---

### Block C — TPL-006: kitId reconciliation (LOW)

**Goal:** Remove `kitId: 7` (undefined in the legend 1–6) from defaults and
templates; fix comment drift on `kitId: 3` entries.

**Dependencies:** TPL-001 is resolved (templates pass validation), so kitId
drift is now unmasked and must be reconciled before shipping.

**Template changes:**

**`settings/defaults/tv/crosstab.json5`:**
- Line 126 (`TOP_NAT_TV_PROGRAMS`): `kitId: 7` → `kitId: 1`; fix comment
  ("TV Index Russia all" → "Russia all")
- Line 164 (`NAT_TV_CHANNELS_BA`): `kitId: 7` → `kitId: 1`; fix comment

**`settings/reports/nat_tv_tvreport.json5`:**
- Line 174 (`NAT_TV_CHANNELS_BA` entry): `kitId: 7` → `kitId: 1`

**`settings/reports/last_category/nat_tv_brands_weekly_reach.json5`:**
- Line 27 (`DYNAMICS_BY_SPOTS` entry): `kitId: 7` → `kitId: 1`
  (overrides default's kitId=1 with undefined 7 — the "wrong direction" noted
  in the plan; correcting to 1 makes the override a no-op, which is correct)

**`settings/defaults/tv_reg/crosstab.json5`:**
- Lines 73, 99: `kitId: 3` comment "TV Index Russia all" → "Cities"
  (kitId=3 is Cities per the legend, not "Russia all")

**VAL-003 residual:** `nat_tv_tvreport.json5` entry[0] (TOP_NAT_TV_ADVERTISERS)
has no `options` block. The default's `options` block has `issueType: "AD"` +
`standRtg` but no `kitId`, so `reports.py:263` defaults to kitId=1. Add an
explicit `options` block with `kitId: 1` for explicitness (per TPL-006 fix
requirement: "every `options` block declared in a template must supply a
complete block").

**Tests to add:**
- `test_bundled_templates_kitId_conformance` — assert no kitId=7 in any
  defaults or template; assert all `options` blocks are complete

**Tests to keep green:**
- `test_bundled_templates_validate` (test_models.py:67)
- `test_data_defaults_covers_all_subtypes` (test_models.py:99)

**Risk:** Low. Only comment fixes and one missing-options-block addition. No
kitId values changed (user confirmed kitId=7 = "Big TV" is valid).

**REVISED after user input:** kitId=7 is "Big TV" — a valid dataset.
The audit's recommendation to change kitId=7→1 was **incorrect**.
Instead, update the legend comments to include the full mapping
(1–10) and fix individual kitId comments that mislabel the dataset:

| kitId | Dataset name in user-provided mapping |
|-------|--------------------------------------|
| 1 | TV Index All Russia |
| 2 | TV Index Russia 100+ |
| 3 | TV Index Cities |
| 4 | TV Index Plus All Russia |
| 5 | TV Index Plus Russia 100+ |
| 6 | TV Index Moscow |
| 7 | Big TV |
| 9 | TV Index Russia Special |
| 10 | TV Index Moscow Special |

**Template changes (revised):**

**`settings/defaults/tv/crosstab.json5`:**
- Update legend comment block to full 1–10 mapping
- Fix `//TV Index Russia all` comments on kitId=1 entries → `//TV Index All Russia`
- Fix `//TV Index Russia all` comments on kitId=7 entries → `//Big TV`
- Add `"kitId": 1` to `TOP_NAT_TV_ADVERTISERS.options` (currently missing —
  complete options block per architecture rules)

**`settings/defaults/tv_reg/crosstab.json5`:**
- Update legend comment block to full 1–10 mapping
- Fix `//TV Reg` comment on DATA_DEFAULTS kitId=3 → `//TV Index Cities`
- Fix `//TV Index Russia all` comments on kitId=3 subtype entries → `//TV Index Cities`

**`settings/reports/nat_tv_tvreport.json5`:**
- Add `options` block to entry[0] (TOP_NAT_TV_ADVERTISERS) — VAR-003:
  currently has no options, silently inherits default's kitId=1. Add explicit
  `options` block with `kitId: 1`, `issueType: "AD"`, `standRtg` to make it
  complete (matching the default's block).

**No kitId value changes** — all kitId=7 entries are valid ("Big TV").
Only comments and one missing options block are corrected.

---

### Block D — Doc fix: stale YAML reference in `coerce_none_to_empty` docstring

**Goal:** Fix the `coerce_none_to_empty` docstring which says "``Null`` entries"
(YAML convention) instead of "JSON5 ``null`` entries".

**Code change:** `core/models.py` (line 146)
- `"when ``DATA_DEFAULTS`` contains ``Null`` entries."` →
  `"when ``DATA_DEFAULTS`` contains ``null`` entries."`

**Risk:** Trivial. Docstring only.

---

### Block E — Documentation update (Doc-specialist)

**Goal:** Update docs affected by the implementation changes.

**Changes:**
1. `docs/00-overview/settings-layers-architecture.md` — update the kitId
   inventory table (line 57) to reflect kitId=7→1 reconciliation; remove
   stale YAML line references.
2. Stale audit docs (`.ai/audit/05-config-templates/findings.md`,
   `_validate.py`, `.ai/audit/02-config-secrets/findings.md`,
   `.ai/audit/10-code-quality/findings.md`) — update YAML references to JSON5.

**Risk:** Low. Documentation only.

---

## Implementation Sequence

```
Block A (TPL-003 code + templates + tests)        → commit 1
Block B (TPL-005 code + tests)                     → commit 2
Block C (TPL-006 templates + tests)                → commit 3 (after user Q)
Block D (docstring fix)                            → commit 4 (or fold into commit 1)
Block E (docs)                                     → commit 5
```

Blocks A and B are independent — they can be done in order (one Implementor
at a time). Block C depends on TPL-001 being resolved (it is). Block C
requires user confirmation on the kitId=7 question.

## Validation Strategy

After each block:
```bash
uv run ruff check src/mko_get_mediascope_data core tests
uv run mypy src/mko_get_mediascope_data
uv run pytest
```

All 52 existing tests must remain green. New tests must pass.

## Do NOT Change

- `extra="allow"` on `Data`/`DataDefaults`/`ReportSettings` (TPL-002) —
  intended two-layer transport.
- The shallow top-level overlay in `merge_data_settings` (TPL-006 rejected
  as a merge-semantics finding).
