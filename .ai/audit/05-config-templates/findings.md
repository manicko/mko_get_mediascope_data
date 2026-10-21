---
phase: "05"
phase_name: "config-templates"
id_prefix: "TPL-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/05-config-templates/references/workflow.md#severity-taxonomy"
code_ref: "ee575dc7f4f9b857039509507f59a79708b0c7cb"
---

# Audit Findings — Config Template Validation & Spec Conformance

## Summary

One LOW finding. The export-location comment points readers to a different directory from the one the application uses. No new template-validation or parameter-consumption failure was established.

## Scope

**Examined:** All six phase blocks: all bundled application/logging, default and report JSON5 against their consuming models; field/period names; defaults-to-Data survival; extra-field policies; actual strategy/vendor readers. Every report entry was merged and Data-validated through the actual service methods. Existing phase-02 validation evidence was reused as context; the current template inventory was independently enumerated and rerun.

**Snapshot:** Current dirty working tree on 2026-10-21 at `code_ref`, unchanged during this phase. Pre-existing `app_config.json5` edits, overview-document drift, plans and report deletions were preserved. This finding describes the current template, not pristine HEAD. Initial/later hashes remain under `.ai/tmp/audit-2026-10-21/`. Historical raw report IDs TPL-001–006 were reserved; current docs and Git history also reserve TPL-007 for prior crosstab parse errors, so the new finding is TPL-008.

**Tools:** Read-only PowerShell, `rg`, Git path/history inspection, existing venv Python and `.ai/tmp/audit-2026-10-21/template_probe.py`; no dependency synchronization or application changes.

**Not verified:** Live vendor acceptance, current business meaning of kit IDs, actual production credentials, remote datasets or full lint/type/test suite. Credential sourcing remains phase 02. API extras are intentionally permitted; accepting synthetic unknown keys is not treated as a defect without an established consumed-value consequence.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TPL-008 | LOW | `src/mko_get_mediascope_data/settings/app_config.json5::"export_folder"` | Export-location comment incorrectly adds the settings subdirectory |

#### TPL-008 — [LOW] Export-location comment incorrectly adds the settings subdirectory

**Anchor:** `src/mko_get_mediascope_data/settings/app_config.json5::"export_folder"`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.export_folder`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::get_app_service`
**Anchor:** `src/mko_get_mediascope_data/core/paths.py::PathResolver.resolve`
**Disambiguator:** `"export_folder": "output"`
**Checked:** ran
**Why this band:** A template comment contradicts the consumed field's path behavior without changing runtime data, exactly the LOW documentation-drift criterion. No parameter loss or validation failure supports a higher band.
**Problem:** The comment says relative exports resolve under `~/.config/mko_get_mediascope_data/settings/`. `get_app_service` instead supplies `APP_PATHS.user_dir` as the resolver base, and `AppService.export_folder` resolves `output` against that root. Settings live in a child directory, but exports do not implicitly use that child. The adjacent statement that `PathResolver.resolve()` creates directories also attributes the later `ensure_dir` behavior to the wrong method.
**Repro:** Read the current template and trace `get_app_service` → `PathResolver(APP_PATHS.user_dir)` → `AppService.export_folder`. The bounded template probe resolves the bundled relative export against an isolated user root and confirms `output`, not `settings/output`.
**Impact:** Operators relying on the template's explanation can look for exported files under the wrong directory or enter a path relative to an incorrect base. Export behavior itself is unchanged.
**Root cause:** Explanatory comments conflate the user configuration root with its settings child and the resolve/ensure operations.
**Fix:** Correct the `export_folder` comment to describe the actual user-root base and directory-creation owner. Benefit: configuration instructions match file placement; effort trivial; advisory priority low. [DOC-UPDATE]
**Verify:** new test needed: review_export_folder_comment_against_resolver_contract
**Related:** No duplicate of CFG-011, which owns omitted-client filter loss.

**Evidence — `Get-Content src/mko_get_mediascope_data/settings/app_config.json5 | Select-Object -First 6`** *(supports: the comment's claimed base and creation behavior)*:
```text
{
  // Where CSV exports are written. Relative paths resolve under the user settings
  // directory (~/.config/mko_get_mediascope_data/settings/); absolute paths point anywhere.
  // Expanded for ~ at validation; PathResolver.resolve() creates the directory if needed.
  "export_folder": "output",

```

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/template_probe.py`** *(supports: actual resolver uses the supplied user root)*:
```text
bundled_relative_export_from_user_root: output
bundled_export_under_settings_child: False
```

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 0; MEDIUM 0; LOW 1.
- **Dependencies:** None.
- **Merge candidates:** None.
- **Conflicting evidence:** Deliberate API passthrough is not an untyped-extra finding; no forbid-all schema requirement is inferred.
- **Cross-phase conflicts:** CFG-011 remains the defaults/client omission defect; credential topology remains phase 02; transport retry is phase 09. TPL-007 remains reserved for historical crosstab parser provenance.
- **Rollout risk:** Correcting this comment should preserve the current export base and runtime behavior. No test or application change is proposed for this documentation-only correction.

## Appendices

### Appendix A — Complete current template probe output

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/template_probe.py` (exit 0, approximately 3.5s).
