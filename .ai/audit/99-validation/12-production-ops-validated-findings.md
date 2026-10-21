---
phase: "12"
phase_name: "production-ops"
id_prefix: "OPS-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/12-production-ops/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Validation Report

## Summary

All three findings are confirmed MEDIUM. The release version check reads missing static metadata, a developer command names the wrong module, and rename-only migration does not convert YAML syntax. Each is a concrete operability problem with a workaround; hosted publication and built artifacts were not tested.

## Scope

**Input/snapshot:** Fresh frozen `.ai/audit/12-production-ops/findings.md`, OPS-007–009; current dirty/staged b27b870 working files, not an atomic HEAD snapshot. Existing external work and authorized stale-audit cleanup were preserved.
**Evidence:** Complete phase-99/12 procedures, actual release workflow/metadata/README/parser, owner D-19 and prior current-run findings. Validator inspected and independently reran ops_probe.py (exit 0). It performs local metadata/parsing/module-discovery checks only: no build, upload, installation, API or user-configuration change.
**Limits:** No hosted Actions or token-authorization result, built wheel/sdist, fresh installation, rollback or legacy installed-version test. setuptools-scm is unavailable locally; no plugin installation/build isolation was attempted. Existing phase11 gate measurements are prior context; unchanged recorded hashes support reuse, not a claim of another suite run.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| OPS-007 | MEDIUM | `.github/workflows/release.yml::"Verify version matches git tag"` | Release guard reads an absent static version |
| OPS-008 | MEDIUM | `README.md::"uv run -m mko_get_medioscope_data run"` | Developer command names a nonexistent module |
| OPS-009 | MEDIUM | `README.md::"Migrating from YAML to JSON5"` | Renaming does not convert legacy YAML syntax |

| ID | Disposition | Final type | Final severity |
|---|---|---|---|
| OPS-007 | confirmed | SPEC-DEVIATION | MEDIUM |
| OPS-008 | confirmed | DOC-UPDATE | MEDIUM |
| OPS-009 | confirmed | DOC-UPDATE | MEDIUM |

#### OPS-007 — [MEDIUM] Release guard reads an absent static version

**Anchor:** `.github/workflows/release.yml::"Verify version matches git tag"`
**Anchor:** `pyproject.toml::"dynamic"`
**Disambiguator:** `['project']['version']`
**Checked:** ran
**Why this band:** A visible release-step failure with an artifact-metadata workaround is MEDIUM degraded operability. No silently vetted/unvetted publication, inactive safety gate or irreversible rollback failure establishes CRITICAL/HIGH.
**Problem:** The version guard indexes project.version, but pyproject declares dynamic=["version"] and setuptools-scm supplies that value during build.
**Repro:** The exact embedded Python expression raises KeyError('version') on current pyproject. The workflow's sequential order is sync, build, version check, then GitHub Release upload; if earlier steps succeed, this check fails before upload. That conditional path is source proof, not an observed hosted job.
**Impact:** Maintainers cannot complete this declared tag-release path without repairing/bypassing its version check.
**Root cause:** Release verification reads a static field rather than the distribution identity produced by the configured dynamic build backend.
**Fix:** In the version-check step, read Version from the built wheel's dist-info/METADATA and sdist PKG-INFO using standard archive/metadata readers; require expected metadata and agreement across artifacts selected for upload. Compare that version with the tag after its leading v is removed and fail on mismatch, absent/ambiguous metadata or no artifacts. Keep dynamic setuptools-scm ownership; do not add a competing static project.version. Verify this locally with matching/mismatching artifact fixtures before any real release.
**Verify:** new focused check needed: test_release_version_check_accepts_dynamic_metadata_and_rejects_mismatch. Also exercise missing/inconsistent artifact metadata; no actual publication required.
**Related:** TST-015 remains the separate type-gate issue. Owner D-19 rejects PyPI publication; this correction concerns the existing GitHub Release path only.
**Evidence/recommendation limits:** Workflow grants contents:read/packages:write and has no CI-job dependency. Upload authorization and other hosted prerequisites remain unverified; fixing this expression alone is not proof the whole release will succeed. The raw outcome-only fix is made concrete above without inventing a built artifact or available plugin.

#### OPS-008 — [MEDIUM] Developer command names a nonexistent module

**Anchor:** `README.md::"uv run -m mko_get_medioscope_data run"`
**Disambiguator:** `mko_get_medioscope_data`
**Checked:** ran
**Why this band:** Copying the procedure prevents startup, with an obvious corrected-name workaround: MEDIUM documented-operability failure, beyond LOW wording drift without runtime effect.
**Problem:** The developer example misspells the package module, and several report paths repeat that misspelled settings-directory basename.
**Repro:** Existing interpreter invoked with the documented module and --help exits 1 with No module named mko_get_medioscope_data. Metadata and the actual package/PathResolver use mko_get_mediascope_data; the synthetic AppPaths check finds no misspelled basename.
**Impact:** Contributors cannot start the copied command, and corrected commands can still reference a nonexistent example settings path.
**Root cause:** Operational examples diverged from the package and configuration-root identity.
**Fix:** Correct the developer module spelling and all corresponding README configuration-path examples. Explain that settings live in the platformdirs-selected user directory rather than promising one universal Linux-style path. Preserve actual CLI/console-script identities.
**Verify:** Manually smoke-check the corrected module --help and inspect generated initialization path on the documented platform. No dedicated string-matching regression test for a typo is required.
**Related:** ENT-008/009 own different public-host/bootstrap failures.
**Evidence/recommendation limits:** Probe uses python -m as the module-resolution equivalent of uv run -m; it does not test uv environment setup or a fresh platform installation. Literal README/module anchors and tracked paths resolve.

#### OPS-009 — [MEDIUM] Renaming does not convert legacy YAML syntax

**Anchor:** `README.md::"Migrating from YAML to JSON5"`
**Disambiguator:** `the content format is otherwise unchanged`
**Checked:** ran
**Why this band:** The documented migration leaves representative valid settings unreadable; content conversion is a practical workaround. This is MEDIUM operability degradation, not hypothetical schema incompatibility or a failed production rollback.
**Problem:** YAML block sequence/mapping syntax is not JSON5 syntax. Changing the suffix alone cannot make it parse through the current JSON5 loader.
**Repro:** A representative YAML report parses and validates as current ReportSettings; preserving those bytes under .json5 causes json5 ValueError. Current json5_to_dict directly uses the same parser. Value/schema compatibility is therefore isolated from serialization compatibility.
**Impact:** Users following rename-only advice cannot load valid hand-authored reports expressed in ordinary YAML block syntax.
**Root cause:** Migration prose conflates retained setting semantics with unchanged serialization grammar.
**Fix:** Explain manual syntax conversion as well as renaming, and show a JSON5 array/object example with quoted ISO date strings preserving the sample values. Rename-only is sufficient only if existing content is already JSON5-compatible. Do not require an automatic migration feature or overwrite user configuration to fix this prose.
**Verify:** Parse the documented converted example with json5 and validate its entry with ReportSettings as documentation QA; no brittle README-text unit test is required.
**Related:** Phase05 bundled-template validation is a different scope; no current bundled-template defect is inferred.
**Evidence/recommendation limits:** No real legacy user file/installed old version was tested. Both literal README anchors and current loader path resolve; representative failure suffices to refute the universal rename-only instruction.

## Fix Order

- **Distribution/dispositions:** MEDIUM 3; CRITICAL/HIGH/LOW 0. Confirmed 3; all other dispositions 0.
- **Dependencies/merges:** No mandatory dependency between findings. The two README edits can land together, with independent module-resolution/conversion QA. Preserve current tag comparison semantics; do not invent a new normalization policy from unbuilt metadata.
- **Conflicts/ownership:** Visible local version failure does not establish an unchecked publication or guarantee success after repair. Earlier current-run product/type/test/integration findings retain ownership; no secret, worker, retry, CSV or coverage defect is refiled as operations.
- **Rollout risk:** Require matching artifact versions/tag and fail closed on missing metadata before upload. Check actual authorization separately; no permission expansion is approved here. Preserve user values and platform path distinctions in docs. Owner D-19's no-PyPI decision remains unchanged.

## Validation-Level Findings

No new distinct VAL issue is allocated. Known generic template/namespace issues remain references to their recorded owners; metadata spelling OPS versus OPS- does not create a second finding namespace.

## Appendices

### A — Independent bounded evidence

Validator command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/ops_probe.py`, exit 0:

```text
release_exact_version_expression: KeyError 'version'
project_dynamic_fields: ['version']
release_permissions: {'contents': 'read', 'packages': 'write'}
build_backend_available: True
required_scm_plugin_available: False
readme_module_command_exit: 1
readme_module_not_found: True
actual_user_settings_has_misspelled_name: False
representative_yaml_model_valid: True
renamed_yaml_json5_parse: ValueError
reused_phase11_gate_input_hash_changes: []
package_import_name: mko_get_mediascope_data
console_script_names: ['mko-get-mediascope']
package_data_settings_paths_matched: 26
package_data_missing_bundled_settings: 0
```

The local expression failure is decisive for OPS-007; no full Ubuntu job/build was run. The misspelled-module subprocess and unchanged-byte YAML/JSON5 contrast decide the README findings. Package-data counts and reused green controls do not approve a built artifact or establish the three findings by themselves.

### B — Phase-99 rulings and completion

- **Control scope/provenance:** Actual CI runs sync/lint/tests/mypy sequentially; release separately runs sync/build/version/upload on v* tags, without a dependency on CI. Existing phase11 source/config hashes match, so its prior measured gate scope is reusable context (165 tests, 28 mypy inputs); this validator did not rerun it. No zero-item gate, branch protection, successful upload or deployment state is inferred from configuration alone.
- **Checklist dependence:** Version identity/guard follows blocks 2–4/7; failing operator instructions follow block 6. The findings rely on independent concrete observations/static workflow ordering, not the raw coverage table. No retained finding requires an unenumerated angle or a presumed Docker/database/edge tier.
- **Requirements/type:** Dynamic setuptools-scm ownership is explicit. Actual package/parser behavior is load-bearing for README correction; no product migration automation or PyPI distribution is required. Documentation moves for OPS-008/009; release code moves for OPS-007.
- **Namespaces:** Phase12 declares OPS-. Historical Git evidence at baseline 4c08eb4 reserves OPS-001–006; OPS-007–009 continue that namespace, with no collision or new compound form. Scoped search found three numeric OPS markers in overview docs, none in source/tests/workflows. They are historical provenance to qualify by report/commit, not current findings. The workflow's historical shipped-marker premise is not substituted for the observed inventory. Existing shared VAL reuse remains VAL-008.
- **Template/fields:** No closed prefix list lives in the controlled template. Known malformed generic severity pointer remains VAL-007; raw phase12 carries exact severity_taxonomy once, with the actual workflow (missing/renamed 0). All required per-finding fields, semantic anchors, original IDs and dispositions are retained here, with correct scope/status and separate validation scale. Primary literal tokens resolve in tracked workflow/README/pyproject files.
- **Evidence limits:** No finding is unsettled: local expression, module lookup and parser incompatibility are decided. Hosted release, token authorization, built contents, fresh install and older-version compatibility remain explicitly unverified, not confirmed absence or success. Historical stale reports removed by user authorization supply no fresh verdict.

Ready for tech-lead acceptance: three confirmed MEDIUM findings, no new distinct VAL issue. Only this assigned report and existing scratch probe artifacts were written; source/tests/config/docs/.kilo/index/raw and earlier reports were not edited.
