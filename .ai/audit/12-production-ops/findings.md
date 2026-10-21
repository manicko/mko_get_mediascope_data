---
phase: "12"
phase_name: "production-ops"
id_prefix: "OPS"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/12-production-ops/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Production Operations

## Summary

Three MEDIUM problems obstruct release or documented operator procedures. The release procedure cannot complete its version check, and two documented setup/migration instructions fail with representative local inputs. Each has a practical workaround; no production publication or vendor request was attempted.

## Scope

**Examined:** all seven phase blocks: automated-control scope, CI/release dependencies, local gates, packaging identity, logging, runbooks, release/rollback. Current working files are authoritative; HEAD is provenance, not an atomic snapshot identifier. Existing external changes, including `.kilo/agents/tech-lead.md`, were preserved.
**Tools:** static reads, `rg`, Git read-only inventory, bounded existing-venv Python probe; phase11 gate evidence reused after input-hash comparison returned no changes.
**Not verified:** hosted Actions execution, repository branch protections/default token policy, actual release upload, wheel/sdist contents, installation into a fresh environment, older-version rollback/config compatibility, or real Mediascope behavior. Existing environment lacks the required setuptools-scm build plugin; no install/build isolation was attempted. Local version-expression behavior was reproduced, not an entire Ubuntu workflow.
**Provenance:** historical OPS-001–006 remain reserved; current source/tests/workflows, audit reports and preserved pre-resume reports contained no OPS-007–009 collision. Findings below were independently derived from current files.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| OPS-007 | MEDIUM | `.github/workflows/release.yml::"Verify version matches git tag"` | Tag release version check reads an absent metadata field |
| OPS-008 | MEDIUM | `README.md::"uv run -m mko_get_medioscope_data run"` | Documented developer command names a nonexistent module |
| OPS-009 | MEDIUM | `README.md::"Migrating from YAML to JSON5"` | Rename-only migration leaves valid YAML unreadable |

#### OPS-007 — [MEDIUM] Tag release version check reads an absent metadata field

**Anchor:** `.github/workflows/release.yml::"Verify version matches git tag"`
**Anchor:** `pyproject.toml::"dynamic"`
**Disambiguator:** `['project']['version']`
**Checked:** ran
**Type:** [SPEC-DEVIATION]
**Why this band:** degraded release operability with a workaround. This fails visibly; it does not silently publish an unvetted artifact or demonstrate an irreversible rollback failure.
**Problem:** the active tag-release job indexes a static project version, while metadata declares the version dynamic and provides no such key.
**Repro:** execute the workflow's exact Python expression against current pyproject metadata. It raises `KeyError: 'version'` before a version value can be compared (Appendix A).
**Impact:** a maintainer's `v*` tag release reaches a failing version-check step after build, preventing the subsequent upload step if earlier steps succeeded.
**Root cause:** release verification and setuptools-scm version ownership disagree.
**Fix:** align `.github/workflows/release.yml::"Verify version matches git tag"` with the version actually assigned to the built distribution. Benefit: usable tag releases; change size: small; advisory priority: first.
**Verify:** new test needed: test_release_version_check_accepts_dynamic_metadata_and_rejects_mismatch
**Related:** TST-015 owns the separate typecheck diagnostic; this finding does not claim the release workflow depends on CI.

**Evidence — current workflow and metadata (static excerpts):**
```text
VERSION=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
[project]
dynamic = ["version"]
```

#### OPS-008 — [MEDIUM] Documented developer command names a nonexistent module

**Anchor:** `README.md::"uv run -m mko_get_medioscope_data run"`
**Disambiguator:** `mko_get_medioscope_data`
**Checked:** ran
**Type:** [DOC-UPDATE]
**Why this band:** a documented procedure fails at runtime, with an obvious corrected-name workaround; this exceeds harmless wording drift.
**Problem:** the developer run example misspells the import name. Report-path examples repeatedly use the same misspelled configuration-directory basename.
**Repro:** invoke the documented module name through the existing interpreter with `--help`, avoiding credentials or configuration writes. It exits 1 with `No module named mko_get_medioscope_data` (Appendix A).
**Impact:** contributors copying the developer command cannot start the tool. Correctly spelled module commands still point at the wrong example directory when that misspelled path is retained.
**Root cause:** README operational examples diverged from package and resolver names.
**Fix:** correct `README.md::"uv run -m mko_get_medioscope_data run"` and its configuration-path examples to match actual package/platform-resolved paths. Benefit: executable setup instructions; change size: trivial; advisory priority: second.
**Verify:** new test needed: test_readme_module_examples_resolve_installed_package
**Related:** ENT-008/009 cover distinct public wrapper/bootstrap contracts, not this documentation typo.

#### OPS-009 — [MEDIUM] Rename-only migration leaves valid YAML unreadable

**Anchor:** `README.md::"Migrating from YAML to JSON5"`
**Disambiguator:** `the content format is otherwise unchanged`
**Checked:** ran
**Type:** [DOC-UPDATE]
**Why this band:** an inaccurate migration procedure prevents loading representative user input; converting the content is a practical workaround.
**Problem:** the migration text says hand-authored YAML reports need renaming while content remains unchanged. Current loading uses JSON5 syntax; YAML sequence/mapping syntax is not accepted by that parser.
**Repro:** parse the Appendix A fixture as YAML and validate its entry with current `ReportSettings`; both succeed. Preserve its bytes under a `.json5` name and parse with current json5: `ValueError`.
**Impact:** users following the rename-only advice retain unreadable reports despite having settings values accepted by the current model.
**Root cause:** migration advice conflates compatible settings semantics with compatible serialization syntax.
**Fix:** correct `README.md::"Migrating from YAML to JSON5"` to explain content conversion for hand-authored YAML. Benefit: recoverable migration; change size: small; advisory priority: third. No automatic migration feature is required by this finding.
**Verify:** new test needed: test_documented_legacy_report_conversion_produces_valid_json5
**Related:** phase05 template validation found no current bundled-template mismatch.

## Fix Order

- **Dependencies:** None.
- **Merge candidates:** OPS-008/009 can share a README correction; their failure mechanisms remain separate.
- **Conflicting evidence:** None. Local reproductions do not establish hosted workflow outcomes beyond the demonstrated failing expression.
- **Cross-phase conflicts:** credential output remains CFG-012; worker lifecycle ASY-008/010; sync timeout concurrency ASY-009; export/continuation PIPE-012/013/014; vendor integration EXT-009–011; type/test issues QLT-021/022 and TST-013–015. These mechanisms are not refiled as operations findings.
- **Rollout risk:** OPS-007 must preserve tag/artifact-version mismatch rejection and needs a dry-run verification before publishing. OPS-008 must preserve actual CLI spelling and platform-dependent paths. OPS-009 must retain users' configuration values during conversion; test the converted sample against current models.

## Appendices

### Appendix A — Bounded hermetic reproductions

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/ops_probe.py`
Exit 0; 2.814 seconds. Exact output:
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
The boolean module diagnostic checks the captured subprocess stderr; it is not a successful CLI execution. The AppPaths check uses an isolated synthetic root and is not evidence of a fresh platform installation. The release-permissions result is inventory only; actual upload authorization was not tested.

Representative YAML fixture, stored only under scratch as `renamed-legacy-report.json5`:
```yaml
- report_type: crosstab
  media: TV
  relative_path: audit
  period:
    date_filter: [2026-01-01, 2026-01-02]
```
YAML parsing plus current model validation demonstrates value compatibility only. JSON5 rejection demonstrates serialization incompatibility; no legacy installed release or real user report was used.

### Appendix B — Coverage and verification boundaries

| Block | Evidence and boundary |
|---|---|
| Control scope | CI explicitly loads Ruff `src tests`, pytest configured tests/default slow/coverage floor, and mypy `src tests`. Reused phase11 measurements: 28 mypy source files, 165 tests, coverage 1042/1194 statements. Ruff current explicit paths and no-cache invocation succeeded; it does not print a per-file count. No configured secret-scan gate was discovered or inferred. |
| CI/release | CI push/PR job runs frozen dependency sync, lint, tests, typecheck sequentially. Release tag job independently syncs/builds/checks version/uploads. No `needs`/workflow dependency connects release upload to CI; current OPS-007 blocks its own path. No claim of a currently successful unchecked publication; remote branch protections are unknown. |
| Gate vs report | Each CI command's nonzero result blocks later steps in that job. Local AGENTS commands target the same tool configuration; Makefile `all` is lint plus tests, with typecheck separate. Hosted enforcement and pre-commit installation were not exercised. |
| Packaging identity | Pyproject name/import are `mko_get_mediascope_data`; console script is `mko-get-mediascope`; module entry exists. Explicit package-data patterns match all 26 settings paths. MANIFEST contains a misspelled recursive include, but explicit package-data prevents inferring missing content from that alone; current artifacts were not built. |
| Input/integrity inventory | No base image. GitHub Actions use major tags, runner `ubuntu-latest`, Python `3.12`: mutable references, not digest-fixed. Backend requirements are minimum ranges; uv.lock fixes resolved packages with artifact hashes for frozen CI sync. README Git URL install is unpinned; package dependency ranges permit resolver changes. No immutability/reproducibility promise or exploit was established. |
| Observability | Current services lazily validate logging settings then use dictConfig; historical eager-import premise is false. Root logger reaches console and UTF-8 rotating file (2MB, two backups). Network retries/exhaustion, service failures and CLI errors have emitted log/error paths. Consumers are local console/file readers; no deployed external collector is declared. Signal absence is not inferred from missing metrics. Known secret/worker issues retain prior ownership. |
| Runbooks | CLI/module identities and install/run/migration claims were compared with metadata, resolver, parser and code. OPS-008/009 show concrete failing steps. Environment-dependent `uv run` bootstrap after pip-only installation was not tested. |
| Release/rollback | Tags drive setuptools-scm; uv.lock controls frozen dependency resolution. README default-branch Git install does not identify an immutable rollback target, but no guaranteed rollback procedure is declared. No old-version package/config round trip was performed, so compatibility is unverified rather than presumed broken. Current init skips existing files unless force is explicit; atomic per-file replacement and credential-name migration are present. Prior isolated init evidence is retained by phase02; no real user directory was touched here. |

### Appendix C — Reused gate evidence

Hash comparison in Appendix A covers the recorded phase11 source/test/config/CI inputs; no change was detected. Existing phase11 output is retained in `.ai/tmp/audit-2026-10-21/phase11-gate-results.md`, `phase11-pytest-output.txt` and `phase11-pytest-result.json`. No broad suite was repeated.

Ruff existing-venv equivalent of AGENTS command: `.\.venv\Scripts\python.exe -m ruff check --no-cache src tests` (exit 0):
```text
All checks passed!
```
Mypy existing-venv equivalent: `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src tests` (exit 1):
```text
tests\test_network.py:541: error: Function does not return a value (it only ever returns None)  [func-returns-value]
Found 1 error in 1 file (checked 28 source files)
```
The complete prior native pytest stdout/stderr remains in the referenced scratch output; recorded exit 0, 165 passed, one warning, 87.27% coverage against a 70% floor, outer elapsed 92.144 seconds. That run blocked live Requests transport and included slow tests. These are prior measured results, not a claim that this phase reran pytest or verified a hosted CI job. Existing mypy failure is owned by TST-015.
