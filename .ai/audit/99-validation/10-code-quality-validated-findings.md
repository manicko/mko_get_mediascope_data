---
phase: "10"
phase_name: "code-quality"
id_prefix: "QLT-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/10-code-quality/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Validation Report

## Summary

Both findings are confirmed: one MEDIUM and one LOW. The advertised package import loses argument checks already available through the implementation module. Its explanation also misstates when that module loads; runtime lazy loading itself remains valid.

## Scope

**Input:** Frozen `.ai/audit/10-code-quality/findings.md`, QLT-021/022; independently adjudicated on 2026-10-21 using the complete phase-99 procedure and original phase-10 rubric.
**Snapshot:** Current dirty/staged working source on b27b8708d4d816940310d4f034b549cb1db6f397, not pristine HEAD. Existing staged changes, user configuration, current reports and authorized historical-audit cleanup were preserved. Raw/validated earlier current-run reports are ownership context; historical reports are consulted only through historical Git evidence where needed.
**Examined:** Package __getattr__/exports, direct function signature and async argument validation, README public import, configured mypy behavior, existing lazy-import tests, exact scratch probes, prior phase10-tool results/input hashes and controlled report template.
**Tools:** Read-only PowerShell/rg/Git/AST, existing venv mypy and fresh-process runtime probe. Independently reran explicit-src type probe (expected exit 1) and quality_inventory.py (exit 0). Checked all 28 stored tool-input digests: none changed. No broad Ruff/mypy/pytest rerun, dependency install or source repair.
**Not verified:** Installed-package typing marker discovery, external type-checker/IDE variants, real API, real Jupyter kernel or full-suite health. The retained static finding is explicitly about analyzing actual source with src included. Earlier test-only mypy diagnostic remains phase-11 context, not a production finding here.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| QLT-021 | MEDIUM | `src/mko_get_mediascope_data/__init__.py::__getattr__` | Lazy root export loses the available static signature |
| QLT-022 | LOW | `src/mko_get_mediascope_data/__init__.py::__getattr__` | Getter explanation overstates single-name deferral |

| ID | Disposition | Final type | Final severity |
|---|---|---|---|
| QLT-021 | confirmed | SPEC-DEVIATION | MEDIUM |
| QLT-022 | confirmed | DOC-UPDATE | LOW |

#### QLT-021 — [MEDIUM] Lazy root export loses the available static signature

**Anchor:** `src/mko_get_mediascope_data/__init__.py::__getattr__`
**Anchor:** `src/mko_get_mediascope_data/app.py::run_report`
**Disambiguator:** `def __getattr__(name: str) -> Any:`
**Checked:** ran
**Why this band:** The narrow audience is library callers using the documented package-root import: a typed boundary becomes Any and loses existing argument checks. This matches MEDIUM's bounded type-width/convention gap. No stored corruption, divergent rule or guaranteed future schema failure establishes CRITICAL/HIGH. Runtime rejection remains available, so this is not a claim of silently accepted production data corruption.
**Problem:** Known public callables exist only through an Any-returning dynamic getter, with no static declarations for those names. Mypy can infer the direct implementation signature while the advertised root name is Any, even when both are analyzed from the same source tree.
**Repro:** Existing quality_public_api.py imports root run_report and app.run_report, reveals their types and passes integer 123 to each. Independent explicit-src mypy invocation flags only the direct call; root call remains unchecked. Runtime introspection sees the actual typed signature, and the invalid async argument raises TypeError before service/API work.
**Impact:** Library users lose type-checker feedback for an invalid argument at the documented import seam and discover the error during execution instead.
**Root cause:** Intentional dynamic runtime exporting is not paired with static public-name bindings. The underlying function annotations already exist; widening to Any at this seam is accidental rather than a necessary vendor/dynamic-data boundary.
**Type rationale:** SPEC-DEVIATION retained against AGENTS' type-safety rule and the declared public import, with the concrete missing check isolated. The rule is applied to this narrow boundary, not used to promote every existing dynamic annotation into a finding.
**Fix:** In package __init__.py import TYPE_CHECKING and add an `if TYPE_CHECKING:` block with explicit same-name re-exports from .app, including `run_report as run_report`, `run_report_async as run_report_async` and the other declared public names. Explicit aliases preserve no_implicit_reexport behavior. Keep the runtime __getattr__/globals cache and __all__ behavior; the guarded imports must not execute during ordinary import. Use implementation signatures rather than duplicating handwritten callable signatures.
**Verify:** new test needed: test_public_package_exports_preserve_static_function_signatures. Explicit-src checking should reveal the same callable types and reject both invalid calls while accepting supported arguments. Preserve tests/test_app.py::test_package_import_does_not_bootstrap, test_lazy_import_same_object_as_app_module and test_lazy_import_raises_attribute_error.
**Related:** ENT-008 owns notebook entry usage; package typing is not that runtime host contract. Installed distribution typing metadata belongs outside this bounded source-level claim.
**Anchor/cause decision:** AST resolves __getattr__ and its Any return; the exact primary declaration is present. app.run_report has typed parameters. README uses the root import. The explicit-src positive control rules out attributing both imports' untyped behavior to package discovery alone.
**Recommendation assessment:** Raw fix was outcome-only; the TYPE_CHECKING alias declarations above give a concrete approach at existing targets without eager runtime imports or a new handwritten signature maintenance surface.

#### QLT-022 — [LOW] Getter explanation overstates single-name deferral

**Anchor:** `src/mko_get_mediascope_data/__init__.py::__getattr__`
**Disambiguator:** `name does not eagerly execute`
**Checked:** ran
**Why this band:** A misleading explanation with no demonstrated runtime damage is exactly LOW documentation drift. The implementation correctly loads the module lazily on first name access; no stronger functional failure is claimed.
**Problem:** The getter docstring says importing the package or accessing a single name does not execute app.py's module body. Bare import defers it, but first public-name access imports the app module and executes its body.
**Repro:** Fresh-process inventory imports only the package, checks sys.modules, accesses run_report, then checks again. app module is absent before access and present afterward. Source importlib.import_module supplies the actual execution path.
**Impact:** Maintainers can reason incorrectly about which dependencies/module initialization are avoided by accessing a public setup or report name.
**Root cause:** The docstring conflates package-level lazy loading with deferring app module execution after its attribute is requested.
**Fix:** Rewrite __getattr__'s docstring to state that bare package import defers app.py, and the first supported public-name access loads it and caches the resulting object. Preserve the current behavior.
**Verify:** new test needed: test_first_public_name_access_loads_app_module, using an isolated process and no service bootstrap. Existing public object identity/bare-import tests remain valid.
**Related:** QLT-021 shares the getter but concerns static type exposure; no merge of the separate acceptance checks.
**Anchor/cause decision:** Getter and literal prose token resolve. The fresh-process observation independently agrees with the direct import statement; it is not inferred from a warm interpreter or an already loaded implementation module.
**Recommendation assessment:** Existing docstring target and specific corrected wording are concrete. No runtime change or dependency prerequisite is needed.

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 0; MEDIUM 1; LOW 1.
- **Disposition tally:** confirmed 2; re-typed 0; re-graded 0; merged 0; not substantiated 0; unsettled 0. Two original IDs, two decisions.
- **Dependencies:** None. Both can be reviewed together, but preserve static checking and runtime import-boundary checks independently.
- **Merge candidates:** None; static declarations and docstring accuracy have different consequences/verification.
- **Conflicting evidence:** Without src included, both imports can appear Any; that earlier control does not isolate the getter and is not confirmation. With src included, the direct typed call supplies the required positive control. Runtime callable typing does not imply static discoverability through __getattr__.
- **Cross-phase conflicts:** Compared current-run entry/configuration/concurrency/pipeline/integration findings and dispositions. ENT-008 and EXT-009/011 retain runtime-entry/integration-documentation ownership; no such mechanism is refiled. The earlier test-only mypy diagnostic belongs to phase 11. Source public type widening and getter prose consistency belong to phase 10.
- **Rollout risk:** Eager imports would break the currently intentional lazy behavior; use TYPE_CHECKING-only aliases. Keep callable identity, __all__, version fallback and unknown-name AttributeError. Documentation-only QLT-022 must not redesign module loading.

## Validation-Level Findings

No new distinct VAL issue is allocated. Known template-pointer/namespace/recommendation concerns are references to their existing owners rather than duplicate findings. The phase workflow's historical warning about shipped QLT provenance is not treated as evidence of current markers: the actual scoped search found zero, as recorded below.

## Appendices

### Appendix A — Independent focused evidence

Command: `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src .ai/tmp/audit-2026-10-21/quality_public_api.py`.
Exit 1, expected positive-control failure, 2.22 seconds:

```text
.ai\tmp\audit-2026-10-21\quality_public_api.py:4: note: Revealed type is "Any"
.ai\tmp\audit-2026-10-21\quality_public_api.py:5: note: Revealed type is "def (report_path: builtins.str | pathlib.Path, progress: mko_get_mediascope_data.core.progress.ProgressReporter | None =)"
.ai\tmp\audit-2026-10-21\quality_public_api.py:7: error: Argument 1 to "run_report" has incompatible type "int"; expected "str | Path"  [arg-type]
Found 1 error in 1 file (checked 15 source files)
```

Probe line 6 contains the equivalent invalid root call; it receives no diagnostic. No probe is executed as a user report. The explicit src inclusion and directly checked invalid call distinguish the getter issue from installed-package typing-marker discovery.

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/quality_inventory.py`.
Exit 0, 3.56 seconds:

```text
app_loaded_after_bare_package_import: False
app_loaded_after_single_public_name_access: True
runtime_public_signature: (report_path: str | pathlib.Path, progress: mko_get_mediascope_data.core.progress.ProgressReporter | None = None) -> None
runtime_invalid_public_argument: TypeError
production_python_files: 14
production_function_count: 111
production_missing_return_annotation_count: 22
production_bare_print_calls: []
production_legacy_helper_direct_call_counts: {'resolve_path': 0, 'ensure_path_exists': 2}
```

Only the import-state/signature/invalid-argument observations substantiate these findings. Function counts, missing annotation counts and no-print inventory are not standalone defects or green-control approvals. The invalid async call reaches Path(123) and raises before service/network work; no event-loop or real API call is required.

### Appendix B — Support and artifact rulings

- **Tool-result freshness:** Compared all 28 entries in phase10-tool-input-hashes.json with current file SHA-256 values; none changed. Prior Ruff/full-source-and-test mypy results remain auditor measurements, not reruns claimed by this validator. They are context only and do not decide either finding.
- **Control existence/decision scope:** Existing tests verify bare-import bootstrap deferral, callability, unknown-name failure and runtime function identity. Those checks exercise runtime behavior; they examine zero static public signatures. They therefore do not refute QLT-021 if green. The fresh-process app-module observation directly decides QLT-022 rather than a test PASS claim.
- **Checklist dependence:** QLT-021 corresponds to type width/enforcement blocks 4 and 8; QLT-022 corresponds to declared-contract block 8. Both rely on independent focused source/runtime/type observations, not the coverage table. Neither requires an angle outside the phase's nine broad blocks. No broad style, Any-population, size or synthetic-schema issue is inferred.
- **Namespace declaration/history:** Phase 10 declares QLT-. Historical Git evidence at baseline 4c08eb4 minted QLT-001..020; retain those IDs as reserved even though obsolete reports were removed from the working audit corpus by user authorization. Current QLT-021/022 continue the same namespace, with no observed collision.
- **Shipped markers and ruling:** Scoped rg search of src/tests/docs found zero QLT markers. The workflow's assertion about already-shipped prior-cycle QLT markers is historical warning text, not a current observed inventory; no current collision is invented from it. Template QLT example rows are examples, not real allocations. Preserve historical IDs through Git/retained scratch provenance and qualify their historical source when referenced. No second or phase-qualified compound QLT namespace is introduced, and no compared current content phase reuses QLT.
- **Shared artifacts:** The controlled template has no closed prefix enumeration; phase registry/workflows supply it. Its malformed generic severity pointer remains known VAL-007; this raw report contains exact severity_taxonomy once with the correct actual workflow path (missing 0, renamed 0). This validated report uses that path and honest dirty/staged scope. Historical shared VAL reuse remains VAL-008, not a new finding here.
- **Required fields/completeness:** Both input findings contain semantic anchors, primary tokens, checked mode, rationale, problem, reproduction, impact, cause, fix, verify and related context. This report retains all fields, IDs and decisions, plus concrete fix targets/approaches, distribution, conflict/rollout analysis and independent output. Content and validation scales are not mixed.
- **Unsettled limits:** No finding remains unsettled. Actual source-level static behavior and fresh-process import state are established. Distribution typing markers, other checkers/IDEs and full-suite state remain unverified and are excluded from the confirmed impacts.

Final report consistency check (existing venv Python, exit 0):

```text
dispositions: {'confirmed': 2}
ids: ['QLT-021', 'QLT-022']
grades: [('QLT-021', 'MEDIUM'), ('QLT-022', 'LOW')]
new_VAL_headers: []
```

Ready for tech-lead acceptance: MEDIUM 1, LOW 1; no unresolved disposition or new distinct VAL issue. Only this assigned report was edited; source/tests/config/docs/.kilo/index/raw and prior reports were preserved.
