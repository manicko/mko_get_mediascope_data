---
phase: "10"
phase_name: "code-quality"
id_prefix: "QLT-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/10-code-quality/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Code Quality

## Summary

Two findings, one MEDIUM and one LOW. The advertised package-level API loses its available static argument checks. Its lazy-export explanation also suggests a stronger import guarantee than the implementation provides.

## Scope

**Examined:** All nine blocks: named fixed values/constant surfaces; model/input boundaries; configured type enforcement/suppressions; responsibilities; repeated rules/helpers; mechanical conventions/reachability; declared-contract enforcement; template drift gates. Production scope is 14 Python files/111 functions. Source, tests, current AGENTS/pyproject/CI/pre-commit and relevant overview docs were inspected. Historical QLT-001–020 are reserved; active corpus and Git history were searched before allocating QLT-021/022. Example-template identifiers are not treated as new findings.

**Snapshot:** Current dirty/staged working tree at base HEAD b27b870 on 2026-10-21, not pristine HEAD. External network/report/service/test/application-settings changes and restored/cleaned audit artifacts were preserved. Earlier phase inputs remain frozen. Exact tool outputs and reuse limits: `.ai/tmp/audit-2026-10-21/phase10-tool-results.md`; current Python input digests: `phase10-tool-input-hashes.json` in the same scratch directory.

**Tools:** Existing venv Ruff/mypy, read-only PowerShell/rg/Git, AST/introspection and bounded local `quality_inventory.py`/`quality_public_api.py`. No dependency sync/install, full pytest, native-loop probe or real API call.

**Limits:** The type reproduction explicitly includes src, so it measures the lazy re-export rather than installed-package typing-marker discovery. It does not measure third-party packaging/type-checker variants. Mypy src/tests reports one test-only diagnostic, reserved for phase11; diagnostic count is not itself a production finding. No broad Any/size/extra-policy complaint or hypothetical schema-drift corruption is filed.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| QLT-021 | MEDIUM | `src/mko_get_mediascope_data/__init__.py::__getattr__` | Lazy public exports erase the source API's static signature |
| QLT-022 | LOW | `src/mko_get_mediascope_data/__init__.py::__getattr__` | Single-name import explanation overstates deferred module execution |

#### QLT-021 — [MEDIUM] Lazy public exports erase the source API's static signature

**Anchor:** `src/mko_get_mediascope_data/__init__.py::__getattr__`
**Anchor:** `src/mko_get_mediascope_data/app.py::run_report`
**Disambiguator:** `def __getattr__(name: str) -> Any:`
**Checked:** ran
**Why this band:** A bounded type-safety/operability gap for library users: an available argument check disappears at the advertised public boundary. MEDIUM, not HIGH/CRITICAL: no stored value is lost and invalid input still raises at runtime.
**Problem:** Package-level API names are provided only through an Any-returning module getter. Mypy sees `from mko_get_mediascope_data import run_report` as Any even when it analyzes the actual source tree and knows the direct app.run_report signature. AGENTS requires strict type safety and the README advertises the package-root import.
**Repro:** The scratch type probe imports both forms, reveals their types and calls each with integer 123. With src explicitly included, mypy diagnoses only the direct-module call; the equivalent package call is unchecked. Runtime introspection confirms the exported callable actually has the typed str-or-Path signature; an invalid public async call raises TypeError before reaching service/API work.
**Impact:** Library users following the advertised import lose argument checking and discover errors only during execution, despite the implementation already declaring the intended contract.
**Root cause:** Lazy runtime re-export has no corresponding static public-name declarations/signatures.
**Fix:** Preserve the static type contract of public names exposed by `__getattr__` while retaining deferred runtime imports. Benefit: callers receive the same checks through both supported imports; effort small; advisory priority medium. Do not introduce eager runtime imports that change the existing lazy contract. [SPEC-DEVIATION]
**Verify:** new test needed: test_public_package_exports_preserve_static_function_signatures
**Related:** ENT-008 owns notebook entry usage, not this type-erasure mechanism.

**Evidence — `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src .ai/tmp/audit-2026-10-21/quality_public_api.py`** *(supports: the same invalid argument is checked only through the direct module)*:
```text
.ai\tmp\audit-2026-10-21\quality_public_api.py:4: note: Revealed type is "Any"
.ai\tmp\audit-2026-10-21\quality_public_api.py:5: note: Revealed type is "def (report_path: builtins.str | pathlib.Path, progress: mko_get_mediascope_data.core.progress.ProgressReporter | None =)"
.ai\tmp\audit-2026-10-21\quality_public_api.py:7: error: Argument 1 to "run_report" has incompatible type "int"; expected "str | Path"  [arg-type]
Found 1 error in 1 file (checked 15 source files)
```
The equivalent invalid package call is probe line 6; only line 7 receives a diagnostic. Exit 1 is expected for the direct-call positive control.

#### QLT-022 — [LOW] Single-name import explanation overstates deferred module execution

**Anchor:** `src/mko_get_mediascope_data/__init__.py::__getattr__`
**Disambiguator:** `name does not eagerly execute`
**Checked:** ran
**Why this band:** Documentation drift with no established runtime damage, exactly LOW. The lazy implementation itself is not asserted broken.
**Problem:** The getter's docstring says importing the package or accessing any single name does not eagerly execute app.py's module body. Bare import defers it, but retrieving a public name calls importlib.import_module and executes the whole app module on first access. The explanation conflates deferred bare import with name-level module-body avoidance.
**Repro:** In a fresh process import only the package, inspect sys.modules, access run_report, inspect again. The app module changes from absent to present; the getter's source directly imports that module.
**Impact:** Maintainers can incorrectly infer that resolving a setup/public function avoids the entry module and its dependencies. Current execution is expected lazy loading; this finding only corrects its stated boundary.
**Root cause:** The docstring describes stronger name-level deferral than the implementation's module-level lazy loading.
**Fix:** Clarify `__getattr__`'s docstring: package import defers app.py, and first public-name access loads it. Benefit: future dependency/startup reasoning uses the actual boundary; effort trivial; advisory priority low. [DOC-UPDATE]
**Verify:** new test needed: test_first_public_name_access_loads_app_module
**Related:** QLT-021 shares the getter but concerns static types, not this prose.

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/quality_inventory.py`** *(supports: first public-name access loads app.py)*:
```text
app_loaded_after_bare_package_import: False
app_loaded_after_single_public_name_access: True
runtime_public_signature: (report_path: str | pathlib.Path, progress: mko_get_mediascope_data.core.progress.ProgressReporter | None = None) -> None
runtime_invalid_public_argument: TypeError
```

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 0; MEDIUM 1; LOW 1.
- **Dependencies:** None. Preserve runtime lazy imports while correcting public static declarations.
- **Merge candidates:** QLT-021/022 can be reviewed together because both touch the getter; their type/prose acceptance checks remain distinct.
- **Conflicting evidence:** Broad untyped-import behavior in a probe without src did not isolate the getter and is not used as proof. Explicit src analysis provides the direct typed-call control.
- **Cross-phase conflicts:** Existing entry, configuration, worker, CSV/continuation and integration findings retain their validated owners. The test-only mypy diagnostic belongs to phase11. Deliberate permissive API extras are not a new schema finding.
- **Rollout risk:** QLT-021 must preserve `tests/test_app.py::test_package_import_does_not_bootstrap`, public callable identity and import errors for unknown names. QLT-022 changes only the explanation. No runtime eager import is proposed.

## Appendices

### Appendix A — Gate results and runtime inventory

Command: `.\.venv\Scripts\python.exe -m ruff check --no-cache src tests` (exit 0, about 1.24s).
```text
All checks passed!
```
Command: `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src tests` (exit 1; total cold-cache duration not captured).
```text
tests\test_network.py:541: error: Function does not return a value (it only ever returns None)  [func-returns-value]
Found 1 error in 1 file (checked 28 source files)
```
This is recorded for phase11 reuse and is not a production finding. Input hashes and full reproduction/control outputs are in the scratch files named in Scope.

Complete `quality_inventory.py` output (exit 0, about 2.7s):
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
*(supports: QLT-021/022 and scope inventory; missing annotations or unreferenced direct calls are not automatically independent findings.)*

### Appendix B — Nine-block coverage and explicit limits

| Block | Evidence/assessment |
|---|---|
| 1: Fixed values | MediaType/ReportType/ReportSubtype/TaskStatus/VerbosityMode have named Enum/StrEnum homes and are used for corresponding control flow. Vendor task type serialization widens to str at its installed API boundary; this is load-bearing. Period time-unit and language values use Literal constraints rather than Enum; remote terminal statuses are a separate raw vendor vocabulary. No named-domain home demonstrably bypassed with a current wrong value was established, so an enum-conversion preference is not promoted into a defect. |
| 2: Constant surface | Enum families use semantic names rather than arbitrary numeric identifiers. Connection strategy mapping is external service wiring, not an alternative membership definition. Verbosity's log-level mapping is owned by its enum method; no divergent second production mapping was found. Public export list has no private identifier. Template/sample QLT IDs and reserved historical population were distinguished before minting new IDs. |
| 3: Boundaries | JSON5 application/log/report/default models validate before their intended service consumers. Credential JSON is vendor-owned (phase02/09), remote responses become vendor DataFrames (phase09/04), and CSV/error persistence follows the established pipeline. Permissive Data/ReportSettings extras are deliberate vendor passthrough, not an undocumented shared-base exception. Absent client field overwrite remains CFG-011, not refiled. |
| 4: Types/suppressions | Mypy configured options include check_untyped_defs, strict_optional, disallow_any_generics, redundant-cast/unused-ignore warnings, ignore_missing_imports and silent followed imports; no global strict/disallow_untyped_defs/disallow_any_expr bar is assumed. Production suppressions found: pyright reportUnsupportedDunderAll=false and Ruff E731 for dynamic package export/dir code. No production type-ignore cluster. Generic NetworkClient uses TypeVar T; vendor/pandas/config extras use load-bearing dynamic values. QLT-021 isolates accidental static erasure at the public boundary; 22 missing return annotations alone are not filed. |
| 5: Responsibilities | Inspected AppService.run_report orchestration, worker item isolation, send/wait/extract stages, Rich reporter lifecycle, DictDataStrategy transform and utils pivot/period helpers. Stage methods and strategies are directly reachable by existing tests; bulk in dictionary reshaping is one cohesive transformation rather than a line-count defect. No additional responsibility split with a demonstrated current consequence was established. |
| 6: Rule ownership | Canonical service paths use PathResolver; the older utils.resolve_path also creates missing paths and has no in-repo direct caller. Its own documented capability differs from resolve-only behavior, so it is not declared a second live implementation with proven divergent users. Regional/default task-name handling is adjacent to existing PIPE-014. Shared progress protocol remains Rich-free; no new contradictory live derived-quantity implementations were established. Arithmetic comparison belongs to phase17. |
| 7: Cheap conventions | AST found no bare production print calls; Rich Console.print is confined to entry output. Commented historical print calls do not execute. The Python-source language sweep found no Cyrillic comments/log/docstrings. Ruff passed. Local importlib import implements deliberate lazy loading, not an unexplained cycle workaround. Both core package markers exist. resolve_path has no repository call site; external usage/feature intent is not established, so deletion is not prescribed. |
| 8: Contracts | Bare-import laziness has an enforcement test; first-name module execution contradicts the getter prose (QLT-022). Static source function typing exists but is erased by root export (QLT-021). Integration's pre-commit/single-send and constructor-local claims are EXT-009/011. Broad strict-typing/no-Any wording in AGENTS exceeds current configured checks; this review files the concrete public check loss rather than counting every dynamic boundary as a violation. |
| 9: Drift gates | CI runs Ruff, pytest and mypy; local pre-commit declares Ruff/format/mypy. Tests iterate bundled report models, subtype defaults and kitId legend, with service/default tests covering all media/type files. Phase05 independently validated all current templates/merged fields. There is no demonstrated current wrong bundled field that these gates silently accept beyond already-owned CFG-011; synthetic permissive extras alone do not prove an unintended production contract or justify forbid-all. Gate completeness for arbitrary future vendor schema changes is not claimed; full pytest was not rerun. |

Earlier phases' accepted findings and validator dispositions were treated as scope boundaries, not copied as new QLT findings. Installed-package typing-marker discovery, full test adequacy, packaging and measured performance remain outside this report's claims. No source, tests, configuration, docs, dependencies, plans, `.kilo` or Git index changes were made.
