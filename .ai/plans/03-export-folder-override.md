---
id: PLAN-03
title: Execution plan for optional per-run export-folder override (Spec 03)
status: draft
created: 2026-11-11
source_spec: .ai/problems/03_export_folder_override_spec.md
source_context: Auditor code-context report (provided in planning request)
---

# Execution Plan вЂ” Spec 03: Optional Export Folder Override

## 1. Overview

### Objective
Enable callers to direct report outputs to a chosen export base directory on a **per-invocation** basis via the public library API (`run_report`, `run_report_async`) and the CLI (`run`), while preserving all existing default behavior, per-entry `relative_path` composition, resume/вЂњdoneвЂќ detection coherence, and process-wide isolation.

### Inputs
- **Spec:** `.ai/problems/03_export_folder_override_spec.md` (CR-1..CR-10, T1вЂ“T5, acceptance criteria, risks).
- **Auditor report:** Current flow is `app.run/_async -> get_app_service().run_report -> AppService.export_folder (cached, resolved relative to user_dir) -> export_path = export_folder / report_settings.relative_path -> TVMediaReport(export_path=...) -> done detection + csv/error artifacts under export_path`; singleton/cache and multi-entry/per-entry semantics are central constraints.

### Scope (minimal, in-plan)
- Add an additive, optional per-run export base override contract and plumbing across API в†’ service/orchestration в†’ CLI, with path normalization/validation consistent with existing conventions and no config/cache mutation.
- Update automated tests and user-facing docs/help as required by spec CR-9/CR-10.

### Non-goals / out of scope
- No change to `relative_path` meaning/format, report generation/data semantics, async/concurrency model, vendor behavior, new config keys/persistent preferences, or unrelated commands.
- No architectural redesign; no speculative abstractions beyond what is necessary for CR-1..CR-8.

## 2. Execution blocks

Execution order is sequential by default because contract decisions gate implementation and implementation gates CLI/tests/docs; limited parallelism is possible only after Block 1 decisions are recorded (see notes).

### Block 1 вЂ” Contract, design resolution, and risk containment (gate)

| Field | Details |
|---|---|
| **ID** | B1 |
| **Depends on** | None (entry/gating block) |
| **Unblocks** | B2, B3, B4, B5 (all downstream work must follow the resolved contract) |

**Scope**
- Resolve and document the per-run override contract before coding (spec T1, Open Decisions В§8): parameter/option name and accepted type(s), absent/default meaning, precedence over configured `export_folder`, interaction with each entryвЂ™s `relative_path` (base replacement only), and path-resolution/normalization/validation expectations (CR-3, CR-4, CR-6, CR-8).
- Determine a safe integration point so the effective base is used for **that run only** without mutating `app_config`, cached `AppService.export_folder`/`app_config`, or leaking into later invocations on the shared singleton (CR-5, spec risk вЂњSingleton/global stateвЂќ).
- Confirm coherence requirements: override applies uniformly to all entries in a run (including multi-entry definitions), to resume/`check_done` detection, to all exported artifacts (CSV, temp/atomic publish, `errors/`), and to returned `export_paths` (CR-4, CR-6, CR-7).
- Identify exact touch points and minimal change set consistent with existing patterns (`app.py`, `core/services.py`, `__init__.py` exports/signatures as needed, `core/paths.py`/resolver usage, CLI wiring).

**Risks**
- **High (isolation/leak):** per-run override can leak via cached properties or mutated service state on process-wide singleton (`get_app_service`), affecting subsequent runs/callers.
- **High (correctness/coherence):** base must drive `get_done_files`, `csv_to_file`, error artifacts, and returned paths for the same run; any divergence breaks resume/skip and user-observable destinations (CR-7).
- **Medium (semantics ambiguity):** relative override paths and interaction with `PathResolver` base (`user_dir`) vs caller CWD; absolute `relative_path` edge case in `Path(base)/relative_path` composition.
- **Medium (compatibility):** signature/export changes in public API and lazy package exports must remain backward-compatible.
- **LowвЂ“Medium (directory preparation/validation):** consistent creation/validation behavior and clear failure modes aligned with project conventions.

**Implementation-path alternatives (technical uncertainty вЂ” do NOT choose final approach here)**

1. **Per-call override parameter threaded through run methods (no state mutation)**
   - *Shape (illustrative):* accept override on `run_report_async`/`run_report` and `AppService.run_report`; compute an effective per-run base locally (resolve/ensure) and use it instead of `self.export_folder` when provided.
   - *Pros:* explicit, avoids cache mutation, naturally run-scoped; aligns with additive API change (CR-1, CR-5).
   - *Cons/implications:* requires signature propagation and careful choice of resolver/base semantics for the override (see open decisions); must not accidentally fall back to cached base inconsistently.

2. **Ephemeral/non-cached effective base computation (helper or inline)**
   - *Shape (illustrative):* derive effective base per run from `(override or app_config.app_settings.export_folder)` using a resolution strategy; may reuse or parallel existing `PathResolver`/`ensure_dir` behavior.
   - *Pros:* centralizes normalization/creation logic, keeps cached `export_folder` reserved for configured default; can make semantics testable.
   - *Cons/implications:* introduces a small helper/branch that must match existing conventions; placement (core vs service) must respect layering and avoid duplication.

3. **Temporary service/context override (e.g. context manager/copy of service) вЂ” higher risk / likely heavier than minimal scope**
   - *Shape (illustrative):* temporarily substitute base/export behavior for a run or use a per-run service instance/path context.
   - *Pros:* can isolate state without threading through every call site in some designs.
   - *Cons/implications:* risks hidden coupling with singleton/global state, config caching, and lifecycle; may violate minimal-scope/established-pattern constraints unless carefully constrained. Included as a considered alternative only; feasibility/fit must be validated, not assumed.

**Open decision points to resolve in this block (must not be finalized prematurely):**
- **Relative override path base:** resolve relative to **current working directory** (caller intent, common for CLI/ad-hoc paths) vs **user config dir** (consistency with configured `export_folder` today) vs other вЂ” implications differ for notebooks/scripts and must be explicit and tested (CR-6, spec В§8/В§9).
- **Parameter/option naming and type contract:** exact name (e.g. `export_folder`/`export_path`/`export_dir`) and accepted types (`str | Path | None` vs `PathLike | None`), kept consistent APIв†”CLI and idiomatic for codebase (`pathlib.Path` is prevalent).
- **Whether/where to add a helper:** inline in `AppService.run_report` vs a small dedicated helper/method for resolving+preparing the effective base вЂ” trade-off between clarity/testability and minimal churn; must follow existing patterns if added.
- **Validation/normalization strictness:** how to treat empty strings, non-path inputs, and directory creation failures, consistent with project conventions.

**Required agents**
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Yes** | Singleton/cache, resolver-base semantics, `relative_path` composition edge cases, and resume/artifact coherence require deeper code-path verification beyond provided summary. |
| Researcher | **Yes** | Multiple viable contract/integration approaches and modern/idiomatic path-override patterns (CLI/API parity, cwd vs config-relative semantics) with maintainability/support implications must be compared. |
| Planner | **Yes** | High-risk, cross-layer change needs detailed pre-implementation design (touch points, sequencing, test matrix) before coding. **Task descriptions for implementation work derived from this block must later be organized using template `.ai/tasks/templates/task_template.yaml` in the Execution plan context (no separate task files created now).** |
| Validator | **Yes** | Independent review needed to catch isolation leaks, resume incoherence, and compatibility risks given shared-service/global-state constraints. |

---

### Block 2 вЂ” Core/library implementation (orchestration, path handling, isolation)

| Field | Details |
|---|---|
| **ID** | B2 |
| **Depends on** | B1 (resolved contract and chosen integration approach recorded) |
| **Unblocks** | B3 (CLI can be finalized in parallel only if contract/signature is stable), B4, B5 |

**Scope**
- Implement the agreed additive API surface in `run_report` and `run_report_async` and propagate the override into report execution so the effective export base is used for that run (CR-1, CR-2 plumbing target, CR-4).
- Ensure effective base resolution/normalization and directory preparation follow the resolved contract and existing conventions; preserve `export_path = <effective_base> / relative_path` composition per entry and per-run uniformity for multi-entry runs (CR-4, CR-6, CR-7).
- Guarantee **no mutation** of `app_config`, cached `export_folder`/settings/state, or cross-run leakage on the singleton; returned export destination(s) must reflect actual written destinations as today (CR-5, CR-6).
- Keep changes minimal and localized to established layers (`app.py` public API, `core/services.py` orchestration; `__init__`/signatures/typing only as required for compatibility). Do not alter report/data semantics, task naming, or artifact formats.

**Risks**
- **High:** accidental cache/state mutation or fallback to configured base for part of the run.
- **Medium:** incorrect path base/normalization breaking user expectations or existing relative-config behavior.
- **Medium:** regressions in multi-entry handling, `NoReportFoundError`/skip behavior, or returned paths.
- **Low:** typing/signature compatibility issues for downstream consumers.

**Implementation-path alternatives (only where uncertainty remains after B1; otherwise follow B1 decision вЂ” do not preempt)**
- **If B1 leaves helper placement open:** inline computation in `AppService.run_report` (fewer symbols, minimal diff) vs small internal helper/method (clearer reuse/test seams) вЂ” choose per project conventions once uncertainty is removed; both must be run-scoped and cache-safe.
- **If B1 leaves resolver strategy open:** reuse `PathResolver` with an appropriate base vs `Path(...).expanduser()`/`resolve()` consistent with decided semantics вЂ” must not change default configured-path behavior.

**Required agents**
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Conditional/Advisory** | Engage if implementation reveals unexpected coupling (e.g. additional `export_folder` consumers, hidden cache paths, or platform-specific path behavior). Not required by default once B1 resolves uncertainties. |
| Researcher | No | B1 should settle approach/options; avoid speculative redesign during coding. |
| Planner | **Yes** | Produce/maintain detailed implementation task breakdown for this block. **Task descriptions must later be organized using template `.ai/tasks/templates/task_template.yaml` in the Execution plan (no separate task files created now).** |
| Validator | **Yes** | Independent review of the implementation diff/plan for isolation (CR-5), coherence (CR-7), and backward compatibility before merge confidence. |

---

### Block 3 вЂ” CLI exposure and parity

| Field | Details |
|---|---|
| **ID** | B3 |
| **Depends on** | B1; B2 for final callable signature/behavior (can be drafted in parallel once contract is fixed, but must align with implemented API) |
| **Unblocks** | B4, B5 |

**Scope**
- Add the agreed export-folder option to `run` (provisionally `--export-folder`, with short alias only if consistent with existing CLI conventions) and forward it to report execution (CR-2, CR-3).
- Update CLI help/usage text for the new option and its precedence/interaction with configured `export_folder` and `relative_path` at the interface level (CR-9, partial).
- Preserve existing CLI arguments/behavior, verbosity/progress handling, and service wiring semantics; no changes to unrelated commands.

**Risks**
- **LowвЂ“Medium:** CLI/API contract drift (name, type coercion, empty/absent handling) or option parsing edge cases.
- **Low:** help text ambiguity around precedence/path semantics.

**Implementation-path alternatives (non-choosing where relevant)**
- **Option typing/coercion:** Typer `Path`-typed option vs `str` then normalized вЂ” must match the resolved API contract and existing path-handling behavior; if B1 leaves this ambiguous, document and align during implementation rather than guessing now.

**Required agents**
| Agent | Required | Rationale |
|---|---|---|
| Auditor | No | Wiring is localized once contract/API are clear. |
| Researcher | No | Conventions are established in `app.py`/Typer usage. |
| Planner | **Yes (lightweight)** | Include CLI as an explicit task in the implementation breakdown. **Task description should later be organized using template `.ai/tasks/templates/task_template.yaml` in the Execution plan.** |
| Validator | No (covered by B2/B4 validation unless risk escalates) | Lower complexity; verification emphasized in Block 4. |

---

### Block 4 вЂ” Automated tests and regression coverage

| Field | Details |
|---|---|
| **ID** | B4 |
| **Depends on** | B2, B3 (behavior and interfaces must exist) |
| **Unblocks** | B5 (final quality gates) |

**Scope**
- Add/update tests to prove: default behavior unchanged when override omitted (CR-3, CR-8); override routes outputs/returned paths to specified base with `relative_path` applied (CR-4, CR-6); uniform application across multi-entry runs (CR-7); resume/`check_done` coherence against overridden destination (spec risk); CLI option forwarding/behavior and help where applicable (CR-2, CR-9); relevant path/edge cases per resolved contract (CR-6, В§8).
- Adjust existing tests that assert signatures/forwarding/identity **only as necessary** to remain compatible with the additive change (`tests/test_app.py`, `tests/test_services.py`, and related fixtures/mocks) while preserving their intent.
- Keep tests hermetic and aligned with existing patterns/fixtures (`tmp_path`, `hermetic_app_service`, mocks).

**Risks**
- **Medium:** gaps in resume/done or multi-entry coverage leave CR-7 regressions undetected.
- **Medium:** brittle tests coupled to singleton/cache internals; tests must verify isolation (subsequent run unaffected) without overfitting implementation.
- **Low:** platform/path normalization differences in assertions.

**Implementation-path alternatives (non-choosing)**
- **Isolation verification strategy:** run sequential invocations in-process on shared service vs construct/drive service in a way that exposes cache state вЂ” select the approach that best matches existing test style once contract is fixed; do not prescribe a single technique prematurely.

**Required agents**
| Agent | Required | Rationale |
|---|---|---|
| Auditor | No | Test targets follow from resolved behavior; escalate only if unexpected behavior found. |
| Researcher | No | Standard pytest/pytest-asyncio patterns in repo. |
| Planner | **Yes** | Define concrete test matrix/cases as tasks. **Task descriptions must later be organized using template `.ai/tasks/templates/task_template.yaml` in the Execution plan.** |
| Validator | **Yes (recommended, focused)** | Sanity-check that coverage actually exercises CR-5/CR-7 risks (leakage, resume coherence), not just happy-path routing. |

---

### Block 5 вЂ” Documentation, quality gates, and rollout verification

| Field | Details |
|---|---|
| **ID** | B5 |
| **Depends on** | B2, B3, B4 |
| **Unblocks** | Completion / ready for implementation sign-off |

**Scope**
- Update user-facing documentation (README/API/docs as applicable in repo) and ensure CLI help clearly describes the option/parameter, precedence relative to configured `export_folder`, and interaction with `relative_path` (CR-9).
- Run and pass project quality gates: `uv run ruff check src tests ci`, `uv run mypy src tests ci`, `uv run pytest` (per AGENTS/CI); fix regressions introduced by this work only (CR-10).
- Confirm acceptance checklist in spec В§7 can be satisfied by evidence from tests/docs.

**Risks**
- **Low:** doc drift from implemented contract if B1 decisions are not reflected accurately.
- **Low:** unrelated baseline failures misattributed; scope fixes to this change.

**Implementation-path alternatives**
- None material; follow resolved contract and existing doc style.

**Required agents**
| Agent | Required | Rationale |
|---|---|---|
| Auditor | No | Verification-focused. |
| Researcher | No | N/A. |
| Planner | **Yes (lightweight)** | Track doc/gate tasks and evidence. **Task descriptions must later be organized using template `.ai/tasks/templates/task_template.yaml` in the Execution plan.** |
| Validator | No | Objective gates (lint/type/test) provide evidence; B2/B4 validation covers behavioral risk. |

## 3. Dependencies and execution order

```text
B1 (Contract + design + risk gate; all agents)
 в””в”Ђ> B2 (Core/library implementation; Planner + Validator)
      в”њв”Ђ> B3 (CLI exposure/parity; Planner)  [may start once signature/contract stable]
      в””в”Ђ> B4 (Tests; Planner + Validator focus)
           в””в”Ђ> B5 (Docs + quality gates; Planner)
```

- **Critical path:** B1 в†’ B2 в†’ B4 в†’ B5 (CLI B3 is typically parallelizable after B2 stabilizes the API contract, but must complete before final docs/gates).
- **Gating rule:** No production code changes for this feature before B1 records resolved open decisions and a reviewed approach consistent with CR-5/CR-7.

## 4. Open technical decisions / uncertainties (status after B1 research)

> B1 research selected a **recommended best path** for each item (see пїЅ9). U1 is the only point where explicit product-owner confirmation may be desirable if config-style base semantics are preferred over the recommended caller-intent semantics.

| # | Uncertainty | Resolved recommendation (B1) | Rationale / notes |
|---|---|---|---|
| U1 | **Relative override path base** | **CWD-relative** for caller-supplied override (`Path(override).expanduser().resolve()`), **distinct** from configured `export_folder` (remains user-dir-resolved via `PathResolver`) | Matches spec goal (CLI/scripts/notebooks/ad-hoc), avoids surprising `<user_dir>/out` for `--export-folder out`; `Path.resolve()` anchors relative paths to CWD. **Confirm with product owner only if** relative overrides must intentionally mirror config/user-dir semantics. |
| U2 | **Parameter/option name and exact type contract** | Name: **`export_folder`** (API param + CLI `--export-folder`) for parity with config/spec; Type: **`str \| Path \| None = None`**; add as **keyword-only** on library/service surfaces (`*, export_folder=...`) after `progress` where present; CLI option typed/normalized to produce same effective value | Additive, preserves existing positional calls; aligns with `report_path: str \| Path`, `pathlib.Path`-centric code, and existing `export_folder` vocabulary. No `PathLike` widening required now (minimal). |
| U3 | **Resolution/normalization and directory-preparation helper** | **Thread per-call override** API?CLI wiring?`AppService.run_report`; compute **local effective base in service** per run (small private helper/method or inline block, e.g. resolve+ensure effective base) with **no cache/config mutation** | Centralizes run-scoped coherence (base/entries/returns) and isolation (CR-5/CR-7) with minimal layering churn; do not mutate `self.export_folder` cache or `app_config`. |
| U4 | **Validation/edge-case behavior** | `None`/absent => configured default path unchanged; for provided override: `expanduser`, resolve/absolutize per U1, **reject empty/blank string** with clear `ValueError` (fail fast), `ensure_dir` on effective base (and per-entry as today) | Consistent with existing mkdir/resolve posture while preventing ambiguous `""` targets; `~` supported; directory creation/errors surface as today (`OSError` etc.). Keep validation minimal and explicit in tests/docs. |
| U5 | **Interaction with absolute `relative_path` in report settings** | **Leave behavior unchanged / no new guard**; document as existing constraint if needed (base discarded when `relative_path` is absolute per pathlib join semantics) | Override must not introduce new surprises (CR-4); changing this is out of scope and risks behavior change. Add/adjust tests only if contract coverage requires explicit assertion of unchanged semantics. |

## 5. Overall implementation, rollout, regression, and compatibility risks

| Risk | Area | Impact | Mitigation (plan-level) |
|---|---|---|---|
| Singleton/cache leak | Implementation, regression | Later runs write to overridden location or behave inconsistently (CR-5) | B1 gate on isolation design; never mutate cached properties; B2 Validator review; B4 isolation tests. |
| Resume/skip incoherence | Correctness, regression | Wrong `check_done` decisions or mixed artifacts across destinations (CR-7) | Single effective base drives all per-entry/report writes and done detection in-run; explicit B4 coverage. |
| Path semantics mismatch | Compatibility, UX | Surprising output locations for relative inputs; platform quirks | Resolve U1/U4 explicitly; document precedence/interaction (CR-9); test CWD/user-home/relative forms per contract. |
| Multi-entry non-uniformity | Correctness | Some entries use default base, others override | Apply same effective base uniformly per run (CR-7); B4 multi-entry tests. |
| Signature/export regressions | Compatibility | Breaks existing library consumers or lazy package exports | Additive optional param only; preserve behavior when omitted; update/keep `__init__`/tests aligned (CR-8). |
| Scope creep / redesign | Maintainability, rollout | Harder review, unintended behavior changes | Enforce minimal touch set and non-goals; Planner/Validator gate. |

## 6. Verification notes

- **Required by spec:** automated tests covering default unchanged, override routing/returned paths, `relative_path` interaction and multi-entry runs, CLI forwarding/behavior, and relevant path/edge cases; existing relevant tests remain valid (CR-10, T4).
- **Quality gates (project):** `uv run ruff check src tests ci`, `uv run mypy src tests ci`, `uv run pytest` (Windows/PowerShell env; `Makefile.ps1` wrappers available). CI runs lint, typecheck, tests on push/PR.
- **Behavioral evidence to confirm before completion:** (1) omitted override == current behavior/paths; (2) provided override used for all entries, artifacts, resume checks, and returned paths in that run only; (3) no config/cache leakage to subsequent run; (4) CLI and API parity per resolved contract; (5) docs/help updated and accurate.
- **Rollout:** additive change; default path unchanged. No migration required. Communicate resolved path-base semantics clearly in docs to avoid user confusion.

## 7. Summary

- **Total blocks:** 5 (B1 gate в†’ B2 core в†’ B3 CLI в†’ B4 tests в†’ B5 docs/gates), with B3 parallelizable after API contract/signature stabilizes.
- **Planner template note:** For every block where Planner is required (B1вЂ“B5), task descriptions produced later **must be organized using template `.ai/tasks/templates/task_template.yaml`** in the Execution plan context; **no separate task files are created now** unless explicitly necessary.
- **B1 research outcome:** Block 1 research/audit recorded (пїЅ8, пїЅ9). **Recommended best path selected** for U1пїЅU5 and overall approach (per-run threaded effective base, CWD-anchored override resolution, additive keyword-only `export_folder`, no cache/config mutation). **Product-owner confirmation** advisable only if relative overrides must mirror configured user-dir semantics instead of recommended CWD-relative caller-intent semantics (U1). No production code changed.

## 8. Block B1 Auditor Findings (evidence-based, deeper investigation)

**Purpose:** evidence for B1 contract/gating; addresses U1пїЅU5, CR-5/CR-7, and spec risks. No production code changed. No final approach selected where uncertainty remains.

### 8.1 Path base semantics today (U1, CR-6)

- **PathResolver implementation** (src/mko_get_mediascope_data/core/paths.py:14-32):
  - PathResolver.__init__(self, base_dir: Path) stores self.base_dir (15-16).
  - 
esolve(self, path: Path | str) -> Path (18-22): path = Path(path).expanduser(); if 
ot path.is_absolute() then path = self.base_dir / path; returns path.resolve().
  - ensure_dir(path) (24-27): path.mkdir(parents=True, exist_ok=True) and returns path. ensure_file_parent similar for parent.
- **Production resolver/base**: APP_PATHS = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=USER_DIR) and USER_DIR = Path(user_config_dir(APP_NAME)) (core/paths.py:8-9,87-88). Singleton: AppService(..., resolver=PathResolver(APP_PATHS.user_dir)) via get_app_service() (core/services.py:368-379).
- **Configured export_folder normalization/resolution/ensuring today**:
  - Model: AppSettings.export_folder: Path with @field_validator("export_folder", mode="before") normalize_path -> Path(folder).expanduser() only (core/models.py:113,125-128). No CWD anchoring/resolve in model.
  - Service: @cached_property export_folder does _path = self.resolver.resolve(self.app_config.app_settings.export_folder) then self.resolver.ensure_dir(_path) (core/services.py:124-127).
  - **Effect:** a **relative** configured export_folder is resolved relative to **user_dir (platform config dir base passed to resolver)**, then absolutized via Path.resolve(). Absolute/~ inputs are expanded and resolved (~ absolute after expanduser). Directory is created eagerly on first access.
- **Caller-supplied relative override: practical difference (CWD vs user_dir) пїЅ technical facts**
  - **If resolved like today (PathResolver(user_dir).resolve)**: --export-folder out / "out" would target <user_dir>/out (consistent with configured setting, but surprising for CLI invocations run from arbitrary CWD and for notebooks/scripts expecting CWD-relative behavior).
  - **If resolved relative to CWD (e.g. Path(...).expanduser() then 
esolve() without user_dir join, or Path.cwd() base)**: matches common CLI/ad-hoc expectations and typical 	yper.Path/pathlib usage; differs from current configured export_folder semantics and from README/config-dir-centric defaults (README shows config under platform user config dirs).
  - utils.resolve_path(path, base_dir=None) exists (core/utils.py:447-480, expands user; absolute returned/ensured; relative resolved against ase_dir or Path(__file__).resolve().parent.parent and ensured) пїЅ different base/default than production export resolver and not currently used for export flow. Relevant context, not a chosen mechanism.
  - **Constraint:** contract must be explicit; tests should pin behavior for relative/absolute/~ forms (CR-6, spec пїЅ8/пїЅ9). No final base chosen here (U1 remains open).

### 8.2 Override integration points and coherence (CR-4, CR-5, CR-7)

- **All consumers / composition sites**
  - AppService.export_folder definition/cached access only in core/services.py:124-127,280 (no other self.export_folder / AppService.export_folder reads in src/ or 	ests/ found by search).
  - AppService.run_report (core/services.py:257-365): loops entries (277); computes export_path: Path = Path(self.export_folder / report_settings.relative_path) (280); self.resolver.ensure_dir(export_path) (281); loads data (283); builds strategies/report (306-321) passing export_path=export_path and check_done=report_settings.check_done; on success appends export_path and increments counters (338-340); returns export_paths (365). Multi-entry: each entry gets its own export_path from same base; NoReportFoundError entries skipped without append (329-336).
  - Report base (core/reports.py:75-105): stores self.export_path; if check_done sets self.done_files = self.get_done_files(). get_done_files() computes suffix, utils.list_files_in_directory(self.export_path, extensions=(suffix,)), excludes utils.CSV_TEMP_PREFIX names, returns utils.dir_content_to_dict(..., suffix=suffix) keyed by 
ame.removesuffix(suffix) (99-105, core/utils.py:235-266,395-423).
  - MediaReport.processing_tasks (core/reports.py:307-395): 
eport_id = str(self.report_settings.relative_path) (329); materializes tasks, disambiguates, reports started, skips enqueue when 	ask.name in self.done_files and marks COMPLETED (359-366). Done detection is against self.done_files captured at init from export_path.
  - MediaReport._write_error_artifact (core/reports.py:206-214): wait asyncio.to_thread(item.to_file, self.export_path) on failure/timeout/exception (279,294,303). TVTask.to_file (core/tasks.py:33-41,57-66) writes to Path(folder) / "errors" (mkdir) as <name>.json5.
  - TVMediaReport.extract_data (core/reports.py:716-772): publishes via wait asyncio.to_thread(utils.csv_to_file, df, file_prefix=task.name, add_time=False, csv_path_out=self.export_path, compression=self.report_settings.compression) (751-758). utils.csv_to_file (core/utils.py:269-338) builds final file under csv_path_out, creates temp sibling in same parent (dir=str(out_file.parent), prefix .tmp_csv_, same suffix), writes/fsyncs/os.replace, cleans temp on failure пїЅ atomic publish and temp location rooted at per-entry export_path.
  - Progress identity: 
eport_id is relative-path-based only (
eports.py:329), independent of filesystem base.
- **Coherence assessment:** substituting an **effective per-run base** at per-entry export_path composition (and ensuring that directory), passing that same export_path into TVMediaReport and appending it to results, would coherently affect init-time get_done_files()/resume-skip, CSV outputs, temp/atomic publish siblings, errors/ artifacts, and returned paths пїЅ **iff** no part of run falls back to self.export_folder and no other path assumes configured base.
- **Other assumptions/coupling:** defaults/merge logic independent of export base (services.py:149-191). No other src/ module reads configured export folder; runtime execution centralized in AppService.run_report. get_app_service() is shared entry for API wrappers and CLI (pp.py:206-219,290-308).

### 8.3 
elative_path composition and edge cases (U5, CR-4)

- **Composition behavior:** Path(self.export_folder / report_settings.relative_path) (core/services.py:280). In pathlib, if right operand is absolute the base is discarded пїЅ same for Path(<effective_base>) / relative_path.
- **Normalization/validation today:** ReportSettings.relative_path: Path normalized only via Path(folder).expanduser() (core/models.py:194,204-207); model_config = ConfigDict(extra="allow") (185). No validator enforces relative/non-absolute, no traversal checks, no empty-path guard beyond coercion.
- **Usage implying relative intent:** observed settings/tests use relative subpaths (e.g. 	ests/test_services.py:510,519,579,590 "tv/raw_data", "tv/dynamic_spots", "tv/no_overlap", "tv/available"). Semantics per spec: пїЅbase + reportпїЅs internal relative layoutпїЅ.
- **Implication:** absolute 
elative_path could escape base today and under override; contract must not introduce new surprises (U5). Document/test scope per decision, not a confirmed in-scope change.

### 8.4 Public API / CLI / signature constraints (U2, CR-1, CR-2, CR-8)

- **Current signatures and forwarding**
  - sync def run_report_async(report_path: str | Path, progress: ProgressReporter | None = None) -> None (pp.py:290-308): validates path exists, wait get_app_service().run_report(path, progress=reporter).
  - def run_report(report_path: str | Path, progress: ProgressReporter | None = None) -> None (pp.py:311-344): rejects running loop, else syncio.run(run_report_async(report_path, progress=progress)).
  - sync def AppService.run_report(self, report_settings_path: Path, progress: ProgressReporter | None = None) -> list[Path] (core/services.py:257-261).
  - Package lazy re-exports via __getattr__/__all__ (src/mko_get_mediascope_data/__init__.py:13-56).
  - CLI 
un(report: Annotated[Path, typer.Argument(exists=True, dir_okay=False, ...)], verbosity: Annotated[VerbosityMode|None, typer.Option("--verbosity","-v",...)] = None) (pp.py:222-265): calls syncio.run(run_report_async(report, progress=reporter)) or without progress.
- **Signature/identity sensitivity** (	ests/test_app.py): sync wrapper expects 
un_report_async called as (str(report_file), progress=None) and syncio.run once (99-109); drive test expects recorded [(str(report_file), progress)] (112-140); 	est_public_package_exports_preserve_static_function_signatures asserts same object as .app, identical inspect.signature, identical 	yping.get_type_hints for 
un_report, 
un_report_async, initialize_settings (226-248). Additive parameter must preserve these invariants.
- **Conventions:** pathlib.Path / str | Path prevalent; optional params default None; Typer options explicit. No existing export override. Short alias for new option not established (only -v on 
un).

### 8.5 Directory preparation / validation patterns (U3, U4, CR-6)

- **Creation points:** configured base ensured in cached export_folder (services.py:126-127); per-entry dir ensured before report creation (services.py:281); TVTask.to_file ensures errors/ (	asks.py:39); csv_to_file writes temp/final in csv_path_out parent (utils.py:298-338) пїЅ satisfied by per-entry ensure. Temp prefix .tmp_csv_ excluded from done detection (
eports.py:101-104, utils.py:21).
- **Validation/error behavior:** mostly permissive (Path coercion, expanduser, mkdir side effects); failures surface as exceptions (e.g. OSError) except explicit guards (missing report file in API pp.py:305-306, missing credentials). CLI validates report arg existence (exists=True, dir_okay=False).
- **Available helpers:** PathResolver.resolve/ensure_dir, utils.ensure_path_exists/resolve_path (447-480, different base semantics/eager create, unused in export flow). Override handling must align with resolved contract and conventions (U4).

### 8.6 Isolation risks and safest minimal integration (CR-5)

- **State/caching facts:** process-wide singleton _app_service (core/services.py:368-379); @cached_property app_config and export_folder stored in __dict__ (81-99,124-127); 
eload_config() pops both and re-prepares (112-122). prepare_app mutates handler filenames and reapplies dictConfig (101-109). Accessing self.export_folder populates cache and ensures configured dir.
- **Leak vectors to prevent:** mutating pp_config.app_settings.export_folder, cache entries, APP_PATHS, or module globals; divergent base for done detection vs CSV/errors/returned paths within run; overridden base persisting to later runs on same singleton.
- **Safest minimal shape (technical, non-prescriptive):** thread optional per-run override through API > AppService.run_report and compute a **local effective base per run** (resolved/prepared per U1/U4) without mutating cached state; derive per-entry export_path from that effective base only for that invocation. Aligns with current per-entry composition; helper/placement/resolution remain open (U1пїЅU4).

### 8.7 Additional risks / notes relevant to B1

- **CLI/service startup** (pp.py:206-219,247-255): service/config load and logging/verbosity setup precede 
un_report_async; override must not rely on mutating loaded config.
- **Resume/check-done coherence:** get_done_files() runs at Report.__init__ from provided export_path (
eports.py:96-97) пїЅ effective path must be final before report construction (CR-7).
- **Backward compatibility:** additive default-None parameter(s), unchanged default behavior; lazy exports keep same objects/names. External override name/type remains contract decision (U2).
- **Test impact hotspots:** 	ests/test_app.py signature/forwarding/identity; 	ests/test_services.py multi-entry/export expectations and hermetic service patterns. Isolation tests must prove no leakage across sequential runs on shared service/singleton as appropriate.

## 9. Block B1 Researcher Findings (alternatives, evaluation, recommended best path)

**Scope:** evidence-based research/planning synthesis for U1пїЅU5, CR-1..CR-10 using spec, auditor {context_a}, and inspected code/tests. No production code changed. Goal: maintainability, future evolution, project conventions, minimal scope, no speculative redesign.

### 9.1 Overall selected implementation path (recommended)

- **Contract (additive, backward-compatible):**
  - Public/library: add optional keyword-only export_folder: str | Path | None = None to 
un_report_async, 
un_report, and AppService.run_report (after progress where present). None/omitted preserves current behavior exactly.
  - CLI: add --export-folder option on 
un (Typer Path | None or str | None normalized consistently; default None) and forward to 
un_report_async(..., progress=..., export_folder=...).
  - Name export_folder chosen for parity with config field/spec vocabulary; type str | Path | None matches existing 
eport_path style and pathlib-centric code.
- **Integration shape (run-scoped, no state mutation):** thread override pp.run_report_async/
un_report -> get_app_service().run_report(...) -> per-invocation effective base in AppService.run_report. Compute effective base **locally** for that run only: if override provided, normalize/resolve/validate and ensure_dir; if not, use existing self.export_folder behavior (default path unchanged). Derive every export_path = <effective_base> / report_settings.relative_path from that same local base, ensure per-entry dirs as today, pass into TVMediaReport, append to returned list. **Never** mutate pp_config, self.__dict__ caches, APP_PATHS, or globals; do not read/write self.export_folder cache when override drives the run except that default path must remain identical.
- **Coherence:** single effective base per run drives done detection (Report.get_done_files at init from passed export_path), CSV/temp/atomic publish, errors/ artifacts, and returned paths for all entries (CR-7). Multi-entry loop applies uniformly.
- **Helper placement:** prefer a small private/service-local helper in core/services.py (e.g. resolve/validate effective export base) or a tightly-scoped inline block пїЅ choose the smallest change that keeps 
un_report readable and testable while respecting existing layering (PathResolver/ensure_dir reuse allowed for configured path; override path follows U1 rule). No new cross-layer abstractions.

### 9.2 U1 пїЅ Relative override path base (selected: CWD-relative)

**Question:** for a *caller-supplied per-run override*, resolve relative paths against CWD, user-dir, or other?

| Alternative | Evaluation | Risks/implications |
|---|---|---|
| **A. CWD-relative (Path(p).expanduser().resolve() / cwd-anchored) пїЅ RECOMMENDED** | Best matches spec intent: "ease of use from scripts, notebooks, automation, and ad-hoc CLI runs". CLI --export-folder ./out or out behaves as users expect from shell CWD; notebooks/scripts can pass explicit relative/absolute paths naturally. Path.resolve() on a relative path anchors to the current working directory in modern Python/pathlib semantics. Keeps configured export_folder semantics unchanged (config remains user-dir-resolved today). | Semantics differ from configured export_folder base; must be documented clearly (precedence/interaction with 
elative_path). CWD can vary in services пїЅ acceptable because override is explicit caller intent per run. |
| **B. User-dir-relative (reuse PathResolver(user_dir))** | Consistent with current configured export-folder resolution and config-dir-centric model. | Surprising for CLI/ad-hoc use (out -> <platform config dir>/out), poor DX for notebooks/automation; conflicts with spec emphasis on per-invocation convenience. Not recommended for caller-supplied override. |
| **C. Hybrid/implicit (e.g. relative to report file, or auto-detect)** | None clearly superior. | Ambiguous, harder to document/test, higher regression risk. Reject (speculative). |

**Recommendation:** **A (CWD-relative)** for provided export_folder override only. Configured default continues to resolve via existing PathResolver(self.app_paths.user_dir) path in AppService.export_folder (unchanged). **Product-owner confirmation** advisable only if stakeholders require relative override paths to behave like configured paths (user-dir-relative).

### 9.3 U2 пїЅ Parameter/option name and type (selected)

| Decision | Alternatives considered | Selected & rationale |
|---|---|---|
| **Name** | export_folder, export_dir, export_path, output_dir | **export_folder**: matches existing config key/property (AppSettings.export_folder, AppService.export_folder), spec language, and least cognitive load. CLI: **--export-folder** (kebab-case conventional; no short alias unless/until a clear convention exists пїЅ only -v used on 
un today; adding alias is optional but not required). |
| **Type** | str \| Path \| None, os.PathLike \| str \| None, Path \| None | **str \| Path \| None = None**: consistent with existing 
eport_path: str | Path and pathlib.Path-centric internals; sufficient and minimal. PathLike widening is additive but unnecessary now. |
| **API shape / compat** | positional optional, keyword-only, **kwargs | **Keyword-only** on library/service methods after existing params (e.g. progress: ... = None, *, export_folder: ... = None or append as keyword-only in signature position consistent with Python rules) to avoid positional ambiguity and preserve all existing call forms. 
un_report_async/
un_report currently accept (report_path, progress=None); adding export_folder as keyword-only/additive default keeps progress positional/keyword behavior intact. **Important:** existing tests/callers that pass progress as keyword remain valid; tests that assert exact forwarding/signature must be updated intentionally (see пїЅ9.6). |
| **Package exports** | no change vs re-export typing | __init__ re-exports same function objects from .app via __all__/__getattr__; no structural change needed beyond underlying function signatures remaining compatible objects (CR-8). |

### 9.4 U3/U4 пїЅ Integration shape, normalization, validation (selected)

| Topic | Alternatives | Selected & rationale |
|---|---|---|
| **Threading** | (1) per-call param threaded end-to-end; (2) mutate service/config/cache temporarily; (3) contextvar/global | **(1) Thread per-call parameter**: explicit, run-scoped, minimal coupling, no singleton/cache leakage risk (CR-5). Reject (2)/(3) as higher-risk and/or out of established patterns. |
| **Where to compute effective base** | inline in 
un_report; private method/helper in AppService; utility in paths/utils | **Compute locally in AppService.run_report**, optionally extracted to a small **private helper/method on AppService** (or module-private in services) for clarity/test seam: input (override | None) -> Path effective base resolved+ensured, without touching cached export_folder. Prefer service-local placement to keep orchestration concern in services; avoid over-generalizing into paths.py unless trivially aligned. |
| **Resolution for override** | reuse PathResolver(user_dir); PathResolver(cwd); direct Path(...).expanduser().resolve() | **Direct normalize/resolve for override** per U1: coerce str|Path, reject blank/empty string (ValueError), expanduser(), then resolve to absolute path (CWD-anchored for relative). Ensure directory exists (mkdir(parents=True, exist_ok=True)) before composing per-entry paths (or ensure base and per-entry as today). **Configured path path unchanged**: continue using self.export_folder cached property when no override. Do not change PathResolver semantics. |
| **None/absent** | treat empty as default; treat None only | **None => default** (current config behavior). **Empty/whitespace-only string => reject** with clear ValueError (avoids ambiguous CWD/odd targets) пїЅ documented/tested (U4). |
| **When to create dirs** | eager on parse; lazy per entry | Create/ensure effective base once per run when override provided (and per-entry dirs as today) to fail fast on invalid/inaccessible destinations while preserving existing per-entry ensure_dir. Errors (OSError, permission) propagate naturally as today. |

### 9.5 U5 пїЅ 
elative_path composition edge cases (selected)

| Alternative | Evaluation | Selected |
|---|---|---|
| **Leave as-is (no new validation/guard)** | Path(base) / relative_path discards base if 
elative_path is absolute пїЅ existing behavior; spec states base replacement while preserving per-entry 
elative_path layout and requires no new surprises. Changing validation alters semantics/scope. | **Selected.** Do not add guards or rewrite 
elative_path. Ensure implementation uses the same composition pattern with effective base. Document only if docs already describe 
elative_path; tests may assert unchanged composition if useful but not required by this feature. |
| **Reject absolute 
elative_path when override used (or always)** | Safer against escape but behavior-changing and potentially breaking existing configs. | Reject (out of scope / CR-4 risk). |

### 9.6 Testing and compatibility strategy (CR-8, CR-10)

**Signature/forwarding sensitivity (must satisfy):**
- 	ests/test_app.py currently asserts: 
un_report_async called as (str(report_file), progress=None) (sync wrapper), recorded (str(path), progress) in drive test, and inspect.signature / 	yping.get_type_hints identical between package exports and pp for 
un_report, 
un_report_async, initialize_settings. initialize_settings unchanged.
- **Recommended compat approach:** add export_folder with default None as keyword-only/additive parameter to both API functions and AppService.run_report. Update tests to reflect the **new intended contract** in a backward-compatible way:
  - Forwarding assertions should accept/expect export_folder=None **only if** implementation forwards it explicitly (recommended for clarity) OR remain valid if parameter exists but default path omits it пїЅ **prefer explicit forwarding of defaults** (progress=progress, export_folder=export_folder) for observability and to keep CLI/API parity, and update the specific call-assertion tests accordingly (e.g. ssert_called_once_with(str(report_file), progress=None, export_folder=None) or equivalent) while preserving that *omitted by caller* means default.
  - Signature/type-hint identity tests between package and app must still pass (same objects; signatures evolve identically). This is an intentional, spec-driven additive API change covered by CR-8.
  - Running-loop guard and docstring references to 
un_report_async must remain valid.

**Test coverage to add/adjust (B4, mapped to spec):**
- **Default unchanged:** no override => same export base/paths/behavior as before (including multi-entry, returned paths, resume logic target).
- **Override routing:** provided absolute/relative (per CWD)/~ paths route all outputs and returned export_paths under effective base with export_path = base / relative_path.
- **Coherence (CR-7):** multi-entry uniform base; check_done/get_done_files sees existing outputs in overridden destination; CSV/temp atomic publish and errors/ land under overridden per-entry path (can be asserted via filesystem/results/mocks consistent with existing tests).
- **Isolation (CR-5):** sequential runs on same service/singleton пїЅ run with override does not change subsequent default run destination; no mutation of pp_config.app_settings.export_folder or cached export_folder.
- **CLI:** --export-folder parsed and forwarded; help shows option and precedence/interaction text as specified; existing args/verbosity/progress unaffected.
- **Edge cases (U1/U4):** relative path resolved from CWD (use monkeypatch.chdir(tmp_path) or equivalent hermetic pattern), ~ expansion, empty/blank rejected, directory creation behavior/errors aligned.
- **Regression:** NoReportFoundError skip semantics and returned paths unchanged; package export identity preserved.

**Implementation notes for test stability:** use 	mp_path/hermetic fixtures, avoid depending on real user config dirs except existing patterns, and compare resolved paths (Path.resolve()) to avoid platform separator issues.

### 9.7 Architectural / implementation / rollout / regression / compatibility risk assessment

| Risk | Severity | Assessment | Mitigation (recommended) |
|---|---|---|---|
| Singleton/cache/config leakage (CR-5) | High | Shared get_app_service() + cached pp_config/export_folder make mutation tempting but dangerous. | Never set/pop caches or mutate settings; pass effective base as local value; add isolation regression test across sequential runs. Validator review focused on services.py/pp.py. |
| Resume/done & artifact incoherence (CR-7) | High | get_done_files() bound at Report.__init__; outputs/errors/returned paths must share base. | Single effective base computed once per run, applied uniformly to all per-entry export_path derivations before report construction; test resume against overridden dest and multi-entry. |
| Path semantics confusion (U1, CR-6/CR-9) | Medium | Relative override base differs from configured-base behavior. | Choose and document CWD-relative explicitly; test relative/absolute/~; keep config resolution unchanged. |
| Signature/forwarding breakage (CR-8) | Medium | Existing tests assert exact call forms/signatures. | Additive keyword-only params with defaults; update targeted assertions to new contract while preserving backward-compatible usage; keep package exports identical objects. |
| CLI parsing/typing edge cases | LowпїЅMedium | Path option coercion and empty values vary by Typer/Click behavior. | Normalize/validate in one place (service or API boundary) with clear error; test CLI forwarding with explicit values. |
| Absolute 
elative_path escape (U5) | Low (pre-existing) | Base may be discarded by pathlib join. | No change; avoid new guards; note as known constraint if encountered. |
| Scope creep | Low | Temptation to refactor resolver/config/path model. | Minimal diff limited to API, CLI option/help, service effective-base computation, tests, docs. |

### 9.8 Points requiring (rare) product-owner confirmation

- **U1 only (recommended to confirm if ambiguity matters):** relative export_folder override is resolved **relative to current working directory** (caller intent), not relative to user config dir. This is the best technical path for the stated spec goals; confirm if any consumer expects config-style resolution for overrides.
- All other points (U2пїЅU5, integration shape, isolation, compat/test strategy) are resolved by evidence, spec constraints, and project conventions; proceed without further confirmation unless implementation uncovers conflicting requirements.


## 10. Block B1 Planner пїЅ Implementation task breakdown (template-aligned)

> Tasks below are organized per `.ai/tasks/templates/task_template.yaml` for downstream execution (B2пїЅB5). They are planning artifacts in this execution plan (no separate task files created).

### task: b2_core_api_service_override
```yaml
id: b2_core_api_service_override
title: Implement per-run export_folder override in API and AppService (run-scoped, isolated) with required core/API tests
priority: high
depends_on:
  - b1_gate_contract
source_reference: .ai\plans\03-export-folder-override.md
source_section: B2 / В§4(U1вЂ“U5) / В§8 / В§9 / В§10 / В§11 / В§12

description: >
  Add an additive, keyword-only export_folder parameter to the public library API and
  AppService orchestration, compute a run-scoped effective export base per invocation,
  and route all per-entry export outputs/resume decisions/returned paths through that
  base when provided. Preserve default configured behavior exactly when omitted, keep
  lazy package exports and initialize_settings unchanged, and add/update required
  hermetic tests for core/API/service behavior and app forwarding/signature
  compatibility. CLI option/help exposure is out of scope for this block (B3) but
  existing CLI behavior/tests must remain valid.

goals:
  - Expose export_folder: str | Path | None = None as additive keyword-only parameter (after progress) on run_report, run_report_async, and AppService.run_report; forward explicitly across API -> singleton service
  - When export_folder is None: preserve existing AppService.export_folder/configured-base behavior unchanged (including resolution/caching semantics)
  - When export_folder is provided: reject empty/blank string with clear ValueError; normalize via Path(...).expanduser().resolve() anchored to CWD; ensure effective base directory; do not mutate app_config, cached AppService properties (__dict__ caches), or module/global state
  - Use one local effective base per run for every report entry: export_path = Path(effective_base / report_settings.relative_path), ensure per-entry directory, pass to TVMediaReport, and append returned export paths consistently
  - Preserve multi-entry uniformity, NoReportFoundError skip behavior, returned-path semantics, resume/check_done coherence, and all default behavior (CR-3/4/5/6/7/8)
  - Keep __init__ lazy exports/object identity intact and initialize_settings signature unchanged; update app forwarding/signature/type-hint tests additively
  - Deliver required hermetic tests verifying logic and component interaction (not trivial implementation details)

extra_context:
  spec:
    - "Spec 03 (.ai/problems/03_export_folder_override_spec.md): CR-1..CR-10; override replaces export base only for the run; relative_path layout preserved; no config/cache/cross-run side effects; resume/check-done and all artifacts coherent with overridden destination."
  resolved_contract_B1_PO_approved:
    - "U1 (PO approved recommended): caller-supplied relative override path resolved CWD-relative via Path(value).expanduser().resolve(); configured export_folder semantics unchanged."
    - "U2: name export_folder, type str | Path | None = None, additive keyword-only after progress on run_report/run_report_async/AppService.run_report."
    - "U3: thread per-call through app -> get_app_service().run_report; compute local effective base per run in service (inline or private helper) without cache/config mutation."
    - "U4: None => default; provided empty/blank string => clear ValueError (fail fast); expanduser+resolve(CWD)+ensure_dir for effective base; dir/IO errors surface naturally."
    - "U5: leave Path(base)/relative_path composition semantics unchanged; do not add guards for absolute relative_path."
  auditor_evidence:
    - "Only export base composition/consumer for run outputs: AppService.run_report computes export_path = Path(self.export_folder / report_settings.relative_path), ensures it, passes to TVMediaReport, appends to export_paths; Report.get_done_files() reads export_path at init (resume coherence requires final path before TVMediaReport construction)."
    - "AppService.export_folder is cached_property resolved via PathResolver(APP_PATHS.user_dir) and ensured; get_app_service() is process-wide singleton; __init__ lazy re-exports .app objects."
  current_signatures_and_forwarding:
    - "app.run_report_async(report_path: str | Path, progress: ProgressReporter | None = None) -> None: validates file, awaits get_app_service().run_report(path, progress=reporter)."
    - "app.run_report(...): sync wrapper, rejects running loop, asyncio.run(run_report_async(report_path, progress=progress))."
    - "AppService.run_report(self, report_settings_path: Path, progress: ProgressReporter | None = None) -> list[Path]: iterates entries, builds export_path from self.export_folder, creates report, handles NoReportFoundError skip, returns export_paths."
  tests_sensitive_to_additive_contract:
    - "tests/test_app.py::test_run_report_sync_wrapper expects asyncio.run and run_report_async called as (str(report_file), progress=None)."
    - "tests/test_app.py::test_sync_wrapper_drives_requested_async_report_work records [(str(report_file), progress)] from real async drive."
    - "tests/test_app.py::test_public_package_exports_preserve_static_function_signatures requires same objects and identical inspect.signature/typing.get_type_hints between package and app for run_report, run_report_async, initialize_settings (initialize_settings unchanged)."
  downstream_coherence_no_changes_expected:
    - "TVMediaReport/Report init capture export_path for get_done_files()/resume; csv publish and errors artifacts are rooted at passed export_path. No changes to core/reports.py, core/tasks.py, core/models.py, core/paths.py semantics."

files:
  - path: src/mko_get_mediascope_data/app.py
    targets:
      - type: module
        name: mko_get_mediascope_data.app
      - type: function
        name: run_report_async
        semantic_anchors:
          insert_after:
            type: parameter
            value: "progress: ProgressReporter | None = None"
      - type: function
        name: run_report
        semantic_anchors:
          insert_after:
            type: parameter
            value: "progress: ProgressReporter | None = None"
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: module
        name: mko_get_mediascope_data.core.services
      - type: class
        name: AppService
      - type: method
        name: AppService.run_report
        semantic_anchors:
          insert_after:
            type: parameter
            value: "progress: ProgressReporter | None = None"
      - type: method
        name: AppService._resolve_effective_export_base (optional, private)
        semantic_anchors:
          insert_after:
            type: method
            value: AppService.run_report or nearby private helper location in AppService
  - path: tests/test_app.py
    targets:
      - type: module
        name: tests.test_app
      - type: function
        name: test_run_report_sync_wrapper
      - type: function
        name: test_sync_wrapper_drives_requested_async_report_work
      - type: function
        name: test_public_package_exports_preserve_static_function_signatures
      - type: function
        name: test_run_report_async_forwards_export_folder_override (new)
      - type: function
        name: test_run_report_sync_wrapper_forwards_export_folder_override (new)
  - path: tests/test_services.py
    targets:
      - type: module
        name: tests.test_services
      - type: function
        name: export-folder override/default/isolation/coherence/path-case tests (new, hermetic)

architectural_constraints:
  - "Preserve strict separation of concerns: app.py handles API/CLI boundary forwarding; core/services.py owns orchestration/effective-base computation; do not move export-path orchestration into reports/tasks/models/paths."
  - "Additive and backward-compatible only: no behavior change when export_folder is omitted/None; existing positional and keyword usages of (report_path[, progress]) remain valid."
  - "Keyword-only placement: add export_folder after progress as *, export_folder: str | Path | None = None on run_report, run_report_async, and AppService.run_report."
  - "Run-scoped isolation (CR-5): never mutate app_config, cached AppService.app_config/export_folder, APP_PATHS, resolver base, or module/global state; do not store override in service instance/caches."
  - "Default path parity (CR-3): when export_folder is None, derive effective base via existing self.export_folder behavior (cached_property) so configured resolution/caching/ensuring semantics remain unchanged."
  - "Override path semantics (CR-6/U1/U4): for provided override, if isinstance(export_folder, str) and export_folder.strip() == '' raise ValueError with clear message; compute effective base as Path(export_folder).expanduser().resolve() (CWD-anchored) and ensure directory exists for the run; do not use PathResolver(user_dir) for override resolution."
  - "Coherence (CR-4/CR-7): single effective base per run for all entries; compose export_path = Path(effective_base / report_settings.relative_path) per entry, ensure per-entry directory, pass same export_path to TVMediaReport before init, and append to returned export_paths; keep NoReportFoundError skip behavior and non-appended semantics unchanged."
  - "No semantic changes outside scope: do not modify core/reports.py, core/tasks.py, core/models.py, core/paths.py behavior/semantics, vendor lifecycle, network, task naming, artifact formats, or config templates. Do not 'fix' absolute relative_path handling (U5)."
  - "Typing/logging/style: keep strict type hints, pathlib usage, existing logging/exception patterns, and small focused changes; no print in core."

implementation_sequence:
  - step: 1
    name: Extend public async API and forward override
    actions:
      - "In src/mko_get_mediascope_data/app.py, update run_report_async signature to (report_path: str | Path, progress: ProgressReporter | None = None, *, export_folder: str | Path | None = None) -> None."
      - "Forward explicitly to await get_app_service().run_report(path, progress=reporter, export_folder=export_folder) (or equivalent explicit kwargs)."
      - "Keep file-existence validation and NullProgressReporter behavior unchanged; update docstring to mention optional per-run export_folder override if concise and consistent."
  - step: 2
    name: Extend public sync wrapper and preserve loop guard contract
    actions:
      - "In src/mko_get_mediascope_data/app.py, update run_report signature to (report_path: str | Path, progress: ProgressReporter | None = None, *, export_folder: str | Path | None = None) -> None."
      - "Forward explicitly via asyncio.run(run_report_async(report_path, progress=progress, export_folder=export_folder))."
      - "Preserve running-event-loop RuntimeError behavior and actionable message referencing run_report_async; keep initialize_settings unchanged."
  - step: 3
    name: Add run-scoped effective export base resolution in AppService
    actions:
      - "In src/mko_get_mediascope_data/core/services.py, add keyword-only export_folder parameter to AppService.run_report."
      - "Compute effective_base once per run before/independent of per-entry composition: if export_folder is None use self.export_folder (existing cached behavior); else validate and resolve override."
      - "Override validation/resolution: reject blank/whitespace-only str with ValueError('export_folder override must not be empty or blank') or equally clear message; resolve with Path(export_folder).expanduser().resolve(); ensure directory exists (mkdir parents/exist_ok)."
      - "Implement as inline logic or a private helper method on AppService (e.g. _resolve_effective_export_base) that does not mutate caches/config/globals; prefer minimal, readable placement."
  - step: 4
    name: Apply effective base uniformly to all report entries
    actions:
      - "In AppService.run_report, replace per-entry base source so export_path = Path(effective_base / report_settings.relative_path) for every entry in the run."
      - "Keep per-entry directory ensure, data/settings/report construction, TVMediaReport creation with export_path and check_done, NoReportFoundError handling (warn, no append, continue), success append of export_path, counters/logging, and returned export_paths semantics unchanged."
      - "Do not read/mutate self.app_config.app_settings.export_folder for overrides; do not pop/set cached properties."
  - step: 5
    name: Update existing app tests for additive signature/forwarding compatibility
    actions:
      - "In tests/test_app.py, update test_run_report_sync_wrapper to assert run_report_async called with (str(report_file), progress=None, export_folder=None) and asyncio.run called once (explicit forwarding)."
      - "In tests/test_app.py, update test_sync_wrapper_drives_requested_async_report_work recording/assertion to include export_folder (default None) so real async drive proves forwarding of the new parameter."
      - "Ensure test_public_package_exports_preserve_static_function_signatures still passes with evolved but identical signatures/type hints for run_report and run_report_async between package and app, and initialize_settings unchanged."
      - "Do not break unrelated CLI tests (run/list-reports/init behavior) that do not pass export_folder."
  - step: 6
    name: Add required core/API/service override tests (hermetic)
    actions:
      - "Add tests covering default unchanged behavior, override routing/returned paths with relative_path, multi-entry uniformity, resume/check_done coherence under override, isolation/no leakage/no cache-config mutation across sequential runs on same service/singleton, path cases (CWD-relative via monkeypatch.chdir, absolute, ~ expansion), and blank/whitespace rejection (ValueError)."
      - "Prefer tests/test_services.py for AppService behavior/isolation/coherence and tests/test_app.py for public API forwarding; use tmp_path/hermetic_app_service/mocks consistent with existing patterns."
      - "Verify component interaction (effective base used by per-entry export_path passed to TVMediaReport and reflected in returned paths/resume checks), not trivial implementation details."

changes:
  - action: add_parameter
    description: "Add additive keyword-only export_folder: str | Path | None = None (after progress) to run_report_async, run_report, and AppService.run_report; forward explicitly end-to-end."
  - action: add_code
    description: "Compute run-scoped effective export base in AppService.run_report (None -> existing self.export_folder; provided -> validated/expanded/CWD-resolved/ensured local Path) and use it for all per-entry export_path composition, per-entry ensure_dir, TVMediaReport wiring, and returned export_paths."
  - action: update_tests
    description: "Update additive app forwarding/signature/type-hint tests and add required hermetic service/API tests for default/override/routing/multi-entry/resume-coherence/isolation/path-cases/blank-rejection."

acceptance_criteria:
  - "run_report and run_report_async accept keyword-only export_folder (str | Path | None = None) after progress and behave exactly as before when omitted/None (CR-1, CR-3, CR-8)."
  - "AppService.run_report accepts keyword-only export_folder and, when provided, uses one CWD-anchored resolved/ensured effective base for all per-entry export_path values, TVMediaReport construction, resume/check_done context, artifacts destination, and returned export_paths in that run only (CR-4, CR-5, CR-6, CR-7)."
  - "Provided override: blank/whitespace string raises clear ValueError; absolute, CWD-relative, and ~ paths are handled per contract; effective base directory is ensured."
  - "No mutation/leak: app_config, cached app_config/export_folder, APP_PATHS, and globals unchanged by override runs; sequential default run after override run uses configured/default behavior as before."
  - "Multi-entry runs apply override uniformly; NoReportFoundError skip semantics and returned path list semantics preserved; U5 absolute-relative_path composition behavior unchanged."
  - "Public API forwarding explicit (including export_folder); package-vs-app object identity and evolved signatures/type hints for run_report/run_report_async preserved; initialize_settings signature/object behavior unchanged; existing CLI behavior/tests not broken."
  - "Required hermetic tests added/updated and passing; implementation changes limited to approved semantic units (no core/reports.py, core/tasks.py, core/models.py, core/paths.py semantic changes)."

tests_to_run:
  - "uv run pytest tests/test_app.py tests/test_services.py -q"
  - "uv run pytest -q (recommended full regression for the block)"

notes:
  - "B3 will add CLI --export-folder option/help and CLI-specific forwarding/help tests; B2 must keep current CLI contract valid."
  - "B4 may extend coverage, but B2 is not complete without the required core/API/service/compat tests listed above."
  - "Prefer comparing resolved Path objects in tests to avoid platform separator issues; use monkeypatch.chdir for CWD-relative override assertions."
```

### task: b3_cli_export_folder_option
```yaml
id: b3_cli_export_folder_option
title: Expose and forward --export-folder on CLI run command (with help and hermetic CLI tests)
priority: high
depends_on:
  - b2_core_api_service_override
source_reference: .ai\plans\03-export-folder-override.md
source_section: B3 / §4 (resolved contract U1,U2,U4) / §10 / §11 / §12

description: >
  Add an additive --export-folder option to the Typer CLI `run` command, forward it
  explicitly to report execution in both progress and non-progress branches, and update
  CLI help/docstring to describe precedence and path semantics per the resolved Spec 03
  contract. Core/library/service/report logic is already implemented in B2 and must not
  change. Deliver hermetic CLI tests that verify forwarding/interaction and preserve
  existing CLI behavior.

goals:
  - Expose --export-folder on `run` as an optional Path argument (Path | None, default None), with no short alias, consistent with existing Typer/Annotated option style
  - Forward export_folder explicitly to run_report_async(...) in the progress branch and in the QUIET/SILENT branch, while preserving existing report argument, verbosity resolution/application, stdout reconfiguration, progress reporter lifecycle, console messages, and exception/exit behavior
  - Update `run` command docstring/help and the option help text to state: override replaces the configured export base for this run only (takes precedence over configured export_folder), per-entry relative_path layout is preserved (base replaced), and relative override paths are resolved from the current working directory (CWD-anchored) per U1
  - Add/update hermetic CLI tests proving forwarding works with and without progress modes as appropriate, default (no option) behavior is unchanged, and `run --help` documents the option and key precedence/path semantics
  - Keep unrelated commands (init, list-reports) and existing CLI contracts unchanged; ensure existing CLI tests still pass

extra_context:
  spec_and_contract:
    - "Spec 03 (CR-2, CR-3, CR-4, CR-5, CR-8, CR-9): CLI option; default preserved; base replaced per run with relative_path preserved; run-scoped; additive/backward-compatible; help/docs updated."
    - "Resolved (B1, PO approved): U1 relative override CWD-anchored (core/service resolves); CLI must accept/pass without reinterpreting. U2 --export-folder, Path|None, no short alias. U4 blank rejection enforced in core/service."
    - "Help semantics: per-run precedence over configured export_folder, base replaced while preserving each entry relative_path, relative override paths CWD-anchored."
  b2_state_and_current_cli:
    - "B2 committed (9f93065): run_report/_async/AppService.run_report accept keyword-only export_folder and implement run-scoped effective base. Do not change core/services.py, core/reports.py, core/tasks.py, core/models.py, core/paths.py, or package exports."
    - "CLI run currently: report arg + --verbosity/-v only; forwards to run_report_async(report, progress=reporter) in progress modes and run_report_async(report) in QUIET/SILENT; preserves existing UX/error behavior."

files:
  - path: src/mko_get_mediascope_data/app.py
    targets:
      - type: function
        name: run
  - path: tests/test_app.py
    targets:
      - type: function
        name: test_cli_run_invokes_report (update if needed for additive call shape)
      - type: function
        name: test_cli_run_forwards_export_folder_option (new)
      - type: function
        name: test_cli_run_forwards_export_folder_option_quiet (new)
      - type: function
        name: test_cli_run_help_documents_export_folder (new)

architectural_constraints:
  - "CLI-only scope (B3): no changes to core/services.py, core/reports.py, core/tasks.py, core/models.py, core/paths.py, core/utils.py, core/network.py, vendor lifecycle, config templates, or public API beyond B2."
  - "Additive/backward-compatible: no option => identical behavior; preserve existing args/options and unrelated commands."
  - "Preserve control flow/UX: service before verbosity application, verbosity resolution, stdout reconfigure, RichProgressReporter/Live only in non-QUIET/non-SILENT, console/exit behavior unchanged."
  - "Forward export_folder explicitly to run_report_async in both progress and QUIET/SILENT branches; do not duplicate core path-resolution/validation in CLI."
  - "Help/docstring must state precedence, per-run scope, base replacement with relative_path preserved, and CWD-anchored relative override resolution (U1)."
  - "Tests hermetic (CliRunner, mocker, tmp_path, monkeypatch.chdir where relevant), verify CLI parsing -> forwarded call interaction."

implementation_sequence:
  - step: 1
    name: Add --export-folder option to CLI run signature
    actions:
      - "Add parameter after verbosity: Annotated[Path | None, typer.Option(\"--export-folder\", help=...)] = None (no short alias)."
  - step: 2
    name: Document precedence and path semantics in CLI help
    actions:
      - "Update run docstring and option help: per-run override takes precedence over configured export_folder, base replaced while each entry relative_path preserved, relative override paths resolved from CWD (U1)."
  - step: 3
    name: Forward export_folder in both execution branches
    actions:
      - "Progress branch: asyncio.run(run_report_async(report, progress=reporter, export_folder=export_folder))."
      - "QUIET/SILENT branch: asyncio.run(run_report_async(report, export_folder=export_folder))."
  - step: 4
    name: Add/update hermetic CLI tests
    actions:
      - "Test forwarding in progress/default mode with --export-folder; assert forwarded call includes export_folder (and appropriate progress handling)."
      - "Test forwarding in QUIET/SILENT mode with --export-folder (no progress reporter branch)."
      - "Preserve/update test_cli_run_invokes_report for default/no option (additive call shape if asserted)."
  - step: 5
    name: Add CLI help coverage and validate
    actions:
      - "Test `run --help` exit 0, includes --export-folder and key semantics (precedence, per-run/base+relative_path, CWD-anchored relative)."
      - "Run targeted/full relevant tests; ensure locally validated."

acceptance_criteria:
  - "CLI run accepts optional --export-folder and forwards it in both progress and QUIET/SILENT branches; omitted => identical behavior (CR-2, CR-3, CR-8)."
  - "Existing CLI args/options/verbosity/progress/messaging/error handling and unrelated commands unchanged."
  - "run --help documents option, precedence over configured export_folder, per-run scope, base replacement with relative_path preserved, and CWD-anchored relative override resolution (CR-9)."
  - "Hermetic tests verify forwarding in progress and non-progress modes, default unchanged, and help semantics; existing CLI tests still pass."
  - "No changes to service/report/core path-resolution logic; scope limited to app.py run command and tests/test_app.py."

tests_to_run:
  - "uv run pytest tests/test_app.py -q"
  - "uv run pytest tests/test_app.py -k \"cli_run or run_help or export_folder\" -q"
  - "uv run pytest -q (recommended before commit)"

notes:
  - "No short alias for --export-folder. Prefer asserting forwarded Path values clearly; in progress mode reporter is internal instance - assert type/behavior and export_folder as appropriate."
  - "B4 may extend coverage; B3 must be working, tested, and committed."
```

### task: b4_tests_export_folder_override
`yaml
id: b4_tests_export_folder_override
title: Verify export-folder override test coverage (satisfied by B2/B3; no additional implementation)
priority: high
status: satisfied
depends_on:
  - b2_core_api_service_override
  - b3_cli_export_folder_option
source_reference: .ai\plans\03-export-folder-override.md
source_section: B4 / §4 / §5-§6 / §9 / §12 (conditions 4,5)

description: >
  B4 scope assessed against implemented behavior and automated tests delivered in B2 (commit 9f93065)
  and B3 (commit d82c026). No concrete coverage gaps were identified against the B4 matrix or plan
  §12 conditions 4/5 (isolation on same service/singleton with subsequent default unaffected and
  config/cache uncorrupted; meaningful resume/check_done coherence against overridden destination).
  No additional production or test implementation is required. This task records evidence and defines
  minimal focused validation to confirm existing coverage remains green for CR-1..CR-10 risks in scope.

goals:
  - Confirm default/omitted and explicit-None behavior remains unchanged with additive API/CLI forwarding (CR-3, CR-8)
  - Confirm override routes outputs and returned paths to the effective base with relative_path applied, uniformly across multi-entry runs (CR-4, CR-6, CR-7)
  - Confirm meaningful resume/check_done coherence against the overridden destination, not only path construction/returns (CR-7)
  - Confirm isolation on the same service/singleton: override run does not leak into a subsequent default run and config/cache remain uncorrupted (CR-5, §12 cond 4)
  - Confirm CLI --export-folder forwarding in progress and QUIET branches and help semantics (precedence, per-run scope, base+relative_path, CWD-anchored relative) (CR-2, CR-9)
  - Confirm path/edge handling per contract (hermetic CWD-relative, absolute, ~, blank/whitespace rejected) and preserved intent of existing adjusted tests (CR-6, U1/U4)

files:
  - path: tests/test_app.py
    targets:
      - type: test
        name: CLI/API wrapper forwarding, default unchanged, help and option behavior for export_folder
    semantic_anchors:
      - test_cli_run_invokes_report
      - test_run_report_sync_wrapper
      - test_sync_wrapper_drives_requested_async_report_work
      - test_cli_run_forwards_export_folder_option
      - test_cli_run_forwards_export_folder_option_quiet
      - test_cli_run_help_documents_export_folder
      - test_run_report_async_forwards_export_folder_override
      - test_run_report_sync_forwards_export_folder_override
      - test_public_package_exports_preserve_static_function_signatures
  - path: tests/test_services.py
    targets:
      - type: test
        name: AppService per-run effective export base, routing, multi-entry, resume coherence, isolation, path/validation edge cases
    semantic_anchors:
      - test_run_report_default_export_folder_unchanged
      - test_run_report_explicit_none_export_folder_uses_default
      - test_run_report_override_routes_exports_and_returned_paths
      - test_run_report_override_applies_uniformly_to_multi_entry
      - test_run_report_override_resume_coherence_against_overridden_destination
      - test_run_report_override_isolation_no_leak_across_sequential_runs
      - test_run_report_override_cwd_relative_and_tilde_paths
      - test_run_report_override_rejects_blank_string
  - path: tests/test_reports_lifecycle.py
    targets:
      - type: module
        name: no export-folder-specific gaps identified; existing lifecycle baseline remains compatible

evidence:
  - "CR-3/CR-8 default unchanged and compatibility: tests/test_services.py::test_run_report_default_export_folder_unchanged, ::test_run_report_explicit_none_export_folder_uses_default; tests/test_app.py::test_cli_run_invokes_report, ::test_run_report_sync_wrapper, ::test_sync_wrapper_drives_requested_async_report_work, ::test_public_package_exports_preserve_static_function_signatures and lazy export identity tests."
  - "CR-4/CR-6 override routing, returned paths, artifacts: tests/test_services.py::test_run_report_override_routes_exports_and_returned_paths captures TVMediaReport.export_path, asserts CSV written under override, default base clean, and returned path resolved to effective base/relative_path."
  - "CR-7 multi-entry uniformity: tests/test_services.py::test_run_report_override_applies_uniformly_to_multi_entry."
  - "CR-7 meaningful resume/check_done coherence at overridden destination (§12 cond 5): tests/test_services.py::test_run_report_override_resume_coherence_against_overridden_destination pre-creates done '.csv.gz' in overridden per-entry dir, asserts returned overridden path, send_task/get_result not called, and same filename absent in default location (skip/resume evaluated against overridden destination)."
  - "CR-5 isolation on same service instance with subsequent default unaffected and config/cache uncorrupted (§12 cond 4): tests/test_services.py::test_run_report_override_isolation_no_leak_across_sequential_runs performs sequential override then default run on same hermetic_app_service, asserts no export_folder cache leak, app_config identity/value unchanged, default run uses configured base, and no write into override base for second entry."
  - "CR-2/CR-9 CLI parity/help: tests/test_app.py::test_cli_run_forwards_export_folder_option (progress branch), ::test_cli_run_forwards_export_folder_option_quiet (QUIET/no-progress branch), ::test_cli_run_help_documents_export_folder (documents --export-folder, per-run scope, configured-base precedence, relative_path preserved, CWD-anchored relative resolution); API forwarding via ::test_run_report_async_forwards_export_folder_override and ::test_run_report_sync_forwards_export_folder_override."
  - "CR-6/U1/U4 path semantics: tests/test_services.py::test_run_report_override_cwd_relative_and_tilde_paths (monkeypatch.chdir for hermetic CWD-relative, HOME/USERPROFILE for ~ expansion) and ::test_run_report_override_rejects_blank_string (parametrized empty/whitespace rejected with ValueError). Absolute override paths exercised by override routing/isolation tests."
  - "Hermetic/aligned: tests use tmp_path, hermetic_app_service, mocks, monkeypatch; additive signature/call-shape adjustments preserve intent; no export-folder-specific gaps identified in tests/test_reports_lifecycle.py."

changes: []

acceptance_criteria:
  - "B4 matrix items are covered by existing automated tests: default unchanged, override routing/returns with relative_path, multi-entry uniformity, meaningful resume/check_done coherence at overridden destination, CLI forwarding/help, CWD-relative/absolute/~/blank path cases, and isolation with no config/cache leak."
  - "Plan §12 conditions 4 and 5 are satisfied by cited evidence (isolation across sequential runs on same service/singleton; meaningful resume/check_done against overridden destination)."
  - "No additional production or test code changes required for B4; focused verification commands pass for the scoped export-folder tests."
  - "Full regression remains consistent with B2/B3 evidence (pytest -q => 240 passed, 90.18% cov) when executed in the project environment; report separately any unrelated baseline/environment issues."

tests_to_run:
  - "uv run pytest tests/test_app.py -k export_folder -q"
  - "uv run pytest tests/test_services.py -k export_folder -q"
  - "uv run pytest tests/test_app.py tests/test_services.py -q"
  - "uv run pytest -q  # full regression confirmation (expected: 240 passed, 90.18% cov per B2/B3)"

notes:
  - "No speculative additions; coverage is limited to spec/plan-required behavior and identified risks."
  - "Resume coherence evidence demonstrates skip behavior (done detection prevents send_task/get_result and returns overridden destination), not merely path construction."
  - "Isolation evidence uses sequential runs on the same AppService instance (hermetic_app_service) and verifies subsequent-default correctness plus absence of config/cache mutation."
  - "If a focused Validator later requires additional artifact-level assertions (e.g. explicit errors/ path under override in a service-level test), treat as a separate follow-up; not required by current spec/plan evidence."
`

### task: b5_docs_and_quality_gates
```yaml
id: b5_docs_and_quality_gates
title: Update docs/help and pass quality gates for export-folder override
priority: medium
depends_on:
  - b2_core_api_service_override
  - b3_cli_export_folder_option
  - b4_tests_export_folder_override
source_reference: .ai\plans\03-export-folder-override.md
source_section: B5 / пїЅ4 / пїЅ10

description: >
  Document new parameter/option, precedence over configured export_folder, CWD-anchoring for relative
  overrides, and relative_path interaction. Run lint/typecheck/tests and confirm spec acceptance evidence.

goals:
  - Update README.md and any relevant user/developer docs; ensure CLI help is accurate
  - Pass ruff, mypy, pytest per project standards (fix only regressions introduced by this work)

files:
  - path: README.md
  - path: src/mko_get_mediascope_data/app.py
    targets:
      - type: docstring
        name: run/run_report/run_report_async

acceptance_criteria:
  - Docs/help clearly describe override, precedence, CWD-relative resolution for overrides, and relative_path behavior (CR-9)
  - uv run ruff check src tests ci, uv run mypy src tests ci, uv run pytest succeed (CR-10)
```

## 11. Block B1 Planner пїЅ design/sequence notes
- **Execution sequence:** B2 (core+API) -> B3 (CLI wiring/help) -> B4 (tests, including compat updates) -> B5 (docs + gates). B3 may be drafted after B2 signature is stable.
- **Minimal touch set:** `app.py`, `core/services.py`, `__init__.py` only if needed, tests, README/docs. No changes to `core/reports.py`, `core/tasks.py`, `core/models.py`, `core/paths.py` semantics, or vendor/config templates unless strictly required by docs.
- **Compatibility focus:** `tests/test_app.py` asserts `run_report_async(str(path), progress=None)` forwarding, drive call recording `(path, progress)`, and exact package-vs-app signature/type-hint identity for `run_report`/`run_report_async`/`initialize_settings` - update additively to include `export_folder=None` and explicit forwarding.
- **Isolation focus:** never access/mutate cached `self.export_folder` or `app_config` to store override; compute effective base locally per call. Default path must keep existing behavior (including cached/configured base resolution relative to user_dir).
- **B2 test scope:** B2 delivers required core/API/service override tests and `tests/test_app.py` additive compat/forwarding updates defined in task `b2_core_api_service_override`; B4 extends coverage (including CLI-option integration after B3) without duplicating B2 obligations.

## 12. Block B1 Validator пїЅ independent plan review
**Scope reviewed:** Spec 03, Auditor пїЅ8, Researcher пїЅ9, resolved contract пїЅ4, tasks пїЅ10пїЅпїЅ11.

**Verdict:** **Conditionally approved for implementation** provided the following constraints are enforced:
1. **CR-5 isolation:** implementation must not mutate `app_config`, `AppService` cached properties, or module globals; override must be strictly run-scoped and must not affect subsequent runs on the singleton. Add explicit sequential-run isolation test.
2. **CR-7 coherence:** a single effective base per run must drive per-entry dir creation, `TVMediaReport(export_path=...)` before init, all artifacts (CSV/temp/errors), resume/check_done, and returned `export_paths` uniformly across multi-entry runs.
3. **Default parity (CR-3/CR-8):** `None` path must behave identically to today (including configured base resolution via existing `self.export_folder` semantics). Additive keyword-only signatures and lazy export identity preserved.
4. **U1 contract clarity:** CWD-anchored relative override resolution must be implemented consistently (API/CLI), documented, and tested hermetically (`monkeypatch.chdir`); avoid accidental user_dir anchoring for overrides.
5. **U4 validation:** blank/empty-string override rejected with clear `ValueError`; `~`/relative/absolute handled per contract; directory creation/IO failures surfaced consistently.
6. **U5 non-regression:** do not introduce guards/behavior changes for absolute `relative_path`; leave existing composition semantics unchanged.
7. **Scope discipline:** no unrelated refactors; docs updated for precedence and `relative_path` interaction; quality gates clean.

**Residual risk accepted with mitigation:** U1 stakeholder preference (CWD vs user_dir for relative overrides) - documented in пїЅ4 and docs; behavior explicit and test-covered. No other open technical blockers identified for B2 start.
