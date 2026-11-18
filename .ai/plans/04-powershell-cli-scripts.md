---
id: PLAN-04
title: Execution plan for PowerShell CLI scripts — init/setup and export (Problem 04)
status: b1_gate_closed_ready_for_implementation
created: 2026-11-18
updated: 2026-11-18
b1_gate: closed
b1_gate_closed_by: Step 3.3 Planner (R1-R7 contracts resolved; residual external uncertainties recorded)
source_spec: .ai/problems/04_powershell_cli_scripts_spec.md
source_problem: .ai/problems/Problem_02.md
source_context: Step 0 codebase/context audit; B1 Step 3.1 Auditor; B1 Step 3.2 Researcher
target_location: examples/01-get-cli/
deliverables:
  - examples/01-get-cli/init.ps1
  - examples/01-get-cli/export.ps1
---

# Execution Plan — Problem 04: PowerShell CLI Scripts (Init/Setup & Export)

## 1. Overview

### Objective
Decompose [`.ai/problems/04_powershell_cli_scripts_spec.md`](../problems/04_powershell_cli_scripts_spec.md) into dependency-safe execution blocks for implementing two Windows PowerShell scripts in `examples/01-get-cli/`:
- **`init.ps1`** — guided environment/setup initialization (Git handling, package install/upgrade in a consistent Python/CLI environment, non-destructive CLI `init`, and safe `export_folder` read/change in user `app_config.json5`).
- **`export.ps1`** — deterministic report selection from user `reports/` (or confirmed custom-path copy flow) and execution via existing CLI `run`, with explicit status and non-closing console behavior.

All module operations must use the **existing CLI only** (`init`, `list-reports`, `run` via `mko-get-mediascope` and/or `python -m mko_get_mediascope_data`). **No module/application code, dependencies, CLI behavior, or existing audit phase files may be changed. No intermediate `.py` files may be created or run.**

### Inputs
- **Spec:** `.ai/problems/04_powershell_cli_scripts_spec.md` (PO decisions Q1–Q8 confirmed, acceptance criteria §13, DoR/constraints/risks).
- **Step 0 audit:** confirms `examples/01-get-cli/` is empty; CLI/init/list-reports/run behavior and path/init semantics; existing `examples/01-get/*` patterns and anti-patterns (internal imports, DI bootstrap) to avoid; key discrepancies/risks (settings path discovery, JSON5-safe edit, interpreter/env consistency, git/winget/PATH restart semantics, list-reports output stability, PS 5.1 compatibility).

### Scope (in plan)
- Create `init.ps1` and `export.ps1` under `examples/01-get-cli/` meeting spec §§2–13.
- Resolve implementation contracts/uncertainties (gated) before coding where multiple viable approaches exist.
- Manual acceptance validation aligned to spec §13; keep scope minimal (no module tests, no unrelated example refactors).

### Non-goals / out of scope
- Any changes under `src/`, package dependencies, CLI implementation/schema, build/release config, or existing examples outside `examples/01-get-cli/`.
- New CLI features (path discovery, config get/set, machine-readable listing, report registration), credential provisioning/validation beyond existing `init`, automated export-directory creation, non-Windows scripts, or scheduler/web/DB concerns.
- Optional usage note/README in `examples/01-get-cli/` is **not required** by spec (open question #193); treat as out-of-scope unless explicitly added later with no scope creep.

## 2. Global constraints and invariants (must hold across all blocks)
- **CLI-only module boundary:** module operations only via `init` (no `--force`), `list-reports`, `run`. External commands allowed only as specified for setup (`git`/`winget`/installers, `python -m pip install --upgrade ...`) and direct, safe targeted edit of user `app_config.json5` `"export_folder"`.
- **No destructive init:** never pass `--force`; rely on non-destructive copy/legacy migration behavior.
- **Environment consistency:** installs/upgrades and module CLI invocation must target the **same** Python/CLI environment; chosen target must be reported clearly.
- **Config safety:** `export_folder` change requires existing directory (`Test-Path -PathType Container`); invalid/missing/ambiguous cases must not modify config; updates must be minimal, targeted, and preserve JSON5 content/structure where possible (abort on unsafe/ambiguous match).
- **UX/robustness:** Russian user-facing messages with clear status labels (`УСПЕХ`/`ОШИБКА`), preserve CLI command output as-is, per-step explicit failure reporting, mandatory stop/restart handling as specified, window remains open on completion/failure.
- **Compatibility:** prefer Windows PowerShell 5.1-compatible constructs; support 5.1 and 7+ where practical; handle UTF-8/Russian output carefully.
- **Idempotence/safety:** re-runnable where possible; Script 2 mutates user `reports/` only on explicit confirmed custom-path copy/replace.

## 3. Dependency graph and execution order

Sequential by default; B1 is a hard gate. Limited drafting overlap is possible only after B1 records resolved contracts (B3/B4 must not finalize behavior while core uncertainties remain open).

```text
B1 (Design/contracts & uncertainty resolution, GATE)
  ├─> B2 (Shared foundations: script scaffolding, env/CLI/path helpers, UX/error model)
  │     └─> B3 (Script 1: init.ps1 — Git, installs, init, export_folder)
  │           └─> B4 (Script 2: export.ps1 — selection/copy/run)
  │                 └─> B5 (Verification, acceptance validation, packaging polish)
  └─(resolved contracts also constrain B3/B4 directly)
```

**Ordering rationale:** B1 removes multi-path technical uncertainty (required before implementation choices). B2 establishes consistent, reusable primitives (interpreter/CLI resolution, settings/report path discovery, status/keep-open/error handling) needed by both scripts. B3 produces initialized/usable settings and export-folder state relied on by Script 2 flows. B4 depends on user settings/reports conventions and CLI run behavior. B5 validates end-to-end acceptance only after deliverables exist.


## 4. Execution blocks

### B1 — Contract definition, technical investigation, and design resolution (gate)

| Field | Details |
|---|---|
| **ID** | B1 |
| **Depends on** | None |
| **Unblocks** | B2, B3, B4, B5 (all downstream work must follow recorded decisions/contracts) |
| **Risk level** | **High** (cross-cutting uncertainty: environment drift, config corruption, PATH/restart semantics, platform differences) |

#### Scope
- Resolve and document (in implementation notes within this plan or as explicit pre-implementation decisions) the concrete contracts for:
  - **User settings/report path discovery without module imports:** deterministic way to locate user settings root (`%APPDATA%`/platformdirs equivalent for `mko_get_mediascope_data/settings`) and derived paths (`app_config.json5`, `reports/`) on Windows/PS 5.1+7+; fallback/inference rules and failure messaging when missing/uninitialized.
  - **Interpreter & CLI resolution precedence and env consistency:** precedence among `mko-get-mediascope`, `py -3`, `python`, `uv run python` (and/or discovered executable paths) such that **the same environment** is used for `pip install --upgrade` and `python -m mko_get_mediascope_data`/CLI calls; how to detect/report chosen target; behavior when console script is missing but module is installed in a specific interpreter.
  - **Git detect/install/update/restart semantics (Q2=A):** `git --version` detection, `winget` availability/elevation considerations, best-effort update conditions, and precise “newly installed or not usable in-session => stop after Step 1 with explicit restart message” criteria (PATH refresh limitations).
  - **JSON5-safe targeted edit of `export_folder` (Q4=A, constraints):** strict, minimal replacement strategy for `"export_folder": "<value>"` preserving comments/structure/encoding where practical; unique-match validation, ambiguity handling, quoting/escaping rules, and abort-without-write behavior.
  - **Script 2 discovery/selection/copy contracts (Q5=A, Q6=A):** filesystem enumeration scope/order (recursive `*.json5`, deterministic sort), display form (e.g. relative path from `reports/`, and/or full path), selection parsing/validation, custom-path validation (`.json5`, existing file), copy destination = user `reports/` root with original filename, conflict prompt `[R]eplace/[C]ancel` default Cancel, and “run copied report path” rule.
  - **UX/error model:** step identifiers/names, Russian status phrasing conventions, exit-code-driven success for CLI calls while preserving CLI output, keep-window-open pattern (`try/catch/finally` + pause/read-host) compatible with PS 5.1/7+ and double-click scenarios, and mandatory stop-on-failure vs restart-stop behavior.
- Confirm alignment with current CLI behavior (init prints formatted path but parsing is brittle; `list-reports` output is human-formatted and not machine-stable — scripts must enumerate filesystem for numbering) and spec acceptance criteria.

#### Risks
- **High:** wrong settings path resolution => scripts operate on wrong/missing config.
- **High:** interpreter/env drift => packages installed into different env than CLI/run target.
- **High:** unsafe JSON5 rewrite => corrupted `app_config.json5`.
- **Medium–High:** winget/elevation/PATH refresh causing false success/failure or missed restart-stop.
- **Medium:** PS 5.1 vs 7+ differences (encoding, cmdlets, syntax) causing UX or runtime issues.
- **Medium:** ambiguous `export_folder` key matches or path quoting/escaping edge cases.
- **Low–Medium:** CLI output coupling if success inferred from text instead of exit/status.

#### Implementation-path alternatives (technical uncertainty — **do NOT choose final approach**)

1. **Settings/report path discovery**
   - **A. Derive from Windows environment (`$env:APPDATA`/`$env:LOCALAPPDATA` as applicable) + known app folder:** compute platformdirs-equivalent path for `mko_get_mediascope_data\settings` directly. *Pros:* no module import, deterministic on standard Windows. *Cons:* must match platformdirs behavior; non-standard env setups need clear errors.
   - **B. Infer from `mko-get-mediascope init`/CLI output or run `init` to obtain path:** capture/parse emitted settings path. *Pros:* leverages actual app behavior. *Cons:* output parsing brittle/localization-sensitive; running `init` has side effects (though non-destructive) and complicates flows.
   - **C. Resolve via discovered Python in target env (read-only query without creating project `.py` files):** e.g. interpreter one-liner/`-c` only if permitted by strict “no intermediate `.py`” rule. *Pros:* accurate to platformdirs. *Cons:* boundary/interpretation risk vs Q1=A; depends on interpreter availability.

2. **Interpreter/CLI consistency model**
   - **A. Interpreter-first:** select one Python interpreter by precedence, use it for all `-m pip` and `-m mko_get_mediascope_data`; prefer console script only if proven same-env. *Pros:* strongest env consistency. *Cons:* console-script UX differs.
   - **B. Console-script-first with env pinning:** prefer `mko-get-mediascope` but derive/pin matching interpreter for pip installs. *Pros:* aligns with installed command UX. *Cons:* mapping script -> interpreter/env can be non-trivial.
   - **C. Hybrid with explicit detection/reporting:** detect candidates, choose deterministic precedence, verify consistency signals where feasible, fail clearly if consistency cannot be established. *Pros:* robust UX. *Cons:* more logic; residual ambiguity possible.

3. **JSON5 `export_folder` update**
   - **A. Strict regex-based single-key targeted replacement:** locate unique `"export_folder"\s*:\s*"..."` and replace only value token. *Pros:* minimal diff, preserves comments/order. *Cons:* must handle spacing/JSON5 nuances and escaping.
   - **B. Line/structured text scan with manual minimal splice:** find key occurrence with uniqueness checks, splice replacement. *Pros:* controllable, preserves indentation. *Cons:* custom logic still must be careful.
   - **C. Full parse/rewrite via JSON5-capable tooling:** *Pros:* syntactic correctness of value. *Cons:* likely loses comments/formatting and adds tooling dependency; high risk to constraints — feasibility must be validated, not assumed.

4. **Git install/restart handling**
   - **A. `winget` opportunistic install when available, else manual guidance:** matches Q2=A. *Cons:* elevation/agreements and PATH refresh uncertainty.
   - **B. Direct installer download fallback:** may work without winget but higher complexity/security/maintenance and not specified — likely out of minimal scope unless required.

#### Required agents
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Yes** | Confirm CLI/init/path/list-reports/run realities, user settings layout, and example patterns/anti-patterns; validate non-module path discovery and output contracts. |
| Researcher | **Yes** | Multiple viable approaches for PS 5.1/7+ compatibility, env-consistent pip/CLI resolution, safe JSON5 targeted edits, and Windows git/winget/PATH restart behavior need best-practice comparison. |
| Planner | **Yes** | High-risk, cross-cutting work requires detailed pre-implementation task breakdown and sequencing. **Organize task descriptions using template `.ai/tasks/templates/task_template.yaml` in the Execution plan context (no separate task files created now).** |
| Validator | **Yes** | Independent review needed to catch env drift, config-corruption risk, restart-stop misinterpretation, and CLI-boundary violations before coding proceeds. |

**Gate exit criteria:** documented decisions/contracts for settings path discovery, interpreter/CLI precedence, git/restart handling, JSON5 edit strategy, Script 2 selection/copy behavior, and UX/error model; unresolved items explicitly flagged as blockers/escalations.

```yaml
id: P04-B1-T1
title: Investigate and resolve PowerShell script contracts and technical uncertainties
priority: high
depends_on: []
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2-§13, §6.3, §7; Step 0 audit"
description: >
  Resolve implementation contracts for init.ps1/export.ps1 without changing module code:
  user settings/reports path discovery, interpreter+CLI env consistency, git install/update
  and restart-stop semantics, JSON5-safe export_folder edit, deterministic report selection
  and custom-copy flow, and PS 5.1/7+ UX/error/keep-open behavior.
goals:
  - document deterministic, CLI-boundary-compliant path and env resolution strategy
  - define safe, minimal JSON5 export_folder update contract with ambiguity abort rules
  - specify Script 1/Script 2 step flows, status labels, and failure/restart semantics
  - map decisions to spec acceptance criteria and identify residual blockers
files:
  - path: examples/01-get-cli/init.ps1
  - path: examples/01-get-cli/export.ps1
acceptance_criteria:
  - contracts recorded with rationale and non-chosen alternatives where uncertainty remains
  - no module/CLI behavior changes required; boundary violations escalated if discovered
  - downstream blocks unblocked by explicit, testable implementation guidance
```

```yaml
id: P04-B1-T2
title: Independent validation of B1 design/contracts
priority: high
depends_on: [P04-B1-T1]
description: >
  Review resolved contracts for safety, compatibility, and spec conformance before implementation.
goals:
  - verify env consistency and no pip/CLI target drift
  - verify JSON5 edit cannot corrupt config on ambiguity/failure
  - verify git restart-stop and CLI-only/module-boundary compliance
acceptance_criteria:
  - design approved or required adjustments recorded
  - residual risks and mitigations explicitly listed
```

---

### B2 — Shared foundations and script scaffolding

| Field | Details |
|---|---|
| **ID** | B2 |
| **Depends on** | B1 (resolved contracts) |
| **Unblocks** | B3, B4 |
| **Risk level** | **High** (foundation errors propagate to both scripts) |

#### Scope
- Create `examples/01-get-cli/init.ps1` and `examples/01-get-cli/export.ps1` scaffolds with consistent structure: strict/error-preference choices compatible with PS 5.1/7+, `try/catch/finally` keep-window-open behavior, and per-step/final status reporting in Russian (`УСПЕХ`/`ОШИБКА`) while preserving CLI output.
- Implement B1-approved primitives (inline or shared per B1 decision, no `.py`): status helpers, interpreter/CLI resolver and invocation wrappers preserving exit codes/output, user settings/report path discovery, and common input/validation utilities.
- Ensure scripts never claim success on failure and clearly report chosen environment/targets as appropriate.

#### Risks
- **High:** incorrect shared env/path resolution breaks downstream steps.
- **Medium:** PS 5.1 incompatibilities (syntax, encoding, host behavior).
- **Medium:** swallowed exit codes or altered CLI output.
- **Low:** duplication vs helper-file trade-off (must stay minimal).

#### Implementation-path alternatives (only if B1 leaves residual uncertainty)
- **Helper organization:** duplicated inline blocks in each script vs colocated helper `.ps1` dot-sourced by both. Trade-off: extra file vs duplication; must not create `.py`.
- **Keep-open mechanism:** `Read-Host` in `finally` vs `pause`/host-aware variant — must be 5.1/7+ compatible.

#### Required agents
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Yes** | Align with audited CLI/path behavior and avoid `examples/01-get` anti-patterns; verify no module-internal assumptions. |
| Researcher | **Yes** | PS 5.1/7+ portable patterns for exit-code handling, encoding, and invocation wrappers need best-practice confirmation. |
| Planner | **Yes** | Detailed ordered task breakdown; **organize task descriptions using template `.ai/tasks/templates/task_template.yaml` in the Execution plan**. |
| Validator | **Yes** | Independent review of scaffolding contracts before feature logic is layered on. |

```yaml
id: P04-B2-T1
title: Implement shared scaffolding and foundational helpers for init.ps1/export.ps1
priority: high
depends_on: [P04-B1-T1, P04-B1-T2]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2.1, §3.2, §5 T1/T6, §13"
description: >
  Create script files in examples/01-get-cli with consistent UX/error/keep-open scaffolding
  and B1-approved primitives for environment/CLI resolution, settings path discovery,
  status reporting, and safe command execution.
goals:
  - both scripts exist with robust try/catch/finally and explicit final status behavior
  - environment and CLI invocation follow B1 consistency contract
  - user-facing runtime messages in Russian with clear УСПЕХ/ОШИБКА labels; CLI output preserved
files:
  - path: examples/01-get-cli/init.ps1
  - path: examples/01-get-cli/export.ps1
acceptance_criteria:
  - scripts do not close on success/failure and report step/final status clearly
  - no module code changes and no intermediate .py files
  - foundation ready for Script 1/Script 2 feature implementation
```

---

### B3 — Script 1 implementation: initialization/setup and export-folder management (`init.ps1`)

| Field | Details |
|---|---|
| **ID** | B3 |
| **Depends on** | B1, B2 |
| **Unblocks** | B4, B5 |
| **Risk level** | **High** (external installs, PATH/restart semantics, env targeting, JSON5 config mutation) |

#### Scope
- Implement `init.ps1` per spec §2.2 and §13:
  - **Step 1 — Git:** detect via `git --version`; if missing attempt standard Windows install (prefer `winget`) or provide clear manual guidance and stop with overall error; if newly installed/not usable in-session stop after Step 1 with explicit restart-required message; if present perform best-effort update when supported, else report version and continue.
  - **Step 2 — `mediascope-api-lib`:** `python -m pip install --upgrade mediascope-api-lib` in consistent environment; check/report failures.
  - **Step 3 — `mko_get_mediascope_data` + init:** `python -m pip install --upgrade git+https://github.com/manicko/mko_get_mediascope_data.git` in same environment; run CLI `init` **without `--force`**; handle/report failures.
  - **Step 4 — export folder:** read/display current `export_folder` from user `app_config.json5` (optionally resolved path per B1); prompt to change; no-change leaves config untouched; on change accept abs/rel/`~`, trim, validate existing directory (`Test-Path -PathType Container`), reject invalid with explicit error and no modification, on valid perform B1-approved safe targeted JSON5 update and confirm saved value.
- Enforce stop-on-failed-mandatory-step policy (respecting restart stop), explicit step/error reporting, and overall failed status when appropriate.

#### Risks
- **High:** git install/update effects, elevation, and PATH refresh correctness; restart-stop misdetection.
- **High:** pip installs targeting wrong interpreter/environment.
- **High:** JSON5 corruption or unintended config changes.
- **Medium:** CLI `init` failure/missing settings context handling.
- **Medium:** user path input edge cases (`~`, relative paths, whitespace, quoting).

#### Implementation-path alternatives (do not choose where uncertainty remains)
- **Best-effort git update mechanism:** concrete action depends on environment/tool availability; fallback to “report version and continue” acceptable per spec.
- **Export-folder display detail:** raw value only vs raw + resolved path — keep minimal and unambiguous per B1.

#### Required agents
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Yes** | Validate alignment with actual `init` behavior, user config paths/format, and non-destructive semantics. |
| Researcher | **Yes** | Windows/PowerShell install and package-management edge cases (winget, pip, PATH/session behavior) require evidence-based handling patterns. |
| Planner | **Yes** | Failure-sensitive flow needs explicit task breakdown and acceptance mapping; **organize task descriptions using template `.ai/tasks/templates/task_template.yaml`**. |
| Validator | **Yes** | Independent review critical for config-safety, env consistency, restart-stop correctness, and CLI-boundary compliance. |

```yaml
id: P04-B3-T1
title: Implement init.ps1 steps 1-3 (Git, package installs, CLI init)
priority: high
depends_on: [P04-B2-T1]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2.2 Steps 1-3, §13 Script 1"
description: >
  Implement environment bootstrap in init.ps1: git detection/install/update with
  restart-stop handling, mediascope-api-lib and mko_get_mediascope_data install/upgrade
  in the consistent environment, and CLI init without --force.
goals:
  - correct stepwise behavior, reporting, and stop conditions per spec
  - consistent interpreter/CLI environment for installs and module operations
files:
  - path: examples/01-get-cli/init.ps1
acceptance_criteria:
  - git/install/restart behavior matches Q2=A
  - package installs and CLI init match Q3=A and non-destructive init requirement
  - failures stop with explicit step/error and overall failed status (except required restart stop)
```

```yaml
id: P04-B3-T2
title: Implement init.ps1 step 4 (read/validate/update export_folder safely)
priority: high
depends_on: [P04-B3-T1]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2.2 Step 4, §7, §13 Script 1"
description: >
  Implement export_folder display and optional change flow in init.ps1 with strict
  validation and B1-approved JSON5-safe targeted update.
goals:
  - show current export_folder (and optional resolved path) from user app_config.json5
  - no change on decline or invalid/non-existent directory; no directory creation
  - valid input updates config safely and confirms saved value
files:
  - path: examples/01-get-cli/init.ps1
acceptance_criteria:
  - existing directory required via Test-Path -PathType Container
  - config unchanged on invalid/missing input or ambiguity/failure
  - JSON5 preserved per safe targeted-edit contract; saved value confirmed
```

---

### B4 — Script 2 implementation: report selection, custom copy, and export (`export.ps1`)

| Field | Details |
|---|---|
| **ID** | B4 |
| **Depends on** | B1, B2, B3 (usable user settings/reports context expected; handle missing/uninitialized state gracefully) |
| **Unblocks** | B5 |
| **Risk level** | **Medium–High** (user data mutation on confirmed copy, path/selection correctness, CLI outcome handling) |

#### Scope
- Implement `export.ps1` per spec §2.3 and §13:
  - **Step 1 — List/select:** enumerate `*.json5` recursively under user settings `reports/` (same scope as `list-reports`), sort deterministically, display numbered items 1..N with unambiguous user-friendly names and option `0` = custom path; validate selection.
  - **Step 2 — Custom path/copy:** on `0`, prompt for existing `.json5` report file path; copy to user `reports/` **root** using original filename; if destination exists warn and ask `[R]eplace/[C]ancel` (default Cancel); **Replace** => overwrite and run copied report; **Cancel** => return to selection without running/copying/replacing.
  - **Step 3 — Run:** execute selected/copied report via CLI `run <report_path>` (no per-run `--export-folder`), preserve CLI output, report success/failure based on CLI exit/status.
- Handle edge cases explicitly: no reports found, invalid selection, missing/invalid custom path, copy failures, run failures; keep window open with final status.

#### Risks
- **Medium–High:** unintended overwrite or wrong target if conflict/default handling incorrect.
- **Medium:** non-deterministic or ambiguous listing/numbering.
- **Medium:** running wrong path (source vs copied destination) on replace flow.
- **Medium:** missing/uninitialized settings or reports directory UX.
- **Low–Medium:** path normalization/quoting issues when invoking CLI.

#### Implementation-path alternatives (do not choose where uncertainty remains)
- **Display label:** relative path vs relative+full path — must be unambiguous (Q5=A); finalize per B1.
- **Selection loop structure:** loop-until-valid vs equivalent flow — simplest PS 5.1-compatible form per B1.

#### Required agents
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Yes** | Ensure filesystem enumeration matches audited scope and `run` path behavior; avoid CLI-output coupling. |
| Researcher | **Conditional** | Required if PS 5.1/7+ file/path/prompt edge cases remain uncertain after B1; otherwise optional. |
| Planner | **Yes** | Branching/copy/conflict/loop semantics need explicit task breakdown; **organize task descriptions using template `.ai/tasks/templates/task_template.yaml`**. |
| Validator | **Yes** | Independent review important for destructive-boundary correctness, correct run target, and deterministic selection. |

```yaml
id: P04-B4-T1
title: Implement export.ps1 report enumeration and selection flow
priority: high
depends_on: [P04-B2-T1, P04-B3-T2]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2.3 Steps 1-2, §13 Script 2"
description: >
  Implement deterministic recursive report listing from user reports folder with numbered
  selection, option 0 custom-path handling, and safe copy/replace/cancel flow.
goals:
  - unambiguous 1..N listing and input validation with return-to-selection on cancel/invalid cases
  - custom .json5 copy to reports root with explicit replace confirmation (default Cancel)
  - no copy/replace without explicit user choice; correct target path for subsequent run
files:
  - path: examples/01-get-cli/export.ps1
acceptance_criteria:
  - listing scope/order is filesystem-based and deterministic (not parsed from list-reports output)
  - conflict handling matches Q6=A exactly
  - edge cases reported explicitly without closing console unexpectedly
```

```yaml
id: P04-B4-T2
title: Implement export.ps1 CLI run execution and outcome reporting
priority: high
depends_on: [P04-B4-T1]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§2.3 Step 3, §13 Script 2"
description: >
  Run selected or copied report via existing CLI run command in the consistent environment
  and report outcome based on CLI status while preserving output.
goals:
  - invoke CLI run with correct report path and no per-run --export-folder override
  - preserve CLI output and derive success/failure from exit/status
  - final explicit status with window kept open
files:
  - path: examples/01-get-cli/export.ps1
acceptance_criteria:
  - selected numbered report runs correctly
  - replaced custom report runs copied path in reports root
  - run failures produce overall failed status with explicit error context
```

---

### B5 — Verification, acceptance validation, and delivery readiness

| Field | Details |
|---|---|
| **ID** | B5 |
| **Depends on** | B2, B3, B4 |
| **Unblocks** | None (terminal) |
| **Risk level** | **Medium** (manual/behavioral validation only; no automated script tests exist) |

#### Scope
- Perform structured manual validation against spec §13 for both scripts, including key branches:
  - `init.ps1`: git detected/missing/newly-installed-not-usable-in-session (as feasible), package install success/failure, CLI `init` without `--force`, export-folder decline/no-change, invalid/missing path rejection, valid path update/confirmation, mandatory-failure stop and keep-open behavior.
  - `export.ps1`: empty/populated recursive listing, deterministic numbering, valid/invalid selection, option 0 invalid/non-.json5/missing path, conflict replace/cancel/default-cancel, correct run target (selected vs copied), CLI success/failure reporting, keep-open behavior.
- Verify cross-cutting constraints: no module code changes, no `.py` artifacts, CLI-only module operations, Russian status labels with preserved CLI output, practical PS 5.1/7+ compatibility checks where environment allows.
- Record environment-dependent limitations (winget/elevation/network) without scope change; escalate blockers that cannot be resolved without module/CLI changes.

#### Risks
- **Medium:** incomplete environment coverage (PS 5.1 vs 7+, winget availability, elevation).
- **Medium:** manual validation may miss encoding/quoting/unusual JSON5 edge cases.
- **Low:** false confidence if success inferred from CLI text instead of exit codes.

#### Implementation-path alternatives
- **Validation depth:** prioritize spec §13 criteria and high-risk areas (env consistency, JSON5 safety, restart-stop, copy/replace boundaries) over exhaustive matrix given no automated harness in scope.

#### Required agents
| Agent | Required | Rationale |
|---|---|---|
| Auditor | **Conditional** | Engage if results contradict audited CLI/path assumptions or reveal hidden coupling. |
| Researcher | No | Unless unexplained external-tool behavior remains after B1/B3/B4. |
| Planner | No | Sequencing complete; follow-up tasking only if defects found. |
| Validator | **Yes** | Independent acceptance review against spec §13 and constraints is essential. |

```yaml
id: P04-B5-T1
title: Validate init.ps1 and export.ps1 against spec acceptance criteria
priority: high
depends_on: [P04-B3-T2, P04-B4-T2]
source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
source_section: "§12, §13"
description: >
  Perform manual, evidence-based validation of both scripts across required flows and
  cross-cutting constraints; document pass/fail, limitations, and any blockers.
goals:
  - confirm acceptance criteria coverage for Script 1 and Script 2
  - confirm no module changes, no intermediate .py files, and CLI-only module operations
  - capture environment-specific limitations without scope expansion
acceptance_criteria:
  - validation results recorded with explicit evidence/observations
  - deviations classified as fix-now (within scope) or escalated blockers
```

## 5. Cross-cutting risks and mitigations (summary)

| Risk | Impact | Primary blocks | Mitigation |
|---|---|---|---|
| User settings path discovered incorrectly without module imports | Wrong config/reports targeted; silent misconfiguration | B1, B2, B3, B4 | Gate with explicit deterministic strategy and clear failure when unresolved. |
| Interpreter/CLI environment drift | Packages installed to wrong env; `init`/`run` use different target | B1, B2, B3, B4 | Single consistent environment contract, explicit reporting, exit-code-checked invocations. |
| JSON5 `export_folder` edit corrupts config | Broken settings; user blocked | B1, B3 | Minimal targeted replacement, unique-match enforcement, abort on ambiguity/failure, no full rewrite. |
| Git install/update and PATH/session restart mishandled | False success or unusable git in-session | B1, B3 | Detect via `git --version`, prefer winget when available, enforce explicit restart-stop when newly installed/unusable. |
| Script 2 copy/replace targets wrong file or overwrites without consent | Data/config confusion in reports folder | B1, B4 | Deterministic FS enumeration, explicit prompt with default Cancel, copy-to-root with original filename, run copied path only on replace success. |
| PS 5.1/7+ and UTF-8/Russian output issues | Runtime errors or unreadable UX | B1, B2, B5 | Prefer 5.1-compatible constructs, careful encoding/output handling, validate on available hosts. |
| Success inferred from CLI text instead of exit/status | Incorrect `УСПЕХ/ОШИБКА` reporting | B1, B2, B3, B4, B5 | Exit-code-driven outcome checks; preserve CLI output separately. |
| No automated tests for scripts | Residual regression/edge-case risk | B5 | Thorough manual acceptance matrix focused on spec §13 and high-risk branches. |

## 6. Decisions status and residual uncertainties (B1 Step 3.3 resolution)

### 6.1 Resolved in B1 gate (authoritative: §9, R1–R7)
- **Settings path discovery mechanism — RESOLVED (R1):** inline interpreter probe via `-c` using the chosen target interpreter to print `APP_PATHS.user_settings_dir`, `APP_PATHS.app_config`, `APP_PATHS.reports` (machine-parseable). No init-output parsing, no README/env-var guessing. Permitted narrowly as read-only discovery/import check (no `.py` files created/run); strict-policy fallback = fail with explicit guidance, no guessing (see §9.2/R1).
- **Interpreter/CLI precedence and proof of same-environment use — RESOLVED (R2):** interpreter-first, module-invocation-first. Precedence `py -3` > `python` > `uv run python` (usability/import-checked). Same `$Exe @Prefix` for all `-m pip install --upgrade ...`, `-m mko_get_mediascope_data init|list-reports|run ...`, and probes. Prefer `-m` over console script; post-install import/path probe as proof.
- **JSON5 edit implementation pattern — RESOLVED (R4):** strict single-match regex replacement of only `"export_folder"` string value; JSON-escaped replacement; UTF-8 read via .NET, UTF-8 no-BOM write via `[System.IO.File]::WriteAllText(..., [System.Text.UTF8Encoding]::new($false))`; abort on 0/>1 matches or invalid input; no parse+rewrite. Store absolute normalized path.
- **Best-effort git update behavior when present — RESOLVED (R3):** detect via `git --version`/`Get-Command`. Missing: try winget install (`Git.Git`) if available else manual guidance and stop `ОШИБКА`. Re-check in-session: if not usable or newly installed this run => stop after Step 1 with explicit restart message, overall failed/non-success. If present initially: Step 1 may succeed; attempt best-effort update when practical (e.g. winget upgrade) but on failure report current version and continue.
- **Report label format in Script 2 — RESOLVED (R5):** relative path from reports root as primary display label (unambiguous), deterministic sort by relative path (case-insensitive, stable). Full path may be shown as secondary detail only if unambiguous; relative path remains primary.
- **`-c` probe boundary under Q1 — RESOLVED (contract, R1):** treated as compliant with “no intermediate `.py` script files may be created or run” because no file is created; scope limited to read-only path discovery and import checks. Escalate if stricter policy imposed.

### 6.2 Residual uncertainties (external / non-blocking for gate, must be handled at runtime)
- Winget availability, acceptance flags behavior, elevation/UAC, corporate policy, installer side effects, and whether PATH becomes usable in-session.
- Network/proxy access for git installers, `pip install --upgrade`, and git+ dependency install.
- PATH refresh limitations after installs; multiple Python/uv environment layouts and uv context semantics.
- Console font/codepage rendering of Cyrillic in some legacy hosts despite UTF-8 script/BOM measures.
- Unusual `app_config.json5` formatting where `"export_folder"` does not match the strict single-match pattern (safe abort, no write — user must fix manually).
- Environment-dependent feasibility of “best-effort git update when present”.

### 6.3 Still out of scope / not required
- **Optional `examples/01-get-cli/README.md` or header-only usage guidance:** not required by spec (open question #193). Out of scope unless explicitly requested later with no scope creep.

## 7. Required agents by block (consolidated)

| Block | Risk | Auditor | Researcher | Planner | Validator | Rationale (summary) |
|---|---|---|---|---|---|---|
| B1 | High | Yes | Yes | Yes | Yes | Cross-cutting technical uncertainty and high-impact risks require investigation, options analysis, detailed design, and independent review. |
| B2 | High | Yes | Yes | Yes | Yes | Shared foundations propagate correctness/safety; compatibility and invocation/path behavior need validated patterns and reviewed contracts. |
| B3 | High | Yes | Yes | Yes | Yes | External installs, PATH/restart handling, and JSON5 mutation are high-risk; require evidence-based implementation and independent safety review. |
| B4 | Medium–High | Yes | Conditional (escalate to Yes if B1 leaves PS/edge-case uncertainty) | Yes | Yes | Correct scoping/copy/run behavior and destructive-boundary safety need audit-backed understanding, planned flow, and independent validation. |
| B5 | Medium | Conditional (if audited assumptions contradicted) | No (unless unexplained external behavior) | No | Yes | Manual acceptance validation required; escalate investigation roles only on evidenced discrepancies. |

## 8. Downstream notes for implementation steps
- Treat B1 as a **decision gate**: do not start irreversible implementation choices for env/path/JSON5/git flows until contracts are recorded and validated.
- Preserve spec traceability: map implementation checks directly to §13 acceptance criteria.
- Keep changes isolated to `examples/01-get-cli/init.ps1` and `examples/01-get-cli/export.ps1` unless an explicitly approved, separate scope decision is made.
- If a required behavior cannot be achieved without module/CLI changes or violates “no intermediate `.py`”, stop and escalate as a separate problem/change request (per spec DoR/constraints).


---

## B1 Audit Notes (Step 3.1, 2026-11-18) — evidence-based contract findings

> Scope: audit only (no code changes). This section records concrete evidence and risks for B1 contract areas to unblock B2–B4/B5.

### B1.1 User settings/report path discovery without module imports (Windows, PS 5.1/7+)

**Authoritative runtime behavior (code facts, cited):**
- `src/mko_get_mediascope_data/core/paths.py`: `APP_NAME = (__package__ or "").split(".")[0]` = `mko_get_mediascope_data`. `USER_DIR = Path(user_config_dir(APP_NAME))` (platformdirs). `user_settings_dir = USER_DIR/settings`, `app_config = user_settings_dir/app_config.json5`, `reports = user_settings_dir/reports`, `defaults_dir = user_settings_dir/defaults`, `log_config = user_settings_dir/log_config.json5`. `connection_config` prefers `connections/mediascope.json`, falls back to legacy `mediascope_RENAME_.json` (warn in logs), returns primary if neither exists.
- Observed (repo, `uv run python`): `user_config_dir("mko_get_mediascope_data")` => on this host `C:\Users\...\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data` (default call in code); `APP_PATHS.user_settings_dir` => `...\settings`; `app_config` => `...\settings\app_config.json5`; `reports` => `...\settings\reports`. Runtime uses default `user_config_dir(APP_NAME)` (platformdirs defaults for the environment).
- `core/init_service.py`: `init_project()` migrates legacy credentials under `APP_PATHS.user_settings_dir/connections/*`, copies bundled templates from `APP_PATHS.app_settings_dir` (package `settings/`) recursively into `user_settings_dir` (atomic copy). Non-force skips existing files; force overwrites. Returns `APP_PATHS.user_settings_dir`.
- CLI (`app.py`): `init` prints `Settings initialized: {path}` where path is `init_project()` result. `list-reports` uses `app_service.app_paths.reports`, lists recursive `*.json5` via `list_files_in_directory(..., include_subfolders=True)`, prints `• {f}` (sorted) or `No report files found in reports/`. `run` requires existing report file. `_get_app_service_or_exit()` only converts `FileNotFoundError` (missing required settings bootstrap context) into a hint suggesting `init`; other validation errors propagate.
- Bundled `src/mko_get_mediascope_data/settings/app_config.json5` is JSON5 with comments and `"export_folder": "output"`. User files may differ; edits must be JSON5-targeted (no full JSON rewrite).

**README drift (critical for scripts):** README states Windows example path `%APPDATA%\mko_get_mediascope_data\settings\` and Linux `~/.config/.../settings/`. Observed runtime on this Windows host resolves under `AppData\Local\...\mko_get_mediascope_data\mko_get_mediascope_data\settings` via platformdirs default. Scripts must not hardcode README paths; they must match platformdirs/runtime truth.

**Discovery options (no module imports constraint is the core tension):**
- **Not recommended:** parse `init` output (brittle, may include emoji/color/locale effects; `init` also mutates state by copying missing templates). **Not recommended:** rely on env-var heuristics alone (`%APPDATA%`/`%LOCALAPPDATA%`) because the resolved `user_config_dir` base shape can include an extra app-name segment on this host and platformdirs rules vary.
- **Recommended for correctness (if boundary allows inline probe):** run the selected Python interpreter with `-c` (no project `.py` file created) to print `APP_PATHS.user_settings_dir`, `APP_PATHS.app_config`, `APP_PATHS.reports` (machine-parseable). Requires package importable in chosen env (after installs) and uses platformdirs exactly as the app. **Flag:** Q1 says “do not create or run intermediate .py files”; an inline `-c` command does not create a file — treat as acceptable in practice unless policy forbids; document decision in B2.
- **Fallback if inline probe disallowed:** do not guess reliably from env vars alone; if settings appear missing, instruct user to run `init` (Script 1) then retry. For Script 2, if config/reports missing, report error and suggest `init.ps1` / `mko-get-mediascope init`.

**Paths scripts must use (once resolved):** user settings root = `user_settings_dir`; `app_config.json5` = `user_settings_dir/app_config.json5`; reports root = `user_settings_dir/reports` (recursive `*.json5` for Script 2). Custom copies go to reports root with original filename (Q6).

### B1.2 Interpreter and CLI resolution precedence and env consistency

**Evidence from examples:** `examples/01-get/run_report.ps1`/`.bat` try `py -3` (verify `import sys`), then `python`, then `uv run python`; invoke scripts via that interpreter. Those examples use internal Python bootstrap (anti-pattern) — this task requires CLI-only module ops.

**Contract recommendation:**
- Choose one target interpreter/command form consistently (precedence proposal): prefer `py -3` if usable, else `python` if usable, else `uv run python`. Record and print the chosen target (Russian message). Use the same chosen form for pip installs (Q3) and for module CLI invocation.
- **Installs (Q3):** run `"<chosen> -m pip install --upgrade mediascope-api-lib"` and `"<chosen> -m pip install --upgrade git+https://github.com/manicko/mko_get_mediascope_data.git"`. If chosen is `uv run python`, the effective form is `uv run python -m pip ...` (consistent with that uv context) — document the exact command string shown.
- **Module CLI ops (Q1):** prefer `"<chosen> -m mko_get_mediascope_data init|list-reports|run ..."` for env consistency (avoids PATH console-script pointing to another env). B2 should standardize on `-m` with chosen interpreter and report it.
- On probe failure, report explicit error and stop per step rules.

### B1.3 Git detect/install/update/restart semantics (Q2=A)

- Detect: `git --version` (check exit code). `Get-Command git` can help locate presence.
- If missing: prefer `winget install --id Git.Git -e --source winget` if `winget` available. If winget unavailable or install cannot complete automatically (elevation/policy/failures) => clear manual guidance (e.g. https://git-scm.com/download/win) and stop with overall error.
- After any install attempt, re-check `git --version` in the same PowerShell session. If still not usable => stop after Step 1 with explicit restart message (PATH/session refresh) and do not claim success (non-zero). If became usable in the same session, treat as newly installed in this run => **stop after Step 1 with explicit restart message** (conservative, per spec intent) and do not proceed to Step 2 in that run (overall failed/non-success for this run). B2 must encode this precisely.
- If present initially: best-effort update when supported (e.g. try `winget upgrade --id Git.Git -e` if available) but if it fails/elevation required, report current version and continue (do not fail Step 1). Step 1 success requires usable `git`.
- Git is required for Step 3 (`pip install --upgrade git+...`).

### B1.4 JSON5-safe targeted edit of export_folder (Q4=A)

- Target: resolved `app_config.json5` (user settings). Do not create directories. Validation: `Test-Path -PathType Container` on the effective directory the user means after normalization rules (trim, expand `~`).
- **Relative/tilde/absolute input:** accept abs/rel/`~`. Expand `~` in PS (`$env:USERPROFILE`). **Critical contract point:** app resolves relative `export_folder` via `PathResolver.resolve()` relative to the resolver base (service context). Scripts must align validation/help text with app behavior to avoid user confusion; B2 should state the assumed base in Russian output and be explicit. Flag as B2 decision.
- **Edit strategy (recommended):** read UTF-8 text. Use a strict single-match regex over the whole file for `"export_folder"` key with a JSON string value (e.g. match `^(\s*)"export_folder"\s*:\s*"((?:\\.|[^"\\])*)"(.*)$` in a multiline scan or equivalent). If 0 matches => abort, no write. If >1 matches => abort, no write. Replace only the value token with a JSON-escaped string of the new validated path (escape backslashes and quotes). Write back as UTF-8 without BOM when feasible (`System.IO.File.WriteAllText` with UTF8 no-BOM) for PS 5.1/7 consistency. Do **not** use `ConvertFrom-Json`/`ConvertTo-Json` rewrites. On any ambiguity/failure => explicit error and no file change.

### B1.5 Script 2 discovery/selection/copy contracts (Q5=A, Q6=A)

- Enumerate: recursive `*.json5` under `<user_settings_dir>/reports` (same scope as `list_files_in_directory` include_subfolders). Use filesystem scan (`Get-ChildItem -Recurse -Filter *.json5 -File`), sort deterministically by relative path from reports root (case-insensitive on Windows).
- Display: `1) <relativePath>` ..., `0) ...` custom. Relative path primary; labels unambiguous.
- Selection: integer 0..N; invalid => re-prompt/return to selection with Russian error.
- Custom (0): prompt for existing `.json5` file path. Validate exists, is file, extension `.json5` (case-insensitive). Invalid => error, return to selection.
- Copy: copy to `<reports_root>/<originalFileName>` (reports root). If destination exists => warn it will be replaced and ask `[R]eplace/[C]ancel` (default Cancel; empty => Cancel). Replace => overwrite and run copied path `<reports_root>/<originalFileName>`. Cancel => return to selection without running.
- Run: `"<chosen> -m mko_get_mediascope_data run <path>"` with no `--export-folder`; success/failure by exit code, preserve CLI output.

### B1.6 UX/error model (consolidated)

- Russian messages with `УСПЕХ` / `ОШИБКА`; preserve CLI output as-is.
- Explicit step labels (Script 1: Шаг 1–4; Script 2: Шаг 1–3). Exit-code driven outcomes for native/CLI calls.
- Keep window open: `try/catch/finally` with `Read-Host "Нажмите Enter для выхода..."` (PS 5.1/7 compatible). Document encoding caveats.
- Stop on failed mandatory step except required restart-stop (Q2) which is controlled stop with explicit message and non-zero overall result.
- Idempotent re-runs where stated; Script 2 mutates reports only on confirmed replace/copy.

### B1.7 Alignment vs current CLI/spec (discrepancies/risks)

- README Windows path differs from observed runtime (platformdirs) — scripts must follow runtime truth.
- `list-reports` output is human-formatted; scripts must enumerate filesystem for numbering (do not parse).
- `init` output parsing brittle; avoid for discovery.
- No CLI get/set for `export_folder`; only file edit allowed (Q1). `run --export-folder` must not be used by scripts.
- Inline `-c` introspection is not a `.py` file on disk but is code execution — B2 must decide acceptability under “no intermediate .py files”.
- CLI errors: treat non-zero exit as failure; `_get_app_service_or_exit()` hints only on `FileNotFoundError`.

---

## B1 Step 3.2 — Researcher: alternatives, best-path contracts, and gate recommendations (2026-11-18)

> Scope: research/design only (no code, no audit-phase changes). Evidence sources: spec/plan, B1.1–B1.7 audit notes, `core/paths.py`, `core/init_service.py`, `app.py`, `core/services.py`, bundled `settings/app_config.json5`, `examples/01-get/run_report.ps1|.bat`, host/runtime probe (`py -3`/`python`/`uv`/console script available, `git 2.55.0`, `winget` present, PS 7.6.10, Windows runtime path resolves to `...\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data\settings`). Confidence is marked HIGH/MEDIUM/LOW per decision.

### R1. Settings/report path discovery (no module imports / runtime truth)

**Decision (selected best path, HIGH):** use **inline interpreter probe via `-c`** with the *chosen* target interpreter to obtain `APP_PATHS.user_settings_dir`, `APP_PATHS.app_config`, and `APP_PATHS.reports` in a machine-parseable form, **after** the package is installed/importable in that environment (Script 1: after Step 3 + successful `init`; Script 2: at start if package importable, else fail fast with guidance). Do **not** parse `init` output and do **not** infer from README or raw `%APPDATA%`/`%LOCALAPPDATA%` heuristics.

**Rationale:** `core/paths.py` resolves `USER_DIR = Path(user_config_dir(APP_NAME))` and appends `settings` (`APP_NAME="mko_get_mediascope_data"`). Observed host truth includes a doubled app-name segment under `AppData\Local\...` (differs from README `%APPDATA%` example). Only platformdirs-in-the-target-environment gives deterministic correctness; derivation from env vars is fragile across hosts/defaults.

**Inline `python -c` boundary (explicit, HIGH):** `-c` executes inline code in memory and **does not create or run an intermediate `.py` file on disk**; it satisfies Q1/A’s literal “no intermediate `.py` script files may be created or run”. It is a narrow, read-only discovery probe (imports `core.paths`/prints constants) — not module report logic and not a script file — and is necessary for correctness. **Contract:** permit `python -c`/`py -3 -c`/`uv run python -c` **only** for deterministic path discovery and minimal environment/import checks; forbid creating temporary `.py` files and forbid embedding business logic in probes. If a stricter interpretation is later imposed, fallback is: require successful `init` in same env and fail with explicit “cannot determine settings path without module probe; run init in the selected environment then retry” (no guessing) — residual correctness risk remains pre-init.

**Probe contract (recommended):**
- Command form: `& $PythonExe @PyPrefix -c "from mko_get_mediascope_data.core.paths import APP_PATHS; print(APP_PATHS.user_settings_dir); print(APP_PATHS.app_config); print(APP_PATHS.reports)"` (or print a single JSON line via stdlib `json.dumps` if quoting is easier). Parse stdout lines in order; trim CRLF.
- Run probe only when import is expected to succeed (post-install/init in Script 1; in Script 2 after environment resolution — if import fails, show `ОШИБКА` + suggest `init.ps1`/`mko-get-mediascope init` and stop).
- Validate returned paths: `app_config` must end with `app_config.json5` under resolved settings dir; `reports` must be `<settings>\reports` (or equivalent). On parse/validation failure: stop with explicit error (do not guess).

**Alternatives considered:**
- *A. Env-var/platformdirs-equivalent reconstruction in PowerShell* — rejected as primary (cannot guarantee parity with Python/platformdirs on all Windows layouts). LOW–MEDIUM confidence for general case.
- *B. Parse `init` output / run init for discovery* — rejected (side effects, brittle formatting/Rich output). LOW.
- *C. Inline `-c` probe (selected)* — HIGH correctness, minimal scope, no `.py` artifacts.

**Residual uncertainty (MEDIUM):** rare environments where `mko_get_mediascope_data` is installed but `platformdirs`/imports behave unexpectedly, or where PATH/venv activation differs from chosen interpreter; mitigated by using the same chosen interpreter for installs and probe and by explicit failure on import/parse errors.

### R2. Interpreter and CLI resolution precedence & same-environment consistency

**Decision (selected best path, HIGH):** **interpreter-first, module-invocation-first** model. Select one target for the whole script run and use it for **all** `pip` installs, `init/list-reports/run` module operations, and path probes.

**Precedence (deterministic):**
1. `py -3` if `Get-Command py` succeeds and `& py -3 -c "import sys"` exits 0.
2. `python` if available and `& python -c "import sys"` exits 0.
3. `uv run python` if `uv` available (treat `uv` as exe and prefix args `@('run','python')`).

Record and print chosen target in Russian (e.g. `Шаг N: выбран интерпретатор ...`).

**Module CLI operations (Q1):** always invoke as `& $Exe @Prefix -m mko_get_mediascope_data <init|list-reports|run> ...` in the chosen environment (not the console script) to avoid console-script/env mismatch (host evidence shows `mko-get-mediascope.exe` under Roaming Python Scripts while project may run under different interpreter/uv context). `list-reports` CLI output remains human-formatted; Script 2 must still enumerate filesystem for numbering (B1.5).

**Package installs (Q3):** use the same target:
- `& $Exe @Prefix -m pip install --upgrade mediascope-api-lib`
- `& $Exe @Prefix -m pip install --upgrade git+https://github.com/manicko/mko_get_mediascope_data.git`

For uv target this is `uv run python -m pip ...` (consistent uv context). **Proof of same-environment use:** every module op/install uses identical `$Exe+@Prefix`; after installs run `& $Exe @Prefix -c "import mko_get_mediascope_data"` (or combined path probe) and fail if non-zero. Report command strings/target clearly.

**Alternatives considered:**
- *B. Console-script-first* — rejected (env pinning/mapping is non-trivial; observed drift risk). MEDIUM.
- *C. Hybrid with extensive auto-detection* — acceptable but unnecessary complexity; selected A gives strongest consistency with minimal logic.

**Residual uncertainty (MEDIUM):** `uv run` environment semantics depend on project/venv state; pip upgrades inside it may affect the resolved env differently than a plain venv — mitigated by consistent `-m` usage and post-install import/probe checks. Multiple Pythons installed system-wide remain a user/environment concern; scripts must report the chosen one.

### R3. Git detect/install/update/restart semantics (Q2=A)

**Decision (selected best path, HIGH for detection/restart contract; MEDIUM for best-effort update outcomes):**
- **Detect:** `$gitCmd = Get-Command git -ErrorAction SilentlyContinue`; confirm with `& git --version` and check `$LASTEXITCODE -eq 0`.
- **If missing:** if `Get-Command winget` exists, attempt `winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements` (non-interactive where possible). If winget missing or install returns non-zero or `git` not usable after attempt, print clear manual guidance (https://git-scm.com/download/win) and stop Step 1 with `ОШИБКА` (overall non-zero).
- **Restart-stop rule (spec-aligned, conservative):** after any install attempt, re-check usability in the **same** session. If `git` is still not usable => stop after Step 1 with explicit restart/PATH message and overall failed status. If `git` became usable in-session but was installed in this run => treat as newly installed and **stop after Step 1** with explicit restart message and overall failed/non-success status for this run (do not proceed to Steps 2–4). Rationale: PATH refresh in current session is unreliable; conservative stop avoids partial success claims.
- **If present initially:** usable `git` means Step 1 can succeed. Perform **best-effort** update only when safe/practical (e.g. if `winget` available try `winget upgrade --id Git.Git -e --source winget ...`); on non-zero/failure/elevation/policy issues report current `git --version` and **continue** (do not fail Step 1). Avoid aggressive update paths that can break working git.
- **No guarantee of in-session PATH refresh:** do not attempt fragile `$env:Path` reconstruction except optional best-effort re-read of Machine/User env vars; do not claim success without `git --version` working.

**Residual uncertainty (MEDIUM–HIGH, external):** UAC/elevation prompts, corporate winget restrictions, network/proxy issues, installer requiring reboot, or `git` installed but shadowed by broken PATH entries. All must result in explicit user-facing status and correct stop behavior, not silent success.

### R4. JSON5-safe targeted edit of `export_folder` (Q4=A, PS 5.1/7+)

**Decision (selected best path, HIGH):** **strict single-match text replacement** of only the `"export_folder"` string value token; preserve all other content/comments/ordering; never parse+rewrite JSON/JSON5 structures.

**Algorithm:**
1. Resolve and validate target file path (`user_app_config = <user_settings_dir>\app_config.json5`, from R1). If missing/unreadable: `ОШИБКА`, no change.
2. Prompt: show current raw `export_folder` value (extracted read-only via same strict regex, first/only match) and optionally show resolved interpretation note. Ask whether to change.
3. On decline/empty-as-no-change (per B1 UX): leave file untouched (`УСПЕХ`/neutral step outcome as defined).
4. On change: `$input = <user input>.Trim()`. Reject empty. Expand leading `~` to `$HOME` (`$env:USERPROFILE` on Windows) for validation and normalization. Accept absolute/relative/`~` input forms.
5. **Existence validation (required):** determine candidate directory path for `Test-Path -LiteralPath <candidate> -PathType Container`. If user provided relative path, validate the resolved absolute form for existence but **do not create directories**. Recommended normalization for storage: save an **absolute, normalized path** (full path, no trailing ambiguity issues) to eliminate resolver-base ambiguity (service resolves relative `export_folder` via `PathResolver` base context; storing absolute is unambiguous and user-visible). If keeping relative is preferred later, contract must state resolution base explicitly — selected path is **store absolute** for safety/clarity.
6. If validation fails (missing/not a container/invalid): explicit `ОШИБКА`, **no file modification**.
7. Read file as UTF-8 text: `$text = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)`.
8. Find key with strict regex requiring **exactly one** match in the whole file, e.g. pattern: `(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)"` (captures leading indentation+key+colon+opening quote context and value). Use `[regex]::Matches($text, $pattern)`. If `Count -ne 1`: abort with explicit ambiguity/missing-key error, **no write**.
9. Escape replacement value for JSON string content: replace `\` -> `\\`, `"` -> `\"`, and escape CR/LF/tabs/control chars if present (unlikely for paths). Build replacement preserving prefix: `$m.Groups[1].Value + '"' + $escaped + '"'`.
10. Replace only that match (single occurrence): `$newText = $regex.Replace($text, [System.Text.RegularExpressions.MatchEvaluator]{ param($m) $m.Groups[1].Value + '"' + $escaped + '"' }, 1)` or equivalent safe string slicing by match index/length.
11. Write UTF-8 **without BOM**: `[System.IO.File]::WriteAllText($path, $newText, [System.Text.UTF8Encoding]::new($false))`. This avoids PS 5.1 `Set-Content -Encoding UTF8` BOM behavior; PS 7+ also fine.
12. Re-read and confirm saved value (and resolved path) to user.

**Alternatives considered:**
- *B. Manual line scan/splice* — viable but easier to get wrong for spacing; regex with count==1 is sufficient and clearer.
- *C. JSON5 parse/rewrite* — rejected (would drop comments/formatting, adds tooling/dependency concerns, high risk to constraints).

**Residual uncertainty (LOW–MEDIUM):** unusual formatting of `export_folder` not matching the strict pattern (e.g. value split oddly, though bundled template is standard) will correctly trigger abort-no-write; users must then fix manually — acceptable safety trade-off. Control characters in paths are unsupported/should be rejected.

### R5. Script 2 discovery/selection/copy/run contracts (Q5=A, Q6=A)

**Decision (selected best path, HIGH):**
- **Enumeration scope:** recursive `*.json5` files under `<reports_root>` from R1 (`Get-ChildItem -LiteralPath $reportsRoot -Recurse -Filter '*.json5' -File -ErrorAction Stop`).
- **Deterministic sort:** sort by **relative path from reports root**, case-insensitive ordinal (`[StringComparer]::OrdinalIgnoreCase`) then ordinal as tiebreaker if needed. Windows paths are case-insensitive in practice but deterministic sort must be stable.
- **Display labels:** numbered `1..N` with **relative path from reports root** as primary label (unambiguous within tree, compact). Optionally append full path in dimmer/detail text is allowed but not required; primary label must be relative path. Show `0` as custom-path option (e.g. `0) Указать путь к файлу отчёта вручную`).
- **Selection loop:** validate integer input in `[0..N]`; on invalid/empty show `ОШИБКА` and re-prompt/return to selection. If N==0, still offer option 0 or report “no reports found” but allow custom path per spec intent (explicit message).
- **Custom path (0):** trim input, accept quoted/absolute/relative/`~` as path string; normalize/expand `~`; require existing file and `.json5` extension (case-insensitive). On invalid: `ОШИБКА`, return to selection.
- **Copy destination/conflict:** destination is **reports root** + original filename (`Split-Path -Leaf`). If destination exists: warn that file will be replaced and prompt `[R]eplace/[C]ancel` (default Cancel; empty or any non-R => Cancel). **Replace:** copy/overwrite to destination and set run target to **copied path in reports root**. **Cancel:** return to selection without running/copying/replacing.
- **Run:** `& $Exe @Prefix -m mko_get_mediascope_data run <targetPath>` with **no** `--export-folder`; preserve CLI stdout/stderr as-is; determine success/failure by exit code (`$LASTEXITCODE -eq 0` and no terminating exception).

**Residual uncertainty (LOW):** filename collisions across subfolders when copying different custom files with same leaf name to reports root — handled explicitly by replace/cancel prompt (by design).

### R6. UX/error model and PS 5.1/7+ compatibility guidance (Q7=A, Q8=A)

**Decision (selected best path, HIGH):**
- **Structure:** each script uses step labels (`Шаг 1`…`Шаг 4` for init, `Шаг 1`…`Шаг 3` for export), per-step status messages, and a final `УСПЕХ`/`ОШИБКА` summary. Do not claim success on failure.
- **Keep window open:** top-level `try { ... } catch { ...; $script:HadError=$true } finally { Read-Host 'Нажмите Enter для выхода' }`. Compatible with PS 5.1 and 7+; works for double-click when console host remains. Ensure final pause runs even on early/mandatory stop paths (use controlled flow flags/returns inside try).
- **Exit codes:** for native commands and CLI/module calls check `$LASTEXITCODE` immediately after invocation; treat non-zero as failure. Do not rely on `$?` for native executables. Capture/report failing step and command context.
- **Error handling:** prefer `$ErrorActionPreference = 'Continue'` globally with explicit checks for native exit codes (matches examples) or use `'Stop'` with try/catch around cmdlets — but native exe failures require `$LASTEXITCODE` checks regardless. Recommended: explicit `$LASTEXITCODE` checks after every external command and throw/stop-step on failure for mandatory steps.
- **Encoding/Russian text (PS 5.1/7+):** save `.ps1` files as **UTF-8 with BOM** for reliable parsing of Cyrillic in Windows PowerShell 5.1; PowerShell 7+ also reads UTF-8 with BOM fine. For console output, consider `[Console]::OutputEncoding = [System.Text.Encoding]::UTF8` and `$OutputEncoding = [System.Text.Encoding]::UTF8` at script start when needed, but preserve child CLI output unmodified (do not re-encode piped CLI text). Prefer `[System.IO.File]` APIs for config read/write with explicit UTF-8 encodings (no BOM on write).
- **Path handling:** use `-LiteralPath` where possible; trim inputs; expand `~` manually for Windows; resolve with `[System.IO.Path]::GetFullPath()` / `Resolve-Path -LiteralPath` for existence checks as appropriate. Quote paths in command invocations.
- **Regex/text replacement:** use `[regex]`/.NET APIs for predictable behavior across hosts; avoid PS 7-only operators/features where 5.1 must work. Use single-match enforcement (`Matches.Count`).
- **Restart-stop (Script 1 Step 1):** controlled stop after Step 1 with explicit restart message and overall failed/non-zero result for this run (not silent success), while still reaching `finally` keep-open.

**Residual uncertainty (MEDIUM):** console font/codepage may still render Cyrillic poorly in some legacy hosts despite UTF-8; file encoding (UTF-8 with BOM for scripts) mitigates parse issues. Double-click behavior depends on file association/host settings — `finally` + `Read-Host` is best-effort standard.

### R7. Consolidated selected best-path summary (for B1 gate)

| Area | Selected path | Confidence | Key risk/mitigation |
|---|---|---|---|
| Path discovery | Inline `-c` probe via chosen interpreter after importable; no init-output parsing, no env guessing | HIGH | Probe fails -> explicit stop; no `.py` artifacts |
| `-c` boundary | Allowed narrowly for read-only discovery/import checks (no `.py` files) | HIGH (contract) | Document exception; fallback = require init/no guessing |
| Interpreter/CLI | Interpreter-first precedence `py -3` > `python` > `uv run python`; all ops via `-m` in same target | HIGH | Report chosen target; post-install import/probe proof |
| Git | Detect via `git --version`; winget opportunistic install/upgrade; restart-stop conservative; usable-present => continue on failed best-effort update | HIGH/MED | Elevation/network/PATH external |
| `export_folder` edit | Single-match regex replace, JSON-escaped value, UTF-8 no-BOM write, abort on 0/>1 or invalid path; store absolute normalized path | HIGH | Unusual formatting -> safe abort |
| Script 2 listing/copy | FS recursive enum, relative-path sort/labels, 0 custom, copy to reports root, replace/cancel default Cancel, run copied path | HIGH | Name collision handled by prompt |
| UX/compat | Step labels, `УСПЕХ/ОШИБКА`, preserve CLI output, exit-code driven, try/catch/finally+Read-Host, UTF-8 BOM scripts | HIGH | Console rendering varies |

### R8. Gate status and residual external uncertainties (cannot be fully eliminated)

- **B1 gate can be closed for implementation** if downstream blocks implement R1–R7 contracts and preserve spec/CLI boundaries. No module/CLI/audit-phase changes required.
- **Residual/external uncertainties:** winget availability/elevation/agreements and install side effects; network access for git/pip; PATH refresh in current session; multiple Python/uv environment layouts; console encoding/font rendering for Cyrillic in legacy hosts; rare `app_config.json5` formats not matching strict key pattern (safe abort). These require clear runtime detection, explicit user messaging, and correct stop semantics rather than speculative automation.
- **Escalation condition:** if implementation cannot satisfy “no intermediate `.py` files” under a stricter policy than R1’s narrow `-c` allowance, or cannot achieve reliable runtime-path discovery without module/CLI changes, stop and raise as a separate problem/change request (per spec §12/constraints).

---

## 9. B1 Gate — Resolved Contracts & Implementation Task Breakdown (Step 3.3, Planner)

**Date:** 2026-11-18  
**Status:** B1 gate **CLOSED** for implementation planning. Downstream blocks B2–B5 may proceed only in accordance with R1–R7 contracts below.  
**Sources:** `.ai/problems/04_powershell_cli_scripts_spec.md`, `.ai/plans/04-powershell-cli-scripts.md` (B1.1–B1.7, R1–R8), `.ai/tasks/templates/task_template.yaml`.  
**Hard constraints:** no changes to `src/`, dependencies, CLI behavior, build/release config, or existing audit phase files; no intermediate `.py` files created or run; module operations via existing CLI only (`init` no `--force`, `list-reports`, `run`); no `--export-folder` in Script 2 run; scripts only `examples/01-get-cli/init.ps1`, `examples/01-get-cli/export.ps1`.

### 9.1 Gate exit criteria (met)
- [x] User settings/report path discovery contract recorded and deterministic (R1).
- [x] Interpreter/CLI precedence and same-environment proof recorded (R2).
- [x] Git detect/install/update/restart-stop semantics recorded (R3).
- [x] JSON5-safe `export_folder` read/edit contract recorded (R4).
- [x] Script 2 discovery/selection/custom-copy/run contracts recorded (R5).
- [x] UX/error/keep-open and PS 5.1+7+ compatibility contract recorded (R6).
- [x] Residual external uncertainties recorded; no module/CLI changes required; escalation path defined (R7/R8, §6.2).

### 9.2 Resolved contracts (implementation-ready, R1–R7)

#### R1 — User settings/report path discovery (runtime truth, no guessing)
- **Truth source:** `APP_PATHS` from `mko_get_mediascope_data.core.paths` in the **chosen target interpreter/environment** (`user_settings_dir = platformdirs user_config_dir(APP_NAME) / "settings"`, `app_config = .../app_config.json5`, `reports = .../reports`). Do not rely on README paths or `%APPDATA%`/`%LOCALAPPDATA%` reconstruction.
- **Mechanism:** inline read-only probe via `-c` (no `.py` file):  
  `& $Exe @Prefix -c "from mko_get_mediascope_data.core.paths import APP_PATHS; print(APP_PATHS.user_settings_dir); print(APP_PATHS.app_config); print(APP_PATHS.reports)"` (or equivalent machine-parseable single JSON line via stdlib `json` if quoting simpler). Trim CRLF; parse in order.
- **When to probe:**
  - `init.ps1`: after Step 3 package install and successful CLI `init` (package importable) to resolve `app_config.json5` for Step 4; also usable for post-install proof.
  - `export.ps1`: after interpreter resolution if package importable; if import/probe fails, show `ОШИБКА` with guidance to run `init.ps1` / `mko-get-mediascope init` in the selected environment and stop.
- **Validation:** `app_config` ends with `app_config.json5` under settings dir; `reports` is settings reports path. Parse/validation/import failure => explicit `ОШИБКА`, no guessing, stop as appropriate.
- **Boundary:** `-c` allowed only for read-only path discovery and minimal import checks (no business logic, no temp `.py`). Stricter-policy fallback: require init and fail clearly without guessing; escalate if blocking.

#### R2 — Interpreter/CLI resolution and same-environment consistency
- **Model:** interpreter-first, module-invocation-first. Single target for entire script run.
- **Precedence:** (1) `py -3` if `Get-Command py` works and `& py -3 -c "import sys"` exits 0; (2) `python` if usable via `-c "import sys"`; (3) `uv run python` if `uv` available (exe=`uv`, prefix=`run`,`python`). Report chosen target in Russian.
- **All operations use same `$Exe @Prefix`:** `-m pip install --upgrade mediascope-api-lib`, `-m pip install --upgrade git+https://github.com/manicko/mko_get_mediascope_data.git`, `-m mko_get_mediascope_data init` (no `--force`), `-m mko_get_mediascope_data run <path>` (no `--export-folder`), and R1 probes/import checks. Prefer `-m` over console script.
- **Proof:** post-install import/probe success in chosen target; report command target clearly. On probe failure => explicit error and stop per step rules.

#### R3 — Git detect/install/update/restart-stop semantics (Q2=A)
- **Detect:** `Get-Command git -ErrorAction SilentlyContinue` and `& git --version` with `$LASTEXITCODE -eq 0`.
- **Missing:** if `winget` available attempt `winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements` (best-effort flags). If winget missing or install fails/unusable => clear manual guidance (e.g. `https://git-scm.com/download/win`) and stop Step 1 with `ОШИБКА` (overall failed).
- **After any install attempt (same session re-check):** if `git` still not usable => stop after Step 1 with explicit restart/PATH message, overall failed/non-success. If `git` became usable but was installed this run => **stop after Step 1** with explicit restart message, overall failed/non-success for this run (do not proceed to Steps 2–4). Reach `finally` keep-open.
- **Present initially:** Step 1 can succeed. Best-effort update when practical (e.g. `winget upgrade --id Git.Git -e --source winget ...` if available); on failure/elevation/policy issues report current `git --version` and continue (do not fail Step 1). Do not claim in-session PATH refresh success without working `git --version`.

#### R4 — JSON5-safe targeted `export_folder` edit (Q4=A)
- **Target:** user `app_config.json5` resolved via R1. Do not create directories.
- **Read/display:** show current raw `"export_folder"` value (read-only single-match extraction). Optional resolved/interpretation note allowed if unambiguous; must not mislead about relative-path base. Ask to change.
- **Input:** trim; empty/decline => no write. Expand leading `~` to `$env:USERPROFILE` for validation/normalization. Accept abs/rel/`~`.
- **Validation:** candidate must exist as directory: `Test-Path -LiteralPath <candidateAbsoluteForCheck> -PathType Container`. No directory creation. Invalid/missing => explicit `ОШИБКА`, no modification.
- **Storage (selected):** save **absolute, normalized** path to avoid resolver-base ambiguity; confirm saved value to user.
- **Edit algorithm:** read UTF-8 via `[System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)`. Regex whole-file single match for `"export_folder"` string value (e.g. `(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)"` or equivalent strict form). If match count != 1 => abort with explicit error, no write. JSON-escape replacement (`\`, `"`, control chars). Replace only that match. Write UTF-8 no BOM via `[System.IO.File]::WriteAllText($path, $newText, [System.Text.UTF8Encoding]::new($false))`. Re-read/confirm. Never `ConvertFrom-Json`/`ConvertTo-Json` rewrite.

#### R5 — Script 2 discovery, selection, custom copy, and run (Q5=A, Q6=A)
- **Enumeration:** recursive `*.json5` under R1 `reports` root (`Get-ChildItem -LiteralPath $reportsRoot -Recurse -Filter '*.json5' -File -ErrorAction Stop`).
- **Sort/labels:** deterministic sort by relative path from reports root, case-insensitive ordinal (`[StringComparer]::OrdinalIgnoreCase`, stable tiebreak). Display `1..N` with **relative path** primary label; `0` = custom path option (Russian text). If none found, report clearly but still allow option 0 where sensible per UX.
- **Selection:** validate integer in `0..N`; invalid/empty => `ОШИБКА` and re-prompt/return to selection.
- **Custom (0):** trim/expand `~`; require existing file and `.json5` extension (case-insensitive). Invalid => `ОШИБКА`, return to selection.
- **Copy:** destination `<reportsRoot>\<originalFileName>` (leaf only). If destination exists: warn replace and prompt `[R]eplace/[C]ancel` (default Cancel; empty/non-R => Cancel). **Replace:** overwrite and run copied destination path. **Cancel:** return to selection without running/copying/replacing.
- **Run:** `& $Exe @Prefix -m mko_get_mediascope_data run <targetPath>` with no `--export-folder`; preserve CLI output; success/failure by `$LASTEXITCODE -eq 0` (checked immediately) and no terminating exception.

#### R6 — UX/error model and PS 5.1/7+ compatibility (Q7=A, Q8=A)
- **Language/labels:** Russian user-facing messages, `УСПЕХ`/`ОШИБКА`, clear step labels (`init.ps1`: Шаг 1–4; `export.ps1`: Шаг 1–3). Preserve CLI command output as-is. Never claim success on failure.
- **Keep open:** top-level `try { ... } catch { ... } finally { Read-Host 'Нажмите Enter для выхода' }` (5.1/7+). Final pause must run on normal completion, failure, and restart-stop paths.
- **Exit/outcome:** check `$LASTEXITCODE` immediately after every native/module/CLI command; do not rely on `$?` for native executables. Mandatory step failure => stop with explicit step/error and overall failed, except controlled Step 1 restart-stop (overall failed/non-success, explicit restart message).
- **Encoding/paths:** save `.ps1` as UTF-8 with BOM (Cyrillic parse in PS 5.1); prefer `[Console]::OutputEncoding`/`$OutputEncoding` UTF-8 where helpful without altering child CLI output. Use `[System.IO.File]` UTF-8 APIs for config read/write (no BOM on write). Use `-LiteralPath`, quote paths, trim/expand `~`, normalize via `[System.IO.Path]::GetFullPath()`/`Resolve-Path -LiteralPath` as appropriate. Use .NET `[regex]` with `Count==1` enforcement; avoid PS 7-only features required for 5.1.
- **Idempotence:** rerunnable where specified; Script 2 mutates reports only on explicit confirmed copy/replace.

#### R7 — Residual uncertainties and escalation
- External/uncontrolled: winget/elevation/policy/PATH refresh, network, uv/multi-Python layouts, console rendering, unusual config formatting (safe abort). Handle via runtime detection + explicit messaging + correct stop semantics.
- **Escalate (separate problem/change request):** if strict policy forbids narrow `-c` probe and correctness cannot be achieved without guessing, or if required behavior needs module/CLI/build/dependency changes or intermediate `.py` files.

### 9.3 Architectural constraints (all downstream tasks)
- **Scope boundary:** only `examples/01-get-cli/init.ps1`, `examples/01-get-cli/export.ps1` (no module code, no existing audit phase files, no dependencies, no unrelated example refactors, no `.py` artifacts).
- **CLI-only module ops:** `-m mko_get_mediascope_data init` (no `--force`), `... list-reports` not required for numbering but module CLI remains available; Script 2 must use filesystem enumeration for deterministic numbering and `-m ... run <path>` for execution. No per-run `--export-folder`. No internal Python imports/bootstrap scripts.
- **Config safety:** targeted edit only, single match, abort-no-write on ambiguity/invalid/failure, no directory creation, store absolute normalized `export_folder`.
- **Env consistency:** one chosen target for installs/probes/CLI; report target; exit-code-driven outcomes.
- **UX:** Russian `УСПЕХ`/`ОШИБКА`, stepwise errors, preserve CLI output, keep window open in `finally`.

### 9.4 Tests and verification policy
- **Automated tests:** none in scope for PowerShell scripts (no existing harness; spec/planning scope does not add module tests). Do not add/modify `tests/` or production code for script automation.
- **Required verification:** manual, evidence-based validation per spec §13 and B5 criteria (see P04-B5-T1), performed in available Windows PowerShell 5.1 and/or 7+ environments where feasible. Verify logic and interaction outcomes (env selection consistency, git restart-stop semantics, init non-destructive behavior, JSON5 no-write-on-failure/single-match edit, deterministic FS listing/copy-replace-cancel/run target, exit-code status, keep-open). Avoid trivial checks detached from behavior.
- **Evidence to capture:** observed step messages/status labels, chosen interpreter, `$LASTEXITCODE`/CLI output excerpts, resulting `app_config.json5` `export_folder` value, reports directory state for copy/replace cases, and environment limitations (winget/elevation/network).

### 9.5 Implementation sequence and dependencies (DAG)
```text
B1 (CLOSED, R1-R7 contracts) ──┬──> P04-B2-T1 (shared scaffolding + primitives in both scripts)
                               │         └──> P04-B3-T1 (init.ps1 Steps 1-3)
                               │               └──> P04-B3-T2 (init.ps1 Step 4 export_folder)
                               │                     └──> P04-B4-T1 (export.ps1 Steps 1-2)
                               │                           └──> P04-B4-T2 (export.ps1 Step 3 run)
                               │                                 └──> P04-B5-T1 (manual §13 validation)
                               └──(contracts constrain all downstream tasks)
```
- **Parallelism:** none required/safe for script authoring; sequential reduces merge/conflict risk in two files. P04-B3 and P04-B4 both touch different primary scripts but share B2 contracts; keep B3 before B4 to ensure initialized settings context exists for validation flows (B4 must still handle missing/uninitialized state gracefully per R1/R5).

### 9.6 Refined implementation tasks (authoritative, supersede earlier B2–B5 YAML stubs)

```yaml
- id: P04-B2-T1
  title: Create script scaffolding and B1-approved shared primitives (init.ps1, export.ps1)
  priority: high
  depends_on: []
  blocked_by: [B1_GATE_R1_R7_CLOSED]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§2.1, §3.2, §13; B1 R1-R2,R6"
  description: >
    Create examples/01-get-cli/init.ps1 and examples/01-get-cli/export.ps1 with
    PS 5.1/7+ compatible scaffolding and reusable semantic helper units implementing
    B1 contracts: keep-open try/catch/finally with final Read-Host, Russian step/final
    status helpers (УСПЕХ/ОШИБКА), interpreter/CLI resolver (py -3 > python > uv run python),
    uniform -m invocation wrapper preserving $LASTEXITCODE and CLI output, and narrow
    read-only -c probe helper for APP_PATHS/import checks. Do not create .py files.
  goals:
    - establish consistent runtime/error/status foundation for both scripts
    - enforce single-target environment for installs/probes/CLI operations
    - preserve child CLI output and derive outcomes from exit codes
  files:
    - path: examples/01-get-cli/init.ps1
      semantic_units:
        - script: top-level flow skeleton (Шаг 1-4 placeholders wired to helpers), try/catch/finally keep-open
        - function: Write-StepStatus / Write-FinalStatus (Russian УСПЕХ/ОШИБКА, no false success)
        - function: Resolve-TargetPython (R2 precedence + usability checks)
        - function: Invoke-TargetCommand (native/module, immediate $LASTEXITCODE check, preserve output)
        - function: Invoke-AppPathsProbe (R1 -c, parse/validate 3 paths)
    - path: examples/01-get-cli/export.ps1
      semantic_units:
        - script: top-level flow skeleton (Шаг 1-3 placeholders), try/catch/finally keep-open
        - function: same B1-approved helper set (inline duplication acceptable to avoid extra files) or equivalent semantic units
  architectural_constraints:
    - no module/source/dependency/audit-phase changes; no intermediate .py files
    - module ops only via -m mko_get_mediascope_data with chosen target; no console-script-first logic
    - -c probes only for read-only path discovery/import checks (R1)
    - UTF-8 BOM script encoding; -LiteralPath/quoted paths; avoid PS 7-only required syntax
  acceptance_criteria:
    - both files exist and can be parsed in PS 5.1/7+ (syntax/encoding compatible)
    - helpers report chosen interpreter and never mask non-zero exit codes
    - window remains open via finally + Read-Host on success/failure paths
  tests_and_verification:
    - manual smoke: run scripts in PS 7+ (and PS 5.1 if available) to confirm parsing, Russian labels render acceptably, and finally pause executes
    - no automated PowerShell tests added

- id: P04-B3-T1
  title: Implement init.ps1 Steps 1-3 (Git, package installs, CLI init without --force)
  priority: high
  depends_on: [P04-B2-T1]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§2.2 Steps 1-3, §13 Script 1; B1 R2-R3"
  description: >
    Implement init.ps1 environment bootstrap: Git detect/install/update with conservative
    restart-stop semantics (R3), mediascope-api-lib and mko_get_mediascope_data install/upgrade
    in the consistent chosen environment (R2), and CLI init via -m ... init without --force.
    Report exact step/errors and stop on mandatory failure; restart-stop yields explicit
    message and overall failed/non-success for this run.
  goals:
    - correct Q2=A and Q3=A behavior with explicit Russian status per step
    - same-environment installs and module CLI invocation with post-install proof/importability
    - non-destructive settings initialization (no --force)
  files:
    - path: examples/01-get-cli/init.ps1
      semantic_units:
        - script step: Шаг 1 Git (detect/version, winget install/upgrade best-effort, in-session re-check, restart-stop rules)
        - script step: Шаг 2 install/upgrade mediascope-api-lib via chosen target -m pip
        - script step: Шаг 3 install/upgrade mko_get_mediascope_data from git+https://github.com/manicko/mko_get_mediascope_data.git, run -m ... init (no --force), verify import/probe readiness for Step 4
  architectural_constraints:
    - no --force; do not parse init output for paths; do not claim success on restart-stop
    - Git required before Step 3 git+ install; failures stop unless R3 allows continue (present git, failed best-effort update)
    - preserve CLI output; exit-code driven
  acceptance_criteria:
    - newly installed/unusable-in-session git => stop after Шаг 1 with restart instruction and overall ОШИБКА
    - installs and init run in reported chosen environment; init invoked without --force
    - any mandatory failure reports failing step/error and produces overall failed status (except controlled restart-stop semantics as specified)
  tests_and_verification:
    - manual: git-present path; if feasible simulate/observe missing-git branch on controlled host; verify pip/init exit-code handling and no --force in invocation
    - no automated PowerShell tests added

- id: P04-B3-T2
  title: Implement init.ps1 Step 4 (read/validate/update export_folder safely)
  priority: high
  depends_on: [P04-B3-T1]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§2.2 Step 4, §7, §13 Script 1; B1 R1,R4"
  description: >
    Implement export_folder display and optional change flow using R1-resolved
    app_config.json5 and R4 strict single-match JSON5-safe targeted edit. Validate
    existing directory (Test-Path -LiteralPath -PathType Container), accept abs/rel/~,
    store absolute normalized path, never create directories, and never modify file on
    decline/invalid/ambiguous/failure.
  goals:
    - show current export_folder and allow safe optional update with confirmation
    - preserve JSON5 comments/structure via targeted replacement only
    - fail closed on 0/>1 matches, IO/validation errors, or unsafe conditions
  files:
    - path: examples/01-get-cli/init.ps1
      semantic_units:
        - script step: Шаг 4 export_folder (R1 probe -> locate app_config, read current value, prompt, validate, edit/write/confirm or abort-no-write)
        - function: Read-ExportFolderValue (single-match extraction, read-only)
        - function: Update-ExportFolderValueSafely (R4 algorithm: validate, escape, single replace, UTF-8 no BOM write, confirm)
  architectural_constraints:
    - no ConvertFrom-Json/ConvertTo-Json rewrites; no directory creation; no file write on ambiguity/failure/invalid input
    - absolute normalized stored value; clear Russian errors and confirmation of saved value
  acceptance_criteria:
    - decline/no-change leaves config unchanged
    - invalid/non-existent path => explicit ОШИБКА and no config modification
    - valid existing directory => config updated safely, saved absolute value confirmed, unrelated JSON5 content preserved
  tests_and_verification:
    - manual: decline, invalid path, valid path, and (if safe) ambiguity/missing-key case against disposable config copy logic expectations; verify file unchanged on failures
    - no automated PowerShell tests added

- id: P04-B4-T1
  title: Implement export.ps1 report enumeration, selection, and custom copy flow
  priority: high
  depends_on: [P04-B2-T1, P04-B3-T2]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§2.3 Steps 1-2, §13 Script 2; B1 R1,R5"
  description: >
    Implement deterministic recursive *.json5 discovery under user reports root (R1),
    numbered selection with option 0 custom path, and safe copy-to-reports-root flow with
    [R]eplace/[C]ancel (default Cancel). Run target must be copied destination only on
    successful replace; cancel/invalid cases return to selection without running.
  goals:
    - unambiguous, deterministic report listing independent of list-reports CLI formatting
    - correct Q5=A/Q6=A branching and file-mutation boundaries
    - graceful handling when settings/reports/package context missing (R1 guidance + stop)
  files:
    - path: examples/01-get-cli/export.ps1
      semantic_units:
        - script step: Шаг 1 resolve env/paths and enumerate reports (R1 probe; FS recursive scan; deterministic relative-path sort; display 1..N + 0)
        - script step: Шаг 2 selection loop and custom path handling (validate 0..N, validate custom .json5 file, copy to reports root, replace/cancel prompt default Cancel)
        - function: Get-ReportCatalog (returns ordered items with relative/absolute paths)
        - function: Resolve-CustomReportSelection (validation + copy/replace/cancel state machine)
  architectural_constraints:
    - no parsing of list-reports output for numbering; no copy/replace without explicit choice
    - use -LiteralPath, trim/expand ~, case-insensitive extension checks
  acceptance_criteria:
    - deterministic relative-path listing and validated selection behavior
    - custom copy conflicts handled exactly as Q6=A; cancel returns to selection without run
    - missing/uninitialized context reported explicitly with actionable guidance and no crash-loop
  tests_and_verification:
    - manual: empty reports, populated nested reports, invalid selection, custom missing/non-json5, conflict replace, conflict cancel/default
    - no automated PowerShell tests added

- id: P04-B4-T2
  title: Implement export.ps1 CLI run execution and outcome reporting
  priority: high
  depends_on: [P04-B4-T1]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§2.3 Step 3, §13 Script 2; B1 R1,R2,R5,R6"
  description: >
    Execute selected or copied report via chosen target -m mko_get_mediascope_data run
    with correct path and no --export-folder override. Preserve CLI output, derive
    success/failure from exit code, and emit final explicit status while keeping window open.
  goals:
    - correct run target for numbered and replaced-custom flows
    - exit-code-driven, user-visible outcome consistent with CLI result
    - no boundary violations or config overrides
  files:
    - path: examples/01-get-cli/export.ps1
      semantic_units:
        - script step: Шаг 3 run report (invoke module run, preserve output, evaluate $LASTEXITCODE, set final status)
  architectural_constraints:
    - no --export-folder; same chosen interpreter/env; preserve child CLI output; no module code changes
  acceptance_criteria:
    - selected numbered report runs correct path
    - replaced custom report runs copied path in reports root
    - CLI failure => overall ОШИБКА with explicit context; success => overall УСПЕХ
  tests_and_verification:
    - manual: successful run path (environment permitting), forced/observed CLI failure path, verify copied-target run after replace
    - no automated PowerShell tests added

- id: P04-B5-T1
  title: Manual acceptance validation of init.ps1 and export.ps1 (spec §13)
  priority: high
  depends_on: [P04-B3-T2, P04-B4-T2]
  source_reference: .ai/problems/04_powershell_cli_scripts_spec.md
  source_section: "§12, §13; B1 R7"
  description: >
    Perform structured manual, evidence-based validation of both scripts against spec §13
    and B1 contracts; record pass/fail, limitations, and blockers without expanding scope
    or adding automated script tests.
  goals:
    - confirm Script 1 and Script 2 acceptance criteria and cross-cutting constraints
    - verify env consistency, JSON5 safety, restart-stop, deterministic selection/copy/run behavior
    - document external/environment limitations and escalate only true module-boundary blockers
  files:
    - path: examples/01-get-cli/init.ps1
    - path: examples/01-get-cli/export.ps1
  acceptance_criteria:
    - validation evidence recorded for exercised branches (including negative cases where feasible)
    - confirmed: no module changes, no .py artifacts, CLI-only module ops, Russian УСПЕХ/ОШИБКА, keep-open behavior
    - deviations classified as fix-now (within scope) or escalated blockers with rationale
  tests_and_verification:
    - manual validation matrix derived from spec §13 and §9.4; prefer PS 5.1 and 7+ coverage when available
    - no automated PowerShell tests added (intentional, per scope)
```

### 9.7 B1 handoff summary for implementors
- **Implement only** `init.ps1` and `export.ps1` in `examples/01-get-cli/` using semantic units/functions and step blocks above; keep helpers minimal and 5.1-compatible.
- **Do not** change module code, add `.py` files, parse brittle CLI text for discovery/numbering, use `--force`, or use `--export-folder` in Script 2.
- **Treat as authoritative:** R1–R7 contracts, §6.1 resolved decisions, and §9.6 task specs. If a contract cannot be satisfied without boundary violation, stop and escalate per §9.2/R7.
---

## B3 Audit Notes (Step 3.1, 2026-11-18) — evidence-based findings for Block B3 (init.ps1 Steps 1–4)

> Scope: audit only (no code/audit-phase changes). Evidence: B1/B2 plan contracts (R1–R7, §9.6 P04-B3-T1/T2), spec §2.2/§13, current scaffolds `examples/01-get-cli/init.ps1` and `export.ps1` (commit `780ec2d`, UTF-8 BOM), CLI `src/mko_get_mediascope_data/app.py` (`init` calls `init_project(force=force)` and prints `Settings initialized: {path}`; non-zero on exception/typer exit), `core/paths.py` (`APP_PATHS.user_settings_dir/app_config/reports`), `core/init_service.py` (non-force copies missing templates, migrates legacy creds, returns user settings dir), bundled `settings/app_config.json5` (`"export_folder": "output",` with comments), examples `01-get/run_report.ps1|.bat` (interpreter probing, `$LASTEXITCODE` checks, `finally` keep-open).

### B3.1 Scaffold baseline and control-flow implications

- **Entry/flow:** `init.ps1` sets `$ErrorActionPreference='Continue'`, best-effort UTF-8 console encoding, `$script:FinalSuccess=$false`, `$script:StopRequested` (unused). All logic inside outer `try { ... } catch { ... } finally { Write-FinalStatus; Read-Host }`. `return` inside `try` (used on prep failure and placeholders) **still executes `finally`** in PowerShell — correct for keep-open and final status on both normal completion and controlled stops (incl. restart-stop). B3 must preserve this pattern: on mandatory failure or restart-stop set `$script:FinalSuccess = $false`, emit explicit Russian step/overall context, and `return`/exit the step flow so `finally` runs; never set success after a failure/stop path.
- **Status helpers:** `Write-StepStatus -Status УСПЕХ|ОШИБКА|ИНФО` and `Write-FinalStatus` are adequate. Step labels must be explicit: `Шаг 1 (Git)`, `Шаг 2 (mediascope-api-lib)`, `Шаг 3 (пакет + init)`, `Шаг 4 (export_folder)` per R6/spec. Final message can remain generic but must not claim success on failure/restart-stop.
- **`Invoke-TargetCommand`:** executes `& $Exe @allArgs` (no capture), reads `$LASTEXITCODE` immediately, normalizes `$null` to `0`, returns `[int]`. This **preserves child CLI/native output as-is** and is the correct primitive for pip/module-CLI/git/winget calls where output must be shown (R6). B3 should use it for all `$Exe @Prefix -m pip ...`, `$Exe @Prefix -m mko_get_mediascope_data init`, and any post-install/proof module invocations where output preservation is desired. For detection commands where version text must be shown, direct invocation with immediate `$LASTEXITCODE` check is also fine (output streams naturally).
- **`Invoke-AppPathsProbe`:** read-only `-c` probe, captures output, returns `[pscustomobject]{UserSettingsDir,AppConfig,Reports}` or `$null` after printing `ОШИБКА` + guidance. It validates suffixes/containment (case-insensitive) — sufficient shape check. On success it does **not** print raw path lines (internal probe) which is acceptable; B3 Step 4 should display current `export_folder` value read from resolved config (and optional resolved note) per R4/spec, not raw probe output.
- **Interpreter resolution:** `Resolve-TargetPython` probes `py -3` > `python` > `uv run python` with `-c "import sys"` (output suppressed) — matches R2 precedence/usability check. It does not prove `pip` or package availability; B3 must treat non-zero exit codes from `-m pip`/module commands as step failures.

### B3.2 R2 — consistent pip/CLI invocations and post-install/probe timing

- **Single target, same `$Exe @Prefix`:** all installs and module operations must use the resolved target: `Invoke-TargetCommand -Exe $target.Exe -Prefix $target.Prefix -Arguments @('-m','pip','install','--upgrade','mediascope-api-lib')` (Step 2), `@('-m','pip','install','--upgrade','git+https://github.com/manicko/mko_get_mediascope_data.git')` (Step 3a), `@('-m','mko_get_mediascope_data','init')` with **no `--force`** (Step 3b). Do not invoke console script `mko-get-mediascope` as primary path (R2). Report chosen target already done in prep — keep visible context.
- **Step ordering/timing:** R1/R2/§9.2 specify `init.ps1` probe "after Step 3 package install and successful CLI `init` (package importable)..." and P04-B3-T1 requires "post-install import/probe readiness for Step 4" while P04-B3-T2 starts with "R1 probe -> locate app_config...". **Recommended flow:** complete Step 1–2–3a–3b successfully, then run `Invoke-AppPathsProbe -Target $target` once (serves as import/path proof and yields config path for Step 4). If probe fails, treat as Step 3/post-install readiness failure (or Step 4 prerequisite failure) with explicit `ОШИБКА`, guidance to initialize in selected environment, `$script:FinalSuccess=$false`, and stop. Do not guess paths (R1). Running probe before `init` is not required and may be less useful because settings/templates may not yet exist; after `init` they should exist (non-force copies missing templates).
- **CLI init semantics:** `app.init` performs non-destructive template copy/legacy migration and prints settings path; scripts must not parse output. Success/failure must be determined by exit code (`$LASTEXITCODE` immediately after call) — B3 must check and stop on non-zero. No `--force` flag anywhere.
- **Git required before Step 3a:** `pip install --upgrade git+https://...` depends on Git; Step 1 must ensure Git is usable per R3 before proceeding. If Step 1 ends in restart-stop, Steps 2–4 must not run.

### B3.3 R3 — Git detect/install/update/restart-stop (PS 5.1/7+, scaffold-aware)

- **Detection:** use `Get-Command git -ErrorAction SilentlyContinue` and/or run `git --version` capturing/checking `$LASTEXITCODE -eq 0` immediately. For user-visible status, let `git --version` output stream (preserve as-is) or capture+print; reporting "current version and continue" on present-initially path requires showing/communicating version — running the command visibly is simplest and compliant. Track whether Git was missing at start of Step 1 to distinguish "newly installed in this run".
- **Missing + winget:** if `Get-Command winget -ErrorAction SilentlyContinue` exists, attempt `winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements` via direct call/`Invoke-TargetCommand` pattern but note: `Invoke-TargetCommand` is suitable (preserves output, returns code). If winget absent or install non-zero => `Write-StepStatus` `ОШИБКА` with manual guidance (e.g. `https://git-scm.com/download/win`) and stop with `$script:FinalSuccess=$false; return`.
- **Post-install re-check / PATH:** after any install attempt, best-effort refresh of `$env:Path` in-session is allowed but must not be trusted: e.g. `$machine=[Environment]::GetEnvironmentVariable(''Path'',''Machine''); $user=[Environment]::GetEnvironmentVariable(''Path'',''User''); if($machine -or $user){ $env:Path = ($machine,$user -join '';'') }` (guard nulls). Then re-check `git --version` usability (`$LASTEXITCODE` immediately). If still not usable => explicit restart/PATH message, `ОШИБКА` for Step 1, overall failed, `return` (finally runs). If became usable but was installed this run => **conservative restart-stop**: explicit message that terminal/script must be restarted, `ОШИБКА` (overall failed/non-success for this run), do **not** proceed to Steps 2–4, `return`. This matches R3/spec and is safe given PATH/session uncertainty.
- **Present initially:** Step 1 may succeed. Best-effort update only when practical (e.g. winget available => try `winget upgrade --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements`); on failure/non-zero/elevation issues report current `git --version` (or state version known) and **continue** with Step 1 `УСПЕХ`/informational success, do not fail Step 1. Avoid claiming update success without evidence.
- **Exit-code discipline:** capture `$LASTEXITCODE` immediately after every native call (`git`, `winget`). Do not rely on `$?` for native executables (R6). When a command is invoked through `& $exe @args` in a helper, helper must capture immediately (already done).
- **Risks/gaps:** elevation/UAC prompts may hang or fail; corporate policies/network; winget source/package agreements flags vary by version but are common best-effort; `git` may be shadowed. All require explicit messaging and correct non-success stop semantics, not silent success. No in-session guarantee of PATH refresh.

### B3.4 R4 — JSON5 `export_folder` read/edit, prompt handling, and PS 5.1 compatibility

- **Path source:** resolve `app_config.json5` via R1 probe result (`$paths.AppConfig`). Do not derive from README/env heuristics. Ensure file exists/readable after init; if missing, `ОШИБКА` and no write.
- **Read/export display:** read UTF-8 with `[System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)`. Extract current raw value with the same strict single-match regex used for edit (read-only): e.g. `$pattern = ''(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)''`; `$matches = [regex]::Matches($text,$pattern)`. If count != 1, report ambiguity/missing-key as `ОШИБКА` for Step 4 (cannot safely show/update) and no modification. Display current raw value clearly in Russian; optional resolved interpretation note allowed if unambiguous but must not guess wrong base for relative values.
- **Prompt/decline (yes/no):** use `Read-Host` for "Хотите изменить export_folder? (y/n)" (or equivalent Russian prompt with accepted answers documented). Trim and compare case-insensitively (`$answer.Trim()`). Treat explicit no/empty-as-decline per UX contract as **no change**: do not modify file, emit `Write-StepStatus -Step ''Шаг 4 (export_folder)'' -Status ''УСПЕХ'' -Message ''export_folder оставлен без изменений.''` (neutral/success branch). If overall prior steps succeeded, script ends `УСПЕХ`. Only explicit affirmative should proceed to path input; ambiguous answers should re-prompt or be treated as decline only if clearly specified — safer to re-prompt on invalid yes/no input.
- **Path input/normalization:** prompt for path (abs/rel/`~`), trim; reject empty as invalid input => `ОШИБКА`, no change. Expand leading `~` manually: `if ($p -eq ''~'') { $p = $env:USERPROFILE } elseif ($p.StartsWith(''~/'') -or $p.StartsWith(''~\'')) { $p = Join-Path $env:USERPROFILE $p.Substring(2) }`. Compute absolute normalized candidate for validation/storage with `[System.IO.Path]::GetFullPath($expanded)` (resolves relative to current working directory) — this aligns with "validate resolved absolute form" and "store absolute, normalized path" (R4). **Validation:** `Test-Path -LiteralPath $candidate -PathType Container` must be true; on false/missing/not-directory => explicit `ОШИБКА`, **no file modification**. Do not create directories.
- **Safe targeted edit:** re-read or reuse validated text; enforce `$matches.Count -eq 1`. Escape replacement for JSON string content: backslash `\` -> `\\`, double quote `" -> `\"`, and escape control chars (`\r`,`\n`,`\t`, other chars < 0x20 as `\u00XX`) to avoid producing invalid JSON5/string. Replace only that single match preserving group 1 (indentation+key+colon+opening quote): `$prefix = $m.Groups[1].Value; $replacement = $prefix + $escaped + ''"''`; use index/length or `[regex]` single replacement. Do **not** use `ConvertFrom-Json`/`ConvertTo-Json` (loses comments/formatting, JSON5-unsafe). Write UTF-8 **without BOM** via `[System.IO.File]::WriteAllText($configPath, $newText, [System.Text.UTF8Encoding]::new($false))` (`.new($false)` is supported in PS 5+/Core; `New-Object System.Text.UTF8Encoding($false)` is equivalent fallback if needed). After write, re-read/extract to confirm saved value and report it (absolute normalized) to user.
- **Actual format compatibility:** bundled template has one `"export_folder"` string key with trailing comma and comments — matches strict pattern. Unusual formatting (non-string, duplicated key, split value) must result in safe abort/no-write (R4/R7).
- **Risks/gaps:** PowerShell string escaping in regex replacement callbacks/evaluator syntax differs between 5.1 and 7+; prefer simple `.Substring` splice by `$m.Index/$m.Length` for maximum compatibility. Relative input is validated/stored as absolute-from-CWD by contract (R4 selected "store absolute") — unambiguous and safe. Users expecting relative storage relative to settings dir will instead get absolute path; communicate saved value clearly.

### B3.5 Cross-cutting B3 implementation notes and residual risks

- **Overall success condition:** set `$script:FinalSuccess = $true` only after all required steps complete acceptably (including Step 4 decline/no-change or successful update). Any mandatory failure => `$false`. Restart-stop after Step 1 => `$false` with explicit restart instruction (controlled stop, reaches `finally`).
- **Preserve CLI output:** do not capture/suppress stdout/stderr of pip, `mko_get_mediascope_data init`, `git --version`/winget when user should see them; rely on exit codes for status (R6/spec). Probe output suppression is fine (internal).
- **No module-boundary violations:** no `.py` files, no internal imports except read-only `-c` probe, no `--force`, no config full-rewrite, no directory creation.
- **External/residual:** network/proxy for pip/git+; winget elevation/policy; uv context differences; PS console Cyrillic rendering on legacy hosts (script is UTF-8 BOM; .NET file IO explicit). These require explicit runtime errors/messages, not speculative fixes.
- **Downstream readiness:** B3 can be implemented by following §9.6 P04-B3-T1/T2 with above evidence; B4 depends on correct user settings/app_config/reports state and may reuse probe/helper patterns consistently.

---

## B3 Research/Decisions (Step 3.2, 2026-11-18) — Researcher recommendations for `init.ps1` (Step 1–4)

> Scope: recommendations only (no code changes). Evidence: §9 R1–R7, §9.6 P04-B3-T1/T2, B3 Audit Notes (§B3.1–§B3.5), spec §2.2/§13 Q1–Q8=A, current scaffold `examples/01-get-cli/init.ps1` (commit baseline behavior), `src/mko_get_mediascope_data/app.py`, `core/paths.py`, `core/init_service.py`, bundled `settings/app_config.json5`, `examples/01-get/run_report.ps1|.bat`. Confidence: HIGH for patterns grounded in current scaffold/spec; MEDIUM for external Git/winget/PATH behaviors (runtime-handled, not assumed).

### B3.R1 — Sequencing (authoritative; must hold)
1. **Prep (done in scaffold):** resolve target interpreter via `Resolve-TargetPython` (R2 precedence `py -3` > `python` > `uv run python`) using `& $Exe @Prefix -c "import sys"`. If none usable: report `ОШИБКА`, set `$script:FinalSuccess=$false`, `return` (finally keeps window open). Print chosen target in Russian (`ИНФО`) (already scaffolded).
2. **Шаг 1 (Git, R3):** detect/install/update with restart-stop rules (see B3.R2). **Only after Step 1 reaches a usable Git state for proceeding** may Steps 2–4 run (Step 3a requires `git+`).
3. **Шаг 2:** `& $target.Exe @target.Prefix -m pip install --upgrade mediascope-api-lib` via `Invoke-TargetCommand` (or direct call + immediate `$LASTEXITCODE`). On non-zero: `ОШИБКА`, set `$false`, `return`.
4. **Шаг 3a:** `& $target.Exe @target.Prefix -m pip install --upgrade git+https://github.com/manicko/mko_get_mediascope_data.git`. On non-zero: `ОШИБКА`, set `$false`, `return`.
5. **Шаг 3b:** `& $target.Exe @target.Prefix -m mko_get_mediascope_data init` (no `--force`). Do **not** parse CLI output (`init` prints path/emoji/Rich text; semantics are exit code). On non-zero: `ОШИБКА`, set `$false`, `return`.
6. **Post-init proof + paths (R1):** run `Invoke-AppPathsProbe -Target $target` (read-only `-c`). If `$null`: treat as Step 3/post-install/readiness failure (`ОШИБКА`), set `$false`, `return` (do **not** guess paths). On success keep `$paths = ...` for Шаг 4.
7. **Шаг 4 (R4):** edit `$paths.AppConfig` only after successful probe. Success/decline rules set final success as specified (see B3.R5).
8. **Finally:** always prints final status and waits for Enter (scaffold). Never claim success after any `return` on failure/restart-stop paths except the explicit Шаг 4 decline-success case when all prior required steps succeeded.

### B3.R2 — Git/winget invocation & PATH refresh (PS 5.1/7+) — concrete patterns
- **Detect (preferred):** `Get-Command git -ErrorAction SilentlyContinue`; confirm with `git --version` and check `$LASTEXITCODE` immediately after the call. If you capture output for display, do it without suppressing streams needed by users (e.g. let it flow, or capture then write) and still test `$LASTEXITCODE` right after. Do **not** use `$?` for native executables.
- **State tracking (required):** record `$gitWasMissingInitially = -not (detect usable git at start)`. This drives “installed this run => conservative restart-stop”.
- **Missing path:** if `winget` available (`Get-Command winget -ErrorAction SilentlyContinue`), run:
  - `winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements`
  - Prefer direct invocation with output preserved; if using a helper, ensure it captures `$LASTEXITCODE` immediately. Check exit code immediately. On non-zero or install attempt failed: `Write-StepStatus` `ОШИБКА` with manual guidance `https://git-scm.com/download/win`, set `$false`, `return`.
- **PATH refresh attempt (best-effort, not trusted):** after any install attempt, optionally refresh current session PATH from Machine/User (guard nulls), e.g.
  - `$m = [Environment]::GetEnvironmentVariable('Path','Machine'); $u = [Environment]::GetEnvironmentVariable('Path','User'); if ($m -or $u) { $env:Path = ($m,$u | Where-Object { $_ }) -join ';' }` (or equivalent safe join). Then re-check `git --version` + immediate `$LASTEXITCODE`.
- **Re-check outcomes (R3):**
  - If still not usable: `ОШИБКА` for Шаг 1 with explicit message that terminal/PATH may need restart or Git not on PATH; set `$false`, `return`.
  - If usable but `$gitWasMissingInitially -and (installed this run context)` => **conservative restart-stop**: `ОШИБКА`, message “Git установлен/стал доступен в этой сессии; закройте терминал и запустите скрипт заново…” (Russian), set `$false`, `return`. Do **not** proceed to Steps 2–4.
  - If usable and was present initially: allow Шаг 1 success path (after optional best-effort update below).
- **Best-effort update when present initially (MEDIUM reliability):** if winget available, try `winget upgrade --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements` (or equivalent). On non-zero/failure/elevation/policy issues: report current `git --version` (show output) and **continue** (do not fail Шаг 1). Do not claim update success without evidence.
- **Compatibility notes (runtime):** winget may prompt/elevate/hang; corporate policy can block; PATH refresh is unreliable across installers. Always surface explicit `ОШИБКА`/instructions and correct stop semantics (no silent success).

**Viable alternatives considered:** (A) Skip PATH rebuild and always restart-stop after any install — safest, but slower UX; chosen is “re-check then conservative restart-stop if newly usable this run” to match R3/spec intent while allowing rare in-session success to be handled explicitly (still conservative on “installed this run”). (B) Aggressive installer fallbacks (direct exe download) — out of minimal scope/spec; rejected.

### B3.R3 — Restart-stop control flow in the current scaffold
- Use the existing structure: all steps inside `try`, failures set `$script:FinalSuccess = $false`, emit step/final context messages via helpers, then `return`. `finally` runs and prints final status + `Read-Host`.
- For **Шаг 1 restart-stop** (newly installed/unusable-in-session after install attempt): after setting message and `$false`, `return` immediately (do not start Steps 2–4). Do not rely on `$script:StopRequested` (unused) except if you explicitly wire it; simplest is direct `return`.
- For **any mandatory step failure** (Steps 1–4): same pattern. Avoid throwing for expected failures when you can handle with messages + `return`; use `catch` only for unexpected exceptions (already present) then set `$false`.
- Keep step labels exactly as planned: `Шаг 1 (Git)`, `Шаг 2 (mediascope-api-lib)`, `Шаг 3 (пакет + init)`, `Шаг 4 (export_folder)`.
- Success path: only set `$script:FinalSuccess = $true` after Step 4 is completed in an allowed success branch (updated or explicitly declined with no changes) **and** all prior required steps succeeded. If Step 4 is declined (no write), that is still an overall success for the script’s contract when prior steps succeeded (per spec/Q4 behavior summarized in plan) — emit a clear `УСПЕХ` step message like “export_folder оставлен без изменений”.

### B3.R4 — Safe yes/no prompting (Шаг 4)
- Prompt example (Russian): `Хотите изменить export_folder? (y/n):` (accept `y`, `Y`, `д`, `Д` for yes if desired; be explicit in prompt text). Use `Read-Host`. Trim input.
- Normalize: `$ans = (Read-Host '...').Trim()`.
- If empty or clearly no (`n`,`no`,`н`,`нет`) => decline: no file change, step `УСПЕХ` “export_folder оставлен без изменений.”, do not proceed to path input. Keep `$script:FinalSuccess` true only if prior steps already made the flow successful so far (set at end of successful flow, or ensure you don’t flip it false).
- If yes (`y`,`yes`,`д`,`да`) => ask for path. 
- On ambiguous input: prefer re-prompt once with clear message; if still ambiguous, treat as decline is safer? Or treat as invalid and stop Step 4 with `ОШИБКА`? The safer, less surprising behavior is: re-prompt once; if invalid again, emit `ОШИБКА` for Шаг 4, set `$false`, `return` (no silent change). Recommend: “Повторите ввод: введите y/n.”
- Implementation note (PS 5.1/7): `Read-Host` is fine in the current flow (inside `try`). Avoid `Host.UI.PromptForChoice` unless necessary; `Read-Host` keeps behavior consistent with scaffold.

### B3.R5 — Tilde / relative / absolute normalization (Шаг 4)
- Read input: `$raw = (Read-Host 'Введите путь к каталогу (абс./отн./~):').Trim()`. Reject empty => `ОШИБКА`, no change.
- Expand leading `~`:
  - if `$raw -eq '~'` => `$expanded = $env:USERPROFILE`
  - elseif `$raw.StartsWith('~/') -or $raw.StartsWith('~\')` => `$expanded = Join-Path $env:USERPROFILE $raw.Substring(2)` (handle both separators)
  - else `$expanded = $raw`
- Compute absolute normalized for validation and storage: `$candidate = [System.IO.Path]::GetFullPath($expanded)`. This resolves relative paths against the **current working directory** of the script process (note this may differ from settings dir; the plan/R4 states “store absolute, normalized path” to avoid resolver-base ambiguity — this is the intended contract).
- Validate: `Test-Path -LiteralPath $candidate -PathType Container`. If false => `ОШИБКА` (“Каталог не существует или это не папка…”), no modification, set `$false`, `return`.
- Store the absolute normalized `$candidate` into `export_folder` (JSON string value). Report saved value clearly after write+confirm.

### B3.R6 — `Test-Path -LiteralPath` usage
- Always use `-LiteralPath` (and quote paths in commands) for user-provided or resolved paths to avoid wildcard interpretation.
- Examples:
  - `Test-Path -LiteralPath $candidate -PathType Container`
  - When reading/writing via .NET, pass the same string; `-LiteralPath` is for cmdlet form. For .NET APIs (`[System.IO.File]::ReadAllText`, `WriteAllText`) there is no wildcard expansion — safe as long as path string is correct.
- Do not create directories (`New-Item`) under any circumstance for export_folder.

### B3.R7 — .NET regex single-match + substring replace (PS 5.1/7+ compatible)
- Read text: `$text = [System.IO.File]::ReadAllText($configPath, [System.Text.Encoding]::UTF8)`.
- Pattern (strict, matches bundled form): `(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)"` (captures prefix group1 up to and including the opening quote of the value string; value group2 is the raw JSON string content inside quotes — but easier to replace by reconstructing: replace the whole match’s value portion by splicing prefix + new escaped value + `"`).
  - Recommended approach (robust, compatible): find first match `$m = [regex]::Match($text, '(?m)^(\s*"export_folder"\s*:\s*)"((?:\\.|[^"\\])*)"')`. If not `$m.Success` or `[regex]::Matches($text,'(?m)^(\s*"export_folder"\s*:\s*)"')`.Count -ne 1 => abort `ОШИБКА`, no write.
  - Build new text by substring splice: `$newText = $text.Substring(0,$m.Groups[1].Length) + '"' + $escapedValue + '"' + $text.Substring($m.Groups[1].Length + 1 + $m.Groups[2].Length + 0)` is error-prone; easier: compute the start of value = `$m.Groups[2].Index` (index of first char inside quotes) and end = `$m.Groups[2].Index + $m.Groups[2].Length`, or replace from `$m.Index + $m.Groups[1].Value.Length` (which is the opening quote char position?) carefully. Safer: `$prefix = $m.Groups[1].Value` (includes the opening `"` of value? pattern shows group1 is `(\s*"export_folder"\s*:\s*)"` so group1 ends at the quote before value — yes: in the regex the value quote is outside group1: `)"((?:...))"`. So `$m.Groups[1].Value` is everything before the opening `"` of the value string; then replacement is `$m.Groups[1].Value + '"' + $escapedValue + '"'`.
    - Verify indices mentally: the full match starts at `$m.Index`, group1 is `$m.Groups[1]` (Value/Index/Length). So reconstructed = text before match start + group1.Value + `"` + escaped + `"` + text after the closing quote of the original value? Original value match is group2 inside quotes: the quote before group2 is char at `$m.Groups[1].Index + $m.Groups[1].Length` (i.e. the opening quote). The closing quote is after group2: `$m.Groups[2].Index + $m.Groups[2].Length`. So new text = `$text.Substring(0, $m.Groups[1].Index + $m.Groups[1].Length) + '"' + $escapedValue + '"' + $text.Substring($m.Groups[2].Index + $m.Groups[2].Length)`. This is PS 5.1/7 compatible and avoids evaluator/callback differences.
- Enforce exactly one match: if `([regex]::Matches($text,'(?m)^(\s*"export_folder"\s*:\s*)"')).Count -ne 1` => abort with explicit `ОШИБКА` (“неоднозначное или отсутствующее ключевое поле export_folder”), no write.

**Viable alternative:** use `[regex]::Replace($text, $pattern, { ... }, 1)` with a scriptblock evaluator — works in PS 5.1 in many forms but syntax/capture behavior can be fiddly; substring splice is more explicit and portable. Prefer splice.

### B3.R8 — JSON string escaping for the replacement value
- Input is the absolute normalized path (Windows paths contain backslashes). Escape for JSON string:
  - Replace `\` with `\\`
  - Replace `"` with `\"`
  - Replace control characters: `\r` -> `\\r`, `\n` -> `\\n`, `\t` -> `\\t`; for any other char with code < 0x20 produce `\\u00XX` (optional but safer). Paths normally won’t contain these, but validate/escape defensively.
- Implementation sketch (PS):
  - `$escaped = $candidate -replace '\\','\\' -replace '"','\"'` (note PowerShell replace uses regex by default; be careful). Better: use .NET: `[System.Text.RegularExpressions.Regex]::Replace($candidate, '[\\"]', { param($m) if ($m.Value -eq '\') { '\\' } else { '\"' } })` not directly; easier do sequential: `$escaped = $candidate.Replace('\','\\').Replace('"','\"').Replace("`r",'\r').Replace("`n",'\n').Replace("`t",'\t')`, then escape any remaining control chars < space by iterating if needed (rare). For Windows absolute paths this is sufficient.
- After escaping, insert into `"<escapedValue>"`.

### B3.R9 — UTF-8 no-BOM write in PS 5.1/7+
- Preferred: `[System.IO.File]::WriteAllText($configPath, $newText, [System.Text.UTF8Encoding]::new($false))` (works in PS 5.1 with .NET Framework). Fallback if constructor style issues: `New-Object System.Text.UTF8Encoding($false)`.
- Avoid `Set-Content -Encoding UTF8` on PS 5.1 (adds BOM) and avoid `Out-File` defaults. Do not use `ConvertTo-Json`/rewrites.

### B3.R10 — Residual uncertainties (treat as runtime handling, not speculative code)
- **winget/elevation/policy/network:** may prompt, hang, fail, or require reboot; always show command/output context, check `$LASTEXITCODE`, and give explicit `ОШИБКА` + manual steps. No hidden retries beyond what’s reasonable; do not claim success.
- **PATH refresh after install:** best-effort only; do not trust until `git --version` succeeds in the same session. If newly usable this run => conservative restart-stop (as above).
- **uv/multi-Python environments:** precedence chosen by R2; if user has unusual layouts, report chosen target and failures clearly. Post-install probe (`Invoke-AppPathsProbe`) is the gate for readiness.
- **Console Cyrillic rendering:** script saved as UTF-8 BOM (scaffold files show BOM) — preserve BOM on edit. Messages are Russian; if some hosts render badly, that’s environmental; do not add complex encoding toggles beyond scaffold’s best-effort `[Console]::OutputEncoding = [System.Text.Encoding]::UTF8`.
- **Unusual `app_config.json5` formatting:** if pattern does not match uniquely => safe abort, no write; instruct user to fix manually (do not attempt partial heuristics).
- **CWD-relative normalization:** absolute storage uses `[System.IO.Path]::GetFullPath($expanded)` relative to current CWD. Recommend that users run the script from a stable location (or document behavior); this is the chosen unambiguous contract (R4). If stricter behavior is needed later, it requires spec/contract change (escalate).

### B3.R11 — Minimal implementation checklist (for P04-B3-T1/T2)
- P04-B3-T1: implement Шаг 1 (R3 full flow + restart-stop), Шаг 2, Шаг 3a, Шаг 3b, then run R1 probe and stop on failure (with `ОШИБКА`, `$false`, `return`). No `--force`. Use `Invoke-TargetCommand`/immediate `$LASTEXITCODE`. Preserve child output.
- P04-B3-T2: Шаг 4 reads `$paths.AppConfig`, shows current raw `export_folder` (single-match extract), yes/no prompt (decline => step success/no write, overall success if prior OK), path input+tilde+absolute normalize+`Test-Path -LiteralPath Container`, abort on invalid/no-write, JSON-escape+single-match splice+UTF-8 no-BOM write via .NET, re-read/confirm. Set `$script:FinalSuccess=$true` only at end of fully successful flow.

### Alternatives summary (kept minimal, no speculative redesign)
- Git install: winget first, else manual guidance (spec Q2=A). No direct-installer download fallback in scope.
- JSON edit: regex+substring splice over parse/rewrite (safer for comments/format). No JSON5 parser dependency.
- Target selection: interpreter-first `-m` invocations (R2). No console-script-first.
- Path storage: absolute normalized (R4) over relative-to-settings to avoid base ambiguity (documented trade-off). 

These recommendations are consistent with R1–R7 and do not require module/CLI changes or intermediate `.py` files.

---

## B4 Research/Decisions (Step 3.2, 2026-11-18) — Researcher recommendations for `export.ps1` (B4-T1/T2)

> **Scope:** recommendations only (no code or audit-phase changes). Evidence: plan §9 R1–R7 (authoritative), §9.6 P04-B4-T1/T2, B1/B3/B4 Audit Notes, spec §2.3/§13/Q5=A/Q6=A, scaffold `examples/01-get-cli/export.ps1` (commit `780ec2d`, UTF-8 BOM), B3 reference `examples/01-get-cli/init.ps1` (`~` expansion/GetFullPath, prompt discipline, `-LiteralPath`/immediate `$LASTEXITCODE`), CLI/runtime `app.py`/`core/utils.py`/`core/paths.py`. B1 gate CLOSED. Confidence: **HIGH** for PS/.NET path, sort-key, control-flow, and quoting patterns grounded in scaffold/spec/contracts; **MEDIUM** for host-specific Cyrillic rendering and non-interactive `Read-Host` edge cases (runtime-handled, not assumed).

### B4.R0 Executive selection (best path)
- **Scaffold reuse:** keep `try/catch/finally`, `$script:FinalSuccess`, `Write-StepStatus`/`Write-FinalStatus`, `Resolve-TargetPython`, `Invoke-TargetCommand`, `Invoke-AppPathsProbe`, and keep-open `Read-Host` in `finally`. Replace placeholder flow with R1 probe -> readiness checks -> deterministic catalog -> interactive selection loop (T1) -> CLI run and outcome mapping (T2). No `.ps1` helper file extraction required; inline helpers/functions in `export.ps1` are sufficient and consistent with B3.
- **Contracts preserved:** R1 runtime-truth paths via probe (no guessing), R2 same target env and `-m mko_get_mediascope_data run` (no console-script-first, no `--export-folder`), R5 deterministic FS enumeration/sort/selection/copy semantics, R6 Russian status + preserved CLI output + exit-code outcomes + keep-open. No module/CLI changes.

### B4.R1 Deterministic stable sort without `Sort-Object -Stable` (PS 5.1/7+)

**Requirement:** sort by relative path from reports root, case-insensitive ordinal, stable (R5, B4 Audit B4.3). `Sort-Object -Stable` is unavailable in Windows PowerShell 5.1.

**Viable alternatives:**
| Option | How | Pros | Cons / risks | Verdict |
|---|---|---|---|---|
| **A. Precomputed invariant sort key + `Sort-Object` + index tiebreaker** | For each item store `Index` (enumeration order, 0-based or 1-based), `RelativePath` (display), `RelSortKey` (e.g. `$RelativePath.ToLowerInvariant()` or separator-normalized lowercase form), then `$items = @($items | Sort-Object -Property @{Expression={$_.RelSortKey}; Ascending=$true}, @{Expression={$_.Index}; Ascending=$true})` | Idiomatic PS, readable, no custom comparer plumbing, deterministic and stable, 5.1/7+ compatible | `ToLowerInvariant()` is not identical to pure ordinal-ignore-case for rare Unicode cases, but deterministic and culture-invariant; adequate for file paths | **Selected (recommended)** |
| **B. .NET generic list `Sort(Comparison[T])` with `OrdinalIgnoreCase` + index** | Build `[System.Collections.Generic.List[object]]` and call `$list.Sort({ param($a,$b) $c=[StringComparer]::OrdinalIgnoreCase.Compare($a.RelativePath,$b.RelativePath); if($c -ne 0){$c}else{$a.Index.CompareTo($b.Index)} })` | True ordinal-ignore-case semantics, efficient, fully stable by tiebreaker | Slightly less idiomatic in PS 5.1 scripts; scriptblock-as-delegate works but is less obvious to maintainers | **Viable alternative** (use if strict ordinal-ignore-case is preferred) |
| **C. `Sort-Object` by `RelativePath` only** | `$items | Sort-Object RelativePath` | Simple | Potential culture sensitivity in some hosts and not explicitly stable on 5.1; weaker contract compliance | **Rejected** |

**Selected implementation path:** Option A for maintainability/readability in a user-facing example script. Capture original enumeration index before sorting and keep it as the final tiebreaker. Force array materialization with `@(...)` before/after sort. If strict `OrdinalIgnoreCase` is later desired, swap key comparison to Option B without changing external behavior.

### B4.R2 Relative path computation and separator normalization for display/sort

**Goal:** compute a stable, unambiguous relative path from normalized reports root; use it as primary display label and sort basis (R5).

**Viable alternatives:**
| Option | How | Pros | Cons / risks | Verdict |
|---|---|---|---|---|
| **A. Normalized prefix strip on full paths (selected)** | `$reportsRootFull = [System.IO.Path]::GetFullPath($paths.Reports)`; `$rootPrefix = $reportsRootFull.TrimEnd('\','/') + [System.IO.Path]::DirectorySeparatorChar`; for each file `$full = [System.IO.Path]::GetFullPath($_.FullName)`; if `$full.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)` then `$rel = $full.Substring($rootPrefix.Length)` else fallback `$_.Name` (or last segment) with explicit note only if unexpected | Host-stable, no `Push-Location`/`Resolve-Path -Relative` formatting differences, uses .NET ordinal-ignore-case boundary check, works for nested paths, 5.1/7+ compatible | Must ensure prefix ends with separator to avoid partial-folder matches; fallback should be rare and non-fatal | **Selected** |
| **B. `Push-Location` + `Resolve-Path -LiteralPath -Relative`** | Change location to reports root and resolve relative | Built-in cmdlet behavior | Relative formatting/prefix (`.\\`) differs across PS versions/contexts, harder to normalize, side effects require careful try/finally | **Rejected** as primary |
| **C. `[System.IO.Path]::GetRelativePath`** | .NET Core API in newer runtimes | Clean API | Not reliably available in Windows PowerShell 5.1/.NET Framework context | **Rejected** for 5.1 compatibility |

**Separator normalization rules (selected):**
- **Computation boundary:** use `GetFullPath` results (Windows-native `\`) for `$reportsRootFull`, `$rootPrefix`, and `$full` before slicing so the sliced relative path inherits consistent separators.
- **Display:** show the sliced relative path (primary label) as-is from the normalized full-path slice (typically `\` on Windows). This is deterministic and matches local expectations.
- **Sort key:** derive `RelSortKey` from the display relative path with a canonical transform, e.g. replace `/` with `\` defensively and `ToLowerInvariant()` for the key only; keep original `RelativePath` for display. (On Windows `GetFullPath` already normalizes, but defensive normalization is cheap and safe.)
- **Comparisons (equality/containment):** always `OrdinalIgnoreCase` for path prefix checks and extension checks; never culture-sensitive string compare for control flow.

### B4.R3 Selection loop control flow and state model

**Selected state model (simple, explicit, PS 5.1-safe):**
- **Fatal/stop states (exit loop/script):** preparation failure, R1 probe failure, reports root missing/inaccessible, enumeration exception, or any unrecoverable prerequisite error => emit step `ОШИБКА` with actionable context, set `$script:FinalSuccess=$false`, `return` (outer `try` ensures `finally` runs keep-open + final status).
- **Interactive continue states (no run, stay in loop):** empty/invalid/out-of-range selection, custom path invalid (empty/missing/not-leaf/not `.json5`), copy failure, conflict cancelled/default-cancel, or any branch where R5 requires "return to selection without running/copying/replacing" => emit appropriate `ОШИБКА`/`ИНФО` message, clear transient state, `continue` loop.
- **Ready-to-run state:** valid numbered selection sets `$runTarget = $selectedItem.FullPath`; valid custom flow that results in a usable copied/in-place destination sets `$runTarget = $destFull` only after copy/guard success => `break` selection loop and proceed to Шаг 3. Script-level success is set **only** after Шаг 3 exit code is `0`.

**Control-flow sketch (semantic):**
1. Resolve target; probe paths (R1). Validate reports container (`Test-Path -LiteralPath $reportsRootFull -PathType Container`).
2. Enumerate/build catalog once at start of Шаг 1; if empty, print clear `ИНФО` ("в папке reports не найдено файлов *.json5") but continue to menu/loop with option 0 available (do not fatal-fail).
3. `while ($runTarget -eq $null)` (or `while ($true)` with explicit `break`): display catalog entries `1..N` with `RelativePath`, display `0) ...`; read/trim input; parse/range-check; dispatch to numbered/custom handling. Use helper `Resolve-CustomReportSelection`-style logic to return a small state object/result (e.g. `RunTarget`, `Action='Continue'|'Run'|'FatalStop'`) or set variables in-scope — either is fine if unambiguous; inline loop with early `continue` is simplest for 5.1.
4. Run once (`Шаг 3`), map exit code, `return`.

**Viable alternative:** rebuild catalog on every loop iteration to reflect external changes after copy/cancel; this is more dynamic but slightly noisier and unnecessary for scope. **Selected:** build once after readiness checks (deterministic within run), because copies write into reports root and a replace/copy may add/overwrite leafs — if dynamic refresh is cheap and clear it is acceptable, but not required by spec. Prefer single build for simplicity unless implementation finds a clear need; document behavior implicitly through deterministic sort at build time.

### B4.R4 Custom path normalization (B3-style) and CI `.json5` validation

**Normalization (selected, aligned with B3):**
- `$raw = Read-Host ...`; if `$null -eq $raw { $raw='' }`; `$trimmed = ([string]$raw).Trim()`. Empty => `ОШИБКА` + `continue`.
- Tilde expansion (B3 pattern): `$profile = [string]$env:USERPROFILE`; if `$trimmed -eq '~'` and profile non-empty => `$expanded = $profile`; elseif `$trimmed.StartsWith('~/') -or $trimmed.StartsWith('~\')` and profile non-empty => `$expanded = [System.IO.Path]::Combine($profile, $trimmed.Substring(2))` (or Join-Path on remainder) else `$expanded = $trimmed`. If tilde used but profile unavailable, treat as normalization failure (`ОШИБКА`, continue).
- `$srcFull = [System.IO.Path]::GetFullPath($expanded)` for validation/copy comparison.
- Validate with `-LiteralPath`: `Test-Path -LiteralPath $srcFull -PathType Leaf` must be true.
- **CI extension check:** `[System.IO.Path]::GetExtension($srcFull).Equals('.json5', [System.StringComparison]::OrdinalIgnoreCase)` must be true. Do not use `-like '*.json5'` without care for edge semantics; `GetExtension` + `OrdinalIgnoreCase` is explicit and correct.

**Viability:** B3 already validates existing directory paths with similar normalization; adapting to leaf+extension is straightforward and consistent.

### B4.R5 Conflict prompt parsing, copy semantics, and same-source==destination handling

**Destination rule (R5/Q6):** `$leaf = [System.IO.Path]::GetFileName($srcFull)`; `$destFull = [System.IO.Path]::GetFullPath((Join-Path -Path $reportsRootFull -ChildPath $leaf))` (reports root only).

**Same-source==destination guard (selected, recommended):**
- Compute normalized full paths and compare `$srcFull.Equals($destFull, [System.StringComparison]::OrdinalIgnoreCase)` (Windows-safe CI comparison).
- **If equal:** skip copy and skip replace/cancel prompt (avoids confusing self-replace and risky `Copy-Item` on same file). Emit `ИНФО`/`УСПЕХ`-neutral status (e.g. "Выбран файл уже находится в папке reports: ... Будет выполнен запуск этого файла.") and set `$runTarget = $destFull` to proceed to run. This is compatible with "run selected/copied report" intent and is safer than strict conflict prompting on identity.
- **If different and destination exists:** warn and prompt `[R]eplace/[C]ancel` (default Cancel). Parse `$answer = ([string](Read-Host ...)).Trim()`; treat `$null` as empty. **Replace only when explicitly** `$answer.Length -gt 0 -and ($answer.Substring(0,1) -eq 'R' -or $answer.Substring(0,1) -eq 'r')` is acceptable but exact-char match is clearer: `$isReplace = ($answer -eq 'R' -or $answer -eq 'r')` (selected, strict). Any other value including empty => Cancel. On Cancel: `ИНФО` "Операция отменена..." and `continue` selection loop (no copy/run).
- **If different and destination does not exist:** no prompt needed; proceed to copy.

**Copy/replace execution (selected):**
- Perform copy in `try/catch` with `Copy-Item -LiteralPath $srcFull -Destination $destFull -Force -ErrorAction Stop` (use `-Force` for replace case; harmless on new file). On success set `$runTarget = $destFull`. On failure emit `ОШИБКА` with exception message, do not run, `continue` loop.
- **Run target invariant:** numbered selection => catalog `FullPath`; custom flow => `$destFull` (copied destination or same-path-in-place). Never run `$srcFull` when a copy-to-reports-root path was the intended target.

**Viable alternatives:** always prompt on existing destination even if same path — rejected as confusing/error-prone. Allow `Replace` word as explicit affirmative — acceptable but unnecessary; strict single-letter `R/r` best matches spec text `[R]eplace` and avoids ambiguity.

### B4.R6 Run invocation, exit-code evaluation, and outcome mapping (T2)

**Selected invocation path:**
- `$runArgs = @('-m','mko_get_mediascope_data','run', $runTarget)` (exactly 4 args after prefix; no `--export-folder`, no extra flags).
- `$code = Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments $runArgs` (or `@($runArgs)`; both safe if helper expects string array). Child CLI output preserved by helper design.

**Outcome mapping:**
- Immediately treat returned `$code` as authoritative: `if ($code -eq 0) { Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'УСПЕХ' -Message ("Отчёт успешно выполнен: {0}" -f $runTarget); $script:FinalSuccess=$true; return } else { Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'ОШИБКА' -Message ("Запуск отчёта завершился с ошибкой (код {0}): {1}" -f $code, $runTarget); $script:FinalSuccess=$false; return }`.
- Do not infer success from CLI text. `finally` emits final summary consistent with `$script:FinalSuccess`.

**Argument/quoting/array practices (PS 5.1/7+):**
- Pass arguments as array elements to the call operator: `& $Exe @allArgs` (already used). Each element, including a path with spaces/Cyrillic, is passed as a distinct argument; do not manually quote inside the string and do not join into one command line.
- Build with `@()`: `$allArgs = @(); if ($Prefix -and $Prefix.Count -gt 0) { $allArgs += @($Prefix) }; $allArgs += @($Arguments)` or preserve existing helper logic.
- Use `-LiteralPath` for all filesystem cmdlets: `Get-ChildItem -LiteralPath`, `Test-Path -LiteralPath`, `Copy-Item -LiteralPath`/`-Destination` as literal. Avoid wildcard expansion.
- Force arrays for counts/single-item safety: `@(Get-ChildItem ...)`, `@($output ...)`, catalog arrays; check `.Count` on forced arrays.

### B4.R7 Empty catalog, readiness, and UX/error handling details

- **Reports root readiness:** after successful R1 probe, require `Test-Path -LiteralPath $reportsRootFull -PathType Container`. If missing => explicit fatal `ОШИБКА` with guidance to run `init.ps1` / `init` (do not create directory; init owns layout) and stop.
- **Empty catalog behavior (selected):** if `@($catalog).Count -eq 0`, print `ИНФО` (not fatal) that no `*.json5` reports were found in reports folder, but still render option `0` and enter selection loop so custom-path bootstrap remains possible (R5 "allow option 0 where sensible").
- **Enumeration failure:** wrap catalog build in `try/catch`, on exception emit `ОШИБКА` with message and stop (`$false`, `return`).
- **Invalid selection:** empty/non-integer/out-of-range => `ОШИБКА` "Некорректный выбор. Введите число от 0 до N." and `continue`.
- **Custom validation failures/cancel/copy failures:** `ОШИБКА` or clear cancellation `ИНФО` as appropriate, no run, `continue`.
- **Catalog display after custom copy:** if catalog is built once, a newly copied file (copied to reports root leaf) will not appear in the numbered list until next script run; this is acceptable and deterministic. Not a blocker.

### B4.R8 Risks, residual uncertainties, and mitigations (B4-specific)

| Risk | Impact | Selected mitigation |
|---|---|---|
| Relative path prefix strip mismatch (unexpected path form) | Wrong labels/sort | Normalize with `GetFullPath`, CI `StartsWith` on separator-terminated root prefix, fallback to leaf name; keep deterministic behavior |
| Same leaf name from different sources collides in reports root | Potential overwrite if user confirms Replace | This is intended by R5/Q6; require explicit `R` only, default Cancel, clear warning showing destination |
| Same source as destination in custom flow | Confusing prompt / risky copy | CI normalized full-path equality guard: skip prompt/copy, run destination in place |
| `Read-Host` returns `$null` in non-interactive host | Validation/parse errors | Guard `$null -> ''`, trim, treat as invalid/empty and continue (or fail safely) |
| Cyrillic rendering in legacy console | UX readability | Script UTF-8 BOM + best-effort console UTF-8 already in scaffold; runtime limitation, do not redesign |
| Large/complex report trees | Minor perf, sort correctness | Deterministic sort key + index tiebreaker; performance is not a concern for expected catalog sizes |
| Reports dir missing after probe success | Cannot enumerate safely | Explicit fatal `ОШИБКА` with init guidance, no guessing/creation |

### B4.R9 Mapping to plan semantic units (P04-B4-T1/T2)
- **P04-B4-T1 / `examples/01-get-cli/export.ps1`:** implement Шаг 1 (R1 probe + readiness + recursive enumeration + `Get-ReportCatalog` producing `Number/RelativePath/FullPath/RelSortKey/Index`, deterministic sort, display 1..N + 0, empty-catalog-allow-0) and Шаг 2 (selection loop, `Resolve-CustomReportSelection` logic: trim/`~`/`GetFullPath`, leaf+`.json5` CI validation, destination leaf in reports root, same-path guard, conflict prompt default Cancel explicit `R`, `Copy-Item -LiteralPath -Force -ErrorAction Stop`, return-to-selection on invalid/cancel/failure). Keep `-LiteralPath`, `@(...)`, and explicit statuses.
- **P04-B4-T2 / Шаг 3:** invoke `$target.Exe @($target.Prefix) -m mko_get_mediascope_data run <runTarget>` with no `--export-folder`, preserve output, evaluate exit code immediately, emit step status with target/code context, set `$script:FinalSuccess` accordingly, `return` to `finally` for final status and keep-open.

These decisions are consistent with R1–R7 and the B4 Audit Notes, require no module/CLI/audit-phase changes, and are scoped to `examples/01-get-cli/export.ps1` only.

---

## B4 Audit Notes (Step 3.1 Auditor, 2026-11-18) — Block B4 `export.ps1` (report selection, custom copy, and export)

> **Scope:** evidence-based audit only (no code or existing audit phase files changed). Sources: `.ai/plans/04-powershell-cli-scripts.md` (§9 R1–R7, §9.6 P04-B4-T1/T2, B1/B2/B3 context), `.ai/problems/04_powershell_cli_scripts_spec.md` (§2.3, §13 Script 2, Q1–Q8=A, Q5=A, Q6=A), current scaffold `examples/01-get-cli/export.ps1` (commit `780ec2d`, 185 lines, UTF-8 with BOM `239,187,191`), B3 reference `examples/01-get-cli/init.ps1` (commit `e466daa`, 518 lines, UTF-8 with BOM), CLI/runtime evidence `src/mko_get_mediascope_data/app.py`, `core/utils.py::list_files_in_directory`, `core/paths.py::AppPaths.reports`. B1 gate is CLOSED; R1–R7 are authoritative.

### B4.0 Evidence baseline (current implementation & constraints)

- **Scaffold control flow (`export.ps1`):** `[CmdletBinding()] param()`, `$ErrorActionPreference='Continue'`, outer `try/catch/finally` with `$script:FinalSuccess=$false`, best-effort console UTF-8 (`[Console]::OutputEncoding`/`$OutputEncoding` in try/catch), helpers `Write-StepStatus` (УСПЕХ/ОШИБКА/ИНФО), `Write-FinalStatus`, `Resolve-TargetPython` (R2: `py -3` > `python` > `uv run python`, `-c "import sys"` usability probe), `Invoke-TargetCommand` (builds `$allArgs = Prefix + Arguments`, invokes `& $Exe @allArgs`, captures `$LASTEXITCODE` immediately, normalizes `$null` to `0`, preserves child output), `Invoke-AppPathsProbe` (R1: `-c` prints `user_settings_dir/app_config/reports`, validates 3 non-empty lines, suffixes `\app_config.json5|/app_config.json5` and `\reports|/reports`, containment of app_config under user settings dir case-insensitively; returns `[pscustomobject]{UserSettingsDir,AppConfig,Reports}` or `$null` after printing `ОШИБКА` + guidance). Main flow: resolve target; on failure => Подготовка ОШИБКА, set false, `return`; print chosen interpreter (ИНФО); then **placeholder** `Шаг 1 (список/выбор)` `ОШИБКА` "Шаг не реализован в текущем этапе (ожидается в задаче B4). Выполнение остановлено." and `return`; Шаг 2/3 placeholders unreachable. `finally` always writes final status and `Read-Host 'Нажмите Enter для выхода'` (best-effort). `return` inside `try` still runs `finally` — keep-open contract satisfied.
- **B3 pattern reference (`init.ps1`):** demonstrates post-probe readiness flow, `~` expansion + `[System.IO.Path]::GetFullPath()` normalization (`Get-NormalizedExportFolderCandidate`, container validation), `Read-Host` + trim + safe re-prompt/decline handling (`Read-YesNoDecision`), UTF-8 .NET file IO, explicit `$LASTEXITCODE` checks, and Russian step labels (Шаг 1–4). B4 must follow consistent R1/R2/R5/R6 style/contracts but implement different interaction (selection loop, `[R]eplace/[C]ancel` conflict prompt with default Cancel).
- **CLI/listing/run evidence (`app.py`, `utils.py`, `paths.py`):** `list_reports()` uses `app_service.app_paths.reports` and `list_files_in_directory(reports_dir, extensions=("json5",), include_subfolders=True)` then prints sorted `• {f}` (human-formatted `Path` string) — **not machine-stable/parseable for numbering**. `utils.list_files_in_directory` does `Path(path).glob(f"{subfolder_pattern}*.{ext.strip('.')}")` recursively when enabled and returns `list[Path]` (no explicit sort). `run` takes existing file path (`typer.Argument(exists=True, dir_okay=False)`), resolves execution in `run_report_async` (`FileNotFoundError` if missing), accepts optional `--verbosity/-v` and `--export-folder` per-run override (must **not** be used by scripts), and reports success/failure by exit/status (non-zero on exception). `AppPaths.reports` = `<user_settings_dir>/reports`. `PathResolver.resolve()` exists in `core/paths.py` but scripts must not import module code; use .NET/PowerShell equivalents.
- **Spec/R1–R7 constraints (binding):** CLI-only module ops (`-m mko_get_mediascope_data run <targetPath>` in same target env, no `--export-folder`), no `.py` files, no guessing paths, deterministic FS enumeration for numbering (not `list-reports` parsing), recursive `*.json5` under R1 reports root, deterministic sort by relative path from reports root (case-insensitive ordinal, stable), display `1..N` relative-path primary + `0` custom, invalid/empty selection => `ОШИБКА` and re-prompt/return to selection, custom path trim/expand `~`/existing file/`.json5` case-insensitive and invalid => `ОШИБКА` return to selection, copy to `<reportsRoot>\<originalFileName>` (leaf), conflict warn + `[R]eplace/[C]ancel` default Cancel (empty/non-R => Cancel), Replace => overwrite and run **copied destination** path on successful copy, Cancel => return to selection without running/copying/replacing, preserve CLI output, success/failure by `$LASTEXITCODE -eq 0` checked immediately, Russian `УСПЕХ/ОШИБКА/ИНФО`, `finally` keep-open, UTF-8 BOM scripts / PS 5.1+7+ compatible constructs.

### B4.1 Reusable scaffold/helpers and required control-flow changes

**Reusable as-is (with care):**
- `$ErrorActionPreference='Continue'`, console UTF-8 best-effort, `$script:FinalSuccess` flag, outer `try/catch/finally`, `Write-StepStatus`, `Write-FinalStatus`, final `Read-Host` keep-open — **keep**; they satisfy R6 and current behavior.
- `Resolve-TargetPython` and `Invoke-TargetCommand` — **directly reusable** for B4 run invocation and for any consistent command execution; `Invoke-TargetCommand` already preserves output and performs immediate `$LASTEXITCODE` capture/normalization (critical for R6/R5 run semantics).
- `Invoke-AppPathsProbe` — **reusable** for R1 path discovery at start of Шаг 1; on `$null` it already emits actionable `ОШИБКА` guidance ("Выполните инициализацию..." / уточните установку) which aligns with graceful missing/uninitialized handling (can be adapted in-step messaging if needed, but behavior is acceptable).

**Not present in export scaffold but needed (add inline, PS 5.1-safe):**
- Path normalization/expansion helper for custom file input (B3 has `Get-NormalizedExportFolderCandidate` for containers; B4 needs file-oriented normalization: trim, expand leading `~` to `$env:USERPROFILE` for `~`, `~/`, `~\`, then `[System.IO.Path]::GetFullPath()`; do **not** require container). Reuse B3 logic shape for tilde/`GetFullPath` but validate `Test-Path -LiteralPath <path> -PathType Leaf` and `.json5` extension.
- Catalog builder helper (semantic unit `Get-ReportCatalog`) returning ordered items with `RelativePath`, `FullPath`/`AbsolutePath`, and optionally display label.
- Selection/custom-copy state helpers (e.g. `Resolve-CustomReportSelection`) or an explicit loop/state machine in Шаг 1/2 handling invalid input, conflict prompt, and run-target resolution.
- Optional helper to compute stable relative path from normalized reports root (avoid `Resolve-Path -Relative` fragility across PS hosts/drives).

**Required control-flow transformation (P04-B4-T1/T2):**
1. Keep preparation: resolve target interpreter; on `$null` => Подготовка `ОШИБКА`, `$script:FinalSuccess=$false`, `return`.
2. Replace placeholder block with **Шаг 1/2 selection workflow**: call R1 probe; on `$null` => `$script:FinalSuccess=$false`, `return` (graceful stop, no crash-loop). Validate `Reports` path exists/is container where sensible before enumeration (if missing/uninitialized, emit clear `ОШИБКА` with init guidance and stop or allow controlled state per R1 — see B4.2). Build deterministic catalog; enter selection loop that can return a run target or continue on invalid/cancel paths.
3. Add **Шаг 3 run**: only when a valid run target is chosen; invoke `-m mko_get_mediascope_data run <targetPath>` with no `--export-folder`; set `$script:FinalSuccess = ($exitCode -eq 0)` and emit explicit step/final outcome; `return` to `finally`.
4. Ensure every mandatory failure path sets `$script:FinalSuccess=$false` and does not claim success. Loop-driven re-prompts are not script failures by themselves, but each invalid/cancel event must emit `ОШИБКА`/warning as specified.

### B4.2 R1 probe-at-start behavior and graceful missing/uninitialized handling

- **Start of Шаг 1:** after successful target resolution, perform `Invoke-AppPathsProbe -Target $target` (same env, R1/R2). This is required because reports root is runtime-truth (`APP_PATHS.reports`) and must not be guessed from README/env vars.
- **Probe failure (`$null`):** `Invoke-AppPathsProbe` already prints `ОШИБКА` (R1 probe/import/parse/shape) and guidance to initialize in selected environment (`init.ps1` or `mko-get-mediascope init`). B4 must treat this as a controlled stop: set `$script:FinalSuccess=$false` and `return` so `finally` reports overall failure and keeps window open. **No crash-loop, no path guessing.**
- **Probe success but reports directory missing/inaccessible:** distinguish from "no `*.json5` files". If `[System.IO.Directory]::Exists($paths.Reports)` is false (or `Test-Path -LiteralPath $paths.Reports -PathType Container` fails), settings may be partially initialized/uninitialized; emit explicit `ОШИБКА` (e.g. "Каталог reports не найден: <path>. Выполните init.ps1 ..." ) and stop with `$false` unless a safe create is implied — **do not create** reports dir implicitly (out of scope; CLI/init owns template layout). This is graceful, explicit failure, not an exception loop.
- **Enumeration errors (access denied, path issues):** `Get-ChildItem -LiteralPath $reportsRoot -Recurse -Filter '*.json5' -File -ErrorAction Stop` inside `try/catch` should be handled as step `ОШИБКА` with exception/message and controlled stop (`$false`, `return`) rather than unhandled termination; outer `catch` remains final safety net.
- **Empty catalog:** R5 allows "If none found, report clearly but allow option 0 where sensible." So Шаг 1 must **not** hard-fail solely because no numbered reports exist; it should inform user (ИНФО/ОШИБКА per UX choice, but clearly non-fatal to option 0 flow), render `0) ...` option, and continue selection loop. This is important for custom-path bootstrap flows.

### B4.3 Deterministic catalog construction (P04-B4-T1 core)

**Source/scope:** `<reportsRoot>` from R1 probe (`$paths.Reports`), recursive `*.json5` files only. Do **not** parse `list-reports` output.

**Enumeration (PS 5.1/7+):**
- Normalize root for comparison/slicing: `$reportsRootFull = [System.IO.Path]::GetFullPath($paths.Reports)`.
- Use `Get-ChildItem -LiteralPath $reportsRootFull -Recurse -Filter '*.json5' -File -ErrorAction Stop` (force array: `@( ... )`). `-Filter` is efficient and case-insensitive on Windows; still defensively verify extension if desired, but filter is sufficient in practice.
- Exclude directories implicitly via `-File`.

**Relative path computation (display/sort key):**
- Compute relative path from reports root for each file in a host-stable way (avoid relying on `Push-Location` + `Resolve-Path -Relative` formatting differences). Recommended: compare normalized full names; ensure `$rootPrefix` ends with separator for slicing: `$rootPrefix = $reportsRootFull.TrimEnd('\','/') + [System.IO.Path]::DirectorySeparatorChar`; `$full = [System.IO.Path]::GetFullPath($_.FullName)`; if `$full.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)` then `$rel = $full.Substring($rootPrefix.Length)` else fallback to `$_.Name` or `Resolve-Path -LiteralPath $_.FullName -Relative` cautiously. Relative path should use native separators for display or normalized consistently; sorting must be case-insensitive ordinal.
- **Primary display label = relative path from reports root** (R5/spec). This is unambiguous for nested reports and matches user-friendly naming requirement; full path may be shown as secondary detail only if unambiguous, but relative must remain primary.

**Deterministic sort (case-insensitive, stable):**
- R5 requires "deterministic sort by relative path from reports root, case-insensitive ordinal/stable" and R1 notes stable tiebreak. `Sort-Object -Property RelativePath` in PS can be locale-sensitive; prefer explicit comparer: `[Array]::Sort` not directly on custom objects, but can project to list and use `Sort-Object { $_.RelativePath } -Stable` in PS 7+ only? `-Stable` is not in Windows PS 5.1. **5.1-safe deterministic approach:** sort by relative path with `[StringComparer]::OrdinalIgnoreCase` via `Sort-Object -Property @{Expression={$_.RelativePath}; Ascending=$true}` is default culture in some cases? better `$items | Sort-Object -Property @{Expression={$_.RelativePath.ToLowerInvariant()}}` or build keys; for tie-break/stability include original index: `$items = $files | ForEach-Object -Begin {$i=0} -Process { [pscustomobject]@{ Index=$i++; RelativePath=...; FullPath=... } }; $sorted = $items | Sort-Object @{e={$_.RelativePath.ToLowerInvariant()}}, @{e={$_.Index}}` (or use `OrdinalIgnoreCase` equivalent by lowercasing invariant for key, and index preserves stability). **Risk:** `ToLowerInvariant()` differs from pure ordinal-ignore-case for some chars but is adequate/deterministic for paths; alternatively compare with custom .NET sort: `$list = [System.Collections.Generic.List[object]]::new($items); $list.Sort({ param($a,$b) $cmp=[StringComparer]::OrdinalIgnoreCase.Compare($a.RelativePath,$b.RelativePath); if($cmp -ne 0){$cmp}else{$a.Index.CompareTo($b.Index)} })` — works in PS 5.1 if delegate/scriptblock form accepted; if not, use LINQ-style ordering not guaranteed. Recommend documenting and implementing a deterministic, culture-invariant key strategy.

**Item model (catalog):**
- Each catalog item should carry at least: `Index` (1-based display number), `RelativePath` (string, primary label/sort key), `FullPath` (absolute normalized path, run/copy target reference), and optionally `LeafName` (`Split-Path -Leaf $FullPath`). Example: `[pscustomobject]@{ Number=1; RelativePath='sub\report.json5'; FullPath='C:\...\reports\sub\report.json5' }`.
- `Get-ReportCatalog` semantic unit should return ordered array/list of such items (or object with items/count/root).

### B4.4 Display format and selection loop semantics

- **Display:** clear Russian header (e.g. "Шаг 1 (список/выбор): доступные отчёты"), then for each item `"<Number>) <RelativePath>"` (relative primary). Always show `0) Указать путь к файлу отчёта вручную` (or equivalent). If catalog empty, print explicit message (e.g. "Отчёты (*.json5) в папке reports не найдены.") but still show option 0.
- **Selection input:** `Read-Host` prompt (e.g. "Выберите номер отчёта (0..N)"). Trim input: `$raw = (Read-Host ...); if ($null -eq $raw) { $raw='' }; $sel = ([string]$raw).Trim()`.
- **Validation:** empty or non-integer => `Write-StepStatus -Step 'Шаг 1 (список/выбор)' -Status 'ОШИБКА' -Message "Некорректный выбор..."` and **re-prompt/return to selection** (continue loop). Parse integer robustly for PS 5.1: `[int]$n = 0; $ok = [int]::TryParse($sel, [ref]$n)` (culture-invariant enough for digits) or regex `^\d+$`. Range must be `0..$catalog.Count` (or `$maxN`). Out of range => same `ОШИБКА` + re-prompt.
- **Valid `1..N`:** resolve selected item and set run target to `$item.FullPath` (existing file under reports). Proceed to Шаг 3 run.
- **Valid `0`:** enter custom-path flow (Шаг 2 logic). Do not run until custom flow yields a confirmed run target.
- **Loop structure:** `while ($true)` (or equivalent) containing display/selection/custom handling; break/return only when a run target is ready (then run) or when script must stop due to fatal prerequisite failure. Invalid/cancel/default-cancel paths **must not run/copy/replace** and must return to selection (continue). This satisfies "invalid/empty => ОШИБКА then re-prompt/return to selection without running".

### B4.5 Custom path flow (option 0, R5/Q6)

- **Input:** prompt for path (e.g. `Read-Host "Введите путь к файлу отчёта (.json5)"`), trim. Empty => `ОШИБКА`, return to selection.
- **Normalization/expansion:** expand leading `~` same as B3 (`~`, `~/`, `~\` -> `$env:USERPROFILE` + rest). Compute absolute/full path via `[System.IO.Path]::GetFullPath($expanded)` for validation/copy source reference. Use `-LiteralPath` in all cmdlets.
- **Validation (all required):** path exists and is a file: `Test-Path -LiteralPath $srcPath -PathType Leaf`. Extension `.json5` case-insensitive: `[System.IO.Path]::GetExtension($srcPath).Equals('.json5',[System.StringComparison]::OrdinalIgnoreCase)` (or `EndsWith` OrdinalIgnoreCase). If any check fails => explicit `ОШИБКА` with reason and **return to selection** (no copy/run).
- **Destination:** `$destPath = Join-Path -Path $reportsRootFull -ChildPath ([System.IO.Path]::GetFileName($srcPath))` / `Split-Path -Leaf $srcPath` (leaf name only, copy to reports root per spec/R5).
- **Same-source-as-destination consideration:** if user points to a file already at `$destPath` (normalized full paths equal, case-insensitively on Windows), conflict prompt would be confusing and `Copy-Item` over same file is risky/unnecessary. **Recommended handling:** detect `$srcFull.Equals($destFull, [System.StringComparison]::OrdinalIgnoreCase)` before copy/conflict prompt; if equal, treat as already-in-place custom selection and set run target to `$destFull` (or report and proceed) without copy/replace prompt — but this must remain within spec intent ("run selected/copied report"). Document as implementation guard; alternatively follow strict R5 flow (will prompt replace because exists) — implementor should prefer safe, non-destructive behavior and be explicit.
- **Conflict detection:** if `Test-Path -LiteralPath $destPath -PathType Leaf` (or exists) => warn that destination will be replaced and prompt `[R]eplace/[C]ancel` (default Cancel).
- **Prompt contract:** `Read-Host "Файл уже существует: <name>. Заменить? [R]eplace/[C]ancel (по умолчанию Cancel)"`. Trim + case-insensitive compare: accept replace only on explicit `R`/`r` (optionally `Replace` if desired, but must not broaden unsafely); **empty or any non-R => Cancel**. Default Cancel.
- **Replace branch:** attempt copy/overwrite `Copy-Item -LiteralPath $srcPath -Destination $destPath -Force -ErrorAction Stop` (or equivalent) inside `try/catch`. On copy failure => `ОШИБКА`, do **not** run, return to selection. On success => set run target to **copied destination path** `$destPath` (not source) and proceed to Шаг 3.
- **Cancel branch:** no copy, no replace, no run; emit clear ИНФО/ОШИБКА-neutral cancellation message as appropriate ("Отменено, возврат к выбору отчёта.") and **return to selection**.
- **No destination conflict:** copy source leaf to reports root, then run copied destination path on success (consistent with "copied report" flow). Handle copy errors explicitly.

### B4.6 CLI run invocation and outcome reporting (P04-B4-T2)

- **Command:** `Invoke-TargetCommand -Exe $target.Exe -Prefix @($target.Prefix) -Arguments @('-m','mko_get_mediascope_data','run', $runTargetPath)` in the **same resolved target environment** (R2). No `--export-folder`, no verbosity override unless spec changes (not requested). `$runTargetPath` must be the chosen numbered report's absolute/full path or the **copied destination path** after successful replace/copy as required.
- **Output preservation:** `Invoke-TargetCommand` streams child output directly (`& $Exe @allArgs` without capture) — compliant ("preserve CLI output"). Do not redirect/suppress.
- **Exit status:** check returned code immediately (helper already does); treat `$code -eq 0` as success, non-zero as failure. Do not infer success from text. Optionally also catch unexpected exceptions around invocation (outer try exists) but `$LASTEXITCODE` is authoritative for CLI outcome.
- **Step/final status:** on success emit `Write-StepStatus -Step 'Шаг 3 (запуск отчёта)' -Status 'УСПЕХ' -Message "Отчёт выполнен: <path-or-name>"` (include target context) and set `$script:FinalSuccess=$true`, then `return`. On failure emit `ОШИБКА` with exit code and target context, set `$script:FinalSuccess=$false`, `return`. `Write-FinalStatus` in `finally` reflects `$script:FinalSuccess` ("Скрипт завершён успешно." / "Скрипт завершён с ошибкой...") — acceptable; ensure per-step error details were printed before return.
- **Run target correctness:** numbered selection => catalog item's `FullPath`; custom replace => copied `$destPath`; custom copy (no prior conflict) => copied `$destPath`. Never run original custom source path after copy flow unless explicitly intended by a safe same-path guard (and document).

### B4.7 Overall success/failure, keep-open, and UX semantics

- `$script:FinalSuccess` becomes `$true` only after a completed, exit-code-successful Шаг 3 run. All preparation/probe/enumeration fatal failures, copy failures, or CLI run failures must leave/set `$false`.
- Selection-loop events (invalid selection, invalid custom path, cancel, copy failure with return-to-selection) are handled interactively; each should print explicit `ОШИБКА`/message and continue loop without marking script successful. Script remains in "not yet successful" state until a run succeeds.
- `finally` always executes `Read-Host 'Нажмите Enter для выхода'` and prints final status — satisfies "window remains open on completion/failure" (best-effort for host; R6). Do not add premature `exit` that skips `finally` unless unavoidable (prefer `return`).
- Messages must be Russian with clear `УСПЕХ/ОШИБКА/ИНФО` labels; CLI output preserved as-is between script status lines.

### B4.8 PS 5.1/7+ compatibility risks and mitigations

- **String comparison/sorting:** avoid culture-sensitive sorting; use invariant/lowercased key or `[StringComparer]::OrdinalIgnoreCase` comparisons for extensions, paths, prompts. `-Stable` sort not available in PS 5.1 — include index tiebreaker. `StringComparison` enum is available in .NET/PS 5.1; method overloads like `StartsWith/EndsWith/Equals(string,StringComparison)` are fine.
- **Path separators in relative display:** Windows reports roots may mix `\`/`/`; normalize for prefix stripping and display consistently (prefer native `\` or preserve discovered relative form). `GetFullPath` normalizes separators on Windows.
- **Quoting/argument passing:** passing `$runTargetPath` as a single argument element in `Invoke-TargetCommand` is correct for `& $Exe @allArgs` (PowerShell handles argument boundaries); paths with spaces/Cyrillic are safe as array elements. Do not concatenate into one string.
- **`Read-Host`:** works in PS 5.1/7; returns `$null` in non-interactive edge cases — guard and treat as empty/invalid. Prompts with Cyrillic/brackets are fine given UTF-8 BOM script.
- **`Copy-Item` behavior:** `-LiteralPath`, `-Destination`, `-Force` with `-ErrorAction Stop` is compatible; copying onto same file path can behave badly — add same-path guard as noted. Ensure destination directory is reports root (exists when reports catalog context is valid; if not, treat as prerequisite failure).
- **Regex/.NET APIs:** `[regex]`, `[System.IO.Path]`, `[System.StringComparison]`, `[System.Text.Encoding]` are PS 5.1-compatible. Avoid PS 7-only operators (`??`, ternary `?:`) — scaffold uses `if`; keep that style.
- **Encoding:** scripts already UTF-8 BOM; `Invoke-AppPathsProbe` captures probe output via `2>&1` and string conversion — Cyrillic in error text generally fine but probe output is paths/ASCII-like; preserve child CLI output unmodified.
- **Array/collection handling:** force arrays `@(...)` to avoid null/single-item issues in PS 5.1 (e.g. `$lines.Count`, catalog count when 0/1 items).

### B4.9 Implementation mapping to plan semantic units (actionable, within scope)

- **P04-B4-T1 / Шаг 1 + Шаг 2 (`examples/01-get-cli/export.ps1`):** replace placeholder with: (1) R1 probe and reports-root readiness checks; (2) `Get-ReportCatalog` (enumerate recursive `*.json5`, compute relative paths, deterministic sort, return ordered items); (3) selection loop rendering `1..N` relative labels + `0`; (4) `Resolve-CustomReportSelection` / inline flow for validation, copy-to-root leaf, conflict `[R]eplace/[C]ancel` default Cancel, same-path guard, and run-target handoff. No `list-reports` parsing, no unconfirmed mutations.
- **P04-B4-T2 / Шаг 3:** invoke module run on resolved target with same `$target`, no `--export-folder`, preserve output, evaluate `$LASTEXITCODE` immediately, set step/final success or failure and exit loop/return.
- **Reuse/consistency:** mirror B3 `~` expansion/GetFullPath style for file paths, `-LiteralPath` discipline, explicit status messages, and `try/catch` around IO/copy/enumeration operations that should fail gracefully.

### B4.10 Open risks / residual notes (non-blocking, handle at implementation/runtime)

- **Name-collision by design:** copying different nested/custom reports with identical leaf names to reports root requires explicit replace/cancel choice (R5/spec) — correct but potentially surprising; messaging should be clear.
- **Interactive exit path:** spec does not define a "quit without running" menu option; invalid/cancel loops return to selection. Users can close window or Ctrl+C; `finally` keep-open may not run on hard termination — acceptable/host-dependent.
- **Missing/uninitialized settings vs empty reports:** probe failure and missing reports container should be explicit fatal `ОШИБКА` states; empty-but-present reports directory should allow option 0.
- **Console rendering:** Cyrillic display depends on host font/codepage despite UTF-8 measures (known residual, R6/R7).
- **No module-boundary escalation observed:** all required behavior is implementable within B4 scope/contracts (`-c` probe allowed per R1, filesystem enumeration, CLI `run` by path). If a stricter policy blocks `-c`, follow R1/R7 escalation (do not guess paths).
