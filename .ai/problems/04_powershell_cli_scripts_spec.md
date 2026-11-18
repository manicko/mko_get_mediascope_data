---
id: powershell-cli-scripts-spec
domain: problems
problem: "04"
tags:
  - specification
  - powershell
  - cli
  - examples
  - windows
related:
  - Problem_02.md
  - ../overview/current-behavior.md
  - ../../README.md
status: ready_for_planning
---

# Problem 04 — PowerShell CLI Scripts (Init/Setup & Export) Specification

## 1. Problem statement

Users need two Windows PowerShell scripts in `examples/01-get-cli/` that automate (a) environment/setup initialization and (b) report export using the **existing** `mko_get_mediascope_data` CLI only. The scripts must replace manual multi-step flows shown in older examples (which rely on intermediate Python scripts or library imports) while preserving module behavior and not modifying module code.

**Source:** [.ai/problems/Problem_02.md](Problem_02.md)  
**Product Owner decisions:** all recommended options confirmed (2026-11-18).

## 2. Confirmed requirements

### 2.1 Global
- **Location:** `examples/01-get-cli/` (target folder; currently empty).
- **Deliverables (proposed filenames):** `init.ps1` (Script 1) and `export.ps1` (Script 2).
- **Constraint (strict):** Module/application code must not be changed.
- **Constraint (CLI-only boundary, PO-confirmed Q1=A):** No intermediate `.py` script files may be created or run. Module operations must use the existing CLI only (`init`, `list-reports`, `run`, via `mko-get-mediascope` and/or `python -m mko_get_mediascope_data`). External tool/package commands required for setup (e.g. Git installer/`winget`, `pip install --upgrade ...`) and direct editing of user `app_config.json5` for `export_folder` are allowed.
- **UX:** Scripts must not close after execution and must show clear success/result status. If any step fails, explicitly indicate the failing step/error to the user.
- **Language (PO-confirmed Q7=A):** User-facing messages in Russian with clear status labels (e.g. `УСПЕХ` / `ОШИБКА`), keeping CLI command output as-is.
- **PowerShell host (PO-confirmed Q8=A):** Support Windows PowerShell 5.1 and PowerShell 7+ where practical; prefer 5.1-compatible constructs and document any limitations.

### 2.2 Script 1 — Initialization / setup (`init.ps1`)
- **Step 1 — Git (PO-confirmed Q2=A):**
  - Detect Git via `git --version`.
  - If missing: attempt standard Windows installation (prefer `winget` if available). If installation cannot be completed automatically, show clear manual-install guidance and stop with overall error.
  - If Git was newly installed in this run (or not usable in-session due to PATH), stop after Step 1 with an explicit message that the script/terminal must be restarted, as required by the problem statement.
  - If present: perform best-effort update when supported; otherwise report current version and continue.
- **Step 2 — Install/update Mediascope client (PO-confirmed Q3=A):** Install/upgrade `mediascope-api-lib` (PyPI, latest) using `python -m pip install --upgrade mediascope-api-lib` in the same Python environment used to run the CLI.
- **Step 3 — Install/update `mko_get_mediascope_data` and initialize settings (PO-confirmed Q3=A):**
  - Install/upgrade `mko_get_mediascope_data` from `git+https://github.com/manicko/mko_get_mediascope_data.git` (default/latest) using `python -m pip install --upgrade ...` in the same Python environment.
  - Run settings initialization via CLI `init` **without `--force`** so existing user settings are not overwritten (non-destructive copy/legacy credential migration per current CLI behavior).
- **Step 4 — Show/change export folder (PO-confirmed Q4=A):**
  - Read and display current `export_folder` value from user `settings/app_config.json5` (key `"export_folder"`). Optionally also display the resolved path for clarity.
  - Ask whether the user wants to change the export folder.
  - If no: leave configuration unchanged.
  - If yes: prompt for a path (accept absolute/relative/`~`), trim input, validate with `Test-Path -PathType Container` (existing directory). Do **not** create directories.
    - If path is missing/invalid: show explicit error and **do not** change configuration.
    - If path is valid: update `export_folder` in `app_config.json5` and confirm the saved value.
- **Completion/failure policy (PO-confirmed Q7=A):** Stop on a failed mandatory step (respecting the required stop/restart case), report the exact step/error, and mark overall result as failed; do not claim success. Window remains open.

### 2.3 Script 2 — Export (`export.ps1`)
- **Step 1 — List and select report (PO-confirmed Q5=A):**
  - Enumerate `*.json5` files recursively under the user settings `reports/` folder (same scope as `list-reports`), sort deterministically, and display numbered items 1..N with a user-friendly name (e.g. relative path from `reports/`, optionally with full path). Selection must be unambiguous.
  - Option `0` = specify a custom path.
  - Ask which report to run (answer = list number).
- **Step 2 — Custom path handling (PO-confirmed Q6=A):**
  - If user chooses `0`, prompt for an existing `.json5` report file path.
  - Copy the report to the user `reports/` **root** using the original filename.
  - If a same-name file already exists in the destination, warn it will be replaced and ask `[R]eplace` / `[C]ancel` (default: Cancel).
    - **Replace:** overwrite and run the **copied** report in user `reports/`.
    - **Cancel:** return to report selection without running.
- **Step 3 — Run export:**
  - Execute the selected/copied report via CLI `run <report_path>` and report success/failure based on CLI outcome (exit/status/messages). Use configured export folder (no per-run `--export-folder` prompt in this flow).
- **Completion:** Window remains open; final status/errors are explicit.

## 3. Business goal, scope, constraints, assumptions

### 3.1 Business goal
- Reduce setup and export friction for Windows users by providing guided, repeatable scripts that prepare required tooling/packages, initialize usable Mediascope loader settings safely (no destructive overwrite), make export-folder configuration visible and controllable, and simplify selecting/running report exports.

### 3.2 Scope (in)
- Two PowerShell scripts in `examples/01-get-cli/` (`init.ps1`, `export.ps1`).
- User-facing prompts, validation, stepwise status/error reporting, and non-closing console behavior.
- Git detection/install/update with restart-stop handling as specified; installation/upgrade of `mediascope-api-lib` and `mko_get_mediascope_data` into a consistently selected Python/CLI environment.
- CLI-based settings initialization (`init`, no `--force`), report discovery context aligned with user `reports/`, and report execution via `run`.
- Reading and updating user `app_config.json5` `"export_folder"` with JSON5-preserving, targeted text edit and required existence validation.

### 3.3 Out of scope
- Any changes to module/application code, dependencies, CLI behavior, configuration schema, or build/release configuration.
- Creating or running intermediate Python script files (`.py`).
- New CLI features (config get/set, machine-readable listing, report registration commands).
- Automated Mediascope credential value entry/validation beyond what existing `init` provides.
- Refactoring existing examples outside `examples/01-get-cli/` (except this spec artifact).
- Non-Windows scripts, PyPI/publication changes, or scheduler/web/DB concerns.

### 3.4 Constraints (current system, evidence-based)
- **CLI commands:** `init [--force/-f]`, `run <REPORT> [--verbosity/-v] [--export-folder PATH]`, `list-reports` (`src/mko_get_mediascope_data/app.py`).
- **User settings location:** platformdirs-based `user_config_dir("mko_get_mediascope_data")`; on Windows typically `%APPDATA%\mko_get_mediascope_data\settings\` with `app_config.json5`, `reports/`, `connections/mediascope.json`, `defaults/`, `log_config.json5` (`core/paths.py`, `README.md`).
- **`init` behavior:** non-`--force` skips existing files and performs legacy credential rename handling; `--force` overwrites same-name template files (`core/init_service.py`). Scripts must use non-`--force`.
- **`list-reports` behavior:** recursive `*.json5` under user `reports/`, human-formatted `• <path>` output (`app.py`, `core/utils.py`).
- **`run` behavior:** requires existing file path; `--export-folder` is per-run override (no config mutation), preserves report `relative_path`; effective output `<effective_base>/<relative_path>/...` (`app.py`, `core/services.py`).
- **Config:** `export_folder` in JSON5 `app_config.json5`; no CLI get/set. JSON5 may contain comments/trailing commas, so PowerShell JSON cmdlets are unsafe for full rewrites.
- **Target folder:** `examples/01-get-cli/` exists and is currently empty.

### 3.5 Assumptions
- Target users run on Windows with PowerShell 5.1 or 7+ available; network access and permissions for installs may vary (scripts must report failures clearly).
- `mediascope-api-lib` is the correct Mediascope client package; project source is `git+https://github.com/manicko/mko_get_mediascope_data.git` (default branch/latest) unless changed by future decision.
- Scripts will select and consistently reuse one Python/CLI environment for pip installs and module CLI invocation (prefer resolved `mko-get-mediascope` when appropriate, with reliable fallback to `python -m mko_get_mediascope_data` via discovered interpreter).
- Script 2 “user folder” means the user settings `reports/` directory; custom-path copies go to the `reports/` root and conflicts are determined by destination filename.
- Config file uses a standard `"export_folder": "<value>"` entry that can be updated via a strict, minimal targeted replacement; if the key cannot be uniquely/safely located, the script must fail without corrupting the file.

## 4. Conceptual development tasks

| # | Task | Purpose | Expected outcome | Dependencies |
|---|---|---|---|---|
| 1 | Script contracts & UX flows | Define unambiguous behavior for `init.ps1` and `export.ps1` (prompts, branching, messages, keep-open behavior) | Implementable flow spec with Russian UX, step labels, and status semantics | PO decisions (confirmed) |
| 2 | Environment/bootstrap implementation (Script 1) | Detect/install/update Git and install/upgrade packages in a consistent Python/CLI environment | Robust Step 1–3 behavior including restart-stop, exit-code checks, and clear errors | T1 |
| 3 | Safe init & export-folder management (Script 1) | Run non-destructive CLI init and manage `export_folder` in JSON5 config safely | Correct read/display/prompt/validate/update/no-change behavior without config corruption | T1–T2 |
| 4 | Report selection, custom copy, and run (Script 2) | Provide deterministic report selection, safe custom-path copy/replace flow, and CLI execution | Numbered listing, option 0 handling, conflict resolution, correct run target, and status reporting | T1, T3 (user paths) |
| 5 | Error handling, idempotence, and acceptance validation | Ensure scripts are re-runnable and user outcomes are explicit | Step-level try/catch/finally behavior, idempotent operations, and verified acceptance criteria | T1–T4 |
| 6 | Example packaging | Deliver scripts in `examples/01-get-cli/` aligned with repo examples | `init.ps1` and `export.ps1` (and optional minimal usage note if desired later) ready for user execution | T1–T5 |

## 5. Product Owner decisions (confirmed)

All recommended options confirmed on 2026-11-18:

| ID | Decision | Selected |
|---|---|---|
| Q1 | CLI-only boundary for setup/config actions | **A:** Allow external tool/package commands required for setup (e.g. Git installer/`winget`, `pip install --upgrade ...`) and direct editing of user `app_config.json5` for `export_folder`; do not create/run intermediate `.py` files. CLI only for module operations (`init`, `list-reports`, `run`). |
| Q2 | Git install/update and restart behavior | **A:** Detect via `git --version`; if missing try standard Windows install (prefer `winget`), else clear manual guidance and stop with error; if newly installed/not usable in-session, stop after Step 1 with explicit restart message; if present, best-effort update when supported else report version and continue. |
| Q3 | “Mediascope” and `mko_get_mediascope_data` targets/environment | **A:** `mediascope-api-lib` (PyPI, latest) and `mko_get_mediascope_data` from `git+https://github.com/manicko/mko_get_mediascope_data.git` (default/latest), installed/upgraded via `python -m pip install --upgrade ...` in the same consistently selected Python/CLI environment. |
| Q4 | Export-folder change UX and validation | **A:** Show current `export_folder` (optionally resolved); accept abs/rel/`~`, trim, validate existing directory (`Test-Path -PathType Container`); missing/invalid => error and no change; valid => update config and confirm. Do not create directories. |
| Q5 | Report listing and selection | **A:** Enumerate recursive `*.json5` under user `reports/` (same scope as `list-reports`), sort deterministically, number 1..N with user-friendly names; `0` = custom path; selection unambiguous. |
| Q6 | Custom path behavior and conflict handling | **A:** Prompt for existing `.json5` file; copy to user `reports/` root with original filename; if same-name exists warn replace and ask `[R]eplace`/`[C]ancel` (default Cancel); Replace => overwrite and run copied report; Cancel => return to selection without running. |
| Q7 | Failure policy and user-facing language | **A (policy):** Stop on failed mandatory step (respecting restart stop), report exact step/error, overall failed. **A (language):** Russian user-facing messages with clear status labels; keep CLI output as-is. |
| Q8 | PowerShell support target | **A:** Support Windows PowerShell 5.1 and PowerShell 7+ where practical; prefer 5.1-compatible constructs and document limitations. |

## 6. Research summary

### 6.1 Current CLI and configuration behavior (evidence)
- **CLI (`src/mko_get_mediascope_data/app.py`):** `init(--force)` initializes user settings; `run` accepts existing report file, `--verbosity/-v`, per-run `--export-folder` override (no config mutation) and reports success/failure with non-zero exit on exception; `list-reports` lists recursive `*.json5` from user `reports/` as sorted `• <path>` or “No report files found in reports/”. Missing-settings bootstrap suggests `init`.
- **Paths (`core/paths.py`):** `user_config_dir(APP_NAME)`/`settings` layout; Windows typically `%APPDATA%\mko_get_mediascope_data\settings\`; `connection_config` prefers `mediascope.json` with legacy fallback/migration behavior.
- **Init (`core/init_service.py`):** migrates legacy credential filename, copies bundled templates recursively; non-force skips existing files (idempotent/non-destructive), force overwrites same-name files.
- **Export/config (`core/services.py`, `core/models.py`, `settings/app_config.json5`):** `export_folder` stored in JSON5 config; service resolves/ensures effective base; CLI per-run override replaces base while preserving `relative_path`; no CLI config get/set.
- **Reports (`core/utils.py`):** recursive `*.json5` discovery helper used by `list-reports`.

### 6.2 Existing examples and patterns (evidence)
- **`examples/01-get/run_report.ps1`, `run_report.bat`:** script-dir context, interpreter discovery (`py -3`/`python`/`uv run python`), exit-code reporting, keep-console-open (`finally`/`pause`) — reusable UX patterns.
- **`examples/01-get/run_report.py`:** library/DI and workspace-local assumptions (not CLI, uses intermediate Python logic) — not compliant with this task’s constraints.
- **`examples/01-get-cli/`:** currently empty target.

### 6.3 Feasible implementation directions (selected)
- **CLI/interpreter resolution:** detect `mko-get-mediascope` and/or a Python interpreter consistently; reuse the same interpreter for `pip` and `python -m mko_get_mediascope_data` calls to avoid environment drift.
- **Git/setup:** detect first, prefer `winget` for install when available, provide manual fallback, and enforce explicit restart-stop when Git becomes newly installed/unavailable in-session.
- **Settings init:** use `mko-get-mediascope init` (no `--force`) for safe, idempotent initialization.
- **JSON5 config edit:** perform a strict, minimal targeted replacement of the `"export_folder"` value (preserve surrounding comments/structure), validate the match/update, and abort without modifying file if unsafe/ambiguous.
- **Report selection:** deterministic recursive filesystem enumeration under user `reports/` for reliable numbering; pass selected/copied existing file path to `run`.
- **Robust UX:** `try/catch/finally` (or equivalent) with per-step status, explicit failure messages, preserved CLI output, and final pause/read-host to keep window open.

## 7. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| JSON5 config edit may corrupt formatting/comments if done naively | Broken config, user unable to run | Targeted single-key replacement with strict pattern/match-count checks, no full JSON rewrite, no change on ambiguity/failure |
| Multiple Python environments/PATH ambiguity | Packages installed to wrong env, CLI/run mismatch | Detect and consistently reuse one interpreter/environment for installs and module invocation; report chosen target |
| Git install requires elevation or PATH refresh | Step 1 cannot complete in-session | Prefer `winget` when available, clear manual guidance on failure, mandatory stop+restart message when newly installed/unusable |
| `list-reports` output is not machine-stable | Fragile parsing/selection | Use filesystem enumeration (same scope) for numbering; optionally display CLI output separately |
| Custom copy conflicts or invalid custom path | Overwrite confusion or run failure | Require existing `.json5`, copy to reports root, explicit replace/cancel (default Cancel), run copied path only after successful copy/replace |
| Windows PowerShell 5.1 vs 7+ differences (encoding/constructs) | Garbled text or runtime incompatibility | Prefer 5.1-compatible constructs, be careful with UTF-8/JSON5 text handling, test for both where practical |
| Existing/malformed user settings or missing credentials | `init`/`run`/selection may fail | Preflight key paths where helpful, rely on CLI exit/status for authoritative outcomes, report exact errors |

## 8. Assumptions

- Proposed script filenames are `init.ps1` and `export.ps1` in `examples/01-get-cli/`.
- Script 1 displays raw `export_folder` value and may also display a resolved path for user clarity (consistent with Q4=A).
- Script 2 uses the configured export folder during `run` (no per-run `--export-folder` prompt).
- Re-running scripts is expected: `init.ps1` should be idempotent where possible (detect/install/upgrade/init without `--force`), `export.ps1` mutates reports only on explicit confirmed custom-path copy.
- Acceptance validation will be based on behavior/CLI outcomes; no module test changes are in scope.

## 9. Constraints

- No changes to `src/`, package behavior, CLI implementation, or build/release configuration.
- No intermediate `.py` files.
- Module operations only via existing CLI (`init`, `list-reports`, `run`).
- Settings initialization must not overwrite existing user settings (no `--force`).
- `export_folder` must not be changed when the provided path does not exist/is not a valid directory.

## 10. Out of scope

- New CLI/config commands or machine-readable output modes.
- Credential provisioning UI, secret validation, or API connectivity troubleshooting beyond reporting CLI/runtime errors.
- Automated creation of missing export directories (explicitly disallowed by Q4=A).
- Advanced report management (rename/move/delete/metadata editing) beyond the specified custom-path copy flow.
- Changes to report formats, export layout semantics, or downstream data processing.

## 11. Open questions (non-blocking / implementation notes)

1. **Optional usage note:** include a short `README.md` or header usage instructions in `examples/01-get-cli/`? (not specified; recommended as a small documentation aid if desired, without changing module docs).
2. **CLI output language/encoding details:** exact Russian phrasing and status-label format to use consistently across both scripts (implementation choice, must remain clear and unambiguous).
3. **Interpreter selection order:** concrete precedence among `mko-get-mediascope`, `py -3`, `python`, and `uv run python` when multiple are present, provided the same environment is used consistently for installs and CLI calls.

No blocking Product Owner decisions remain; Q1–Q8 are confirmed.

## 12. Definition of Ready

- [x] PO decisions for Q1–Q8 recorded and confirmed (all recommended).
- [x] CLI-only boundary is unambiguous for install, invocation, and config-edit actions.
- [x] Script purposes, target location, and proposed filenames (`init.ps1`, `export.ps1`) are defined.
- [x] Script 1 flow, restart condition, export-folder validation/persistence, and failure policy are specified.
- [x] Script 2 report source/numbering, custom-path copy destination/conflict handling, and run target are specified.
- [x] User-facing language and supported PowerShell hosts are specified.
- [x] Assumptions, constraints, risks, out-of-scope items, and acceptance criteria basis are documented.
- [x] No module code changes required; discovered blockers (if any during implementation) must be escalated as a separate problem/change request.

**Status:** Ready for implementation planning.

## 13. Acceptance criteria

### Script 1 — `init.ps1`
- [ ] Window remains open on completion; overall status is clearly reported as success or failed with explicit step/error details.
- [ ] Git is detected and handled per Q2=A; on first-time install not usable in-session, script stops after Step 1 with explicit restart instruction and does not claim success.
- [ ] `mediascope-api-lib` and `mko_get_mediascope_data` are installed/upgraded per Q3=A in the consistently selected environment; failures are reported explicitly.
- [ ] Settings initialization runs via CLI `init` without `--force` and does not overwrite existing user settings.
- [ ] Current `export_folder` is shown from user `app_config.json5`; declining change leaves config unchanged.
- [ ] Non-existent/invalid export directory input produces explicit error and does not modify `export_folder`.
- [ ] Valid existing directory input updates `export_folder` (JSON5 preserved) and confirms saved value.
- [ ] Any mandatory step failure stops execution (except required restart stop), reports exact error, and results in overall failed status.

### Script 2 — `export.ps1`
- [ ] Window remains open on completion; final status/errors are explicit.
- [ ] Reports are enumerated recursively from user `reports/` (`*.json5`), sorted deterministically, and displayed as numbered items 1..N with option `0` for custom path.
- [ ] Selecting a numbered report runs it via CLI `run` and reports outcome based on CLI result.
- [ ] Option `0` requires an existing `.json5` path and copies it to user `reports/` root with original filename.
- [ ] If destination filename exists, script warns it will be replaced and offers `[R]eplace`/`[C]ancel` (default Cancel); Replace overwrites and runs copied report, Cancel returns to selection without running.
- [ ] No report is copied/replaced without explicit user choice per the confirmed flow.
