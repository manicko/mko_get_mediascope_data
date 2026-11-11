# Specification: Optional Export Folder Override for Reports

**Spec ID:** 03  
**Problem Name:** Optional export folder override (`run_report` / CLI)  
**Status:** Draft  
**Date:** 2026-11-11  
**Analyst:** Tech Lead (task definition)  
**Source:** User request — allow running a report into a caller-specified export directory instead of the configured `export_folder`

---

## 1. Problem Statement

Currently the base export directory for all report outputs is taken from `app_settings.export_folder` in the application configuration (`app_config.json5`). Each report entry writes to `<export_folder>/<relative_path>` (where `relative_path` comes from the report definition). Users who need outputs in a different location (e.g. a project-specific or notebook-specific folder such as `C:\data`) must edit the global/user config or construct a custom service workspace.

There is no way to override the export base directory per run through the supported public API or the CLI while keeping the rest of the configured behavior intact.

## 2. Business Goal

Allow a caller to direct report outputs to a chosen export directory on a per-invocation basis, without changing global configuration, so the tool is easier to use from scripts, notebooks, automation, and ad-hoc CLI runs. Default behavior must remain unchanged for existing users and integrations.

## 3. Scope

### In scope
- A per-run, optional export-folder override exposed through the public library API and the CLI.
- The override replaces the configured export base directory (`export_folder` from app config) for that run only, while preserving how report entries determine their output subpaths (`relative_path`) relative to that base.
- CLI support via a dedicated option (e.g. `--export-folder`) on the report execution command.
- Tests, quality checks, and documentation updates required to make the change safe and supportable.

### Out of scope
- Changing the meaning, format, or location of `relative_path` in report definitions, or altering report-generation/data-processing semantics.
- Introducing new configuration files/keys, persistent user preferences, or workspace/bootstrap changes beyond what is necessary for this feature.
- Changes to unrelated commands, progress/reporting semantics, vendor API behavior, or the async/concurrency model.
- Redesign of path/config architecture; the solution must fit existing patterns.

## 4. Confirmed Requirements

| ID | Requirement | Acceptance intent |
|---|---|---|
| CR-1 | **Optional export-folder parameter in public API** | Both public report-execution entry points (`run_report_async` and `run_report`) accept an optional parameter that specifies the export base directory for that invocation. |
| CR-2 | **CLI option** | The CLI `run` command exposes an option (provisionally `--export-folder`, with a short alias if consistent with existing CLI conventions) that passes the override through to report execution. |
| CR-3 | **Default behavior preserved** | When the override is not provided (or is explicitly absent per the chosen API contract), behavior is identical to today: the export base directory comes from the configured `app_settings.export_folder` and all existing callers continue to work without modification. |
| CR-4 | **Override semantics** | When provided, the override replaces the configured export base directory for that run. The per-report-entry output location remains derived from that base together with the entry’s `relative_path` (i.e. the override changes the base, not the report’s internal relative layout), consistent with current path-composition semantics. |
| CR-5 | **Scope and isolation** | The override is effective only for the invoked run. It must not mutate application configuration, cached settings/state, or the process-wide service in a way that affects subsequent runs or other callers. |
| CR-6 | **Path handling** | The provided path is handled consistently with existing path-resolution/normalization and directory-preparation behavior (including user-home expansion and absolute/relative path handling) and is validated in line with project conventions. The effective export destination(s) must be observable/returned in the same way as today. |
| CR-7 | **Correctness across report entries** | The override applies uniformly to all report entries processed in a single run (including multi-entry report definitions) and to resume/“already done” detection and all exported artifacts for that run, so outputs and skip decisions are coherent with the overridden destination. |
| CR-8 | **Backward compatibility** | The change is additive and backward-compatible: existing signatures/usages, CLI invocations without the new option, library consumers, and documented workflows remain valid. |
| CR-9 | **Documentation** | User-facing documentation (CLI help text and relevant README/API/docs) is updated to describe the new option/parameter, its precedence relative to configured `export_folder`, and its interaction with `relative_path`. |
| CR-10 | **Quality and verification** | The implementation includes appropriate automated tests and passes project quality gates (lint, type checking, and test suite) without regressions. |

## 5. Conceptual Tasks

| Task | Purpose | Expected outcome | Dependencies |
|---|---|---|---|
| T1. Define the override contract | Make behavior unambiguous before coding. | A clear, consistent contract for the parameter/option name, accepted types, default/absent meaning, precedence over config, interaction with `relative_path`, and path-resolution expectations — aligned across API and CLI. | This spec. |
| T2. Apply override in execution flow | Thread the override through report execution so the effective export base is used for that run. | Report outputs (and any destination-derived behavior such as resume checks and returned export paths) are written under the overridden base when provided, with no impact when absent and no cross-run side effects. | T1. |
| T3. Expose CLI option | Provide parity with the API for command-line users. | `run` command accepts the export-folder option and forwards it correctly; help output documents it. | T1, T2. |
| T4. Tests | Prove correctness and prevent regressions. | Automated coverage for: default behavior unchanged; override routes outputs/returned paths to the specified base; interaction with `relative_path` and multi-entry runs; CLI option forwarding/behavior; and relevant path/edge cases consistent with the contract. Existing tests remain valid. | T2, T3. |
| T5. Documentation and verification | Make the feature discoverable and keep the codebase healthy. | Updated CLI help and user/developer docs; lint, typecheck, and full test suite pass. | T2–T4. |

## 6. Constraints and Non-Goals (for the implementor)

- **Architecture:** Preserve strict separation of concerns (CLI/interface vs. orchestration/services vs. report/data logic). Do not introduce new abstractions or cross-layer coupling without clear justification; follow established patterns in `core/services`, `core/paths`, `app`, and reports.
- **Code quality:** Maintain type safety (strict hints, Pydantic/models where appropriate), small focused changes, meaningful naming, and existing logging/console conventions (no `print`, correct CLI vs. core logging boundaries).
- **Behavioral safety:** No silent change to existing export locations, file naming, or report semantics for callers that do not opt in. Avoid global mutable side effects.
- **Scope discipline:** Implement only what is required to satisfy this specification; defer unrelated refactors or “cleanup”.

## 7. Acceptance Criteria

- [ ] `run_report` and `run_report_async` accept an optional export-folder override and, when it is omitted, behave exactly as before.
- [ ] When the override is provided, outputs for the run are created under the specified base directory with each entry’s `relative_path` applied relative to it; returned export path(s) reflect the actual destination(s).
- [ ] The CLI `run` command provides an `--export-folder` (or equivalent, per contract) option that produces the same effect as the API override.
- [ ] The override does not alter configuration, cached state, or subsequent runs; multi-entry runs are handled consistently.
- [ ] Automated tests cover the new behavior and the unchanged default path; existing relevant tests continue to pass.
- [ ] Linting, type checking, and the test suite pass per project standards; user-facing docs/help are updated.

## 8. Open Decisions (to resolve during implementation, consistent with this spec)

- Final parameter/option naming and exact type contract (kept consistent between API and CLI and idiomatic for the codebase).
- How strictly to validate or normalize relative vs. absolute override paths, provided it remains consistent with existing path handling and CR-6.
- Whether the CLI option should accept the same path forms as the API (recommended for consistency).

## 9. Risks / Notes

- **Resume/check-done coherence:** Because completion detection is based on existing files in the export location, the override must ensure skip/resume logic evaluates against the same destination used for writing in that run (CR-7).
- **Path semantics:** The contract must be explicit and tested for the interaction between the overridden base and `relative_path` so users get predictable results (especially for absolute vs. relative inputs).
- **Singleton/global state:** Care is needed so a per-run override on the shared service does not leak into later invocations.
