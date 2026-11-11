---
id: PLAN-02
title: Apply configured verbosity_mode for library/notebook usage
status: draft
created: 2026-11-10
source_request: "Сделай правки, чтобы verbosity_mode из app_config.json5 реально влиял на уровень логов в ноутбуке/библиотечном использовании). Исправь тесты, если нужно добавь тесты."
source_context: "{code_context} from Auditor (step 0)"
---

# PLAN-02 — Apply `verbosity_mode` for library/notebook usage

## 1. Objective
Ensure `app_config.json5` `verbosity_mode` controls logging levels in notebook/library usage (at minimum `run_report_async` / `run_report` and default `get_app_service()` flows), while preserving CLI behavior and existing configuration/lifecycle semantics.

## 2. Background / Evidence (step 0)
- **Implemented:** `VerbosityMode` + `to_logging_level()` and `AppSettings.verbosity_mode` default (`core/models.py`); template default (`settings/app_config.json5`); `AppService.prepare_app()` applies `dictConfig` on init/reload (`core/services.py`); CLI `run` applies `configure_logging_level` after service resolution using flag-or-config precedence (`app.py`).
- **Gap:** library/notebook entry points (`run_report`, `run_report_async`, plain singleton) do not apply configured verbosity; levels come from `log_config.json5` via `dictConfig` side effect of first `AppService` creation.
- **Existing tests:** enum/default/coercion, `configure_logging_level` behavior, level survives dictConfig, CLI flag/omitted cases (`tests/test_verbosity.py`). No library/notebook verbosity assertions.
- **Constraints:** keep level logic reusable from `core` without `core -> app` dependency; preserve layering (`core` no CLI/Rich), singleton + global stdlib logging, post-`dictConfig` ordering, `reload_config()` re-applies `dictConfig`, public API stability, minimal scope.

## 3. Scope
### In scope
- Make configured `verbosity_mode` effective for library/notebook usage.
- Reuse a shared/core-level verbosity application mechanism from CLI where appropriate (thin CLI wrapper acceptable).
- Fix/update existing tests if behavior changes; add tests for library/notebook flows as needed.
- Preserve current CLI `run` precedence/UX semantics unless required by the fix.

### Out of scope (non-goals)
- Redesign of logging architecture, progress/reporting UX, or config schema beyond what is necessary.
- Changes to `VerbosityMode` value semantics (e.g. `QUIET`/`DEFAULT` mapping) unless explicitly required by a resolved defect.
- Broad refactors, unrelated cleanup, or speculative API expansion.

## 4. Assumptions
- Product intent is clear from request; no PO clarification required (step 2 skippable).
- “Ноутбук/библиотечное использование” includes programmatic calls that construct/use the service via public API/singleton, not only CLI.
- Existing `dictConfig` remains the authoritative logger/handler/root setup source.

## 5. Execution blocks

### Block 1 — Resolve technical design (centralization, timing, precedence, reload)
- **Goal:** Eliminate implementation uncertainty and select a safe, convention-aligned approach (or confirm one) before code changes.
- **Scope:**
  - Audit exact call/lifecycle points: `get_app_service`, `AppService.__init__`, `prepare_app`, `reload_config`, `run_report_async/run_report`, CLI `run`/`list_reports`.
  - Evaluate viable paths for where verbosity is applied (after `dictConfig`) and how CLI flag precedence coexists with config-driven library behavior.
  - Decide handler scope (root vs all loggers), reload behavior, and whether an explicit library override parameter/helper is warranted within minimal scope.
  - Produce a short design decision record embedded in this plan or as implementation notes for downstream blocks.
- **Dependencies:** none (entry block). Requires step 0 context (available).
- **Risks:**
  - Wrong placement causes verbosity to be ignored (pre-dictConfig) or overwritten (`reload_config`).
  - Global/singleton side effects leak across calls/notebook cells.
  - Layering violation if logic remains only in `app.py`.
  - Public API/behavior regression (CLI precedence, existing tests).
- **Alternatives considered (do NOT choose here — technical uncertainty):**
  - **A. Core-centralized, service-lifecycle application:** move/apply helper in `core` and invoke after `dictConfig` in `prepare_app()` (and thus on init/reload) so configured mode applies uniformly; CLI reuses helper and keeps flag precedence (flag may override post-apply). *Pros:* single source, covers singleton/library; *Cons:* implicit global side effect on service creation/reload, flag-vs-config interaction needs care.
  - **B. Core helper + explicit application in public/library entry points:** call after `get_app_service()` in `run_report_async` (and CLI) without changing service init. *Pros:* localized; *Cons:* plain `get_app_service()` users still unconfigured, duplication risk.
  - **C. Optional override parameter on public API:** e.g. allow caller to pass verbosity/mode. *Pros:* explicit notebook control; *Cons:* API surface change, still need default-from-config behavior, scope growth.
  - **D. Apply in `get_app_service()` after construction:** *Pros:* covers singleton access; *Cons:* ordering with init dictConfig must be guaranteed, reload still a concern, hidden side effect.
  - *Note:* paths can be combined (e.g. A + thin CLI wrapper) if minimal and justified by resolved risks.
- **Required agents:** **Auditor, Researcher, Planner, Validator** (technical uncertainty + global logging/singleton/lifecycle risks justify full pre-implementation review; treat as high-risk decision block).

```yaml
id: P02-B1-T1
title: Investigate and decide verbosity application design for library usage
priority: high
depends_on: []
description: >
  Resolve where and when to apply app_config verbosity_mode so it is effective
  for library/notebook flows while preserving CLI semantics and layering.
goals:
  - confirm authoritative post-dictConfig application points (init/reload/entry points)
  - compare alternatives A-D (and minimal combinations) against risks and conventions
  - define chosen behavior: default source, CLI precedence, reload handling, handler scope
  - identify required test updates/additions and any API impact
files:
  - path: src/mko_get_mediascope_data/app.py
    targets:
      - type: function
        name: configure_logging_level
      - type: function
        name: run_report_async
      - type: function
        name: run
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: class
        name: AppService
      - type: method
        name: prepare_app
      - type: method
        name: reload_config
      - type: function
        name: get_app_service
  - path: src/mko_get_mediascope_data/core/models.py
    targets:
      - type: enum
        name: VerbosityMode
acceptance_criteria:
  - single recommended approach documented with rationale and rejected/conditional alternatives
  - reload, singleton, and CLI precedence behavior explicitly specified
  - downstream implementation and test tasks unblocked with concrete guidance
```

```yaml
id: P02-B1-T2
title: Validate Block 1 design against risks and project conventions
priority: high
depends_on: [P02-B1-T1]
description: >
  Independent review of the proposed verbosity design for correctness, compatibility,
  and alignment with layering/config lifecycle rules.
goals:
  - verify no core->app dependency introduced
  - verify post-dictConfig ordering and reload semantics are sound
  - confirm minimal scope and public API compatibility
acceptance_criteria:
  - design approved or specific required adjustments recorded
  - residual risks and mitigations listed
```

### Block 2 — Implement shared verbosity application (core) and wire entry points
- **Goal:** Apply the Block 1 decision so `verbosity_mode` from config takes effect in library/notebook usage; keep CLI working with correct precedence.
- **Dependencies:** Block 1 (design decision approved).
- **Required agents:** **Planner** (detailed change design from approved decision), **Implementor** (execution). **Auditor/Researcher** if unresolved; **Validator** if residual risk high.

```yaml
id: P02-B2-T1
title: Implement core verbosity application and integrate call sites
priority: high
depends_on: [P02-B1-T2]
description: >
  Implement the approved design: centralize verbosity application in core and
  ensure library/notebook and CLI paths apply it in correct order with correct precedence.
goals:
  - configured verbosity_mode affects library/notebook logging as requested
  - no core -> app dependency
  - CLI flag precedence and existing run behavior preserved
  - reload/init ordering consistent with dictConfig lifecycle
acceptance_criteria:
  - library/notebook usage reflects app_config verbosity_mode
  - CLI run still honors explicit --verbosity over config when provided
  - verbosity remains effective after relevant lifecycle events per decision (e.g. reload)
  - no regressions in existing verbosity-related behavior covered by current tests
```

### Block 3 — Tests (fix existing, add library/notebook coverage)
- **Goal:** Lock in requested behavior and prevent regressions.
- **Dependencies:** Block 2.
- **Required agents:** **Implementor**.

```yaml
id: P02-B3-T1
title: Fix and extend tests for verbosity in library/notebook usage
priority: high
depends_on: [P02-B2-T1]
description: >
  Adjust existing verbosity tests if needed and add coverage ensuring
  app_config.json5 verbosity_mode is honored in library/notebook flows.
acceptance_criteria:
  - new/updated tests fail before fix (conceptually) and pass after
  - existing relevant tests still pass
  - no reliance on real user config; uses tmp/hermetic fixtures
```

### Block 4 — Verification, regression check, and independent validation
- **Goal:** Confirm correctness, safety, and proportional verification for the change.
- **Dependencies:** Blocks 2 and 3.
- **Required agents:** **Validator** (+ Implementor for fixes if needed).

```yaml
id: P02-B4-T1
title: Verify and validate verbosity fix for library/notebook usage
priority: high
depends_on: [P02-B3-T1]
acceptance_criteria:
  - agreed test set passes in the project environment
  - validator sign-off or documented issues with required follow-ups
  - no undocumented behavior changes beyond intended fix
```

## 6. Dependencies and execution order
1. **B1 (P02-B1-T1 → P02-B1-T2)** — design resolution & validation. **Gate:** approved decision required before implementation.
2. **B2 (P02-B2-T1)** — implementation per approved design.
3. **B3 (P02-B3-T1)** — tests aligned to implementation; finalize after B2.
4. **B4 (P02-B4-T1)** — verification & independent validation.

**Critical path:** B1 → B2 → B3 → B4.

## 7. Step 2 (Product Owner clarification)
- **Skippable:** yes. No blocking business ambiguity identified.

## 8. Design decision (Block 1, approved with required adjustments)

- **Status:** **Approved (conditionally satisfied gate)** by Validator; proceed to Block 2 with adjustments below incorporated.
- **Chosen approach:** **A + shared core helper + thin CLI reuse** (minimal, convention-aligned).
  - Add module-level `apply_logging_verbosity(mode: VerbosityMode) -> None` in `src/mko_get_mediascope_data/core/services.py` (core, near `AppService` lifecycle; no CLI/Rich deps, no `core -> app`). Semantics identical to current `app.configure_logging_level`: `level = mode.to_logging_level()`, set root logger level and **every** `root.handlers[*]` level.
  - Invoke at **end of `AppService.prepare_app()` after `logging.config.dictConfig(...)`** with `self.app_config.app_settings.verbosity_mode`. Covers init, `get_app_service()`/singleton/library, `run_report_async`/`run_report`, and `reload_config()` (reruns `prepare_app`) -> configured verbosity reapplied on reload (reset-to-config).
- **CLI precedence:** explicit `--verbosity` overrides config. `app.run` keeps: resolve service, `mode = verbosity if verbosity is not None else app_service.app_config.app_settings.verbosity_mode`, apply `mode` after service resolution (idempotent when omitted). `app.configure_logging_level` becomes thin wrapper delegating to core helper (preserve name/signature). `app -> core` import allowed. Core must not know CLI flags.
- **Reload:** reapplies configured `verbosity_mode` on `reload_config()` via `prepare_app`.
- **Handler scope:** root logger + all root handlers (unchanged). Global stdlib logging side effect (process-wide/singleton).
- **`list_reports`:** inherits configured verbosity as side effect of service lifecycle (acceptable/consistent).
- **Public API impact:** **none** (no signature changes to `run_report`, `run_report_async`, `get_app_service`; `VerbosityMode` semantics unchanged).
- **Rejected/conditional:** B (entry points only) misses plain `get_app_service()`/reload; C (API override param) scope growth/unnecessary; D (in `get_app_service()` only) fragile vs reload/ordering.

### Validator-required adjustments (must be incorporated)
1. **`tests/test_verbosity.py::TestConfigureLoggingLevel::test_level_survives_dict_config` baseline:** hermetic `app_config` omits `verbosity_mode` => DEFAULT/WARNING, while `log_config` sets INFO, so `prepare_app()` will now auto-apply WARNING after `dictConfig`. Split/update intent: (a) service init applies configured verbosity after dictConfig (expect WARNING with default fixture or explicit mode proving override of INFO), and (b) explicit `configure_logging_level`/CLI-style override after init still effective.
2. **Add library/init coverage:** hermetic service with explicit `verbosity_mode` (e.g. DEBUG/VERBOSE/SILENT) asserts root + all root handlers reflect configured level after init/`get_app_service()` path.
3. **`run_report_async` library coverage:** mock/patch `AppService.run_report` (AsyncMock) or pipeline boundary to avoid real I/O/network; assert configured verbosity applied and call path exercised.
4. **Reload reapplies configured verbosity:** mutate root/handler levels, call `reload_config()` -> restored to configured mode (reset-to-config).
5. **CLI precedence/mapping tests remain valid:** omitted flag uses config-derived mode; explicit flag wins.
6. **Global logging isolation mandatory:** save/restore root level and handler levels/state in tests touching init/reload/library paths; avoid singleton/cross-test leakage.
7. **Optional:** small assertion that `list_reports` runs under configured verbosity (consistent side effect).

### Residual risks (Validator)
- Global/singleton side effects (process-wide/notebook kernel).
- Reload drops transient in-process override in favor of configured mode (intended).
- Only root + root handlers adjusted (unchanged).
- `run_report_async` test mock boundary must match real call path.
- Other tests depending on hermetic post-init INFO may be sensitive (review/update).

## 9. Block task descriptions (approved design)

### Block 2 — Implementation (`P02-B2-T1`)
```yaml
id: P02-B2-T1
title: Implement core verbosity helper and lifecycle/CLI integration
priority: high
depends_on: [P02-B1-T2]
source_reference: .ai/plans/02-verbosity-library-usage.md
description: >
  Add shared core verbosity application and invoke it after dictConfig in the
  AppService lifecycle so configured verbosity applies for library/notebook and
  singleton usage; make CLI configure_logging_level a thin wrapper while preserving
  explicit flag precedence in app.run.
goals:
  - centralize verbosity application in core (no core -> app)
  - apply configured app_config.app_settings.verbosity_mode after dictConfig on init and reload
  - preserve app.configure_logging_level name/signature and CLI run flag-over-config behavior
  - keep scope minimal; no public API or verbosity semantics changes
files:
  - path: src/mko_get_mediascope_data/core/services.py
    targets:
      - type: function
        name: apply_logging_verbosity
        note: "new module-level helper: def apply_logging_verbosity(mode: VerbosityMode) -> None"
      - type: method
        name: AppService.prepare_app
        note: "after logging.config.dictConfig(...) call apply_logging_verbosity(self.app_config.app_settings.verbosity_mode)"
      - type: method
        name: AppService.reload_config
        note: "no direct change expected; must continue to re-run prepare_app"
  - path: src/mko_get_mediascope_data/app.py
    targets:
      - type: function
        name: configure_logging_level
        note: "thin wrapper delegating to core.services.apply_logging_verbosity(mode); preserve signature"
      - type: function
        name: run
        note: "keep mode = verbosity if verbosity is not None else app_service.app_config.app_settings.verbosity_mode; apply(mode) after service resolution"
acceptance_criteria:
  - library/notebook/service-init/get_app_service/run_report paths reflect configured verbosity_mode
  - CLI explicit --verbosity overrides config; omitted uses configured mode
  - reload_config reapplies configured verbosity after dictConfig
  - root logger and all root handlers set; no core -> app dependency
```

### Block 3 — Tests (`P02-B3-T1`)
```yaml
id: P02-B3-T1
title: Update and extend verbosity tests for lifecycle/library usage
priority: high
depends_on: [P02-B2-T1]
source_reference: .ai/plans/02-verbosity-library-usage.md
description: >
  Adjust hermetic fixtures/tests for auto-applied configured verbosity and add
  coverage for service/library paths and reload reapplication. Isolate global
  logging state.
files:
  - path: tests/test_verbosity.py
    targets:
      - type: class
        name: TestConfigureLoggingLevel
        note: "update test_level_survives_dict_config for lifecycle auto-apply/default WARNING baseline"
      - type: class
        name: "new or existing appropriate class/module tests"
        note: "add library/init, run_report_async (mocked), reload reapplies coverage"
  - path: tests/conftest.py
    targets:
      - type: fixture
        name: hermetic_app_service
        note: "keep hermetic isolation; adjust only if required, prefer per-test explicit verbosity_mode"
acceptance_criteria:
  - configured verbosity proven for library/service-init and run_report_async (mocked pipeline)
  - reload reapplies configured verbosity
  - CLI precedence and mapping semantics preserved
  - global logging isolated; no real user config dependency
```

### Block 4 — Validation (`P02-B4-T1`)
- Focus: configured verbosity effective in library/notebook/service/run_report paths; CLI flag precedence/omitted behavior unchanged; reload reapplies; no `core -> app`; no unintended API/semantics changes; tests adequate and passing.

## 10. Notes
- Keep changes minimal and dependency-safe; prefer semantic targets over positional edits.
- Apply verbosity **after** `dictConfig`; avoid `core -> app`; preserve CLI flag-over-config precedence for `run`.

## Execution log
- [x] Step 0: Auditor code context gathered.
- [x] Step 1: Plan decomposed and saved (`.ai/plans/02-verbosity-library-usage.md`).
- [x] Step 2: PO clarification skipped (no business uncertainty).
- [x] Block 1: design resolution & validation (Auditor/Researcher/Planner/Validator). Design approved with required adjustments.
- [ ] Block 2: implementation (P02-B2-T1).
- [ ] Block 3: tests (P02-B3-T1).
- [ ] Block 4: final validation (P02-B4-T1).
- [ ] Step 4: documentation (if needed).
- [ ] Step 5: final validation.