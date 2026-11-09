# Execution Plan: Jupyter Notebook Launch (Problem 01 / Spec 02)

**Plan file:** `.ai/plans/01-jupyter-notebook-launch.md`  
**Source spec:** `.ai/problems/02_jupyter_notebook_launch_spec.md`  
**Planning input:** `{code_context}` from `Auditor` (read-only inspection, 2026-11-09; no code changes made)

## 0. `{code_context}` summary (Auditor)

- **Verdict:** ready with caveats for notebook execution using the existing async API; **not** a first-class public portable-workspace notebook API today.
- **Already implemented:** `app.run_report_async(report_path, progress=None)` (active-loop path), `app.run_report(...)` running-loop guard with actionable `RuntimeError`, PEP 562 lazy exports in package `__init__`, `initialize_settings()`/`init_project()`, `core.progress.ProgressReporter` + `NullProgressReporter`, DI-capable `AppPaths`/`PathResolver`/`AppService`/`Report`/`TVMediaReport`.
- **Key constraints (CR-3):** settings/config workspace root, `init`, and `list-reports` are user-dir fixed in public behavior (`platformdirs`/`APP_PATHS` singleton). `run*` accepts an explicit report file path; export root resolves from `app_config.app_settings.export_folder` via `PathResolver`; per-report output uses `ReportSettings.relative_path`. A portable workspace is feasible only via internal DI or new public surface (out of Q8=A redesign).
- **Notebook safety:** `AppService.prepare_app()` applies global `logging.config.dictConfig`; singleton `_app_service` caches config across kernel reuse; `RichProgressReporter` is terminal-oriented — notebooks should use `NullProgressReporter` or notebook-safe behavior.
- **Packaging:** `pip install git+...` documented; `pyproject.toml` has no Jupyter extras / no `nest_asyncio`. No repo-shipped `.ipynb` exists.
- **Evidence:** `app.py`, `__init__.py`, `core/paths.py`, `core/services.py`, `core/init_service.py`, `core/progress.py`, `core/models.py`, `README.md`, `docs/00-overview/current-behavior.md`, `tests/test_app.py`, `tests/test_progress.py`, `tests/test_init_credentials.py`.

## 0.1 B1 decision log (resolved 2026-11-09, PO + Tech Lead)

| Item | Decision |
|---|---|
| OQ-1 notebook location | **Top-level `examples/` folder** (discoverable, maintainable). Notebook file: `examples/mediascope_notebook_workflow.ipynb`. No CI execution implied. |
| OQ-2 example report (CR-5) | Use **`nat_tv_brands_theiss.json5`** from the user's existing Mediascope settings workspace. Notebook must reference the report path via a configurable variable; default example points to the platform/user config location (Windows example provided by PO: `%LOCALAPPDATA%\mko_get_mediascope_data\mko_get_mediascope_data\settings\reports\nat_tv_brands_theiss.json5`) but notebook must prefer portable resolution (`platformdirs`/`Path`) and allow override. No secrets committed. |
| OQ-3 CR-3 mechanism | **Alternative A — internal DI workspace bootstrap (caveated, current architecture).** Notebook cell variables control output/export, reports source path, and settings/config workspace. For a portable workspace, notebook creates/validates a workspace-local settings tree (from bundled package templates or user-provided source) and constructs `core.paths.AppPaths(user_dir=<workspace_root>, app_dir=<package APP_DIR>, app_name=<APP_NAME>)` + `core.paths.PathResolver(<workspace_root>)` + `core.services.AppService(app_paths,resolver)`. Report runs via `await service.run_report(<report_path>, progress=NullProgressReporter())`. **Caveats (must document):** uses internal modules/classes (not a stable public notebook contract), `AppService.prepare_app()` applies global `logging.config.dictConfig`, bypasses public singleton wrappers (`init`/`list-reports` remain user-dir fixed), requires a complete valid settings tree, and changing files in a long-lived kernel may require restart. Public `run_report*`/`initialize_settings` remain unchanged. No production code changes planned unless B3 validation finds a blocking defect. |
| OQ-4 sanitized evidence | **Option A.** Markdown evidence note at `.ai/evidence/01-jupyter-notebook-e2e.md`: selected Git ref, run date/environment (local JupyterLab/classic), non-sensitive status, configured export folder path, output file listing/count. **No** credentials/tokens/auth URLs/raw API payloads. |
| CR-5 if creds/network unavailable | **Option A.** Deliver notebook + docs + contract verification; record B5 as **blocked** with explicit reason and no success claim. Notebook must reference credentials location as a user-managed file outside notebook content (Windows example: `%LOCALAPPDATA%\mko_get_mediascope_data\mko_get_mediascope_data\settings\connections\mediascope.json`) while using portable path variables; never embed secrets. |

## 1. Goal

Deliver confirmed requirements CR-1–CR-8 from the spec: evidence-grounded readiness verdict/caveats, a runnable local JupyterLab/classic Jupyter notebook in the repository, Git-based install flow with selectable ref and pinned default, notebook-cell folder variables covering portable workspace scope (output/export, reports source path, settings/config workspace), credentials kept in a user-managed config file outside notebook content, and a documented/sanitized real-credential E2E acceptance path.

## 2. Constraints and non-goals

- Q8=A: deliver within the current architecture; document caveats. No async-pipeline/vendor/retry/report-semantic redesign.
- No new auth mechanisms, secret stores, or secrets persisted in notebook artifacts.
- CR-3 requires notebook cell variables for **output/export, reports source path, and settings/config workspace**. Public singleton API does not fully satisfy settings/config workspace; the CR-3 mechanism must be explicit and caveated.
- `logging.config.dictConfig` is global; singleton caching affects long-lived kernels.
- Real-credential E2E acceptance is environment-specific; retain only sanitized evidence.
- **Never change existing audit phase files.**

## 3. Dependency graph / execution order

```text
B1 (resolve OQ-1..OQ-4, CR-3 mechanism, evidence policy) — gate
  ├─> B2 (readiness/caveat contract + doc alignment)
  ├─> B3 (notebook artifact implementation; high risk)
  │     └─> B4 (README/docs discoverability; after B2+B3)
  └─> B5 (sanitized real-credential acceptance evidence; manual/authorized env)
```

## 4. Execution blocks

### B1 — Resolve planning decisions and safety gates (OQ-1..OQ-4, CR-3 mechanism)
- **Depends on:** none.
- **Risk:** medium coordination; wrong CR-3 choice causes rework or unsafe UX.
- **Scope:** planning/decision record only; no production code.
- **Decisions:**
  - OQ-1 notebook location (discoverable, maintainable, not implied CI-executed).
  - OQ-2 example report/settings (no secrets, minimize data volume/cost).
  - OQ-3 CR-3 mechanism — **not chosen here** (see B3 alternatives).
  - OQ-4 sanitized evidence policy (no credentials/tokens/auth URLs/raw sensitive payloads).

| Agent | Required | Reason |
|---|---:|---|
| Planner | yes | decision framing and acceptance mapping |
| Auditor | no | code context already available |
| Researcher | conditional | only if new external Jupyter/packaging constraint appears |
| Validator | recommended | if selected CR-3 mechanism is high risk |
| Tech Lead/PO delegate (human gate) | yes | unresolved business/acceptance choices |

### B2 — Readiness verdict and notebook caveat contract
- **Depends on:** B1.
- **Risk:** low-medium doc drift / overstated support.
- **Scope (semantic):** `README.md` notebook/install/public API guidance; `docs/00-overview/current-behavior.md` notebook/async/config/logging/progress caveats; notebook markdown instructions.

| Agent | Required | Reason |
|---|---:|---|
| Planner | yes | caveat/contract checklist tied to CR-1/CR-8 |
| Auditor | conditional | if implementation contradicts audited behavior |
| Researcher | no | not needed unless new practices required |
| Validator | recommended | check docs do not claim unsupported public workspace API |

### B3 — Working notebook deliverable (HIGH RISK; CR-3 technical uncertainty)
- **Depends on:** B1, B2 contract.
- **Risk:** **high** — secret leakage, wrong folder mapping, global logging side effects, singleton stale state, external API/environment failures, CR-3 mechanism fragility.
- **Scope (semantic):** repo-shipped `.ipynb` at OQ-1 path; targeted production-code changes **only if B1 explicitly selects Alternative B** and they remain minimal/compatible.

**CR-3/OQ-3 alternatives (evaluated; final choice recorded in B1 — Alternative A selected):**

| Alternative | Summary | Pros | Cons/caveats | Code change |
|---|---|---|---|---|
| A. Internal DI workspace bootstrap | Notebook constructs `AppPaths(user_dir=workspace_root, app_dir=APP_DIR, app_name=APP_NAME)`, `PathResolver(workspace_root)`, `AppService(...)`; validates complete settings tree; uses explicit report/output paths and `NullProgressReporter`; awaits `service.run_report(...)`. | Satisfies portable workspace intent with current architecture. | Uses internal modules; global `dictConfig`; bypasses public singleton wrappers (`init`/`list-reports` remain user-dir fixed); needs full settings tree. | none expected |
| B. Minimal supported workspace API/CLI | Add explicit supported workspace-root/paths override while preserving defaults. | Cleaner public contract. | Touches public/runtime surface; needs compatibility tests; broader than pure docs/notebook. | yes (targeted) |
| C. Public/user-dir flow + overrides | Use `initialize_settings`/public config and supported report/output knobs. | Lowest core risk. | Likely insufficient for true settings/config workspace (CR-3/Q4=B). | no |
| D. Subprocess/CLI orchestration | Shell out to CLI from notebook. | Avoids in-process singleton complexity. | Blocked by current CLI fixed workspace/init/list-reports behavior; awkward ergonomics. | possibly |

**B3 starts only after B1 records the selected mechanism.** B1 selected **Alternative A**; document internal-API caveats clearly. No production code changes planned.

#### B3 design — selected Alt A (implementor-ready, validator-updated)

**Status:** design ready for implementation (post-B1, post-Validator PASS WITH REQUIRED CHANGES). **Mechanism:** OQ-3 Alternative A. **Production code:** no changes expected.

| Item | Design |
|---|---|
| Deliverable | `examples/mediascope_notebook_workflow.ipynb` (nbformat 4+, JupyterLab/classic). Clear outputs before commit. |
| Git install | `%pip install git+${REPO_URL}@${GIT_REF}` with `REPO_URL` default `https://github.com/manicko/mko_get_mediascope_data.git`; `GIT_REF` default **pinned immutable ref** (tag or commit SHA set at delivery; current repo has no tags so use delivery commit SHA) and selectable. Provide commented `main`/branch override (non-default). Echo `REPO_URL@GIT_REF` and installed package version. Add kernel-restart note after install/dependency changes. No `nest_asyncio`. |
| CR-3 folder variables (cell-driven) | `WORKSPACE_ROOT`, `SETTINGS_WORKSPACE` (= config workspace), `REPORTS_DIR` (= reports source path, workspace-local after bootstrap), `REPORT_FILENAME`/`REPORT_PATH` (default workspace-local `nat_tv_brands_theiss.json5`, OQ-2; override may point to user config/external path), `EXPORT_DIR` (= output/export). Portable defaults via `pathlib.Path` + `platformdirs`; Windows example paths only in markdown/comments. |
| Bundled template source | `PACKAGE_APP_DIR = core.paths.APP_DIR`; `BUNDLED_SETTINGS_DIR = PACKAGE_APP_DIR/settings`. Do **not** call `init_project`/`initialize_settings` to target workspace. |
| Workspace bootstrap | Create `SETTINGS_WORKSPACE`/subdirs. Copy required templates into `SETTINGS_WORKSPACE`: `app_config.json5`, `log_config.json5`, `defaults/**` (incl. `tv/crosstab.json5`), selected/all bundled reports. **Explicitly exclude `connections/mediascope.json` and any credential files from blanket template copy** (bundled file is placeholder). Non-destructive default (`OVERWRITE_WORKSPACE_TEMPLATES=False`). Update workspace `app_config.json5` `export_folder` to **absolute `EXPORT_DIR`** preserving other keys. **Bootstrap/mapping must happen before `AppService` construction** (C6 before C7). Ensure complete settings tree for construction+run. Document that `prepare_app()` resolves relative log-handler filenames against resolver base and may create workspace log dirs. |
| Credentials (CR-7) | `CREDENTIALS_PATH` defaults to portable user config `.../settings/connections/mediascope.json`; external/user-managed, never embedded/printed. Workspace `SETTINGS_WORKSPACE/connections/mediascope.json`: create `connections/`; if target exists keep (never overwrite); else copy only when `COPY_CREDENTIALS_INTO_WORKSPACE=True` and external source exists (explicit opt-in). Never overwrite existing workspace creds; never silently promote bundled placeholder to usable creds. Preflight checks existence and performs a non-secret placeholder/shape heuristic (no secret values printed); real E2E requires valid external creds. Note `get_connection` resolves from workspace `AppPaths.connection_config` and its missing-file message mentions `init` (generic) — Alt A remedy is workspace bootstrap/explicit credential copy. |
| DI run (Alt A) | `app_paths = AppPaths(user_dir=WORKSPACE_ROOT, app_dir=PACKAGE_APP_DIR, app_name=APP_NAME)`; `resolver = PathResolver(WORKSPACE_ROOT)` (`export_folder` absolute to avoid ambiguity); `service = AppService(app_paths,resolver)`; `export_paths = await service.run_report(Path(REPORT_PATH), progress=NullProgressReporter())`. Uses internal modules (not stable public notebook API). Construction applies global `logging.config.dictConfig()`; kernel restart may help. Bypasses singleton/public wrappers; public `run_report*`/`initialize_settings`/CLI `init`/`list-reports` remain user-dir/singleton. |
| Preflight / verification | Preflight checks existence/shape only — no secret/payload printing. Distinguish states: real E2E vs **B5 blocked** (missing creds/network/external API unavailable) with explicit reason and **no success claim**. Verification lists/counts export files under `EXPORT_DIR`/returned paths with `relative_path` awareness; `run_report` returns export target paths and entries may be skipped (`NoReportFoundError` warned) — do not claim success from path existence alone. |
| Evidence (OQ-4) | Notebook includes sanitized template for `.ai/evidence/01-jupyter-notebook-e2e.md`: pinned Git ref, date/env (local JupyterLab/classic), package version, non-sensitive status (success/**blocked**+reason), configured paths, output listing/count/sizes; exclude creds/tokens/usernames/passwords/client secrets/auth secrets/raw API payloads. Clear outputs before commit/sharing. |
| Suggested cells | C1 markdown caveats/safety; C2 install; C3 imports/version; C4 config variables; C5 preflight (+blocked state); C6 workspace bootstrap; C7 DI service; C8 `await service.run_report`; C9 output verification; C10 evidence template/guardrails. |
| Validation | Static: `.ipynb` JSON parse, nbformat present, no secret literals, `NullProgressReporter` used, `service.run_report` awaited, no `nest_asyncio`/no `run_report_async` singleton wrapper in Alt A run path. Manual/authorized: kernel run-through and CR-5 E2E only with real external creds/network. Full pytest expected green (notebook-only change). |

| Agent | Required | Reason |
|---|---:|---|
| Auditor | yes | verify path semantics, `prepare_app`/`dictConfig`, defaults/connections/export resolution, touchpoints |
| Researcher | yes | Jupyter async/logging/progress best practices, reproducible Git install, secret-safe notebook patterns |
| Planner | yes | detailed cell structure, variable schema, preflight, output verification, sanitized evidence, test/doc impact |
| Validator | yes | independent review against CR-1–CR-8, especially CR-3/CR-7 and caveats |
| Implementor | yes (after validation gate) | deliver notebook (+ minimal compatible code only if Alternative B selected) |

### B4 — Documentation and discoverability updates
- **Depends on:** B2, B3.
- **Risk:** low-medium doc drift.
- **Scope (semantic):** `README.md`, `docs/00-overview/current-behavior.md`, notebook-adjacent instructions.

| Agent | Required |
|---|---:|
| Planner | yes |
| Validator | recommended |
| Auditor/Researcher | conditional |

### B5 — Sanitized real-credential acceptance evidence (CR-2/CR-5)
- **Depends on:** B1 (OQ-2/OQ-4), B3; authorized local Jupyter environment + valid Mediascope credentials.
- **Risk:** high operational/environmental (external API). Not a CI gate.
- **Scope:** sanitized evidence only (output listing/paths/status; no secrets/tokens/raw payloads).

| Agent | Required |
|---|---:|
| Planner | yes (evidence template/redaction rules) |
| Implementor/operator | yes (authorized execution) |
| Validator | yes (redaction/CR-5 coverage check) |

## 5. Implementable task breakdown (template-aligned, conceptual)

### T-01 — Planning decision record (B1)
```yaml
id: T-01_jupyter_notebook_planning_decisions
title: Record OQ-1..OQ-4 and CR-3 mechanism decision
priority: high
depends_on: []
source_reference: .ai/problems/02_jupyter_notebook_launch_spec.md
source_section: "6. Product Owner decisions; 11. Open questions"
description: >
  Capture final decisions for notebook location, example report/settings,
  CR-3 portable-workspace mechanism, and sanitized acceptance evidence policy.
  Do not change production code.
goals:
  - make B3 implementation unambiguous
  - preserve CR-3/CR-7/CR-8 constraints
  - do not choose Alternative B without explicit authorization and compatibility plan
files:
  - path: .ai/plans/01-jupyter-notebook-launch.md
    targets:
      - type: document
        name: Decision log / selected CR-3 mechanism
acceptance_criteria:
  - OQ-1..OQ-4 resolved or explicitly assigned with blocking status
  - CR-3 mechanism selected with rationale, caveats, and compatibility impact
  - no secrets or credentials recorded
agent_requirements:
  planner: required
  validator: recommended_if_high_risk_mechanism
```

### T-02 — Notebook contract and caveats (B2)
```yaml
id: T-02_jupyter_notebook_readiness_contract
title: Document readiness verdict, supported notebook path, and caveats
priority: high
depends_on: [T-01_jupyter_notebook_planning_decisions]
source_reference: .ai/problems/02_jupyter_notebook_launch_spec.md
source_section: "CR-1, CR-8, T1/T6"
description: >
  Update docs/notebook guidance to state readiness verdict, active-loop
  execution path (await run_report_async), notebook-safe progress/logging,
  singleton/kernel-reuse caveats, credential safety rules, and selected
  CR-3 folder-mapping mechanism.
goals:
  - satisfy CR-1/CR-8 in documentation
  - avoid claims of unsupported public workspace configurability
files:
  - path: README.md
    targets:
      - type: section
        name: notebook usage guidance
  - path: docs/00-overview/current-behavior.md
    targets:
      - type: section
        name: notebook/async/config/progress/logging caveats
  - path: "<OQ-1 notebook path>/README or notebook markdown"
    targets:
      - type: document
        name: prerequisites and run order
acceptance_criteria:
  - supported path uses await run_report_async for active loops
  - credentials outside notebook content
  - dictConfig/singleton/kernel-reuse caveats documented
  - CR-3 mapping explicit for selected mechanism
agent_requirements:
  planner: required
  validator: recommended
```

### T-03 — Implement notebook artifact (B3)
```yaml
id: T-03_jupyter_notebook_artifact
title: Add runnable local Jupyter notebook for Mediascope workflow
priority: high
depends_on: [T-01_jupyter_notebook_planning_decisions, T-02_jupyter_notebook_readiness_contract]
source_reference: .ai/problems/02_jupyter_notebook_launch_spec.md
source_section: "CR-2..CR-7, T2..T5"
description: >
  Create the repo-shipped notebook implementing the selected CR-3 mechanism.
  Include Git install cell with selectable ref and pinned default; cell
  variables for output/export, reports source path, and settings/config
  workspace; credential preflight pointing to user-managed config outside
  notebook; active-loop-safe execution; notebook-safe progress; output
  verification; sanitized status output only.
goals:
  - runnable in local JupyterLab/classic Jupyter
  - satisfies CR-2..CR-7 under Q8=A
  - no secrets embedded in notebook source/outputs
files:
  - path: "<OQ-1 notebook path>/<name>.ipynb"
    targets:
      - type: notebook
        name: example Mediascope notebook workflow
  # Conditional only if T-01 explicitly selects minimal Alternative B:
  # - path: src/mko_get_mediascope_data/app.py
  # - path: src/mko_get_mediascope_data/core/services.py
  # - path: src/mko_get_mediascope_data/core/paths.py
  # - path: src/mko_get_mediascope_data/core/init_service.py
  # - path: tests/test_app.py
  # - path: tests/test_paths.py
acceptance_criteria:
  - Git install uses selectable ref with pinned default
  - folder variables map to output/export, reports source path, settings/config workspace
  - execution is active-loop safe (await run_report_async or approved equivalent)
  - no credentials/secrets/tokens in notebook source
  - output verification checks configured export location
  - if production code changed, existing behavior preserved and tests pass
tests_to_run_if_code_changed:
  - uv run pytest tests/test_app.py
  - uv run pytest tests/test_progress.py
  - uv run pytest tests/test_init_credentials.py
  - uv run pytest
agent_requirements:
  high_risk: true
  auditor: required
  researcher: required
  planner: required
  validator: required
  implementor: required_after_validation
```

### T-04 — Discoverability and doc finalization (B4)
```yaml
id: T-04_jupyter_notebook_docs_discoverability
title: Link notebook workflow from README/docs and finalize guidance
priority: medium
depends_on: [T-03_jupyter_notebook_artifact]
source_reference: .ai/problems/02_jupyter_notebook_launch_spec.md
source_section: "CR-1, CR-8, T6"
files:
  - path: README.md
    targets:
      - type: section
        name: notebook/example workflow link
  - path: docs/00-overview/current-behavior.md
    targets:
      - type: section
        name: notebook workflow alignment
acceptance_criteria:
  - notebook discoverable from README/docs
  - guidance matches delivered mechanism and caveats
agent_requirements:
  planner: required
  validator: recommended
```

### T-05 — Sanitized real-credential acceptance evidence (B5)
```yaml
id: T-05_jupyter_notebook_acceptance_evidence
title: Capture sanitized real-credential E2E notebook evidence
priority: medium
depends_on: [T-03_jupyter_notebook_artifact, T-01_jupyter_notebook_planning_decisions]
source_reference: .ai/problems/02_jupyter_notebook_launch_spec.md
source_section: "CR-5, OQ-4, 10"
description: >
  Execute the notebook in an authorized local Jupyter environment with real
  credentials configured outside notebook content. Record sanitized evidence of
  install/config/run/output verification. No credentials, tokens, auth material,
  or sensitive API payloads.
files:
  - path: "<OQ-4 evidence path>"
    targets:
      - type: document
        name: sanitized acceptance evidence
acceptance_criteria:
  - evidence shows install -> configure -> run actual report -> output confirmed
  - evidence contains no secrets/tokens/raw sensitive payloads
agent_requirements:
  planner: required_for_template
  validator: required_for_redaction_check
  implementor: required_for_authorized_execution
```

## 6. Verification matrix (minimum)

| Area | Verification | When |
|---|---|---|
| Planning/spec alignment | CR-1–CR-8 mapping, OQ-1–OQ-4 recorded | B1/T-01 |
| Docs accuracy | Claims match `app.run_report*`, `core.paths`, `core.services`, progress/logging behavior | B2/B4 |
| Notebook safety | Inspect notebook source/outputs for secrets; ensure active-loop-safe execution | B3 |
| CR-3 mechanism | Manual walkthrough: cell variables → resolved paths/objects/export verification | B3 |
| Regression (if code changed) | targeted tests + `uv run pytest`/CI parity | B3 |
| Real E2E | Authorized local run with real credentials; sanitized evidence only | B5 |

## 7. Definition of done

- [ ] B1/T-01 decisions recorded (OQ-1–OQ-4, CR-3 mechanism).
- [ ] B2/T-02 readiness verdict/caveats documented and consistent with implementation.
- [ ] B3/T-03 notebook delivered, runnable in local JupyterLab/classic Jupyter, secret-safe, with output verification.
- [ ] B4/T-04 README/docs discoverability updated.
- [ ] B5/T-05 sanitized real-credential E2E evidence captured or explicit blocked reason recorded (no false success claim).
- [ ] No audit phase files changed; no unrelated production redesign.
