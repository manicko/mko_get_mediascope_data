# Analytical Specification: Jupyter Notebook Launch (Problem 01)

## 1. Problem statement

Users need to run `mko_get_mediascope_data` from Jupyter notebooks. The current package is primarily used as a CLI and as a Python library from scripts/Airflow, with configuration rooted in a platform-specific user configuration directory (`platformdirs`). The business request is to (1) assess module readiness for notebook launch and (2) deliver a working notebook with configurable folders, where the package is installed directly from Git.

Source request: `.ai/problems/Problem_01.md`.

## 2. Business goal

Enable an analyst/operator to execute a Mediascope data-loading workflow in a local notebook environment with operational confidence, using a reproducible Git-based package install and user-controlled workspace folders, while keeping credentials outside notebook content.

## 3. Scope

### In scope
- Readiness assessment for running the module from Jupyter notebooks (local JupyterLab / classic Jupyter Notebook), based on current architecture and verified behavior.
- A repository-shipped example notebook (checked into this repo) that demonstrates an end-to-end workflow with real credentials:
  - install package from Git (user-selectable ref, default pinned for reproducibility),
  - configure folders/credentials,
  - run an actual Mediascope report,
  - confirm output files in the configured output folder.
- Configurable folders covering a portable workspace: output/export, reports source path, and settings/config workspace (where app config, credentials, and defaults are read).
- Notebook UX where users set folders via variables/parameters in notebook cells.
- Credentials handled via a user-managed config file outside notebook cells/content (no secrets in cells).
- Documented caveats and a working notebook delivered within the current architecture (no architecture redesign required as part of this specification’s delivery target).

### Out of scope
- Support guarantees beyond the primary local environments (VS Code notebooks, Google Colab) except where existing behavior incidentally works; Colab-specific secret/filesystem integration is not required.
- New authentication mechanisms, secret stores, or credential prompts that persist secrets in notebook artifacts.
- Redesign of the async pipeline, vendor API client, retry/concurrency model, or report semantics.
- Automatic migration of arbitrary hand-authored report formats beyond existing documented behavior.
- CI execution of real-credential end-to-end notebook runs against Mediascope (acceptance evidence is defined separately below).
- Adding unrelated tooling (Docker, DB, web framework).

## 4. Confirmed requirements

| ID | Requirement | Acceptance intent |
|---|---|---|
| CR-1 | The module must be assessed for readiness to run via Jupyter notebooks. | Produce a clear readiness verdict (ready / ready with caveats / not ready) grounded in evidence, including supported execution path and known caveats. |
| CR-2 | Deliver a working notebook. | A runnable notebook exists in the repository and can be executed by a user in local JupyterLab/classic Jupyter. |
| CR-3 | Notebook supports configurable folders. | User can control output/export, reports source path, and settings/config workspace through notebook cell parameters/variables. |
| CR-4 | Package is taken directly from Git in the notebook flow. | Notebook install step uses a Git source; ref is user-selectable with a pinned default (tag/commit) and an option for latest default branch. |
| CR-5 | End-to-end real-credential workflow. | Notebook demonstrates install → configure folders/credentials → run an actual Mediascope report → confirm output files in the configured output folder. |
| CR-6 | Target environment is local JupyterLab/classic Jupyter (primary). | Instructions and notebook behavior are validated for that baseline. |
| CR-7 | Credentials remain outside notebook content. | Credentials live in `connections/mediascope.json` (or equivalent user-managed config file) pointed to/configured by the notebook; no secrets in cells. |
| CR-8 | Deliver within current architecture; document caveats. | Notebook works with current public/internal capabilities as applicable to the agreed contract; caveats are explicit. |

## 5. Conceptual development tasks

| Task | Purpose | Expected outcome | Dependencies |
|---|---|---|---|
| T1. Notebook-readiness assessment | Establish what “ready for notebook launch” means for current behavior. | Evidence-based readiness section: supported API path for active event loops, config/bootstrap prerequisites, logging/progress behavior, singleton/lifecycle caveats, and acceptance checks. | Problem request; research evidence. |
| T2. Notebook execution contract | Define the supported user workflow from install to verified output. | Stepwise contract: Git install (selectable ref, pinned default), workspace/folder variables, credential file location, report selection, run command/path, output verification. | T1, PO decisions. |
| T3. Configurable-folders model | Make folder scope unambiguous and portable. | Definition of each configurable location (settings/config workspace, reports source path, output/export), defaults, expected precedence/relationships, and how cell variables map to them. | PO decisions Q4–Q5. |
| T4. Credentials & safety behavior | Prevent secret leakage while enabling real execution. | User-facing rules: credentials file outside notebook, no secrets in cells/committed artifacts, notebook points to configured location; handling when credentials are missing/invalid is described. | T3, PO decision Q7. |
| T5. Working notebook deliverable | Provide the runnable artifact. | Repo-shipped notebook implementing T2–T4 for local JupyterLab/classic Jupyter, with clear placeholders/instructions and output verification cells. | T2–T4. |
| T6. Documentation & verification evidence | Make the workflow discoverable and auditable. | README/docs/notebook guidance updated as needed; caveats documented; acceptance evidence captured (what was run, where outputs landed) without exposing credentials. | T1–T5. |

## 6. Product Owner decisions (recorded)

| Question | Decision |
|---|---|
| Q1. Notebook deliverable form | **A.** Repository-shipped example notebook (checked into this repo), for local JupyterLab/classic Jupyter. |
| Q2. “Working” acceptance level | **A.** End-to-end with real credentials: install from Git → configure folders/credentials → run an actual Mediascope report → confirm output files in the configured output folder. |
| Q3. Target notebook environments | **A.** Local JupyterLab / classic Jupyter Notebook (primary). |
| Q4. Configurable folders scope | **B.** Output/export + reports source path + settings/config workspace (where app config/credentials/defaults are read) — portable workspace. |
| Q5. Configuration UX | **B.** Set variables/parameters in notebook cells (paths assigned in code/inputs) that drive the run. |
| Q6. Install-from-Git policy | **C.** User-selectable ref shown in the notebook; default pinned (`git+...@tag` or commit) for reproducibility, with option for latest default branch. |
| Q7. Credentials handling | **A.** Credentials stay in `connections/mediascope.json` (or equivalent user-managed config file) outside the notebook; notebook points to/configures location; no secrets in cells. |
| Q8. Readiness gaps priority | **A.** Document caveats and deliver a working notebook within current architecture. |

## 7. Research summary (evidence-based)

Research was performed read-only against the current codebase and docs. Key sources: `src/mko_get_mediascope_data/app.py`, `__init__.py`, `core/services.py`, `core/network.py`, `core/paths.py`, `core/init_service.py`, `core/progress.py`, `core/models.py`, `core/reports.py`, `core/utils.py`, `pyproject.toml`, `README.md`, `docs/00-overview/current-behavior.md`, `tests/test_app.py`, `tests/test_progress.py`. No `.ipynb` files or `nest_asyncio` usage were found in the repository.

### 7.1 Notebook execution readiness (facts)
- **Async-first notebook path exists:** `run_report_async(report_path, progress=None)` (`app.py`) is the documented API for Jupyter/IPython/active event-loop hosts; README shows `await run_report_async(...)`. `run_report` is a sync wrapper that probes `asyncio.get_running_loop()` and raises `RuntimeError` with guidance to use `run_report_async` when a loop is running.
- **Public API:** `__init__` lazily re-exports `run_report`, `run_report_async`, `initialize_settings` (PEP 562); `initialize_settings(force=False)` wraps `init_project`. Tests cover running-loop contract and lazy exports.
- **Pipeline:** `AppService.run_report` builds per-entry config, creates `NetworkClient`, uses strategies/reports, and writes outputs. `NetworkClient` uses `asyncio.Semaphore`, retries/backoff/jitter, and `asyncio.to_thread`/timeout handling — compatible with awaiting inside a caller event loop.
- **Progress/logging:** Library API defaults to `NullProgressReporter` (no Rich UI). `RichProgressReporter` (CLI) uses `rich.live.Live` and handler stream manipulation suited to terminals. `AppService` initialization applies `logging.config.dictConfig` (global logging side effect); CLI additionally overrides levels after service creation.
- **Config/paths:** `core/paths.py` resolves user config via `platformdirs.user_config_dir(APP_NAME)`; global `APP_PATHS` points to user settings tree (`settings/app_config.json5`, `log_config.json5`, `defaults/`, `reports/`, `connections/mediascope.json`). `init_project` copies bundled templates into the user settings dir (atomic copy, `--force` overwrites same-name files).

### 7.2 Packaging and folder control (facts)
- **Install surface:** `pip install git+https://github.com/manicko/mko_get_mediascope_data.git` is documented; package has runtime deps including `json5`, `mediascope-api-lib`, `pandas`, `platformdirs`, `pydantic`, `rich`, `typer`; no notebook extra/Jupyter deps. Console script and module entry exist.
- **Existing folder knobs:** report file path is an explicit argument to `run`/`run_report(_async)`. `app_config.json5.export_folder` controls export root (resolved relative to resolver base/user dir per current behavior; absolute supported). `ReportSettings.relative_path` controls per-report output subfolder. `list-reports` lists files under the fixed user-settings `reports` directory.
- **Limitation relevant to CR-3 (portable workspace):** settings/config root is not overridable via public CLI/env/public API; public wrappers use a process-wide singleton built from `APP_PATHS`. `AppService(app_paths, resolver)` and `AppPaths`/`PathResolver` are constructible (dependency-injectable), which makes a workspace-local service technically feasible, but this is an internal/core-level pattern rather than a first-class public notebook API. `init_project` currently targets `APP_PATHS.user_settings_dir`.

### 7.3 Feasible approaches (research synthesis)
- **Best-practice direction:** prefer native `await run_report_async(...)` in notebooks; avoid adding `nest_asyncio` as a core/default dependency; keep progress optional and terminal Rich UI out of notebook defaults; treat logging/global config and singleton lifecycle as notebook caveats.
- **Approaches considered for configurable folders:**
  1. **Workspace-local settings with custom `AppService`/`AppPaths`/`PathResolver` (DI).** Strongest fit for portable workspace control; uses internal/core objects and requires a complete valid settings tree. *Relevant to Q4/Q5/Q8.*
  2. **Default user-config dir via public API + configure `export_folder`/report path.** Simplest and supported publicly, but settings/credentials root remains platform user config (not fully portable workspace).
  3. **Singleton patch/subprocess workarounds.** Fragile or lower ergonomics; not preferred.

### 7.4 Research conclusion
The module is **ready with caveats** for local JupyterLab/classic Jupyter when using `await run_report_async(...)`, valid settings/credentials, and notebook-safe defaults (no secrets in cells, progress default/null). Full CR-3 “portable workspace” (output + reports source path + settings/config workspace) exceeds the current public-API folder knobs and must be delivered by a documented, caveated workflow within current architecture (Q8=A), with explicit mapping from notebook cell variables to the chosen mechanism.

## 8. Assumptions

| ID | Assumption | Validation point |
|---|---|---|
| A-1 | Users have valid Mediascope credentials and network/API access. | Q2 acceptance requires real execution; failures due to invalid credentials/access are environmental, not notebook-contract defects. |
| A-2 | Users run local JupyterLab/classic Jupyter with Python 3.12+ and can install packages from Git. | Q1/Q3/Q6. |
| A-3 | A “working notebook” is a `.ipynb` in the repository with executable cells and instructions. | Q1. |
| A-4 | “Configurable folders” means user-controlled locations for settings/config workspace, reports source path, and output/export, driven by notebook cell variables. | Q4/Q5; detailed mapping finalized in T3. |
| A-5 | Reproducibility is achieved by a pinned default Git ref while allowing user override to latest branch or another ref. | Q6. |
| A-6 | Existing report semantics and required Mediascope report parameters remain the user’s responsibility; the notebook demonstrates a supported example report/workflow. | Scope boundary. |

## 9. Constraints

- Python 3.12+; dependencies managed by `uv`/pip ecosystem as applicable.
- Async execution model: active notebook event loops require the async entry path (`run_report_async` / `await`), not the sync loop-guarded wrapper.
- Current config system is file-based (JSON5/JSON) and Pydantic-validated; bundled defaults/templates are part of package settings.
- `platformdirs` determines the default user config location; OS paths differ (Linux/macOS/Windows).
- Credentials are sensitive; notebook deliverable must not embed or persist secrets.
- Mediascope API behavior, availability, rate limits, and data correctness depend on the external service and credentials.
- Q8=A constrains delivery to the current architecture and requires caveat documentation.

## 10. Risks

| Risk | Impact | Mitigation (spec-level) |
|---|---|---|
| Portable-workspace requirement (Q4=B) conflicts with public singleton/config-root behavior | Notebook may need internal DI pattern or documented workspace bootstrap; user confusion | Make folder mapping explicit in T3; document caveats and exact supported mechanism; validate against acceptance criteria. |
| Global logging configuration (`dictConfig`) affects long-lived notebook kernels | Unexpected log noise/handler changes | Document caveat; prefer library-safe usage guidance; avoid unnecessary config reinitialization in notebook flow. |
| Process-wide singleton/cached config in kernel reuse | Changed config/credentials may not be picked up without restart/reload | Document kernel restart/reload caveat; define expected notebook execution order. |
| Real-credential E2E acceptance cannot be reproduced in CI or by maintainers without secrets | Acceptance evidence is environment-specific | Require sanitized evidence (paths/status/output presence) and a repeatable procedure; do not record credentials. |
| Git install variability (unpinned/latest branch, transient network/package resolution) | Non-reproducible runs | Default to pinned ref; make selected ref visible in notebook output. |
| Terminal-oriented progress/reporters misbehave in notebooks | Cluttered or broken output | Default to `NullProgressReporter`/notebook-safe behavior in notebook; document. |
| Missing/invalid credentials or incomplete settings tree | Run fails before/at API call | Clear preflight checks/messages in notebook contract; credentials remain in user-managed file. |

## 11. Open questions

No blocking Product Owner questions remain for this specification version. The following implementation-planning details should be resolved during planning/execution but do not change PO decisions:

| ID | Question | Owner | Notes |
|---|---|---|---|
| OQ-1 | Where in the repo should the example notebook live (e.g., `notebooks/`, `examples/`, `docs/`)? | Tech Lead / maintainer | Must remain maintainable and discoverable; Q1 requires repo-shipped notebook. |
| OQ-2 | Which example report/settings should the notebook use for E2E acceptance (existing report template vs. dedicated example)? | Tech Lead / PO delegate | Must not expose secrets; should minimize data volume/cost where possible. |
| OQ-3 | Exact mechanism for Q4=B within current architecture (public workflow vs. documented internal `AppService`/`AppPaths` bootstrap vs. workspace file-copy flow) — selected for notebook deliverable? | Planner / Implementor under Q8=A | Must satisfy CR-3/CR-5 and be clearly documented; architectural redesign is out of scope. |
| OQ-4 | What sanitized evidence is retained for CR-5 (screenshot/log snippet/output listing) and where is it stored? | Tech Lead | Must exclude credentials and sensitive payloads. |

## 12. Definition of ready

This specification is ready for implementation planning when:
- Problem statement, scope, and confirmed requirements (CR-1–CR-8) are accepted.
- PO decisions Q1–Q8 are recorded (complete).
- Conceptual tasks T1–T6 have clear purpose/outcomes and dependencies.
- Research summary covers current notebook API behavior, config/path constraints, and folder-control gaps with evidence.
- Assumptions, constraints, risks, and out-of-scope boundaries are explicit.
- Remaining items are non-blocking planning details (OQ-1–OQ-4), not unresolved business decisions.
- The next workflow step is implementation planning for a repo-shipped notebook + caveat/readiness documentation within the current architecture.
