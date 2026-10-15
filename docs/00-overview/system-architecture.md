---
id: system-architecture
domain: overview
tags:
  - architecture
  - components
  - concurrency
  - diagrams
related:
  - doc-maintenance-rules
  - current-behavior
  - data-flow-architecture
  - product-overview
  - open-questions
  - settings-layers-architecture
  - async-concurrency-architecture
---

## Purpose

Technical architecture of `mko_get_mediascope_data`: the system context, the
major components and their responsibilities, the direction of calls, the public
interfaces, the execution flow of the key scenario, and the significant risks
and trade-offs. Deep dives on the two cross-cutting mechanisms live in their own
documents and are not repeated here:
[settings-layers-architecture.md](settings-layers-architecture.md) and
[async-concurrency-architecture.md](async-concurrency-architecture.md).

## Main Concepts

### System context

```mermaid
flowchart LR
    subgraph actors["Human / machine actors"]
        U["Analyst / seller<br/>(CLI user)"]
        D["Developer / scheduler<br/>(Jupyter, Airflow)"]
    end
    subgraph tool["mko_get_mediascope_data"]
        CLI["CLI app.py<br/>init · run · list-reports"]
        LIB["Library API<br/>run_report · run_report_async"]
        SVC["AppService<br/>core/services.py"]
        ENGINE["Report engine<br/>core/reports.py · tasks.py · network.py"]
    end
    subgraph external["External systems"]
        MS["Mediascope API<br/>mediascope-api-lib<br/>(auth + MediaVortex tasks)"]
        FS["File system<br/>user settings + export folders"]
    end
    U --> CLI
    D --> LIB
    CLI --> SVC
    LIB --> SVC
    SVC --> ENGINE
    ENGINE -->|"HTTP JSON (retry, semaphore 5)"| MS
    SVC -->|"reads YAML/JSON settings"| FS
    ENGINE -->|"writes CSV(.gz), error YAML, log.log"| FS
```

There is no database, no web server, and no message broker; the only external
dependency is the Mediascope HTTP API, reached through the vendor library
`mediascope-api-lib` (`pyproject.toml` dependencies; `services.py:7`).

### Components and responsibilities

| Component | File | Responsibility | Public? |
|---|---|---|---|
| CLI app | `app.py` | Typer commands (`init`, `run`, `list-reports`), Rich console output, sync/async public wrappers | yes — module-level functions |
| Package root | `__init__.py` | Lazy re-export of the public API via PEP 562 `__getattr__` so importing the package does not bootstrap the service | yes |
| Path resolution | `core/paths.py` | `AppPaths` (where settings, defaults, reports, credentials live) and `PathResolver` (relative → absolute under the user folder) | no |
| Settings bootstrap | `core/init_service.py` | Copy bundled templates into the user settings folder | no |
| Models | `core/models.py` | Pydantic v2 schema for settings (`ReportSettings`, `PeriodModel`, `LastTimeModel`, `DataDefaults`, `Data`, …) and the fixed-value enums | no |
| Orchestrator | `core/services.py` | `AppService`: config loading and caching, settings merge, connection creation, per-report loop; process-wide lazy singleton | no (`get_app_service` is the internal accessor) |
| Report engine | `core/reports.py` | `ReportFactory` + strategies; `TVMediaReport` lifecycle: period resolution, task generation, submit/poll/extract, worker pool | no |
| Task model | `core/tasks.py` | `TVTask`: name, settings, status, error; error-YAML serialization | no |
| Network layer | `core/network.py` | `NetworkClient.call`: semaphore-bounded, thread-offloaded, retrying wrapper around every vendor call — the one exception is the direct `cats.get_tv_region()` catalog read in `TVRegTaskStrategy` | no |
| Utilities | `core/utils.py` | Date/period math, YAML/CSV I/O, suffix mapping, slice localization, frequency pivot, file listing | no |
| Errors | `core/errors.py` | `NoReportFoundError`, `WrongDataSettings` — **declared but never raised anywhere in the current code** | no |

### Component diagram (call direction)

```mermaid
flowchart TD
    APP["app.py<br/>(CLI + public API)"] --> SVC["AppService<br/>core/services.py"]
    INIT["init_service.py"] --> PATHS["core/paths.py<br/>AppPaths / PathResolver"]
    SVC --> PATHS
    SVC --> MODELS["core/models.py<br/>(Pydantic v2 + enums)"]
    SVC --> UTILS["core/utils.py<br/>(yaml/csv/date helpers)"]
    SVC --> REPORTS["core/reports.py<br/>ReportFactory + strategies"]
    REPORTS --> MODELS
    REPORTS --> UTILS
    REPORTS --> TASKS["core/tasks.py<br/>TVTask"]
    REPORTS --> NET["core/network.py<br/>NetworkClient"]
    SVC --> NET
    NET --> VENDOR["mediascope_api<br/>MediaVortexTask (vendor)"]
    UTILS --> MODELS
```

Verified call directions (not inferred from names) `[code]`:

- `app.run` → `get_app_service().run_report` (`app.py:50`, `services.py:137`).
- `AppService.run_report` → `get_report_settings` → `ReportSettings`
  (`services.py:84-91`); → `get_data_settings` → `get_default_data_settings` +
  `merge_data_settings` → `Data` (`services.py:122-125`); → `NetworkClient.call(
  self.get_connection, media)` (`services.py:157-160`); → `ReportFactory(...)` →
  `TVMediaReport(...)` → `create_report()` (`services.py:162-175`).
- `TVMediaReport.create_report` → `get_period()` → `get_available_period()` →
  `network_client.call(catalogs.get_availability_period)` (`reports.py:166`,
  `:193-205`); → `processing_tasks()` → `generate_tasks()` →
  `task_strategy.generate(self)` (`reports.py:155-162`, `:335-337`).
- `worker` → `send_task` → `network_client.call(builder)` then
  `network_client.call(sender)`; → `wait_task` → `network_client.call(
  get_status)`; → `extract_data` → `network_client.call(get_result)` and
  `network_client.call(result2table)` → `prepare_data` → `data_strategy.prepare`
  → `asyncio.to_thread(utils.csv_to_file, …)` (`reports.py:141-153`, `:252-333`,
  `:339-340`).

### Data-flow overview

```mermaid
flowchart LR
    Y1["report YAML<br/>(CLIENT deltas)"] --> V["ReportSettings<br/>validation"]
    Y2["defaults YAML<br/>(DEFAULT base + subtype)"] --> M["merge_data_settings<br/>shallow overlay"]
    V --> M
    M --> D["Data<br/>(API parameters)"]
    D --> G["task fan-out<br/>interval × audience [× region]"]
    API["Mediascope API<br/>availability, build, send, status, result"] --> P["period resolution<br/>clip + slice"]
    P --> G
    G --> W["worker pool<br/>(max 100) / HTTP semaphore (5)"]
    W --> E{"result ok?"}
    E -- yes --> DF["data strategy<br/>column select / pivot / melt"]
    DF --> CSV["CSV(.gz) per task<br/>export/&lt;relative_path&gt;/"]
    E -- no --> ERR["error YAML<br/>export/&lt;relative_path&gt;/errors/"]
```

The per-task transformation detail is documented in
[data-flow-architecture.md](data-flow-architecture.md).

### Execution flow of the key scenario (run)

```mermaid
sequenceDiagram
    actor U as Analyst
    participant CLI as app.py run
    participant SVC as AppService
    participant REP as TVMediaReport
    participant NET as NetworkClient
    participant API as Mediascope API
    participant FS as File system
    U->>CLI: run report.yaml
    CLI->>SVC: run_report(path)
    SVC->>FS: read user settings (log/app config)
    SVC->>SVC: validate entries (ReportSettings)
    loop for each report entry
        SVC->>SVC: merge defaults + entry → Data
        SVC->>NET: get_connection(media)
        NET->>API: auth / session (vendor lib)
        SVC->>REP: create_report()
        REP->>NET: get_availability_period(kitId)
        NET->>API: HTTP
        API-->>REP: available [from, to]
        REP->>REP: resolve + slice period
        REP->>REP: generate tasks (interval × audience)
        loop worker pool (≤100 concurrent tasks)
            REP->>NET: build_task + send_task
            NET->>API: HTTP
            API-->>REP: task key
            loop poll
                REP->>NET: get_status
                NET->>API: HTTP
                API-->>REP: DONE / FAILED / CANCELLED
            end
            REP->>NET: get_result + result2table
            NET->>API: HTTP
            API-->>REP: DataFrame
            REP->>REP: data strategy
            REP->>FS: CSV (mode="x")
        end
    end
    CLI-->>U: success / error line + exit code
```

Error path worth understanding (the one the current code handles worst), with
the failure classes separated `[code]`:

- **Before the worker pool** (`create_report` → `get_period` →
  `get_available_period`): a failure aborts the run with a printed error and
  exit code 1 — visible and loud.
- **Logical failures inside the pool** (no task key, empty result, missing
  columns): `task.status = False`, error YAML written, warning logged, run
  continues — the designed recovery path.
- **Exceptions raised inside a worker** (`BadRequestError`, `AuthorizationError`,
  `RuntimeError` after retry exhaustion): no `try/except` in the worker, so
  `task_done()` is never called (`reports.py:153`), `task_queue.join()` never
  returns (`reports.py:161`), and no traceback is printed — the run hangs
  silently (probe-verified with the real worker; lead: audit PIPE-002 /
  ASY-001). The owner has not observed this in practice; the full analysis is
  in [Open Questions](open-questions.md#precision-note-on-g-02-which-failures-hang-and-the-owners-observation).

### Configuration and external dependencies

| Dependency | Role | Notes |
|---|---|---|
| `mediascope-api-lib` | Vendor API client: authentication, task build/send/status/result, catalogs | Instantiated per report as `MediaVortexTask(settings_filename=…/mediascope.json, check_version=False)` (`services.py:127-135`) |
| `pandas` | Result table conversion and CSV writing | `result2table` returns a DataFrame; `csv_to_file` writes it |
| `pydantic` v2 | All settings models and validation | see [settings-layers-architecture.md](settings-layers-architecture.md) |
| `typer` + `rich` | CLI and console output | `app.py` |
| `platformdirs` | User settings folder location | `paths.py:8` |
| `PyYAML` | Template/config parsing | `utils.yaml_to_dict` |
| `Unidecode` | ASCII-safe file names for audiences and regions | `reports.py:375`, `:413-421` |
| `python-dateutil` | Calendar arithmetic for periods | `utils.get_last_period`, `iter_period_slices` |

Runtime knobs that shape behavior, all hard-coded defaults (no config file
maps to them): `max_tasks = 100` (`reports.py:92`), `NetworkClient`
(`max_concurrent_requests=5`, `sleep_time=2`, `max_attempts=10`,
`network.py:49-54`), poll cadence `asyncio.sleep(2)` + `get_status(
sleep_time=40)` (`reports.py:278-281`).

### Design decisions and trade-offs

| Decision | Rationale (as recorded) | Trade-off |
|---|---|---|
| Two-layer settings with shallow overlay | Owner decision (D-01); templates stay small and vendor parameters live in one place | A typo in a report YAML that is not a `DATA_DEFAULTS` key is silently dropped by the merge; a typo inside a DEFAULT/subtype block reaches the vendor `build_task`, which has no `**kwargs`, and raises `TypeError` (verified against the installed library) |
| Two-level concurrency (`max_tasks=100` vs HTTP semaphore 5) | Documented as intentional independence in [async-concurrency-architecture.md](async-concurrency-architecture.md) | More knobs to reason about; the worker pool can pin 100 vendor tasks while only 5 HTTP calls fly |
| Resume via existing file names (`check_done`) | Cheap idempotent re-runs without a state store | The name is the only identity key — rename a file and the task re-runs; the skip is silent in the TV_REG path (`reports.py:430-431`) |
| `mode="x"` + swallow `FileExistsError` | Never overwrite an analyst's existing export | A skipped write is indistinguishable from success (`utils.py:192-200`) |
| Errors as side-car YAML files | Keeps the export folder the single place to look | Written with `yaml.dump`, producing `!!python/tuple` tags (`tasks.py:5, 46-51`); raised exceptions never reach this path (the worker has no exception isolation) |
| `extra="allow"` on settings models | Intended pass-through of template-defined API parameters | No validation of unknown keys; legacy keys (`folder`, `category_name`) are silently ignored (zero references in `core/`) |
| Lazy singleton `get_app_service()` | Importing the library must not touch the file system (commit `706eed0`; covered by `tests/test_app.py::test_package_import_does_not_bootstrap`, run: passed) | First use pays config loading; a missing user config surfaces at first command, not at import |

### Risks and known technical limitations

Every row below was verified against the code by this investigation. The audit
corpus is a lead source only (owner ruling D-05): the IDs in the last column
are pointers, not evidence, and several audit entries do not match HEAD.

| Risk / limitation (code-verified) | Code evidence | Also recorded as |
|---|---|---|
| No shipped report template can run end-to-end (30/32 entries fail validation; both crosstab defaults malformed) | probe: [Current Behavior](current-behavior.md#verification) | audit TPL-001 / TPL-004 leads; gap G-05 |
| A single network raise hangs the whole run (no worker exception isolation) | probe: `queue.join()` never returns after a raise (`reports.py:141-153`) | audit PIPE-002 / ASY-001 leads |
| Zero-task and out-of-window periods exit 0 (silent success) | probe: zero intervals for a non-overlapping period (`reports.py:242-246`, `utils.iter_period_slices` early return) | audit PIPE-004 / PIPE-005 leads |
| Poll loop has no deadline | `reports.py:277-293` | audit PIPE-006 lead |
| Type safety unenforced: mypy red (56 errors in 7 files at HEAD, verified), no CI, manual release | `uv run mypy src/` on this machine | audit OPS-001 lead |
| Named loggers in `log_config.yaml` never fire; root handler applies | `services.py:57-64` vs module logger names | audit OPS-003 lead |
| Test suite covers utils/models/strategy selection only; no coverage of the period, worker, and CLI happy paths | `tests/` (read; 23 run) | audit TST-* leads |
| `errors.py` exceptions are dead code; failures crash with raw exceptions | `errors.py:1-5`; grep: no raise sites | — |
| One `requests.Session` per report entry, never closed | `services.py:127-135`; vendor `mediavortex/tasks.py:43-47`, `core/net.py:73` | audit EXT-006 lead |
| Live API behavior never observed in this investigation (no credentials) | stated limitation of this investigation | — |

### Gaps between owner intent and this architecture

Recorded from the owner's 2026-10-15 answers; the full register with decision
IDs is in
[Open Questions](open-questions.md#gaps-between-owner-intent-and-current-implementation).
These are pending work items, not defects introduced by this documentation:

- **G-01** — silent empty runs: no warning when a period has no data (intent:
  warn, never interrupt, D-08).
- **G-02** — exceptions raised inside a worker hang the run silently and bypass
  the error description (intent: never stop the export; always persist a failed
  task's description, D-09/D-17). Failures before the pool and logical failures
  behave correctly; the owner has not observed the hang in practice — see the
  precision note in
  [Open Questions](open-questions.md#precision-note-on-g-02-which-failures-hang-and-the-owners-observation).
- **G-03** — region selection is hard-coded (id 99) instead of template-driven
  (intent: user template expresses inclusion/exclusion; research R-01, D-10).
- **G-04** — output conventions (separators, BOM, DICT columns) are hard-coded
  (intent: configurable defaults + per-report overrides, D-13/D-14).
- **G-05** — bundled templates are not runnable (intent: keep everything
  runnable, D-06).

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Current Behavior](current-behavior.md)
- [Data Flow Architecture](data-flow-architecture.md)
- [Product Overview](product-overview.md)
- [Open Questions](open-questions.md)
- [Settings Layers Architecture](settings-layers-architecture.md)
- [Async Concurrency Architecture](async-concurrency-architecture.md)
