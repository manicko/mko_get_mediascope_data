---
id: current-behavior
domain: overview
tags:
  - behavior
  - reference
  - cli
  - pipeline
related:
  - doc-maintenance-rules
  - product-overview
  - data-flow-architecture
  - system-architecture
  - open-questions
  - settings-layers-architecture
  - async-concurrency-architecture
---

## Purpose

Describes how `mko_get_mediascope_data` behaves **today**, traced from entry
point to output, with the source location for every important claim. It answers:
what can be run, with what inputs, what transformations happen, what comes out,
and what happens when things go wrong. It deliberately separates what the code
does from what the owner wants: the owner-confirmed target behavior lives in
[Product Overview](product-overview.md#target-behavior-owner-confirmed), and
the places where the code contradicts that intent are registered as gaps
G-01…G-05 in
[Open Questions](open-questions.md#gaps-between-owner-intent-and-current-implementation).
Nothing in this document is a change request — implementation is out of scope
for this investigation.

Line numbers refer to the current source tree. **Evidence policy (owner
ruling, D-05 in [Open Questions](open-questions.md)): the code and the owner's
decisions are the only sources of truth.** The audit corpus in
`.ai/audit/99-validation/` is used only as a lead generator: every finding cited
in this document was re-verified directly against the code (by inspection or by
executing the probe scripts listed in [Verification](#verification)) before it
was restated here. Audit finding IDs are kept as pointers for readers who want
the background, with the caveat that the reports were produced at frozen
baselines (`d093d9c`/`b04d177`), contain errors, and in several places no longer
match HEAD — where the audit and the code disagree, this document follows the
code.

## Main Concepts

### Entry points

| Entry point | How it is invoked | Implementation |
|---|---|---|
| Console script | `mko-get-mediascope <command>` | `pyproject.toml:44` → `mko_get_mediascope_data.app:app` |
| Python module | `uv run -m mko_get_mediascope_data <command>` (or `python -m …`) | `__main__.py:1-4` imports `app` from `app.py` |
| Library (sync) | `from mko_get_mediascope_data import run_report; run_report("report.yaml")` | `app.py:83-91`, re-exported lazily in `__init__.py:16-29` |
| Library (async) | `from mko_get_mediascope_data import run_report_async` | `app.py:73-80` |
| Library (setup) | `initialize_settings(force=False)` | `app.py:94-96` → `init_project` |

Both library functions delegate to `AppService.run_report`
(`services.py:137`), the same method the CLI uses.

### CLI command surface (verified)

| Command | Arguments / options | Behavior |
|---|---|---|
| `init` | `--force / -f` | Copies the package's bundled `settings/` tree into the user settings folder; without `--force`, existing files are kept (`init_service.py:23-27`) |
| `run` | `REPORT` (required, must exist, must be a file), `--verbose / -v` (default true) | Runs the report(s) in the YAML file (`app.py:36-55`) |
| `list-reports` | — | Lists `*.yaml` files (recursive) under the user settings `reports/` folder (`app.py:58-69`) |

Discrepancies between the README and the actual CLI, verified by invoking the
app with `typer.testing.CliRunner`:

- The command is `list-reports`; `list_reports` fails with exit code 2
  (Typer hyphenates command names).
- `--no-verbose` is not a valid option; `run report.yaml --no-verbose` fails
  with exit code 2, although the README documents it.

### Configuration and where it lives

| Item | Location | Code |
|---|---|---|
| Package templates | `<package>/settings/` (app_config, log_config, connections, defaults, reports) | `paths.py:42-43` |
| User settings | `platformdirs.user_config_dir("mko_get_mediascope_data")/settings/` — on the investigation machine this resolves to `…\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data\settings` | `paths.py:8, 38-39` |
| Export root | `app_config.yaml → export_folder`, resolved against the user settings folder; default `output` | `services.py:78-82`, `settings/app_config.yaml` |
| Log file | `log_config.yaml → handlers.rotating_file.filename` (`log.log`), resolved against the user settings folder | `services.py:57-64`, `settings/log_config.yaml` |
| Mediascope credentials | `<user settings>/connections/mediascope.json` (username, passw, client_id, client_secret, auth_server, root_url) | `paths.py:58-59`, `settings/connections/` |

On the investigation machine the resolved user settings folder is
`C:\Users\Om\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data`
(probe: `APP_PATHS.user_dir`), and the default export folder resolves to
`<user settings>/output`.

Two startup defects are relevant to configuration handling:

- The shipped credential template is `connections/mediascope_RENAME_.json`
  but the code reads `connections/mediascope.json` — verified against the
  shipped template; after `init`, the expected file does not exist until the
  user renames it (`paths.py:58`; lead: audit CFG-003).
- `AppService` is created lazily on first use and immediately reads
  `log_config.yaml` and `app_config.yaml` (`services.py:31-35`, `get_app_service`
  at `:191`). If the user has not run `init`, the first command raises
  `FileNotFoundError` from `yaml_to_dict` (`utils.py:365-367`) and the CLI
  prints `❌ Error: …` and exits 1 (`app.py:53-55`).

### Scenario: `run` — step by step

Entry: `run <report.yaml>` → `asyncio.run(get_app_service().run_report(path))`
(`app.py:50`).

1. **Parse and validate.** `get_report_settings` iterates the YAML list and
   constructs `ReportSettings(**entry)` per entry (`services.py:84-91`). A
   validation error in any entry aborts the whole file: the generator logs the
   exception and re-raises (`services.py:89-91`).
2. **Per entry** (`services.py:149-177`):
   a. `export_path = export_folder / report_settings.relative_path`, created if
   missing (`services.py:151-152`).
   b. `get_data_settings` builds the API parameters: the DEFAULT template for
   `(media, report_type)` is loaded (`services.py:93-101`), the report's
   subtype block (if present) is overlaid, then the report's own keys that
   exist in the defaults are overlaid, then Russian slice names are substituted
   when `data_lang: ru` (`services.py:103-120`).
   c. A fresh `NetworkClient` is created and the vendor connection
   (`MediaVortexTask`) is built through it (`services.py:156-160`, `:127-135`).
   d. `ReportFactory` selects the task-generation and data-preparation
   strategies (`reports.py:27-47`); a `TVMediaReport` is constructed and
   `create_report()` is awaited (`services.py:162-175`).
3. **Resolve the period** (`reports.py:166`, `:223-250`):
   - the available date window for the dataset is fetched from the API
     (`cats.get_availability_period`, filtered by `options.kitId`, default 1;
     `reports.py:193-205`, `:189`);
   - if `period.date_filter` is absent, the period is computed from
     `period.last_time` (`utils.get_last_period`, `utils.py:88-130`);
   - the requested period is clipped to the available window with
     `max/min` (`reports.py:242-244`);
   - if `multiple_files` is true (default), the period is split into intervals
     of `last_time.time_unit` (`utils.slice_period`, `utils.py:69-85`).
4. **Generate tasks** (`reports.py:335-337` → strategy `generate`):
   - `DefaultTVTaskStrategy` (`reports.py:358-381`): for each interval, for each
     `target_audiences` entry: task name = `<start>_<end>_<ascii audience>`,
     settings = merged parameters with `date_filter=[interval]` and
     `basedemo_filter=<audience filter>`. Tasks whose name already exists as an
     output file are skipped with a warning (`reports.py:378-380`).
    - `TVRegTaskStrategy` (`reports.py:384-432`): additionally iterates regions
      (explicit `regions` list, else all catalog regions from
      `cats.get_tv_region()`), skips region id 99, adds
      `regionId IN (<id>)` to the company filter, and appends the region's
      English name to the file name. Only id 99 is excluded in the code; the
      owner recalled 99 **and** 100 (recorded as DISC-09), and the region set is
      to move into the user template (D-10, gap G-03).
5. **Process tasks** (`reports.py:155-162`, worker at `:134-153`): up to
   `max_tasks=100` worker coroutines pull tasks from a bounded queue. Each
   worker:
   - `send_task` — builds the API request and submits it; if no task key comes
     back, the task is marked failed (`reports.py:252-269`);
   - `wait_task` — polls `get_status` every ~2 s until `DONE` / `FAILED` /
     `CANCELLED`, or until the status call reports retries exhausted
     (`reports.py:271-293`);
   - `extract_data` — fetches the result JSON, converts it to a DataFrame,
     applies the data strategy, and writes the CSV (`reports.py:295-333`);
   - on a logical failure (`task.status is False`), writes the error YAML
     instead of a CSV (`reports.py:148-150`).
6. **Result:** `run_report` returns the list of export directories
   (`services.py:176`, `:185`); the CLI prints a success line and exits 0
   (`app.py:51`).

### Inputs: the report YAML file

A report file is a **list** of report entries. Fields (from `ReportSettings`,
`models.py:83-106`, plus keys that fall through to the API parameters via the
merge):

| Field | Required | Meaning / accepted values |
|---|---|---|
| `report_type` | yes | Enum: `crosstab`, `timeband`, `simple` (`models.py:44-47`). Selects the DEFAULT template file |
| `media` | yes | Enum: `TV`, `TV_REG` (`models.py:39-41`). Selects the DEFAULT template file |
| `relative_path` | yes | Sub-folder under the export root for this report's outputs (`models.py:93`) |
| `period` | yes | Exactly one of `date_filter: [start, end]` or `last_time: {time_unit: d/w/m/y, unit_count: N, include_current: bool}` must be present; both may be present (`models.py:68-80`) |
| `report_subtype` | no (default `""`) | Selects a subtype block inside the DEFAULT template (`models.py:50-58`) |
| `data_lang` | no (default `ru`) | `ru` substitutes Russian slice names; `en` keeps the API's English names (`models.py:92`, `services.py:117-118`) |
| `compression` | no (default `{"method": "gzip"}`) | Output compression; drives the file extension (`models.py:89-91`, `utils.get_files_suffix`) |
| `multiple_files` | no (default `true`) | Split the period into `last_time.time_unit` intervals (`models.py:94-96`) |
| `target_audiences` | no (default `{"not_set": null}`) | Map of audience name → basedemo filter string; one task per audience (`models.py:98-100`, `reports.py:369`) |
| `ad_filter` | no | Ad filter expression; overlaid onto the defaults (`models.py:97`, `services.py:113-115`) |
| `check_done` | no (default `true`) | Skip tasks whose output file already exists (`models.py:85`, `reports.py:70-73`) |
| `slices`, `statistics`, `options`, `company_filter`, … | no | API parameters; overlaid onto the DEFAULT template when the key exists there (`services.py:113-115`) |

Reference example that validates end-to-end:
[`settings/reports/nat_tv_brands_tefal.yaml`](../../src/mko_get_mediascope_data/settings/reports/nat_tv_brands_tefal.yaml)
— the only bundled report whose entries pass `ReportSettings` validation
(verified by probe; lead: audit TPL-001).

### Inputs: the DEFAULT templates

`settings/defaults/<media>/<report_type>.yaml` provides `DATA_DEFAULTS` (the
full Mediascope parameter set) plus optional subtype blocks. Bundled coverage:

| File | Status |
|---|---|
| `defaults/tv/crosstab.yaml` | **Fixed** — `yaml.safe_load` previously raised `ParserError` at line 138 (the `TOP_NAT_TV_PROGRAMS.slices` sequence was closed with `}` instead of `]`). Verified: the regression entered with commit `b04d177` (the i18n translation commit) and the template parses correctly at HEAD (TST-007) |
| `defaults/tv_reg/crosstab.yaml` | **Fixed** — `yaml.safe_load` previously raised `ParserError` at line 162 (unbalanced flow-mapping braces: 13 open / 12 close). Verified: parses correctly at HEAD (TST-007; lead: audit TPL-004) |
| `defaults/tv/timeband.yaml` | Parses; contains only `DATA_DEFAULTS`, no subtype blocks |
| `tv/simple`, `tv_reg/timeband`, `tv_reg/simple` | **Not shipped** — a report with that `(media, report_type)` pair fails with `FileNotFoundError` from `get_default_data_settings` (verified: no such files; `services.py:96-101`; lead: audit TPL-005) |

Consequence: with the crosstab template fixes (TST-007), the TV and TV_REG
crosstab DEFAULT templates now parse correctly. However, **no bundled report
can complete a full run against the real API** — the only entries that pass
`ReportSettings` validation are the crosstab entries of `nat_tv_brands_tefal.yaml`
(now able to load their DEFAULT template), but every other bundled entry fails
validation itself, and `tv/simple`, `tv_reg/timeband`, `tv_reg/simple` DEFAULT
templates are not shipped. This is the top current-state gap ([Open Questions](open-questions.md)
Q-01, Q-06).

### Outputs

| Output | Rule | Code |
|---|---|---|
| Data CSV | `<export folder>/<relative_path>/<task name>.csv[.gz]`; `task name = <start>_<end>_[<region>_]<ascii audience>` | `reports.py:325-332`, `utils.csv_to_file` `utils.py:167-200` |
| CSV format | `sep=";"`, `decimal=","`, `index=False`, `encoding="utf-8-sig"`, `compression` from settings, exclusive create (`mode="x"`), no timestamp suffix in the report pipeline | `utils.py:186-198` |
| Columns | slices (plus `targetAudience` first when `target_audiences` is set, and `frequencyDistInterval` when frequency distribution is configured) followed by statistics; `prj_name` renamed to `targetAudience` | `reports.py:207-221`, `:444-448` |
| Frequency distribution | When `frequency_dist_conditions` is configured, the base row and the per-interval frequencies are pivoted into one row per slice combination | `utils.pivot_df_frequency` `utils.py:214-252`, `reports.py:449-450` |
| Dictionary output | Subtypes containing `DICT` produce the melted term-dictionary table instead of the raw columns | `reports.py:457-509` |
| Error YAML | `<export path>/errors/<task name>.yaml` with name, report type/subtype, target, error, settings | `tasks.py:29-51` |

### Errors, warnings, and edge cases

| Situation | Current behavior | Evidence |
|---|---|---|
| Report entry fails validation | `ReportSettings` error is logged and re-raised; the whole file aborts; CLI prints the error, exit 1 | `services.py:84-91`, `app.py:53-55` |
| Missing/malformed DEFAULT template | `FileNotFoundError` / `ParserError` propagates from `yaml_to_dict`; run aborts with exit 1 | `utils.py:359-367`, `services.py:101` |
| `date_filter` set but `last_time: null` | **Crash** — `get_period` reads `period.last_time.time_unit` unconditionally (`reports.py:241`); `AttributeError`, logged and re-raised by the surrounding `try/except` (`reports.py:248-250`). Executed in a probe: `AttributeError: 'NoneType' object has no attribute 'time_unit'`. The test fixture encodes exactly this configuration (`tests/conftest.py:45-48`) but the path is mocked in the test | code + probe (lead: audit PIPE-003) |
| Requested period outside the API window | Period is clipped (`reports.py:242-244`); when the ranges do not overlap, the clipped period is inverted and `iter_period_slices` returns immediately (`utils.py`: `if overall_start > overall_end: return`), so the run generates **zero intervals** and exits 0 with a success message — a silent empty run. Executed in a probe: zero intervals for a 1900 request against a 2024-2025 window. **The owner requires a warning here (D-08, D-15) — gap G-01** | code + probe (lead: audit PIPE-005) |
| Zero tasks generated (e.g. `target_audiences: {}`) | `processing_tasks` consumes an empty queue and returns; run completes with exit 0, no output, no warning. Executed in a probe | `reports.py:155-162`; probe (lead: audit PIPE-004) |
| Output file already exists | `csv_to_file` uses `mode="x"`, catches `FileExistsError`, logs a warning, returns `None`; the task is still reported as ready. Executed in a probe: second write returns `None`, no exception, one file | `utils.py:192-200`; probe (lead: audit PIPE-001 / OUT-003) |
| Task fails logically (no key, empty result, missing columns) | `task.status = False` → warning logged, error YAML written, run continues | `reports.py:148-150`, `tasks.py:29` |
| Vendor rejects a request (HTTP 4xx / auth / bad request) | Raised immediately by `NetworkClient`, no retry | `network.py:76-93` |
| Transient network failure | Retried up to 10 attempts with backoff + jitter inside `NetworkClient.call`; then `RuntimeError("Network retry limit exceeded")` | `network.py:59-126`; covered by `tests/test_network.py` (run: passed) |
| Exception raised inside a worker | **No `try/except` in the worker** — an exception raised in `send_task`/`wait_task`/`extract_data` skips `task_done()` (`reports.py:153`) and `queue.join()` never returns (`reports.py:161`). Verified by probe with the real `MediaReport.worker` for `RuntimeError` and `BadRequestError`. **No traceback is printed** — the worker task's exception is never retrieved; the symptom is a console that stops advancing and a process that never exits. **Contradicts the owner's "never stop an export" rule (D-09, D-17) — gap G-02** (owner has not observed this in practice; see the precision note in [Open Questions](open-questions.md#precision-note-on-g-02-which-failures-hang-and-the-owners-observation)) | `reports.py:141-153`; probe (lead: audit PIPE-002 / ASY-001) |
| Failure before the worker pool | Validation errors, missing/malformed DEFAULT templates, connection and availability-check failures (`create_report` → `get_period`) abort the run with a printed error and exit code 1 — visible, no hang | `services.py:84-101`, `app.py:53-55`, `reports.py:164-167` |
| `wait_task` never terminates | Poll loop has no deadline; a stuck task pins a worker indefinitely | `reports.py:277-293` |
| Task status `FAILED`/`CANCELLED` | Breaks the poll loop but leaves `status=True`, so `extract_data` is attempted for a failed task | `reports.py:289-293` |
| Error YAML round-trip | Written with `yaml.dump` (not `safe_dump`), so tuples render as `!!python/tuple`. Executed in a probe: the file contains `!!python/tuple` and `yaml.safe_load` raises `ConstructorError` | `tasks.py:5, 46-51`; probe (lead: audit OUT-001) |
| `log_error` distinction | Set on the task in five places, read nowhere (grep: 5 writes, 0 reads) — "empty result" vs "error" has no observable effect | `reports.py:268-323`, `tasks.py:22`; grep (lead: audit PIPE-009) |
| Duplicate output files on re-run | Prevented before submission by the `check_done` pre-filter (name match against existing files) | `reports.py:70-80`, `:378-380`; `TVRegTaskStrategy` skip is silent (`reports.py:430-431`, lead: audit PIPE-011) |

### Configuration that affects behavior

| Setting | Effect |
|---|---|
| `app_config.yaml → export_folder` | Root of all outputs (default `output`, resolved under the user settings folder) |
| `log_config.yaml` | Handlers/formatters/levels applied via `logging.config.dictConfig` at service construction (`services.py:57-64`). The named loggers `__main__`, `tests`, `app` never match the real module loggers (which use `__name__`, e.g. `mko_get_mediascope_data.core.reports`), so everything falls through to `root`. `AppService.reload_config()` re-reads the YAML files and re-applies logging (`services.py:66-76`, covered by tests, run: passed) |
| `compression` | File extension and on-disk compression |
| `data_lang` | `ru` → slice fields `*EName` are requested as `*Name` (`utils.en_to_ru`, `services.py:117-118`) |
| `multiple_files` + `last_time.time_unit` | Export granularity — the interval length used to split the period |
| `max_tasks` (100) and `NetworkClient` limits (5 concurrent HTTP calls, 10 attempts, 2 s base sleep) | Concurrency and retry behavior; see [Async Concurrency Architecture](async-concurrency-architecture.md) for the intentional two-level design |

### Tests

The suite was executed on the investigation machine: **31 passed**
(`uv run pytest`, all mocked — no network, no Mediascope credentials). One test,
`test_run_report_with_real_models`, carries the `@pytest.mark.slow` marker and can
be excluded with `uv run pytest -m "not slow"`. Coverage is reported by default
via `--cov=src/mko_get_mediascope_data --cov-report=term-missing` (configured in
`pyproject.toml`).

| Test file | What it pins down |
|---|---|
| `tests/test_app.py` | CLI `init --help`; lazy re-export of `run_report`, `run_report_async`, `initialize_settings` via PEP 562 `__getattr__`; package import does not bootstrap `AppService`; re-exported functions are identical objects to those in `app.py` |
| `tests/test_models.py` | `ReportSettings` validation with the current field names; `PeriodModel` exclusivity rules; `merge_data_settings` does not call `en_to_ru` when `data_lang="en"` |
| `tests/test_network.py` | Retry exhaustion raises `RuntimeError("Network retry limit exceeded")`; `mock_network_client` fixture retries on retryable errors; success returns the function result; `BadRequestError` (4xx) and `AuthorizationError` (401) are re-raised without retry; semaphore limits concurrent in-flight calls |
| `tests/test_reports.py` | `DefaultTVTaskStrategy` task naming; `DictDataStrategy` output columns |
| `tests/test_reports_lifecycle.py` | Real `create_report` → `get_period` → `processing_tasks` → `worker` → `extract_data` → `csv_to_file` pipeline (unmocked except the `MediaVortexTask` double) writes CSV and the queue drains without hanging |
| `tests/test_services.py` | Full `run_report` pipeline end-to-end (slow; doubled `MediaVortexTask`, real `AppService`); `app_config` caching; `reload_config()` invalidates cache, is safe before first access, reapplies logging, and re-resolves `export_folder` |
| `tests/test_utils.py` | Date parsing; period slicing for `m` and `w`; gzip CSV writing; suffix mapping; `csv_to_file` writes into the report directory (`tmp_report_dir` fixture) |

Behaviors with **no test coverage** — these code paths were therefore never
executed during this investigation: `TVRegTaskStrategy` (tests use `media=TV`
only), `TVTask.to_yaml`, the worker's **error** handling (lifecycle tests
exercise the happy path only), the `run` and `list-reports` CLI happy paths,
and any end-to-end run against the real API. The following behaviors previously
listed as uncovered are **now exercised** by the lifecycle and hermetic pipeline
tests: `get_period` (including the period-clipping logic), `wait_task`,
`en_to_ru` (indirectly, via the `data_lang="ru"` path in
`test_run_report_with_real_models`), and queue completion accounting
(`task_done` / `queue.join`).

## Known defects and limitations

The defects below were each verified against the code by this investigation
(directly or by executing the probe scripts listed in
[Verification](#verification)). The audit corpus
(`.ai/audit/99-validation/`) is a lead source only — per the owner's ruling
(D-05) it is not authoritative; the IDs in the last column are pointers for
background reading, not evidence.

| Defect (code-verified) | Impact | Also recorded as |
|---|---|---|
| 30 of 32 bundled report entries fail `ReportSettings` validation (stale `period_type`/`period_num`, stale `report_type` values such as `TV_CROSSTAB`, missing `media`/`relative_path`) — verified by probe | A user following the README's example (`nat_tv_brands_last.yaml`) cannot run it; the owner requires all bundled templates to be runnable (D-06) | audit TPL-001 / CFG-001 leads; gap G-05 |
| Both bundled crosstab DEFAULT templates were malformed YAML (`defaults/tv/crosstab.yaml` ParserError at line 138; `defaults/tv_reg/crosstab.yaml` at line 162) — verified by probe; the TV regression entered with commit `b04d177` (verified by parsing the file at `3408747` vs `b04d177`). **Fixed at HEAD** (TST-007): both templates now parse correctly | Fixed — templates now parse at HEAD | audit TPL-004 (+ DISC-07) |
| CSV write collision is swallowed and the task reports "ready" — verified by probe | A re-run without `check_done` silently produces no file | audit PIPE-001 / OUT-003 |
| Worker has no exception isolation; `queue.join()` hangs on any raise — verified by probe | A single transient failure can stall the whole run instead of failing it | audit PIPE-002 / ASY-001 |
| Zero-task and out-of-window periods exit 0 with a success message — verified by probe | An analyst cannot distinguish "no data" from "done" | audit PIPE-004 / PIPE-005 |
| Poll loop has no deadline — verified by reading `wait_task` | A stuck vendor task pins a worker forever | audit PIPE-006 |
| `log_error` is write-only — verified by grep (5 writes, 0 reads) | "Empty result" and "error" are indistinguishable downstream | audit PIPE-009 lead |
| Region id 99 is hard-coded as excluded — verified by reading `reports.py:406` | Users cannot include 99 or express an exclusion list; the owner requires template-driven region selection (D-10) | gap G-03; DISC-09 |
| Error YAML written with `yaml.dump` — verified by probe (`!!python/tuple`, not `safe_load`-able) | Error artifacts cannot be re-loaded with safe tooling | audit OUT-001 |
| Module-import `sys.stdout.reconfigure()` | Fixed at HEAD — the call was moved into the `run` command (commit `706eed0`); importing the library no longer reconfigures stdout (verified: `app.py:45`) | audit ENT-001 (baseline only; fixed at HEAD) |
| `print()` in `core/` | Fixed at HEAD — replaced by `logger.info()` in commit `606c899` (verified: every remaining `print(` in `core/` is commented out) | audit OPS-003 / CFG-010 (baseline only; fixed at HEAD) |

Further limitations observed during this investigation, each traceable to code:

- `list-reports` lists the **user settings** folder, while `run` accepts any
  filesystem path — the README's example path (`settings/reports/…`) only
  resolves when the current directory happens to be the package directory.
- Unknown keys in the DEFAULT layer or a subtype block reach the vendor
  `build_task`, whose signature has no `**kwargs`: an unknown parameter raises
  `TypeError: build_task() got an unexpected keyword argument …` (verified
  against the installed `mediascope_api.mediavortex.tasks.MediaVortexTask.build_task`,
  `.venv/Lib/site-packages/mediascope_api/mediavortex/tasks.py:50-60`). Unknown
  keys in a *report* YAML are silently ignored instead, because the merge only
  overlays keys that already exist in `DATA_DEFAULTS` (verified: legacy keys
  `folder`/`category_name` have zero references in `core/`).
- The vendor library creates one `requests.Session` per
  `MediaVortexTask` (`.venv/…/core/net.py:73`, created in
  `MediaVortexTask.__init__`, `mediavortex/tasks.py:43-47`), and
  `AppService.get_connection` constructs a new `MediaVortexTask` per report
  entry (`services.py:127-135`, `:157-160`) — so each entry in a report file
  opens its own session; none is explicitly closed.
- Type safety is not enforced: `uv run mypy` (which checks both `src` and
  `tests` per `pyproject.toml` `files = ["src", "tests"]`) reports **56 errors
  in 7 files** at HEAD (verified on the investigation machine) and there is no
  CI gate for typechecking (CI runs only `ruff check src tests` + `pytest`),
  so typechecking cannot be treated as a safety net.

## Verification

Read-only probes executed for this document (scripts kept for reproduction in
`.ai/tmp/`):

| Script | What it established |
|---|---|
| `.ai/tmp/doc_probe_current_state.py` | 2 of 32 bundled report entries validate; the other 30 fail; both crosstab DEFAULT templates raise `ParserError`; `tv/simple`, `tv_reg/timeband`, `tv_reg/simple` are missing |
| `.ai/tmp/doc_probe_cli_surface.py` | `run --help` shows `--verbose` only (no `--no-verbose`); `run … --no-verbose` exits 2 |
| `.ai/tmp/doc_probe_cmdname.py` | `list_reports` exits 2; `list-reports` exits 0 |
| `.ai/tmp/doc_probe_paths.py` | Resolved user settings folder, connection config path, default export folder; instantiating the service with a missing user config raises `FileNotFoundError` |
| `.ai/tmp/doc_probe_git_defaults.py` | `tv/crosstab.yaml` parses at `3408747` and fails from `b04d177` onward |
| `.ai/tmp/doc_probe_worker_hang_classes.py` | The real `MediaReport.worker` with `RuntimeError` and `BadRequestError` raised in `send_task`: both hang `queue.join()` and print no traceback |
| `.ai/tmp/doc_probe_reverify_audit.py` | 11 defect claims re-executed against the code: `get_period` crash, inverted-period → zero intervals, zero-task silent run, worker `queue.join()` hang, swallowed duplicate write, `en_to_ru(None)` TypeError, `!!python/tuple` in the error YAML + `safe_load` ConstructorError, FAILED/CANCELLED status handling, done_files keying — 11/11 pass |
| `.ai/tmp/doc_probe_options_none.py` | A subtype with no defaults block leaves `slices`/`statistics`/`options` as `None`; `Data(**merged)` then raises a Pydantic `ValidationError` for `data_lang: en` (the `data_lang: ru` path crashes earlier in `en_to_ru`) |
| `.ai/tmp/doc_probe_vendor_build_task.py` + reading `.venv/Lib/site-packages/mediascope_api/` | Vendor `build_task` has no `**kwargs` (unknown key → `TypeError`); one `requests.Session` per `MediaVortexTask` (`core/net.py:73`) |
| `.ai/tmp/doc_probe_links.py` | All relative links and heading anchors in the documentation resolve (2 pre-existing broken example links in `doc-maintenance-rules.md` were found and left untouched) |
| `uv run pytest` | 31 passed (full suite, mocked — no network, no Mediascope credentials; coverage reported via `--cov`) |
| `uv run mypy` | 56 errors in 7 files (recorded as the current-tree typecheck state; mypy now checks both `src` and `tests` per `pyproject.toml` `files = ["src", "tests"]`) |
| Inspection of the machine's `output/` folder | Confirms the file naming convention `<start>_<end>_<audience>.csv.gz` and that real report runs happened historically; the files predate the current column-selection code |

Side effect of probing, disclosed for completeness: constructing `AppService`
once (`.ai/tmp/doc_probe_paths.py`) created the empty default export folder and
the log file under the user's own settings folder — the tool's designed
behavior, in the user's own configuration directory. Nothing in the repository
or the user's existing exports was modified.

Not verified (stated as limitations): no live Mediascope API call was made (no
credentials authorized for this investigation), so every API-interaction claim
above is code-traced — not runtime-observed. The audit corpus was used as a
lead source only, per the owner's ruling (D-05). Output-format claims come
from the code path and from the historical export files found under the
user's `output/` folder (file naming convention); those historical files
predate the current column-selection code and were not used to justify it.

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Product Overview](product-overview.md)
- [Data Flow Architecture](data-flow-architecture.md)
- [System Architecture](system-architecture.md)
- [Open Questions](open-questions.md)
- [Settings Layers Architecture](settings-layers-architecture.md)
- [Async Concurrency Architecture](async-concurrency-architecture.md)
- [Audit: data pipeline](../../.ai/audit/99-validation/04-data-pipeline-validated-findings.md)
- [Audit: config templates](../../.ai/audit/99-validation/05-config-templates-validated-findings.md)
- [Audit: config & secrets](../../.ai/audit/99-validation/02-config-secrets-validated-findings.md)
- [Audit: production ops](../../.ai/audit/99-validation/12-production-ops-validated-findings.md)
