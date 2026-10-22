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
| Library (sync) | `from mko_get_mediascope_data import run_report; run_report("report.json5", progress=None)` | `app.py:311-344` (accepts optional `progress`; delegates to `run_report_async` via `asyncio.run`). **Not safe in a running event loop** — if `asyncio.get_running_loop()` succeeds, `run_report` raises `RuntimeError` (naming `run_report_async` as the alternative) before constructing the coroutine, so no `run_report_async` coroutine is ever left unawaited. Synchronous scripts and Airflow tasks use this; Jupyter/IPython and other active-loop hosts use `await run_report_async(...)` instead. Re-exported lazily via PEP 562 `__getattr__` in `__init__.py:35-54` |
| Library (async) | `from mko_get_mediascope_data import run_report_async` | `app.py:290-308` (accepts optional `progress: ProgressReporter \| None`) — the supported entry point for Jupyter/IPython, FastAPI handlers, and other hosts with a running event loop |
| Library (setup) | `initialize_settings(force=False)` | `app.py:347-349` → `init_service.init_project` (`init_service.py:93`) |

Both library functions delegate to `AppService.run_report`
(`services.py:167`), the same method the CLI uses.

### CLI command surface (verified)

| Command | Arguments / options | Behavior |
|---|---|---|
| `init` | `--force / -f` | Copies the package's bundled `settings/` tree into the user settings folder via `_copy_templates` (`init_service.py:37-57`, force skip at `:53-54`); also runs the legacy credential rename `_migrate_old_credential_name` (`init_service.py:60-90`); without `--force`, existing files are kept |
| `run` | `REPORT` (required, must exist, must be a file), `--verbosity [QUIET|DEFAULT|VERBOSE|DEBUG|SILENT] / -v` (default: from config) | Runs the report(s) in the JSON5 file (`app.py:222-271`); bootstraps the service via `_get_app_service_or_exit()` (`app.py:206-219`) before configuring logging and dispatching to `asyncio.run(run_report_async(...))` |
| `list-reports` | — | Lists `*.json5` files (recursive) under the user settings `reports/` folder (`app.py:274-286`); also bootstraps via `_get_app_service_or_exit()` (`app.py:206-219`) |

Discrepancies between the README and the actual CLI, verified by invoking the
app with `typer.testing.CliRunner`:

- The command is `list-reports`; `list_reports` fails with exit code 2
  (Typer hyphenates command names).

### Console verbosity and progress bars

The `run` CLI command accepts a `--verbosity` (`-v`) flag that controls both log
output volume and whether progress bars are rendered:

| Mode | Logging level | Progress bars |
|---|---|---|
| `QUIET` | `WARNING` — only warnings and errors are shown | Suppressed (`NullProgressReporter`) |
| `DEFAULT` | `WARNING` — only warnings and errors are shown | Live 4-bar display (see below) |
| `VERBOSE` | `INFO` — task start, completion, and duration logs | Live 4-bar display (see below) |
| `DEBUG` | `DEBUG` — all internal state, API timing, and poll logs | Live 4-bar display (see below) |
| `SILENT` | `CRITICAL` — all console output suppressed | Suppressed (`NullProgressReporter`) |

`configure_logging_level` (`app.py:46-57`) sets the root logger level **and**
every handler level to the mapped value, so previously-capped handler levels
(e.g. `INFO` from `log_config.json5`) are lowered when `VERBOSE` or `DEBUG` is selected.
The flag does not affect the JSON5 file contents or pipeline logic — it is
purely a presentation concern. The mapping is defined by
`VerbosityMode.to_logging_level()` in `core/models.py:60-73`.

**Progress bars.** When verbosity is `DEFAULT`, `VERBOSE`, or `DEBUG`, the CLI creates a
`RichProgressReporter` (`app.py:60-204`) that renders four fixed-position bars — one per task lifecycle status (`QUEUED`, `SENT`, `COMPLETED`, `FAILED`).
Each bar is labeled `STATUS [██░░░░░] XX% (N/total)` and updates as tasks
transition between statuses. `report_started` / `report_completed` are called
in `MediaReport.processing_tasks` (`reports.py:227-265`) at the point the real
task count becomes known; `task_status_changed` is invoked at every status
transition in the worker loop (`reports.py:149-225`) and at the send
(`reports.py:422`) and extract (`reports.py:553`) boundaries. The Rich `Live`
region is started lazily on `report_started` and stopped in
`report_completed`, with `redirect_stdout`/`redirect_stderr` ensuring log
output flows above the bars. In `QUIET` mode no `Live` region is created and a
`NullProgressReporter` (a no-op `Protocol` implementation in
`core/progress.py:51-70`) is used instead, so the business-logic layer never
imports Rich.

**Library mode.** `run_report()` and `run_report_async()` accept an optional
`progress: ProgressReporter | None` parameter; when omitted,
`NullProgressReporter` is used, producing no terminal output. This keeps the
`core/` package free of any presentation dependency (`core/progress.py` defines
the `ProgressReporter` protocol without importing Rich).

### Configuration and where it lives

| Item | Location | Code |
|---|---|---|
| Package templates | `<package>/settings/` (app_config, log_config, connections, defaults, reports) | `paths.py:44-46` (`app_settings_dir`) |
| User settings | `platformdirs.user_config_dir("mko_get_mediascope_data")/settings/` — on the investigation machine this resolves to `…\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data\settings` | `paths.py:9` (`USER_DIR`), `:40-42` (`user_settings_dir`) |
| Export root | `app_config.json5 → export_folder`, resolved against the user settings folder; default `output` | `services.py:79-83`, `settings/app_config.json5` |
| Log file | `log_config.json5 → handlers.rotating_file.filename` (`log.log`), resolved against the user settings folder | `services.py:58-65` (`prepare_app`), `settings/log_config.json5` |
| Mediascope credentials | `<user settings>/connections/mediascope.json` (username, passw, client_id, client_secret, auth_server, root_url) | `paths.py:60-81` (`connection_config`), `settings/connections/` |

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
  `log_config.json5` and `app_config.json5` (`services.py:35-38` calls `prepare_app()`; `prepare_app` at `services.py:60-67`; `get_app_service`
  at `services.py:239-247`). If the user has not run `init`, the first command raises
  `FileNotFoundError` from `json5_to_dict` (`utils.py:371-395`). Both `run` and
  `list-reports` route bootstrap through `_get_app_service_or_exit()`
  (`app.py:206-219`): on `FileNotFoundError` it prints `❌ Error: …` followed by
  `Run 'mko-get-mediascope init' to create the settings directory.` and exits 1.
  Malformed-settings errors (`ValueError`, `pydantic.ValidationError`,
  `TypeError`) are **not** caught here — they propagate without an `init`
  suggestion, so they are reported distinctly from the missing-settings case.

### Scenario: `run` — step by step

Entry: `run <report.json5>` → `_get_app_service_or_exit()` (bootstrap boundary, `app.py:206-219`) → resolve `mode` from the `-v` flag or `app_config` (`app.py:248-252`) → `configure_logging_level(mode)` (`app.py:253`) → `asyncio.run(run_report_async(report, progress=reporter))` (`app.py:258-264`),
where `reporter` is a `RichProgressReporter` in DEFAULT/VERBOSE/DEBUG modes and
omitted (→ `NullProgressReporter`) in QUIET and SILENT modes. `run_report_async` delegates
to `AppService.run_report` (`services.py:167-233`).

1. **Parse and validate.** `get_report_settings` iterates the JSON5 list and
   constructs `ReportSettings(**entry)` per entry (`services.py:86-98`). A
   validation error in any entry aborts the whole file: the generator logs the
   exception and re-raises (`services.py:96-98`).
2. **Per entry** (`services.py:156-204`):
   a. `export_path = export_folder / report_settings.relative_path`, created if
   missing (`services.py:170-171`).
   b. `get_data_settings` builds the API parameters: the DEFAULT template for
   `(media, report_type)` is loaded (`services.py:100-114`, `get_default_data_settings`),
   then `merge_data_settings` overlays the subtype block and report deltas
   (`services.py:116-133`), then Russian slice names are substituted
   when `data_lang: ru` (`services.py:130-131`).
    c. A fresh `NetworkClient` is created and the vendor connection
    (`MediaVortexTask`) is built through it via `_get_connection_with_attempt_cleanup`
    (`services.py:268-286`; the connection is retried with transport-only
    `_CONSTRUCTOR_RETRIABLE` and the vendor sessions it allocates are captured for
    deterministic release in the `finally`).
    d. `ReportFactory` selects the task-generation and data-preparation
    strategies (`reports.py`, `ReportFactory`); a `TVMediaReport` is
    constructed and `create_report()` is awaited (`services.py`,
    `AppService.run_report`). A `NoReportFoundError` from `create_report()` —
    raised by `get_period` for an out-of-range period, or by
    `processing_tasks` for a zero-task fan-out — is caught here **per entry**:
    a warning names the entry and reason, no export path is appended for the
    skipped entry, and the loop continues to the next entry (PIPE-013).
    `AppService.run_report` also accumulates `attempted` / `exported` /
    `warned` counters and logs a summary at the end. Unexpected errors are
    **not** caught and still abort the run.
3. **Resolve the period** (`reports.py:229` → `TVMediaReport.get_period` at
    `reports.py:295-338`):
    - the available date window for the dataset is fetched from the API
      (`cats.get_availability_period`, filtered by `options.kitId`;
      `reports.py:265-277`, `:259-261`);
    - if `period.date_filter` is absent, the period is computed from
      `period.last_time` (`utils.get_last_period`, `utils.py:106-155`);
    - the requested period is clipped to the available window with
      `max/min` (`reports.py:322-325`);
    - if the clipped period is inverted (`start > end`), `NoReportFoundError` is
      raised (see [Errors, warnings, and edge cases](#errors-warnings-and-edge-cases));
    - if `multiple_files` is true (default), the period is split into intervals
      of `time_unit` (`utils.slice_period`, `utils.py:69-85`; the unit lives on
      `PeriodModel`, not `LastTimeModel`, since commit `183aed6`).
4. **Generate tasks** (`reports.py:452-454` → strategy `generate`):
    - `DefaultTVTaskStrategy` (`reports.py:481-506`): for each interval, for each
      `target_audiences` entry: task name = `<start>_<end>_<ascii audience>`,
      settings = merged parameters with `date_filter=[interval]` and
      `basedemo_filter=<audience filter>`. Tasks whose name already exists as an
      output file are skipped with a warning (`reports.py:507-509`).
    - `TVRegTaskStrategy` (`reports.py:513-569`): additionally iterates regions
      (explicit `regions` list, else all catalog regions from
      `cats.get_tv_region()` at `reports.py:516`), skips region id 99, adds
      `regionId IN (<id>)` to the company filter (`reports.py:531-534`), and
      appends the region's English name to the file name (`reports.py:546-554`).
      Only id 99 is excluded in the code; the owner recalled 99 **and** 100
      (recorded as DISC-09), and the region set is to move into the user
      template (D-10, gap G-03).
5. **Process tasks** (`reports.py:189-216` — `processing_tasks`, worker at `reports.py:149-188`): up to
    `max_tasks=100` worker coroutines pull tasks from a bounded queue. Each
    worker wraps `_process_item` (send → wait → extract) in `try/except/finally`
    (W-02, `reports.py:149-188`): `task_done()` is always called in `finally`, so
    `queue.join()` cannot hang on an exception. Per-item failures are converted to
    a failed task with an error JSON5 written via `json5.dump` (W-02,
    `tasks.py:7, 52`). The branching on `log_error` distinguishes "no data"
    warnings from "processing failure" error files (W-02, `reports.py:164-169`).
    `send_task` (`reports.py:358-382`):
    builds the API request via `network_client.call(builder, api_call=False)`
    (`reports.py:365-367`) — bypassing the semaphore/jitter/retry path since
    `build_task` is a pure CPU call; if no task key comes back, the task is marked
    failed (`reports.py:378-381`). The task-submission POST (`sender`) is
    config-driven via `SendTaskSettings`, keeping the semaphore + jitter: by
    default it enters the application retry loop for `RETRIABLE_EXCEPTIONS`
    (`ConnectionError`, `Timeout` including `ReadTimeout`) while HTTP 429/5xx
    are re-raised, and `retryable=False` makes it a single attempt. Retrying a
    selected transport error does not prove the request body was unapplied, and
    the vendor's independent `urllib3.Retry` layer can replay a POST regardless
    of application retries — unique task creation is best-effort (D-16).
    Structured `task_stage_complete stage=send` timing log replaces the prior
    `logger.info("=")` placeholder (`reports.py:372-377`). `wait_task`
    (`reports.py:383-440`): polls `get_status` every ~30 s (first poll), ramping to 50 s then 60 s (capped), with exponential backoff up to 60 s, until `DONE` / `FAILED` / `CANCELLED`, a poll deadline
    (`max_polls=60`, `poll_deadline=3600s`, `reports.py:384`), or retries exhausted
    (`reports.py:411-414`); `FAILED` / `CANCELLED` now marks the task as failed
    rather than proceeding to extraction (`reports.py:430-431`). Structured
    `task_stage_complete stage=wait` timing log at exit (`reports.py:435-440`).
    `extract_data` (`reports.py:439-486`): fetches the result JSON via
    `network_client.call(get_result)` (`reports.py:446-448`). There is **no
    row-count limit** — `resultBody` of any size is passed straight through to
    `result2table`. Converts to a DataFrame via
    `network_client.call(result2table, api_call=False)`
    (`NetworkClient.call`, `result2table`), applies the data strategy, and writes
    the CSV via `csv_to_file` with `mode="w"` — atomic publication (temp file +
    `os.replace`, PIPE-012) in `TVMediaReport.extract_data`. Structured
    `task_stage_complete stage=extract` timing log at exit.
 6. **Result:** `run_report` returns the list of export directories actually
    produced (`AppService.run_report`); entries skipped for a missing period
    (`NoReportFoundError`) are **not** in the list — they are warned and the CLI
    exits 0 (missing-period entries are non-fatal since PIPE-013). The CLI prints
    a success line and exits 0 (`app.py`).

### Inputs: the report JSON5 file

A report file is a **list** of report entries. Fields (from `ReportSettings`,
`models.py:87-115`, plus keys that fall through to the API parameters via the
merge):

| Field | Required | Meaning / accepted values |
|---|---|---|
| `report_type` | yes | Enum: `crosstab`, `timeband`, `simple` (`models.py:44-47`). Selects the DEFAULT template file |
| `media` | yes | Enum: `TV`, `TV_REG` (`models.py:39-41`). Selects the DEFAULT template file |
| `relative_path` | yes | Sub-folder under the export root for this report's outputs (`models.py:102`) |
| `period` | yes | Exactly one of `date_filter: [start, end]` or `last_time: {unit_count: N, include_current: bool}` must be present (`models.py:67-84`); `time_unit: d/w/m/y` (default `w`) controls the splitting frequency and is required when `multiple_files: true` |
| `report_subtype` | no (default `""`) | Selects a subtype block inside the DEFAULT template (`models.py:50-58`) |
| `data_lang` | no (default `ru`) | `ru` substitutes Russian slice names; `en` keeps the API's English names (`models.py:101`, `services.py:130-131`) |
| `compression` | no (default `{"method": "gzip"}`) | Output compression; drives the file extension (`models.py:98-100`, `utils.get_files_suffix` `utils.py:158-189`) |
| `multiple_files` | no (default `true`) | Split the period into `period.time_unit` intervals (`models.py:67-84`, `reports.py:333-334`) |
| `target_audiences` | no (default `{"not_set": null}`) | Map of audience name → basedemo filter string; one task per audience (`models.py:107-109`, `reports.py:494-506`) |
| `ad_filter` | no | Ad filter expression; overlaid onto the defaults (`models.py:106`, `services.py:126-128`) |
| `check_done` | no (default `true`) | Skip tasks whose output file already exists (`models.py:94`, `reports.py:75-76, 78-83`, `:507-509`, `:563-567`) |
| `slices`, `statistics`, `options`, `company_filter`, … | no | API parameters; overlaid onto the DEFAULT template when the key exists there (`services.py:126-128`) |

Reference example that validates end-to-end:
[`settings/reports/nat_tv_brands_tefal.json5`](../../src/mko_get_mediascope_data/settings/reports/nat_tv_brands_tefal.json5)
— the only bundled report whose entries pass `ReportSettings` validation
(verified by probe; lead: audit TPL-001).

### Inputs: the DEFAULT templates

`settings/defaults/<media>/<report_type>.json5` provides `DATA_DEFAULTS` (the
full Mediascope parameter set) plus optional subtype blocks. Bundled coverage
(verified at HEAD — all 6 combinations ship, all parse with `json5.load`):

| File | Status |
|---|---|
| `defaults/tv/crosstab.json5` | Parses cleanly at HEAD (fixed in TPL-007); 15 `DATA_DEFAULTS` keys; subtype blocks: `DYNAMICS_BY_SPOTS` (kitId=1, L59), `DYNAMICS_BY_SPOTS_DICT` (kitId=1, L87), `TOP_NAT_TV_ADVERTISERS` (no kitId — duplicate `issueType` at L107), `TOP_NAT_TV_PROGRAMS` (kitId=7, L128), `NAT_TV_CHANNELS_ATV` (kitId=1, L150), `NAT_TV_CHANNELS_BA` (kitId=7, L166), `NAT_TV_CHANNELS_SOC_DEM` (kitId=1, L181) |
| `defaults/tv/timeband.json5` | Parses; `DATA_DEFAULTS` only (includes `time_filter`), no subtype blocks |
| `defaults/tv/simple.json5` | Ships at HEAD (was missing, now added) |
| `defaults/tv_reg/crosstab.json5` | Parses cleanly at HEAD (fixed in TPL-007); 15 `DATA_DEFAULTS` keys; `options.kitId=3` in base |
| `defaults/tv_reg/timeband.json5` | Ships at HEAD (was missing, now added) |
| `defaults/tv_reg/simple.json5` | Ships at HEAD (was missing, now added) |

**Updated 2026-10-16 (W-01):** All 6 `(media, report_type)` DEFAULT combinations
are now shipped and parse correctly. All 32 bundled report entries pass
`ReportSettings` validation (verified by `test_bundled_templates_validate`).
Previously 30 of 32 failed and `tv/simple`, `tv_reg/timeband`,
`tv_reg/simple` were missing — these gaps are resolved by W-01 template repair.
The kitId comments in templates remain (DISC-11); the owner has no current
legend and will supply the mapping when available (D-20, Q-13).

**Updated 2026-10-16 (W-02):** With the W-02 error-semantics changes,
`NoReportFoundError` is now raised when a period has zero overlap with the
API window (was a silent empty run), so the "silent empty run" edge case in
the table below is resolved — see G-01.

### Outputs

| Output | Rule | Code |
|---|---|---|
| Data CSV | `<export folder>/<relative_path>/<task name>.csv[.gz]`; `task name = <start>_<end>_[<region>_]<ascii audience>`; colliding names (distinct audiences that transliterate identically) get a deterministic `"_" + sha256(collision_id)[:8]` suffix (PIPE-014) | `reports.py` (`TVMediaReport.extract_data`); `utils.csv_to_file` |
| CSV format | `sep=";"`, `decimal=","`, `index=False`, `encoding="utf-8-sig"`, `compression` from settings, **`mode="w"`** (overwrites; the `check_done` pre-filter prevents re-submission of already-written tasks), no timestamp suffix in the report pipeline. Published atomically: `csv_to_file` writes to a sibling temp file (`utils.CSV_TEMP_PREFIX = ".tmp_csv_"`) carrying the same format suffix, `fsync`s, then `os.replace`s onto the final path — a partial write is never observed on resume, and a failure leaves any previously published final file intact while removing only the temp artifact. **Atomic publication since PIPE-012.** | `utils.csv_to_file` |
| Columns | slices (plus `targetAudience` first when `target_audiences` is set, and `frequencyDistInterval` when frequency distribution is configured) followed by statistics; `prj_name` renamed to `targetAudience` | `reports.py:279-293`, `:581-583` |
| Frequency distribution | When `frequencyDistInterval` is among the columns, the base row and the per-interval frequencies are pivoted into one row per slice combination | `utils.pivot_df_frequency` `utils.py:236-274`, `reports.py:589` |
| Dictionary output | Subtypes containing `DICT` produce the melted term-dictionary table instead of the raw columns | `reports.py:599-652` |
| Error JSON5 | `<export path>/errors/<task name>.json5` with name, report type/subtype, target, interval, error, log_error, settings — written with `json5.dump` (W-02) | `tasks.py:27-57` |

### Errors, warnings, and edge cases

| Situation | Current behavior | Evidence |
|---|---|---|
| `run_report` called from a running event loop | `run_report` raises `RuntimeError` (the message names `run_report_async` as the alternative) before constructing its coroutine, so no `run_report_async` coroutine is ever left unawaited. Active-loop hosts (Jupyter/IPython, FastAPI handlers, async frameworks) must use `await run_report_async(...)` instead; `run_report` is for synchronous scripts and Airflow tasks only | `app.py:335-344` |
| Report entry fails validation | `ReportSettings` error is logged and re-raised; the whole file aborts; CLI prints the error, exit 1 | `services.py:86-98`, `app.py:258-271` |
| Missing/malformed DEFAULT template | `FileNotFoundError` / `ParserError` propagates from `json5_to_dict`; run aborts with exit 1 | `utils.py:381-389`, `services.py:100-114` |
| `date_filter` set, `last_time: null` | Works — `time_unit` is on `PeriodModel` (default `w`), so `get_period` reads it without dereferencing `last_time` | code (`reports.py:295-338`, `models.py:67-84`) |
| Requested period entirely outside the API window | Period is clipped (`TVMediaReport.get_period`); when the clipped period is inverted (`start > end`), `NoReportFoundError` is raised. Since PIPE-013 this is caught by `AppService.run_report` around `create_report()`: a warning logs the entry and reason, no export path is appended for the skipped entry, and the loop continues to the next entry — the CLI does **not** exit 1 for a single missing-period entry. **Resolved by W-02 (G-01); per-entry continuation since PIPE-013.** | `reports.py` (`get_period`); `services.py` (`AppService.run_report`); `errors.py:1-9` (lead: audit PIPE-005) |
| Requested period partially overlaps the API window | Period is clipped to the intersection; a `logger.warning()` names the uncovered remainder; the overlapping part is exported. **Resolved by W-02 (G-01)**. | `reports.py:331-336` (lead: audit PIPE-003, PIPE-004) |
| Zero tasks generated (e.g. `target_audiences: {}`) | `processing_tasks` raises `NoReportFoundError` when no tasks are generated (`total == 0`); since PIPE-013 this is caught by `AppService.run_report` and the entry is skipped with a warning while later entries continue. (The `enqueued == 0` "all already exported" case, by contrast, warns and returns without raising.) **Resolved by W-02 (G-01) — the run is now visible, not a silent empty exit; per-entry continuation since PIPE-013.** | `reports.py` (`processing_tasks`) (lead: audit PIPE-004) |
| Output file already exists | `csv_to_file` uses `mode="w"` (overwrites) and publishes atomically (temp file + `os.replace`); a failure leaves any prior final file intact. In the report pipeline, `check_done=True` pre-filters tasks by name match against existing files *before* submission (`Report.get_done_files`, which excludes `utils.CSV_TEMP_PREFIX` temp artifacts so a crashed write is never treated as published output), so already-exported tasks are skipped with a warning. `check_done=False` re-runs bypass the pre-filter and overwrite. **Updated 2026-10-16 (W-02): `mode` changed from `"x"` to `"w"`; the `FileExistsError`-swallow path is gone. Atomic publication and temp-exclusion from resume discovery since PIPE-012.** | `utils.csv_to_file`; `reports.py` (`Report.get_done_files`) (lead: audit PIPE-001 / OUT-003) |
| Task fails logically (no key, empty result, missing columns) | `task.status = False` → worker checks `log_error`: if `True` (processing failure), writes error JSON5 via `json5.dump` and logs a warning; if `False` (no data), logs a warning only, no error JSON5 (D-15). Run continues. **Updated 2026-10-16 (W-02): worker branching on `log_error` (`reports.py:164-169`).** Updated 2026-10-19 (W-13): oversized `resultBody` (formerly guarded by `max_result_rows`, 10 000) is **no longer rejected** — there is no row-count cap; the full result is always passed to `result2table` (`reports.py:454-457`). | `reports.py:164-169`, `tasks.py:27-57` |
| Vendor rejects a request (HTTP 4xx / auth / bad request) | Raised immediately by `NetworkClient`, no retry | `network.py:76-118` |
| Transient network failure | Retried up to 10 attempts with backoff + jitter inside `NetworkClient.call`; then `RuntimeError("Network retry limit exceeded")`. Retry/backoff log messages append `task=<task_name>` for traceability when `call()` is given a `task_name` — all per-task call sites in `reports.py` pass `task_name=task.name`, while the `get_connection` constructor call through `_get_connection_with_attempt_cleanup` passes `None` (`services.py:285`) | `network.py:135-296` (`call` + retry loop), `network.py:143` (`task_name` parameter), `network.py:183` (`task=<name>` context); `reports.py:452, 471, 540, 582, 591, 601` (`task_name=task.name`), `services.py:285` (`task_name=None`); covered by `tests/test_network.py` (run: passed) |
| Exception raised inside a worker | **Resolved by W-02 (G-02).** The worker now wraps `_process_item` in `try/except/finally`: `TimeoutError` and `Exception` are caught, the task is marked failed, an error JSON5 is written via `json5.dump`, and `task_done()` is always called in `finally` — so `queue.join()` cannot hang. The pre-W-02 behavior (worker death → silent hang, no traceback) is documented as historical context below. | `reports.py:149-188`; `tasks.py:7, 52` (lead: audit PIPE-002 / ASY-001) |
| Failure before the worker pool | Validation errors, missing/malformed DEFAULT templates, and connection/auth failures (`create_report` → `get_period` → `get_available_period`) abort the run with a printed error and exit code 1 — visible, no hang — **except** `NoReportFoundError` (out-of-range period or zero tasks), which `AppService.run_report` now catches per entry, warns, and continues (PIPE-013). | `services.py` (`AppService.run_report`); `app.py`; `reports.py` (`get_period`, `get_available_period`, `processing_tasks`) |
| `wait_task` never terminates | **Resolved by W-02 (G-02, slice 4).** `wait_task` now has two guard deadlines: `max_polls=60` iterations and `poll_deadline=3600.0` seconds (wall-clock). Additionally, the outer worker wraps `_process_item` in `asyncio.wait_for` when a `timeout` is set. A stuck task now marks itself failed and writes an error artifact instead of pinning a worker forever. | `reports.py:363-386, 403-408` (lead: audit PIPE-006) |
| Task status `FAILED`/`CANCELLED` | Now sets `task.status = False`, `task.log_error = True`, and `task.error = f"Task {terminal}"` before breaking the poll loop — `extract_data` is NOT attempted for a failed task. **Updated 2026-10-16 (W-02).** | `reports.py:403-408` (lead: audit PIPE-006) |
| Error JSON5 round-trip | Written with `json5.dump` (not `yaml.dump`), so tuples render as plain JSON5 and `json5.load` round-trips correctly. **Updated 2026-10-16 (W-02).** | `tasks.py:7, 52-57`; probe (lead: audit OUT-001) |
| `log_error` distinction | `log_error` is now **read** in the worker (`reports.py:165`): `True` → processing failure → error JSON5 written; `False` → "no data" → warning only, no error JSON5 (D-15). Written at 12 sites (`reports.py:177, 183, 356, 375, 381, 391, 402, 419, 427, 431, 436`; `tasks.py:20`), read at 1 site. **Updated 2026-10-16 (W-02: now consumed).** | `reports.py:164-169`; grep (lead: audit PIPE-009) |
| Duplicate output files on re-run | Prevented before submission by the `check_done` pre-filter (name match against existing files); `TVRegTaskStrategy` skip logs a warning (`reports.py:572-576`) | `reports.py:75-76, 507-509, 572-576` (lead: audit PIPE-011) |

**Historical note (pre-W-02, preserved for context):** Before W-02
(commit `3f3dd56`), the worker had **no `try/except`** — an exception raised in
`send_task`/`wait_task`/`extract_data` skipped `task_done()` and `queue.join()`
never returned. No traceback was printed; the symptom was a console that
stopped advancing and a process that never exited. This contradicted the
owner's "never stop an export" rule (D-09, D-17) — gap G-02. See the precision
note in [Open Questions](open-questions.md#precision-note-on-g-02-which-failures-hang-and-the-owners-observation).
Error YAML was previously written with `yaml.dump` (`!!python/tuple` tags) [pre-JSON5 migration],
`log_error` was write-only (5 writes, 0 reads), the poll loop had no deadline,
`FAILED`/`CANCELLED` left `status=True`, and `csv_to_file` used `mode="x"` with
a swallowed `FileExistsError`. All of these have been **resolved by W-02**.

### Configuration that affects behavior

| Setting | Effect |
|---|---|
| `app_config.json5 → export_folder` | Root of all outputs (default `output`, resolved under the user settings folder) |
| `log_config.json5` | Handlers/formatters/levels applied via `logging.config.dictConfig` at service construction (`services.py:58-65`). The named loggers `__main__`, `tests`, `app` never match the real module loggers (which use `__name__`, e.g. `mko_get_mediascope_data.core.reports`), so everything falls through to `root`. `AppService.reload_config()` re-reads the JSON5 files and re-applies logging (`services.py:67-77`, covered by tests, run: passed) |
| `compression` | File extension and on-disk compression |
| `data_lang` | `ru` → slice fields `*EName` are requested as `*Name` (`utils.en_to_ru`, `services.py:130-131`) |
| `multiple_files` + `time_unit` | Export granularity — the interval length used to split the period |
| `verbosity_mode` | Sets the log level (`QUIET`→WARNING, `DEFAULT`→WARNING, `VERBOSE`→INFO, `DEBUG`→DEBUG, `SILENT`→CRITICAL) and whether Rich progress bars render (QUIET and SILENT suppress them; DEFAULT/VERBOSE/DEBUG show 4 bars per report). See [Console verbosity and progress bars](#console-verbosity-and-progress-bars) for the full model. |
| `max_tasks` (100) and `NetworkClient` limits (5 concurrent HTTP calls, 10 attempts, 2 s base sleep) | Concurrency and retry behavior; see [Async Concurrency Architecture](async-concurrency-architecture.md) for the intentional two-level design |
| Row-count limit on results | **None.** `resultBody` of any size is always passed through to `result2table`/`DataFrame` materialization; there is no configurable or hardcoded cap (`max_result_rows` was removed in W-13). |

### Tests

The suite was executed: **189 passed** (`uv run pytest`, all mocked — no
network, no Mediascope credentials). All tests pass at HEAD, including
`test_create_report_skips_done_files` and the PIPE-012/013/014 tests
(`test_csv_to_file_atomic_*`, `test_done_files_excludes_temp_csv`,
`test_distinct_audience_names_cannot_collide_after_transliteration`,
`test_non_colliding_names_preserved_without_hash`, and
`test_no_overlap_entry_warns_and_continues_later_entries`). One test,
`test_run_report_with_real_models`, carries the
`@pytest.mark.slow` marker and can be excluded with
`uv run pytest -m "not slow"` (188 passed, 1 deselected). Coverage is
reported by default via `--cov=src/mko_get_mediascope_data --cov-report=term-missing`
(configured in `pyproject.toml`).

| Test file | What it pins down |
|---|---|
| `tests/test_app.py` | CLI `init --help`; lazy re-export of `run_report`, `run_report_async`, `initialize_settings` via PEP 562 `__getattr__`; package import does not bootstrap `AppService`; re-exported functions are identical objects to those in `app.py` |
| `tests/test_init_credentials.py` | After `init_project()`, `connections/mediascope.json` exists and is resolvable via `AppPaths.connection_config` (verifies the legacy-rename migration, W-06) |
| `tests/test_models.py` | `ReportSettings` validation with the current field names; `PeriodModel` exclusivity rules; `merge_data_settings` does not call `en_to_ru` when `data_lang="en"`; all 32 bundled entries pass `ReportSettings` validation; all 6 `ReportSubtype` values have matching DEFAULT keys |
| `tests/test_network.py` | Retry exhaustion raises `MaxRetriesExceeded("Network retry limit exceeded")` (subclasses `RuntimeError`); `mock_network_client` fixture retries on retryable errors; success returns the function result; `BadRequestError` (4xx) and `AuthorizationError` (401) are re-raised without retry; HTTP 429 (`TooManyRequestsError`) is retried with backoff; `TimeoutError` from `asyncio.wait_for` (`call_timeout` ceiling) is retryable; `retryable=False` makes the task-submission POST non-retried (EXT-001); `call_timeout` caps wall-clock via `asyncio.wait_for` (EXT-002); `retry_5xx` toggle controls 5xx retry (EXT-005); `log_errors_detail` redacts PII (args/kwargs) from error logs (EXT-008); semaphore limits concurrent in-flight calls; default `max_attempts=10`; PERF-001: `api_call=False` short-circuits `NetworkClient.call` to direct `_invoke` (no jitter, no semaphore, no retry, timeout-bounded) — verified by `test_cpu_local_calls_bypass_semaphore_and_jitter` |
| `tests/test_reports.py` | `DefaultTVTaskStrategy` task naming; `DictDataStrategy` output columns |
| `tests/test_reports_lifecycle.py` | Real `create_report` → `get_period` → `processing_tasks` → `worker` → `extract_data` → `csv_to_file` pipeline (unmocked except the `MediaVortexTask` double) writes CSV and the queue drains without hanging; `NoReportFoundError` raised when zero tasks enqueued; timeout path produces error JSON5; `test_large_result_is_processed` verifies there is **no** row-count cap — an oversized `resultBody` (> 10 000 rows) is processed through `result2table` and produces a CSV with no error artifact; PERF-003: `test_worker_emits_per_task_duration` verifies structured `task_stage_complete`/`task_complete` timing logs; **temp CSV files are excluded from resume discovery via `get_done_files` (PIPE-012); collision-safe task identity via `Report._disambiguate_task_names` — unique names stay verbatim, colliding names gain a deterministic `sha256(collision_id)[:8]` suffix (PIPE-014)** |
| `tests/test_services.py` | Full `run_report` pipeline end-to-end (slow; doubled `MediaVortexTask`, real `AppService`); `app_config` caching; `reload_config()` invalidates cache, is safe before first access, reapplies logging, and re-resolves `export_folder`; missing credentials raise `FileNotFoundError`; **a no-overlap entry is warned and skipped while later entries still export (`test_no_overlap_entry_warns_and_continues_later_entries`, PIPE-013)** |
| `tests/test_utils.py` | Date parsing; period slicing for `m` and `w`; gzip CSV writing; suffix mapping; `csv_to_file` writes into the report directory (`tmp_report_dir` fixture); **atomic publication** — final-file publication + temp-artifact cleanup/preservation on failure (`test_csv_to_file_atomic_success_publishes_final`, `test_csv_to_file_atomic_failure_preserves_prior_final`, PIPE-012) |
| `tests/test_progress.py` | `NullProgressReporter` no-op semantics (all methods complete without raising); `ProgressReporter` protocol `isinstance` check (runtime_checkable); `TVTask.task_status` default is `QUEUED` |
| `tests/test_verbosity.py` | `VerbosityMode.to_logging_level()` mapping (QUIET→WARNING, DEFAULT→WARNING, VERBOSE→INFO, DEBUG→DEBUG, SILENT→CRITICAL); `AppSettings.verbosity_mode` default and Pydantic string/enum coercion; `configure_logging_level()` sets root logger and all handlers; CLI `--verbosity` flag integration via `CliRunner` |
| `tests/test_reports_progress.py` | `ProgressReporter` interaction at send→extract boundaries — `send_task` emits QUEUED→SENT, `extract_data` emits SENT→COMPLETED; failure path emits SENT→FAILED; `TVMediaReport` defaults to `NullProgressReporter` when no reporter is passed |

**Updated 2026-10-16:** W-02 added `test_create_report_timeout_does_not_hang`
(verifies the worker does not hang on a non-terminal poll and writes an error
JSON5) and `test_create_report_skips_done_files` (verifies `check_done` skip
behavior — the test was updated to account for the `NoReportFoundError` change
by testing the warning-only path when `check_done=True` and files exist). The
lifecycle tests now exercise the real `worker` try/except/finally path, not just
the happy path.

**Updated 2026-10-18:** Phase 09 (External API) added tests covering
`NetworkClient.call` behavior and the report lifecycle:
- `retryable=False` on the task-submission POST (EXT-001)
- `call_timeout` capping wall-clock via `asyncio.wait_for` (EXT-002)
- `sleep_time=30` on `get_status` polling (EXT-004 fixed: restored from 2 to 30)
- HTTP 429 retry (`TooManyRequestsError`, EXT-005); the `retry_5xx` toggle for 5xx
  classification (EXT-005)
- PII redaction of args/kwargs via `log_errors_detail` (EXT-008)
- EXT-003 regression tests (`test_wait_task_resets_status_on_failed`,
  `test_wait_task_resets_status_on_cancelled`, `test_worker_does_not_log_ready_on_failed`)
  verifying that FAILED/CANCELLED terminal statuses reset `task.status=False` and are
  not logged as "is ready"

See the `tests/test_network.py` and `tests/test_reports_lifecycle.py` rows above.

**Updated 2026-10-19:** Phase 13 (Performance) added tests covering PERF-001 and
PERF-003:
- PERF-001: `test_cpu_local_calls_bypass_semaphore_and_jitter` — verifies `api_call=False`
  short-circuits to direct `_invoke` (no jitter sleep, no semaphore, no retry, but
  timeout-bounded via `asyncio.wait_for`)
- PERF-003: `test_worker_emits_per_task_duration` — verifies structured
  `task_stage_complete`/`task_complete` timing logs are emitted at the expected stages
  (send, wait, extract, and end-to-end)

**Updated 2026-10-20:** Console verbosity and progress bars (Blocks 4–5) added
`tests/test_progress.py`, `tests/test_verbosity.py`, and
`tests/test_reports_progress.py` (27 new tests) covering `VerbosityMode` and
`TaskStatus` enums, `configure_logging_level()`, the CLI `--verbosity` flag, the
`ProgressReporter` protocol and `NullProgressReporter`, `TVTask.task_status`
defaults, and progress-reporter interactions at the send/extract task-status
boundaries.

**Updated 2026-10-22:** Data-pipeline hardening (PIPE-012/013/014) and tests:
- PIPE-012 — `csv_to_file` now publishes atomically: a sibling temp file
  (`utils.CSV_TEMP_PREFIX = ".tmp_csv_"`) is `fsync`'d then `os.replace`'d onto the
  final path; a partial write is never observable on resume and a failure leaves
  any previously published final file intact while removing only the temp
  artifact. `Report.get_done_files` excludes temp files from resume discovery.
  Tests: `test_csv_to_file_atomic_success_publishes_final`,
  `test_csv_to_file_atomic_failure_preserves_prior_final`,
  `test_done_files_excludes_temp_csv`.
- PIPE-013 — `AppService.run_report` catches `NoReportFoundError` from
  `create_report()` per entry, logs a warning naming the entry and reason, skips
  the entry (no export path appended), and continues to the next entry; the
  function accumulates `attempted`/`exported`/`warned` counters and logs a
  summary. Unexpected errors still abort. Test:
  `test_no_overlap_entry_warns_and_continues_later_entries`.
- PIPE-014 — collision-safe task identity: each strategy records the original
  pre-transliteration identity as `TVTask._collision_id`, and
  `Report._disambiguate_task_names` (called in `processing_tasks` after the
  zero-task check, before `report_started`) appends a deterministic
  `"_" + sha256(collision_id)[:8]` to colliding names; non-colliding legacy names
  stay verbatim (resume unchanged). Tests:
  `test_distinct_audience_names_cannot_collide_after_transliteration`,
  `test_non_colliding_names_preserved_without_hash`.

Behaviors with **no test coverage** — these code paths were therefore never
executed during this investigation: `TVRegTaskStrategy` (tests use `media=TV`
only), the `run` CLI happy path (actual report execution against the API — the
`--verbosity` flag itself is now unit-tested, but the full `run` command path
is not), `list-reports`, and any end-to-end run against the real API.

The following behaviors are **now exercised** by the lifecycle and hermetic
pipeline tests: `get_period` (including the period-clipping
logic, `NoReportFoundError` for out-of-range, and `logger.warning` for partial
overlap), `wait_task` (poll deadline), `en_to_ru` (indirectly, via the
`data_lang="ru"` path in `test_run_report_with_real_models`), queue completion
accounting (`task_done` / `queue.join`), and the worker's `try/except/finally`
isolation.

## Known defects and limitations

The defects below were each verified against the code by this investigation
(directly or by executing the probe scripts listed in
[Verification](#verification)). The audit corpus
(`.ai/audit/99-validation/`) is a lead source only — per the owner's ruling
(D-05) it is not authoritative; the IDs in the last column are pointers for
background reading, not evidence.

| Defect (code-verified) | Impact | Also recorded as |
|---|---|---|
| 30 of 32 bundled report entries fail `ReportSettings` validation (stale `period_type`/`period_num`, stale `report_type` values such as `TV_CROSSTAB`, missing `media`/`relative_path`) — verified by probe | A user following the README's example (`nat_tv_brands_last.json5`) cannot run it; the owner requires all bundled templates to be runnable (D-06) | **RESOLVED — W-01 (2026-10-16):** all 32 entries now pass validation; all 6 DEFAULT templates ship; both crosstab defaults parse correctly. audit TPL-001 / CFG-001 leads; gap G-05 → CLOSED |
| Both bundled crosstab DEFAULT templates were malformed YAML [pre-JSON5 migration] — verified by probe; the TV regression entered with commit `b04d177` | Fixed at HEAD — templates now parse at HEAD | audit TPL-004 (+ DISC-07); **RESOLVED — W-01 (2026-10-16)** |
| CSV write collision is swallowed and the task reports "ready" — verified by probe at the audit baseline (`mode="x"`, `FileExistsError` swallowed) | A re-run without `check_done` silently produces no file. **Resolved by W-02 (2026-10-16):** `csv_to_file` now uses `mode="w"` (overwrites); the `FileExistsError`-swallow path is removed. **Resolved by PIPE-012:** publication is now atomic — `csv_to_file` writes to a sibling temp file (`utils.CSV_TEMP_PREFIX = ".tmp_csv_"`) with the matching format suffix, `fsync`s, then `os.replace`s onto the final path; a partial write is never observed on resume (`Report.get_done_files` also excludes temp files), and a failure leaves any previously published final file intact while removing only the temp artifact. | audit PIPE-001 / OUT-003; PIPE-012 |
| Worker has no exception isolation; `queue.join()` hangs on any raise — verified by probe at the audit baseline | A single transient failure can stall the whole run instead of failing it. **Resolved by W-02 (2026-10-16):** the worker now wraps `_process_item` in `try/except/finally` with `task_done()` in `finally`; exceptions are caught, the task is marked failed, and an error JSON5 is written (`reports.py:149-188`). | audit PIPE-002 / ASY-001; gap G-02 → RESOLVED |
| Zero-task and out-of-window periods exit 0 with a success message — verified by probe at the audit baseline | An analyst cannot distinguish "no data" from "done". **Resolved by W-02 (2026-10-16):** `get_period` raises `NoReportFoundError` for entirely out-of-range periods; `processing_tasks` raises `NoReportFoundError` when zero tasks are enqueued; partial overlap emits a `logger.warning` (`reports.py:327-330, 213-218`). **Refined by PIPE-013:** that `NoReportFoundError` is now caught per entry by `AppService.run_report`, which warns and continues to the next entry instead of aborting the run. | audit PIPE-004 / PIPE-005; gap G-01 → RESOLVED |
| Poll loop has no deadline — verified by reading `wait_task` at the audit baseline | A stuck vendor task pins a worker forever. **Resolved by W-02 (2026-10-16):** `wait_task` now has `max_polls=60` and `poll_deadline=3600.0` guards (`reports.py:363`, `373-386`). | audit PIPE-006; gap G-02 slice 4 → RESOLVED |
| `log_error` is write-only — verified by grep (5 writes, 0 reads) at the audit baseline | "Empty result" and "error" are indistinguishable downstream. **Resolved by W-02 (2026-10-16):** `log_error` is now read in the worker branching at `reports.py:165`; `log_error=True` → error JSON5, `log_error=False` → warning only. | audit PIPE-009; gap G-01/G-02 → RESOLVED |
| Region id 99 is hard-coded as excluded — verified by reading `reports.py:539` | Users cannot include 99 or express an exclusion list; the owner requires template-driven region selection (D-10) | gap G-03; DISC-09 |
| Error YAML written with `yaml.dump` [pre-JSON5 migration] — verified by probe (`!!python/tuple`, not `safe_load`-able) at the audit baseline | Error artifacts cannot be re-loaded with safe tooling. **Resolved by W-02 (2026-10-16):** `tasks.py` now uses `json5.dump` (`tasks.py:7, 52-57`). | audit OUT-001; gap G-02 → RESOLVED |
| Module-import `sys.stdout.reconfigure()` | Fixed at HEAD — the call was moved into the `run` command (commit `706eed0`); importing the library no longer reconfigures stdout (verified: `app.py:240-241`) | audit ENT-001 (baseline only; fixed at HEAD) |
| `print()` in `core/` | Fixed at HEAD — replaced by `logger.info()` in commit `606c899` (verified: no active `print(` in `core/`) | audit OPS-003 / CFG-010 (baseline only; fixed at HEAD) |

Further limitations observed during this investigation, each traceable to code:

- `list-reports` lists the **user settings** folder, while `run` accepts any
  filesystem path — the README's example path (`settings/reports/…`) only
  resolves when the current directory happens to be the package directory.
- Unknown keys in the DEFAULT layer or a subtype block reach the vendor
  `build_task`, whose signature has no `**kwargs`: an unknown parameter raises
  `TypeError: build_task() got an unexpected keyword argument …` (verified
  against the installed `mediascope_api.mediavortex.tasks.MediaVortexTask.build_task`,
  `.venv/Lib/site-packages/mediascope_api/mediavortex/tasks.py:50-60`). Unknown
  keys in a *report* JSON5 are silently ignored instead, because the merge only
  overlays keys that already exist in `DATA_DEFAULTS` (verified: legacy keys
  `folder`/`category_name` have zero references in `core/`).
- The vendor library creates one `requests.Session` per
  `MediaVortexTask` (`.venv/…/core/net.py:73`, created in
  `MediaVortexTask.__init__`, `mediavortex/tasks.py:43-47`), and
  `AppService.get_connection` constructs a new `MediaVortexTask` per report
  entry (`services.py:176-206`, invoked through `_get_connection_with_attempt_cleanup`
  at `services.py:281-286`). Each entry opens its own sessions, which are captured
  by `core/vendor_lifecycle.py` (`capture_sessions` / `capture_singleton_sessions`)
  and deterministically released in `AppService.run_report`'s `finally` block via
  `release_sessions` (`services.py:330-335`), guarded by
  `NetworkClient.has_outstanding_deferred()` so a session is never closed beneath
  in-flight deferred work (EXT-010, EXT-011).
- Type safety is now enforced: `uv run mypy src tests ci` passes with **0 errors**
  at HEAD (verified on the investigation machine; down from 56 errors in 7 files
  at the audit baseline). mypy checks `src`, `tests`, and `ci` per
  `pyproject.toml` (`files = ["src", "tests", "ci"]`). CI runs
  `ruff check src tests ci`, `mypy src tests ci`, and `pytest`
  (`.github/workflows/ci.yml`).

## Verification

Read-only probes executed for this document (scripts kept for reproduction in
`.ai/tmp/`):

| Script | What it established |
|---|---|
| `.ai/tmp/doc_probe_current_state.py` | (Re-run after W-01/W-02) all 32 bundled report entries validate; all 6 DEFAULT templates parse; `tv/simple`, `tv_reg/timeband`, `tv_reg/simple` now ship |
| `.ai/tmp/doc_probe_cli_surface.py` | `run --help` shows `--verbosity [QUIET|DEFAULT|VERBOSE|DEBUG|SILENT] / -v` (replaces the former `--verbose` flag); no `--verbose` or `--no-verbose` flag exists |
| `.ai/tmp/doc_probe_cmdname.py` | `list_reports` exits 2; `list-reports` exits 0 |
| `.ai/tmp/doc_probe_paths.py` | Resolved user settings folder, connection config path, default export folder; instantiating the service with a missing user config raises `FileNotFoundError` |
| `.ai/tmp/doc_probe_git_defaults.py` | `tv/crosstab.json5` parses at `3408747` and fails from `b04d177` onward |
| `.ai/tmp/doc_probe_worker_hang_classes.py` | (Pre-W-02 probe) The real `MediaReport.worker` with `RuntimeError` and `BadRequestError` raised in `send_task`: both hang `queue.join()` and print no traceback — behavior since fixed by W-02 try/except/finally |
| `.ai/tmp/doc_probe_reverify_audit.py` | 11 defect claims re-executed against the code: `get_period` crash, inverted-period → zero intervals, zero-task silent run, worker `queue.join()` hang, swallowed duplicate write, `en_to_ru(None)` TypeError, `!!python/tuple` in the error JSON5 + `load` ConstructorError, FAILED/CANCELLED status handling, done_files keying — 11/11 pass |
| `.ai/tmp/doc_probe_options_none.py` | A subtype with no defaults block leaves `slices`/`statistics`/`options` as `None`; `Data(**merged)` then raises a Pydantic `ValidationError` for `data_lang: en` (the `data_lang: ru` path crashes earlier in `en_to_ru`) |
| `.ai/tmp/doc_probe_vendor_build_task.py` + reading `.venv/Lib/site-packages/mediascope_api/` | Vendor `build_task` has no `**kwargs` (unknown key → `TypeError`); one `requests.Session` per `MediaVortexTask` (`core/net.py:73`) |
| `.ai/tmp/doc_probe_links.py` | All relative links and heading anchors in the documentation resolve |
| `uv run pytest` | **189 passed** (full suite, mocked — no network, no Mediascope credentials; 188 passed with the `@slow` `test_run_report_with_real_models` deselected; coverage reported via `--cov`) |
| `uv run mypy` | **0 errors** (`uv run mypy src tests ci` passes with zero errors; mypy checks `src`, `tests`, and `ci` per `pyproject.toml` `files = ["src", "tests", "ci"]`) |
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
- [Audit: data pipeline](../../.ai/audit/99-validation/04-data-pipeline-validated-findings_done.md)
- [Audit: config templates](../../.ai/audit/99-validation/05-config-templates-validated-findings.md)
- [Audit: config & secrets](../../.ai/audit/99-validation/02-config-secrets-validated-findings.md)
- [Audit: production ops](../../.ai/audit/99-validation/12-production-ops-validated-findings.md)
