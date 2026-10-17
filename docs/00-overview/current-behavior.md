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
| Library (sync) | `from mko_get_mediascope_data import run_report; run_report("report.yaml")` | `app.py:90-98`, re-exported lazily via PEP 562 `__getattr__` in `__init__.py:17-30` |
| Library (async) | `from mko_get_mediascope_data import run_report_async` | `app.py:80-87` |
| Library (setup) | `initialize_settings(force=False)` | `app.py:101-103` → `init_project` |

Both library functions delegate to `AppService.run_report`
(`services.py:156`), the same method the CLI uses.

### CLI command surface (verified)

| Command | Arguments / options | Behavior |
|---|---|---|
| `init` | `--force / -f` | Copies the package's bundled `settings/` tree into the user settings folder via `_copy_templates` (`init_service.py:37-57`, force skip at `:53-54`); also runs the legacy credential rename `_migrate_old_credential_name` (`init_service.py:60-90`); without `--force`, existing files are kept |
| `run` | `REPORT` (required, must exist, must be a file), `--verbose / -v` (default true) | Runs the report(s) in the YAML file (`app.py:40-62`) |
| `list-reports` | — | Lists `*.yaml` files (recursive) under the user settings `reports/` folder (`app.py:66-77`) |

Discrepancies between the README and the actual CLI, verified by invoking the
app with `typer.testing.CliRunner`:

- The command is `list-reports`; `list_reports` fails with exit code 2
  (Typer hyphenates command names).
- `--no-verbose` is not a valid option; `run report.yaml --no-verbose` fails
  with exit code 2, although the README documents it.

### Configuration and where it lives

| Item | Location | Code |
|---|---|---|
| Package templates | `<package>/settings/` (app_config, log_config, connections, defaults, reports) | `paths.py:44-46` (`app_settings_dir`) |
| User settings | `platformdirs.user_config_dir("mko_get_mediascope_data")/settings/` — on the investigation machine this resolves to `…\AppData\Local\mko_get_mediascope_data\mko_get_mediascope_data\settings` | `paths.py:9` (`USER_DIR`), `:40-42` (`user_settings_dir`) |
| Export root | `app_config.yaml → export_folder`, resolved against the user settings folder; default `output` | `services.py:79-83`, `settings/app_config.yaml` |
| Log file | `log_config.yaml → handlers.rotating_file.filename` (`log.log`), resolved against the user settings folder | `services.py:58-65` (`prepare_app`), `settings/log_config.yaml` |
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
  `log_config.yaml` and `app_config.yaml` (`services.py:33-36` calls `prepare_app()`, `services.py:58-65`; `get_app_service`
at `services.py:210`). If the user has not run `init`, the first command raises
  `FileNotFoundError` from `yaml_to_dict` (`utils.py:365-389`) and the CLI
  prints `❌ Error: …` and exits 1 (`app.py:60-62`).

### Scenario: `run` — step by step

Entry: `run <report.yaml>` → `asyncio.run(get_app_service().run_report(path))`
(`app.py:57`).

1. **Parse and validate.** `get_report_settings` iterates the YAML list and
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
   (`MediaVortexTask`) is built through it (`services.py:175-179`, `:140-154`).
   d. `ReportFactory` selects the task-generation and data-preparation
   strategies (`reports.py:30-50`); a `TVMediaReport` is constructed and
   `create_report()` is awaited (`services.py:181-194`).
3. **Resolve the period** (`reports.py:229` → `TVMediaReport.get_period` at
    `reports.py:295-338`):
    - the available date window for the dataset is fetched from the API
      (`cats.get_availability_period`, filtered by `options.kitId`, default 1;
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
    a failed task with an error YAML written via `yaml.safe_dump` (W-02,
    `tasks.py:7, 52`). The branching on `log_error` distinguishes "no data"
    warnings from "processing failure" error files (W-02, `reports.py:164-169`).
    `send_task` (`reports.py:344-361`):
    builds the API request; if no task key comes back, the task is marked failed
    (`reports.py:358-361`). `wait_task` (`reports.py:363-411`): polls `get_status`
    every ~2 s with exponential backoff up to 60 s, until `DONE` / `FAILED` /
    `CANCELLED`, a poll deadline (`max_polls=60`, `poll_deadline=3600s`,
    `reports.py:363`), or retries exhausted (`reports.py:388-393`); `FAILED` /
    `CANCELLED` now marks the task as failed rather than proceeding to extraction
    (`reports.py:399-408`). `extract_data` (`reports.py:412-449`): fetches the
    result JSON, converts to a DataFrame, applies the data strategy, and writes
    the CSV via `csv_to_file` with `mode="w"` (`reports.py:442-449`).
6. **Result:** `run_report` returns the list of export directories
    (`services.py:204`); the CLI prints a success line and exits 0
   (`app.py:58`).

### Inputs: the report YAML file

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
[`settings/reports/nat_tv_brands_tefal.yaml`](../../src/mko_get_mediascope_data/settings/reports/nat_tv_brands_tefal.yaml)
— the only bundled report whose entries pass `ReportSettings` validation
(verified by probe; lead: audit TPL-001).

### Inputs: the DEFAULT templates

`settings/defaults/<media>/<report_type>.yaml` provides `DATA_DEFAULTS` (the
full Mediascope parameter set) plus optional subtype blocks. Bundled coverage
(verified at HEAD — all 6 combinations ship, all parse with `yaml.safe_load`):

| File | Status |
|---|---|
| `defaults/tv/crosstab.yaml` | Parses cleanly at HEAD (fixed in TPL-007); 15 `DATA_DEFAULTS` keys; subtype blocks: `DYNAMICS_BY_SPOTS` (kitId=1, L59), `DYNAMICS_BY_SPOTS_DICT` (kitId=1, L87), `TOP_NAT_TV_ADVERTISERS` (no kitId — duplicate `issueType` at L107), `TOP_NAT_TV_PROGRAMS` (kitId=7, L128), `NAT_TV_CHANNELS_ATV` (kitId=1, L150), `NAT_TV_CHANNELS_BA` (kitId=7, L166), `NAT_TV_CHANNELS_SOC_DEM` (kitId=1, L181) |
| `defaults/tv/timeband.yaml` | Parses; `DATA_DEFAULTS` only (includes `time_filter`), no subtype blocks |
| `defaults/tv/simple.yaml` | Ships at HEAD (was missing, now added) |
| `defaults/tv_reg/crosstab.yaml` | Parses cleanly at HEAD (fixed in TPL-007); 15 `DATA_DEFAULTS` keys; `options.kitId=3` in base |
| `defaults/tv_reg/timeband.yaml` | Ships at HEAD (was missing, now added) |
| `defaults/tv_reg/simple.yaml` | Ships at HEAD (was missing, now added) |

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
| Data CSV | `<export folder>/<relative_path>/<task name>.csv[.gz]`; `task name = <start>_<end>_[<region>_]<ascii audience>` | `reports.py:442-449`, `utils.csv_to_file` `utils.py:192-220` |
| CSV format | `sep=";"`, `decimal=","`, `index=False`, `encoding="utf-8-sig"`, `compression` from settings, **`mode="w"`** (overwrites; the `check_done` pre-filter prevents re-submission of already-written tasks), no timestamp suffix in the report pipeline | `utils.py:211-220` |
| Columns | slices (plus `targetAudience` first when `target_audiences` is set, and `frequencyDistInterval` when frequency distribution is configured) followed by statistics; `prj_name` renamed to `targetAudience` | `reports.py:279-293`, `:581-583` |
| Frequency distribution | When `frequencyDistInterval` is among the columns, the base row and the per-interval frequencies are pivoted into one row per slice combination | `utils.pivot_df_frequency` `utils.py:236-274`, `reports.py:589` |
| Dictionary output | Subtypes containing `DICT` produce the melted term-dictionary table instead of the raw columns | `reports.py:599-652` |
| Error YAML | `<export path>/errors/<task name>.yaml` with name, report type/subtype, target, interval, error, log_error, settings — written with `yaml.safe_dump` (W-02) | `tasks.py:27-57` |

### Errors, warnings, and edge cases

| Situation | Current behavior | Evidence |
|---|---|---|
| Report entry fails validation | `ReportSettings` error is logged and re-raised; the whole file aborts; CLI prints the error, exit 1 | `services.py:86-98`, `app.py:60-62` |
| Missing/malformed DEFAULT template | `FileNotFoundError` / `ParserError` propagates from `yaml_to_dict`; run aborts with exit 1 | `utils.py:381-389`, `services.py:100-114` |
| `date_filter` set, `last_time: null` | Works — `time_unit` is on `PeriodModel` (default `w`), so `get_period` reads it without dereferencing `last_time` | code (`reports.py:295-338`, `models.py:67-84`) |
| Requested period entirely outside the API window | Period is clipped (`reports.py:322-325`); when the clipped period is inverted (`start > end`), `NoReportFoundError` is raised and the CLI prints `❌ Error: …` and exits 1 — visible, not silent. **Resolved by W-02 (G-01)**. | `reports.py:327-330`; `errors.py:1-9` (lead: audit PIPE-005) |
| Requested period partially overlaps the API window | Period is clipped to the intersection; a `logger.warning()` names the uncovered remainder; the overlapping part is exported. **Resolved by W-02 (G-01)**. | `reports.py:331-336` (lead: audit PIPE-003, PIPE-004) |
| Zero tasks generated (e.g. `target_audiences: {}` after repair) | `processing_tasks` raises `NoReportFoundError` when `enqueued == 0`; CLI prints the error, exits 1. **Resolved by W-02 (G-01)** — the run is now visible, not a silent empty exit. | `reports.py:213-218` (lead: audit PIPE-004) |
| Output file already exists | `csv_to_file` uses `mode="w"` (overwrites). In the report pipeline, `check_done=True` pre-filters tasks by name match against existing files *before* submission (`reports.py:75-76`, `:507-509`, `:563-567`), so already-exported tasks are skipped with a warning. `check_done=False` re-runs bypass the pre-filter and overwrite. **Updated 2026-10-16 (W-02): `mode` changed from `"x"` to `"w"`; the old `FileExistsError`-swallow path is gone (`utils.py:214`).** | `utils.py:192-220`; `reports.py:75-76, 507-509, 563-567` (lead: audit PIPE-001 / OUT-003) |
| Task fails logically (no key, empty result, missing columns) | `task.status = False` → worker checks `log_error`: if `True` (processing failure), writes error YAML via `yaml.safe_dump` and logs a warning; if `False` (no data), logs a warning only, no error YAML (D-15). Run continues. **Updated 2026-10-16 (W-02): worker branching on `log_error` (`reports.py:164-169`).** | `reports.py:164-169`, `tasks.py:27-57` |
| Vendor rejects a request (HTTP 4xx / auth / bad request) | Raised immediately by `NetworkClient`, no retry | `network.py:76-118` |
| Transient network failure | Retried up to 10 attempts with backoff + jitter inside `NetworkClient.call`; then `RuntimeError("Network retry limit exceeded")` | `network.py:59-126`; covered by `tests/test_network.py` (run: passed) |
| Exception raised inside a worker | **Resolved by W-02 (G-02).** The worker now wraps `_process_item` in `try/except/finally`: `TimeoutError` and `Exception` are caught, the task is marked failed, an error YAML is written via `yaml.safe_dump`, and `task_done()` is always called in `finally` — so `queue.join()` cannot hang. The pre-W-02 behavior (worker death → silent hang, no traceback) is documented as historical context below. | `reports.py:149-188`; `tasks.py:7, 52` (lead: audit PIPE-002 / ASY-001) |
| Failure before the worker pool | Validation errors, missing/malformed DEFAULT templates, connection and availability-check failures (`create_report` → `get_period` → `get_available_period`) abort the run with a printed error and exit code 1 — visible, no hang | `services.py:86-114`, `app.py:60-62`, `reports.py:220-223, 327-330` |
| `wait_task` never terminates | **Resolved by W-02 (G-02, slice 4).** `wait_task` now has two guard deadlines: `max_polls=60` iterations and `poll_deadline=3600.0` seconds (wall-clock). Additionally, the outer worker wraps `_process_item` in `asyncio.wait_for` when a `timeout` is set. A stuck task now marks itself failed and writes an error artifact instead of pinning a worker forever. | `reports.py:363-386, 403-408` (lead: audit PIPE-006) |
| Task status `FAILED`/`CANCELLED` | Now sets `task.status = False`, `task.log_error = True`, and `task.error = f"Task {terminal}"` before breaking the poll loop — `extract_data` is NOT attempted for a failed task. **Updated 2026-10-16 (W-02).** | `reports.py:403-408` (lead: audit PIPE-006) |
| Error YAML round-trip | Written with `yaml.safe_dump` (not `yaml.dump`), so tuples render as plain YAML sequences and `yaml.safe_load` round-trips correctly. **Updated 2026-10-16 (W-02).** | `tasks.py:7, 52-57`; probe (lead: audit OUT-001) |
| `log_error` distinction | `log_error` is now **read** in the worker (`reports.py:165`): `True` → processing failure → error YAML written; `False` → "no data" → warning only, no error YAML (D-15). Written at 12 sites (`reports.py:177, 183, 356, 375, 381, 391, 402, 419, 427, 431, 436`; `tasks.py:20`), read at 1 site. **Updated 2026-10-16 (W-02: now consumed).** | `reports.py:164-169`; grep (lead: audit PIPE-009) |
| Duplicate output files on re-run | Prevented before submission by the `check_done` pre-filter (name match against existing files); `TVRegTaskStrategy` skip logs a warning (`reports.py:572-576`) | `reports.py:75-76, 507-509, 572-576` (lead: audit PIPE-011) |

**Historical note (pre-W-02, preserved for context):** Before W-02
(commit `3f3dd56`), the worker had **no `try/except`** — an exception raised in
`send_task`/`wait_task`/`extract_data` skipped `task_done()` and `queue.join()`
never returned. No traceback was printed; the symptom was a console that
stopped advancing and a process that never exited. This contradicted the
owner's "never stop an export" rule (D-09, D-17) — gap G-02. See the precision
note in [Open Questions](open-questions.md#precision-note-on-g-02-which-failures-hang-and-the-owners-observation).
Error YAML was previously written with `yaml.dump` (`!!python/tuple` tags),
`log_error` was write-only (5 writes, 0 reads), the poll loop had no deadline,
`FAILED`/`CANCELLED` left `status=True`, and `csv_to_file` used `mode="x"` with
a swallowed `FileExistsError`. All of these have been **resolved by W-02**.

### Configuration that affects behavior

| Setting | Effect |
|---|---|
| `app_config.yaml → export_folder` | Root of all outputs (default `output`, resolved under the user settings folder) |
| `log_config.yaml` | Handlers/formatters/levels applied via `logging.config.dictConfig` at service construction (`services.py:58-65`). The named loggers `__main__`, `tests`, `app` never match the real module loggers (which use `__name__`, e.g. `mko_get_mediascope_data.core.reports`), so everything falls through to `root`. `AppService.reload_config()` re-reads the YAML files and re-applies logging (`services.py:67-77`, covered by tests, run: passed) |
| `compression` | File extension and on-disk compression |
| `data_lang` | `ru` → slice fields `*EName` are requested as `*Name` (`utils.en_to_ru`, `services.py:130-131`) |
| `multiple_files` + `time_unit` | Export granularity — the interval length used to split the period |
| `max_tasks` (100) and `NetworkClient` limits (5 concurrent HTTP calls, 10 attempts, 2 s base sleep) | Concurrency and retry behavior; see [Async Concurrency Architecture](async-concurrency-architecture.md) for the intentional two-level design |

### Tests

The suite was executed: **42 passed** (`uv run pytest`, all mocked
— no network, no Mediascope credentials). All tests pass at HEAD,
including `test_create_report_skips_done_files` — the W-02 change that
raises `NoReportFoundError` when zero tasks are enqueued is now accounted
for in the test (which sets up existing files and verifies the
`check_done` warning path at `reports.py:204-208` rather than expecting a
silent pass). One test, `test_run_report_with_real_models`, carries the
`@pytest.mark.slow` marker and can be excluded with
`uv run pytest -m "not slow"` (41 passed, 1 deselected). Coverage is
reported by default via `--cov=src/mko_get_mediascope_data --cov-report=term-missing`
(configured in `pyproject.toml`).

| Test file | What it pins down |
|---|---|
| `tests/test_app.py` | CLI `init --help`; lazy re-export of `run_report`, `run_report_async`, `initialize_settings` via PEP 562 `__getattr__`; package import does not bootstrap `AppService`; re-exported functions are identical objects to those in `app.py` |
| `tests/test_init_credentials.py` | After `init_project()`, `connections/mediascope.json` exists and is resolvable via `AppPaths.connection_config` (verifies the legacy-rename migration, W-06) |
| `tests/test_models.py` | `ReportSettings` validation with the current field names; `PeriodModel` exclusivity rules; `merge_data_settings` does not call `en_to_ru` when `data_lang="en"`; all 32 bundled entries pass `ReportSettings` validation; all 6 `ReportSubtype` values have matching DEFAULT keys |
| `tests/test_network.py` | Retry exhaustion raises `RuntimeError("Network retry limit exceeded")`; `mock_network_client` fixture retries on retryable errors; success returns the function result; `BadRequestError` (4xx) and `AuthorizationError` (401) are re-raised without retry; semaphore limits concurrent in-flight calls; default `max_attempts=10` |
| `tests/test_reports.py` | `DefaultTVTaskStrategy` task naming; `DictDataStrategy` output columns |
| `tests/test_reports_lifecycle.py` | Real `create_report` → `get_period` → `processing_tasks` → `worker` → `extract_data` → `csv_to_file` pipeline (unmocked except the `MediaVortexTask` double) writes CSV and the queue drains without hanging; `NoReportFoundError` raised when zero tasks enqueued; timeout path produces error YAML |
| `tests/test_services.py` | Full `run_report` pipeline end-to-end (slow; doubled `MediaVortexTask`, real `AppService`); `app_config` caching; `reload_config()` invalidates cache, is safe before first access, reapplies logging, and re-resolves `export_folder`; missing credentials raise `FileNotFoundError` |
| `tests/test_utils.py` | Date parsing; period slicing for `m` and `w`; gzip CSV writing; suffix mapping; `csv_to_file` writes into the report directory (`tmp_report_dir` fixture) |

**Updated 2026-10-16:** W-02 added `test_create_report_timeout_does_not_hang`
(verifies the worker does not hang on a non-terminal poll and writes an error
YAML) and `test_create_report_skips_done_files` (verifies `check_done` skip
behavior — the test was updated to account for the `NoReportFoundError` change
by testing the warning-only path when `check_done=True` and files exist). The
lifecycle tests now exercise the real `worker` try/except/finally path, not just
the happy path.

Behaviors with **no test coverage** — these code paths were therefore never
executed during this investigation: `TVRegTaskStrategy` (tests use `media=TV`
only), the `run` and `list-reports` CLI happy paths, and any end-to-end run
against the real API. The following behaviors are **now exercised** by the
lifecycle and hermetic pipeline tests: `get_period` (including the period-clipping
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
| 30 of 32 bundled report entries fail `ReportSettings` validation (stale `period_type`/`period_num`, stale `report_type` values such as `TV_CROSSTAB`, missing `media`/`relative_path`) — verified by probe | A user following the README's example (`nat_tv_brands_last.yaml`) cannot run it; the owner requires all bundled templates to be runnable (D-06) | **RESOLVED — W-01 (2026-10-16):** all 32 entries now pass validation; all 6 DEFAULT templates ship; both crosstab defaults parse correctly. audit TPL-001 / CFG-001 leads; gap G-05 → CLOSED |
| Both bundled crosstab DEFAULT templates were malformed YAML — verified by probe; the TV regression entered with commit `b04d177` | Fixed at HEAD — templates now parse at HEAD | audit TPL-004 (+ DISC-07); **RESOLVED — W-01 (2026-10-16)** |
| CSV write collision is swallowed and the task reports "ready" — verified by probe at the audit baseline (`mode="x"`, `FileExistsError` swallowed) | A re-run without `check_done` silently produces no file. **Resolved by W-02 (2026-10-16):** `csv_to_file` now uses `mode="w"` (overwrites); the `FileExistsError`-swallow path is removed (`utils.py:214`). | audit PIPE-001 / OUT-003 |
| Worker has no exception isolation; `queue.join()` hangs on any raise — verified by probe at the audit baseline | A single transient failure can stall the whole run instead of failing it. **Resolved by W-02 (2026-10-16):** the worker now wraps `_process_item` in `try/except/finally` with `task_done()` in `finally`; exceptions are caught, the task is marked failed, and an error YAML is written (`reports.py:149-188`). | audit PIPE-002 / ASY-001; gap G-02 → RESOLVED |
| Zero-task and out-of-window periods exit 0 with a success message — verified by probe at the audit baseline | An analyst cannot distinguish "no data" from "done". **Resolved by W-02 (2026-10-16):** `get_period` raises `NoReportFoundError` for entirely out-of-range periods; `processing_tasks` raises `NoReportFoundError` when zero tasks are enqueued; partial overlap emits a `logger.warning` (`reports.py:327-330, 213-218`). | audit PIPE-004 / PIPE-005; gap G-01 → RESOLVED |
| Poll loop has no deadline — verified by reading `wait_task` at the audit baseline | A stuck vendor task pins a worker forever. **Resolved by W-02 (2026-10-16):** `wait_task` now has `max_polls=60` and `poll_deadline=3600.0` guards (`reports.py:363`, `373-386`). | audit PIPE-006; gap G-02 slice 4 → RESOLVED |
| `log_error` is write-only — verified by grep (5 writes, 0 reads) at the audit baseline | "Empty result" and "error" are indistinguishable downstream. **Resolved by W-02 (2026-10-16):** `log_error` is now read in the worker branching at `reports.py:165`; `log_error=True` → error YAML, `log_error=False` → warning only. | audit PIPE-009; gap G-01/G-02 → RESOLVED |
| Region id 99 is hard-coded as excluded — verified by reading `reports.py:539` | Users cannot include 99 or express an exclusion list; the owner requires template-driven region selection (D-10) | gap G-03; DISC-09 |
| Error YAML written with `yaml.dump` — verified by probe (`!!python/tuple`, not `safe_load`-able) at the audit baseline | Error artifacts cannot be re-loaded with safe tooling. **Resolved by W-02 (2026-10-16):** `tasks.py` now uses `yaml.safe_dump` (`tasks.py:7, 52-57`). | audit OUT-001; gap G-02 → RESOLVED |
| Module-import `sys.stdout.reconfigure()` | Fixed at HEAD — the call was moved into the `run` command (commit `706eed0`); importing the library no longer reconfigures stdout (verified: `app.py:51-52`) | audit ENT-001 (baseline only; fixed at HEAD) |
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
  keys in a *report* YAML are silently ignored instead, because the merge only
  overlays keys that already exist in `DATA_DEFAULTS` (verified: legacy keys
  `folder`/`category_name` have zero references in `core/`).
- The vendor library creates one `requests.Session` per
  `MediaVortexTask` (`.venv/…/core/net.py:73`, created in
  `MediaVortexTask.__init__`, `mediavortex/tasks.py:43-47`), and
  `AppService.get_connection` constructs a new `MediaVortexTask` per report
  entry (`services.py:140-154`, `:175-179`) — so each entry in a report file
  opens its own session; none is explicitly closed.
- Type safety is now enforced: `uv run mypy src tests` passes with **0 errors**
  at HEAD (verified on the investigation machine; down from 56 errors in 7 files
  at the audit baseline). mypy checks both `src` and `tests` per
  `pyproject.toml`. CI runs `ruff check src tests` + `pytest`
  (`.github/workflows/ci.yml`).

## Verification

Read-only probes executed for this document (scripts kept for reproduction in
`.ai/tmp/`):

| Script | What it established |
|---|---|
| `.ai/tmp/doc_probe_current_state.py` | (Re-run after W-01/W-02) all 32 bundled report entries validate; all 6 DEFAULT templates parse; `tv/simple`, `tv_reg/timeband`, `tv_reg/simple` now ship |
| `.ai/tmp/doc_probe_cli_surface.py` | `run --help` shows `--verbose` only (no `--no-verbose`); `run … --no-verbose` exits 2 |
| `.ai/tmp/doc_probe_cmdname.py` | `list_reports` exits 2; `list-reports` exits 0 |
| `.ai/tmp/doc_probe_paths.py` | Resolved user settings folder, connection config path, default export folder; instantiating the service with a missing user config raises `FileNotFoundError` |
| `.ai/tmp/doc_probe_git_defaults.py` | `tv/crosstab.yaml` parses at `3408747` and fails from `b04d177` onward |
| `.ai/tmp/doc_probe_worker_hang_classes.py` | (Pre-W-02 probe) The real `MediaReport.worker` with `RuntimeError` and `BadRequestError` raised in `send_task`: both hang `queue.join()` and print no traceback — behavior since fixed by W-02 try/except/finally |
| `.ai/tmp/doc_probe_reverify_audit.py` | 11 defect claims re-executed against the code: `get_period` crash, inverted-period → zero intervals, zero-task silent run, worker `queue.join()` hang, swallowed duplicate write, `en_to_ru(None)` TypeError, `!!python/tuple` in the error YAML + `safe_load` ConstructorError, FAILED/CANCELLED status handling, done_files keying — 11/11 pass |
| `.ai/tmp/doc_probe_options_none.py` | A subtype with no defaults block leaves `slices`/`statistics`/`options` as `None`; `Data(**merged)` then raises a Pydantic `ValidationError` for `data_lang: en` (the `data_lang: ru` path crashes earlier in `en_to_ru`) |
| `.ai/tmp/doc_probe_vendor_build_task.py` + reading `.venv/Lib/site-packages/mediascope_api/` | Vendor `build_task` has no `**kwargs` (unknown key → `TypeError`); one `requests.Session` per `MediaVortexTask` (`core/net.py:73`) |
| `.ai/tmp/doc_probe_links.py` | All relative links and heading anchors in the documentation resolve |
| `uv run pytest` | **42 passed** (full suite, mocked — no network, no Mediascope credentials; coverage reported via `--cov`) |
| `uv run mypy` | **0 errors** (`uv run mypy src tests` passes with zero errors; mypy checks both `src` and `tests` per `pyproject.toml` `files = ["src", "tests"]`) |
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
