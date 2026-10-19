---
id: data-flow-architecture
domain: overview
tags:
  - data-flow
  - transformation
  - pipeline
  - settings
related:
  - doc-maintenance-rules
  - current-behavior
  - system-architecture
  - product-overview
  - open-questions
  - settings-layers-architecture
  - async-concurrency-architecture
---

## Purpose

Single source of truth for **where data comes from, what happens to it, and
where it ends up** in `mko_get_mediascope_data`: the two-layer settings merge,
the period resolution rules, the task fan-out, the DataFrame preparation, and
the CSV writing rules. It complements
[Current Behavior](current-behavior.md) (the scenario view) and
[System Architecture](system-architecture.md) (the component view) by focusing
on the data itself. Each rule carries a certainty label; rules whose business
meaning is unconfirmed are marked `[open]` and tracked in
[Open Questions](open-questions.md).

## Main Concepts

### Data sources and formats

| Source | Format | Where it enters | Certainty |
|---|---|---|---|
| User report definition | JSON5 list of report entries | `run <file>` / `run_report(path)` → `ReportSettings` (`services.py:85-98`, `models.py:87-115`) | `[code]` |
| DEFAULT parameter template | JSON5 with `DATA_DEFAULTS` + subtype blocks | `settings/defaults/<media>/<report_type>.json5` → `DataDefaults` (`services.py:100-114`, `models.py:128-130`) | `[code]` |
| App/log configuration | JSON5 | `settings/app_config.json5`, `settings/log_config.json5` (`services.py:33-56`) | `[code]` |
| Mediascope credentials | JSON | `settings/connections/mediascope.json` (`paths.py:60-81`) | `[code]` |
| Mediascope API | HTTP JSON | vendor library `mediascope-api-lib` (`MediaVortexTask`), called through `NetworkClient` (`services.py:20`, `network.py:59-126`) | `[code]` |
| Previously written outputs | CSV/CSV.gz files | read as a name set (not parsed) for resume logic (`reports.py:75-83`) | `[code]` |

### The merge: two layers into one API parameter set

```mermaid
flowchart LR
    A["CLIENT layer<br/>settings/reports/**/*.json5<br/>ReportSettings"] --> M["merge_data_settings<br/>services.py:116-133"]
    B["DEFAULT layer<br/>settings/defaults/&lt;media&gt;/&lt;type&gt;.json5<br/>DataDefaults.DATA_DEFAULTS"] --> M
    S["subtype block<br/>e.g. DYNAMICS_BY_SPOTS"] --> M
    M --> D["Data model<br/>services.py:135-138"]
    D --> T["TVTask.settings<br/>tasks.py:17"]
    T --> API["MediaVortexTask.build_task<br/>reports.py:350-352"]
```

Merge rules, in order `[code]`:

1. Start from a shallow copy of `DATA_DEFAULTS` (the full Mediascope parameter
   set: `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`,
   `location_filter`, `basedemo_filter`, `targetdemo_filter`, `program_filter`,
   `break_filter`, `ad_filter`, `slices`, `statistics`, `sortings`, `options`,
   `frequency_dist_conditions`) (`services.py:120`).
2. If `report_subtype` names a block that exists as an attribute on the loaded
   defaults, overlay that block with `dict.update` — whole-value replacement per
   top-level key (`services.py:122-124`).
3. Overlay the report's own keys, but **only keys that already exist** in the
   merged dict (`services.py:126-128`). In practice the intersection of
   `ReportSettings` fields with `DATA_DEFAULTS` is `ad_filter` plus any API
   parameter keys the report defines as extras (for example `slices`,
   `statistics`, `options`, `company_filter` in
    [`nat_tv_tvreport.json5`](../../src/mko_get_mediascope_data/settings/reports/nat_tv_tvreport.json5)).
   Report-only fields (`media`, `period`, `relative_path`, …) never leak into
   the API parameters.
4. If `data_lang == "ru"`, replace the `EName` suffix of every slice with
   `Name` (`services.py:130-131`, `utils.en_to_ru`, `utils.py:223-233`) — e.g.
   `advertiserListEName` → `advertiserListName`.

The merge is intentionally a **shallow top-level overlay**, not a deep merge —
this is an owner decision about the settings architecture (D-01, given during
the template-conformance review; pointer: the re-validation note in the audit
corpus). Consequence for template authors: a report that overrides a
block-level key (`options`, `slices`, …) must supply the complete block, because
partial sub-keys are not merged.

### Period resolution

```mermaid
flowchart TD
    A["period config"] --> B{"date_filter set?"}
    B -- no --> C["get_last_period(today = latest available API date,<br/>last_time, time_unit = period.time_unit)  utils.py:106-155"]
    B -- yes --> D["requested = date_filter"]
    C --> E["clip to API window<br/>max(window.start, requested.start), min(window.end, requested.end)<br/>reports.py:322-325"]
    D --> E
    E --> F{"multiple_files?"}
    F -- true --> G["slice by period.time_unit<br/>utils.slice_period  utils.py:69-85"]
    F -- false --> H["single interval"]
    G --> I["interval list → tasks"]
    H --> I
```

Rules and their edge cases `[code]`:

- The reference "today" for `last_time` is the **latest date the API reports as
  available**, not the system clock (`reports.py:265-277` passes it to
  `utils.get_last_period` as `today=available_period[1]`).
- `last_time` semantics (`utils.get_last_period`): start = N whole units back
  from the start of the current unit; `include_current: false` additionally
  moves the end back to just before the current unit (for weeks, end = start +
  N−1 weeks + 6 days).
- The API window comes from `cats.get_availability_period()` filtered by
  `options.kitId` (`reports.py:265-277`; `base_id` default 1, `reports.py:259-261`).
  What each `kitId` value means is **not** asserted anywhere: the owner has
  declared the in-template legend stale and will supply the current mapping
  when available (D-20, DISC-11).
- **Clipping can invert the period** (start > end) when the requested range does
  not overlap the window — since W-02, this raises `NoReportFoundError` rather
   than silently producing zero intervals (`reports.py:327-330`; lead: audit PIPE-005).
- The interval length comes from `period.time_unit` (default `w`), which is
  always available on `PeriodModel` regardless of whether `date_filter` or
  `last_time` is used (`models.py:67-84`, `reports.py:317`).

### Task fan-out

| Strategy | Produces | Task/file name |
|---|---|---|
| `DefaultTVTaskStrategy` (`reports.py:481-506`) | interval × audience tasks | `<start>_<end>_<ascii audience name>` |
| `TVRegTaskStrategy` (`reports.py:513-569`) | region × interval × audience tasks | `<start>_<end>_<ascii audience>_<ascii region name>`; region id 99 is currently hard-coded as excluded (only 99 — the owner recalled 99 and 100, DISC-09; template-driven selection is the intent, D-10); company filter extended with `regionId IN (<id>)` |

Audience names are transliterated with `unidecode` (`reports.py:504`, `reports.py:550`
for region names), producing a flat, URL-safe task name used as the CSV stem. This is
why on-disk file names contain plain ASCII audience labels
such as `all 20-45 BC`. The current exclusion of region id 99 is a code-level
rule the owner has declared incorrect; the meaning of region ids is not
documented in the repository — see Q-05 / D-10.

### Per-task data path inside the API cycle

```mermaid
sequenceDiagram
    participant R as TVMediaReport
    participant N as NetworkClient
    participant V as Mediascope API (vendor lib)
    participant FS as File system
    R->>N: build_task(report_type, **settings) [api_call=False]
    N->>V: HTTP (via _invoke, thread-offloaded + timeout-bounded; no semaphore/retry)
    V-->>N: task JSON
    N->>V: send_task(task JSON) [retryable=False, semaphore 5]
    V-->>N: task key
    loop poll every ~2 s
        R->>N: get_status(key)
        N->>V: HTTP
        V-->>R: status (DONE / FAILED / CANCELLED)
    end
    R->>N: get_result(key)
    V-->>R: result JSON
    R->>N: result2table(json) [api_call=False]
    V-->>R: DataFrame
    R->>R: data_strategy.prepare(df)
    R->>FS: csv_to_file(df, file_prefix=task.name)
```

Most vendor calls go through `NetworkClient.call`, which serializes blocking
vendor calls into worker threads under an `asyncio.Semaphore(5)` and retries
transient errors (`network.py:59-126`; full contract in
[async-concurrency-architecture.md](async-concurrency-architecture.md)). Two calls
short-circuit this path via `api_call=False` (`build_task` at `reports.py:365-367`,
`result2table` at `reports.py:478-481`): they still go through `_invoke` (thread
offloaded + timeout-bounded) but skip the jitter sleep, semaphore, and retry loop
(`network.py:101-102`). Additionally, `TVRegTaskStrategy` calls
`catalogs.get_tv_region()` directly (`reports.py:578-580`), so that catalog read
gets no retry, no semaphore, and no thread offloading `[code]`.

### DataFrame preparation rules

**Default strategy** (`reports.py:568-592`) `[code]`:

1. rename `prj_name` → `targetAudience` if present;
2. keep exactly the configured columns: slices (with `targetAudience` inserted
   first when `target_audiences` is set, and `frequencyDistInterval` appended
   when frequency distribution is configured) then statistics
   (`reports.py:279-293`);
3. if `frequencyDistInterval` is among the columns, split the base row
   (`frequencyDistInterval == "-"`) from the frequency rows and pivot the
   frequencies into columns (`utils.pivot_df_frequency`, `utils.py:236-274`);
4. any missing column raises `KeyError`, which is logged (column name, without
   the task name) and turned into `None` → the task is marked failed and an
    error JSON5 is written (`reports.py:591-596`; lead: audit PIPE-008).

**Dictionary strategy** (`reports.py:599-652`, selected via
`ReportSubtype.DYNAMICS_BY_SPOTS_DICT` in `ReportFactory.data_strategy`
`reports.py:46-48`) `[code]`:

1. check the configured slice columns exist; if any is missing, log and return
   `None`;
2. melt the frame to `(column, value)` pairs, drop nulls and duplicates;
3. build the fixed output layout `action, search_column_idx, value, term, cat,
   adv, bra, sbr, mdl, cln_0 … cln_5`, where `action` is the constant `upd`,
   `term` is the quoted value, and the slice columns fill `cat`…`mdl` and
   `cln_*` positionally.

The business meaning of this dictionary layout (which downstream system consumes
it, and what `cat`/`adv`/`bra`/`sbr`/`mdl` stand for beyond the obvious
category/advertiser/brand/subbrand/model reading) is not documented in the
repository — `[open]` Q-09.

### Output writing rules

`utils.csv_to_file` (`utils.py:192-220`) `[code]`:

- path = `<csv_path_out>/<file_prefix><optional timestamp><ext>`, where `ext`
  follows the `compression` setting (`.csv`, `.csv.gz`, `.csv.bz2`, …;
  `utils.get_files_suffix`, `utils.py:158-189`);
- `to_csv` with `sep=";"`, `decimal=","`, `index=False`, `encoding="utf-8-sig"`,
  `compression=<setting>`, **`mode="w"`**;
- `lineterminator` is not set, so line endings follow the platform
  (`utils.py:211-220`; lead: audit OUT-002);
- (since W-02) there is no `FileExistsError` swallow — the file is overwritten;
  the `check_done` pre-filter prevents re-submission of already-written tasks.

### Handling of missing, duplicate, and unexpected values

| Case | Handling | Certainty |
|---|---|---|
| Output file already exists (duplicate) | Pre-filtered before submission by `check_done`: existing file names (extension stripped) are collected by `get_done_files` and matching tasks are skipped (`reports.py:75-76, 78-83`, `:507-509`); the TV_REG strategy skips silently (`reports.py:563-567`; lead: audit PIPE-011) | `[code]` |
| Empty result DataFrame | Task marked failed with `log_error=False` (no data → warning only, no error JSON5 written) (`reports.py:433-436`); the worker checks `log_error` at `reports.py:165`: `True` → error JSON5, `False` → warning only (D-15, W-02). Now resolved per owner intent | `[code]` |
| Result JSON not returned | Task marked failed with `log_error=True`, error JSON5 (`reports.py:421-424`) | `[code]` |
| Missing columns in the DataFrame | `KeyError` → log → task failed (`reports.py:591-596`) | `[code]` |
| Unknown `report_subtype` (no matching defaults block) | No overlay happens, so the base `DATA_DEFAULTS` Nulls survive. With `data_lang: ru` (the default) the merge crashes in `en_to_ru(None)` with `TypeError` (probe-verified). With `data_lang: en` the merge completes and the run instead fails at `Data(**merged)` with a Pydantic `ValidationError` (probe-verified), because `slices`/`statistics`/`sortings`/`options` are non-optional `dict | list` fields (`models.py:135-137`) | `[code]` |
| Unknown keys in a report JSON5 (e.g. legacy `folder`, `category_name`) | Absorbed as Pydantic extras, then dropped by the merge unless the key exists in `DATA_DEFAULTS`; no error, no warning (verified: both legacy keys have zero references in `core/`) | `[code]` |
| Missing DEFAULT template for the `(media, report_type)` pair | `FileNotFoundError` from `json5_to_dict`, run aborts (`services.py:100-114`; verified: files absent; lead: audit TPL-005) | `[code]` |
| Vendor rejects a request (HTTP 4xx / auth) | Raised immediately by `NetworkClient`, aborting the run (`network.py:76-118`) | `[code]` |

### Provenance and preservation of original values

- The exported CSV contains only the columns the report selects, renamed
  (`prj_name` → `targetAudience`) or localized (`*EName` → `*Name`); no
  values are recomputed by this tool `[code]`.
- The original request that produced each file is preserved in
  `TVTask.settings` (`tasks.py:17`) and written to the error JSON5 when a task
  fails (`tasks.py:27-57`). For successful tasks the settings are **not**
  persisted — the output file name encodes only interval and audience `[code]`.
  The owner confirmed this is intended: only failures are persisted (D-18).
- Decimal values are written with a comma decimal separator and a BOM
  `[code]`; the owner confirmed these conventions are correct for now and are
  to become configurable later — defaults in `settings/app_config.json5` with
  per-report overrides (D-13).

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Current Behavior](current-behavior.md)
- [System Architecture](system-architecture.md)
- [Product Overview](product-overview.md)
- [Open Questions](open-questions.md)
- [Settings Layers Architecture](settings-layers-architecture.md)
- [Async Concurrency Architecture](async-concurrency-architecture.md)
