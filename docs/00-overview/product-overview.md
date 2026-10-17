---
id: product-overview
domain: overview
tags:
  - product
  - overview
  - requirements
related:
  - doc-maintenance-rules
  - current-behavior
  - data-flow-architecture
  - system-architecture
  - open-questions
  - settings-layers-architecture
  - async-concurrency-architecture
---

## Purpose

Explains what `mko_get_mediascope_data` is for, who uses it, and how it is
expected to behave — in product terms first, technical terms second. It is the
starting point for both the product owner and a developer who is new to the
project. Where a statement is confirmed by code, tests, or the owner, the
certainty label in brackets says so; where it is not, the statement links to a
question or decision in [Open Questions](open-questions.md).

Certainty labels used across the product documentation:

| Label | Meaning |
|---|---|
| `[code]` | Traced directly through the implementation |
| `[test]` | Specified or checked by a test (run status noted) |
| `[owner]` | Confirmed by the product owner (recorded decision) |
| `[assumption]` | Plausible but unconfirmed |
| `[open]` | Open question, see [Open Questions](open-questions.md) |

## Main Concepts

### The problem

Mediascope (the Russian TV audience measurement vendor) exposes TV advertising
data only through a task-based HTTP API: a client submits a calculation
request, polls for completion, and then downloads a result table. There is no
"give me the report" call. Producing a routine analytical export therefore
requires the same steps to be repeated for every reporting period and every
target audience, with credentials, retries, and file management handled by
hand `[code — the task/poll/download cycle is implemented in
core/reports.py, TVMediaReport.send_task / wait_task / extract_data
(reports.py:344-361, :363-411, :412-449)].`

`mko_get_mediascope_data` automates that cycle. A seller or analyst describes
once, in a YAML file, which data slice they need (channels, product category,
period, audiences, statistics); the tool submits the work to Mediascope,
tracks it, and writes one CSV file per period interval per audience.

### Who uses it

| User | What they do | Evidence |
|---|---|---|
| Media analyst / seller | Authors report YAML files (one per client or topic) and runs them — on demand from Jupyter, or on a schedule via Airflow — to obtain raw data for further analysis | `settings/reports/` template folders are named after clients or analytical topics: `gap_resurs`, `last_category`, `sovcombank`, `tefal`, `theiss`, `tv_report` `[code]`; the two usage modes are owner-confirmed (D-19) |
| Developer / data engineer | Imports `run_report()` from Python instead of using the CLI | `app.run_report` / `run_report_async` public API; README "Public API (Jupyter, Airflow, scripts)" section `[code, owner-doc]` |

The two entry styles are the same engine: CLI (`app.py` Typer commands) and
library (`run_report`, `run_report_async`, `initialize_settings`) both resolve
to `AppService.run_report` `[code — __init__.py:17-30 (re-exports),
app.py:40-62, app.py:80-87, services.py:156]`.

### What the product does today (summary)

Given a report YAML file, the tool `[code]`:

1. Validates each report entry against the Pydantic schema `ReportSettings`
   (`core/models.py:87-115`).
2. Merges the entry over the matching DEFAULT template
   (`settings/defaults/<media>/<report_type>.yaml`) into one set of API
   parameters (`core/services.py:100-114`).
3. Resolves the reporting period from the entry (explicit dates or "last N
   weeks/months/…") and clips it to the period the Mediascope database
   actually has (`core/reports.py:295-338`).
4. Splits the period into intervals (day/week/month/year) and the audiences
   into filters, producing one API task per interval × audience
   (`core/reports.py:494-506`).
5. Submits tasks with bounded concurrency, polls until each finishes, and
   downloads the result tables (`core/reports.py:189-216`, `core/network.py:59-126`).
6. Selects and renames the configured columns, then writes one
   semicolon-separated, comma-decimal CSV (optionally gzipped) per task into
   `<export folder>/<relative_path>/` (`core/utils.py:192-220`).
7. Skips tasks whose output file already exists, so an interrupted run can be
   resumed (`core/reports.py:78-83, :503-505`).
8. Writes an error YAML file for tasks that failed logically
   (`core/tasks.py:27-57`).

### Key user scenarios

**Scenario A — first-time setup.** Install the package, run `init`, place the
Mediascope credentials into the user settings folder, and run a first report
`[code — app.py:101-103, core/init_service.py:37-57]`. The README documents this as the
intended onboarding path `[owner-doc]`. Current-state caveat: the shipped
credential template is named `mediascope_RENAME_.json` while the code reads
`connections/mediascope.json`, so a rename step is required that the README
does not mention `[code — core/paths.py:60, init_service.py:60-90 (legacy
rename), settings/connections/; lead: audit CFG-003]` — see [Open Questions](open-questions.md) Q-07.

**Scenario B — recurring client export.** The analyst points `run` at a report
YAML; every interval × audience file for the requested period appears under the
export folder; files already present from a previous run are skipped.
Historical evidence of this usage exists on the development machine: export
folders such as `output/tefal/raw_data/`, `output/tv_report/top_adv/`,
`output/tv_report/channels_tvr_w/` contain monthly and weekly CSV.gz files
named `<start>_<end>_<audience>.csv.gz` `[code — the naming rule is implemented
in DefaultTVTaskStrategy.generate, core/reports.py:494-506, and matches the
on-disk files]`.

**Scenario C — regional (TV_REG) export.** For regional data, the tool expands
the report over all catalog regions (or an explicit `regions` list), produces
one task per region × interval × audience, and embeds the region name in the
file name `[code — TVRegTaskStrategy, core/reports.py:513-569]`.

**Scenario D — dictionary export.** When the report subtype name contains
`DICT`, the output is not the raw table but a melted "search dictionary" table
— this is one report type's strategy, not an anomaly `[owner — D-07; code —
DictDataStrategy, core/reports.py:599-652; the column layout
`action/search_column_idx/value/term/cat/adv/bra/sbr/mdl/cln_0..5` is
hard-coded there]`. What consumes this dictionary file is not documented in the
repository `[open — Q-09]`.

**Scenario E — partial failure and re-run.** One report file can contain
several report entries; entries are processed one after another `[code]`, so a
failure inside one report must not abort the invocation `[owner — D-08, D-17]`.
When individual tasks fail, their description is persisted next to the output
folder, and a re-run skips everything already written and re-submits only the
remaining tasks `[code — core/reports.py:78-83, :503-505; owner — D-09]`.

### Target behavior (owner-confirmed)

The statements below come from the owner's answers on 2026-10-15 (decision IDs
in parentheses). Where the current code differs, the gap is registered in
[Open Questions](open-questions.md#gaps-between-owner-intent-and-current-implementation)
and is **not** fixed by this documentation.

1. **Never fail fast (D-17).** The tool must not abort because something went
   wrong; it extracts as much as possible and finishes normally. The model the
   owner uses: a chunked downloader where a failed block does not restart the
   whole download. Exit codes must not stop the program.
2. **Missing data is a warning, not an error (D-08, D-15).** If the requested
   period has no data in Mediascope: partial overlap → export the overlapping
   part and warn that the rest has no data; no overlap → warn and export
   nothing. A period with no data must **not** produce an error file.
3. **Error files mean "the calculation was rejected or could not be processed"
   (D-15).** They are written only for remote rejections or processing failures
   such as network errors — the situations where a re-run might behave
   differently and the user needs the persisted task description to diagnose
   the persistent problem (D-09).
4. **Re-run continues, it does not restart (D-09, D-16).** Already-written
   outputs are detected by file name and skipped; only unfinished tasks are
   resubmitted. Idempotency is best-effort: network failures and data that
   appears later can legitimately change a re-run's result.
5. **Bundled templates are always runnable (D-06, D-11).** Every template
   shipped with the product must validate and work; templates are incomplete
   by design and grow as the product is used; nothing is deleted.
6. **Output shape is per report type, not fixed (D-07).** Slices + statistics
   is the default, and each report type may shape its output differently
   through its own data strategy — the Strategy pattern is the intended
   mechanism, and the DICT output is one such strategy.
7. **Regional reports default to all regions (D-10).** The region set belongs
   in the user template (inclusion or exclusion list); a hard-coded exclusion
   in the code is not acceptable. The design is to be researched (R-01).
8. **Output conventions become configurable later (D-13, D-14).** The current
   `;`/`,`/UTF-8 BOM/gzip conventions are accepted for now and intentionally
   hard-coded; the target is defaults in `settings/app_config.yaml` with
   per-report overrides, and a user settings file for the DICT columns.
9. **Installation is from GitHub, not PyPI (D-19).** The product will not be
   published to PyPI; the intended path is `pip install <github_url>`. (The
   current README's PyPI instructions contradict this — DISC-10.)
10. **Only failures are persisted (D-18).** Successful tasks leave their CSV
    and nothing else; with millions of requests, logging successes would
    produce gigabytes of useless reports.
11. **Credential setup should follow best practice (D-12 — recommendation,
    awaiting the owner's choice).** Ship the credentials template under its
    final file name (`connections/mediascope.json`) with placeholder values,
    have `init` copy it verbatim so no rename step exists, keep the real
    credentials file out of version control, and fail at startup with an
    explicit "credentials missing or incomplete" message instead of an opaque
    error from the vendor library. Today the shipped name is
    `mediascope_RENAME_.json` while the code reads `mediascope.json`
    (`core/paths.py:60`, `init_service.py:60-90` migrates the legacy name) — a
    rename step the README does not mention.

### Owner-confirmed intent

Evidence policy (owner ruling D-05): the audit corpus in `.ai/audit/` is a lead
generator, not a source of truth — only the code and the owner's statements are
authoritative.

| Decision | Source |
|---|---|
| The settings system is intentionally two layers: a DEFAULT layer (full API parameter set + per-subtype blocks) and a CLIENT layer (per-report deltas), combined by a shallow top-level overlay. Stale templates are to be repaired to conform, not the code. | Owner statement during the template-conformance review (pointer: the re-validation note in the audit corpus, 2026-10-14); architecture reference [settings-layers-architecture.md](settings-layers-architecture.md) `[owner]` |
| The product is delivered as both a CLI and an importable library | `README.md` Quick Start + Public API sections; `pyproject.toml` console script `mko-get-mediascope` `[owner-doc, code]` |
| English-only user-visible strings | Owner rule, `AGENTS.md` rule 1; executed by commit `b04d177` `[owner, code]` |
| The ten target-behavior rules above | Owner answers, 2026-10-15; full register in [Open Questions](open-questions.md#owner-decisions) `[owner]` |

### Constraints and non-goals

Confirmed by the project's own rules and dependencies `[code, owner-doc]`:

- **No database, no web framework, no scheduler.** The tool is a standalone
  fetcher; Jupyter/Airflow are external drivers (`AGENTS.md`: "No database, no
  Docker, no web framework"; owner-confirmed usage modes, D-19).
- **Vendor-coupled.** Report parameters are Mediascope API parameters; the
  vendor client library `mediascope-api-lib` performs the HTTP work
   (`pyproject.toml` dependency; `core/services.py:8`).
- **Raw export only.** The tool writes the selected API result columns; apart
   from reshaping frequency-distribution rows into columns it does not
   aggregate, join, or model the data `[code — core/reports.py:279-293 selects
   and renames columns; core/reports.py:408-445 extracts; utils.py:236-274
   pivot_df_frequency pivots the API's own frequency rows]`.
- **Fixed value sets are enums** (`MediaType`, `ReportType`, `ReportSubtype`),
  so adding a report kind is a code change, not a config change
  (`AGENTS.md` rule 10).
- **No PyPI publication** (D-19); releases are manual.

### Success criteria

Rewritten to match the owner's answers (D-08, D-09, D-15, D-16, D-17) on
2026-10-15. A run is considered correct when:

1. every report entry is attempted, and one failure — missing data, a rejected
   calculation, a network error — never aborts the invocation (D-08, D-17);
2. every requested interval × audience that the database can serve produces
   exactly one output file; tasks that the remote base rejected or that could
   not be processed produce a persisted error description instead (D-15);
3. periods with no data produce a **warning** naming the missing range, never
   an error file (D-08, D-15);
4. a re-run re-submits only tasks whose output file is absent (D-09);
5. the final console/report output lets the operator see which intervals and
   audiences were exported and which were skipped or warned about.

The current code meets these criteria at HEAD: W-02 resolved the
zero-task/hang gaps (G-01, G-02) — `NoReportFoundError` is now raised for
unrunnable periods (`reports.py:327-330`) and the worker's
`try/except/finally` isolates exceptions so the run continues with an error
file written via `yaml.safe_dump` (`reports.py:149-188`, `:164-169`). A minor
gap remains: no console summary of exported vs. skipped intervals is printed
(criterion 5, see G-03). The remaining gap is registered in
[Open Questions](open-questions.md#gaps-between-owner-intent-and-current-implementation)
and is not fixed by this documentation.

### Unresolved product questions

All original questions were answered on 2026-10-15; the register keeps the
answers and rationale. What remains open is small and mostly implementation
planning:

- **D-12** — how the credential file best-practice recommendation
  (Q-07) is implemented (naming, placeholders, startup validation).
- **R-01 / R-02 / R-03** — three design questions the owner wants researched
  before implementation: region selection syntax, configurable output
  conventions, configurable dictionary columns.
- **kitId** — no meaning is asserted anywhere in the docs; the owner will
  supply the current mapping when available (D-20, DISC-11).
- **A-04** — the concrete downstream consumer of the CSV exports is still
  undocumented, which is why format conventions cannot be validated (D-13
  makes them configurable later).
- **D-19** — the README still describes PyPI installation (DISC-10); aligning
  it with the GitHub-install intent is pending.

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Current Behavior](current-behavior.md)
- [Data Flow Architecture](data-flow-architecture.md)
- [System Architecture](system-architecture.md)
- [Open Questions](open-questions.md)
- [Settings Layers Architecture](settings-layers-architecture.md)
- [Async Concurrency Architecture](async-concurrency-architecture.md)
- [README.md](../../README.md)
