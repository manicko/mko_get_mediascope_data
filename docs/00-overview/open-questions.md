---
id: open-questions
domain: overview
tags:
  - open-questions
  - decisions
  - discrepancies
  - risks
related:
  - doc-maintenance-rules
  - product-overview
  - current-behavior
  - data-flow-architecture
  - system-architecture
  - settings-layers-architecture
  - async-concurrency-architecture
---

## Purpose

The single register of everything that is **not settled**: questions for the
product owner, the owner's recorded decisions, discrepancies between sources,
assumptions awaiting validation, gaps between the owner's intent and the
current code, and known limitations. Every entry records its status and its
impact on product behavior. Answered questions keep their decision and
rationale — nothing is deleted, so the reasoning behind the current
implementation stays traceable.

Statuses: `open · asked · answered · superseded`. Answers are recorded in place
with the date and the source of the answer. The questions were asked and
answered on 2026-10-15; the register below reflects the answers.

## Main Concepts

### Owner decisions

| ID | Decision | Source |
|---|---|---|
| D-01 | The settings architecture is two layers (DEFAULT base + CLIENT deltas) combined by a shallow top-level overlay; this is the intended design, and stale templates must be repaired to conform rather than changing the code | Owner statement during the template-conformance review (pointer: the re-validation note in the audit corpus, 2026-10-14) |
| D-02 | All user-visible text (code, logs, docs) is English-only | `AGENTS.md` rule 1; executed by commit `b04d177` |
| D-03 | The product ships as a CLI **and** an importable library | `README.md` (owner-authored); `pyproject.toml` |
| D-04 | `max_tasks` (remote capacity) and the HTTP semaphore (client concurrency) are intentionally independent knobs | Owner-maintained [async-concurrency-architecture.md](async-concurrency-architecture.md) |
| D-05 | **The audit corpus (`.ai/audit/`) contains many errors and is not a source of truth. Only the code and the owner's decisions are authoritative.** The audit may be used to generate leads, but every claim must be re-verified against the code before it is trusted or restated | Owner ruling, 2026-10-15, in response to this documentation investigation |
| D-06 | **All bundled report templates are to be migrated to the current schema and kept runnable** (Q-01). Nothing is to be removed; templates and reports are incomplete by design and will grow as the product is used | Owner answer, 2026-10-15 |
| D-07 | **Output columns = slices + statistics (the current code), extended per report type by strategies.** There is deliberately no single fixed column option: the Strategy pattern is the mechanism that lets each report type shape its own output (DICT writes a dictionary, default writes slices+statistics) | Owner answer, 2026-10-15 |
| D-08 | **Missing periods must never interrupt processing.** Other reports/tasks keep running. Partial overlap: export the overlapping part and warn that the rest has no data. No overlap at all: warn only, export nothing, exit normally | Owner answer, 2026-10-15 |
| D-09 | **Never stop an export because one task failed — this is critical.** The resume mechanism (`check_done`: already-written files are skipped, only unfinished tasks are resubmitted) is the recovery path, and the per-task error description file is critical: after repeated re-runs the user must be able to study the persistent failure from that description | Owner answer, 2026-10-15 |
| D-10 | **Regional reports:** when the user specifies no region list, load **all** regions. The hard-coded exclusion of region id 99 in `core/reports.py` is incorrect and must be removed; instead the user template must be able to express an inclusion list (`id IN (1,2,3…)`) or an exclusion (`id NOT IN (99,100)`). Design approach to be researched first | Owner answer, 2026-10-15 |
| D-11 | Reports/templates are intentionally incomplete and will be implemented as the product is used; nothing is deleted from the bundled examples | Owner answer, 2026-10-15 |
| D-12 | The credential template should follow best setup practice (name, placeholders, validation). A recommendation is recorded under Q-07; the owner asked for a recommendation and has not yet chosen an implementation | Owner answer, 2026-10-15 |
| D-13 | CSV conventions (`;` separator, `,` decimal, UTF-8 BOM, gzip) are correct for now and intentionally hard-coded. Future intent: defaults in `settings/app_config.yaml` overridable per report file in the user's own report | Owner answer, 2026-10-15 |
| D-14 | The DICT dictionary column layout must not stay hard-coded. Future intent: a user settings file that defines which columns the dictionary gets and how. Design to be researched without breaking the current infrastructure | Owner answer, 2026-10-15 |
| D-15 | Error files are written **only** when the remote calculation was rejected or a task could not be processed (network, processing errors). A period that simply has no data must produce a **warning**, not an error file | Owner answer, 2026-10-15 |
| D-16 | Idempotency is best-effort, not guaranteed: network failures and data that appears later (a period absent today may exist next week) can change the result of a re-run. This is accepted | Owner answer, 2026-10-15 |
| D-17 | **No fail-fast.** The program must not abort on errors; it must extract as much as possible and finish. Model: a chunked downloader where a failed block does not restart the whole download — a re-run continues from where it stopped | Owner answer, 2026-10-15 |
| D-18 | Only failed tasks get a persisted description; successful tasks are not logged. Rationale: millions of successful requests would produce gigabytes of useless reports | Owner answer, 2026-10-15 |
| D-19 | One invocation runs one report file; a file may contain several report entries (the current design). Both ad-hoc analyst runs (Jupyter) and scheduler runs (Airflow) are intended. **Do not publish to PyPI**; the intended install path is `pip install <github_url>` in the future | Owner answer, 2026-10-15 |
| D-20 | No `kitId` meaning is asserted in the documentation; the in-template legend is stale and will be replaced by the owner when the current API mapping is known | Owner answer, 2026-10-15 |

### Questions for the product owner (all answered 2026-10-15)

---

**Q-01 · answered (D-06) · CRITICAL impact — the shipped report templates do not run**

- *Repository state:* 30 of the 32 bundled report entries fail `ReportSettings`
  validation — they use `period_type`/`period_num` instead of
  `time_unit`/`unit_count`, `report_type` values like `TV_CROSSTAB` /
  `REG_TV_CROSSTAB` that are not in the enum, and omit the required `media` and
  `relative_path` fields. The only conforming example is
  [`settings/reports/nat_tv_brands_tefal.yaml`](../../src/mko_get_mediascope_data/settings/reports/nat_tv_brands_tefal.yaml).
  Verified by probe (lead: audit TPL-001).
- *Owner's answer (2026-10-15):* option (a) — **migrate all bundled templates to
  the current schema and keep them runnable.** Templates and reports are
  incomplete by design and will be extended as the product is used; nothing is
  to be deleted (D-06, D-11).
- *Recorded as gap:* G-05 — templates are not runnable today; repairing them is
  accepted work (D-06), not a dispute about intent. **Resolved by W-01
  (2026-10-16)** — all 32 bundled entries now pass `ReportSettings` validation;
  all 6 DEFAULT templates ship; both crosstab defaults parse correctly.

---

**Q-02 · answered (D-07) · HIGH impact — what columns should an exported CSV contain?**

- *Repository state:* the current code writes the configured slices plus
  statistics (plus `targetAudience` when several audiences are requested, plus
  pivoted frequency columns when frequency distribution is configured) —
   `core/reports.py:437-449`, `:571-596` (column select/rename + pivot).
   Historical export files on the development machine (e.g. `output/tv_report/top_adv/*.csv.gz`, dated before
  the 2026-02/2026-03 code changes) contain the two slice columns duplicated
  and **no** statistics columns.
- *Owner's answer (2026-10-15):* **slices + statistics (the current code) is
  correct, and may differ per report type — that is exactly why the Strategy
  pattern is used for data output: each report type gets its own strategy.
  There is no single fixed option** (D-07). The DICT strategy is the intended
  per-type mechanism, not an anomaly.
- *Consequence for the historical files:* the old duplicated-column layout is
  superseded by the current code; no change requested.

---

**Q-03 · answered (D-08) · HIGH impact — behavior when the requested period is outside the available data**

- *Repository state:* the requested period is clipped to the window the API reports (`reports.py:322-325`); an entirely non-overlapping request raises `NoReportFoundError` and exits 1; a partially overlapping request logs `logger.warning()` about the uncovered remainder and exports the overlapping part — both since W-02 (G-01), resolving the original silent-exit behavior.
- *Owner's answer (2026-10-15):* **never interrupt processing** — one report
  failing must not break another. Emit a warning and continue. If the requested
  period partially overlaps the data, export the overlapping part and warn that
  the rest has no data. If there is no overlap at all, warn only and export
  nothing (D-08).
- *Recorded implementation gap:* G-01 (the code was silent before W-02). **Resolved by W-02** — see the [gaps table](#gaps-between-owner-intent-and-current-implementation). `get_period` now intersects the requested range with the API window explicitly: entirely out-of-range raises `NoReportFoundError`; partial overlap logs `logger.warning()` about the uncovered remainder and exports the overlapping part; a zero-interval run emits a warning naming the requested range instead of a silent exit-0 success.

---

**Q-04 · answered (D-09) · HIGH impact — failure policy for individual tasks**

- *Repository state:* logical failures (no task key, empty result, missing
  columns) mark `task.status = False`, write an error YAML next to the output
   folder, and the run continues (`reports.py:344-361`, `:412-449`). Raised
  exceptions inside the worker are caught by `try/except/finally` since W-02:
  `task_done()` is always called in `finally` (`reports.py:149-188`), so
  `queue.join()` returns normally.
- *Owner's answer (2026-10-15):* **never stop the export because one task
  failed — this is critical.** A user can re-run; the current mechanism checks
  already-exported periods and starts only the unfinished tasks. The mechanism
  that saves a separate description of a failed task is critical and must be
  described: if a problem persists across re-runs, the user studies it from
  that description (D-09).
- *Recorded implementation gap:* G-02 — the error-description file existed for
  logical failures but was bypassed by raised exceptions (worker hang), and its
  `yaml.dump` output could not be re-loaded with `safe_load`. **Resolved by W-02** —
  see the [gaps table](#gaps-between-owner-intent-and-current-implementation).
  The worker now catches raised exceptions, writes a re-loadable error YAML
  (`yaml.safe_dump`), and always calls `task_done()` in `finally`.

---

**Q-05 · answered (D-10) · MEDIUM impact — regions in regional reports**

- *Repository state:* `TVRegTaskStrategy` iterates over an explicit `regions`
  list (report extra) or **all** catalog regions from `cats.get_tv_region()`,
  and always skips region id 99 (  `reports.py:539-540`). Region names come from
  the vendor catalog. No bundled template defines `regions`.
- *Owner's answer (2026-10-15):* default = all regions when the user specifies
  no list. The hard-coded exclusion is wrong and must be removed; the user
  template should be able to specify regions as a list (`id IN (1,2,3…)`) or an
  exclusion (`id NOT IN (99,100)`). The owner asked for a researcher to study
  the best modern implementation approach before coding (D-10, research item
  R-01).
- *Discrepancy recorded:* DISC-09 — the owner recalled that **99 and 100** are
  excluded; the code excludes **only 99** (verified at `reports.py:406`).
  *Owner clarification (2026-10-15):* region 100 was added to the exclusion
  recently, and the code will need to be rewritten anyway for R-01 — so the
  redesign replaces the hard-coded set entirely rather than extending it.

---

**Q-06 · answered (D-11) · MEDIUM impact — DEFAULT template coverage**

- *Repository state:* all 6 `(media, report_type)` DEFAULT templates ship and parse at HEAD (resolved by W-01); previously only `tv/crosstab`, `tv/timeband`, `tv_reg/crosstab` existed (the first and third were malformed YAML). The `ReportType` enum includes `simple`, which no client report template uses but for which DEFAULT templates now ship.
- *Owner's answer (2026-10-15):* reports and templates are not finished yet and
  will be implemented as the product is used; nothing is to be deleted
  (D-11). Missing combinations are pending work, not rejected features.
- *Recorded as gap:* G-05 — previously 3 of 6 DEFAULT templates were missing and
  both crosstab defaults were malformed; **RESOLVED by W-01 (2026-10-16)** — all
  6 now ship, all 32 bundled report entries pass validation.

---

**Q-07 · answered (D-12) · LOW impact — credential file name**

- *Repository state:* `init` copies
  `settings/connections/mediascope_RENAME_.json`, while the code reads
  `settings/connections/mediascope.json` (`paths.py:60`, `init_service.py:60-90`
  migrates the legacy name). On the development
  machine the user performed the rename manually (verified: only the renamed
  file exists locally).
- *Owner's answer (2026-10-15):* "Recommend the best modern practice for this
  behavior." Recommendation recorded in the Target section of
  [product-overview.md](product-overview.md#target-behavior-owner-confirmed)
  (D-12): ship the template under its final file name with placeholder values,
  have `init` copy it verbatim, add the real file to `.gitignore`, and fail at
  startup with an explicit "credentials missing" message; implementation is a
  pending decision, not taken here.

---

**Q-08 · answered (D-13) · MEDIUM impact — who consumes the exported CSVs and their format**

- *Repository state:* outputs are `;`-separated, comma-decimal, UTF-8-with-BOM,
  optionally gzipped; line endings follow the operating system
  (`utils.py:211-220`). The repository does not say what reads them.
- *Owner's answer (2026-10-15):* the current conventions are correct and
  intentionally hard-coded for now. Future intent: defaults in
  `settings/app_config.yaml` (e.g. separators), with the ability to override
  them per report in the user's own report file; the design is not yet decided
  (D-13, research item R-02). The concrete downstream consumer remains
  undocumented.

---

**Q-09 · answered (D-14) · MEDIUM impact — the dictionary (`DICT`) output**

- *Repository state:* subtypes whose name contains `DICT` produce a melted
  table with the fixed columns `action, search_column_idx, value, term, cat,
  adv, bra, sbr, mdl, cln_0 … cln_5` (`reports.py:599-652`). The bundled
  example is [`nat_tv_brands_tefal.yaml` entry 2](../../src/mko_get_mediascope_data/settings/reports/nat_tv_brands_tefal.yaml).
- *Owner's answer (2026-10-15):* confirmed the hard-coded columns are bad.
  Future intent: a user settings file defining which columns the dictionary
  gets and how, implemented without breaking the current infrastructure;
  research needed first (D-14, research item R-03). What consumes the file
  remains undocumented by the owner.

---

**Q-10 · answered (D-15, D-16, D-17) · MEDIUM impact — success criteria for a run**

- *Repository state:* proposal (see
  [product-overview.md](product-overview.md#success-criteria)); the code meets
  it only partially today.
- *Owner's answers (2026-10-15):*
  1. "Every entry produces files or per-task errors" holds **except** for
     missing data: when the database has no data for the requested period, the
     tool must only **warn**, not create an error file. Error files are written
     only when the remote database rejected the calculation or the task could
     not be processed (network errors and similar) (D-15).
  2. Idempotency is **not guaranteed**: the product is strongly dependent on
     network and run time. Ideally identical requests return identical data,
     but network failures break that, and a period that does not exist today
     may exist in a week, so a re-run can return a different result. Accepted
     (D-16).
  3. **No fail-fast** — exit codes must not stop the program; the goal is to
     pull as much as possible. Analogy: a chunked downloader where one failed
     block does not restart the whole download, because restarting from zero is
     worse; a re-run continues what is unfinished (D-17).
- *Consequence:* the criteria were rewritten in
  [product-overview.md](product-overview.md#success-criteria) to match these
  answers.

---

**Q-11 · answered (D-18) · LOW impact — traceability of successful tasks**

- *Repository state:* the full request settings are persisted only for
   **failed** tasks (`core/tasks.py:27-57`).
- *Owner's answer (2026-10-15):* correct as-is — only failed tasks are logged;
  successful runs would produce gigabytes of useless reports given millions of
  requests (D-18).

---

**Q-12 · answered (D-19) · MEDIUM impact — product direction and packaging**

- *Repository state:* the README presents a standalone CLI plus a
  Jupyter/Airflow library published to **Test** PyPI, version 1.0.8
  (`pyproject.toml:7`). `run` accepts exactly one file per invocation; a file
  may contain several report entries.
- *Owner's answers (2026-10-15):* (1) one invocation = one report file that can
  contain a block of exports — the current design; (2) both usage modes are
  intended: analysts on demand (e.g. Jupyter) and a scheduler (e.g. Airflow);
  (3) **do not publish to PyPI**; the future install path is
  `pip install <github_url>` (D-19).
- *Discrepancy recorded:* DISC-10 — the README's PyPI install instructions
  contradict the owner's stated install path.

---

**Q-13 · answered · MEDIUM impact — dataset identifiers (`kitId`) in templates**

- *Repository state:* templates set `options.kitId` (values 1, 3, 7 observed)
  with an in-file legend that only explains 1–6; `TOP_NAT_TV_PROGRAMS` uses
  kitId 7 with the comment "TV Index Russia all". The value filters the
  availability window (`reports.py:259-261`, `:265-277`).
- *Owner's answer (2026-10-15):* the entry is **outdated and should be
  removed**; the API has changed and the owner has no current information on
  what each `kitId` means; the owner will fill in the data when it becomes
  available. Until then the documentation must not assert any `kitId` meaning
  (D-20).
- *Recorded discrepancy:* DISC-11 — the kitId legend in the bundled templates
  is stale.

### Discrepancies between sources

| ID | Discrepancy | Sources | Current handling in this documentation |
|---|---|---|---|
| DISC-01 | Command name `list_reports` vs actual `list-reports` | README/AGENTS.md vs CLI (verified exit code 2 for the underscore form) | **Resolved** — README and AGENTS.md corrected to `list-reports` (2026-10-16) |
| DISC-02 | README documents `run --no-verbose`, which does not exist (verified exit code 2) | README vs `app.py:42` | **Resolved** — invalid example removed from README (2026-10-16) |
| DISC-03 | README's quick-start example (`nat_tv_brands_last.yaml`) fails validation | README vs `ReportSettings` (probe) | Covered by D-06 repair work |
| DISC-04 | Shipped credential template `mediascope_RENAME_.json` vs expected `mediascope.json` | `settings/connections/` vs `paths.py:60` (verified; lead: audit CFG-003) | Q-07 / D-12 recommendation recorded |
| DISC-05 | `multiple_files` comment says "if period exceeds 1 year, split exports by month/week"; the code splits by `period_model.time_unit` | `models.py:103-105` vs `reports.py:317` (verified) | Documented current behavior (code); comment treated as stale |
| DISC-06 | The audit corpus is pinned to frozen baselines (`d093d9c`/`b04d177`), contains errors, and several entries no longer match HEAD | `.ai/audit/99-validation/*` vs HEAD | Per owner ruling D-05 the audit is a lead source only; these docs follow the code and re-verify each claim |
| DISC-07 | `defaults/tv/crosstab.yaml` parses at commit `3408747` and fails from `b04d177` onward — the i18n translation commit introduced the YAML syntax error at line 138 | Git probe | Documented as the cause of the TV-defaults outage; covered by D-06 repair work |
| DISC-08 | The owner's personal settings folder still uses the legacy dialect and older templates, and its exports predate the current column-selection code | Machine state vs HEAD | Used as evidence for Q-01/Q-02, not as a specification |
| DISC-09 | The owner recalled that regions **99 and 100** are excluded; the code excludes **only 99** | Owner answer vs `reports.py:539-540` (verified) | Recorded for the region-option redesign (R-01): start from the code |
| DISC-10 | README instructs installation from (Test) PyPI; the owner's stated path is `pip install <github_url>`, with no PyPI publication | README vs owner answer (D-19) | **Resolved** — README corrected to `pip install git+https://github.com/manicko/mko_get_mediascope_data.git` (2026-10-16) |
| DISC-11 | The `kitId` legend and values in the bundled templates are stale per the owner | Templates vs owner answer | No `kitId` meaning is asserted anywhere in these docs |

### Gaps between owner intent and current implementation

Recorded per the owner's answers. G-01 and G-02 have been **resolved by W-02**
(error semantics overhaul — see
[delivery plan](../../.ai/problems/01-delivery-plan.md#sprint-2--w-02-error-semantics)).
G-05 has been **resolved by W-01** (all 6 DEFAULT templates ship, all 32
entries validate). G-03–G-04 remain open pending their respective work items.
The implementation sequence and agent assignments for all items are tracked in
[`.ai/problems/01-delivery-plan.md`](../../.ai/problems/01-delivery-plan.md).

| Status | ID | Intent (owner decision) | Current code | Impact |
|---|---|---|---|---|
| **RESOLVED** — W-02 slice 3 (entirely out-of-range → `NoReportFoundError`, PIPE-005; partial overlap → `logger.warning()` + export overlap, PIPE-003/PIPE-004; zero-interval → warning, not silent exit) | G-01 | Warn on missing/partial period data; never interrupt (D-08) | `get_period` now intersects the requested range with the API window explicitly: entirely out-of-range raises `NoReportFoundError` (PIPE-005); partial overlap logs `logger.warning()` about the uncovered remainder and exports the overlapping part (PIPE-003, PIPE-004); a zero-interval run emits a warning naming the requested range instead of a silent exit-0 success | **RESOLVED** — analysts can now distinguish "no data" (warning) from "done"; the multi-report safety intent of D-08 is implemented |
| **RESOLVED** — W-02 slices 1-2 (worker `try/except/finally` converts exceptions to a failed-task error artifact, PIPE-002/PIPE-007; `log_error` branching distinguishes "no data" warnings from "processing failure" error files, PIPE-009; `yaml.safe_dump` makes error YAML re-loadable, D-09) | G-02 | A failed task always leaves a persistable description (D-09); network failures must not stop the run | Worker now wraps its body in `try/except Exception` that sets `task.status = False`, reaches `TVTask.to_yaml`, and calls `task_done()` in `finally` (PIPE-002, PIPE-007); `log_error` is consumed by branching logic: "no data" → `logger.warning()` with no error file (D-15); "processing failure" → error YAML written with `yaml.safe_dump` so it is re-loadable with `safe_load` (PIPE-009, D-09) | **RESOLVED** — the recovery path works for all failure classes; raised exceptions in workers no longer hang `queue.join()` |
| OPEN — pending W-03 (R-01 region-selection design) | G-03 | Region selection belongs in the user template, not hard-coded (D-10) | Region id 99 is hard-coded as excluded (`reports.py:539`); no `regions`/exclusion support in any template | Users cannot include 99 or express an exclusion list; the owner requires template-driven region selection (D-10) |
| OPEN — pending W-04 (R-02 output-conventions design) | G-04 | Per-report overrides of output conventions (D-13, D-14) | Separators/decimal/BOM/compression and the DICT column layout are hard-coded (`utils.py:211-220`, `reports.py:599-652`) | Users cannot adapt output to their downstream consumers |
| **RESOLVED** — W-01 (2026-10-16) | G-05 | Bundled templates stay runnable (D-06) | All 6 DEFAULT templates ship and parse (formerly 3 were missing and both crosstab defaults were malformed); all 32 bundled report entries pass `ReportSettings` validation | No bundled report runs end-to-end → all templates parse and validate; `nat_tv_brands_tefal.yaml` is the reference runner |

### Assumptions awaiting validation

| ID | Assumption | Basis | Risk if wrong |
|---|---|---|---|
| A-01 | The folders under `settings/reports/` correspond to clients or analytical projects (`gap_resurs`, `sovcombank`, `last_category`, `tefal`, `theiss`, `tv_report`) | Directory names; user's local settings contain a `nat_tv_brands_theiss.yaml` not present in the package | Template organization may need restructuring |
| A-02 | Audience-name suffixes such as `BC` encode income groups (e.g. `incomeGroupRussia IN (2,3)`) per the analysts' convention | Template contents (`all 20-45 BC` ↔ `incomeGroupRussia IN (2,3)`) | Mislabeled outputs |
| A-03 | The dictionary output feeds a downstream search-term configuration process | Column names (`term`, `action=upd`, `search_column_idx`) | Wrong dictionary layout |
| A-04 | The concrete consumer of the CSV exports (and therefore the required format conventions) is outside this repository | No consumer is documented anywhere in the repository | Format choices may need to change (see D-13) |

### Planned research items (owner-requested)

Not to be implemented within the documentation task; each needs a design
decision first.

| ID | Topic | Related decision | Notes from the owner |
|---|---|---|---|
| R-01 | Region selection design: template-expressed inclusion/exclusion lists (`id IN (…)` / `id NOT IN (99,100)`), all-regions default, removal of the hard-coded exclusion | D-10 | "Launch a researcher and study modern practices for this kind of implementation" — executed per the 3 × researcher → validator pattern (see `.ai/problems/01-delivery-plan.md`) |
| R-02 | Configurable output conventions: defaults in `settings/app_config.yaml`, per-report overrides | D-13 | Owner has not decided the design yet |
| R-03 | Configurable dictionary (`DICT`) output columns via a user settings file, without breaking the current infrastructure | D-14 | Owner has not decided the design yet |

### Precision note on G-02: which failures hang, and the owner's observation

> **Resolved by W-02.** The failure-class diagram below is preserved as the
> pre-implementation record. After W-02 (commit `3f3dd56`), the worker wraps its
> body in `try/except Exception` → `finally: task_done()`, so the third class (raised
> exception inside a worker) no longer hangs `queue.join()`; it is converted to a
> failed task with an error artifact written via `yaml.safe_dump` (PIPE-002,
> PIPE-007). The owner's observation about not noticing the hang in practice
> remains valid as historical context.

The failure classes in the current code are distinct, and only one of them
hangs (all verified by probe against the real worker):

| Failure | Where it happens | Current outcome |
|---|---|---|
| Validation error, missing/malformed template, connection/auth setup failure | **Before** the worker pool (`create_report` → `get_period` → `get_available_period`) | Clean abort: error printed by the CLI, exit code 1 — visible (`reports.py:220-223`) |
| Logical failure: no task key, empty result, missing columns | **Inside** a worker, but signalled as `status=False`, not an exception | Error YAML written via `yaml.safe_dump`, warning logged, run continues — the recovery path the owner knows (`reports.py:149-188`) |
| Raised exception inside a worker: `BadRequestError`, `AuthorizationError`, `RuntimeError("Network retry limit exceeded")`, unexpected `AttributeError` | **Inside** a worker (`send_task` / `wait_task` / `extract_data`) | **Resolved by W-02.** The worker's `try/except/finally` catches the exception, marks the task failed, writes an error YAML via `yaml.safe_dump`, and calls `task_done()` in `finally` — so `queue.join()` returns and the run continues. The pre-W-02 behavior (hang, no traceback) was probe-verified but is no longer current (`reports.py:149-188`). |

*Owner's observation (2026-10-15):* errors were encountered in practice, but a
worker hang was never noticed. This does not contradict the mechanism (the
hang was reproduced with the real `MediaReport.worker` against both
`RuntimeError` and `BadRequestError` — probe scripts in `.ai/tmp/`); it
indicates that the observed errors belonged to the first two classes, where
the run ends visibly or continues. Consistent explanations for not noticing
class three: an exception inside the pool had not been hit yet; or a stuck run
was killed and re-run, and because `check_done` skips already-written files the
re-run looks like a normal resume. Recorded as an observation pending runtime
confirmation — the mechanism stands, its observed frequency is unknown.

### Known limitations (documented, not fixed)

This task was investigation-only; items below marked **[fixed by W-02]** were
subsequently resolved by the W-02 error-semantics implementation. The remaining
items are still open. Each item was re-verified against the code (probes and
readings are listed in [Current Behavior](current-behavior.md#verification));
per owner ruling D-05 the audit IDs are lead pointers, not evidence:

- [Fixed by W-02, G-01] `get_period` crashed when `date_filter` was combined
  with `last_time: null` (PIPE-003); now handled correctly.
- [Fixed by W-02, G-01] Out-of-window periods produced zero tasks with a
  success exit — now `get_period` raises `NoReportFoundError` for entirely
  out-of-range requests and `logger.warning()` for partial overlap (PIPE-004,
  PIPE-005).
- [Fixed by W-02, G-02] A worker exception left `task_done()` uncalled and
  `queue.join()` never returned — the run hung instead of failing (PIPE-002);
  now the worker has `try/except/finally` so a failure writes an error artifact
  and the run continues.
- [Fixed by W-02, G-02] Error YAML was written with `yaml.dump`
  (`!!python/tuple`) and raised exceptions never reached the error path;
  now `yaml.safe_dump` is used and all failure classes produce a re-loadable
  error file (PIPE-007, PIPE-009).
- [Fixed by W-01 (2026-10-16)] Bundled report templates failed validation
  (30 of 32 entries used the stale `period_type`/`period_num` schema and
  missing `media`/`relative_path` fields; both crosstab DEFAULT templates were
  malformed YAML). Now all 32 entries pass validation and all 6 DEFAULT templates
  ship (see G-05, test_models.py::test_bundled_templates_validate).
- [Fixed by W-02 (2026-10-16)] Duplicate CSV writes were swallowed; the task was
  still reported as ready (PIPE-001). Now `csv_to_file` uses `mode="w"` and the
  `FileExistsError`-swallow path is removed.
- [Fixed by W-02 (2026-10-16)] The poll loop had no deadline; `FAILED`/`CANCELLED`
  statuses still attempted extraction (PIPE-006). Now `wait_task` has
  `max_polls=60` and `poll_deadline=3600.0` guards, and `FAILED`/`CANCELLED` marks
  the task failed without attempting extraction.
- `list-reports` reads the user settings folder while `run` accepts any path.
- Region id 99 is hard-coded as excluded (see G-03).

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Product Overview](product-overview.md)
- [Current Behavior](current-behavior.md)
- [Data Flow Architecture](data-flow-architecture.md)
- [System Architecture](system-architecture.md)
