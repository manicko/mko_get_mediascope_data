---
phase: "13"
phase_name: "performance"
id_prefix: "PERF"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/13-audit-performance.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

<!-- JSON5-MIGRATION TODO: This validated-findings doc references pre-migration .yaml
  file paths that are now .json5. Anchors need re-verification in a future audit pass;
  this does NOT block the implementation PR. Verdicts remain factually accurate as
  historical records of the code_ref (d093d9c). -->

# Audit Findings — Validation Report: Performance & Scalability

## Summary

Three findings, all MEDIUM: PERF-001, PERF-002, PERF-003. All three anchors resolve to the
cited source locations; all three disambiguator tokens exist inside their primary anchors; and
all three substantive claims reproduce under static source analysis of `core/network.py`,
`core/reports.py`, `core/services.py`, and the vendored `mediascope_api/mediavortex/tasks.py`.

The two runtime probes the report relies on (Semaphore cap = 5 in Appendix A; `result2table`
materializing 10 000 rows in Appendix E) could not be re-run verbatim in this environment (no
live Mediascope API, and the probes import the vendored client), but each is **structurally
confirmed** by source inspection and the backoff-formula recompute. The test run (23 passed at
HEAD; 12 passed at `code_ref d093d9c`) matches the reported count.

Two supporting-evidence imprecisions are recorded as VAL findings: PERF-003's grep repro
overstates a match count (claims 1 match for `perf_counter|timeit|elapsed|duration`; the tree
returns 0 — see VAL-001), and PERF-002's escalation Impact example cites numbers that do not
match the referenced `nat_tv_brands_last.yaml` (see VAL-002). Neither alters a verdict: the
core claims hold under direct inspection. A third VAL finding records drift in the Phase 13
instruction's namespace premise (see VAL-003). A fourth VAL finding records stale
cross-phase citations of ASY-004 as a "pool/Semaphore decoupling" defect — ASY-004 was since
re-characterized to not substantiated / LOW (the two-level design is intentional); the
citations are corrected in place and no PERF verdict changes (see VAL-004).

**3 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 merged, 0 unsettled.**

## Scope

**Examined (source):** `core/network.py` (`NetworkClient.__init__`, `NetworkClient.call`),
`core/reports.py` (`MediaReport.__init__`, `MediaReport.worker`, `processing_tasks`,
`TVMediaReport.send_task`, `wait_task`, `extract_data`, `prepare_data`,
`DefaultTVTaskStrategy`, `DefaultDataStrategy`, `DictDataStrategy`), `core/services.py`
(`AppService.run_report`), vendored
`<repo>/.venv/Lib/site-packages/mediascope_api/mediavortex/tasks.py` (`build_task`,
`get_status`, `get_result`, `result2table`), `settings/reports/last_category/nat_tv_brands_last.yaml`.

**Examined (tests):** `tests/test_network.py`, `tests/test_reports.py`, `tests/test_services.py`.

**Examined (audit corpus):** `13-audit-performance.md` (severity taxonomy, scope, namespace
premise), `templates/audit-findings.md`, sibling findings `09-external-api`,
`04-data-pipeline`, `03-async-concurrency`, `01-entry-architecture`, `02-config-secrets`.

**Runtime probes performed:** backoff-formula recompute (`40,48,57.6,69.1,82.9,99.5,119.4,143.3,
172.0,206.4` → 1038.3 s tail sum, 1168.3 s worst case); full-repo content grep for `PERF-`
(excluding `.venv`/`.git`); grep for timing primitives in `core/`.

**Runtime probes relied upon (not re-runnable here):** Phase 13 Appendix A (Semaphore cap = 5)
and Appendix E (result2table materializes 10 000 rows). Both structurally confirmed by source.

**Not verified:** live Mediascope API (no credentials); end-to-end `create_report` run (all
tests mock `create_report` entirely — see Evidence Index).

## Conformance

**Validated against:** commit `d093d9c` (Phase 13 baseline)
**HEAD at re-validation:** `8ed385f` (commit `e8fd1867` code-identical to `b04d177`)
**Survival at HEAD:** all 3 findings survive — no per-task clock calls in worker/wait_task/send_task/extract_data (reports.py:134-332); sole per-task signal is `logger.info("=")` at reports.py:265 (was `print("=", end="", flush=True)` at code_ref — same line, function upgraded to logging); report-level timing moved services.py:110→137, :116→143, :153→179.
**Correction type:** pytest 12→23; git HEAD d093d9c→8ed385f; `print("=")`→`logger.info("=")`; services.py line-number shift; test mock line shift (47-50→49-52); PERF-003 cross-reference to ENT-005 noted as rejected at HEAD.

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). All 3 findings re-verified at HEAD. Scope precision confirmed: the
semaphore + initial-jitter cost applies to `send_task`'s local `build_task`
(reports.py:258-260) and `extract_data`'s local `result2table`
(reports.py:309-311); `csv_to_file` is dispatched via bare
`asyncio.to_thread` (reports.py:325) and is NOT semaphore-gated, and
`DefaultDataStrategy.prepare` runs synchronously in the event loop — the
finding's own text already states this correctly. PERF-002's vendor-side
mechanism confirmed (`get_result` / `result2table` hold the full `resultBody`
in memory — `.venv/Lib/site-packages/mediascope_api/mediavortex/tasks.py:1422-1424, 1455-1458`).
Mapping: PERF-001..003 → advisory; no dedicated work item in
`.ai/problems/01-delivery-plan.md`. The retry-policy half of the PERF-001
neighborhood overlaps W-02 slice 4 (poll deadline) and the EXT-005 retry
classification fix (429/5xx currently un-retried) — coordinate if W-02 grows a
performance slice; PERF-003's counters overlap EXT-007.

---

| ID | Disposition | Severity | Anchor resolves | Disambiguator exists |
|---|---|---|---|---|
| PERF-001 | confirmed | MEDIUM | network.py:59 (`NetworkClient.call`) + reports.py:252,295 | `async with self._semaphore` at network.py:73 ✓ |
| PERF-002 | confirmed | MEDIUM | reports.py:84 (`MediaReport.__init__`) + reports.py:295 (`extract_data`) | `asyncio.Queue(maxsize=self.max_tasks)` at reports.py:115 ✓ |
| PERF-003 | confirmed | MEDIUM | services.py:110 (`AppService.run_report`) + reports.py:134 (`MediaReport.worker`) | `start_time = datetime.now()` at services.py:116 ✓ |

---

### PERF-001 — confirmed — [MEDIUM] CPU/local DataFrame work is routed through the I/O Semaphore and the initial-jitter gate

**Block 1 — Claim re-derived from executing path.**
`NetworkClient.call` (network.py:59-124) applies the initial jitter on **every** invocation
at line 69 — *before* the retry loop and *before* `async with self._semaphore` (line 73):

```python
async def call(self, func, *args, sleep_time=2, **kwargs):       # 59
    _sleep_time = max(sleep_time, self._sleep_time)               # 66
    await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))  # 69 — BEFORE semaphore
    for attempt in range(1, self._max_attempts + 1):               # 71
        try:
            async with self._semaphore:                           # 73
                return await asyncio.to_thread(func, *args, **kwargs)  # 74
```

`TVMediaReport.send_task` (reports.py:252-269) routes the **local** `build_task` through
`call` (line 258: `self.builder = self.report_service.build_task` is set at line 110; the
call site at 258-260 passes it to `network_client.call`). `TVMediaReport.extract_data`
(reports.py:295-332) routes the **local** `result2table` through `call` (lines 309-311).

**Backbone evidence — build_task / result2table contain no `send_request` (local, CPU):**
`grep -n send_request` in the vendored `tasks.py` returns matches at lines
952, 1124, 1142, 1174, 1240, 1272, 1305, 1335, 1370, 1402, 1424, 1819. None fall inside
`build_task` (lines 50-324) or `result2table` (lines 1426-1508). Line 1424 belongs to
`get_result` (1405-1424); 952 belongs to `_send_task`. The audit's cited subset (952,
1124, 1142, 1174, 1240, 1424) is a partial but correct enumeration of the relevant matches.

`DefaultDataStrategy.prepare` (reports.py:436-454) is `async def` with **no `await`** in its
body — it runs synchronously in the event loop — while `csv_to_file` is dispatched via bare
`asyncio.to_thread` (reports.py:325) *without* the Semaphore. CPU/local work is thus
inconsistently either over-gated (Semaphore) or under-isolated (event loop). **Claim reproduced.**

**Block 2 — Support that is only a green control.** None. The findings' support is source
inspection plus two runtime probes (Appendix A: Semaphore caps `call()` at 5 — structurally
guaranteed by `asyncio.Semaphore(max_concurrent_requests)` at network.py:55 with the default 5
from network.py:51; Appendix B: backoff formula recompute matches exactly). No shipped test
asserts the absence of the defect — `test_network.py` only covers retry exhaustion; it does
not assert that CPU work bypasses the Semaphore.

**Block 3 — Grade against Phase 13 rubric.** Phase 13 MEDIUM = "a cost demonstrated but
conditional on volume or configuration not yet in effect, where the design is unbounded
rather than the present value being large." Phase 13 HIGH reserves the "polling loop with no
max-wait," "retry holding Semaphore across backoff," and "single oversized result" examples.
PERF-001's own Appendix B confirms the "retry holds Semaphore across backoff" HIGH scenario
**does not apply** (the `async with` wraps only `to_thread`; an exception releases the permit
before the backoff sleep at network.py:124). No single invocation is unbounded. The cost is
real and on every task but conditional on volume/size. MEDIUM matches; no re-grade.

**Block 4 — Load-bearing artefact.** Code (`core/network.py::NetworkClient.call`,
`core/reports.py::TVMediaReport.send_task`/`extract_data`). The fix changes code, not docs.

**Block 5 — Recommendation carry-out.** Fix targets `NetworkClient.call`,
`TVMediaReport.send_task`, `TVMediaReport.extract_data` — all resolve and are stable. The
recommendation (route CPU/local work via `to_thread` without the I/O Semaphore; suppress the
initial jitter for non-API calls) is implementable. It requires `call` to distinguish API from
local invocations (e.g. a flag). Dependency: `test_services.py` mocks `create_report` entirely
and patches `NetworkClient.call`, so the worker loop is not exercised there — the fix cannot
break it; verified by test source (tests/test_services.py:47-50). ✓

**Block 6 — Cross-phase.** ASY-004 (Phase 03, validated) originally alleged that the
`max_tasks` worker pool and the `max_concurrent_requests` Semaphore are a "decoupled"
defect; it was since re-characterized to **not substantiated / LOW** — the two levels are
intentional and independent (Level 1 bounds in-flight tasks in the remote calculation base;
Level 2 bounds client HTTP concurrency and polling cadence). PERF-001 is unaffected by that
re-characterization: it reports the *cost* of routing CPU/local work (`build_task`,
`result2table`) through the I/O gate (Semaphore + initial jitter) — a distinct performance
angle, not a correctness claim about the two-level design. EXT-002 (Phase 09, validated) is
the *timeout* gap on the `to_thread` bridge — distinct. No conflict; no merge.

---

### PERF-002 — confirmed — [MEDIUM] No data-volume backpressure; oversized result loads full resultBody into memory

**Block 1 — Claim re-derived.** `MediaReport.__init__` (reports.py:84-120) sets
`self.max_tasks = max_tasks` (default 100, reports.py:92) and
`self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (reports.py:115) — a **task-count**
cap only. `services.py` run_report (lines 138-147) constructs `TVMediaReport` without passing
`max_tasks`, so the default 100 applies. `TVMediaReport.extract_data` (reports.py:295-332)
calls `get_result` then `result2table` with **no byte/size guard**.

Vendored `result2table` (tasks.py:1426-1508) materializes the entire payload:
```text
tasks.py:1451  res = {}
tasks.py:1458  if isinstance(data['resultBody'], list) and len(data['resultBody']) == 0: ...
tasks.py:1466  for item in data['resultBody']: ... res[k].append(v) ...
tasks.py:1479  for item in data['resultBody']: ... res[k].append(v) ...
tasks.py:1496  df = pd.DataFrame(res)
```
No streaming, no size threshold — confirmed by source read (lines 1451-1496). The Phase 13
Appendix E probe (10 000 rows fully materialized) is the runtime corroboration. **Claim reproduced.**

**Block 2 — Support that is only a green control.** The 12-passing test suite provides no
assertion on result-size handling — `test_services.py` mocks `result2table` to return an
empty DataFrame (line 33). Not a green control for this finding; the finding rests on source
inspection.

**Block 3 — Grade against Phase 13 rubric.** Phase 13 HIGH = "a single oversized result
that can starve the worker pool or exhaust memory." Phase 13 MEDIUM = "a cost demonstrated
but conditional on volume... where the design is unbounded rather than the present value
being large." The audit correctly identifies the absent size cap as the MEDIUM design gap and
the oversized-result starvation as the HIGH *escalation* ("conditional on result volume not
present for small reports; the HIGH taxonomy example ... is the escalation once data volume is
large"). The design is unbounded; the present-value consequence requires large data. MEDIUM
matches the rubric; no re-grade.

**Block 4 — Load-bearing artefact.** Code (`core/reports.py::MediaReport.__init__`,
`core/reports.py::TVMediaReport.extract_data`, vendored `result2table`). Code-side.

**Block 5 — Recommendation carry-out.** Fix targets `TVMediaReport.extract_data` — exists and
is stable. The recommendation (cap `result2table` input or stream slices; bound in-flight data
volume independently of task count) is actionable. New test required:
`test_large_result_is_rejected_or_streamed` (currently absent). ✓

**Block 6 — Cross-phase.** Cross-referenced EXT-005 (Phase 09, validated) is the retry/429
classification gap; PIPE-004 (Phase 04, validated) is the zero-task path. Neither overlaps
PERF-002's volume-backpressure concern. No conflict; no merge. (PERF-001+PERF-002 are the
merge candidate per Fix Order — co-located in `extract_data`.)

---

### PERF-003 — confirmed — [MEDIUM] No per-task latency measurement or attribution

**Block 1 — Claim re-derived.** `AppService.run_report` (services.py:137-186 at HEAD; 110-160
at `code_ref d093d9c`) emits only report-level timing via `logger`: `start_time = datetime.now()`
(line 143 at HEAD; 116 at code_ref), `end_time` (line 179; 153), and a total (services.py:180-184; 154-158).
`MediaReport.worker` (reports.py:134-153), `wait_task` (271-293), `send_task` (252-263), and
`extract_data` (295-332) contain **no clock calls** — confirmed by reading each method. The sole
per-task signal is a bare `logger.info("=")` at reports.py:265 (was `print("=", end="", flush=True)`
at `code_ref d093d9c` — same line, upgraded to the logging layer at HEAD, but still no timing context).

**Block 2 — Support that is only a green control.** The 12-passing tests mock
`create_report` entirely (tests/test_services.py:49-52 at HEAD; 47-50 at `code_ref
d093d9c`) and provide no per-task timing
assertion; `test_network.py` covers only retry exhaustion. No green control substantiates the
absence of latency instrumentation — the finding rests on direct source inspection.

**Block 3 — Grade against Phase 13 rubric.** Phase 13 MEDIUM explicitly names "a latency
measurement that is asserted but not emitted." `run_report` asserts/emits a report-level total
but emits **no per-task breakdown** — the audit's exact characterization. MEDIUM matches with
no re-grade.

**Block 4 — Load-bearing artefact.** Code (`core/services.py::AppService.run_report`,
`core/reports.py::MediaReport.worker`). Code-side; the fix adds structured logging.

**Block 5 — Recommendation carry-out.** Fix targets `MediaReport.worker`,
`TVMediaReport.send_task`/`wait_task`/`extract_data` — all resolve and are stable.
Recommendation (time each pipeline stage; log structured per-task records) is implementable.
New test required: `test_worker_emits_per_task_duration` (currently absent). Must not alter
`test_app.py` (CLI `--help` only). ✓

**Block 6 — Cross-phase.** Cross-referenced EXT-007 (Phase 09, validated: NetworkClient emits
no metrics), ASY-006 (Phase 03: queue depth non-observable), ENT-005 (Phase 01: `print()`
discipline — **rejected at HEAD**; `print()` replaced with `logger.info` at commit `606c899`,
but PERF-003's `logger.info("=")` at reports.py:265 is a different concern: it is the sole
per-task signal with no timing context, surviving as MEDIUM). PERF-003 is the per-task *latency*
attribution surface — distinct from EXT-007's retry-metrics gap. The unbounded poll
(PIPE-006 / ASY-005, Phases 04/03) and the `sleep_time=40` amplification (EXT-004, Phase 09)
are the escalation triggers for "stuck vs slow," owned elsewhere. No conflict; no merge.

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | PERF-003 repro overstates grep result for `perf_counter\|timeit\|elapsed\|duration` in `core/` (claims 1 match / `time_unit`; actual is 0) |
| VAL-002 | LOW | confirmed | PERF-002 Impact example `4 audiences × ~208 weekly slices ≈ 832 tasks` does not match `nat_tv_brands_last.yaml` (3 audiences, monthly, ~15-month period, TV_CROSSTAB not TV_REG) |
| VAL-003 | LOW | confirmed | Phase 13 instruction (13-audit-performance.md:154) asserts `PERF-` is "already in use ... with at least one identifier resolving to two different findings"; no such usage exists in the tree |
| VAL-004 | LOW | confirmed | Cross-phase citations of ASY-004 (Phase 03) as "ASY-004 (MEDIUM)" / "pool Semaphore decoupling correctness" are stale after ASY-004's re-characterization to not substantiated / LOW; citations corrected in place, no verdict impact |

### VAL-001 — [LOW] PERF-003 repro grep overstates match count

**Block 2 / Block 9 (support is only a green control; enumerated-check reliance).**

**Observed:** PERF-003's Repro states: "`grep` for `perf_counter`/`timeit`/`elapsed`/`duration`
across `core/` → 1 match, `time_unit` (models.py:64)." An independent content grep for
`perf_counter|timeit|elapsed|duration` across `src/mko_get_mediascope_data/core/` returns
**0 matches**. The `time_unit` field (models.py:64: `time_unit: Literal["d","w","m","y"] = "w"`)
does not contain any of the four patterns. `time_unit` appears in 5 locations (models.py:64,
reports.py:241, utils.py:102/112/115) — all about period-frequency configuration, unrelated to
latency timing.

**Impact on verdict:** None. The substantive claim ("no per-task clock calls") is independently
confirmed by direct inspection of `worker`, `wait_task`, `send_task`, `extract_data` — none
contain `perf_counter`, `datetime.now()`, or any elapsed-time computation. The grep was
supporting evidence; its overstatement does not sustain the finding (source inspection does).

**Disposition:** confirmed. No re-grade or rejection.

### VAL-002 — [LOW] PERF-002 escalation example cites config values that do not match

**Block 1 (claim re-derived).**

**Observed:** PERF-002 Impact states: "A single oversized result (driven by intervals ×
target audiences × regions for TV_REG, see `nat_tv_brands_last.yaml`: 4 audiences × ~208 weekly
slices ≈ 832 tasks)." The actual `nat_tv_brands_last.yaml` (last_category) contains two
reports:
- report[0]: `report_type=TV_CROSSTAB` (not TV_REG), `target_audiences` has **3** keys (not 4),
  `period.last_time.period_type='m'` (monthly, not weekly), `date_filter=['2025-10-01',
  '2026-12-31']` (~15 months → ~15 monthly slices, not 208 weekly).
- report[1]: `TV_DICT_CROSSTAB`, 1 audience, monthly.

The cited file is not a TV_REG report and its audience/slice counts do not match the example.

**Impact on verdict:** None. This is an illustrative escalation scenario in the Impact section,
not a claim about the executing path. The core finding — no size cap in `result2table`, no
volume guard in `extract_data` — is confirmed independently (Block 1 above). The TV_REG
task-volume model (regions × intervals × audiences) is valid per `TVRegTaskStrategy.generate`
(reports.py:384-432); only the specific example numbers are imprecise.

**Disposition:** confirmed.

### VAL-003 — [LOW] Phase 13 instruction asserts a `PERF-` collision that does not exist

**Block 7 (namespace integrity) / Block 8 (template as artefact).**

**Observed:** `13-audit-performance.md` line 154 states the `PERF-` prefix is "already in
use in shipped source, tests, workflows, and reports from a prior cycle, with at least one
identifier already resolving to two different findings; check for a collision before minting an
ID." An exhaustive content search for `PERF-` across `src/`, `tests/`, `.kilo/`, `.ai/`,
`workflows/`, `.github/` (excluding `.venv`/`.git`) returns matches in **only two files**:
(1) `13-audit-performance.md:154` (the instruction text itself) and (2)
`13-performance/findings.md` (the report being authored). There is no prior-cycle
`PERF-` report and no `PERF-` usage in shipped source, tests, or workflows.

**The findings' handling is correct.** Appendix C performed the collision check, found the
namespace empty, and reported "no collision... PERF-001..003 minted fresh under the existing
PERF- namespace (no second namespace introduced)." The Phase 13 instruction mandated exactly
this: "check for a collision... report it rather than creating a second namespace." The
findings.md complied.

**Impact on verdict:** None. The three PERF findings are valid regardless of the namespace
premise. The instruction's "already in use" assertion is drift in the phase instruction,
correctly resolved by the auditor.

**Namespace ruling (per Block 7):** The `PERF-` prefix is declared by Phase 13. No prior
identifiers were minted in the tree. No in-source provenance markers exist. No compound
form and no cross-phase reuse. The findings reused the declared `PERF-` namespace and did
**not** mint a second namespace. Ruling: **correct — no collision, single namespace.**

**Disposition:** confirmed.

### VAL-004 — [LOW] Stale cross-phase citations of ASY-004 ("decoupling correctness", MEDIUM)

**Block 6 (compare against sibling reports).**

**Observed:** This report's cross-phase sections cited ASY-004 (Phase 03, validated) in three
places as a MEDIUM "Semaphore/pool decoupling correctness" finding complementary to PERF-001
(PERF-001 Block 6, the Cross-Phase Analysis table, and the closing "No cross-phase conflicts"
paragraph). ASY-004 was subsequently re-verified and re-characterized to **not
substantiated / LOW**: the claim that "95 workers are perpetually blocked at the semaphore"
is factually incorrect (the smoothing sleep at network.py:69 precedes semaphore acquisition at
network.py:73), and the `max_tasks` (Level 1 — remote calculation capacity) /
`max_concurrent_requests` (Level 2 — client HTTP concurrency) decoupling is intentional
design, not drift. The citations are therefore stale in both severity and substance.

**Impact on verdict:** None. PERF-001's own claim — CPU/local work (`build_task`,
`result2table`) routed through the I/O Semaphore and the initial-jitter gate — is independent
of ASY-004's decoupling allegation and remains confirmed. The stale citations created only a
false implication that a decoupling *correctness* problem exists.

**Disposition:** confirmed. All three citations corrected in place to reference the
re-characterized ASY-004 and to describe PERF-001 as an independent performance angle.

## Cross-Phase Analysis

| Cross-phase pair | Phase | Finding | Phase 13 PERF finding | Shared concern | Disposition |
|---|---|---|---|---|---|
| Semaphore over-broad gating | 03 (validated) | ASY-004 (not substantiated / LOW — decoupling shown to be intentional two-level design) | PERF-001 (cost of shared gate for CPU/local work) | Semaphore + initial jitter applied to every `call()` invocation | distinct axes; ASY-004 re-characterization does not conflict with PERF-001; no merge |
| Wait-task unbounded poll | 04 (validated) / 03 (validated) | PIPE-006 (HIGH) / ASY-005 (MEDIUM) | PERF-003 (latency attribution) | `wait_task` `while True` no deadline | escalation trigger; owned elsewhere; not re-filed |
| Polling interval amplification | 09 (validated) | EXT-004 (HIGH) | PERF-003 | `sleep_time=40` → 20–80 s jitter per poll | escalation trigger; owned elsewhere |
| `to_thread` bridge timeout | 09 (validated) | EXT-002 (HIGH) | PERF-001 (over-gating) | `to_thread` has no deadline | related but distinct (timeout vs cost) |
| Retry/backoff contract | 09 (validated) | EXT-005 (MEDIUM) | PERF-001 (initial jitter) | backoff formula | PERF-001 is initial-jitter cost, not retry contract |
| No network metrics | 09 (validated) | EXT-007 (MEDIUM) | PERF-003 (per-task latency) | observability gap | PERF-003 = latency attribution; distinct axis |
| Queue depth observability | 03 | ASY-006 (MEDIUM) | PERF-002 (data-volume backpressure) | queue backpressure | PERF-002 = byte-volume; ASY-006 = observability; complementary |
| `print()` discipline | 01 (validated) / 02 (validated) | ENT-005 (MEDIUM) / CFG-010 (LOW) | PERF-003 (report-level `print`) | `print()` in core/ | acknowledged, not re-claimed by Phase 13 |

**Ownership check — Phase 13 domain:** Phase 13 owns "the measurable consequence of latency,
throughput, and capacity" (13-audit-performance.md:23). All three PERF findings fall squarely
in this domain: PERF-001 = cost of the concurrency ceiling; PERF-002 = memory/capacity
backpressure; PERF-003 = latency observability. No finding was filed under a phase that does
not own it.

**No cross-phase conflicts.** The findings correctly treat the pool↔Semaphore architecture
question (ASY-004 — re-characterized to not substantiated; the two-level design is
intentional), the retry contract (EXT-005/EXT-002), unbounded polling (PIPE-006/ASY-005), and
polling amplification (EXT-004) as owned elsewhere, and reference them only as escalation
context — not as re-filed defects.

## Rollout Safety Assessment

**Dependencies (validated from source):**
- PERF-001 off-gates `result2table`/`build_task` from the I/O Semaphore. `result2table` is
  called via `network_client.call` (reports.py:309); changing that path must keep
  `test_services.py` green — but that test mocks `create_report` entirely and patches
  `NetworkClient.call`, so the worker `extract_data` path is **not** exercised (tests/test_services.py:47-50). No shipped test asserts the current over-gating behaviour. ✓
- PERF-002 adds a size guard in `extract_data`; the happy-path DataFrame output is unchanged
  for results within the cap.
- PERF-003 adds per-stage timing + structured log records; must not alter `test_app.py`
  (CLI `--help` only) — confirmed it only tests `app.py` CLI invocation.
- PERF-001+PERF-002 merge candidate: both touch `extract_data` — co-locate in one change.

**Fix ordering (from findings.md Fix Order, consistent with source):**
1. PERF-001 + PERF-002 together — both rework `extract_data` routing/result handling.
2. PERF-003 independent (timing instrumentation, separate target methods).
No inter-finding ordering dependency among the three.

**Rollout risk:**
- PERF-001 off-gating: changes which calls acquire the Semaphore. Risk: if CPU work is moved
  to `to_thread` without the Semaphore, throughput on that path is uncapped relative to the
  I/O ceiling — acceptable if intended (CPU work shouldn't compete with I/O slots). No test
  asserts the cap on CPU paths. ✓
- PERF-002 size guard: must choose a rejection threshold that does not break currently-passing
  reports; the empty-DataFrame path (test_services mocks `result2table` → `pd.DataFrame()`)
  already exercises the `df.empty` branch (reports.py:316-319). ✓
- PERF-003 per-task logging: must not break `test_app.py` (CLI `--help`). ✓

**Backward compatibility:** All three fixes alter internal call routing / add observability —
no public API signature changes, no config-schema changes, no output-format changes.

## Evidence Index

| Evidence | Source | Verified |
|---|---|---|
| `call` wraps every invocation in Semaphore + initial jitter before it | network.py:59-124 (lines 66-74) | ✓ static read |
| `async with self._semaphore` wraps only `to_thread` | network.py:73-74 | ✓ static read |
| Backoff computed outside Semaphore; not held across backoff | network.py:71-124 | ✓ static read |
| `send_task` routes local `build_task` through `call` | reports.py:258-260 (was `:110,258-260` at code_ref) | ✓ static read |
| `extract_data` routes local `result2table` through `call` | reports.py:309-311 | ✓ static read |
| `csv_to_file` via bare `to_thread`, no Semaphore | reports.py:325 | ✓ static read |
| `DefaultDataStrategy.prepare` is `async def`, no `await` | reports.py:436-454 | ✓ static read |
| `build_task` (50-324) has no `send_request` | tasks.py:50-324; grep: next match 952 | ✓ static + grep |
| `result2table` (1426-1508) has no `send_request` | tasks.py:1426-1508; grep: 1424 in `get_result` | ✓ static + grep |
| `result2table` materializes full `resultBody` into dict-of-lists → DataFrame | tasks.py:1451-1496 | ✓ static read |
| Queue `maxsize` bounds count, not bytes | reports.py:115 | ✓ static read |
| `max_tasks=100` default; not passed by `services.py` | reports.py:92; services.py:138-147 | ✓ static read |
| No per-task clock calls in `worker`/`wait_task`/`send_task`/`extract_data` | reports.py:134-332 | ✓ static read |
| Only report-level timing in `run_report` | services.py:143-146,180-184 (was `:116-120,153-158` at code_ref) | ✓ static read |
| `logger.info("=")` is sole per-task signal (was `print("=", end="", flush=True)`) | reports.py:265 | ✓ static read |
| `perf_counter|timeit|elapsed|duration` grep in `core/` | 0 matches (not 1) | ✓ grep |
| Backoff formula recompute (sleep_time=40) | Python probe | ✓ matches Appendix B |
| Semaphore caps `call()` at 5 | asyncio.Semaphore(5), network.py:51,55 | ✓ structure (probe not re-run) |
| `result2table` materializes full payload | tasks.py:1466-1479,1496 | ✓ structure (probe not re-run) |
| Test suite: 12 passed, none exercise worker pipeline | pytest run + source (tests/) | ✓ 12 passed (18.85s) |
| `PERF-` prior usage in tree | repo-wide grep (excl. `.venv`/`.git`) | 0 prior (only instruction + findings) |
| `nat_tv_brands_last.yaml` config | yaml parse | 3 aud, monthly, ~15 mo, TV_CROSSTAB |
| Cross-ref IDs exist in siblings | grep across `.ai/audit/*/findings.md` | ✓ EXT-002/004/005/007, PIPE-004/006, ASY-004/006, ENT-005 all present |
| `git ls-files` path gate / code_ref | git rev-parse --short HEAD | d093d9c (matches front matter) |

## Disposition Tally

| Disposition | Count |
|---|---|
| confirmed | 3 |
| re-typed | 0 |
| re-graded | 0 |
| merged | 0 |
| not substantiated | 0 |
| unsettled | 0 |
| **Total** | **3** |

**Validation-level findings (VAL-): 4** (VAL-001, VAL-002, VAL-003, VAL-004 — all LOW)
