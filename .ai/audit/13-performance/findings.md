---
phase: "13"
phase_name: "performance"
id_prefix: "PERF"  # minted fresh — see Appendix C (collision check)
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/13-audit-performance.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Performance & Scalability

## Summary

Three findings (all MEDIUM). The highest severity present in this phase's own scope is
MEDIUM: the cost-model gaps that the prior phases' correctness/safety fixes did not
cover. The sharper performance perils — the unbounded polling loop (PIPE-006, HIGH),
the `sleep_time=40` 20–80 s amplification per poll (EXT-004, HIGH), the missing
request timeout on the `to_thread` bridge (EXT-002, HIGH), and the duplicate-task-retried
POST (EXT-001, CRITICAL) — were already filed under their owning phases (04, 09, 03) and
are cross-referenced here rather than re-filed (Phase 13 scope: "Semaphore acquisition
and cancellation semantics (03); the retry and backoff contract (09); the blocking-work
bridge (09)" are owned elsewhere). Phase 13 adds the *cost* lens they did not carry:
CPU/local work conflated with the I/O gate, absent data-volume backpressure, and a
per-task latency surface that does not exist.

## Scope

**Examined:** `core/network.py` (`NetworkClient.__init__`/`call` — Semaphore, backoff
formula, per-call initial jitter, `to_thread` bridge); `core/reports.py`
(`MediaReport.__init__`/`processing_tasks`/`worker`, `TVMediaReport.send_task`/
`wait_task`/`extract_data`/`prepare_data`, `DefaultTVTaskStrategy`,
`DefaultDataStrategy`, `DictDataStrategy`); `core/services.py`
(`AppService.run_report` — `NetworkClient()`/`TVMediaReport` construction and report-level
timing); vendored `mediascope_api/mediavortex/tasks.py` (`build_task`/`get_status`/
`get_result`/`result2table` — verified by reading the installed package source, git-ignored
under `.venv`); `settings/reports/*.yaml`, `settings/app_config.yaml`;
`tests/conftest.py` + `test_network.py`/`test_reports.py`/`test_services.py`/
`test_models.py`/`test_app.py`/`test_utils.py`.

**Tools:** Grep (content search), `uv run python` runtime probe, `uv run pytest`
(12 passed in 20.18s), AST/source inspection, `git ls-files` (path gate), backoff-formula
computation via `uv run python`.

**Not verified:** live Mediascope API (no credentials); full end-to-end report run
(`test_services.py` mocks `create_report` and patches `NetworkClient.call`; none of the
`send_task`/`wait_task`/`extract_data`/`worker` paths are exercised against a real
`MediaVortexTask`).

<!-- GATES — every cited src/ path appears in git ls-files; symbols verified by probe/AST. -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| PERF-001 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data` + `src/mko_get_mediascope_data/core/network.py::NetworkClient.call` | CPU/local DataFrame work is routed through the I/O Semaphore and the initial-jitter gate |
| PERF-002 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::MediaReport.__init__` + `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data` | No data-volume backpressure; oversized result loads full resultBody into memory |
| PERF-003 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.run_report` + `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.worker` | No per-task latency measurement or attribution; slow vs stuck task indistinguishable |

#### PERF-001 — [MEDIUM] CPU/local DataFrame work is routed through the I/O Semaphore and the initial-jitter gate

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data`
`src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
`src/mko_get_mediascope_data/core/reports.py::TVMediaReport.send_task`
**Disambiguator:** `async with self._semaphore`
**Checked:** ran + static
**Why this band:** MEDIUM — the cost is demonstrated on an always-on path (every task) but
its magnitude is conditional on DataFrame/result size and task volume; the design
conflates CPU/local cost with the network-I/O concurrency ceiling rather than the present
value being large. (Not HIGH: no single invocation here is unbounded in time — that is
PIPE-006/EXT-002's domain.)
**Problem:** `NetworkClient.call` wraps *every* invocation in `async with self._semaphore`
(network.py:73-74) and applies a per-call initial jitter
`await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))` (network.py:69) *before*
acquiring the semaphore. `TVMediaReport.extract_data` routes the CPU-bound `result2table`
through `call` (reports.py:309-311), and `send_task` routes the *local* `build_task`
through `call` (reports.py:258-260). `build_task` (vendored `mediascope_api/.../tasks.py:50-324`)
contains no `send_request` — it is local JSON construction — yet it still pays the jitter
and a Semaphore slot. Conversely `prepare_data` (`DefaultDataStrategy.prepare`,
reports.py:435-454) is declared `async def` with **no `await`** and runs synchronously in
the event loop, and `csv_to_file` is dispatched via bare `asyncio.to_thread` (reports.py:325)
*without* the Semaphore — so non-API CPU work is inconsistently either over-gated (semaphore)
or under-isolated (event loop).
**Repro:** Static + runtime probe. `send_request` is absent from `build_task` and
`result2table` (grep over `mediascope_api/mediavortex/tasks.py`: `send_request` matches at
952 [_send_task], 1124/1142/1174/1240 [status], 1424 [get_result] — none within `build_task`
50-324 or `result2table` 1426-1508). `build_task`/`result2table` are therefore local/CPU;
both enter `async with self._semaphore` via `call`. The probe in Appendix A confirms the
Semaphore caps real `call()` executions at 5.
**Impact:** The I/O concurrency ceiling (5) also serializes CPU and local work, so raising it
to improve API throughput raises CPU contention, and a large `result2table` holds an I/O slot
for the duration of the DataFrame build. Each task also pays the initial jitter on `build_task`
+ `send_task` + `get_result` + `result2table` (defaults `sleep_time=2` → 1-4 s each) before
any retry — pure added latency on a healthy path.
**Root cause:** A single `asyncio.Semaphore` and a single jitter policy gate the entire
`call()` surface with no distinction between I/O, CPU, and local work.
**Fix:** Route CPU/local work off the network gate: call `result2table`/`build_task` via a
dedicated CPU executor (or directly under `to_thread`) without acquiring the I/O Semaphore,
and remove the initial-jitter for non-API calls. Target symbols:
`NetworkClient.call`, `TVMediaReport.send_task`, `TVMediaReport.extract_data`.
**Verify:** new test needed: `test_cpu_local_paths_run_without_semaphore`
**Related:** PERF-002, EXT-002, ASY-004

#### PERF-002 — [MEDIUM] No data-volume backpressure; oversized result loads full resultBody into memory

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.__init__`
`src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data`
**Disambiguator:** `asyncio.Queue(maxsize=self.max_tasks)`
**Checked:** ran + static
**Why this band:** MEDIUM — absent size bound is a real design gap, but exhaustion/starvation
is conditional on result volume not present for small reports; the HIGH taxonomy example
("a single oversized result that can starve the worker pool or exhaust memory") is the
escalation once data volume is large (co-enabled by PERF-001).
**Problem:** `self.task_queue = asyncio.Queue(maxsize=self.max_tasks)` (reports.py:115)
bounds only the count of in-flight tasks (capped at `max_tasks=100`, reports.py:92, never
passed by `services.py` run_report), not the *byte size* of any result. `extract_data`
fetches the entire result via `get_result` and builds the entire DataFrame in
`result2table`, which the vendored client constructs by iterating the full
`data['resultBody']` list into a dict-of-lists then `pd.DataFrame(res)`
(`mediascope_api/.../tasks.py:1451-1496) — no streaming, no byte/size guard. Combined with
PERF-001, a large result's `result2table` holds a Semaphore slot while allocating the full
frame.
**Repro:** Static — `result2table` (tasks.py:1458-1496: `res={}`; `for item in
data['resultBody']: ... res[k].append(v) ...`; `df = pd.DataFrame(res)`) loads the whole
payload; `extract_data` (reports.py:295-332) applies no size cap; `Queue` maxsize (reports.py:115)
governs item count only. Runtime probe (Appendix E) confirms the vendored `result2table`
materializes the full `resultBody` into a row-for-row DataFrame with no streaming or size cap.
**Impact:** A single oversized result (driven by intervals × target audiences × regions for
TV_REG, see `nat_tv_brands_last.yaml`: 4 audiences × ~208 weekly slices ≈ 832 tasks) can
allocate large DataFrames across many in-flight workers with no backpressure on data volume,
risking memory exhaustion and pool starvation while `result2table` holds a Semaphore slot.
**Root cause:** Backpressure is expressed only as a task-count cap; result size is unbounded
and CPU-bound extraction is co-gated with I/O.
**Fix:** Add a result-size guard / streaming boundary in `extract_data` (cap
`result2table` input or stream slices), and bound in-flight data volume independently of
task count. Target symbol: `TVMediaReport.extract_data`.
**Verify:** new test needed: `test_large_result_is_rejected_or_streamed`
**Related:** PERF-001, EXT-005, PIPE-004

#### PERF-003 — [MEDIUM] No per-task latency measurement or attribution; slow vs stuck task indistinguishable

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
`src/mko_get_mediascope_data/core/reports.py::TVMediaReport.worker`
**Disambiguator:** `start_time = datetime.now()` (report-level only)
**Checked:** ran (grep) + static
**Why this band:** MEDIUM — a latency measurement (report-level total) is asserted/emitted,
but the per-task breakdown is not; this is "a latency measurement that is asserted but not
emitted" at the task granularity.
**Problem:** `run_report` logs only a report-level start/stop via `print`
(services.py:116-120, 153-158) — `datetime.now()` start/end and a total — with no
per-task wall time and no attribution (API wait vs CPU vs disk). `worker` (reports.py:134-153)
and `wait_task` (reports.py:271-293) record no elapsed time. `grep` for `perf_counter`/
`timeit`/`elapsed`/`duration` across `core/` returns only the unrelated `time_unit` field
(models.py:64). The lone per-task signal is a bare `print("=", end="", flush=True)`
(reports.py:265) — progress with no timing context. A slow task is therefore
indistinguishable from a stuck one without the timeout/deadline fixes owned by Phase 04
(PIPE-006) / Phase 09 (EXT-002), neither of which emits timing.
**Repro:** Grep (`perf_counter|timeit|elapsed|duration|\.seconds`) in `core/` → 1 match,
`time_unit` (models unrelated). `worker`/`wait_task` contain no clock calls; the only
timing primitive in the run path is the report-level `datetime.now()` pair in
`run_report`.
**Impact:** Operators cannot identify the worst-case task, cannot attribute latency to API
wait vs CPU vs disk, and cannot tell a slow dependency from a working one — so throttling
or a stuck task (PIPE-006/EXT-002) is only diagnosable by tailing logs, not by metrics.
**Root cause:** No timing instrumentation in the task pipeline; observability is report-scoped
`print`, not per-task or structured.
**Fix:** Time each stage of the per-task pipeline (`send_task`→`wait_task`→`extract_data`)
and log structured records (task name, interval, target, stage durations, total) at INFO;
emit a per-task total. Target symbol: `MediaReport.worker` / `TVMediaReport.send_task`/
`wait_task`/`extract_data`.
**Verify:** new test needed: `test_worker_emits_per_task_duration`
**Related:** PERF-001, EXT-007, ASY-006, ENT-005

---

## Fix Order

- **Dependencies:** PERF-001 and PERF-002 share `TVMediaReport.extract_data`/`result2table`
  — co-locate the extraction-isolation fix and the size-guard fix in one change. All three
  findings are independent of one another.
- **Merge candidates:** PERF-001 + PERF-002 (both rework `extract_data` routing / result
  handling).
- **Conflicting evidence:** None.
- **Cross-phase conflicts (owned elsewhere, not re-filed — referenced as context/escalation):**
  - The unbounded poll loop (PIPE-006 HIGH / ASY-005) and the `sleep_time=40` 20–80 s
    amplification per poll (EXT-004 HIGH) live in `wait_task` — this phase rates them as
    already-covered *escalation triggers* for PERF-003's "stuck vs slow" problem, not as new
    defects.
  - The pool↔Semaphore sizing question (ASY-004 — since re-characterized to not
    substantiated / LOW: the two levels are an intentional, independent design), the
    synchronous-catalog-bypass issue (ASY-002), and the missing `to_thread` deadline
    (EXT-002 HIGH) are concurrency-safety concerns; Phase 13 treats the *cost*
    (PERF-001 gate + PERF-003 attribution gap) only.
  - Retry metrics gap (EXT-007 MEDIUM / Phase 12 observability stack) — PERF-003 is the
    per-task *latency* attribution surface, a distinct axis.
- **Rollout risk:** None of the three MEDIUM fixes alter happy-path data output; PERF-001's
  off-gating of `result2table`/`build_task` must keep `test_services.py` green (it mocks
  `create_report`, so the worker loop is not exercised there). PERF-003's per-task logging
  must not change `test_app.py` (CLI `--help` only).

## Appendices

### Appendix A — Runtime probe: Semaphore caps real `call()` executions at 5

`uv run python` probe (`NetworkClient(max_concurrent_requests=5, sleep_time=0, max_attempts=1)`,
40 concurrent `call()` invocations of a thread-blocking function):

```text
peak concurrent call() executions (expect 5): 5
```

*(supports: PERF-001 — every call, including non-API `result2table`/`build_task`, competes for
the same 5-slot gate)*

### Appendix B — Backoff / initial-jitter magnitudes (formula computation, not a live wait)

`uv run python` over `NetworkClient` defaults (`sleep_time=2`, `max_attempts=10`) and the
`get_status` call site (`sleep_time=40`):

```text
initial delay range (sleep_time=2, non-status calls): 1..4 s  (uniform(1,4))
initial delay range (sleep_time=40, get_status polls): 20..80 s  (uniform(20,80))
backoff per attempt, sleep_time=40: [40, 48, 57.6, 69.1, 82.9, 99.5, 119.4, 143.3, 172.0, 206.4]
backoff tail sum (no jitter): 1038 s ; worst-case single get_status call ≈ 1168 s (~19.5 min)
```

The backoff IS bounded by `max_attempts=10` (the `for` loop at network.py:71 is `range`-bounded);
the unbounded cost is the *poll iteration count* (PIPE-006), owned by Phase 04. *(supports:
PERF-003 attribution gap + cross-ref EXT-004/PIPE-006)*

Block 3 (retry backoff cost) — examined. The backoff formula is
`backoff = _sleep_time * 1.2**(attempt-1)` + `uniform(0,5)` jitter (network.py:120-124); the
initial jitter `uniform(_sleep_time*0.5, _sleep_time*2)` (network.py:69) is paid **once per
`call()`** (above the loop), not per attempt. A retried call does **not** hold the Semaphore
across the backoff: the `async with self._semaphore` (network.py:73-74) wraps only
`to_thread(...)`; an exception exits the block (releasing the permit) before the `except` at
network.py:95 catches it and the backoff at network.py:122 sleeps — so the "retry holds the
Semaphore across backoff" HIGH scenario does **not** apply here (the HIGH scenario that does
apply is CPU `result2table` holding the slot — PERF-001).

### Appendix C — PERF- ID collision check (per phase instruction)

`grep -rF "PERF-"` across the whole tree (src/, tests/, .ai/audit/, workflows, .kilo/) returns
**only** the Phase 13 phase file itself (the literal string in the instruction text). No
shipped source, test, workflow, or prior-cycle report carries a `PERF-NNN` identifier. The
namespace is empty in the current tree → **no collision**, so `PERF-001..003` are minted
fresh under the existing `PERF-` namespace (no second namespace introduced).

*(supports: "check for a collision before minting an ID and report it rather than creating a
second namespace")*

### Appendix D — Per-task API call cost model (call count)

Per task, the sequence in `TVMediaReport` is: `send_task` → `build_task` + `send_task`
(reports.py:258-263); `wait_task` → N × `get_status` (reports.py:280-282, N unbounded per
PIPE-006); `extract_data` → `get_result` + `result2table` (reports.py:301-311) → `prepare_data`
+ `csv_to_file` (reports.py:320-332). Minimum = 4 gated calls + N polls, where N scales with
the task's remote processing time and the total task count scales with intervals × target
audiences (× regions for TV_REG). With 5 Semaphore slots, throughput is I/O-gated at 5 until
CPU/local work (PERF-001) or a stall (EXT-002) blocks a slot.

### Appendix E — Runtime probe: `result2table` fully materializes the payload with no size cap

`uv run python` probe that bypasses `MediaVortexTask.__init__` (no credentials/catalogs) and
neutralizes the catalog-dependent `_get_text_names`, then calls the vendored
`result2table` with a 10,000-row `resultBody`:

```text
resultBody rows ingested: 10000
result2table DataFrame rows: 10000
rows fully materialized (no streaming): True
any size cap/threshold in result2table source: False
```

*(supports: PERF-002 — the entire result set is loaded into memory as a dict-of-lists and a
DataFrame; no cap guards an oversized result)*
