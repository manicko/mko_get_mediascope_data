---
phase: "03"
phase_name: "async-concurrency"
id_prefix: "ASY"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/03-audit-async-concurrency.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Async Concurrency & Worker Safety

## Summary

Seven findings (1 CRITICAL, 1 HIGH, 4 MEDIUM, 1 LOW). All seven anchors resolve to the
cited locations in `core/reports.py` or `core/network.py`, all seven disambiguator tokens
exist inside their primary anchors, and all seven claims reproduce under static
control-flow analysis. Two findings (ASY-001, ASY-005) are independently confirmed by
Phase 04 runtime probes (REPRO D and REPRO E respectively).

**7 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 merged, 0 unsettled.**

No finding was downgraded, re-typed, or withdrawn. The sole imprecisions — ASY-001's
repro cites line 269 as a raise site (it is an assignment) — do not affect any finding's
verdict. One cross-phase gap is recorded as VAL-001.

## Scope

**Examined:** `core/reports.py` (`MediaReport.__init__`, `MediaReport.worker`,
`MediaReport.processing_tasks`, `TVMediaReport.send_task`, `TVMediaReport.wait_task`,
`TVMediaReport.extract_data`, `TVRegTaskStrategy.generate`, `DefaultTVTaskStrategy.generate`),
`core/network.py` (`NetworkClient.__init__`, `NetworkClient.call`), `core/services.py`
(`AppService.run_report` instantiation site), `core/errors.py`, `core/tasks.py` (partial),
`tests/test_network.py`, `tests/test_reports.py`, `tests/test_services.py`,
`tests/conftest.py`, `app.py`, `.kilo/commands/audit/phases/03-audit-async-concurrency.md`,
`.kilo/commands/audit/templates/audit-findings.md`, `.ai/audit/04-data-pipeline/findings.md`,
`.ai/audit/99-validation/04-data-pipeline-validated-findings.md`,
`.ai/audit/99-validation/01-entry-architecture-validated-findings.md`,
`.ai/audit/99-validation/02-config-secrets-validated-findings.md`,
`docs/00-overview/doc-maintenance-rules.md`.

**Runtime probes performed:** None in this phase (Phase 03 instructions note no Docker
composition exists for this project). Static control-flow analysis used instead.

**Runtime probes relied upon (cross-phase confirmation):**
- REPRO D (Phase 04): `queue.join()` HUNG >2s after worker raised before `task_done()` — confirms ASY-001.
- REPRO E (Phase 04): `wait_task` source contains `while True`; contains no `max_attempts`, `deadline`, or `timeout` — confirms ASY-005.

**Not verified:** Runtime behaviour under load or cancellation — no Docker services to start;
tests mock `create_report` entirely and provide no coverage of the worker pool lifecycle.

---

## Findings

| ID | Disposition | Severity | Anchor resolves | Disambiguator exists |
|---|---|---|---|---|
| ASY-001 | confirmed | CRITICAL | reports.py:141–153 (worker) | `task_done` at line 153 ✓ |
| ASY-002 | confirmed | HIGH | reports.py:387 (TVRegTaskStrategy.generate) | `get_tv_region` at line 387 ✓ |
| ASY-003 | confirmed | MEDIUM | reports.py:162 (processing_tasks) | `t.cancel` at line 162 ✓ |
| ASY-004 | confirmed | MEDIUM | reports.py:92, reports.py:115, reports.py:159, network.py:51, services.py:130 | `max_tasks` / `max_concurrent_requests` ✓ |
| ASY-005 | confirmed | MEDIUM | reports.py:277 (wait_task) | `while True` at line 277 ✓ |
| ASY-006 | confirmed | MEDIUM | reports.py:115, reports.py:160 (processing_tasks) | — (no qsize logging, confirmed by absence) |
| ASY-007 | confirmed | LOW | network.py:126, errors.py:1-6, tests/test_network.py:15 | `RuntimeError` at line 126 ✓ |

---

### ASY-001 — confirmed — [CRITICAL] Worker exception skips `task_done()`, `queue.join()` deadlocks forever

**Anchor resolved:** `core/reports.py::MediaReport.worker` — method at line 134. `worker` body
(lines 141–153):

```python
async def worker(self):
    while True:
        item = await self.task_queue.get()          # 142
        await self.send_task(item)                   # 143 — raises on API error
        if item.status is True:
            await self.wait_task(item)               # 145 — raises on API error
        if item.status is True:
            await self.extract_data(item)            # 147 — raises on API error
        ...
        self.task_queue.task_done()                  # 153 — only reached on happy path
```

**Claim tested:** `worker` has no `try/except/finally` around any `await` call. `send_task`
(line 143), `wait_task` (line 145), and `extract_data` (line 147) all call
`self.network_client.call(...)`, which raises `RuntimeError`, `BadRequestError`,
`AuthorizationError`, or `FileNotFoundError` (network.py:76–118). Any of these propagates
out of `worker` before line 153, so `task_done()` is never called for that item.
`processing_tasks` then blocks forever at `await self.task_queue.join()` (line 161).

**Disambiguator:** `task_done` — present at line 153. ✓

**Evidence (source inspection):**
```text
reports.py:141:         while True:
reports.py:143:             await self.send_task(item)
reports.py:145:             await self.wait_task(item)
reports.py:147:             await self.extract_data(item)
reports.py:153:             self.task_queue.task_done()
reports.py:159:         task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]
reports.py:161:         await self.task_queue.join()
reports.py:162:         [t.cancel() for t in task_group]

network.py:76:   except BadRequestError as err:  raise
network.py:80:   except AuthorizationError as err:  raise
network.py:88:   except FileNotFoundError as err:  raise
network.py:113:  except Exception as err:  raise
network.py:126:  raise RuntimeError("Network retry limit exceeded")
```

**Rubric grading:** Phase 03 CRITICAL = "a queue `task_done` that is never called so `join()`
deadlocks forever, hanging every invocation." The worker has no `try/finally` to guarantee
`task_done()`. CRITICAL matches. No re-grade.

**Cross-phase confirmation:** Phase 04 PIPE-002 (same `MediaReport.worker` defect) was
independently confirmed via runtime probe. Phase 04 REPRO D:
`queue.join() HUNG >2s (task_done never called -> worker raised before line 153)`.
Phase 04 grades PIPE-002 as HIGH (its own rubric's HIGH band covers "worker death before
task_done"), but Phase 03's CRITICAL band explicitly covers "hangs every invocation" —
ASY-001 hangs on **every** error path, which is stronger. Each phase's grading is correct
under its own rubric. No conflict. (See VAL-001 for the citation gap.)

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.worker` and
`core/reports.py::MediaReport.processing_tasks`).

**Recommendation carry-out:** "Wrap the worker body in `try/finally` so `self.task_queue.task_done()`
is always called." Target `MediaReport.worker` exists and is stable. ✓
The fix resolves the root cause: `task_done()` in `finally` guarantees the queue item is
acked regardless of exception.

---

### ASY-002 — confirmed — [HIGH] Synchronous `get_tv_region()` call bypasses Semaphore, retry, and `to_thread`

**Anchor resolved:** `core/reports.py::TVRegTaskStrategy.generate` — method at line 385.
Line 387:

```python
async def generate(self, report: TVMediaReport):
    settings = report.data_settings.model_dump()
    available_regions = report.catalogs.get_tv_region()   # 387 — synchronous, bypasses NetworkClient
```

**Claim tested:** `TVRegTaskStrategy.generate` calls `report.catalogs.get_tv_region()`
directly — a synchronous API call that bypasses `NetworkClient.call` entirely. No
`Semaphore` permit, no retry/backoff, no `asyncio.to_thread` bridge. The event loop is
blocked for the full duration of the API call.

**Disambiguator:** `get_tv_region` — present at line 387. ✓

**Evidence (source inspection — all API calls in TVMediaReport):**
```text
reports.py:197:  ... (see context — routes through network_client.call)
reports.py:258: task_json = await self.network_client.call(self.builder, ...)
reports.py:263: task.key = await self.network_client.call(self.sender, task_json)
reports.py:280: status = await self.network_client.call(self.report_service.get_status, ...)
reports.py:301: res_json = await self.network_client.call(self.report_service.get_result, ...)
reports.py:309: df = await self.network_client.call(self.report_service.result2table, ...)
reports.py:387: available_regions = report.catalogs.get_tv_region()   # ONLY call not via network_client
```

**Rubric grading:** Phase 03 HIGH = "exceeding the concurrency ceiling." The synchronous
call holds no `Semaphore` permit — it is unbounded relative to the 5-permit ceiling.
HIGH matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVRegTaskStrategy.generate`).

**Recommendation carry-out:** "Route `get_tv_region()` through
`self.network_client.call(self.catalogs.get_tv_region)`." Target
`TVRegTaskStrategy.generate` exists and is stable. ✓

---

### ASY-003 — confirmed — [MEDIUM] Workers cancelled with bare `cancel()`, never awaited or gathered

**Anchor resolved:** `core/reports.py::MediaReport.processing_tasks` — method at line 155.
Line 162:

```python
[await self.task_queue.put(t) async for t in self.generate_tasks()]   # 160
await self.task_queue.join()                                           # 161
[t.cancel() for t in task_group]                                       # 162 — fire-and-forget
```

**Claim tested:** `processing_tasks` creates 100 worker tasks at line 159 and cancels them
with `[t.cancel() for t in task_group]` at line 162 — a list comprehension that discards
return values and never `await`s. (a) Cancellation is not confirmed before return.
(b) If `generate_tasks()` at line 160 raises mid-stream, line 162 is never reached and
all workers remain as dangling pending tasks.

**Disambiguator:** `t.cancel` — present at line 162. ✓

**Evidence (source inspection):**
```text
reports.py:159: task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]
reports.py:160: [await self.task_queue.put(t) async for t in self.generate_tasks()]
reports.py:161: await self.task_queue.join()
reports.py:162: [t.cancel() for t in task_group]
```

**Rubric grading:** Phase 03 MEDIUM = "a cancellation path that is not awaited, risking
partial state across Python versions." The cancel is fire-and-forget. MEDIUM matches.
No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.processing_tasks`).

**Recommendation carry-out:** "Replace line 162 with explicit cancellation and
`await asyncio.gather(*task_group, return_exceptions=True)`." Target
`MediaReport.processing_tasks` exists and is stable. ✓

---

### ASY-004 — confirmed — [MEDIUM] Pool size (100) and Semaphore permits (5) are decoupled

**Anchor resolved:** `core/reports.py::MediaReport.__init__` (line 92),
`core/network.py::NetworkClient.__init__` (line 51),
`core/services.py` instantiation site (line 130).

**Claim tested:** `max_tasks=100` (reports.py:92) and `max_concurrent_requests=5`
(network.py:51) are defined independently with no relationship enforced.
`services.py:130` instantiates `NetworkClient()` with defaults — no override propagated.
At any moment, up to 100 workers can exist but only 5 can hold a Semaphore permit inside
`NetworkClient.call` (network.py:73); the remaining 95 are suspended at
`async with self._semaphore`.

**Disambiguator:** `max_tasks` — present at reports.py:92. ✓

**Evidence (source inspection):**
```text
reports.py:92:  max_tasks: int = 100,
reports.py:114: self.max_tasks = max_tasks
reports.py:115: self.task_queue = asyncio.Queue(maxsize=self.max_tasks)
reports.py:159: task_group = [asyncio.create_task(self.worker()) for _ in range(self.max_tasks)]

services.py:130: network_client: NetworkClient = NetworkClient()

network.py:49: def __init__(
network.py:51:     max_concurrent_requests: int = 5,
network.py:55: self._semaphore = asyncio.Semaphore(max_concurrent_requests)
network.py:73: async with self._semaphore:
```

**Rubric grading:** Phase 03 MEDIUM = "architectural drift that obscures the effective
concurrency model." The pool and Semaphore are decoupled. MEDIUM matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.__init__` parameter `max_tasks`
and `core/network.py::NetworkClient.__init__` parameter `max_concurrent_requests`).

**Recommendation carry-out:** "Derive the worker pool size from the `NetworkClient` Semaphore
limit." Target `MediaReport.__init__` parameter `max_tasks` exists and is stable. ✓

---

### ASY-005 — confirmed — [MEDIUM] `while True` poll loop has no timeout

**Anchor resolved:** `core/reports.py::TVMediaReport.wait_task` — method at line 271.
`while True:` at line 277.

**Claim tested:** `wait_task` (lines 271–293) contains `while True:` (line 277) with no
`asyncio.wait_for`, no deadline, and no maximum iteration count. The loop breaks only when
`status.get("taskStatus")` is in `("DONE", "FAILED", "CANCELLED")` (line 292). If the API
returns a non-terminal status (e.g. `"RUNNING"`) indefinitely, the worker never returns,
never calls `task_done()`, and `queue.join()` deadlocks.

**Disambiguator:** `while True` — present at line 277. ✓

**Evidence (source inspection):**
```text
reports.py:277:          while True:
reports.py:278:              await asyncio.sleep(2)
reports.py:280:              status = await self.network_client.call(
reports.py:292:              if status.get("taskStatus", None) in ("DONE", "FAILED", "CANCELLED"):
reports.py:293:                  break
```

**Cross-phase confirmation:** Phase 04 PIPE-006 (same `wait_task` defect) was confirmed
via Phase 04 REPRO E:
`wait_task source contains 'while True': True`
`wait_task source contains 'max_attempts' or 'deadline' or 'timeout': False`

Phase 04 grades PIPE-006 as HIGH (its HIGH band covers "unbounded polling"); Phase 03
grades ASY-005 as MEDIUM because the Phase 03 MEDIUM band covers "robustness gaps that
can escalate under adverse conditions." The difference: Phase 03 CRITICAL is reserved for
defects that "hang every invocation" — ASY-005 only hangs when the API returns a
non-terminal status indefinitely (an adverse condition). Each phase's grading is correct
under its own rubric. No conflict.

**Rubric grading:** Phase 03 MEDIUM. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::TVMediaReport.wait_task`).

**Recommendation carry-out:** "Wrap the poll loop in `asyncio.wait_for` with a configurable
timeout, or add a maximum iteration count." Target `TVMediaReport.wait_task` exists and
is stable. ✓

---

### ASY-006 — confirmed — [MEDIUM] Queue depth is non-observable

**Anchor resolved:** `core/reports.py::MediaReport.processing_tasks` — method at line 155.
`Queue(maxsize=self.max_tasks)` at line 115. Producer loop at line 160.

**Claim tested:** The `Queue(maxsize=100)` at reports.py:115 is bounded, but no code path
logs `qsize()`, pending count, or producer-stall duration. When backpressure stalls the
producer (line 160 blocks on a full queue), there is no diagnostic signal.

**Disambiguator:** `task_queue.put` — present at line 160. ✓ (The finding is about absence
of observability, confirmed by absence of `qsize()` or depth logging.)

**Evidence (source inspection — no qsize or depth logging):**
```text
reports.py:115: self.task_queue = asyncio.Queue(maxsize=self.max_tasks)
reports.py:160: [await self.task_queue.put(t) async for t in self.generate_tasks()]
reports.py:161: await self.task_queue.join()
```

Grep for `qsize` in `reports.py`: 0 matches. No `logger.debug` or `logger.info` in
`processing_tasks` or `worker` references queue depth.

**Rubric grading:** Phase 03 MEDIUM = "a queue depth that is bounded or non-observable."
The queue is bounded but non-observable. MEDIUM matches. No re-grade.

**Load-bearing artefact:** Code (`core/reports.py::MediaReport.processing_tasks` and
`core/reports.py::MediaReport.worker`).

**Recommendation carry-out:** "Add `logger.debug` calls in `processing_tasks` and
`worker` to log `self.task_queue.qsize()` when the queue crosses a threshold or the
producer stalls." Targets exist and are stable. ✓

---

### ASY-007 — confirmed — [LOW] Exhausted-retry raises generic `RuntimeError`

**Anchor resolved:** `core/network.py::NetworkClient.call` — exception raised at line 126.
`core/errors.py` — only `NoReportFoundError` and `WrongDataSettings` (no network exception).
`tests/test_network.py:15` — asserts `RuntimeError`.

**Claim tested:** After `max_attempts` retries, `NetworkClient.call` raises
`RuntimeError("Network retry limit exceeded")` (line 126) — a generic Python builtin, not
a custom exception. `core/errors.py` defines only `NoReportFoundError(Exception)` and
`WrongDataSettings(Exception)`. The test at `tests/test_network.py:15` asserts
`pytest.raises(RuntimeError, match="Network retry limit exceeded")`.

**Disambiguator:** `RuntimeError` — present at network.py:126. ✓

**Evidence (source inspection):**
```text
network.py:126: raise RuntimeError("Network retry limit exceeded")

errors.py:1: class NoReportFoundError(Exception): pass
errors.py:5: class WrongDataSettings(Exception): pass
(no network exception class in errors.py)

tests/test_network.py:15: with pytest.raises(RuntimeError, match="Network retry limit exceeded"):
```

**Rubric grading:** Phase 03 LOW = "observability and error-classification drift with no
runtime consequence today." The exception is raised with a descriptive message; callers
just cannot catch it specifically. LOW matches. No re-grade.

**Load-bearing artefact:** Code (`core/network.py::NetworkClient.call` line 126 and
`core/errors.py`).

**Recommendation carry-out:** "Define `MaxRetriesExceeded(RuntimeError)` in
`core/errors.py` and raise it at line 126." Target exists and is stable. The proposed
subclass of `RuntimeError` preserves backward compatibility with `test_network.py:15`
(which asserts `RuntimeError` — a subclass passes `isinstance` checks). ✓

---

## Validation-Level Findings (VAL-)

| ID      | Severity | Disposition | Description |
|---------|----------|-------------|-------------|
| VAL-001 | LOW      | confirmed   | Phase 03 findings omit cross-reference to Phase 04 PIPE-002 and PIPE-006 (same defects independently discovered) |
| VAL-002 | LOW      | confirmed   | ASY-001 repro cites line 269 as a raise site; line 269 is an assignment (`task.error = ...`), not a raise |
| VAL-003 | LOW      | confirmed   | Phase 03 Fix Order references "phase 10" for print() issues; Phase 04 cross-phase analysis already covers print() ownership via ENT-005/CFG-010 |

### VAL-001 — [LOW] Phase 03 findings omit cross-reference to Phase 04 PIPE-002 and PIPE-006

**Template mandate:** `.kilo/commands/audit/phases/99-audit-validate.md` block 6 —
"Compare against whatever sibling reports exist."

**Observed:** ASY-001 (`MediaReport.worker` — no try/except, `task_done` skipped) and
PIPE-002 (`MediaReport.worker` — no try/except, `task_done` skipped) describe the
**same defect** at the **same code location** (reports.py:141-153). Similarly, ASY-005
(`wait_task` — `while True` no timeout) and PIPE-006 (`wait_task` — `while True` no
timeout) describe the same defect at the same location (reports.py:277). Phase 03's
findings.md cross-references only ASY-003 and ASY-005 (internal), not PIPE-002 or PIPE-006.
Phase 04's findings.md likewise does not reference ASY-001 or ASY-005.

**Impact:** No correctness impact — both phases independently and correctly discover
the same defects. Phase 04 provides runtime confirmation (REPRO D, REPRO E) that Phase 03
could not obtain (no Docker). The omission is a documentation gap, not a validation defect.

**Cause:** The two phases were authored independently without cross-citation.

**Disposition:** confirmed. Does not affect any finding's verdict.

### VAL-002 — [LOW] ASY-001 repro cites line 269 as a raise site

**Template mandate:** `.kilo/commands/audit/phases/99-audit-validate.md` block 1 —
"Do not treat the quoted snippet as the claim; test the claim on its own terms."

**Observed:** ASY-001's "Repro" section states: "send_task raises at line 263 or 269."
- Line 263: `task.key = await self.network_client.call(self.sender, task_json)` — valid
  raise site (`network_client.call` re-raises). ✓
- Line 269: `task.error = "Task.key is not received from API."` — this is an **assignment**
  (setting `task.status = False` at line 267, then `task.error = ...` at line 269),
  not a `raise` statement. ✗

The actual raise paths in `send_task` (lines 252–269) are `network_client.call` at
line 258 and line 263. The claim is still valid (any `network_client.call` raise skips
`task_done()`) — the cited line is simply misidentified.

**Impact:** No effect on the finding's verdict. The root cause (no `try/finally`
around `task_done`) is unchanged.

**Disposition:** confirmed.

### VAL-003 — [LOW] Phase 03 Fix Order references "phase 10" imprecisely

**Template mandate:** `.kilo/commands/audit/phases/99-audit-validate.md` block 6 —
"Compare against whatever sibling reports exist."

**Observed:** Phase 03's "Fix Order" → "Cross-phase conflicts" states: "The `print("=")`
progress statement at `reports.py:265` and the `print()` calls in `services.py:117-158` are
code-quality issues owned by phase 10 — not filed here."

Phase 04's validated report (line 477) cross-references `print()` as:
"ENT-005 (MEDIUM) | CFG-010 (LOW) | — (acknowledged, not re-claimed) | `print()` in
`core/services.py` (117, 124, 154) and `core/reports.py` (265)."
Phase 01's validated report confirms ENT-005; Phase 02's confirms CFG-010. The
"phase 10" reference is imprecise — these are owned by Phase 01 (ENT-005) and Phase 02
(CFG-010), which have already been validated.

**Impact:** No effect on Phase 03 findings. The exclusion of `print()` from Phase 03
scope is correct.

**Disposition:** confirmed.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 01 finding | Phase 02 finding | Phase 03 finding | Phase 04 finding | Shared root cause | Disposition |
|---|---|---|---|---|---|---|
| worker exception / task_done | — | — | ASY-001 (CRITICAL) | PIPE-002 (HIGH) | `MediaReport.worker` no try/except → `task_done` skipped → `queue.join()` deadlock | cross-reference; both confirmed; grade divergence justified (see below) |
| wait_task unbounded poll | — | — | ASY-005 (MEDIUM) | PIPE-006 (HIGH) | `TVMediaReport.wait_task` `while True` no timeout/deadline/iteration cap | cross-reference; both confirmed; grade divergence justified (see below) |
| pool Semaphore decoupling | — | — | ASY-004 (MEDIUM) | — | `max_tasks=100` vs `max_concurrent_requests=5`, no propagation | Phase 03 only; not claimed by Phase 04 |
| `RuntimeError` exhaustion | — | — | ASY-007 (LOW) | — | `NetworkClient.call` raises generic `RuntimeError` at line 126 | Phase 03 only; not claimed by Phase 04 |
| print() in core/ | ENT-005 (MEDIUM) | CFG-010 (LOW) | — (acknowledged, not claimed) | — (acknowledged, not re-claimed) | `print()` in `core/services.py` and `core/reports.py:265` | cross-reference; all confirmed; Phase 03 correctly excludes |

**Severity divergence rationale (ASY-001 / PIPE-002 and ASY-005 / PIPE-006):**

- **ASY-001 (CRITICAL) vs PIPE-002 (HIGH):** Phase 03 CRITICAL = "hangs every invocation"
  (any API error in the worker kills it). Phase 04 HIGH = "worker death before task_done."
  The same defect receives different grades because each phase's rubric anchors to a
  different blast-radius assessment. Phase 03 treats the per-invocation hang as CRITICAL;
  Phase 03's MEDIUM does not apply because the mechanism (unbounded hang) is the worst
  case in Phase 03's taxonomy. Both are correct under their respective rubrics. No conflict.

- **ASY-005 (MEDIUM) vs PIPE-006 (HIGH):** Phase 03 MEDIUM = "robustness gaps that can
  escalate under adverse conditions." Phase 04 HIGH = "unbounded polling with no
  deadline." Phase 03 reserves CRITICAL for "hangs every invocation" — ASY-005 only hangs
  when the Mediascope API returns a non-terminal status indefinitely (an adverse condition,
  not the normal path). Phase 04's HIGH band does not distinguish normal-vs-adverse.
  Both are correct under their respective rubrics. No conflict.

**Ownership check — Phase 03 domain:** Phase 03 owns async concurrency, worker lifecycle,
Semaphore/pool interaction, backpressure, retry exhaustion, and cancellation discipline.
All seven ASY findings fall squarely within this domain. No finding was filed under a
phase that does not own it.

**No cross-phase conflicts detected.** No finding was withdrawn, downgraded, or re-graded
due to cross-phase interaction.

---

## Rollout Safety Assessment

**Dependencies identified (validated from source):**
- ASY-001 fix (`try/finally: task_done()` in `worker`) is the foundation for ASY-003
  (cancellation cleanup) and ASY-007 (exception classification) — all touch the same
  `worker`/`processing_tasks`/`call` code paths. ✓
- ASY-002 and ASY-004 are independent of the worker-lifecycle fixes (ASY-002 is in
  `TVRegTaskStrategy.generate`; ASY-004 is a configuration-alignment concern). ✓

**Fix ordering:**
1. ASY-001 first — `try/finally` in `worker` guarantees `task_done()` regardless of
   exception. This is load-bearing for all other worker-lifecycle fixes.
2. ASY-003 alongside ASY-001 — `processing_tasks` cleanup (`gather(return_exceptions=True)`
   + `try/finally`) builds on the same method.
3. ASY-005 alongside ASY-001 — `wait_task` timeout must use `finally`-protected
   `task_done` to avoid masking the original exception.
4. ASY-007 alongside ASY-001 — exception type change only affects what callers catch;
   subclassing `RuntimeError` keeps `test_network.py:15` green.
5. ASY-002, ASY-004, ASY-006 independent — can proceed in parallel.

**Rollout risk:**
- Fixing ASY-001 changes hang-on-error to continue-on-error behaviour. Existing report
  runs that currently hang on an API error will now complete (with some tasks skipped).
  `tests/test_services.py::test_run_report_with_real_models` mocks `create_report`
  entirely (conftest.py line 48:
  `mocker.patch("...TVMediaReport.create_report", new_callable=AsyncMock)`) —
  not affected. No shipped test asserts the hang.
- Fixing ASY-002 (routing `get_tv_region` through `network_client.call`) introduces
  retry/backoff on a previously-unprotected call — may slow TV_REG report startup under
  transient failures but will no longer abort the entire report. No test asserts the
  synchronous bypass.
- Fixing ASY-007 (subclassing `RuntimeError`) is backward-compatible with
  `test_network.py:15`. ✓

**Backward compatibility:** All recommendations preserve existing test contracts. The
`MaxRetriesExceeded(RuntimeError)` subclass proposal (ASY-007) explicitly maintains
backward compatibility with `test_network.py:15`.

---

## Evidence Index

| Evidence reference | Source |
|---|---|
| `reports.py:134-153` | `MediaReport.worker` — no try/except, `task_done` at line 153 |
| `reports.py:155-162` | `MediaReport.processing_tasks` — `create_task`, `queue.join()`, bare `cancel()` |
| `reports.py:252-269` | `TVMediaReport.send_task` — routes through `network_client.call` |
| `reports.py:271-293` | `TVMediaReport.wait_task` — `while True` at line 277, no timeout |
| `reports.py:385-387` | `TVRegTaskStrategy.generate` — synchronous `get_tv_region()` |
| `reports.py:92,114-115,159` | `MediaReport.__init__` / pool / queue sizing |
| `network.py:49-58` | `NetworkClient.__init__` — `max_concurrent_requests=5`, `Semaphore` |
| `network.py:59-126` | `NetworkClient.call` — retry, backoff, `raise RuntimeError` at line 126 |
| `errors.py:1-6` | Only `NoReportFoundError`, `WrongDataSettings` — no network exception |
| `services.py:130` | `NetworkClient()` instantiated with defaults, no override |
| `tests/test_network.py:15` | Asserts `RuntimeError("Network retry limit exceeded")` |
| `tests/test_services.py:48` | `create_report` mocked entirely — no worker lifecycle coverage |
| `03-audit-async-concurrency.md` | Phase 03 severity taxonomy, audit blocks, ID prefix |
| `99-audit-validate.md` | Validation methodology (blocks 1–10) |
| `04-data-pipeline-validated-findings.md` | Cross-phase: REPRO D (ASY-001 confirmation), REPRO E (ASY-005 confirmation) |
| `01-entry-architecture-validated-findings.md` | Cross-phase: ENT-005 (print() in core/) |
| `02-config-secrets-validated-findings.md` | Cross-phase: CFG-010 (print() in core/) |
| `docs/00-overview/doc-maintenance-rules.md` | Document maintenance rules |

---

## Disposition Tally

| Disposition | Count |
|---|---|
| confirmed | 7 |
| re-typed | 0 |
| re-graded | 0 |
| merged | 0 |
| not substantiated | 0 |
| unsettled | 0 |
| **Total** | **7** |

**Validation-level findings (VAL-): 3** (VAL-001, VAL-002, VAL-003 — all LOW)
