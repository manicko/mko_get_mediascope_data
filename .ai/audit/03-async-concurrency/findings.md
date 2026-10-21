---
phase: "03"
phase_name: "async-concurrency"
id_prefix: "ASY-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/03-async-concurrency/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Async Concurrency & Worker Safety

## Summary

Four findings, including one CRITICAL and one HIGH. A failure while recording a task error can leave the report waiting indefinitely, and timed-out work can exceed the configured concurrency ceiling. Some early exits leave workers alive, while task generation proceeds ahead of the intended queue bound.

## Scope

**Examined:** All five phase blocks: API/pool ceilings, worker cancellation and shutdown, producer backpressure, worker failure isolation and retry permit lifetime. Inspected core reports/network/tasks/services/models, concurrency documentation and relevant network/report tests. Historical ASY-001–007 are reserved; none is reused.

**Snapshot:** Current moving dirty working tree on 2026-10-21; `code_ref` identifies the base commit, not pristine audited content. Preserve initial hashes/status under `.ai/tmp/audit-2026-10-21/`. Pre-existing application-settings and overview-document edits, report deletions and settings-wiring plan are external work; coordinator also reported external async-concurrency/data-flow document drift. Findings use current source and scoped probes; prior phase inputs remain frozen.

**Tools:** Read-only PowerShell, `rg`, Git path/history inspection, existing venv Python. Probe `.ai/tmp/audit-2026-10-21/async_probe.py` invokes real application coroutines, asyncio tasks/queues/semaphores and worker threads with local fakes.

**Verification limits:** No real API calls, remote computations, real filesystem failure or production load. Native Windows asyncio setup previously stalled in socketpair creation inside the sandbox. The initial native escalation was interrupted by coordinator control; it was not a user rejection or product defect. Initial runtime evidence used `--socket-free`, an actual SelectorEventLoop with self-pipe/selector I/O mocked. The coordinator subsequently brokered approval and executed the native branch outside the sandbox without that flag: exit 0 in 1.9s, with matching findings. The auditor inspected its exact saved output in `.ai/tmp/audit-2026-10-21/async-native-output.txt`; Appendix C preserves it. Every case has a 2s deadline; the initial socket-free run also had an outside-process 12s timeout. Intentional pending tasks are explicitly collected/cancelled. No full test/lint/type suite was run. Process inventory via CIM was unavailable; no unidentified process was killed.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| ASY-008 | CRITICAL | `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker` | Error-persistence failure kills workers and strands queue completion |
| ASY-009 | HIGH | `src/mko_get_mediascope_data/core/network.py::NetworkClient._invoke` | Timed-out synchronous calls continue beyond released semaphore permits |
| ASY-010 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks` | Generation and enqueue exits leave workers outside shutdown cleanup |
| ASY-011 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks` | Full task materialization bypasses producer backpressure |

#### ASY-008 — [CRITICAL] Error-persistence failure kills workers and strands queue completion

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_file`
**Disambiguator:** `await asyncio.to_thread(item.to_file, self.export_path)`
**Checked:** ran
**Why this band:** On the reproduced failure path, no worker remains to acknowledge the queued task, leaving `queue.join()` without a completion path: the phase's CRITICAL indefinite-join criterion. This is conditional on an error-output failure, not every healthy invocation; the probe is stopped externally after observing the condition.
**Problem:** Error-handler branches await `TVTask.to_file` without protecting against its own failures. Disk/path/permission errors can escape the handler and terminate the worker. The coordinator waits only for queue completion, without monitoring failed workers; its final gather cannot execute until that wait ends.
**Repro:** Use one worker and two generated tasks. Make `_process_item` raise an ordinary task error, and patch only the error-file writer to raise a synthetic `OSError`. After a bounded observation, the worker is done with OSError, the second task remains queued, and report processing is pending. Cancel and collect the coordinator explicitly to end the probe.
**Impact:** Sellers can face an indefinite report wait precisely when the error-output destination fails; queued report work stops progressing instead of reporting a terminal run failure.
**Root cause:** Secondary failures inside worker error handling escape the isolation boundary; `processing_tasks` does not detect worker termination while awaiting queue join.
**Fix:** Make `MediaReport.worker` and `processing_tasks` preserve a terminating queue/pool contract when error persistence fails. Benefit: a broken output destination cannot silently hang remaining work; effort medium; advisory priority highest. [SPEC-DEVIATION]
**Verify:** new test needed: test_error_persistence_failure_does_not_strand_queue_join
**Related:** ASY-010 concerns a separate pre-join cleanup gap.

**Evidence — native and socket-free probes, commands in Appendices A/C** *(supports: dead worker plus queued work and pending coordinator)*:
```text
persistence_failure_parent_pending: True
persistence_failure_queued: 1
persistence_failure_worker_result: OSError
```

#### ASY-009 — [HIGH] Timed-out synchronous calls continue beyond released semaphore permits

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient._invoke`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `asyncio.wait_for(coro, timeout=self._call_timeout)`
**Checked:** ran
**Why this band:** The actual underlying call count exceeds the configured production ceiling and remains active after exhaustion. Rated HIGH for a broken concurrency boundary, rather than MEDIUM cancellation hygiene. No live remote task abandonment is proved, so the CRITICAL remote-computation criterion is not claimed.
**Problem:** A timeout cancels the awaited `to_thread` future, but the synchronous function can keep running. `call` releases its semaphore permit and retries, so subsequent attempts overlap earlier timed-out work despite the configured limit.
**Repro:** Set concurrency=1, attempts=2, timeout=0.02s and deterministic zero jitter/backoff. The synchronous fake increments a thread-safe activity counter and waits on a bounded threading event. After both timeouts, two calls remain active, exceeding the single permit. Release the event and allow threads to finish before exiting.
**Impact:** Under slow/stalled transport, operators' concurrency setting no longer bounds actual outbound work; retries add overlap during the failure condition it should constrain. The measured work is local fake calls; remote request volume is not measured.
**Root cause:** Semaphore permit lifetime tracks the cancellable awaitable rather than the lifetime of the synchronous operation it dispatched.
**Fix:** Make `NetworkClient._invoke`/`call` retain an effective bound on still-running synchronous operations after timeout/cancellation. Benefit: failure-time concurrency remains within the operator's ceiling; effort medium; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_timed_out_sync_calls_remain_within_concurrency_ceiling
**Related:** Phase 09 owns the blocking bridge and retry policy; this finding owns the measured concurrency-bound failure.

**Evidence — same bounded probe** *(supports: two active synchronous operations under a one-permit ceiling)*:
```text
thread_timeout_final_exception: MaxRetriesExceeded
thread_timeout_peak_active: 2
thread_timeout_semaphore_capacity: 1
thread_timeout_still_active_after_call: 2
```

#### ASY-010 — [MEDIUM] Generation and enqueue exits leave workers outside shutdown cleanup

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `async for task in self.generate_tasks()`
**Checked:** ran
**Why this band:** Cleanup/cancellation is not awaited on these reachable exits, matching MEDIUM shutdown discipline. No live remote abandonment is proved; the probe observes leftover local worker tasks, not a CRITICAL remote orphan.
**Problem:** Workers are created before generation and enqueueing, but the cancellation/gather `finally` covers only `queue.join()`. Generation failure, progress callbacks or cancellation while a full queue blocks `put` exit without cleaning up the already created workers.
**Repro:** First, make an async task generator raise after yielding control: one worker remains pending after `processing_tasks` raises. Separately, generate five tasks with one worker blocked in a fake item pipeline. Cancel the coordinator while queue capacity is full. The parent finishes cancellation, but one worker remains alive and one task remains queued. The probe explicitly cancels/gathers leftovers afterward.
**Impact:** In a long-lived async library host, a failed/cancelled report can leave background work running independently of the caller and retain queued task state. CLI loop-level cleanup may mask the lifetime gap; it is not a report-owned shutdown guarantee.
**Root cause:** The worker-group lifetime begins before the encompassing cleanup boundary.
**Fix:** Make `MediaReport.processing_tasks` own worker cleanup across generation, progress notification, enqueueing and joining. Benefit: every report exit has a defined worker lifetime; effort small; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_processing_tasks_cleans_workers_on_generation_failure_and_enqueue_cancel
**Related:** ASY-008 is worker death after entering processing; ASY-010 is coordinator exit before its cleanup boundary.

**Evidence — same bounded probe** *(supports: leftover workers on both pre-join exit paths)*:
```text
generation_failure_pending_workers: 1
enqueue_before_cancel_queue_size: 1
enqueue_cancel_pending_workers: 1
enqueue_cancel_unprocessed_queued: 1
```

#### ASY-011 — [MEDIUM] Full task materialization bypasses producer backpressure

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `all_tasks.append(task)`
**Checked:** ran
**Why this band:** The producer's retained backlog is not bounded by the queue, analogous to the phase's MEDIUM unbounded-depth criterion. Not HIGH: no silent producer stall is claimed. No measured resource exhaustion or capacity cost is claimed.
**Problem:** `processing_tasks` consumes the complete generator into `all_tasks` before enqueueing any task. Queue `maxsize` bounds only the subsequent handoff, so worker demand cannot apply backpressure to task generation or its retained backlog.
**Repro:** Configure one worker/queue slot, generate 50 local tasks and record how many have been generated when processing begins. All 50 are generated before the first item starts; queue capacity is one.
**Impact:** Large date/audience/region combinations retain the entire task set before processing and cannot rely on queue capacity to bound production. This establishes the missing bound; memory growth under production load is not measured here.
**Root cause:** A full intermediate task list is materialized to supply an exact progress total before bounded enqueueing.
**Fix:** Make `MediaReport.processing_tasks`' producer retention contract match the intended backpressure boundary while preserving accurate progress behavior. Benefit: worker demand can limit outstanding generated work; effort medium; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_task_generation_obeys_bounded_producer_backpressure
**Related:** Phase 13 may measure capacity effects; this phase establishes queue/producer semantics only.

**Evidence — same bounded probe** *(supports: producer consumes the full set ahead of a one-slot queue)*:
```text
generation_before_first_processed: 50
generation_queue_capacity: 1
```

## Fix Order

- **Distribution:** CRITICAL 1; HIGH 1; MEDIUM 2; LOW 0.
- **Dependencies:** None are required before the independent thread-bound fix. Review queue supervision and lifetime cleanup together before changing task production.
- **Merge candidates:** ASY-008 and ASY-010 touch the same worker-group lifecycle but have different triggers; do not collapse their verification cases. ASY-011 may interact with lifecycle/progress changes.
- **Conflicting evidence:** Ordinary task exceptions remain isolated when error recording succeeds; ASY-008 addresses the secondary error path. Existing `task_done` in `finally` acknowledges the current item, not still-queued work after all workers die.
- **Cross-phase conflicts:** ENT-008/009 and CFG-011/012 remain in their existing phases. Phase 09 owns sync bridging/retry semantics adjacent to ASY-009; phase 13 owns measured memory/capacity effects adjacent to ASY-011.
- **Rollout risk:** ASY-008/010 changes must preserve successful drain, no-task/all-skipped exits and ordinary per-item failure continuation. ASY-009 must preserve retry classification, cancellation delivery and bounded latency while accounting for outstanding sync work; `tests/test_network.py::test_invoke_applies_timeout_and_thread` currently checks prompt exhaustion, not thread termination, and may constrain remediation timing. ASY-011 must preserve progress totals/skipped-file reporting; inspected strategy tests document this materialization contract, so progress behavior is a remediation constraint.

## Appendices

### Appendix A — Complete bounded socket-free runtime output

Exact command:
```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/async_probe.py','--socket-free'],timeout=12); print('outer_process_exit:',p.returncode)"
```
Output (outer command exit 0; elapsed approximately 3.3s):
```text
generation_failure_pending_workers: 1
enqueue_before_cancel_queue_size: 1
enqueue_cancel_pending_workers: 1
enqueue_cancel_unprocessed_queued: 1
persistence_failure_parent_pending: True
persistence_failure_queued: 1
persistence_failure_worker_result: OSError
generation_before_first_processed: 50
generation_queue_capacity: 1
ordinary_failure_tasks_processed: ['0', '1']
thread_timeout_final_exception: MaxRetriesExceeded
thread_timeout_peak_active: 2
thread_timeout_semaphore_capacity: 1
thread_timeout_still_active_after_call: 2
outer_process_exit: 0
```
*(supports: ASY-008–011 under the explicitly mocked I/O-loop environment. The ordinary failure line records block-4 methodology, not a separate finding.)*

### Appendix B — Five-block scope coverage and static boundaries

| Block | Coverage and limit |
|---|---|
| 1: Ceiling/pool | Service supplies `max_tasks` (default 100) to report worker count and queue capacity, and creates a per-report NetworkClient with `max_concurrent_requests` (default 5). These distinct controls are intentionally independent. API call paths for task submit/status/result and availability/regions use that client's semaphore. `api_call=False` bypasses it for builder/result conversion and connection construction; vendor constructor internals may themselves perform authentication/catalog calls and are outside a strict local-only assumption. ASY-009 measures a separate timeout breach on the normal semaphore path. This phase makes no global cross-process ceiling claim. |
| 2: Shutdown | Normal queue join, zero generated tasks and all-skipped paths cancel and gather the worker group. Pre-join exits do not share that protection (ASY-010). Polling has configured count/deadline checks rather than the historical unbounded-loop claim. Cancellation of dispatched synchronous operations cannot prove transport termination (ASY-009). Live remote task cleanup and partial output fidelity were not tested. |
| 3: Queue | Queue `maxsize=self.max_tasks`; `await put` does block the enqueueing coordinator when capacity is full, observed in the enqueue-cancel probe. Worker debug logs report queue depth after get, so the historical non-observability finding is not repeated. Generation has already finished before enqueue backpressure occurs (ASY-011). No production memory/load benchmark was performed. |
| 4: Isolation | Worker wraps ordinary `_process_item` errors and calls `task_done` in finally. Controlled ordinary error plus functioning writer allows tasks 0 and 1 to be processed. A secondary writer OSError instead terminates the worker and strands remaining queue work (ASY-008). Reporter callback failures are statically reachable adjacent cases but not separate runtime findings. |
| 5: Retry isolation | Static `async with self._semaphore` surrounds `_invoke` only; exception handlers and backoff execute after permit release. Formula is `sleep_time * 1.2**(attempt-1) + uniform(0,5)` plus initial smoothing, so historical full-backoff permit holding/no-jitter claims are not repeated. Probe exhaustion is `MaxRetriesExceeded`, distinct from permanent exceptions. Actual synchronous operation lifetime outlasts the permit after timeout (ASY-009). Vendor transport retries are reserved for phase 09. |

Anchor gates: each cited production path is tracked (`git ls-files`); named classes/functions and primary disambiguators were resolved by source inspection. Probe writes are limited to the assigned `.ai/tmp/` directory; application code/configuration/tests/documentation and `.kilo` were not changed. No malformed shared-template pointer was copied into front matter.

### Appendix C — Native Windows asyncio confirmation

Coordinator-brokered approved execution outside the sandbox, with no `--socket-free` flag:
```powershell
.\.venv\Scripts\python.exe -u -X utf8 '.ai\tmp\audit-2026-10-21\async_probe.py'
```
Coordinator reported exit 0 in 1.9s. Auditor inspected `.ai/tmp/audit-2026-10-21/async-native-output.txt`; its exact stdout is:
```text
generation_failure_pending_workers: 1
enqueue_before_cancel_queue_size: 1
enqueue_cancel_pending_workers: 1
enqueue_cancel_unprocessed_queued: 1
persistence_failure_parent_pending: True
persistence_failure_queued: 1
persistence_failure_worker_result: OSError
generation_before_first_processed: 50
generation_queue_capacity: 1
ordinary_failure_tasks_processed: ['0', '1']
thread_timeout_final_exception: MaxRetriesExceeded
thread_timeout_peak_active: 2
thread_timeout_semaphore_capacity: 1
thread_timeout_still_active_after_call: 2
```
*(supports: ASY-008–011 in the native Windows asyncio branch, matching initial mocked-I/O measurements. Synthetic task/writer/transport fakes remain; no actual Mediascope behavior is claimed.)*
