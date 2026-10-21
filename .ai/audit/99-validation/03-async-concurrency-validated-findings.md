---
phase: "03"
phase_name: "async-concurrency"
id_prefix: "ASY-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/03-async-concurrency/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Validation Report

## Summary

Three findings are substantiated: one CRITICAL, one HIGH and one MEDIUM. Error-output failure can strand pending work indefinitely, timeout retries can exceed the configured concurrency ceiling, and early exits leave report workers alive. The fourth claim is rejected: full task collection is deliberate for exact progress totals, and the evidence does not establish a breached retention contract or harmful capacity effect.

## Scope

**Input:** Frozen `.ai/audit/03-async-concurrency/findings.md`; all four IDs independently adjudicated on 2026-10-21.
**Examined:** Current reports/network/tasks, report and network tests, configuration semantics, concurrency documentation, input probe and saved native output, prior phase 01/02 raw and validated findings, controlled report template and historical ASY provenance.
**Snapshot:** Current dirty working tree on the stated HEAD, not pristine HEAD; baseline hashes/status are under `.ai/tmp/audit-2026-10-21/`. Concurrent overview-document changes were read as corroboration and preserved. Source, explicit source contracts and tests establish the decisions independently.
**Tools:** Read-only PowerShell, rg, Git object/path inspection, AST and existing venv Python. The validator inspected and independently reran the exact approved native probe outside the sandbox, without --socket-free; exit 0 in 1.77 seconds. This is separate from the earlier coordinator-brokered native measurement inspected by the auditor.
**Not verified:** Live remote computation/request cancellation, real disk failure, production memory/capacity cost, CSV fidelity or full test/lint/type suite. Fakes replace item processing and error persistence; actual application queues, workers, semaphores, loop and worker threads execute. Every probe has a 2-second deadline; intentional leftovers are collected and worker-thread release is bounded.

## Findings

| ID | Final severity | Anchor | Title |
|---|---|---|---|
| ASY-008 | CRITICAL | `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker` | Secondary error-output failure strands queued work |
| ASY-009 | HIGH | `src/mko_get_mediascope_data/core/network.py::NetworkClient._invoke` | Timed-out thread work exceeds the configured permit ceiling |
| ASY-010 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks` | Pre-join exits leave workers outside report cleanup |
| ASY-011 | Not assigned: rejected | `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks` | Deliberate full materialization is not established as a defect |

| ID | Disposition | Original type / severity | Final type / severity |
|---|---|---|---|
| ASY-008 | confirmed | SPEC-DEVIATION / CRITICAL | SPEC-DEVIATION / CRITICAL |
| ASY-009 | confirmed | SPEC-DEVIATION / HIGH | SPEC-DEVIATION / HIGH |
| ASY-010 | confirmed | SPEC-DEVIATION / MEDIUM | SPEC-DEVIATION / MEDIUM |
| ASY-011 | not substantiated | SPEC-DEVIATION / MEDIUM | No approved type or severity |

#### ASY-008 — [CRITICAL] Secondary error-output failure strands queued work

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.worker`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_file`
**Disambiguator:** `await asyncio.to_thread(item.to_file, self.export_path)`
**Checked:** ran
**Why this band:** The reproduced one-worker configuration leaves a queued item with unfinished accounting and no worker capable of consuming it; queue.join has no remaining completion path. That is the phase's CRITICAL indefinite-join effect, beyond HIGH isolated worker failure. It is conditional on secondary persistence failure, not a claim that every configured/healthy invocation hangs.
**Problem:** Exceptions raised while recording a processing failure escape worker's except block. processing_tasks waits only for queue completion, so the dead worker cannot trigger a terminal coordinator error.
**Repro:** Native probe uses max_tasks=1, two tasks, a synthetic ordinary processing exception and TVTask.to_file raising OSError. It observes a failed worker, one queued item and a pending coordinator; explicit cancellation ends the probe. Appendix A repeats these values.
**Impact:** A report can wait indefinitely with unprocessed work when its error destination fails, rather than surfacing the output failure to the caller.
**Root cause:** Secondary error handling is outside its own exception-isolation guard, and the coordinator does not supervise worker termination. Crucially, the current dequeued item's task_done does execute in finally; the stranded unfinished work is the remaining queued item. This is not historical ASY-001's missing-finally cause.
**Fix:** In MediaReport.processing_tasks supervise queue completion and unexpected worker termination together; if a worker exits exceptionally before drain, fail the report with that cause and cancel/gather all remaining workers under one enclosing finally. Keep current-item task_done in MediaReport.worker and ensure persistence failure is logged once without recursively attempting the same failed writer. On abort, define and test cleanup/accounting of queued items rather than waiting on an impossible join. This is a fail-fast pool policy; do not silently report success after the error destination fails.
**Verify:** new test needed: test_error_persistence_failure_does_not_strand_queue_join. Exercise one worker/two tasks, multiple workers losing all consumers, and ordinary task failure with functioning error output; assert bounded terminal error and no owned worker remains.
**Related:** ASY-010 shares the cleanup target but has a distinct pre-join trigger; phase 04 may cross-reference error artifact details.
**Anchor/cause decision:** MediaReport.worker, processing_tasks and TVTask.to_file resolve mechanically; the disambiguator occurs inside worker. TVTask.to_file performs mkdir/open/json5.dump, so OSError is a reachable real failure class. Runtime simulation proves the pool consequence without claiming actual disk exhaustion.
**Recommendation usability:** The raw instruction states the desired terminating contract but does not select supervision/abort policy. The explicit fail-fast supervision and cleanup mechanism above makes it implementable; preserve ordinary per-item error isolation when recording succeeds.

#### ASY-009 — [HIGH] Timed-out thread work exceeds the configured permit ceiling

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient._invoke`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `asyncio.wait_for(coro, timeout=self._call_timeout)`
**Checked:** ran
**Why this band:** Actual synchronous-operation concurrency exceeds the operator's API ceiling on the normal semaphore-protected path. The phase grades effects and blast radius rather than an exhaustive list of mechanisms: this is HIGH failure of its core concurrency contract, beyond MEDIUM cancellation hygiene. No live remote computation abandonment is established, so CRITICAL is not claimed.
**Problem:** wait_for cancels the awaitable while its dispatched synchronous function remains active. The async-with block releases the semaphore, and the retry starts another function under the same nominal one-permit ceiling.
**Repro:** Native probe sets one permit, two attempts, 0.02-second timeout and deterministic zero backoff/jitter. The thread-safe activity counter reaches two, and both functions remain active after MaxRetriesExceeded. The bounded release event is then set so all thread work terminates.
**Impact:** Slow transport can overlap retry attempts beyond configured request concurrency, undermining failure-time throttling. The observed operations are local synchronous fakes; actual outbound request volume is not measured.
**Root cause:** Permit ownership ends with the cancellable wait instead of actual synchronous completion. Normal permit release before backoff is otherwise correct; holding an idle permit through retry sleep is not the proposed repair.
**Fix:** Refactor NetworkClient.call/_invoke so each dispatched synchronous operation owns its permit until its retained underlying future actually completes. Keep that future alive across timeout/caller cancellation, shield it from cancellation of the timed wait, and release its permit on actual completion while consuming terminal exceptions. A timeout must not admit another operation through that occupied slot. Define bounded admission/deadline behavior separately and preserve explicit api_call=False bypass semantics; Python threads cannot be forcibly terminated by cancelling the awaitable. Verify transport timeouts provide eventual completion rather than claiming a thread-stop guarantee.
**Verify:** new test needed: test_timed_out_sync_calls_remain_within_concurrency_ceiling; cover timeout, caller cancellation, final exhaustion, permit release after actual completion and ordinary retry success.
**Related:** Phase 09 owns bridge implementation and retry policy; phase 03 owns this measured ceiling breach. Later phase 09 should cross-reference ASY-009 rather than duplicate its consequence.
**Anchor/cause decision:** Both methods and exact primary token resolve. Native worker-thread overlap independently confirms the control-flow proof. No assertion depends on replacing the event loop's selector or on a live vendor API.
**Recommendation usability:** The raw instruction is outcome-only; retained-future permit ownership supplies a concrete implementation approach. Preserve prompt cancellation of the caller while acknowledging that a retry can wait for actual capacity. A finite call timeout alone is not an end-to-end shutdown guarantee.

#### ASY-010 — [MEDIUM] Pre-join exits leave workers outside report cleanup

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `async for task in self.generate_tasks()`
**Checked:** ran
**Why this band:** MEDIUM covers missing awaited cancellation/cleanup and partial retained state. The affected path never cancels/gathers its owned workers; the actual leftovers persist in a live library loop. No worker exception aborting unrelated healthy work, unobserved silent backpressure stall, or abandoned remote computation is established to justify HIGH/CRITICAL.
**Problem:** Workers are started before generation/progress/enqueue, but the enclosing finally begins only at queue.join. Generator failure and cancellation during a blocked put exit before that cleanup boundary.
**Repro:** Native probe first raises from an async generator after yielding loop control; one owned worker remains pending. It separately cancels the coordinator while one worker waits in a fake item pipeline and the queue is full; the coordinator returns cancelled with one worker still pending and one item queued. Explicit cleanup collects the leftovers afterward.
**Impact:** An async library caller can receive completion/failure/cancellation while its report workers remain alive and retain work. asyncio.run's whole-loop cleanup in CLI execution may mask this report-lifetime defect.
**Root cause:** Worker-group lifetime begins before the try/finally that owns worker shutdown. The zero-task and all-skipped paths have their own cleanup, but do not protect intervening failures or cancellation.
**Fix:** Start one encompassing try/finally immediately after creating the worker group in MediaReport.processing_tasks; include generation, report_started/skipped notifications, enqueue and join in its scope. Always cancel/gather owned workers on exit. Apply ASY-008's supervision policy inside that lifetime, and define whether report_completed runs only after report_started so failing callbacks do not mask the original exception or skip worker cleanup.
**Verify:** new test needed: test_processing_tasks_cleans_workers_on_generation_failure_and_enqueue_cancel; include report_started/task_status_changed failure, no-task and all-skipped exits, normal drain and cancellation during queue.put.
**Related:** ASY-008; separate trigger/case, shared cleanup implementation dependency. Phase 01's entry-loop contract and phase 02's value/secret findings are distinct.
**Anchor/cause decision:** The method, generator-loop token, worker creation and narrower join-only finally resolve in tracked source. Native cancellation shows actual live workers in the continuing host loop, not a synthetic unsupported claim about loop shutdown.
**Recommendation usability:** Stable existing target; moving the lifetime finally around all owned work is concrete and preserves the present reporting contract. Implement alongside ASY-008 to avoid competing coordinator cleanup designs.

#### ASY-011 — [not substantiated] Deliberate full materialization is not established as a defect

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `all_tasks.append(task)`
**Checked:** ran
**Original band:** MEDIUM; no approved final severity or type.
**Problem adjudication:** The observation is accurate: all 50 generated task objects exist before processing starts, despite a queue of capacity one. It does not prove the claimed bounded-producer contract. The actual queue remains bounded and its awaited puts exert handoff backpressure. max_tasks bounds worker/remote-calculation concurrency and queue handoff; no current requirement makes it a bound on all retained generated task definitions.
**Repro:** The independent native run prints generation_before_first_processed=50 and generation_queue_capacity=1. It measures ordering and capacity, not memory exhaustion, unacceptable memory/latency, or violation of a configured task-definition limit.
**Impact adjudication:** No adverse current resource consequence is demonstrated. Work proportional to the chosen finite report scope is retained deliberately to establish an exact initial progress total; calling this backlog an unbounded queue conflates two different data structures/contracts.
**Root cause adjudication:** Materialization is explicit in processing_tasks' docstring/comment, not an accidentally bypassed put. The source says to include all generated and pre-existing-output tasks in progress totals. `tests/test_reports_progress.py::test_processing_tasks_reports_all_tasks_including_skipped` expects report_started(total=3) and exact skipped/processed transitions. `tests/test_reports.py::test_tv_reg_task_strategy_yields_all_tasks_including_done_files` preserves the complete generated scope for that purpose.
**Type/severity decision:** Rejected as not substantiated; neither SPEC-DEVIATION nor a proven BEST-PRACTICE defect is approved. The phase's MEDIUM unbounded/non-observable queue-depth criterion does not apply to a queue that is bounded and has depth logging merely because a separate intentional finite list exists. The checklist's streaming premise is not an independent product requirement.
**Fix:** No remediation approved. Preserve the exact-total/skipped-task contract. If phase 13 establishes unacceptable retained-task memory or startup latency on supported reports, investigate a count-then-produce or bounded-spooling design there with cost measurements and progress invariants; do not assume regeneration is side-effect-free or change progress behavior solely to remove this finding.
**Verify:** No corrective test required for a rejected claim. Existing exact-total/skipped-task tests remain load-bearing. Any future phase 13 optimization needs measurements and explicit acceptance limits before redesigning production.
**Related:** Phase 13 may investigate capacity costs; this is an investigation boundary, not transfer or approval of a performance finding. Phase 03 continues to own actual queue handoff semantics and cancellation during put, already covered by ASY-010.
**Anchor/cause decision:** The primary method/token resolves and the ordering probe reproduces. The asserted defect is rejected after comparing intended behavior, actual limits and missing consequence evidence; it is not unsettled by the environment.
**Recommendation usability:** The raw fix does not explain how accurate progress is preserved or establish why a change is needed. Its proposed architectural change is withheld; the reader is not asked to implement streaming as a presumed improvement.

## Fix Order

- **Content distribution:** Approved/actionable CRITICAL 1; HIGH 1; MEDIUM 1; LOW 0. ASY-011 has no final severity and is excluded.
- **Disposition tally:** confirmed 3; re-typed 0; re-graded 0; merged 0; not substantiated 1; unsettled 0. Four dispositions for four original IDs.
- **Dependencies:** Design ASY-008 supervision and ASY-010 encompassing cleanup together, keeping both independent failure cases. ASY-009 is independent but requires explicit ownership of outstanding thread work and bounded admission semantics.
- **Merge candidates:** Shared symbols do not make ASY-008/010 the same root cause: worker death while join waits differs from parent exit before cleanup. Preserve both IDs and verification cases.
- **Conflicting evidence:** Current-item task_done exists; the pending queued item strands ASY-008. Queue maxsize and progress materialization are both intentional; ASY-011's inferred whole-producer bound is unsupported. Ordinary error isolation works when error recording succeeds, matching the native control observation.
- **Cross-phase conflicts:** Compared phase 01/02 raw and validated reports; no same-cause duplicate. ASY-009 is the ceiling-effect owner; phase 09 owns adjacent bridge/retry mechanics. Phase 04 may cross-reference error artifacts. Do not transfer rejected ASY-011 into phase 13 as a confirmed issue.
- **Rollout risk:** Preserve current-item accounting, per-item continuation with functioning error output, no-task/all-skipped handling, exact progress totals and skipped-task reporting. Keep ordinary retry classification/jitter and prompt caller cancellation while avoiding promises that Python threads stop on timeout. Existing `tests/test_network.py::test_invoke_applies_timeout_and_thread` tests exhaustion, not actual thread completion; it cannot settle the permit-lifetime fix.
- **Readiness:** The three approved findings are ready for a remediation plan using the concrete mechanisms above. Implementation must resolve worker-abort policy, reporting lifecycle and outstanding-thread ownership before rollout. No source or test repair is included in this audit.

## Validation-Level Findings

No new VAL finding is allocated. Existing VAL-007 (template pointer) and VAL-008 (historical namespace) are referenced from phase 01 validation, not duplicated or counted. VAL-006's phase 01 grading correction does not apply here. Raw outcome-only fix wording for ASY-008/009 exhibits the recommendation-usability pattern already recorded as VAL-009 in phase 02; their raw recommendations are recorded as unusable unchanged and made concrete here, without allocating a duplicate issue. ASY-011 receives an explicit rejected disposition, not a silently dropped finding.

## Appendices

### Appendix A — Validator-owned native reproduction

Exact approved outside-sandbox command:

```powershell
.\.venv\Scripts\python.exe -u -X utf8 .ai\tmp\audit-2026-10-21\async_probe.py
```

Exit 0, 1.77 seconds. This validator independently executed it after inspecting the script. No --socket-free flag or selector modification was used; synthetic per-item processing, writer and blocking-call inputs remain. Complete stdout:

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

The persistence case plus source proves an impossible join without external cancellation; it does not claim to have waited forever. Ordinary-failure output demonstrates the probe's isolation baseline, not a finding or a green-control approval. Generation ordering is reproduced but its defect inference is rejected. The parent previously brokered a separate native run whose saved output the auditor inspected; that measurement was not misattributed to the validator.

### Appendix B — Namespace, template and support rulings

- **Declared/history:** Phase 03 declares ASY-. Historical raw report at HEAD minted ASY-001..007, including historical withdrawn/advisory ASY-004; all remain reserved. Current ASY-008..011 continue the same namespace with no collision. Historical ASY-001 is not ASY-008's cause: the present worker has a finally/task_done, but secondary persistence failure can remove all consumers.
- **Shipped markers:** Search of src/tests/docs found four ASY references: ASY-004 in async-concurrency architecture, ASY-001 twice in current-behavior and once in system-architecture. These are historical architectural/remediation provenance; qualify the historical report/commit where future maintenance requires resolution. Preserve the intentionally independent pool/semaphore control described by historical ASY-004, rather than repurposing its identifier for a new defect.
- **Compound/reuse:** No phase-qualified compound ASY identifier is minted by this input/report; no compared current sibling reuses ASY. Historical shared VAL collisions and in-source provenance remain VAL-008's separate maintenance concern. This report allocates no new VAL IDs, and does not invent a second namespace to avoid disposition.
- **Template coverage:** The controlled template contains no closed prefix enumeration; the phase registry/workflows supply it. Its malformed generic severity pointer is already VAL-007, referenced without a duplicate. The phase 03 raw report contains severity_taxonomy under the exact name once, with the actual workflow path; missing 0, renamed 0. This report also uses that real path, validated status and honest dirty-tree scope.
- **Fields/tallies:** Each raw finding supplies semantic anchors, primary disambiguator, checked mode, band rationale, problem, reproduction, impact, cause, fix, verify and related fields. This report retains each ID and an explicit disposition, including the rejected item. Approved severity totals exclude that rejected ID; new validation findings are zero and do not share the content scale.
- **Green controls:** The native probe's per-case observations, not the raw report's coverage table, substantiate ASY-008/009/010. Existing source tests were inspected but not run. test_invoke_applies_timeout_and_thread invokes one short-timeout call and asserts MaxRetriesExceeded; it observes no activity counter and decides nothing about actual thread termination or ceiling. The two progress tests specify total/skipped behavior but do not measure acceptable memory cost or prove that streaming is required.
- **Checklist dependence:** ASY-008 corresponds to worker failure isolation and shutdown blocks; ASY-009 to concurrency ceilings; ASY-010 to cancellation/producer lifecycle. Independent native observations substantiate them, rather than a PASS row. ASY-011 corresponds to the backpressure block's question but supplies no breached generation-retention requirement; a checklist premise cannot turn intentional materialization into a defect. No approved finding requires an angle absent from the phase's broad blocks.
- **Ownership:** Phase 03 owns abandoned local workers, queue completion and actual ceiling effects. Adjacent phase 04 output lifecycle, phase 09 bridging/retries and phase 13 measured capacity remain separate. Phase 01/02 raw and validated reports have no duplicate cause; their IDs and frozen inputs were left untouched.
- **Unsettled scope:** No input finding is left unsettled. Rejection of ASY-011 follows intended behavior and insufficient defect evidence, not an environment blocker. Real remote orphaning, production capacity consequences and actual filesystem failure remain outside the accepted claims and must not appear as confirmed impact.

### Appendix C — Final local consistency check

Existing venv Python AST and report-text check (exit 0):

```text
semantic_anchors_resolved: 8
dispositions: {'confirmed': 3, 'not substantiated': 1}
unique_input_ids: ['ASY-008', 'ASY-009', 'ASY-010', 'ASY-011']
approved_headers: [('ASY-008', 'CRITICAL'), ('ASY-009', 'HIGH'), ('ASY-010', 'MEDIUM')]
rejected_headers: ['ASY-011']
new_VAL_headers: []
line_locators: []
```

The eight resolved symbols comprise the five production methods named in the findings plus the two progress tests and named timeout test. All cited production paths were checked as tracked files, and primary disambiguator tokens occur inside their methods. No source, tests, dependency, application configuration, docs, plans, .kilo files, input findings or shared template was edited.

Ready for tech-lead acceptance with three approved findings and ASY-011 rejected. The raw report must not be adopted unchanged: use the corrected three-item distribution, distinct failure causes and concrete recommendations here.
