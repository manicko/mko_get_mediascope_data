---
report_status: "validated"
mode: "problems-only"
code_ref: "6c9cc7ab3228c09c65d7a2140e2a2f594eb9291e"
final_observed_head: "ee575dc7f4f9b857039509507f59a79708b0c7cb"
baseline_code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Snapshot Freshness — Validation Addendum

All seven previously accepted findings still apply to the inspected updated implementation. The earlier rejection of ASY-011 also stands. No finding is resolved by the observed task-context/settings changes; approved content totals remain one CRITICAL, five HIGH and one MEDIUM.

## Scope and moving snapshot

Validator-only recency check on 2026-10-21, using the already loaded validator role and phase-99 procedure. Inputs are the frozen phase 01/02/03 raw and validated reports. This addendum preserves their original IDs/dispositions and changes none of those reports; it creates no content or VAL namespace.

HEAD observed at the start and source-hash capture: `6c9cc7ab3228c09c65d7a2140e2a2f594eb9291e`. The original baseline HEAD was `4c08eb445f80584a00f16d929bc995239a1bd3d3`, but its code_ref alone never represented the complete original dirty snapshot. Existing `.ai/tmp/audit-2026-10-21/initial-hashes.json` and initial-git-status.txt remain historical evidence.

The tree remains dirty. Initial status showed rewritten audit outputs/deleted historical reports, user application-settings edits, overview-document edits and untracked settings/network-context plans/task artifacts. During this check, additional overview-document differences appeared against HEAD. This is a moving workspace, not an atomic whole-tree snapshot. Source hashes below identify inspected implementation bytes; subsequent source drift requires another recency check.

Read current root AGENTS.md in full and compared it with baseline. Its relevant change corrects the task-submission retry description to config-driven SendTaskSettings; role boundaries, no-code-edit constraints and workflow rules are unchanged. Inspection of current source, rather than either old or new prose, decides applicability.

Relevant source delta against baseline: NetworkClient.call adds task_name and contextual log messages; TVMediaReport passes task names through builder/send/status/result/conversion calls; AppService passes task_name=None to connection construction; worker's skip warning now includes item.error. New network/lifecycle tests cover log context and warning reason. These changes do not move the timeout/permit boundary, broaden worker cleanup, supervise worker death, alter the data overlay or alter entry bootstrap. No conclusion is inferred from merely naming these changes logging changes; the executing methods and probes were checked.

## Per-finding applicability

| ID | Freshness verdict | Current type / severity | Evidence and remaining scope |
|---|---|---|---|
| ENT-008 | still applies | DOC-UPDATE / HIGH | app.run_report still advertises Jupyter and calls asyncio.run unconditionally. Current guard simulation repeats RuntimeError plus unawaited-coroutine warning; no real Jupyter kernel tested. Existing async wrapper remains the supported active-loop route. |
| ENT-009 | still applies | SPEC-DEVIATION / HIGH | app.run still bootstraps before try; list_reports also lacks contextual bootstrap handling. Current CliRunner run/list fail with FileNotFoundError without init recovery; scratch init succeeds. |
| CFG-011 | still applies | SPEC-DEVIATION / HIGH | AppService.merge_data_settings still dumps synthesized client defaults. Current omitted ad_filter becomes None in validated Data and a generated task, despite nonempty selected subtype filter. No remote result claim. |
| CFG-012 | still applies | SPEC-DEVIATION / HIGH | get_connection still reaches the installed vendor constructor with settings_filename. Synthetic authenticated proxy password remains present in both captured constructor and real application-path stdout. No actual secret was used or emitted in evidence. |
| ASY-008 | still applies | SPEC-DEVIATION / CRITICAL | Worker persistence handler and join-only coordinator supervision are unchanged. Current native probe has dead worker/OSError, one queued item and pending parent. Current-item task_done still executes; queued work is what remains stranded. |
| ASY-009 | still applies | SPEC-DEVIATION / HIGH | _invoke still cancels the wait rather than the running synchronous operation; call still releases its permit and retries. Native measurement again reaches two active operations under one permit, with two still active at exhaustion. |
| ASY-010 | still applies | SPEC-DEVIATION / MEDIUM | processing_tasks still creates workers before generation, outside the eventual join-only finally. Native generation-failure and enqueue-cancellation cases each leave one pending worker until probe cleanup. |
| ASY-011 | still applies: prior rejection | No approved type/severity | Materialization remains explicit and the same 50-before-first-processing observation repeats. Exact progress totals/skipped-task semantics remain intentional; queue depth is bounded. No new retention requirement or harmful capacity measurement establishes the rejected defect. |

The eight rows resolve to the same semantic anchors as their respective validated reports: app.run_report/run/list_reports, AppService.merge_data_settings/get_connection, NetworkClient._invoke/call, MediaReport.worker/processing_tasks and TVTask.to_file. Affected method bodies and primary tokens still resolve in current source. No finding is marked changed, resolved or unsettled in this addendum.

## Repeated measurements and provenance

The validator inspected and executed existing unmodified probes during this freshness check. The entry and configuration probes ran in the default sandbox; the exact already-approved native async command ran outside the sandbox with no socket-free flag. All exited 0. Application/vendor transport was not used; credential files were synthetic and scratch-only, and deliberate leftover asyncio/thread work was collected/released by the bounded probe.

These are current observations, distinct from the earlier phase evidence. The original reports' timestamps/code_ref and saved outputs remain historical measurements; the earlier coordinator-brokered native run is not being relabeled as this validator's measurement. This validator separately repeated the native run here.

### Entry reproduction

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_probe.py`, exit 0. Relevant output:

```text
missing_config_exit: 1
missing_config_exception: FileNotFoundError [Errno 2] No such file or directory: 'C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5'
missing_config_init_hint: False
missing_list_exit: 1
missing_list_exception: FileNotFoundError
isolated_init_exit: 0
isolated_init_app_config_exists: True
isolated_init_log_config_exists: True
running_loop_exception: asyncio.run() cannot be called from a running event loop
running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```

### Configuration reproduction

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/config_probe.py`, exit 0. Relevant output:

```text
subtype_ad_filter_nonempty: True
report_explicitly_sets_ad_filter: False
merged_ad_filter: None
generated_task_ad_filter: None
proxy_secret_in_constructor_stdout: True
proxy_secret_in_get_connection_stdout: True
```

The remaining probe shape-validation/control observations are not used to claim general correctness or comprehensive credential validity.

### Native concurrency reproduction

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai\tmp\audit-2026-10-21\async_probe.py`, approved outside-sandbox execution, exit 0 in 2.07 seconds. Complete stdout:

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

### Source-byte identity and final drift check

SHA-256 values captured after current reproductions and independently rechecked before completing the addendum:

```text
AGENTS.md e73a00b66383f719fe67994b2410727e69bc71f39d1c01bc0ac17a6c5feb1d92
src/mko_get_mediascope_data/app.py ab00f0955e8191d0f15c7da8a75054671fc449c27fa49a390e840e541f6cf337
src/mko_get_mediascope_data/core/models.py f16cfe43ef70e0d7afc56057085ee96c63784b955c5fea34c966ee910efc4892
src/mko_get_mediascope_data/core/network.py 7dc8183c7b21e1a24d0cf608c05aad7168a2be865391bf5fa9d904263e2ff7d2
src/mko_get_mediascope_data/core/reports.py 2eb1175c05826921e5254cd99ca0287a40d1711b8f5f97a37320323982022ca3
src/mko_get_mediascope_data/core/services.py e64972bb7a7a868bb8915dc2174d671682682bc5c859312ee6bc7d613132866c
src/mko_get_mediascope_data/core/tasks.py 8f0d723a4b127bd8e14f52f200808ac172470577b3fa15a715e56868d92c2f41
.venv/Lib/site-packages/mediascope_api/core/net.py d4ed1aa9389f9502d02ae0b92ac9be32ef08e846ee39761485232c990ddc92b9
```

Final existing-venv hash/HEAD check, exit 0:

```text
final_head: ee575dc7f4f9b857039509507f59a79708b0c7cb
changed_since_source_hash_capture: []
checked_files: 8
```

HEAD advanced again during the check. Read-only comparison of `6c9cc7a..ee575dc` showed only async-concurrency/current-behavior documentation changes (8 insertions, 1 deletion); none of the inspected implementation bytes changed. Dirty overview edits still remain. Therefore the applicability decisions also hold for the final observed HEAD plus the described dirty tree, subject to the recorded source hashes. This explicitly supersedes neither the frozen phase code_ref nor its original evidence provenance.

## Counts, limits and required rechecks

- Phase 01 counts remain usable: two HIGH (ENT-008 DOC-UPDATE; ENT-009 SPEC-DEVIATION).
- Phase 02 counts remain usable: two HIGH, both SPEC-DEVIATION.
- Phase 03 counts remain usable: one CRITICAL, one HIGH, one MEDIUM, all SPEC-DEVIATION; ASY-011 stays rejected and excluded.
- Aggregate approved/applicable: seven findings — CRITICAL 1, HIGH 5, MEDIUM 1, LOW 0. Rejected 1. Resolved 0; changed 0; unsettled 0.
- No new validation-level issue is allocated. Existing VAL-006..009 dispositions/maintenance references remain in their owning reports; this is recency evidence only.
- Any subsequent change to the recorded entry, data merge, vendor constructor, worker supervision/cleanup or timeout/permit paths needs another targeted recheck. A future fixed dependency artifact must be measured before marking CFG-012 resolved; logging context alone is not that correction.
- Real Jupyter execution, real disk failure, live remote orphaning, production capacity, production credentials and full test/lint/type execution remain unverified. Earlier observations are only measurements of their original snapshots; current observations above separately establish present applicability.

Ready for tech-lead use as the bounded freshness addendum. No source/test/config/docs/plan/.kilo, input finding or prior validation report was modified.
