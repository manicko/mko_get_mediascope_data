---
report_status: "validated"
mode: "problems-only"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Resume Snapshot Freshness — Validation Addendum

The validator session resumed successfully. All ten previously accepted content findings still apply to the inspected current working source; the two rejected claims remain rejected. Current approved counts are CRITICAL 3, HIGH 6, MEDIUM 1, LOW 0. No usage, approval or verification blocker arose.

## Scope and source identity

This is a bounded change-impact/recency check under the validator role and phase-99 procedure, not a new full audit or a new identifier namespace. Inputs are current-run phase 01–04 raw/validated reports and snapshot-freshness.md; copies preserved under `.ai/tmp/audit-2026-10-21/resume-b27b870-reports/` provide resume provenance. Restored historical future-phase/YAML/d093d9c reports are historical evidence only and are not adopted as current results.

Current HEAD was b27b8708d4d816940310d4f034b549cb1db6f397. The index and working tree differ from HEAD: staged source/tests/guidance changes, restored/staged historical audit outputs and user application-template changes coexist with current-run reports and untracked plans/task artifacts. HEAD alone therefore does not identify the executing bytes. This report records working-file hashes and current measurements rather than treating restored historical reports or a staged snapshot as application truth. No Git mutation, index update, restore, checkout or reset was performed.

Current AGENTS.md was read in full. Its async-pipeline prose again says task submission is single-attempt; the executing source still uses config-driven SendTaskSettings. Role/edit restrictions remain unchanged. This prose drift does not prove that retry behavior changed; actual source decides this narrow check.

Compared working source against the prior ee575dc checkpoint. NetworkClient.call no longer declares task_name or appends context to logs; reports/services no longer pass that context, and the worker skip warning no longer appends item.error. Corresponding network/lifecycle logging tests were removed. These deltas do not alter the accepted bootstrap, omission merge, proxy constructor, timeout/permit, worker lifetime, CSV publication, no-overlap propagation or output-name collision branches. The code was inspected and the observations repeated before drawing that conclusion.

## Per-ID applicability

| ID | Freshness verdict | Current approved type/severity | Current evidence |
|---|---|---|---|
| ENT-008 | still applies | DOC-UPDATE / HIGH | Advertised synchronous notebook wrapper still reaches asyncio.run; current guard simulation reproduces RuntimeError and unawaited-coroutine warning. |
| ENT-009 | still applies | SPEC-DEVIATION / HIGH | run bootstrap remains outside its handler; run/list-reports again fail on scratch missing settings without init guidance, while isolated init succeeds. |
| CFG-011 | still applies | SPEC-DEVIATION / HIGH | Omitted client ad_filter still overwrites the selected nonempty subtype filter with None; generated task has no restriction. |
| CFG-012 | still applies | SPEC-DEVIATION / HIGH | Installed vendor constructor still prints authenticated proxy user information; both direct/application-path synthetic captures detect it before blocked transport. |
| ASY-008 | still applies | SPEC-DEVIATION / CRITICAL | Secondary writer OSError still kills the sole worker, leaves one queued item and leaves its parent pending until explicit probe cancellation. Current-item task_done is not the missing acknowledgement. |
| ASY-009 | still applies | SPEC-DEVIATION / HIGH | Two timed-out synchronous operations remain active under one configured permit; exhaustion returns MaxRetriesExceeded before those operations complete. |
| ASY-010 | still applies | SPEC-DEVIATION / MEDIUM | Generation failure and enqueue cancellation each leave one owned worker pending until probe cleanup. |
| PIPE-012 | still applies | SPEC-DEVIATION / CRITICAL | Induced partial final CSV is still accepted by filename-only resume; no recovery task processes. |
| PIPE-013 | still applies | SPEC-DEVIATION / HIGH | No-overlap first entry still raises and prevents data-settings work for later available entry; owner D-08/D-17 wording is unchanged. |
| PIPE-014 | still applies | SPEC-DEVIATION / CRITICAL | Two distinct valid audience/filter inputs still generate one name; real sequential CSV writes preserve only the second result. |
| ASY-011 | still applies: prior rejection | No approved type/severity | Full materialization remains intentional for initial exact progress totals; the same 50-before-first-process observation supplies no breached retention contract or adverse capacity measurement. |
| PIPE-015 | still applies: prior rejection | No approved type/severity | All-null input still becomes a header-only completed dictionary; D-07 still allows strategy-shaped output, with no new contract making that valid empty transformation a failure. |

All twelve source anchors remain the same semantic symbols as in their owning validation reports. No original ID is renumbered or remapped. There are no resolved, changed or unsettled content verdicts in this check. The two rejected cases are behaviorally observed but remain excluded from actionable counts.

## Bounded measurements and provenance

The validator independently executed four existing, unmodified probes during this resumed session. Entry/configuration probes ran inside the default sandbox; native async_probe used its exact persisted approved outside-sandbox command; pipeline_probe used its existing selector-I/O-mocked harness within a bounded subprocess. All exited 0. Prior measurements remain tied to their older snapshots; the outputs below are new resume observations, not relabelled coordinator/auditor evidence.

Entry command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_probe.py`. Relevant output:

```text
missing_config_exit: 1
missing_config_init_hint: False
missing_list_exit: 1
missing_list_exception: FileNotFoundError
isolated_init_exit: 0
isolated_init_app_config_exists: True
isolated_init_log_config_exists: True
running_loop_exception: asyncio.run() cannot be called from a running event loop
running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```

Configuration command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/config_probe.py`. Relevant output:

```text
subtype_ad_filter_nonempty: True
report_explicitly_sets_ad_filter: False
merged_ad_filter: None
generated_task_ad_filter: None
proxy_secret_in_constructor_stdout: True
proxy_secret_in_get_connection_stdout: True
```

No real credential was used or displayed. The synthetic proxy capture is reported as booleans only; attempted request transport remains blocked in that probe.

Native concurrency command: `.\.venv\Scripts\python.exe -u -X utf8 .ai\tmp\audit-2026-10-21\async_probe.py`, approved outside-sandbox execution; exit 0 in 2.71 seconds. Complete stdout:

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

Pipeline command:

```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/pipeline_probe.py'],timeout=12); print('outer_process_exit:',p.returncode)"
```

Exit 0, 5.10 seconds during parallel independent probe execution. Relevant stdout:

```text
interrupted_csv_contents: 'dimension;metric\npartial;'
resume_considers_partial_complete: True
resume_tasks_processed: 0
no_overlap_exception: NoReportFoundError
multi_entry_attempts_after_no_overlap: ['outside']
distinct_audiences_generated: 2
audience_output_name_count: 1
audience_filters_distinct: True
audience_final_csv: 'dimension\n1\n'
empty_prepared_task_status: COMPLETED
empty_prepared_csv_lines: 1
outer_process_exit: 0
```

The writer fixture intentionally creates a partial file then raises; this is a publication/resume failure model, not observation of an actual OS crash. The pipeline harness mocks selector/self-pipe I/O, unlike the native concurrency probe. Neither performs a Mediascope call or establishes remote data correctness.

## Counts, qualifications and follow-up

| Owning phase | Current actionable counts |
|---|---|
| 01 | HIGH 2 |
| 02 | HIGH 2 |
| 03 | CRITICAL 1, HIGH 1, MEDIUM 1 |
| 04 | CRITICAL 2, HIGH 1 |
| Total | CRITICAL 3, HIGH 6, MEDIUM 1; ten accepted |

Two rejected claims remain rejected; resolved 0, changed 0, unsettled 0. These counts can be relied on for the inspected working bytes, not arbitrary future changes or restored historical audit reports. Existing phase-specific corrections/recommendations remain subject to the qualifications in their validated reports, including service-level missing-period continuation and bounded observable worker cleanup rather than a presumed universal infrastructure recovery policy.

This narrow check allocates no new VAL issue. Removal of the new logging tests also removes the recent marker sites involved in VAL-011; its earlier observation belongs to the older snapshot and must not be asserted as unchanged current provenance. Historical reports/remaining marker lineage were not fully re-audited here. Other restored future-phase validation findings likewise are not automatically current evidence.

## Working-source SHA-256 checkpoint

Hashes were captured after the current reproductions and compared again using independent PowerShell Get-FileHash reads. All fourteen matched; HEAD remained b27b8708d4d816940310d4f034b549cb1db6f397. This is byte-level recency evidence, not a claim that the whole index/working tree was atomically frozen.

```text
AGENTS.md 455dcf06494eb36596955d04c578e5e68334fe6841fbf0f784bfb3883be72994
src/mko_get_mediascope_data/app.py ab00f0955e8191d0f15c7da8a75054671fc449c27fa49a390e840e541f6cf337
src/mko_get_mediascope_data/core/models.py f16cfe43ef70e0d7afc56057085ee96c63784b955c5fea34c966ee910efc4892
src/mko_get_mediascope_data/core/network.py f78ac078c49dc379ba23b7d44ca047d9f27f7cb3e2df5737b8322ed7474670d6
src/mko_get_mediascope_data/core/reports.py 7438d032bc77fa2de06ea4c43069a23e90742a073457ce1eca66794c72944923
src/mko_get_mediascope_data/core/services.py 7a332abec486b7910af6e2cc0cf59a60e4ceb2e9ad2f17dfe926607eae68d9b0
src/mko_get_mediascope_data/core/tasks.py 8f0d723a4b127bd8e14f52f200808ac172470577b3fa15a715e56868d92c2f41
src/mko_get_mediascope_data/core/utils.py 5d17355d4d95f18796d407c3256c02dff8b45ae891f00179067c1d70ac68edd2
src/mko_get_mediascope_data/core/paths.py fb77f63a6b61421b7bc0c336d01f0bd38ad3c11a31a78cba723e7185749fc303
src/mko_get_mediascope_data/core/init_service.py 099002ef472f1731abda335b93fcc6e1cecc11a2383c04a2b984fb8441d8151a
src/mko_get_mediascope_data/settings/defaults/tv/crosstab.json5 18a79a570204d6500aea2af005a96135faebea15a2c6d5eddd7318416f78f5d0
src/mko_get_mediascope_data/settings/app_config.json5 3e61aba77088f2882cfd956dee77dae7d79bbf101a77390d183f2a920a302f46
docs/00-overview/open-questions.md fce76445ca59a10b4de6a34801501b552f0608f8bac58eb7f999fd739ff97d8b
.venv/Lib/site-packages/mediascope_api/core/net.py d4ed1aa9389f9502d02ae0b92ac9be32ef08e846ee39761485232c990ddc92b9
```

Final check output:

```text
head: b27b8708d4d816940310d4f034b549cb1db6f397
changed_since_capture: []
checked_files: 14
```

Ready for tech-lead use: all ten accepted content findings remain applicable, the two rejected claims remain rejected, and no new verification blocker was found.

Remaining limits: real Jupyter kernel behavior, real disk interruption, live remote abandonment/data, production capacity, production credential validity and full lint/type/test suite remain unverified. If any inspected source hashes change, recheck the affected finding paths. No permission/usage blocker remains. This addendum modified only its assigned report; existing safe probes wrote scratch artifacts only, and no Git/index operation or source/test/config/docs/plan/.kilo/prior-report edit was performed.
