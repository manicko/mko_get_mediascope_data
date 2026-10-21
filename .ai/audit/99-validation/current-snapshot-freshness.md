---
report_status: "validated"
mode: "problems-only"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Current Snapshot Freshness — Validation Addendum

All 15 accepted content findings still apply: CRITICAL 3, HIGH 6, MEDIUM 4, LOW 2. ASY-011 and PIPE-015 remain rejected. No accepted finding is resolved or unsettled by the latest source/test drift.

## Change impact and evidence scope

This is a narrow validator recency check of accepted phases 01–04/09/10, not another full audit. Existing raw/validated reports, earlier freshness addenda and original IDs are preserved. HEAD remains b27b870, but staged AGENTS/application-settings changes mean it is not an atomic identifier of working bytes. No Git/index mutation was performed.

Compared all 28 stored phase10-tool-input-hashes: exactly five Python inputs differ—services.py, reports.py, network.py, test_network.py and test_reports_lifecycle.py. Against the 14-file resume fingerprint, only the three core source files differ. Working core files restore the task_name logging/context parameter/calls and skip-warning error reason; they match the earlier task-context source hashes in snapshot-freshness.md. Source diff against current HEAD is empty for Python files; that does not make the dirty policy/config tree identical to HEAD.

The added logging tests and source context do not move bootstrap boundaries, change omitted-field merge, alter vendor sessions, supervise worker failure, extend cleanup, change timeout/permit ownership, atomically publish CSV, catch no-overlap outcomes or change task identities. Relevant current methods were inspected; bounded concurrency/integration/pipeline probes were repeated because those core files changed. Entry/configuration/package findings use unchanged own-path bytes plus the isolated changed-method comparison and earlier measurements, not falsely claimed fresh reruns.

The parent completed the native phase-11 suite (reported 165 passed, one warning, 87.27% coverage). This validator did not start a second suite and does not use a suite PASS as evidence against the specific accepted defects. Source-drift qualification of that run remains separate.

## Per-ID current applicability

| ID | Current verdict | Final type / severity | Re-derivation for current bytes |
|---|---|---|---|
| ENT-008 | still applies | DOC-UPDATE / HIGH | app.py unchanged; same notebook prose and unconditional asyncio.run, with no added active-loop route selection. Earlier guard observation remains applicable, not relabelled as a fresh run. |
| ENT-009 | still applies | SPEC-DEVIATION / HIGH | Entry bootstrap still precedes run's try and list_reports has no recovery boundary; service delta only adds task_name=None to connection dispatch. |
| CFG-011 | still applies | SPEC-DEVIATION / HIGH | merge_data_settings/ReportSettings/default filter unchanged; synthesized omitted None still replaces the selected restriction. |
| CFG-012 | still applies | SPEC-DEVIATION / HIGH | get_connection and installed proxy print unchanged; task_name is consumed as wrapper context and does not sanitize vendor stdout. |
| ASY-008 | still applies | SPEC-DEVIATION / CRITICAL | Native repeat: worker OSError, queued=1, parent pending. New warning reason does not contain a secondary writer failure or supervise a dead worker. |
| ASY-009 | still applies | SPEC-DEVIATION / HIGH | Native repeat: active synchronous peak=2 with permits=1, two active at exhaustion; task context does not alter permit lifetime. |
| ASY-010 | still applies | SPEC-DEVIATION / MEDIUM | Native repeat leaves one pending worker after both generation failure and enqueue cancellation; cleanup still begins only around join. |
| PIPE-012 | still applies | SPEC-DEVIATION / CRITICAL | Pipeline repeat: partial final CSV is in done_files, and zero recovery tasks process. CSV helper/resume discovery are unchanged. |
| PIPE-013 | still applies | SPEC-DEVIATION / HIGH | Pipeline repeat attempts only outside entry; no-overlap still escapes before the later available entry. Owner D-08/D-17 are unchanged. |
| PIPE-014 | still applies | SPEC-DEVIATION / CRITICAL | Pipeline repeat: two distinct audiences/filters produce one name and only second CSV result survives. Name construction is unchanged. |
| EXT-009 | still applies | DOC-UPDATE / LOW | Model/send comments still promise pre-commit-only selection; repeat shows ReadTimeout replay eligibility. Non-deduplicating simulator still gives two commits/one key; actual Mediascope duplication remains unverified and unapproved. |
| EXT-010 | still applies | BEST-PRACTICE / MEDIUM | Integration repeat: four allocated session doubles, no explicit closes; failure close count also zero. GC fallback remains; no permanent leakage claim. |
| EXT-011 | still applies | SPEC-DEVIATION / MEDIUM | Integration repeat: constructor ConnectTimeout once vs ordinary three attempts; api_call=False remains on the I/O-performing constructor path despite added task context. |
| QLT-021 | still applies | SPEC-DEVIATION / MEDIUM | Package getter/app signature/typing configuration unchanged; no TYPE_CHECKING public-name declarations added. Explicit-src typed-vs-Any control remains applicable. |
| QLT-022 | still applies | DOC-UPDATE / LOW | Unchanged getter docstring still overstates single-name deferral; first-name importlib load is unchanged. |
| ASY-011 | still applies: rejection | No approved type/severity | Repeat still generates all 50 tasks before first processing; no new bounded-retention contract or measured adverse cost establishes a defect. |
| PIPE-015 | still applies: rejection | No approved type/severity | Repeat still writes a header-only completed dictionary; no new contract forbids a valid empty transformation. |

No accepted row is silently retained because it appeared in an older report. Changed source paths were tested directly; unaffected paths were checked by byte/semantic stability. The two rejected observations remain behaviorally true but have no established defect contract.

## Current bounded observations

Validator reran the unchanged native async_probe using its exact approved outside-sandbox prefix (exit 0, 4.28s). Validator also reran unchanged external_probe.py and pipeline_probe.py with their existing 12s outer subprocess bound (both exit 0, approximately 6.3/6.4s while parallel independent probes ran). No real API or credential was used; synthetic Session transport remains blocked. Integration/pipeline selector-I/O mocks and induced partial writer remain explicit harness limits.

Relevant native concurrency stdout:

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
thread_timeout_final_exception: MaxRetriesExceeded
thread_timeout_peak_active: 2
thread_timeout_semaphore_capacity: 1
thread_timeout_still_active_after_call: 2
```

Relevant integration/pipeline stdout:

```text
default_send_readtimeout_eligible: True
simulated_committed_writes_after_lost_response: 2
application_retained_keys: 1
constructor_catalog_attempts: 1
ordinary_catalog_attempts: 3
constructor_failed_sessions_closed: 0
successful_two_entries_sessions_created: 4
successful_two_entries_sessions_closed: 0
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
```

## Source hash stability

Final repeated hash reads checked 17 files; changes since capture: ["src/mko_get_mediascope_data/settings/app_config.json5"]. HEAD remained b27b8708d4d816940310d4f034b549cb1db6f397. SHA-256 checkpoint:

```text
.venv/Lib/site-packages/mediascope_api/core/net.py d4ed1aa9389f9502d02ae0b92ac9be32ef08e846ee39761485232c990ddc92b9
AGENTS.md 455dcf06494eb36596955d04c578e5e68334fe6841fbf0f784bfb3883be72994
docs/00-overview/open-questions.md fce76445ca59a10b4de6a34801501b552f0608f8bac58eb7f999fd739ff97d8b
src/mko_get_mediascope_data/__init__.py 44b5ac8cb5fb379b8b6114e547f4b9dc39ae7ef79abd9b5521fb11642045b079
src/mko_get_mediascope_data/app.py ab00f0955e8191d0f15c7da8a75054671fc449c27fa49a390e840e541f6cf337
src/mko_get_mediascope_data/core/init_service.py 099002ef472f1731abda335b93fcc6e1cecc11a2383c04a2b984fb8441d8151a
src/mko_get_mediascope_data/core/models.py f16cfe43ef70e0d7afc56057085ee96c63784b955c5fea34c966ee910efc4892
src/mko_get_mediascope_data/core/network.py 7dc8183c7b21e1a24d0cf608c05aad7168a2be865391bf5fa9d904263e2ff7d2
src/mko_get_mediascope_data/core/paths.py fb77f63a6b61421b7bc0c336d01f0bd38ad3c11a31a78cba723e7185749fc303
src/mko_get_mediascope_data/core/reports.py 2eb1175c05826921e5254cd99ca0287a40d1711b8f5f97a37320323982022ca3
src/mko_get_mediascope_data/core/services.py e64972bb7a7a868bb8915dc2174d671682682bc5c859312ee6bc7d613132866c
src/mko_get_mediascope_data/core/tasks.py 8f0d723a4b127bd8e14f52f200808ac172470577b3fa15a715e56868d92c2f41
src/mko_get_mediascope_data/core/utils.py 5d17355d4d95f18796d407c3256c02dff8b45ae891f00179067c1d70ac68edd2
src/mko_get_mediascope_data/settings/app_config.json5 3e61aba77088f2882cfd956dee77dae7d79bbf101a77390d183f2a920a302f46
src/mko_get_mediascope_data/settings/defaults/tv/crosstab.json5 18a79a570204d6500aea2af005a96135faebea15a2c6d5eddd7318416f78f5d0
tests/test_network.py 7b4dc05b29255a485bc8115df7f3177098d5fdcf0196ef02bf80e3017641fdd7
tests/test_reports_lifecycle.py 44ce3f2e7a52e945d44e083a3dd49da14656926e18ffe5548bfb2de881c10b55
```

The final stability check found concurrent drift in app_config.json5 only; the other 16 files matched. The checkpoint above records the earlier captured config bytes, not an assertion that configuration stayed frozen. A final scoped policy inspection read:

```text
app_config_sha256: 395452e7b78e3842aa451d89d689729aadc4d5a8f6bbd14dea658fa0344589c2
max_concurrent_requests: 5
max_attempts: 10
call_timeout: 300.0
send_task.retryable: True
send_task.retry_on_connection_error: True
```

These current policy values do not eliminate any accepted path. No source/hash change affected the measured implementation; bounded probes use explicit valid configurations and their own isolated destinations. The workspace remains moving, so this addendum establishes applicability for the recorded source bytes/current inspected controls rather than an atomic configuration snapshot.

## Existing validation-artifact issue — separate scale

| ID | Current artifact status | Phase-99 severity | Verified evidence |
|---|---|---|---|
| VAL-011 | current again | MEDIUM | Restored network/lifecycle logging tests reuse PIPE-010 while task serialization tests retain its historical different subject. |

Focused rg/source verification found PIPE-010 in test_call_logs_task_name_in_errors and test_call_omits_task_context_when_name_is_none, plus test_worker_will_be_skipped_warning_includes_error_reason. tests/test_tasks.py still labels test_to_file_raises_on_none_folder and test_to_file_writes_valid_json5 as PIPE-010. This is the same existing identifier ambiguity recorded in phase-04 validation, not a new VAL allocation. The resume addendum's historical-only status applied when those logging markers were absent; it must not be carried forward to these restored bytes. No marker, test, source or original finding was edited here.

## Final counts and limits

Accepted/actionable content: 15 — CRITICAL 3, HIGH 6, MEDIUM 4, LOW 2. Rejected content: ASY-011 and PIPE-015. Resolved 0; changed finding dispositions 0; unsettled 0. Current applicability of existing VAL-011 is separate from those content totals. No new content/VAL ID was minted.

Earlier frozen reports remain historical measurements of their original snapshots, augmented by this explicit current applicability check. User-authorized stale audit cleanup did not turn old _done/future-phase findings into current results; they remain historical Git/scratch evidence and contribute no actionable content rows here. No accepted finding is excluded as resolved, because none was repaired by the inspected deltas.

Remaining limits: real Mediascope idempotence/duplicate effects, live sockets/GC duration, real Jupyter, real disk interruption, production capacity and unobserved future source/config edits. The conditional simulator and Session doubles remain narrowly interpreted; EXT-009 stays LOW DOC-UPDATE, EXT-010 stays MEDIUM BEST-PRACTICE. No verification or approval blocker arose. Only this assigned addendum and existing scratch probe outputs were written; raw/validated prior reports, production files, docs, configuration, .kilo and index were preserved.
