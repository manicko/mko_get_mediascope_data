---
phase: "04"
phase_name: "data-pipeline"
id_prefix: "PIPE-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/04-data-pipeline/references/workflow.md#severity-taxonomy"
code_ref: "ee575dc7f4f9b857039509507f59a79708b0c7cb"
---

# Audit Findings — Validation Report

## Summary

Three findings are substantiated: two CRITICAL and one HIGH. Partial exports can be accepted as finished, colliding audience names can erase an independently requested result, and a missing period prevents later report entries from running. The empty-dictionary claim is rejected because a valid zero-row transformation has no established requirement for a different completion outcome.

## Scope

**Input:** Frozen `.ai/audit/04-data-pipeline/findings.md`, four IDs adjudicated independently on 2026-10-21. Original code_ref 6c9cc7a and subsequent ee575dc documentation-only advance were checked against current source and the accepted snapshot-freshness addendum.
**Examined:** CSV writer and filename-only resume; period/entry orchestration; both naming strategies and target/filter inputs; extraction/DICT preparation; owner decisions D-07/D-08/D-09/D-15/D-17 and success criteria; related tests, prior phase findings/validation and isolated pipeline probe.
**Snapshot:** Dirty/moving working tree on the stated HEAD; original baseline hashes/status remain under `.ai/tmp/audit-2026-10-21/`. Pre-existing application-template/documentation/plan/audit changes were preserved. Current src/tests diff against HEAD identified only the already-known application template edit; audited implementation paths remain the current committed versions. Source hashes in snapshot-freshness identify the same reports/services/network implementation.
**Tools:** Read-only PowerShell, rg, Git inspection, AST and existing venv Python. The validator inspected and independently reran pipeline_probe.py with the existing 12-second outer subprocess bound; exit 0 in 3.1 seconds.
**Not verified:** Live API, native-loop execution of this particular harness, real OS crash/disk failure, production concurrent writes or remote data. Selector/self-pipe I/O is mocked in this harness; actual application functions, pandas transformations/files, queues and asyncio tasks run. The broken writer deliberately truncates a local final path then raises; it demonstrates the publication/resume contract under an induced partial-write failure, not a measured OS crash. No full lint/type/test suite was run.

## Findings

| ID | Final severity | Anchor | Title |
|---|---|---|---|
| PIPE-012 | CRITICAL | `src/mko_get_mediascope_data/core/utils.py::csv_to_file` | Failed publication is accepted as completed on resume |
| PIPE-014 | CRITICAL | `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate` | Distinct audience results collapse to one persisted output |
| PIPE-013 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.run_report` | Missing period prevents later available entries |
| PIPE-015 | Not assigned: rejected | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data` | Naturally empty prepared dictionary is not established as a defect |

| ID | Disposition | Original type / severity | Final type / severity |
|---|---|---|---|
| PIPE-012 | re-graded | SPEC-DEVIATION / HIGH | SPEC-DEVIATION / CRITICAL |
| PIPE-014 | re-graded | SPEC-DEVIATION / HIGH | SPEC-DEVIATION / CRITICAL |
| PIPE-013 | confirmed | SPEC-DEVIATION / HIGH | SPEC-DEVIATION / HIGH |
| PIPE-015 | not substantiated | SPEC-DEVIATION / MEDIUM | No approved type or severity |

#### PIPE-012 — [CRITICAL] Failed publication is accepted as completed on resume

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::Report.get_done_files`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `mode="w"`
**Checked:** ran
**Why this band:** The failed task's intended result is absent, yet its partial final file is treated as completed and the next report attempt submits no recovery task. This is the phase's CRITICAL false-completion/missing-output effect. The original write failure is visible, but does not prevent the subsequent false-complete resume. Grade that complete lifecycle; absence of historical mode-x is not a downgrade reason. HIGH partial period truncation is a weaker effect than suppressing the failed task's recovery as though its output were complete.
**Problem:** A final filename exists before its contents are successfully published. Filename-only resume cannot distinguish that failed publication from a completed export.
**Repro:** The inspected probe creates an existing scratch CSV, calls the real csv_to_file with an induced partial pandas write/OSError, then creates a fresh report with check_done=True and the same task name. The final contents are a partial row, done_files contains the name, and no task is processed. Appendix A preserves the repeated observation.
**Impact:** A seller's failed requested result is not reconstructed on resume; a truncated artifact substitutes for finished output without a corruption/retry signal. This is not an assertion that every write interruption is silent or every completed CSV is invalid.
**Root cause:** Publication uses the authoritative final path during mode-w writing, and completion discovery trusts existence alone. The defect is independent of ASY-008's error-artifact persistence failure.
**Fix:** In utils.csv_to_file write to a sibling temporary file on the destination filesystem, close it successfully, then atomically replace the final path. Remove only the failed temporary artifact on exception and preserve any previously published final file. Preserve configured encoding/delimiter/decimal/compression and intended overwrite semantics; keep compression inference tied to the final format rather than an arbitrary temporary suffix. Report.get_done_files must recognize final published names only, excluding temporary files.
**Verify:** new test needed: test_failed_csv_publication_is_not_resume_complete; cover failure with no prior final file, failure preserving a prior successful file, successful replacement, configured compression and check_done discovery after each outcome.
**Related:** PIPE-014 shares final identity/resume but has a separate cause; ASY-008 owns secondary error-file write failure and queue completion.
**Anchor/cause decision:** All symbols resolve; the primary token is inside csv_to_file. The helper directly passes the final path to pandas, and Report.get_done_files derives keys only from matching filenames; processing_tasks marks those names complete without reading contents. The induced failure models a reachable writer exception, not corruption introduced by a changed production helper.
**Recommendation usability:** The raw fix states the desired completion contract only. Same-filesystem temporary publication plus explicit final-only resume supplies the missing implementation approach, without requiring validation of every historical CSV on every run.

#### PIPE-014 — [CRITICAL] Distinct audience results collapse to one persisted output

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate`
**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `task.name += "_" + unidecode(t_name)`
**Checked:** ran
**Why this band:** A whole independently requested audience result disappears while both exports can complete normally; later resume treats the shared path as completion for both. This is CRITICAL false completion with a missing requested output, not merely partial truncation of one valid result. The overwrite mechanism is different from historical exclusive-create, but the phase grades the silent loss and false completion, not its mechanism name.
**Problem:** Distinct accepted audience labels/filters map to the same transliterated task/filename identity for one period. Publishing both results replaces the first rather than preserving one output per requested audience.
**Repro:** Real default strategy generates two tasks for Cyrillic А and Latin A with distinct filters. Unique name count is one. Real csv_to_file sequentially writes distinct frames under those generated names; the final CSV contains only result 1, with result 0 absent. No racing writer is needed to establish the loss.
**Impact:** Sellers requesting both audiences receive one persisted audience result without a collision diagnostic. Filename-based resume can skip both independent requests after finding that one file.
**Root cause:** Lossy display transliteration is reused as the uniqueness key for export, error identity and resume. Neither strategy nor coordinator validates name uniqueness against distinct requested tasks.
**Fix:** Introduce a deterministic output identity in both generation strategies that preserves original audience identity and regional ID, with collision detection before publication. Preserve legacy names where unambiguous; deterministically disambiguate colliding identities using original labels/region identity and verify uniqueness, rather than overwriting. Treat pre-existing ambiguous legacy names as unresolved for the affected tasks instead of evidence that both are complete. Retain the existing readable interval/audience portion; document the compatibility transition before changing resume naming.
**Verify:** new test needed: test_distinct_audience_names_cannot_collide_after_transliteration; cover both strategies, different filters, stable rerun names, Windows filename comparison and legacy ambiguous resume. Assert two distinct final results survive publication.
**Related:** PIPE-012 concerns incomplete publication, not identity; phase 16 can inspect rendered naming, but collision/overwrite ownership is phase 04.
**Anchor/cause decision:** Both strategy methods resolve and contain the transliteration paths. ReportSettings accepts a dict of distinct string labels and report.targets preserves it; no ASCII-only uniqueness requirement excludes the input. The default path is runtime-reproduced; the corresponding regional collision is statically derived for a fixed region/interval because its audience component also uses unidecode. No live regional catalog or regional runtime claim is made.
**Recommendation usability:** The raw outcome-only fix omits identity/migration decisions. The concrete collision-safe identity and compatibility policy above is actionable for planning; the implementation must retain one result per distinct requested audience without sacrificing owner D-09 resume behavior.

#### PIPE-013 — [HIGH] Missing period prevents later available entries

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period`
**Disambiguator:** `await rep.create_report()`
**Checked:** ran
**Why this band:** Expected missing data prevents an independent available report from producing output, a HIGH pipeline interruption effect under the phase's effect-based rubric. It is visible failure rather than CRITICAL false successful publication. The fact that the later report has data distinguishes this from an accepted naturally empty report.
**Problem:** Expected no-overlap NoReportFoundError escapes the entire multi-entry service loop. The second valid report entry is never attempted.
**Repro:** The inspected probe runs the real service loop with two valid entries and local availability covering only the second. The actual first report's get_period raises NoReportFoundError; the service never requests data settings for entry two. Appendix A records only ['outside'] attempted.
**Impact:** A seller's unavailable historical entry prevents later independent available exports in the same file.
**Root cause:** The service has no per-entry handling for the expected no-data outcome raised by period resolution. The class-level diagnostic itself can remain meaningful for direct single-report calls.
**Type rationale:** SPEC-DEVIATION: open-questions Owner decisions D-08 explicitly says missing periods never interrupt, no overlap warns/exports nothing/exits normally, and other reports keep running. D-17 confirms extracting as much as possible. Product success criteria require every entry attempted. These owner-recorded requirements override the inaccurate RESOLVED prose asserting the current implementation already meets them.
**Fix:** Catch the expected NoReportFoundError around each rep.create_report invocation in AppService.run_report; emit a warning identifying the report and unavailable range/reason, then continue to the next entry without claiming an exported path for the empty one. Keep direct TVMediaReport.get_period diagnostics and distinguish missing-period outcomes from unrelated exceptions rather than swallowing all errors. Define final summary as attempted/exported/warned outcomes so no-data continuation is observable.
**Verify:** new test needed: test_no_overlap_entry_warns_and_continues_later_entries. Verify first entry warns with no error artifact, second executes, final result reflects actual exports and invocation does not stop. Preserve direct single-report out-of-range/zero-task diagnostics unless deliberately changing their public contract.
**Related:** ASY-010 owns worker cleanup if a report exits after starting workers; this period failure occurs before worker generation. No duplication of phase 02 values or entry startup diagnostics.
**Anchor/cause decision:** Both methods and the exact call token resolve. Source uses max/min intersection and raises for start>end; the service awaits create_report without local handling. The probe patches only availability/network behavior and report input acquisition, not this exception path.
**Recommendation usability:** Stable service target, scoped exception boundary and observable continuation supplied. No need to convert all no-data results to silent success or invent a universal retry policy.

#### PIPE-015 — [not substantiated] Naturally empty prepared dictionary is not established as a defect

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DictDataStrategy.prepare`
**Disambiguator:** `df = await self.prepare_data(df, task_name=task.name)`
**Checked:** ran
**Original band:** MEDIUM; no approved type/severity.
**Problem adjudication:** The observed header-only CSV and COMPLETED status are accurate. The input raw DataFrame is nonempty; DICT intentionally drops null dictionary values and can legitimately return a zero-row dictionary. No requirement establishes that this valid transformation must become a distinct unsuccessful/EMPTY outcome.
**Repro:** The independent probe passes one brand row containing None through the real DictDataStrategy and extraction. Preparation returns the configured dictionary columns with no rows; extraction writes one header line and marks the task COMPLETED. The harness observation is reproduced, not disputed.
**Impact adjudication:** No requested valid dictionary term is shown lost or invalid. The strategy intentionally removes the only null value. A valid empty dictionary file is a possible completed transformation, and the owner permits strategies to shape output (D-07). Naturally empty contents do not by themselves establish false completion.
**Root cause adjudication:** Missing post-preparation df.empty checking explains the branch, but is not an independently established defect. Raw-empty API conversion has a different no-data path because it has no input data to prepare; that contrast does not establish that a successful strategy transformation is invalid.
**Type/severity decision:** Rejected as not substantiated. Phase 04's MEDIUM empty-frame criterion grades an established wrong success classification; it does not supply a missing business requirement that every empty dictionary is a failure. No EMPTY state exists in TaskStatus, and more importantly no owner decision or strategy output contract requires an EMPTY-versus-COMPLETED distinction for naturally exhausted dictionary terms. D-08/D-15 concern unavailable/missing source data, not this demonstrated successful nonempty-input transformation.
**Fix:** No corrective change approved. Preserve dictionary dropna/layout behavior unless the owner defines zero-term dictionary publication semantics. Any future requirement can choose warning/no-file versus a valid empty dictionary, but that unresolved product choice is not a confirmed defect now.
**Verify:** No corrective regression test required for this rejected claim. Existing DICT transformation tests document null removal and empty-string retention; neither proves a failure contract for an all-null prepared frame.
**Related:** Phase 16 may measure output conformance against a defined dictionary contract; phase 17 owns arithmetic. Neither receives a transferred confirmed finding from this rejected claim.
**Anchor/cause decision:** Both symbols, preparation call and null-dropping transformation resolve. The runtime observation is sufficient to decide behavior, but insufficient to approve the asserted wrong-result claim. This is rejection for lack of a required contract/consequence, not an unsettled environment result.
**Recommendation usability:** The raw fix presumes that completion must mean nonzero rows. Applying it would change valid empty-output behavior without an established requirement; it is withheld rather than made vaguely advisory.

## Fix Order

- **Approved content distribution:** CRITICAL 2; HIGH 1; MEDIUM 0; LOW 0. Rejected PIPE-015 has no final severity and is excluded.
- **Disposition tally:** confirmed 1; re-graded 2; re-typed 0; merged 0; not substantiated 1; unsettled 0. Four dispositions for four original IDs.
- **Dependencies:** Plan publication and collision-safe resume identity together, keeping separate failure cases. PIPE-013 is independent and preserves direct period diagnostics while adding service-level continuation.
- **Merge candidates:** None. PIPE-012 is incomplete publication; PIPE-014 is identity collapse; PIPE-013 is multi-entry outcome propagation. Shared final filenames are an adjacent concern, not one root cause.
- **Conflicting evidence:** Owner D-08 says continue, while RESOLVED prose and current code abort on no-overlap. Direct lifecycle tests correctly assert NoReportFoundError at the report boundary and need not be deleted merely to implement service continuation. Empty dictionary construction has no defined wrong-success contract, so PIPE-015 is rejected.
- **Cross-phase conflicts:** CFG-011 retains lost-filter ownership. ASY-008/009/010 retain worker/persistence/timeout ownership; primary CSV publication is PIPE-012. Phase 16 rendered conformance and phase 17 arithmetic are not refiled here.
- **ASY-008 recommendation qualification:** D-08 specifically requires missing-period continuation. D-09/D-17 broadly favor continuing useful work after individual failure, but do not define recovery from every broken persistence/infrastructure condition. The earlier phase 03 recommendation's fail-report supervision must be read as a bounded observable completion/cleanup mechanism, not a universal instruction to abort the whole invocation or to keep dispatching when no safe progress is possible. Choose containment/recovery versus a terminal report outcome against those owner goals; do not claim a documented mandatory disk-recovery mechanism. ASY-008's stranded-join finding/grade remains valid, and the frozen report is not edited here.
- **Rollout risk:** PIPE-012 must retain successful explicit overwrite/compression conventions. PIPE-014 must preserve deterministic identities and avoid trusting ambiguous legacy resume names. PIPE-013 must preserve no-data warnings/no-error-artifact policy and later-entry progress. Do not classify every naturally empty dictionary as failure to satisfy the rejected finding.

## Validation-Level Findings

Existing systemic VAL-007/008 (template/namespace) and recommendation-usability pattern VAL-009 remain references, not duplicates. Two distinct issues are newly recorded; their scale below is phase 99's audit scale.

| ID | Severity | Anchor | Title |
|---|---|---|---|
| VAL-010 | HIGH | `.ai/audit/04-data-pipeline/findings.md::"#### PIPE-012"` | False completion and missing-output effects downgraded by mechanism |
| VAL-011 | MEDIUM | `tests/test_network.py::test_call_logs_task_name_in_errors` | PIPE-010 provenance is reused for unrelated logging work |

#### VAL-010 — [HIGH] False completion and missing-output effects downgraded by mechanism

**Anchor:** `.ai/audit/04-data-pipeline/findings.md::"#### PIPE-012"`
**Anchor:** `.ai/audit/04-data-pipeline/findings.md::"#### PIPE-014"`
**Disambiguator:** `historical exclusive-create skip is absent`
**Checked:** static
**Why this band:** Phase 99 HIGH covers true findings graded below their own rubric, suppressing their roadmap priority.
**Problem:** Both raw band rationales exclude CRITICAL partly because exclusive-create is absent, despite the demonstrated lifecycle producing false completion without the requested result preserved. PIPE-012's visible initial error does not prevent its later false-complete resume; PIPE-014 loses a whole distinct audience result without an export error.
**Impact:** Both actionable data-loss paths enter the roadmap below the audited phase's false-success/missing-output severity.
**Root cause:** Severity reasoning treats the historical mechanism as necessary rather than comparing present effect/blast radius.
**Fix:** Use the two re-graded CRITICAL dispositions and effect-based rationale in this validated report; leave the raw input frozen. Compare the whole publication/resume lifecycle when planning remediation.
**Verify:** new check needed: preserve critical priority for both false-completion paths and verify separate interrupted-publication/colliding-identity cases.
**Related:** PIPE-012, PIPE-014. VAL-006 is an earlier phase 01 grading correction, not this phase's mechanism-based downgrade.

#### VAL-011 — [MEDIUM] PIPE-010 provenance is reused for unrelated logging work

**Anchor:** `tests/test_network.py::test_call_logs_task_name_in_errors`
**Anchor:** `tests/test_network.py::test_call_omits_task_context_when_name_is_none`
**Anchor:** `tests/test_reports_lifecycle.py::test_worker_will_be_skipped_warning_includes_error_reason`
**Anchor:** `tests/test_tasks.py::test_to_file_raises_on_none_folder`
**Disambiguator:** `task_name is included in log messages when provided (PIPE-010)`
**Checked:** static
**Why this band:** Phase 99 MEDIUM includes an identifier resolving to multiple findings. This is a current shipped provenance ambiguity, distinct from the historical shared VAL namespace recorded as VAL-008.
**Problem:** Historical phase 04 PIPE-010 concerns error-serialization destination/None-folder behavior, and test_tasks still uses that meaning. Recent network and lifecycle tests use PIPE-010 for task-context logging and warning reason, an unrelated subject.
**Impact:** A maintainer following current regression-test provenance cannot resolve PIPE-010 to one issue; fixes and historical evidence can be attributed to the wrong change.
**Root cause:** New work reused an already minted content identifier instead of qualifying its own task/issue provenance.
**Fix:** In separately authorized provenance maintenance, qualify the new logging tests with their actual task/plan or issue source and retire their ambiguous PIPE-010 attribution. Preserve historical PIPE-010 and existing error-serialization lineage; do not renumber historical findings or reuse PIPE-012..015. This audit does not edit tests or plans.
**Verify:** new check needed: resolve every PIPE-010 marker to one documented historical subject, with new logging work independently identified.
**Related:** VAL-008 covers different VAL identifier reuse, and is not refiled here.

## Appendices

### Appendix A — Independently repeated bounded probe

Exact command:

```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/pipeline_probe.py'],timeout=12); print('outer_process_exit:',p.returncode)"
```

Exit 0, 3.1 seconds; full stdout:

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
clamped_explicit_period: [(datetime.date(2026, 1, 1), datetime.date(2026, 1, 15))]
computed_sliced_period: [('2026-01-01', '2026-01-31'), ('2026-02-01', '2026-02-28')]
zero_tasks_exception: NoReportFoundError
failed_build_status_and_error_flag: False False
error_serialized_keys: ['error', 'interval', 'log_error', 'name', 'report_subtype', 'report_type', 'settings', 'target']
raw_frame_disposition: none False True
raw_frame_disposition: empty False False
default_prepared_shape_columns: (1, 3) ['targetAudience', 'dimension', 'metric']
outer_process_exit: 0
```

The first three scenarios substantiate publication, continuation and collision findings. The fourth demonstrates behavior but does not establish that an empty dictionary is invalid. Remaining lines describe bounded controls; none is a PASS-based verdict. Selector I/O mocks affect loop plumbing, not pandas/file behavior or the inspected source branches; native-loop performance, transport and OS-crash behavior remain unclaimed.

### Appendix B — Support, ownership and namespace rulings

- **Owner decisions independently checked:** open-questions Owner decisions D-08 explicitly requires missing-period continuation; D-17 requires attempting as much as possible without invocation abort, and D-09 makes resume the recovery path. Product success criteria say one file per served interval/audience and every entry attempted. D-07 permits report strategies to shape dictionary output; D-15 reserves error files for rejection/processing failure and warns for missing source periods. The RESOLVED paragraphs contradict actual no-overlap orchestration and are not approval evidence.
- **Control existence/decision scope:** The two direct out-of-range/zero-task lifecycle tests exist and assert exceptions at create_report; they do not test a later entry in AppService.run_report. Their deciding assertion count for service continuation is zero. The DICT test tests null dropping/duplicates/empty strings with surviving terms; it does not assert all-null output is failure. No existing test was run or relied on as green approval in this validation.
- **Checklist dependence:** PIPE-012 maps to export safety/partial files, PIPE-013 to periods/zero-task report behavior, PIPE-014 to generation and filenames, and PIPE-015 to conversion/empty-result questions. All four observations are independent runtime measurements within the broad phase angles. The list supplies no missing natural-empty-dictionary failure contract; rejected PIPE-015 is not rescued by matching a checklist phrase. No approved finding requires a previously unnamed angle.
- **Cross-phase review:** Compared current phase 01/02/03 raw and validated reports plus snapshot-freshness. Publication/collision effects are phase 04; source data overlay stays CFG-011; local worker lifetimes and timed-out operation concurrency stay ASY-008/009/010. There is no merge of independent causes or silent amendment of earlier reports. The ASY-008 recommendation qualification appears explicitly in Fix Order.
- **Declared/history:** Phase 04 declares PIPE-. Historical report at HEAD minted PIPE-001..011; current PIPE-012..015 continue the same namespace and preserve original IDs. Historical PIPE-001's mode-x implementation is obsolete; current PIPE-012/014 are different causes with independently established current effects. No phase-qualified compound PIPE namespace is introduced.
- **Shipped markers/reuse ruling:** Source/tests/docs contain historical PIPE references in reports.py strategy traceability, report/network/task/lifecycle tests and overview documents. Preserve their load-bearing historical provenance with report/commit qualification when needed. The specific PIPE-010 reuse now spans error serialization and recent logging tests; VAL-011 records the ambiguity and a migration ruling rather than merely counting it. No other current content phase reuses PIPE. Existing VAL-007/008 remain referenced; new VAL-010/011 follow the family maximum without reuse.
- **Controlled template:** The template derives prefixes from workflows and contains no closed prefix list. Its malformed generic severity path is already VAL-007. Phase 04 raw has exact severity_taxonomy once, missing 0, renamed 0, pointing to its real workflow. This report also uses the real workflow pointer and honest current dirty scope. All raw per-finding fields are retained with explicit dispositions, including rejection; separate content and validation tables use their own severity scales.
- **Completeness/limits:** No finding is unsettled. The default collision is runtime-reproduced; regional naming is a static extension with an explicit local-region assumption. No remote result or real filesystem outage is claimed. Empty dictionary observation is decided but its wrong-completion allegation is unsubstantiated, not confirmed in a narrowed vague form.

### Appendix C — Mechanical consistency and acceptance

Existing venv Python AST resolved all nine raw production symbols. Each primary token occurs inside its resolved symbol; tracked paths were checked with git ls-files. Final report-text check (exit 0):

```text
dispositions: {'re-graded': 2, 'confirmed': 1, 'not substantiated': 1}
unique_input_ids: ['PIPE-012', 'PIPE-013', 'PIPE-014', 'PIPE-015']
approved_headers: [('PIPE-012', 'CRITICAL'), ('PIPE-014', 'CRITICAL'), ('PIPE-013', 'HIGH')]
rejected_headers: ['PIPE-015']
new_VAL_headers: [('VAL-010', 'HIGH'), ('VAL-011', 'MEDIUM')]
VAL010_token: True
VAL011_token: True
line_locators: []
```

Final HEAD check remained ee575dc7f4f9b857039509507f59a79708b0c7cb. Reports/services SHA-256 values remained the snapshot-freshness values:

```text
reports.py 2eb1175c05826921e5254cd99ca0287a40d1711b8f5f97a37320323982022ca3
services.py e64972bb7a7a868bb8915dc2174d671682682bc5c859312ee6bc7d613132866c
```

Ready for tech-lead acceptance with three approved findings (two CRITICAL, one HIGH) and PIPE-015 rejected. New audit-maintenance issues are VAL-010 HIGH and VAL-011 MEDIUM. Raw recommendations must be interpreted through the concrete publication, identity and continuation mechanisms above; earlier frozen reports remain unchanged. This validation edited only its assigned report and ran scratch-only probes; it changed no application/test/config/docs/plan/.kilo/input/shared-template files.
