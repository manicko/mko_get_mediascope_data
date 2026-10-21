---
phase: "04"
phase_name: "data-pipeline"
id_prefix: "PIPE-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/04-data-pipeline/references/workflow.md#severity-taxonomy"
code_ref: "6c9cc7ab3228c09c65d7a2140e2a2f594eb9291e"
---

# Audit Findings — Data Pipeline & Report Lifecycle

## Summary

Four findings, three HIGH and one MEDIUM. Interrupted or colliding exports can leave incomplete results that later appear finished. A missing period also prevents subsequent report entries from running, and an empty transformed result can be reported as completed.

## Scope

**Examined:** All six phase blocks: explicit/computed periods and availability intersection; strategy fan-out and resume; send/wait/extract ordering; DataFrame conversion/preparation; CSV publication/existence handling; error JSON5 location/context. Inspected reports/utils/tasks/services/models, entry wrappers, lifecycle/report tests and owner decisions in overview documentation.

**Snapshot:** Moving dirty working tree on 2026-10-21. HEAD advanced from the earlier audit baseline `4c08eb4` to the `code_ref` above before this phase; current source includes task-name logging propagation in reports/network/services. Before completion, HEAD advanced again to `ee575dc7f4f9b857039509507f59a79708b0c7cb` through documentation-only changes; `git diff` between that commit and `code_ref` showed no src/tests changes. Earlier reports remain frozen. Existing application-template edits, documentation drift and plan/report deletions are external work and preserved. Initial hashes/status remain under `.ai/tmp/audit-2026-10-21/`. Historical PIPE-001–011 are reserved; new findings start at PIPE-012.

**Tools:** Read-only PowerShell, `rg`, Git path/history inspection, existing venv Python and `.ai/tmp/audit-2026-10-21/pipeline_probe.py`. All writes are isolated under the assigned scratch directory or this report.

**Not verified:** Real API calls/remote data, a real crash or disk failure, production concurrency, full lint/type/test suite. Local probe uses real application/pandas file behavior and actual asyncio tasks with selector/self-pipe I/O mocked to avoid the known sandbox Windows socketpair issue. Cases have 2s deadlines and an outer subprocess 12s timeout. Native Windows loop behavior was verified in phase 03, but this phase's modified fixture is not claimed as a native-loop rerun. The interrupted-write fixture intentionally writes a partial file then raises; it models publication failure rather than an OS crash measurement.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| PIPE-012 | HIGH | `src/mko_get_mediascope_data/core/utils.py::csv_to_file` | Interrupted exports become resume-complete partial files |
| PIPE-013 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.run_report` | A no-overlap report aborts later entries contrary to continuation policy |
| PIPE-014 | HIGH | `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate` | Audience transliteration collisions overwrite distinct results |
| PIPE-015 | MEDIUM | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data` | Empty prepared DataFrames are exported and marked completed |

#### PIPE-012 — [HIGH] Interrupted exports become resume-complete partial files

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::Report.get_done_files`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::MediaReport.processing_tasks`
**Disambiguator:** `mode="w"`
**Checked:** ran
**Why this band:** The current path can truncate published data and suppress its recovery, comparable in effect to HIGH silent data truncation in the phase rubric. Not CRITICAL: the historical exclusive-create skip is absent, and the induced original write failure does propagate rather than itself reporting success.
**Problem:** CSV is written directly to its final filename in overwrite mode. A write failure can leave a partial replacement at that path. Resume identifies completion by filename alone and skips it without checking whether publication completed.
**Repro:** Create a valid existing scratch CSV. Invoke the real `csv_to_file` with a patched pandas writer that truncates/replaces it with a partial row then raises OSError. Construct a new report with `check_done=True` and the matching generated task. The partial file appears in `done_files`, and processing submits zero tasks.
**Impact:** Sellers can retain a truncated export after an interrupted rerun and cannot recover it through the documented resume path without manually identifying/removing the partial file.
**Root cause:** Final-path publication and the filename-only completion signal are indistinguishable during/after a failed write.
**Fix:** Make `csv_to_file` and `Report.get_done_files`' completion contract distinguish successful publication from partial writes while preserving explicit overwrites. Benefit: resume cannot accept a failed export as complete; effort small to medium; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_failed_csv_publication_is_not_resume_complete
**Related:** ASY-008 covers secondary error-artifact write failures; this finding concerns the primary CSV's published state.

**Evidence — bounded probe, exact command in Appendix A** *(supports: partial final file suppresses recovery)*:
```text
interrupted_csv_contents: 'dimension;metric\npartial;'
resume_considers_partial_complete: True
resume_tasks_processed: 0
```

#### PIPE-013 — [HIGH] A no-overlap report aborts later entries contrary to continuation policy

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period`
**Disambiguator:** `await rep.create_report()`
**Checked:** ran
**Why this band:** An expected no-data condition prevents unrelated entries from producing output, a HIGH pipeline correctness impact. Not CRITICAL: the failure is visible and not a false success or misplaced error file.
**Problem:** `get_period` raises `NoReportFoundError` when availability has no intersection. `AppService.run_report` has no per-entry recovery boundary, so the exception ends iteration. Owner decisions D-08/D-17 explicitly require missing-period warnings and continuation to other entries; the documentation's assertion that this is resolved conflicts with current execution.
**Repro:** Supply two valid report entries to the actual service: first requests January 2025, second January 2026. Fake availability is January–March 2026. The first actual report raises NoReportFoundError, and the service never requests data settings for the second entry.
**Impact:** A missing historical period in one entry prevents independent available reports later in the file from running.
**Root cause:** Expected no-data outcomes propagate across the whole multi-entry orchestration loop.
**Fix:** Align `AppService.run_report`/`TVMediaReport.get_period`'s no-overlap outcome with the recorded warning-and-continue policy. Benefit: one unavailable period cannot suppress independent exports; effort small; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_no_overlap_entry_warns_and_continues_later_entries
**Related:** Existing zero-task/no-overlap lifecycle tests expect `NoReportFoundError` for a single report; preserve meaningful diagnostics while resolving the multi-entry policy conflict.

**Evidence — same probe command** *(supports: a later available entry is never attempted)*:
```text
no_overlap_exception: NoReportFoundError
multi_entry_attempts_after_no_overlap: ['outside']
```

#### PIPE-014 — [HIGH] Audience transliteration collisions overwrite distinct results

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate`
**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `task.name += "_" + unidecode(t_name)`
**Checked:** ran
**Why this band:** Distinct valid report tasks silently collapse to one persisted result, a HIGH data-correctness impact. Not CRITICAL: no exclusive-create skip or unlocatable error output is involved; the defect is an identity collision followed by overwrite.
**Problem:** Filename identity includes the transliterated audience label but not a collision check. Distinct mapping keys such as Cyrillic `А` and Latin `A`, with different audience filters, become the same name for the same period. CSV overwrite mode then replaces one audience's result with the other's.
**Repro:** Generate one interval for the two distinct labels with different filters through the real default strategy. Two tasks are emitted with one unique output name. Write two distinct synthetic results using the real CSV helper and generated names. The final file contains only the second result.
**Impact:** Sellers can receive one output for two independently requested audiences with no filename conflict or failure signal. Re-running with resume sees the shared filename and can skip both tasks.
**Root cause:** Lossy display-name transliteration is also used as the uniqueness key for outputs and resume.
**Fix:** Ensure `DefaultTVTaskStrategy.generate` and `TVRegTaskStrategy.generate` cannot publish distinct requested tasks to the same output identity without detecting the conflict. Benefit: every requested audience retains its own result; effort small to medium; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_distinct_audience_names_cannot_collide_after_transliteration
**Related:** PIPE-012 shares filename-based resume but has a different trigger and needs separate coverage.

**Evidence — same bounded probe** *(supports: two distinct requests overwrite the same final path)*:
```text
distinct_audiences_generated: 2
audience_output_name_count: 1
audience_filters_distinct: True
audience_final_csv: 'dimension\n1\n'
```

#### PIPE-015 — [MEDIUM] Empty prepared DataFrames are exported and marked completed

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.extract_data`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DictDataStrategy.prepare`
**Disambiguator:** `df = await self.prepare_data(df, task_name=task.name)`
**Checked:** ran
**Why this band:** An empty DataFrame is treated as success, exactly the phase's MEDIUM criterion. Not HIGH: no distinct report is prevented from running and no nonempty input result is claimed lost; the observable defect is false completion for a zero-row prepared result.
**Problem:** `extract_data` checks raw conversion output for emptiness before preparation, but checks only None afterward. A strategy can validly transform nonempty raw input into zero output rows. The empty result is written and the task transitions to COMPLETED.
**Repro:** Use real `DictDataStrategy` with one configured brand slice and a nonempty raw frame containing a null brand. Preparation drops its only value. Real extraction then produces a header-only CSV and marks the task COMPLETED.
**Impact:** Sellers see successful completion and a resume-complete file even though no dictionary rows were exported. The raw empty-frame path already has a distinct no-data disposition, making outcomes inconsistent.
**Root cause:** The post-preparation result lacks the emptiness classification applied to the raw result.
**Fix:** Make `TVMediaReport.extract_data` classify zero-row prepared results consistently with the no-data contract before CSV publication and completion reporting. Benefit: completion represents exported data rather than an empty transformation; effort small; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_empty_prepared_frame_is_not_completed_or_resume_complete
**Related:** None.

**Evidence — same bounded probe** *(supports: header-only publication with completed status)*:
```text
empty_prepared_task_status: COMPLETED
empty_prepared_csv_lines: 1
```

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 3; MEDIUM 1; LOW 0.
- **Dependencies:** None are mandatory. Agree output identity/publication semantics before changing resume-related behavior.
- **Merge candidates:** PIPE-012/014/015 all affect output identity/completion, but their interrupted-write, collision and empty-result triggers require distinct verification.
- **Conflicting evidence:** PIPE-013 follows explicit owner continuation policy even though documents mark the no-overlap behavior resolved. This is a concrete code/policy contradiction, not a proposal to restore silent zero-task success.
- **Cross-phase conflicts:** CFG-011 owns omitted-client filter loss; ASY-008/010 own worker/error-persistence lifecycle; ASY-009 owns timed-out concurrency. Phase 09 owns transport retry. None is refiled here.
- **Rollout risk:** PIPE-012 must preserve intended successful `mode='w'` reruns. PIPE-013 must reconcile `tests/test_reports_lifecycle.py::test_create_report_out_of_range_date_filter_raises` and `test_create_report_zero_tasks_raises_no_report_found` with the service-level continuation contract. PIPE-014 must consider existing filename/resume compatibility. PIPE-015 must preserve ordinary successful DICT transformations and raw None/empty distinctions; existing DICT tests intentionally preserve empty-string terms, which are different from a zero-row frame.

## Appendices

### Appendix A — Exact bounded local probe command/output

```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/pipeline_probe.py'],timeout=12); print('outer_process_exit:',p.returncode)"
```
Final run (exit 0, approximately 3.3s):
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
*(supports: PIPE-012–015; other rows document scope checks rather than separate findings.)*

### Appendix B — Required block coverage and boundaries

| Block | Evidence and limits |
|---|---|
| 1: Periods | Real `get_period` with fake availability January–March 2026 clamps a December–January request to January 1–15; its source emits a partial-overlap warning. Computed last-two-months excluding current March resolves/slices to January and February. Zero generated tasks raise a distinct NoReportFoundError. Correct max/min intersection is not refiled as the historical inverted-period bug. No-overlap continuation is PIPE-013. |
| 2: Strategies | Default strategy emits period × audience combinations, preserving the interval in task settings. Regional strategy also fans out catalog regions and applies filters; no live region catalog was queried. Existing outputs are skipped in the coordinator with a warning rather than silently inside the strategy. The partial-file probe exercises actual check_done detection and skipped processing. Filename collision is PIPE-014; configured filter merge remains CFG-011. |
| 3: Ordering | `_process_item` awaits send, then checks `status is True` before wait and again before extraction. A real failed-build path returns False/False and does not attempt a missing fake status method. Poll terminal set is DONE/FAILED/CANCELLED, with failed/cancelled outcomes marked unsuccessful; configured initial/ramped sleep and count/deadline bounds prevent the historical unbounded loop. Transport timing/retry semantics are outside this phase. |
| 4: Conversion | Real conversion/extraction distinguishes raw None (failure/error artifact eligible) and empty DataFrame (no-data warning). Default preparation renames project name to targetAudience and selects configured slices/statistics; observed output shape 1×3. Default KeyError source logs the missing column and task name. Dict strategy drops null values and emits dictionary columns; its zero-row outcome exposes PIPE-015. Frequency pivot implementation inspected; remote frequency datasets not tested. |
| 5: CSV | Real writer uses `mode='w'`, semicolon separator, comma decimal, UTF-8 BOM, configured compression and no timestamp on task exports (`add_time=False`). Service combines export root with report relative_path; configured export root resolves against the user root. Existing successful names support intended resume. Direct publication and lossy filename identity expose PIPE-012/014. No exclusive-create mode-x finding is repeated. |
| 6: Errors | Actual `TVTask.to_file(root)` wrote parseable JSON5 under scratch `errors/serialized-error.json5`, including error/interval/target/settings/type/subtype/name/log_error. Names are reproducible, not timestamped. Worker persists processing exceptions/failed-send keys/None conversion when log_error is true; explicit no-data branches only warn by design. Secondary persistence failures remain ASY-008. Failure context is not written to CWD. |

Source-path gates: all cited production files were confirmed tracked with `git ls-files`; primary symbols/disambiguators were resolved in the current source. The initial optional prepared-shape fixture omitted `prj_name` and correctly returned None; that fixture was corrected before the final exit-0 probe. Its temporary AttributeError was a probe error, not a product finding. No source/test/config/documentation or `.kilo` edits were made.
