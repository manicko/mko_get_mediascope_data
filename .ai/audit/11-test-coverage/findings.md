---
phase: "11"
phase_name: "test-coverage"
id_prefix: "TST-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/11-test-coverage/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Test Coverage

## Summary

Three findings: two HIGH assurance gaps and one MEDIUM red gate. All 165 tests pass, but key test doubles accept an invalid integration handoff, and the synchronous-wrapper test accepts scheduling the wrong object. The declared type gate also fails on a test-double/type mismatch.

## Scope

**Examined:** All eight blocks: assertion discrimination, doubles, fixture contracts, both boundary sides, harness/isolation, reproducibility, loaded merge gates/coverage scope, and test-layer self-description. Tests, fixtures, relevant installed vendor methods, project pytest/coverage/mypy/CI configuration and prior accepted defect ownership were inspected.

**Snapshot:** Current dirty/staged b27b870 working tree on 2026-10-21, not pristine HEAD or a guaranteed atomic snapshot throughout the suite. Five Python inputs differed from phase10 hashes, so Ruff/mypy were rerun. Current input digests and gate outputs are in `.ai/tmp/audit-2026-10-21/phase11-gate-input-hashes.json` and `phase11-gate-results.md`. Restored historical artifacts/index were preserved before audit cleanup by the coordinator; TST-000–012 are reserved, including prior collision provenance. Higher-ID searches found no collision for TST-013–015. This fresh report supersedes historical conclusions, not prior accepted phase reports.

**Runtime:** One coordinator-brokered approved native full run via existing venv: 165 passed, 1 warning, statement coverage 87.27% against the configured 70% gate. Outer wall time 92.144s, pytest 88.74s, outer timeout180s. Default slow tests/project addopts remained active; temp/cache/coverage/output moved into scratch and actual Session transport was prohibited. No dependency sync/install. Exact artifacts: `phase11-pytest-output.txt`, `phase11-pytest-result.json`, `phase11-coverage.json` in the same scratch directory.

**Limits:** No live API or real user configuration mutations, randomized/parallel-order experiment, CI-host execution or full-suite mutation campaign. Focused fidelity/counterexample probes do not claim the shipped suite currently fails: it passes. A post-summary unawaited-AsyncMock RuntimeWarning is preserved without claiming a unique originating test from stderr alone. Native execution was coordinator-brokered; the auditor inspected its exact saved output.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TST-013 | HIGH | `tests/test_reports_lifecycle.py::test_create_report_writes_csv_and_marks_queue_done` | Lifecycle doubles admit a task key rejected by the installed vendor consumer |
| TST-014 | HIGH | `tests/test_app.py::test_run_report_sync_wrapper` | Wrapper assertions accept scheduling a different non-awaitable object |
| TST-015 | MEDIUM | `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter` | Test-double type mismatch keeps the declared mypy gate red |

#### TST-013 — [HIGH] Lifecycle doubles admit a task key rejected by the installed vendor consumer

**Anchor:** `tests/test_reports_lifecycle.py::test_create_report_writes_csv_and_marks_queue_done`
**Anchor:** `tests/test_services.py::test_run_report_with_real_models`
**Disambiguator:** `mock_report_service.send_task.return_value = "mock_task_key_123"`
**Checked:** ran
**Why this band:** Both sides of an integration handoff are replaced so the real shared-value contract cannot be observed, exactly HIGH boundary/double assurance. Not CRITICAL: no actual current product wrong task key is established; this is a test fidelity gap.
**Problem:** Lifecycle/service doubles return a string task key and their independently mocked get_status/get_result accept it. The installed vendor get_status consumes a mapping via `.get('taskId')`; it rejects the same string before transport. Eighteen send_task string-return fixture sites were found across three test files.
**Repro:** The full shipped suite passes. The isolated fidelity probe feeds its representative string key to the matching status double (DONE) and the actual installed unbound vendor get_status with a transport spy (AttributeError before any request). The two boundary sides disagree on what constitutes a valid handoff.
**Impact:** Tests described as real pipeline coverage do not detect malformed task-key propagation at the submission→poll/result boundary. A key-shape regression can remain green through these fixtures despite the real consumer rejecting it.
**Root cause:** Independently configured collaborator doubles bypass the value contract between producer and consumer.
**Fix:** Make lifecycle/service fixture keys representative of the installed consumer contract and assert the observable handoff across a contract-faithful boundary. Benefit: integration-shape regressions are detected without real API calls; effort small; advisory priority high. Do not distort production behavior to accept the fixture string. [SPEC-DEVIATION]
**Verify:** new test needed: test_submission_key_handoff_matches_installed_vendor_consumer
**Related:** Existing integration/product findings remain owned by their phases; this finding only concerns the test assurance boundary.

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/phase11_fidelity.py`** *(supports: double accepts a key rejected by the actual local consumer)*:
```text
lifecycle_double_accepts_string_key: DONE
installed_vendor_same_key_exception: AttributeError
vendor_transport_reached: False
send_task_string_return_fixture_sites: 18
send_task_string_return_fixture_files: ['tests\\test_reports_lifecycle.py', 'tests\\test_reports_progress.py', 'tests\\test_services.py']
```

#### TST-014 — [HIGH] Wrapper assertions accept scheduling a different non-awaitable object

**Anchor:** `tests/test_app.py::test_run_report_sync_wrapper`
**Disambiguator:** `mock_run.assert_called_once()`
**Checked:** ran
**Why this band:** A test claiming scheduling/delegation coverage replaces the scheduler and never observes which work it receives: HIGH stand-in/unchecked decision. Not CRITICAL: current production passes the correct coroutine; the wrong scheduling is a scratch-local counterexample, not a shipped behavior claim.
**Problem:** The wrapper test checks that the async API and asyncio.run were each called, but not that the scheduler receives/executes the requested report work. Its scheduler double accepts any argument.
**Repro:** Invoke the actual shipped test function with its equivalent isolated patch helper, replacing only the wrapper in memory. The counterexample calls the expected async API but hands asyncio.run a separate plain object. All shipped assertions pass, while the intended async API await count is zero. Created coroutine is explicitly closed and all monkeypatches are restored.
**Impact:** The test would stay green if the synchronous library entry stopped driving the requested export while retaining the two call counts.
**Root cause:** Call-count assertions substitute for observing the scheduling relationship and execution of intended work.
**Fix:** Make `test_run_report_sync_wrapper` discriminate whether the synchronous entry actually drives the requested async report operation. Benefit: losing or misrouting requested work breaks the test; effort small; advisory priority high. Keep the test hermetic and do not merely assert incidental source text. [SPEC-DEVIATION]
**Verify:** new test needed: test_sync_wrapper_drives_requested_async_report_work
**Related:** ENT-008 covers notebook entry contract; QLT-021 covers public static types; neither is refiled.

**Evidence — same focused probe** *(supports: shipped assertions survive wrong scheduled work)*:
```text
shipped_wrapper_test_accepts_wrong_nonawaitable: True
mutant_scheduler_argument_awaitable: False
intended_async_pipeline_await_count: 0
```

#### TST-015 — [MEDIUM] Test-double type mismatch keeps the declared mypy gate red

**Anchor:** `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter`
**Anchor:** `.github/workflows/ci.yml::"uv run mypy src tests"`
**Disambiguator:** `result = await client.call(lambda: None, api_call=False)`
**Checked:** ran
**Why this band:** A loaded type gate is red for one bounded test-tree inconsistency, a MEDIUM operability gap. It is not a production diagnostic population or a missing gate; no strictness beyond current configuration is assumed.
**Problem:** The generic client call receives a callable whose result is None, while the test replaces `_invoke` to return a string sentinel and consumes that result. The declared src/tests mypy command rejects this inconsistent subject/double pairing; pytest still passes because the mocked path does not obey the supplied callable's result type.
**Repro:** After changed input hashes prevented phase10 reuse, rerun existing-venv mypy with the same src/tests scope and only its cache redirected to scratch. It reports func-returns-value at the call in this test and exits 1. Ruff and the full pytest suite remain green.
**Impact:** Developers and CI cannot obtain a green declared type gate until the test's type contract is corrected or bypassed. This is a test-layer issue, not evidence that production should change its generic return behavior.
**Root cause:** The test's invoked callable and mocked return describe different result contracts.
**Fix:** Align `test_cpu_local_calls_bypass_semaphore_and_jitter`'s callable/double result types while retaining assertions that the local path bypasses sleep/semaphore/retries. Benefit: the declared gate can pass without suppressing a valid inconsistency or weakening production types; effort trivial; advisory priority medium. [SPEC-DEVIATION]
**Verify:** `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter` plus the configured `mypy src tests` gate
**Related:** Phase10 recorded the test-only diagnostic for this phase rather than treating diagnostic count as a production finding.

**Evidence — `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src tests`** *(supports: the configured test-tree gate is loaded and red)*:
```text
tests\test_network.py:541: error: Function does not return a value (it only ever returns None)  [func-returns-value]
Found 1 error in 1 file (checked 28 source files)
```
Exit 1; 6.16s. This is the installed-venv equivalent of `uv run mypy src tests`; no uv dependency synchronization was performed.

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 2; MEDIUM 1; LOW 0.
- **Dependencies:** None. Resolve the type gate without changing production generic behavior.
- **Merge candidates:** TST-013/014 both improve doubles' discrimination but affect different boundaries and need distinct counterexamples.
- **Conflicting evidence:** 165 passing tests is consistent with both HIGH findings: the observed gaps accept invalid fixture/scheduling relationships. It is not proof of actual server or shipped-wrapper failure.
- **Cross-phase conflicts:** ASY-008/009/010, PIPE-012/013/014, CFG-011/012 and accepted integration issues retain product ownership. Their prescribed regression tests remain remediation constraints; not every absent regression case is a separate TST finding.
- **Rollout risk:** TST-013 must preserve hermetic transport replacement and valid failure/no-data cases. TST-014 must avoid leaving unawaited coroutine objects and should assert work delivery rather than private implementation shape. TST-015 must retain the local-path bypass checks and generic client contract. No tests were modified or removed by this audit.

## Appendices

### Appendix A — Exact full-suite outcome and coverage scope

Approved coordinator execution: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/phase11_suite.py`.
The inspected runner calls pytest.main with existing project addopts plus scratch basetemp/cache/JSON-coverage paths, blocks actual Session transport, and uses an outer180s subprocess limit. Default `slow` coverage is included. Exact full output and child command are preserved in the Scope artifacts; selected outcome:
```text
TOTAL                                               1194    152    87%
Required test coverage of 70% reached. Total coverage: 87.27%
================== 165 passed, 1 warning in 88.74s (0:01:28) ==================
```
Coverage JSON records version7.16.2, branch_coverage=False, covered_lines1042, num_statements1194, missing_lines152, excluded_lines28, percent_covered87.26968174204355, fourteen production Python files. This is single-process statement coverage for the configured src package; it excludes installed vendor internals and is not a boundary/branch guarantee. `__main__.py` is included with0% rather than silently omitted. No subprocess-spawning test tree was found in the inspected tests.

Fresh Ruff command: `.\.venv\Scripts\python.exe -m ruff check --no-cache src tests` (exit0,1.33s):
```text
All checks passed!
```
The full log also preserves a vendor PyparsingDeprecationWarning and a post-summary RuntimeWarning for an unawaited AsyncMock coroutine; no allocation trace or unique attribution is asserted here.

### Appendix B — Eight required blocks

| Block | Evidence and limits |
|---|---|
| 1: Observed decisions | Tests assert period clipping/count guards, task failure transitions, queue drain, actual CSV existence/content in utility tests, error JSON5 context, configuration merge, verbosity and progress state. The wrapper scheduling decision's two call counts do not discriminate intended work (TST-014). File-resume integrity/collision/entry continuation remain accepted product findings with their regression needs; naturally empty DICT output is not reasserted as a rejected defect. |
| 2: Doubles | Vendor transport replacement is appropriate; independent string submission keys and status/result doubles erase their shared shape contract (TST-013). The wrapper's replaced scheduler erases work identity/execution (TST-014). Semaphore test's `_invoke` coroutine verifies wrapper permit contention but cannot observe lingering sync threads; ASY-009 retains that product boundary and its needed regression. Actual lifecycle tests use real create_report/network worker flow with fake vendor methods, not historical mocked create_report claims. |
| 3: Fixture contract | Root fixture uses isolated settings/export temp paths and copies bundled defaults. DEFAULT_KIT_ID='7' matches the current selected crosstab default; report fixture uses enum.value members. expected_output_columns mirrors report column selection for fixture data, not an independent schema proof. String keys are the demonstrated unfaithful handoff; JSON result conversion remains doubled, with no claim of live payload fidelity. |
| 4: Boundary sides | CSV helper content/null formatting and JSON5 error serialization are exercised on real scratch files. Lifecycle poll/result consumer methods are independently mocked, leaving key handoff one-sided (TST-013). Entry wrapper's scheduling relationship is unobserved (TST-014). Missing-config, cancellation, output publication and worker error-write failure fixes must preserve earlier phases' named tests; product findings are not duplicated. |
| 5: Harness | pytest asyncio auto/importlib/pythonpath configuration is read from project root; fixtures patch init/user paths and vendor operations, and the audit runner adds an unconditional real-transport block. `slow` is registered and applied to service pipeline testing; default full run includes it. No DB/container/xdist universe exists to partition. Cache/temp/coverage outputs remain scratch. Per-item/runaway protections vary; the timeout lifecycle test has an outer deadline, and this audit adds an overall process bound without claiming the repository imposes a per-test bound globally. |
| 6: Reproducibility | Tests mainly use tmp_path, fixed dates and controlled sleeps; root logging and singleton state are process-global with several explicit save/reset fixtures. Some service fixtures legitimately call dictConfig; no order-independent global logging guarantee is established. No randomization/parallel permutation experiment was run, so order independence is not inferred from one green run. Relevant commands/tool versions/output and source drift are preserved for red-run reproduction. |
| 7: Gates | CI loads Ruff, pytest/coverage and mypy src/tests. Ruff/test/coverage green; mypy red on a current test inconsistency (TST-015). Configured coverage threshold70 measures source statements, not branches/vendor network behavior. Local pre-commit declarations do not prove hook installation or branch-protection enforcement; neither remote policy was queried. Static/safety/translation gates beyond declared scope are not invented. |
| 8: Self-description | Claimed real lifecycle execution is now accurate; historical no-CI/no-hermetic/no-coverage claims are obsolete. The wrapper's claimed delegation coverage is too weak (TST-014); installed-consumer fidelity is not carried by string-key fixtures (TST-013). `slow` and tmp_report_dir are used; no inert-marker finding is repeated. Current mypy type mismatch is distinct from general annotations/suppression population owned by phase10. |

### Appendix C — Focused fidelity evidence and preservation

Exact command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/phase11_fidelity.py` (exit0,2.77s). Full output is the two finding fences combined: key-contract outcome, eighteen site count/files, and wrong-wrapper assertions surviving with intended await count0. Only in-memory module attributes are patched and restored; test-generated files live under assigned scratch. No production file, test, configuration, docs, dependencies, plans, `.kilo` or Git index was changed. All anchors are tracked current paths and primary symbols/disambiguators resolve. The installed vendor method is primary dependency evidence, not a fabricated repository anchor.
