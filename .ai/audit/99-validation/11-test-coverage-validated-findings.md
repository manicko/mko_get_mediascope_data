---
phase: "11"
phase_name: "test-coverage"
id_prefix: "TST-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/11-test-coverage/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Validation Report

## Summary

All three findings are confirmed: two HIGH assurance gaps and one MEDIUM red gate. Pipeline doubles accept an invalid task-key handoff, and the wrapper test accepts scheduling the wrong work. The configured type command independently fails on one test's inconsistent callable/double return contract.

## Scope

**Input:** Frozen `.ai/audit/11-test-coverage/findings.md`; TST-013–015 independently checked using phase 99 and the complete phase-11 rubric.
**Snapshot:** Current dirty/staged b27b870 working tree, not pristine HEAD. After the intentional pause, all 30 phase11-gate-input hashes were rechecked: changed 0, missing 0. This supports reuse of the focused checks completed before interruption. External configuration/guidance/.kilo edits are preserved, not attributed to this validator.
**Evidence:** Independently inspected/reproduced phase11_fidelity.py and the configured src/tests mypy command. Inspected native suite runner, saved result/output and coverage JSON; the coordinator ran that suite once, not this validator. It passed 165 tests, statement coverage 87.27%; no second full run was started.
**Limits:** No live API, real credentials, full-suite mutation campaign, random/parallel order experiment or CI-host execution. The counterexample is an in-memory intentionally wrong wrapper, not current production behavior. Session/network transport is absent from the fidelity checks. Overall statement coverage is not a boundary-contract guarantee.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TST-013 | HIGH | `tests/test_reports_lifecycle.py::test_create_report_writes_csv_and_marks_queue_done` | Independent doubles erase task-key handoff fidelity |
| TST-014 | HIGH | `tests/test_app.py::test_run_report_sync_wrapper` | Wrapper test accepts scheduling unrelated non-awaitable work |
| TST-015 | MEDIUM | `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter` | Inconsistent test return contract keeps the declared type gate red |

| ID | Disposition | Final type | Final severity |
|---|---|---|---|
| TST-013 | confirmed | SPEC-DEVIATION | HIGH |
| TST-014 | confirmed | SPEC-DEVIATION | HIGH |
| TST-015 | confirmed | SPEC-DEVIATION | MEDIUM |

#### TST-013 — [HIGH] Independent doubles erase task-key handoff fidelity

**Anchor:** `tests/test_reports_lifecycle.py::test_create_report_writes_csv_and_marks_queue_done`
**Anchor:** `tests/test_services.py::test_run_report_with_real_models`
**Disambiguator:** `mock_report_service.send_task.return_value = "mock_task_key_123"`
**Checked:** ran
**Why this band:** HIGH explicitly covers a boundary asserted from one side only and a double removing the property under test. No current production wrong key or user-visible incorrect export is established, so CRITICAL false product behavior is not claimed.
**Problem:** The representative positive lifecycle/service fixtures return a string submission key while independent status/result mocks accept any argument. Installed vendor get_status/get_result require a mapping containing taskId; the same string fails before transport.
**Repro:** The independent fidelity probe obtains DONE from a status double given mock_task_key_123, but actual installed MediaVortexTask.get_status raises AttributeError for it without invoking its network spy. Source confirms the shipped positive fixtures use that shape.
**Impact:** These pipeline assertions do not discriminate a key-shape regression that the real consumer rejects. Real CSV creation/queue drain in those tests does not restore the missing integration-value contract.
**Root cause:** Producer and consumer doubles are configured independently rather than sharing the installed task-key contract.
**Fix:** Use representative positive submission mappings such as a synthetic taskId in lifecycle/service fixtures. Add a transport-free contract test routing that same fixture key through the real installed status/result methods with their network boundary stubbed, verifying the requested identifier reaches the expected state/result endpoint. Keep invalid/missing/falsy-key cases as explicit failures; do not change production to accept fixture strings.
**Verify:** new test needed: test_submission_key_handoff_matches_installed_vendor_consumer. A nonmapping regression must fail before a purported successful lifecycle export; valid mappings must preserve the task identifier across submission, polling and result retrieval.
**Related:** Product integration findings retain their owners; this is assurance fidelity only.
**Evidence qualification:** Eighteen literal string return assignments exist across three test files. This is an inventory, not eighteen reachable successful-path violations: some are deliberate failure/unused fixtures. The two primary positive fixtures plus actual consumer rejection substantiate this finding independently of that count.
**Recommendation assessment:** Contract-faithful positive keys plus a real-consumer/fake-transport handoff check supplies a concrete meaningful behavior test rather than another permissive mock or source-text assertion.

#### TST-014 — [HIGH] Wrapper test accepts scheduling unrelated non-awaitable work

**Anchor:** `tests/test_app.py::test_run_report_sync_wrapper`
**Disambiguator:** `mock_run.assert_called_once()`
**Checked:** ran
**Why this band:** HIGH applies to a scheduling decision exercised only through a stand-in that cannot observe the intended work. The actual wrapper currently schedules its correct coroutine; a scratch mutant does not establish a present product failure or justify CRITICAL.
**Problem:** Separate assertions prove that the async API was called and the scheduler was called, but not that the scheduler drives the requested async report operation.
**Repro:** The probe runs the actual shipped test function with equivalent isolated patching and an in-memory wrapper counterexample. It calls the expected async API but passes a separate plain object to mocked asyncio.run; all shipped assertions pass and intended await count is zero. Patches are restored and the created coroutine is closed.
**Impact:** This test can stay green if the synchronous entry ceases to execute the requested report while preserving those call counts.
**Root cause:** Replacing the scheduling mechanism and checking calls independently removes the work-delivery relationship the test claims to cover.
**Fix:** Keep real asyncio.run in this synchronous test and replace only run_report_async with a small async recording stub. Call the public synchronous wrapper and assert that the stub body actually ran with the requested path/progress. A wrong/non-awaitable scheduled object must fail the test. This observes work delivery without asserting private scheduler choreography or source text.
**Verify:** new test needed: test_sync_wrapper_drives_requested_async_report_work; preserve hermetic execution, forwarded arguments and coroutine cleanup on failing counterexamples.
**Related:** ENT-008 owns notebook host usage; QLT-021 owns static public signatures. No runtime product regression is refiled here.
**Recommendation assessment:** Concrete collaborator boundary and observable stub execution are supplied; changing production is unnecessary.

#### TST-015 — [MEDIUM] Inconsistent test return contract keeps the declared type gate red

**Anchor:** `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter`
**Anchor:** `.github/workflows/ci.yml::"uv run mypy src tests"`
**Disambiguator:** `result = await client.call(lambda: None, api_call=False)`
**Checked:** ran
**Why this band:** A loaded gate is locally red on a bounded test-tree contract mismatch: MEDIUM operability impact. It is not an absent gate or a production diagnostic population; no stricter checking mode than the configured one is assumed.
**Problem:** The supplied callable returns None, but mocked _invoke returns a string sentinel and the test consumes that result. The generic real method and fake therefore describe different return contracts; current mypy rejects the call although pytest accepts the mock.
**Repro:** Independently rerun the existing-venv equivalent of configured mypy src tests with only cache redirected to scratch. It exits 1 with one func-returns-value diagnostic in this test. CI configuration explicitly includes the same source/test scope; no remote CI outcome is claimed.
**Impact:** Developers cannot obtain a green declared local type command without correcting or bypassing this test inconsistency.
**Root cause:** The test's callable and its mocked result disagree, not a defect in production generic inference.
**Fix:** Make the supplied callable return the same typed sentinel, for example lambda: sentinel, while retaining result and bypass assertions. Do not weaken NetworkClient's generic type or silence the valid diagnostic. The test must still establish local-path result delivery and absence of retry/jitter/permit acquisition.
**Verify:** `tests/test_network.py::test_cpu_local_calls_bypass_semaphore_and_jitter` and the configured mypy src tests command.
**Related:** Phase 10 correctly deferred this test-only diagnostic here.
**Recommendation assessment:** One existing test target, explicit matching return contract, no production repair or broad suppression required.

## Fix Order

- **Distribution/dispositions:** HIGH 2, MEDIUM 1; CRITICAL/LOW 0. Confirmed 3; re-typed/re-graded/merged/not substantiated/unsettled 0.
- **Dependencies/merges:** None. TST-013 and TST-014 concern distinct boundaries and retain separate counterexamples.
- **Conflicting evidence:** A green suite and 87.27% statement coverage coexist with these discrimination gaps; neither proves actual product failure or complete boundary fidelity. The eighteen-site inventory is not a failure count.
- **Ownership/rollout:** Product findings in earlier phases remain separate. Preserve hermetic transport, valid negative/no-data cases and runtime lazy imports; do not alter production key handling, scheduling or generic returns to suit tests. Remote CI execution/order independence remain unverified.

## Validation-Level Findings

No new distinct VAL issue is allocated. Existing template/namespace concerns remain with their recorded owners; they are not repeated or counted as content findings.

## Appendices

### A — Independent focused evidence and suite provenance

Validator ran `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/phase11_fidelity.py` before the pause (exit 0):

```text
lifecycle_double_accepts_string_key: DONE
installed_vendor_same_key_exception: AttributeError
vendor_transport_reached: False
send_task_string_return_fixture_sites: 18
send_task_string_return_fixture_files: ['tests\test_reports_lifecycle.py', 'tests\test_reports_progress.py', 'tests\test_services.py']
shipped_wrapper_test_accepts_wrong_nonawaitable: True
mutant_scheduler_argument_awaitable: False
intended_async_pipeline_await_count: 0
```

Validator independently ran `.\.venv\Scripts\python.exe -m mypy --cache-dir .ai/tmp/audit-2026-10-21/mypy-phase10 src tests` (exit 1):

```text
tests\test_network.py:541: error: Function does not return a value (it only ever returns None)  [func-returns-value]
Found 1 error in 1 file (checked 28 source files)
```

After interruption, hash recheck found 30 checked, changed=[], missing=[]; HEAD remained b27b8708d4d816940310d4f034b549cb1db6f397. No redundant probe/suite was rerun on unchanged inputs.

The coordinator-brokered native suite artifact records exit 0, outer 92.144s, pytest 165 passed/one warning in 88.74s. Inspected runner preserves project addopts/default slow tests, redirects temporary/cache/coverage paths into scratch and blocks actual Session transport. Coverage JSON measures fourteen production files, statement mode (branch_coverage=False), 1042/1194 covered statements =87.2697%, over the configured 70%. Installed vendor code is outside that measure. The post-summary unawaited-AsyncMock warning has no unique originating test attribution in its log; none is invented here. This is inspected saved evidence, not a validator-owned full-suite run.

### B — Phase-99 support and artifact rulings

- **Anchors/causes:** Tracked test/CI paths and primary functions/tokens resolve. Vendor get_status calls tsk.get('taskId'); get_result has the same mapping seam. Wrapper test independently mocks both calls and asserts only their counts. The generic callable/double mismatch and loaded CI mypy command resolve in actual source. No line-number locator substitutes for a semantic anchor.
- **Green-control separation:** Suite existence/scope and its actual 165 passing count were verified from artifacts; the assertions relevant to key-consumer fidelity and scheduled-work identity remain deficient. Focused rejected-value/counterexample observations decide TST-013/014. The one fresh local type diagnostic decides TST-015; a production diagnostic count is not substituted for it.
- **Checklist/scope:** TST-013 maps to fixture/double/two-sided boundary blocks; TST-014 to exercised decisions/doubles; TST-015 to loaded gates. Each is independently observed within the enumerated angles, not supported solely by a coverage checklist. No retained finding depends on an unenumerated angle.
- **Namespaces:** Phase 11 declares TST-. Historical IDs TST-000–012 stay reserved; TST-013–015 continue the declared namespace without renumbering or a second compound form. Current src/tests/docs search found no numeric TST markers; a documentation wildcard TST-* is historical context, not a current finding allocation or proof of a collision. Preserve historical provenance through Git/retained scratch records; no current collision is invented from the workflow's historical warning. Shared VAL reuse remains existing VAL-008.
- **Template/fields:** The controlled template has no closed prefix list; workflows/registry supply it. Known malformed generic severity pointer remains VAL-007; raw phase11 severity_taxonomy is present once under its exact name with the actual workflow, omitted/renamed 0. This report uses that path and retains each mandatory per-finding field/ID/disposition with separate content/validation scales. No shared template repair occurred.
- **Cross-phase/limits:** Earlier current-run validation ownership was compared; no key/product failure, cancellation, export or static-public-signature finding is duplicated. Historical reports removed by user authorization contribute no fresh conclusions. No live server behavior, actual shipped wrong scheduler decision, full-suite mutation result or remote CI failure is approved.

Ready for tech-lead acceptance: three confirmed findings, HIGH 2 and MEDIUM 1; no unsettled finding or new distinct VAL issue. Only this assigned report and existing scratch outputs were written; raw/previous reports, source/tests/config/docs/.kilo/index were preserved by this validator.
