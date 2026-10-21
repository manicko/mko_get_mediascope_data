---
phase: "09"
phase_name: "external-api"
id_prefix: "EXT-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/09-external-api/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Validation Report

## Summary

Three findings remain actionable after qualification: two MEDIUM and one LOW. Constructor requests miss the normal retry guard, and deterministic session release is an architectural improvement. Submission documentation incorrectly promises pre-commit-only or single-attempt behavior; the simulator does not prove actual duplicate Mediascope calculations, so CRITICAL severity is not established.

## Scope

**Input:** Frozen `.ai/audit/09-external-api/findings.md`, EXT-009–011; all independently adjudicated on 2026-10-21. Prior current-run validated phases 01–04 and resume-snapshot-freshness were compared for ownership; restored historical reports are provenance only.
**Snapshot:** Dirty/staged working tree on b27b8708d4d816940310d4f034b549cb1db6f397, not pristine HEAD. Current source and installed dependencies were inspected. Existing staged/index changes, user configuration and restored audit artifacts were preserved; resume copies remain under `.ai/tmp/audit-2026-10-21/resume-b27b870-reports/`.
**Examined:** Submission defaults and retry tuple/class hierarchy; service constructor/orchestration; vendor task/catalog singleton/session construction and requests/urllib3 cleanup; owner D-16; relevant test scope; actual dependency adapter policy; original phase rubric and controlled report template.
**Tools:** Read-only PowerShell/rg/Git, AST, existing venv Python and official HTTP specification. Independently inspected and reran external_probe.py under its existing outer 12-second timeout; exit 0 in 3.19 seconds. No native-loop approval was necessary.
**Not verified:** Actual Mediascope duplicate side effects, idempotence/deduplication, production sockets/FD retention, real credentials, live API, GC timing under traffic or full test/lint/type suite. Probe uses synthetic credentials, transport prohibition, a deliberately non-deduplicating sender, instrumented Session doubles and mocked selector/self-pipe I/O. These limits narrow what is approved below.

## Findings

| ID | Final severity | Anchor | Title |
|---|---|---|---|
| EXT-011 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.run_report` | Constructor dependency requests miss the application retry guard |
| EXT-010 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.get_connection` | Session release lacks deterministic report ownership |
| EXT-009 | LOW | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.send_task` | Submission documentation promises an unimplemented pre-commit predicate |

| ID | Disposition | Original type / severity | Final type / severity |
|---|---|---|---|
| EXT-009 | re-graded | SPEC-DEVIATION / CRITICAL | DOC-UPDATE / LOW |
| EXT-010 | re-typed | SPEC-DEVIATION / MEDIUM | BEST-PRACTICE / MEDIUM |
| EXT-011 | confirmed | SPEC-DEVIATION / MEDIUM | SPEC-DEVIATION / MEDIUM |

EXT-009 has one primary disposition, re-graded, and an explicit accompanying type correction. Its claimed real-server duplicate effect is not approved. The precise false predicate documented in the original finding is independently established and is the retained LOW issue, not a weakened generic claim that retries might be risky.

#### EXT-011 — [MEDIUM] Constructor dependency requests miss the application retry guard

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `api_call=False`
**Checked:** ran
**Why this band:** The phase explicitly rates a guard present on one path but absent from a sibling reaching the same dependency MEDIUM. That is the demonstrated retry-policy divergence. Report/entry continuation blast radius is not promoted into a duplicate of PIPE-013, and no unbounded operation is measured.
**Problem:** run_report classifies get_connection as local/bypass work, but the actual vendor constructor initializes catalog data through authenticated requests. Transient catalog startup failures therefore miss the application retry loop used for ordinary API calls.
**Repro:** Real NetworkClient configured for three attempts calls the real application constructor with api_call=False; a patched vendor catalog initializer raises ConnectTimeout once. The same failure class in an ordinary wrapped callable executes three times and exhausts as MaxRetriesExceeded. All transport remains prohibited.
**Impact:** Operator max_attempts does not cover this reachable dependency-request path. A transient initialization outage receives different application recovery from an equivalent later catalog outage.
**Root cause:** The bypass discriminator is applied to a constructor whose downstream initializer performs I/O. check_version=False suppresses version checking, not catalog/authentication initialization.
**Type rationale:** SPEC-DEVIATION retained against the declared application API retry boundary and phase's sibling-guard contract. The installed constructor's calls are verified, not inferred from documentation saying it is local.
**Fix:** After defining constructor cleanup/replay ownership, make AppService.run_report use the normal API classification for its get_connection call instead of api_call=False. Verify which constructor steps can already have completed before replay; make their failure cleanup deterministic first (EXT-010), retain permanent-error classification and account for actual transport/session timeouts (ASY-009). Correct local/no-network constructor documentation. Do not turn every local builder/conversion call into a retried API operation.
**Verify:** new test needed: test_constructor_dependency_calls_honor_declared_retry_contract; cover transient catalog failure, permanent auth failure, partial initialization cleanup and ordinary-call parity using the actual vendor constructor seam.
**Related:** EXT-010 owns release; ASY-009 owns underlying operation lifetime; CFG-012 owns proxy stdout; PIPE-013 retains service continuation. No new whole-report interruption finding is filed here.
**Anchor/cause decision:** All three methods resolve; primary token is inside the service's connection call. Vendor MediaVortexTask.__init__ creates MediaVortexCats, whose __init__ creates msapi_network and calls load_tv_property/get_units. Their source reaches catalog requests. Probe patches the constructor-time request point, preserving this executing caller chain.
**Recommendation assessment:** The raw alignment instruction is outcome-only. The explicit API classification change, cleanup prerequisite and replay/permanent-error checks above supply its implementation mechanism and rollout order.

#### EXT-010 — [MEDIUM] Session release lacks deterministic report ownership

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Disambiguator:** `return connection_service(`
**Checked:** ran
**Why this band:** MEDIUM's bounded resource/operability gap applies to unspecified deterministic release timing. This is not a demonstrated unbounded leak, descriptor exhaustion, live socket accumulation or HIGH correctness failure. GC cleanup and bounded latest-singleton retention remain material counterevidence to stronger claims.
**Problem:** The application/vendor seam allocates task and catalog sessions without a report-owned explicit release contract on success or partial constructor failure. Latest vendor singleton owners can retain their network/session references after report completion.
**Repro:** The actual service and vendor constructors allocate four instrumented Session doubles across two entries, with no explicit close calls; a constructor catalog failure also has zero close calls. The probe does not open sockets or measure retention duration, and its session instrumentation is not proof of physical leakage.
**Impact:** Long-lived hosts lack a deterministic report-boundary release decision. Some replaced owners become collectible, while latest singleton owners persist. Predictable ownership improves maintainability and resource-lifetime reasoning; production harm remains unmeasured.
**Root cause:** Session lifetime is delegated to vendor singletons and object collection rather than an explicit application integration lifecycle.
**Type rationale:** Re-typed BEST-PRACTICE: the concrete release-ownership gap is a valid lifecycle improvement, but no owner decision or public product contract mandating report-boundary close was found. An unproven permanent leak cannot justify SPEC-DEVIATION or mandatory emergency remediation.
**Fix:** Implement exception-safe ownership in the installed vendor's existing MediaVortexTask.__init__/MediaVortexCats.__init__ construction paths, with an explicit close lifecycle for both task network_module.session and catalog msapi_network.session. Consume a verified dependency artifact exposing that lifecycle, then invoke it from AppService.run_report under a finally after owned operations are actually finished. Respect singleton reuse and ASY-009's still-running work; do not close shared sessions beneath active operations. No currently available fixed release is assumed.
**Verify:** new test needed: test_report_connections_have_explicit_success_and_failure_release; instrument both sessions, constructor partial failure and execution failure, and verify no premature closure during retained synchronous work or other shared-owner use.
**Related:** EXT-011 must not multiply partial constructors before release is settled; ASY-009 remains operation-lifetime owner. Historical EXT-006 is reserved and is not resurrected as a permanent-leak finding.
**Anchor/cause decision:** Both application methods and exact constructor token resolve. Actual vendor objects allocate separate sessions; the service has no corresponding finally/close. Installed Requests Session.close iterates adapters; urllib3 HTTPConnectionPool registers weakref.finalize with _close_pool_connections. Thus unreachability can clean pools, and zero explicit closes proves only the stated lifecycle gap.
**Recommendation assessment:** The raw ownership instruction needs an explicit release mechanism. The vendor exception-safe construction/close lifecycle plus application adoption supplies a concrete approach, but remains an architectural improvement whose dependency/reuse complexity must be assessed before implementation.

#### EXT-009 — [LOW] Submission documentation promises an unimplemented pre-commit predicate

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.send_task`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::RETRIABLE_EXCEPTIONS`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Anchor:** `src/mko_get_mediascope_data/core/models.py::SendTaskSettings`
**Disambiguator:** `RETRIABLE_EXCEPTIONS if cfg.retry_on_connection_error else None`
**Checked:** ran
**Why this band:** The exact LOW condition is a comment/docstring naming a predicate the executing path does not evaluate. Code tests exception membership, not pre-commit state; raw/model/report comments and single-attempt guidance claim stronger semantics. CRITICAL requires a non-idempotent retryable write with a duplicate effect at the far side. That property was stipulated by the simulator, not established for Mediascope. No HIGH silent correctness failure or unbounded retry multiplication was demonstrated. The current proven issue is inaccurate mechanism documentation, not a claimed realized server-side duplicate.
**Problem:** SendTaskSettings and send_task describe selected transport failures as occurring before body commitment. The selected tuple includes requests.Timeout, so ReadTimeout is eligible even though its type does not establish that the remote write was unapplied. Guidance saying task submission is always single-attempt also disagrees with current defaults; vendor adapter retry policy is an independent layer.
**Repro:** Actual send_task/NetworkClient replay a fake sender after ReadTimeout; the deliberately non-deduplicating fake records two commits and returns one key. The installed vendor adapter reports POST/503 retry eligibility with total five. This independently disproves the pre-commit-only explanation, while actual remote duplication remains unverified.
**Impact:** Maintainers/operators are given an inaccurate explanation of what submission flags and transport layers guarantee. The current evidence does not show that an actual Mediascope result was duplicated/lost or that its accepted best-effort contract is violated.
**Root cause:** Exception-class selection is described as knowledge of remote write state. Requests distinguishes ConnectTimeout as safe to retry, while ReadTimeout only describes absent response data; no application check of commitment/deduplication state exists.
**Type rationale:** DOC-UPDATE: owner D-16 explicitly accepts best-effort idempotency; current settings intentionally enable selective retry. The load-bearing behavior is that configured retry policy, and the directly contradicted artifact is its safety description. D-16 is not evidence that the server deduplicates, but neither is it a requirement to remove every ambiguous retry. The retained issue is exact, testable documentation drift already present in the raw claim; the raw CRITICAL far-side consequence is withheld.
**Fix:** Correct SendTaskSettings' docstring, TVMediaReport.send_task comments and applicable AGENTS/runbook descriptions to state actual default exception membership, configurable app retry behavior and independent vendor POST retry behavior. Explicitly avoid a guarantee that the request body was not committed or that disabling app retries disables every transport replay. Preserve owner-accepted best-effort behavior and ConnectTimeout recovery. A later safe-replay policy/code review is conditional on an authoritative Mediascope contract or an owner decision to change that tradeoff; it is not a mandate from this LOW documentation finding and must not invent an idempotency header/key.
**Verify:** new test needed: test_submission_lost_response_after_commit_respects_replay_contract, scoped to the documented policy using a simulator; preserve existing ConnectTimeout coverage. Any test asserting real-server deduplication requires independent authoritative/controlled-server evidence, not the current fake.
**Related:** ASY-009 owns still-running operations after await timeout; this distinct finding documents which completed/ambiguous exceptions are replay eligible. Historical EXT-001 is preserved as provenance, not treated as the current configured contract.
**Anchor/cause decision:** send_task/NetworkClient.call/SendTaskSettings resolve; RETRIABLE_EXCEPTIONS resolves as a module-level assignment. The primary token appears in send_task. Installed requests ReadTimeout subclasses Timeout, a selected tuple member; adapter configuration independently permits POST statuses. The simulator observation is reproduced but its receiver semantics are explicitly not generalized to Mediascope.
**Recommendation assessment:** Documentation correction is actionable immediately at existing targets. The raw instruction to establish safe replay is a conditional follow-up, not proof that a production retry change is required. No speculative fixed dependency or server feature is proposed.

The general HTTP rule makes replay safety depend on known request semantics or evidence that the original was unapplied; it does not establish the behavior of this specific API. Independently checked [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2).

## Fix Order

- **Approved distribution:** CRITICAL 0; HIGH 0; MEDIUM 2; LOW 1. EXT-010 is architectural BEST-PRACTICE; EXT-009 is DOC-UPDATE; EXT-011 is SPEC-DEVIATION.
- **Disposition tally:** re-graded 1 (also type-corrected explicitly); re-typed 1; confirmed 1; merged 0; not substantiated 0; unsettled 0. Three dispositions for three original IDs. Actual Mediascope non-idempotence/duplicate effects remain unverified and are not approved as part of the retained documentation finding.
- **Dependencies:** Correct EXT-009 documentation without changing owner-accepted retry tradeoffs. Define constructor/session release before broadening constructor retry coverage; verify replay semantics of partially initialized constructor steps. EXT-010's dependency artifact/ownership prerequisites must precede its application finally-close adoption.
- **Merge candidates:** None. Explicit release, constructor retry omission and inaccurate submission predicates are separate causes. EXT-010/011 share vendor initialization targets but require distinct success/failure/retry verification.
- **Conflicting evidence:** The simulator deliberately assumes no deduplication; actual server semantics are unknown. Source claims pre-commit-only despite ReadTimeout eligibility. GC finalizers refute inferring permanent leakage from zero explicit closes. These limits are carried into final types/grades rather than hidden in a footnote beneath CRITICAL.
- **Cross-phase conflicts:** Current-run phases 01–04 and resume freshness were compared; earlier ten accepted content findings retain their ownership. No duplication of ASY-009 operation lifetime, CFG-012 proxy output or PIPE-013 continuation. Restored future-phase historical reports are not treated as fresh validated results. Phase 13 may measure cost/FD retention but receives no confirmed leak claim here.
- **Rollout risk:** Preserve safe ConnectTimeout retry and documented best-effort behavior unless separately changed. Closing retained sessions prematurely can break in-flight thread work or singleton reuse. Retrying whole construction can multiply partially created sessions/authentication; explicit cleanup must precede that change. `api_call=False` remains valid for actual local builder/conversion paths.
- **Required/advisory distinction:** EXT-011 is a contract correction and EXT-009 a documentation correction. EXT-010 is an architectural improvement with no demonstrated production resource incident. This validation approves findings/planning evidence, not a dependency release or production rollout.

## Validation-Level Findings

No new VAL identifier is allocated. Existing systemic VAL-007 (template pointer), VAL-008 (historical validation namespace) and VAL-009 (outcome-only recommendation pattern) remain references, not duplicate findings. EXT-009's overstated band is corrected directly in its disposition; no generic grading issue is minted again. This report does not approve the unverified far-side claim or label a missing explicit close a proven leak.

## Appendices

### Appendix A — Independent bounded probe output

Exact command:

```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/external_probe.py'],timeout=12); print('outer_process_exit:',p.returncode)"
```

Exit 0, 3.19 seconds. Script inspected first; actual Session.request transport is patched to fail, credentials/sender are synthetic and Session allocation/close counters are instrumented doubles. Complete stdout:

```text
auth_wire_form_fields: ['client_id', 'client_secret', 'grant_type', 'password', 'username']
auth_requests_timeout_supplied: False
api_wire_authorization_header_present: True
api_requests_timeout_supplied: False
vendor_retry_total: 5
vendor_retries_post_503: True
vendor_post_backoff_jitter: 0.0
installed_versions: {'mediascope-api-lib': '1.8.3', 'requests': '2.32.5', 'urllib3': '2.6.3'}
default_send_readtimeout_eligible: True
simulated_committed_writes_after_lost_response: 2
application_retained_keys: 1
application_task_send_status: True
constructor_outage_exception: ConnectTimeout
constructor_catalog_attempts: 1
constructor_failed_sessions_closed: 0
ordinary_outage_exception: MaxRetriesExceeded
ordinary_catalog_attempts: 3
successful_two_entries_sessions_created: 4
successful_two_entries_sessions_closed: 0
recorded_backoff_sleeps: [0, 2.0, 0, 2.0, 2.4, 2.88]
outer_process_exit: 0
```

The sender's non-idempotence was an input assumption. The adapter eligibility query is not an actual POST replay to Mediascope. Session counters establish explicit-close absence only, not actual sockets, FD retention or GC behavior. No actual credential or token value is emitted in this evidence.

### Appendix B — Controls, namespace and report-artifact adjudication

- **Independent requirements:** D-16 in open-questions accepts best-effort idempotency, not guaranteed repeat-identical remote behavior. It neither proves deduplication nor authorizes claiming that every selected error was pre-commit. The exact documentation contradiction is retained at LOW. The HTTP specification provides general replay criteria, not missing Mediascope server evidence.
- **Control scope:** `tests/test_network.py::test_send_task_retries_on_connection_error` exists, but calls NetworkClient directly with retry_on=(ConnectTimeout,), not actual TVMediaReport.send_task defaults. Its three-call assertion decides safe connection retry for that supplied tuple; it examines zero ReadTimeout/default-selection cases and cannot substantiate pre-commit-only behavior. It was inspected, not run. The actual sender-path probe establishes ReadTimeout selection independently.
- **Session evidence:** Four allocated doubles/zero close calls do not measure four retained live connection pools. The probe's local instrumentation list itself retains doubles. Separate installed singleton source establishes latest-owner reachability, while Requests.close and urllib3's weakref finalizer show explicit and GC release mechanisms respectively. No green lifecycle control or absent close count is converted into a permanent-leak verdict.
- **Retry parity evidence:** Constructor attempt count one and ordinary count three are deciding direct observations of actual wrapper paths using the same transient exception class. Neither a test PASS nor documentation saying the constructor is local establishes that distinction. Source confirms catalog methods issue requests before construction returns.
- **Checklist dependence:** EXT-009 maps to retry safety and declared mechanisms; EXT-010 maps to bridge release; EXT-011 maps to sibling guards/degradation and declared dependency behavior. All use independent observations/source paths within enumerated angles. No substantive retained claim relies only on the input coverage table, and no retained finding requires an angle outside the phase's broad blocks.
- **Declared/history:** Phase 09 declares bare EXT-. Historical raw report inspected explicitly as Git evidence at baseline 4c08eb4 minted EXT-001..008. Current EXT-009..011 continue that namespace and preserve every input ID. Historical EXT-006's claimed permanent FD leak is not current evidence; EXT-010's ownership improvement is narrower and independently derived.
- **Shipped markers:** Search of src/tests/docs counted 28 EXT occurrences, representing EXT-001,002,003,004,005,006,008. These are historical regression/architecture references, not current finding confirmations. Preserve their identities and qualify their historical report/commit source during provenance maintenance rather than reusing IDs. No phase-qualified compound EXT namespace is introduced; no compared current sibling phase reuses EXT. Shared historical VAL reuse remains systemic VAL-008, not a new issue here.
- **Controlled template:** No closed prefix list exists in the controlled template; workflows/registry supply prefixes. Its malformed generic severity pointer remains VAL-007. Input phase 09 carries severity_taxonomy under the exact name once with the correct workflow path; omitted 0, renamed 0. This output uses that path and validated status, with dirty/staged scope stated honestly.
- **Field/disposition completeness:** Each raw finding supplies primary/secondary anchors, token, checked mode, band rationale, problem, reproduction, impact, cause, fix, verify and related context. This report retains those fields, original IDs and one primary disposition each. EXT-009's additional type correction is explicit, not a hidden fourth disposition. Content grades and any VAL grades are never mixed in one table; no new VAL finding is counted.
- **Cross-phase comparison:** Current-run raw/validated phases 01–04 and resume-snapshot-freshness were compared for cause/ownership. No accepted earlier content finding is renumbered, merged away or edited. Restored historical future-phase audit artifacts and their later cleanup are not implementation evidence or fresh verdicts. No source correctness claim depends on preserving those stale files in the working tree.
- **Unresolved limits:** Actual Mediascope non-idempotence/deduplication, duplicate calculations, live socket/FD behavior, GC latency and native Windows loop performance remain unverified. They are not confirmed in any final impact/band. The retained documentation/ownership/sibling-guard claims are decided within their bounded scope, so no whole finding is marked unsettled.

### Appendix C — Mechanical checks and acceptance

AST independently resolved the five class/function anchors: TVMediaReport.send_task, AppService.get_connection/run_report, NetworkClient.call and SendTaskSettings. A separate AST assignment check resolved module-level RETRIABLE_EXCEPTIONS. Primary disambiguator tokens occur inside their named methods; all four production files are tracked. Installed dependency locations are evidence context, not falsely claimed tracked application anchors.

Existing venv Python final report-text check (exit 0):

```text
dispositions: {'re-graded': 1, 're-typed': 1, 'confirmed': 1}
unique_input_ids: ['EXT-009', 'EXT-010', 'EXT-011']
content_headers: [('EXT-011', 'MEDIUM'), ('EXT-010', 'MEDIUM'), ('EXT-009', 'LOW')]
new_VAL_headers: []
line_locators: []
final_head: b27b8708d4d816940310d4f034b549cb1db6f397
module_retry_anchor_resolves: True
```

Ready for tech-lead acceptance with two MEDIUM and one LOW, no CRITICAL/HIGH. EXT-009 does not mandate removing owner-accepted retries; safety-contract changes remain conditional. EXT-010 is an architectural lifecycle improvement, not evidence of a resource incident. No new distinct VAL issue is filed. Only this assigned validation report was changed; no application/source/tests/settings/docs/.kilo/index/input report or prior validation report was edited.
