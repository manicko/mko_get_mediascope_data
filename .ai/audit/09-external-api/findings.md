---
phase: "09"
phase_name: "external-api"
id_prefix: "EXT-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/09-external-api/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — External Integrations & API

## Summary

Three findings, one CRITICAL and two MEDIUM. Submission retries can replay a write after its response is lost, and the local simulator creates two tasks while the application retains one identifier. Constructor-time requests also miss the configured retry policy, and report connections have no explicit normal or failure teardown.

## Scope

**Examined:** All eight integration blocks: auth/token wire construction; application/vendor replay eligibility; bounds/backoff/jitter; sync-thread bridge and session lifetime; outage blast radius; degraded-result distinctions; logging/redaction; documented mechanisms against actual execution. Installed primary dependencies: mediascope-api-lib 1.8.3, requests 2.32.5, urllib3 2.6.3. Relevant application readers are core network/reports/services/models; installed vendor net/tasks/catalogs and requests exception/retry classes were inspected.

**Snapshot:** Dirty/staged working tree at base HEAD `b27b8708d4d816940310d4f034b549cb1db6f397` on 2026-10-21, not pristine HEAD. Source/test/application-config differences and restored historical reports/index belong to other work. Historical reports were read for provenance only and preserved before resumption under `.ai/tmp/audit-2026-10-21/resume-b27b870-reports/`. Historical EXT-001–008 are reserved; current docs/tests and Git-history search found no higher EXT identifier. Earlier accepted claims are not re-audited here.

**Tools:** Read-only PowerShell, rg/Git source inspection, existing venv Python and isolated `external_probe.py`. Authoritative HTTP/Requests documentation was consulted for replay and timeout interpretation; no dependency upgrade or presumed fixed version is recommended.

**Limits:** No real Mediascope requests, real credentials, actual-server duplicate task observation, measured open sockets/FD leak or full lint/type/test suite. Replay effects are a deliberately non-deduplicating simulated receiver. Actual Mediascope idempotency/deduplication is unknown; no server idempotency contract is invented. Runtime uses real application asyncio/thread/pandas-independent paths with selector I/O mocked, per-case 2s deadlines and a 12s outer subprocess timeout. All actual requests.Session.request transport is blocked; fake Session methods construct/observe wire arguments only. Native Windows I/O behavior is not retested in this phase.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| EXT-009 | CRITICAL | `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.send_task` | Default submission retries admit post-commit response failures |
| EXT-010 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.get_connection` | Report connection sessions lack explicit teardown |
| EXT-011 | MEDIUM | `src/mko_get_mediascope_data/core/services.py::AppService.run_report` | Constructor API requests bypass the configured retry guard |

#### EXT-009 — [CRITICAL] Default submission retries admit post-commit response failures

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.send_task`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::RETRIABLE_EXCEPTIONS`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Anchor:** `src/mko_get_mediascope_data/core/models.py::SendTaskSettings`
**Disambiguator:** `RETRIABLE_EXCEPTIONS if cfg.retry_on_connection_error else None`
**Checked:** ran
**Why this band:** The phase grades replay of a non-idempotent committed write as CRITICAL; that duplicate side effect is reproduced against the explicit local simulator. Applying this consequence to the real server is conditional on its unverified deduplication behavior. The established application defect is replaying an ambiguous post-commit failure without establishing idempotence, not an assertion of observed Mediascope duplicates.
**Problem:** Default submission selects the entire transient-error tuple, including requests Timeout (hence ReadTimeout), ConnectionError and response/body failures. These types do not establish that the POST body was never committed, contrary to the source comments. The installed vendor's adapter additionally allows POST retries on 502/503/504 before application-level HTTP filtering sees them.
**Repro:** Invoke actual `TVMediaReport.send_task` with default SendTaskSettings and actual NetworkClient. The fake sender commits a synthetic task, then raises ReadTimeout to model a lost response; its next invocation commits another task and returns its key. The application reports a successful send with one key, while the simulator has two committed writes. Inspect the actual installed HTTPS adapter's retry eligibility: POST/503 is true with total=5.
**Impact:** With a receiver that does not deduplicate, ambiguous transport/server-response failure can create an extra calculation whose identifier the caller never receives. Current configuration offers no demonstrated real-server idempotency guarantee. Owner D-16 accepts best-effort idempotency; acceptance does not substantiate the claimed pre-commit-only predicate.
**Root cause:** Broad exception-type eligibility is used as a substitute for knowledge of whether the remote write happened; a second retry layer also permits POST.
**Fix:** Align `TVMediaReport.send_task`/NetworkClient and the installed vendor transport policy with a documented, demonstrated safe replay contract; correct the pre-commit-only and single-attempt claims. Benefit: submission behavior and duplicate-side-effect limits are explicit; effort medium; advisory priority highest. Do not invent a server-supported idempotency key. [SPEC-DEVIATION]
**Verify:** new test needed: test_submission_lost_response_after_commit_respects_replay_contract
**Related:** ASY-009 owns still-running synchronous calls after await timeout; this finding owns replaying an already committed write. Existing `tests/test_network.py::test_send_task_retries_on_connection_error` deliberately asserts ConnectTimeout retries; safe connect-failure recovery should remain distinguished from response-loss replay.

**Evidence — bounded transport-blocked probe, exact command in Appendix A** *(supports: ambiguous-failure replay and independent vendor POST eligibility)*:
```text
vendor_retry_total: 5
vendor_retries_post_503: True
default_send_readtimeout_eligible: True
simulated_committed_writes_after_lost_response: 2
application_retained_keys: 1
application_task_send_status: True
```

The HTTP specification cautions against automatically retrying non-idempotent requests unless the client knows their semantics are idempotent or knows the original was not applied. This is the relevant contract check, not a claim about Mediascope's implementation. [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)

#### EXT-010 — [MEDIUM] Report connection sessions lack explicit teardown

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Disambiguator:** `return connection_service(`
**Checked:** ran
**Why this band:** A bounded resource-lifecycle gap: report completion/failure does not explicitly release its owned sessions. MEDIUM does not imply an unbounded leak; GC can close collected pools, and no real sockets/FD retention was measured.
**Problem:** The installed constructor allocates separate task and catalog network sessions. The application has no success/failure teardown for them, and vendor task/catalog singletons retain their latest network owners beyond the report boundary. Earlier replaced owners can become collectible; there is no demonstrated deterministic report-level close.
**Repro:** Execute actual service orchestration for two report entries with actual vendor constructors, but stub catalog initializers and report bodies to prohibit transport. Instrument Session construction/close. Four session objects are allocated, with zero explicit closes. An actual constructor catalog failure also exits with zero closes.
**Impact:** Session/pool release after a report or constructor failure depends on retained references and garbage collection rather than the report boundary. Latest singleton-owned sessions can outlive the completed report. Resource duration under real traffic and persistent descriptor growth remain unverified.
**Root cause:** Vendor object allocation is consumed without an application-owned deterministic release contract.
**Fix:** Define and enforce release ownership for sessions created by `AppService.get_connection`, on both normal and failed report startup/execution, respecting vendor singleton reuse. Benefit: resource lifetime is predictable in long-lived hosts; effort medium; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_report_connections_have_explicit_success_and_failure_release
**Related:** Historical EXT-006 is reserved; no measured persistent-leak claim is carried forward. ASY-009 owns still-running thread lifetime.

**Evidence — same bounded probe** *(supports: allocation without explicit report/failure close)*:
```text
constructor_failed_sessions_closed: 0
successful_two_entries_sessions_created: 4
successful_two_entries_sessions_closed: 0
```

Installed fallback inspected: Requests `Session.close` closes all adapters; urllib3 `HTTPConnectionPool.__init__` registers `weakref.finalize(self, _close_pool_connections, pool)`. Thus unreachable collected pools have cleanup; absence of explicit close alone is not proof of a permanent socket leak.

#### EXT-011 — [MEDIUM] Constructor API requests bypass the configured retry guard

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.run_report`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `api_call=False`
**Checked:** ran
**Why this band:** A retry guard applies to normal API calls but is absent from a sibling path reaching the same dependency: explicitly MEDIUM in this phase. Broad continuation failure already belongs to PIPE-013, so that impact is not promoted/refiled here.
**Problem:** Connection construction is marked local with `api_call=False`, bypassing retries. The installed `MediaVortexCats.__init__` immediately calls `load_tv_property` and `get_units`, which reach authenticated API catalogs. A transient constructor-time catalog failure therefore does not receive the configured application retry behavior applied to later catalog calls. Documentation explicitly claiming this constructor never hits the network is false.
**Repro:** Let the real vendor constructor call its catalog initializer, patched to raise ConnectTimeout with transport blocked. Via actual service get_connection and NetworkClient(max_attempts=3), the constructor catalog is called once and exposes ConnectTimeout. An equivalent normal wrapped catalog fake is attempted three times and exposes MaxRetriesExceeded.
**Impact:** The same transient dependency failure receives materially different recovery depending on when its API request occurs. Operators cannot apply max_attempts consistently to startup catalog/authentication calls.
**Root cause:** The local/CPU bypass discriminator is assigned to a constructor that performs dependency I/O.
**Fix:** Align `AppService.run_report`'s connection-construction classification with actual vendor I/O and document the effective retry scope. Benefit: configured outage handling applies predictably across dependency entry paths; effort small to medium; advisory priority medium. [SPEC-DEVIATION]
**Verify:** new test needed: test_constructor_dependency_calls_honor_declared_retry_contract
**Related:** PIPE-013 owns orchestration continuation after an entry fails; ASY-009 owns the permit lifetime of dispatched sync work; CFG-012 owns constructor proxy stdout.

**Evidence — same bounded probe** *(supports: different retry guard on equivalent transient errors)*:
```text
constructor_outage_exception: ConnectTimeout
constructor_catalog_attempts: 1
ordinary_outage_exception: MaxRetriesExceeded
ordinary_catalog_attempts: 3
```

## Fix Order

- **Distribution:** CRITICAL 1; HIGH 0; MEDIUM 2; LOW 0.
- **Dependencies:** Establish replay safety before increasing retry coverage on constructor paths. Session ownership must account for in-flight sync work and vendor singleton reuse.
- **Merge candidates:** EXT-010/011 touch constructor integration but cover distinct release/retry contracts. Do not merge away either exception-path test.
- **Conflicting evidence:** AGENTS/runbooks say submission is sent once with retryable=False; current settings/models/code enable selective retry. Source comments equate all transport failures with pre-commit failures; actual ReadTimeout eligibility disproves that equivalence. Real-server duplicate behavior remains unknown.
- **Cross-phase conflicts:** CFG-012, ASY-008/009/010 and PIPE-012/013/014 retain their validated ownership. No export, queue, or secret-proxy mechanism is refiled. Phase 13 owns measured retry/resource costs.
- **Rollout risk:** EXT-009 must preserve proven safe connect-failure recovery and reconcile tests asserting that recovery; do not assume a server key/header contract. EXT-010 must avoid closing singleton-owned sessions still used by other work. EXT-011 can replay partially completed constructor catalog/auth flows and must be coordinated with release ownership and replay policy.

## Appendices

### Appendix A — Complete bounded transport-blocked probe output

Exact command:
```powershell
.\.venv\Scripts\python.exe -u -X utf8 -c "import subprocess,sys; p=subprocess.run([sys.executable,'-u','-X','utf8','.ai/tmp/audit-2026-10-21/external_probe.py'],timeout=12); print('outer_process_exit:',p.returncode)"
```
Exit 0; approximately 5.7s. `uniform` and sleep are controlled to observe replay without real waits. The simulated sender is deliberately non-deduplicating; actual Session transport is prohibited. Session objects for teardown measurements are instrumented doubles, not live socket measurements.
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
*(supports: EXT-009–011 and the explicit wire/bound coverage below; timeout absence is adjacent ASY-009 evidence, not another finding.)*

### Appendix B — Eight-block integration coverage

| Block | Current evidence and residual limits |
|---|---|
| 1: Wire/credentials | Installed get_token sends client_id/client_secret/username/password/grant_type in a form POST; API catalog/task/state/result calls use an Authorization bearer header. Synthetic constructed-call observation confirms field names/header placement without printing values. Missing credential file has an application existence guard; empty/malformed JSON fail parsing, and empty-string fields can reach blocked authentication, as phase02's isolated probe established against the same guard/vendor code. No real token or credential was requested. Proxy output is CFG-012. |
| 2: Retry/idempotence | Application retries transient exception tuple/429 and enabled 5xx up to max_attempts; permanent client errors re-raise. Submit's default retry_on restricts application HTTP-status replay but includes ambiguous read/body/connection failures (EXT-009). Vendor Retry total5/status502/503/504/allowedPOST adds another layer. Exhaustion is distinct MaxRetriesExceeded. HTTP-level deduplication remains unknown; no server key support is presumed. |
| 3: Backoff/jitter | Source formula is effective_sleep × 1.2^(attempt−1) + uniform(0,5), after uniform(0.5×sleep,2×sleep) initial smoothing. Effective sleep=max(call sleep, configured floor). It is finite for ordinary configured max_attempts; a backoff sleep also follows the last failed attempt. An awaiter-side default upper expression is 10×300s + 4s + sum(i=0..9, 2×1.2^i + 5s), assuming prompt coroutine cancellation. This is not a bound on underlying blocking transport/shutdown, already covered by ASY-009. Vendor backoff factor1 has zero jitter in installed policy; no standalone thundering-herd/load consequence is measured or filed. |
| 4: Bridge/release | Every wrapped sync operation reaches asyncio.to_thread and wait_for if call_timeout is set. Timeout/cancellation of the await does not stop its thread (ASY-009, not refiled). Requests calls themselves receive no timeout, observed with constructed synthetic calls. Session report/exception teardown is EXT-010; pool-GC fallback explicitly exists. Default thread pool size is Python-managed rather than derived from max_tasks; capacity effects are phase13. |
| 5: Blast radius | Credential files, auth/catalog endpoints, task submission/status/result, and export storage are distinct dependencies. Constructor auth/catalog failure occurs before per-task worker isolation and lacks application retry (EXT-011). In-pool network exceptions become failed tasks/error descriptions if persistence succeeds. Earlier report continuation and output-storage failures remain PIPE-013/ASY-008/PIPE-012, not new EXT claims. The constructor failure probe contrasts ordinary-call retries using the same failure class. |
| 6: Degraded results | Builder False and absent submission keys become failed tasks rather than success; FAILED/CANCELLED statuses set status false and prevent extraction. None result conversion is failure; empty raw DataFrame is a distinct no-data condition. Vendor missing slice/statistic fields can map to '-' / None, but no independent API completeness contract was established, so these are not asserted as corrupt substituted results. Naturally empty prepared DICT output was rejected as a defect in phase04 and is not refiled. |
| 7: Logging | Args/kwargs redacted by default, explicit log_errors_detail opt-in enables repr; retry/exhaustion/permanent-error outcomes have logging. Some vendor exception text and patched non-JSON response logs include raw response text; no blanket proof that a server-reflected secret is sanitized is claimed. CFG-012 is the observed synthetic proxy-secret output seam. No current real secret exposure or absent-metrics-only finding is invented. Worker logs identify task failures even where current network logging lacks propagated task_name. |
| 8: Declared mechanisms | AGENTS/runbooks' single-attempt POST claim is contradicted by defaults/current submit path (EXT-009). Source/model claims that all selected transport errors occur before commitment are contradicted by ReadTimeout and the simulator. The documented local/no-network constructor claim is contradicted by actual catalog initialization (EXT-011). Timeout wording bounds the await, not wire/thread execution (ASY-009). check_version=False is passed by get_connection, so no version-check network claim is inferred. |

Installed primary-source ownership context: `MediaVortexTask.__init__` creates its task `MediascopeApiNetwork` and `MediaVortexCats`; catalog initialization creates its own network and fetches properties/units. Both task/catalog classes retain singleton instances. `MediascopeApiNetwork.get_token`, `send_request`, `_raise_error` and `MediaVortexTask.send_task/get_status/get_result/result2table` define the observed boundary. These installed files are context rather than tracked application anchors.

Authoritative source cross-check: Requests documents that omitted timeout can leave requests waiting indefinitely, consistent with the constructed argument observation. The application-level wait_for is a separate awaiter mechanism, not proof of a Requests transport timeout. [Requests timeout documentation](https://requests.readthedocs.io/en/latest/user/quickstart/#timeouts)

### Appendix C — Preservation and anchor checks

Tracked production paths were confirmed with git ls-files; primary symbols/disambiguators resolve in the current dirty source. No finding uses a line-number locator. `EXT-009` primary disambiguator is the configured retry_on selection inside send_task; `EXT-010` is the actual get_connection constructor call; `EXT-011` is the service's api_call=False connection branch. Existing historical findings/validation were inspected only for namespace/provenance, not copied as present conclusions. No application, tests, dependencies, settings, docs, plans, `.kilo` or Git index changes were made.
