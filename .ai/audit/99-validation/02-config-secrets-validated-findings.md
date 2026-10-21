---
phase: "02"
phase_name: "config-secrets"
id_prefix: "CFG-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/02-config-secrets/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Validation Report

## Summary

Both reported HIGH findings are substantiated. Omitted client configuration removes an intended request restriction, and an authenticated proxy setting exposes its password through console output. Both need remediation; the proxy fix requires a concrete dependency change and verification before adoption.

## Scope

**Input:** Frozen `.ai/audit/02-config-secrets/findings.md`; two claims, independently adjudicated on 2026-10-21.
**Examined:** Current models, service merge/connection entry, task strategy and builder call, default filter, settings architecture contract, relevant merge tests, installed vendor constructor/filter builder, inspected synthetic probe, shared report template and identifier history. Existing phase 01 raw and validated reports were compared for ownership and systemic validation issues.
**Snapshot:** Current dirty working tree on the stated HEAD, not pristine HEAD. Initial evidence remains `.ai/tmp/audit-2026-10-21/initial-hashes.json` and `initial-git-status.txt`; externally changed async-concurrency/data-flow documentation was preserved. Settings-layer requirements were checked against the actual merge path; findings do not depend on those concurrent edits.
**Tools:** Read-only PowerShell, rg, Git object/path inspection, AST, installed package metadata, existing venv Python. No dependency installation or synchronization.
**Not verified:** Live authentication, remote calculation/results, production credential validity, deployment access controls, a fixed vendor release or full lint/type/test suite. The validator inspected and reran the existing synthetic-only probe; all attempted request transport was blocked. Direct vendor network-object construction only initializes its local session, as independently verified in source.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| CFG-011 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings` | Omitted client filter removes the subtype restriction |
| CFG-012 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.get_connection` | Authenticated proxy user information is printed |

| ID | Disposition | Final type | Final severity | Recommendation outcome |
|---|---|---|---|---|
| CFG-011 | confirmed | SPEC-DEVIATION | HIGH | Actionable with explicit field-set handling below |
| CFG-012 | confirmed | SPEC-DEVIATION | HIGH | Raw fix substantiated but unusable; concrete dependency approach supplied below |

#### CFG-011 — [HIGH] Omitted client filter removes the subtype restriction

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
**Anchor:** `src/mko_get_mediascope_data/core/models.py::ReportSettings`
**Anchor:** `src/mko_get_mediascope_data/settings/defaults/tv/crosstab.json5::"TOP_NAT_TV_ADVERTISERS"`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate`
**Disambiguator:** `report_settings.model_dump().items()`
**Checked:** ran
**Why this band:** The phase HIGH band covers merged values reaching the API consumer without the validation needed to prevent silent selection corruption. Schema validation does run, but does not validate omission semantics: a meaningful constraint is silently replaced with null and the builder omits that filter. This is more than MEDIUM ineffective configuration because request selection changes. No CRITICAL secret or bypassed startup gate is established.
**Problem:** An omitted client ad_filter becomes ReportSettings' synthesized None, and model_dump includes that value. The service overlays it onto the nonempty subtype ad_filter, even though the intended shallow merge keeps omitted client keys at their selected default.
**Repro:** Inspect then execute `.ai/tmp/audit-2026-10-21/config_probe.py` using the existing venv. Its real service merge loads copied bundled defaults for TV/TOP_NAT_TV_ADVERTISERS; the report's model_fields_set excludes ad_filter. The real strategy yields a task with None. Appendix A contains repeated output.
**Impact:** The generated request no longer restricts advertisements by the configured status/type expression. Sellers can receive different scope/totals if that task is submitted; actual remote totals were not measured.
**Root cause:** The overlay treats synthesized field defaults as explicit client choices. Data permits ad_filter as an extra and validates no inheritance invariant. DefaultTVTaskStrategy serializes the lost value; TVMediaReport.send_task passes task.settings to its vendor builder; the installed builder's add_filter returns immediately for None.
**Type rationale:** SPEC-DEVIATION: settings-layers-architecture explicitly states that an omitted CLIENT key keeps its default and that shallow replacement is intentional. The code must preserve that contract; rewriting the documented default semantics would change report meaning.
**Fix:** In AppService.merge_data_settings, derive client overlays from `report_settings.model_dump(exclude_unset=True)` while retaining the existing selected-base-key guard and shallow replacement order. Keep explicit null included; do not use exclude_none. Preserve field values populated by callers, subtype selection, API extras and language conversion. Assess other model defaults that overlap selected data keys before broadening the implementation.
**Verify:** new test needed: test_omitted_ad_filter_preserves_subtype_and_explicit_null_overrides. Cover omitted, explicit null and explicit nonempty ad_filter; check generated task settings as well as merged values. Keep existing subtype merge coverage.
**Related:** Phase 05 owns template schema conformance; phase 02 owns the omitted-value overlay semantics. This is not historical CFG-007's absent subtype-block concern.
**Anchor/cause decision:** All four raw semantic anchors resolve; both source symbols and literal subtype token exist. The primary disambiguator occurs inside the merge method. Tracked source paths were checked with git ls-files. The repeated runtime result independently supports the asserted cause.
**Recommendation assessment:** Stable existing target, no prerequisite dependency, explicit null behavior retained. Whole-value replacement remains intentional; a deep merge or global prohibition on API extras would not address the actual cause.

#### CFG-012 — [HIGH] Authenticated proxy user information is printed

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Disambiguator:** `settings_filename=cred_path`
**Checked:** ran
**Why this band:** Phase 02 explicitly assigns secret values reaching output HIGH. Synthetic proxy user information reaches captured stdout through the real application entry. No unauthorized access to a real credential file, actual compromised production secret or cross-environment sharing is demonstrated, so CRITICAL is not established.
**Problem:** A supported proxy_server URL containing user information is interpolated verbatim into vendor constructor stdout. get_connection reaches that constructor without an output-safe dependency boundary.
**Repro:** The inspected probe writes only synthetic credentials under scratch. It captures the real vendor constructor and real AppService.get_connection output separately and tests secret presence using booleans. Session.request is patched to raise before every attempted outbound request in the application path. Both captured streams contain the synthetic password.
**Impact:** Proxy passwords can persist in terminal capture, notebook cells or scheduler output, regardless of application logger/error-argument redaction. A later failed authentication does not erase the earlier output.
**Root cause:** Installed mediascope-api-lib 1.8.3 `mediascope_api/core/net.py::MediascopeApiNetwork.__init__` sets self.proxies and unconditionally prints that mapping when proxy_server is supplied. `MediaVortexTask.__init__` reaches it before catalog/authentication work; check_version=False only skips the version check. The application cannot redact already emitted text through its ordinary logging policy.
**Type rationale:** SPEC-DEVIATION retained: secrets-free configuration consumption is the phase's credential-seam contract. The defect is downstream but is reachable from the supported application constructor boundary; no claim that the print originates in tracked application source is made.
**Fix:** Patch the dependency's `MediascopeApiNetwork.__init__` proxy branch to remove the mapping print or replace it with a message containing no URL user information. Consume a verified release containing that exact correction through project dependency/lock resolution, then verify AppService.get_connection against the installed fixed constructor, including constructor failure. No fixed release is asserted to exist. If a maintained dependency patch is chosen before upstream release, its packaging/ownership must be explicit before implementation approval. Do not use process-global stdout redirection or monkeypatch builtins as a production fix in the threaded API bridge.
**Verify:** new test needed: test_get_connection_does_not_emit_authenticated_proxy_password; use synthetic credentials, block transport, capture stdout/stderr/logs on both success and constructor failure, and verify proxy routing remains configured.
**Related:** Phase 15 may cross-reference lifecycle; phase 09 may cross-reference the vendor seam. Secret config-to-output exposure is owned by phase 02. Missing-settings recovery stays ENT-009.
**Anchor/cause decision:** The primary tracked symbol and settings_filename token resolve. Installed dependency symbols are evidence context, not misrepresented as tracked repository anchors. Independent source inspection and repeated capture agree on the exact emitting branch.
**Recommendation assessment:** The raw imperative only states a desired absence of output and names no mechanism; under phase 99 it is substantiated but unusable as filed. The dependency-level change above supplies the missing implementation target and order. It is actionable as dependency remediation planning; adoption is conditional on a verified fixed artifact, not a claimed available release. See VAL-009.

## Fix Order

- **Distribution:** Actionable content findings: CRITICAL 0; HIGH 2; MEDIUM 0; LOW 0. Both confirmed unchanged in type and grade; recommendations are concretized above.
- **Disposition tally:** confirmed 2; re-typed 0; re-graded 0; merged 0; not substantiated 0; unsettled 0. Two dispositions for two input IDs.
- **Dependencies:** CFG-011 is independent. CFG-012 requires a dependency fix, a verified packaged artifact and then application dependency adoption/verification in that order. No available fixed version is assumed.
- **Merge candidates:** None; client omission and vendor secret output have different causes.
- **Conflicting evidence:** Pydantic success does not validate semantic inheritance. The existing subtype overlay test deliberately excludes ReportSettings fields from its assertions, so it does not contradict the lost ad_filter observation. No inspected test asserts that omission must erase ad_filter or that secret output is intended.
- **Cross-phase conflicts:** Compared current phase 01 raw and validated reports. ENT-009 owns missing-settings startup diagnostics, distinct from proxy output and data-value merge. Phase 05 template conformance and phase 09 transport contracts remain adjacent owners, not duplicates. No other current sibling report was available for comparison.
- **Rollout risk:** CFG-011 must preserve explicit null, nonempty replacement and permitted extras, without switching to deep merge. CFG-012 must preserve proxy routing, vendor errors and ordinary authentication behavior; runtime stdout interception is unsafe as a concurrent application workaround. Ordinary tests that mock the vendor constructor cannot prove output safety of the installed dependency.
- **Required actions:** Accept the two factual findings and HIGH/HIGH priority; use the concrete recommendations above. A dependency repair remains a separate implementation/dependency task and requires verified artifact availability before rollout.

## Validation-Level Findings

Separate scale: phase 99 grades the audit, not the application findings. Existing systemic VAL-007 and VAL-008 remain recorded in phase 01 validation and are referenced, not refiled or counted here.

| ID | Severity | Anchor | Title |
|---|---|---|---|
| VAL-009 | MEDIUM | `.ai/audit/02-config-secrets/findings.md::"#### CFG-012"` | Proxy recommendation lacks an implementation mechanism |

#### VAL-009 — [MEDIUM] Proxy recommendation lacks an implementation mechanism

**Anchor:** `.ai/audit/02-config-secrets/findings.md::"#### CFG-012"`
**Disambiguator:** `supported proxy-credential path`
**Checked:** static
**Why this band:** Phase 99 MEDIUM describes a report the reader must repair before acting. The raw finding is true, but its fix only restates the desired outcome; no code-level mechanism or dependency rollout path is selected.
**Problem:** The emitting print belongs to the installed vendor constructor, while the raw fix merely says to ensure the application path does not emit credentials. This leaves the reader to invent a dependency repair, unsupported interception workaround or other implementation approach.
**Impact:** An implementor cannot execute the recommendation as written without resolving architecture and artifact prerequisites; a process-wide output workaround could interfere with concurrent use.
**Root cause:** The recommendation names the application's call seam but not the vendor symbol that must change or how its correction reaches the installed dependency.
**Fix:** Use CFG-012's concrete vendor-constructor correction and verified-artifact adoption sequence in this validated report. Do not edit the frozen raw input or assume a fixed release exists. Revisit artifact availability before approving implementation/rollout.
**Verify:** new check needed: confirm the remediation plan names MediascopeApiNetwork.__init__, identifies the fixed packaged dependency artifact and preserves proxy routing/error propagation.
**Related:** CFG-012.

## Appendices

### Appendix A — Independently repeated runtime evidence

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/config_probe.py` (exit 0). The script was inspected first: scratch-only synthetic data, no user credential store reads, and attempted request transport intercepted before I/O. Secret checks emit booleans; neither captured secret text nor real credential values are included below.

```text
user_edit_preserved: True
subtype_ad_filter_nonempty: True
report_explicitly_sets_ad_filter: False
merged_ad_filter: None
generated_task_ad_filter: None
type_mismatch_fields: [('slices', 'dict[str,any]'), ('slices', 'list[any]')]
misspelled_max_attempt_retained: False
misspelled_max_attempt_effective_attempts: 10
permissive_data_input: {'slices': [42]} accepted: {'options': [], 'statistics': [], 'slices': [42]}
permissive_data_input: {'options': ['kitId', 7]} accepted: {'options': ['kitId', 7], 'statistics': [], 'slices': []}
credential_state: absent result: FileNotFoundError mentions_init: True
credential_state: empty result: JSONDecodeError mentions_init: False
credential_state: malformed result: JSONDecodeError mentions_init: False
credential_state: empty_fields result: AssertionError mentions_init: False
intercepted_auth_empty_fields: ['client_id', 'client_secret', 'password', 'username']
proxy_secret_in_constructor_stdout: True
proxy_secret_in_get_connection_stdout: True
bundled_report_entries_validated: 34
bundled_validation_failures: []
bundled_unconsumed_extra_names: []
```

Only the lost-filter and captured-secret observations support CFG-011/012. The other probe results describe bounded methodology; they do not establish broad credential validity or absence of all ineffective configuration.

### Appendix B — Independent source and support adjudication

- **Executing value path:** AppService.get_data_settings calls get_default_data_settings, merge_data_settings and Data validation. TOP_NAT_TV_ADVERTISERS defines `adIssueStatusId = R and adTypeId IN (1, 23, 25)`; ReportSettings defines ad_filter=None. The client's model_fields_set excludes it, but unrestricted model_dump contains it. DefaultTVTaskStrategy.generate serializes Data into TVTask settings. TVMediaReport.send_task calls self.builder with task.settings. The installed vendor builder's filter helper returns for None, proving missing selection restriction without claiming observed remote corruption.
- **Executing secret path:** AppService.get_connection passes the resolved settings path to MediaVortexTask(check_version=False). That constructor initializes MediascopeApiNetwork, which loads proxy_server from the synthetic JSON and prints self.proxies before initializing its session. Subsequent catalog/authentication work is where the transport guard stops the application probe. Proxy presence is supported by actual vendor loading, not merely an invented application field.
- **Independent requirements:** `docs/00-overview/settings-layers-architecture.md::"Override rules (explicit):"` says omitted client keys keep defaults; it also specifies shallow replacement and allowed API extras. This semantic requirement and the nonempty shipped filter are load-bearing; changing documentation to endorse accidental None would discard the selected report restriction. The document's source-code walkthrough is not treated as proof of correct behavior.
- **Green-control separation:** All 34 bundled report entries/merged Data values passed shape validation in the probe, but none of that proves preserved omitted-client semantics. The mutation-specific omission observation decides CFG-011. Boolean capture on the real constructor path decides CFG-012. No green checklist row decides either claim.
- **Existing test scope:** `tests/test_services.py::test_merge_data_settings_applies_subtype_overlay` exists and loops over subtype keys, but deliberately skips keys present in ReportSettings.model_fields. For the ad_filter at issue, assertion count is zero. It is therefore not a regression guard for this finding, even if it passes. Constructor mocks in connection tests similarly cannot exercise the downstream print. No such test was run or cited as confirming either claim.
- **Phase checklist dependence:** CFG-011 maps to phase block 3's merge-preservation questions and block 5's derived-consumer validity. CFG-012 maps to blocks 2/4's secret-output seam and guard behavior. Both use independent runtime observations in those enumerated angles. No finding in this input requires a previously unnamed angle; this is a scope conclusion, not a control verdict.
- **Limits:** No remote task was submitted, no output CSV or totals were compared, and no real proxy credential was examined. A synthetic secret proves a reachable output mechanism, not a past production compromise. The accepted scope is current request-generation and secret-output behavior.

### Appendix C — Namespace and controlled-artifact ruling

- **Declared/minted:** Phase 02 declares CFG-. Historical phase 02 reports at HEAD minted CFG-001..010; current CFG-011/012 continue that namespace and preserve their input IDs. Historical id_prefix metadata spelled CFG without a trailing hyphen; that is not a second finding namespace.
- **Shipped provenance:** Search of src/tests/docs found eight CFG identifier occurrences: current-behavior has CFG-003, CFG-001 and CFG-010; product-overview and open-questions each have CFG-003; test_services has CFG-004, CFG-006 and CFG-007. They are historical remediation references, not new current defects. Keep their identities and qualify historical report/commit context during future provenance maintenance; do not recycle those IDs.
- **Compound/reuse ruling:** No phase-qualified compound CFG identifier occurs in the input; no other current phase claims CFG. Historical CFG-007's missing-subtype observation is distinct from CFG-011's omitted ad_filter overlay. Continue the declared namespace rather than inventing a second prefix. VAL-009 continues after VAL-006..008 from phase 01; historical shared VAL collisions are already owned by VAL-008, not duplicated here.
- **Controlled prefix coverage:** The controlled template derives its prefix from phase workflows and contains no closed prefix list. Registry/workflow enumeration is the authority; no absent-prefix finding applies. Phase 02's equivalent declaration is its explicit CFG- instruction rather than the exact phrase Finding-ID prefix.
- **Severity pointer/metadata:** The template's malformed generated pointer remains systemic VAL-007 in phase 01 validation. Phase 02 raw carries the exact severity_taxonomy field once and points to its actual workflow; missing 0, renamed 0. This report uses the same correct pointer and validated status. No shared template was repaired.
- **Required-field retention:** Both raw findings supply semantic anchors, primary disambiguator, checked mode, band rationale, problem, reproduction, impact, cause, fix, verify and related fields. This report retains those fields, both original IDs and one disposition each, with type, grading, recommendation usability and evidence limits. The header identifies HEAD while Scope explicitly records current dirty state; no pristine-snapshot claim is made.
- **Cross-report state:** Current phase 01 raw and validated reports were compared. Their startup diagnostic finding remains ENT-009 and is not duplicated here. Historical sibling reports accessed as Git objects are provenance only; no silently edited or silently merged finding exists. Systemic VAL-007/008 are references, excluded from this report's new VAL count.

### Appendix D — Mechanical checks and acceptance

Existing venv Python AST/metadata check (exit 0):

```text
semantic_anchors_resolved: 4
vendor_version: 1.8.3
default_filter_token: True
```

The four resolved raw anchors are AppService.merge_data_settings, AppService.get_connection, ReportSettings and DefaultTVTaskStrategy.generate. Their paths and the default file are tracked. Both primary disambiguators occur inside their respective methods. Installed dependency anchors were independently inspected as dependency evidence. Literal subtype and VAL-009 raw-heading/token anchors resolve.

Final report-text consistency check (existing venv Python, exit 0):

```text
dispositions: {'confirmed': 2}
unique_input_ids: ['CFG-011', 'CFG-012']
content_headers: [('CFG-011', 'HIGH'), ('CFG-012', 'HIGH')]
validation_headers: [('VAL-009', 'MEDIUM')]
VAL_disambiguator_resolves: True
line_locators: []
```

No factual claim remains unsettled within the bounded accepted scope. Ready for tech-lead acceptance: two confirmed HIGH findings, one new MEDIUM validation issue and references to existing systemic VAL-007/008. CFG-012's raw remediation is not executable unchanged; use the concrete vendor-symbol correction and artifact prerequisite here. A fixed vendor release is unverified, so dependency adoption/rollout is not approved by this validation. No application, test, dependency, configuration, documentation, input finding or shared-template edit was made.
