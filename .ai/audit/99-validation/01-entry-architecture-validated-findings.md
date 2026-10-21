---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".agents/skills/01-entry-architecture/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Validation Report

## Summary

Both reported problems are substantiated and actionable. The notebook guidance directs users to an entry point that cannot run in their active event loop, and missing settings prevent commands from presenting recovery guidance. Both have HIGH startup impact under the audited phase's rubric; the first requires documentation correction and the second requires entry-layer error handling.

## Scope

**Input:** `.ai/audit/01-entry-architecture/findings.md`, frozen two-finding report; every finding adjudicated independently on 2026-10-21.
**Examined:** Current dirty working tree, not pristine `code_ref`; `app.py`, service bootstrap, paths, init service, JSON5 reader, public re-exports, README, current-behavior/system-architecture documentation, relevant tests, CI, phase workflows, controlled template and historical identifier records at HEAD. Initial tree evidence remains `.ai/tmp/audit-2026-10-21/initial-hashes.json` and `initial-git-status.txt`.
**Tools:** Read-only PowerShell, `rg`, Git object inspection, AST resolution and existing venv Python. The inspected isolated runtime probe was rerun; no dependencies were installed or synchronized.
**Not verified:** Real Jupyter kernel, live API, exports or full lint/type/test suite. A real asyncio-loop result is reused from the auditor's approved outside-sandbox measurement after independently inspecting its script and the executing path; validator runtime repetition used the explicit CPython running-loop guard simulation. No claim about native kernel execution or independent repetition of the real-loop measurement is made.
**Sibling comparison:** The only present raw content report is phase 01; no current sibling validated report exists. Deleted historical raw/validated reports were inspected as Git objects for provenance and historical cross-phase ownership, not treated as present-state findings. Worker semantics remain phase 03; blocking transport remains phase 09; settings values/credentials remain phase 02.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| ENT-008 | HIGH | `src/mko_get_mediascope_data/app.py::run_report` | Notebook guidance selects an incompatible wrapper |
| ENT-009 | HIGH | `src/mko_get_mediascope_data/app.py::run` | Missing-settings startup bypasses recovery diagnostics |

| ID | Disposition | Original type / severity | Final type / severity |
|---|---|---|---|
| ENT-008 | re-typed | SPEC-DEVIATION / HIGH | DOC-UPDATE / HIGH |
| ENT-009 | re-graded | SPEC-DEVIATION / MEDIUM | SPEC-DEVIATION / HIGH |

#### ENT-008 — [HIGH] Notebook guidance selects an incompatible wrapper

**Anchor:** `src/mko_get_mediascope_data/app.py::run_report`
**Anchor:** `src/mko_get_mediascope_data/app.py::run_report_async`
**Anchor:** `README.md::"Public API (Jupyter, Airflow, scripts)"`
**Disambiguator:** `asyncio.run(run_report_async(report_path, progress=progress))`
**Checked:** ran
**Why this band:** The advertised notebook startup contract fails in an active host loop: the phase explicitly assigns failed startup contracts HIGH. LOW documentation drift applies only without runtime consequence; CRITICAL import corruption or unavoidable import I/O is absent from this claim.
**Problem:** The synchronous wrapper's docstring explicitly advertises a Jupyter call; README presents the same wrapper under Jupyter/Airflow/scripts. That call raises before report validation inside a running loop and leaves its newly created coroutine unawaited.
**Repro:** Rerun `.ai/tmp/audit-2026-10-21/entry_probe.py` using existing venv Python; it marks a running loop explicitly and calls the real wrapper. Observed RuntimeError and RuntimeWarning are reproduced in Appendix A. The separate real-loop script runs the same call from an async function and supports the stronger observation in the frozen input.
**Impact:** Notebook users copying the advertised example cannot begin their report and receive an additional coroutine warning.
**Root cause:** Documentation conflates synchronous and active-loop hosts. The unconditional `asyncio.run` causes the observed rejection; using it for ordinary synchronous hosts is correct. The explicit async public wrapper already provides the intended host-owned-loop path.
**Type rationale:** DOC-UPDATE: the public split into sync and async wrappers is load-bearing and matches ordinary Python usage. No specification requires making the synchronous API block inside a host loop. The correction must direct notebook users to `await run_report_async(...)`; the code need not acquire a nested-loop mechanism.
**Fix:** Update `app.run_report`'s docstring and README public API example to distinguish synchronous scripts/Airflow tasks from active-loop notebooks; show `await run_report_async(existing_report_path)` for the latter. If adding a diagnostic guard in `app.run_report`, check the running loop before constructing its coroutine and name `run_report_async` in the message. This is supplemental API usability work, not a requirement to implement nested synchronous execution.
**Verify:** new test needed: test_run_report_in_running_host_loop_has_actionable_contract; preserve `tests/test_app.py::test_run_report_sync_wrapper` for ordinary synchronous delegation and public import identity tests. Validate notebook guidance uses the existing async function.
**Related:** None; phase 01 owns documented entry selection and loop ownership at its boundary.
**Anchor/cause decision:** AST resolves both entry functions; the exact disambiguator occurs inside `run_report`. README heading resolves literally. The simulated observation and inspected real-loop measurement agree with the executing CPython guard; this is independent causal verification, not reliance on a green test.
**Recommendation usability:** Existing targets, no prerequisite change, no new dependency. Updating examples removes the advertised incompatible call; any optional guard must preserve ordinary sync behavior. The raw recommendation is usable after the explicit documentation targets above are named.

#### ENT-009 — [HIGH] Missing-settings startup bypasses recovery diagnostics

**Anchor:** `src/mko_get_mediascope_data/app.py::run`
**Anchor:** `src/mko_get_mediascope_data/app.py::list_reports`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.app_config`
**Disambiguator:** `app_service = get_app_service()`
**Checked:** ran
**Why this band:** Phase block 4 requires an actionable missing-settings startup message; the phase's HIGH band covers a startup contract that does not hold. The observed command fails before its error boundary. MEDIUM's environment divergence or unnecessary harmless import effects do not describe this path. The ability to recover manually limits blast radius but is not the rubric's downgrade criterion. CRITICAL silent fallback is absent.
**Problem:** With an existing report and missing user settings, `run` calls service bootstrap before its `try`; `list_reports` also calls bootstrap without contextual handling. Both propagate FileNotFoundError without the available init recovery instruction.
**Repro:** The inspected probe patches `services.APP_PATHS` to its nonexistent scratch directory and invokes both real commands through `CliRunner`; both exit 1 with FileNotFoundError. The run output/exception lacks `init`; separate scratch-only initialization succeeds. Appendix A records the repeated observation.
**Impact:** First-time or damaged-settings users cannot start these commands and must discover the recovery action independently; direct invocation may render a traceback instead of the intended CLI error presentation.
**Root cause:** `get_app_service` constructs `AppService`, whose constructor calls `prepare_app`, whose cached `app_config` reads the required logging file first. `utils.json5_to_dict` logs and re-raises FileNotFoundError; the command boundary is reached only after bootstrap. The cause survives independently of doc claims that this error is already caught.
**Fix:** Give `app.run` and `app.list_reports` an entry-layer bootstrap error boundary. For absent required settings, display the failed settings path and `mko-get-mediascope init`, then exit nonzero. Keep malformed-settings errors distinct; do not create fallback configuration or absorb unrelated exceptions. Resolve pre-config verbosity explicitly rather than referring to an unassigned mode.
**Verify:** new test needed: test_cli_missing_settings_explains_init_recovery; preserve `tests/test_app.py::test_cli_run_missing_file` and `tests/test_app.py::test_cli_init_invokes_init_project`.
**Related:** None; phase 02 is adjacent for settings values/credential provenance, not ownership of this bootstrap diagnostic.
**Anchor/cause decision:** AST resolves `run` and `list_reports`; `AppService.app_config` resolves as a cached-property function. The primary token is inside `run` before its try. All three source paths are tracked. The repeated command failure directly establishes the claim.
**Type rationale:** SPEC-DEVIATION retained: missing-file recovery is the entry bootstrap contract assessed in phase block 4. The current-behavior text claiming the ordinary CLI error boundary handles this failure is inaccurate; code movement is necessary to deliver that contract.
**Recommendation usability:** Both existing command symbols are named and actionable. No cross-finding prerequisite. Error handling must cover bootstrap before progress setup and before configured verbosity is available; `init` remains settings-free.

## Fix Order

- **Distribution:** Actionable content findings: CRITICAL 0; HIGH 2; MEDIUM 0; LOW 0. Validation findings below use a different scale.
- **Disposition tally:** confirmed unchanged 0; re-typed 1; re-graded 1; merged 0; not substantiated 0; unsettled 0. Total 2 of 2. Both claims are substantiated; neither is approved as originally typed and graded.
- **Dependencies:** None between ENT-008 and ENT-009. Define the supported-host wording before publishing notebook examples. Bootstrap error handling precedes mode-dependent error presentation.
- **Merge candidates:** None; independent causes. No deletion or silent merging of historical reports is authorized.
- **Conflicting evidence:** Current-behavior documentation claims missing-config errors reach the normal printed CLI boundary; the executed path refutes that claim. Treat documentation as evidence of the intended user-facing contract, not proof that code meets it.
- **Cross-phase conflicts:** No current sibling finding conflicts were available. Historical eager-import findings are obsolete against current lazy bootstrap and are not merged into ENT-008/009. Async worker lifecycle belongs to 03; API bridge/resource semantics to 09; credential values to 02.
- **Rollout risk:** ENT-008 must retain ordinary sync delegation and async API identity. ENT-009 must preserve Typer argument validation, settings-free init and nonzero failure exits; clarify pre-config verbosity behavior when implementing the boundary.
- **Required corrections before relying on raw input:** Use this report's final type/severity and concrete targets; do not carry the raw MEDIUM into the remediation roadmap. Application changes remain separate authorized work.

## Validation-Level Findings

These findings grade audit artifacts using phase 99's scale. They are excluded from the ENT severity distribution and disposition tally.

| ID | Severity | Anchor | Title |
|---|---|---|---|
| VAL-006 | HIGH | `.ai/audit/01-entry-architecture/findings.md::"Why this band:"` | Startup diagnostic is downgraded outside its own rubric |
| VAL-008 | MEDIUM | `tests/test_models.py::test_tv_report_template_entry0_has_options` | Shipped validation provenance uses a reused identifier |
| VAL-007 | LOW | `.codex/templates/audit-findings.md::"severity_taxonomy:"` | Controlled template generates a nonexistent severity path |

#### VAL-006 — [HIGH] Startup diagnostic is downgraded outside its own rubric

**Anchor:** `.ai/audit/01-entry-architecture/findings.md::"#### ENT-009"`
**Disambiguator:** `rated MEDIUM by its limited effect`
**Checked:** static
**Why this band:** Phase 99 HIGH covers a true finding graded below its owning rubric, which would suppress its roadmap priority. This is a grading defect, not a false factual claim or wrong approval.
**Problem:** The raw report excludes HIGH because configured runs remain usable and init exists, but the owning rubric expressly grades failed startup contracts HIGH and its MEDIUM clauses do not match this defect.
**Impact:** Remediation receives a lower priority than the phase's agreed scale assigns.
**Root cause:** The raw decision substitutes recovery availability for the phase rubric's startup-contract criterion.
**Fix:** In future phase 01 grading, apply the workflow's HIGH startup-contract criterion to this failure; adopt ENT-009's re-graded disposition here. Do not edit the frozen input from this validation run.
**Verify:** new check needed: compare ENT-009 final severity with phase 01 Severity Taxonomy before roadmap acceptance.
**Related:** ENT-009.

#### VAL-008 — [MEDIUM] Shipped validation provenance uses a reused identifier

**Anchor:** `tests/test_models.py::test_tv_report_template_entry0_has_options`
**Disambiguator:** `VAL-003: nat_tv_tvreport.json5 entry[0]`
**Checked:** static
**Why this band:** Phase 99 MEDIUM includes an identifier resolving to multiple findings. The ambiguity exists in shipped provenance even though historical report files are currently deleted in the working tree.
**Problem:** Historical validation reports at HEAD reuse VAL-001 through VAL-005 per phase. VAL-003 names distinct phase 03, 05, 09, 10 and 13 observations; the shipped model test retains an unqualified VAL-003 marker for the phase 05 options observation.
**Impact:** A reader cannot resolve the test's validation source from its ID alone; historical links and remediation traceability require manual disambiguation.
**Root cause:** A shared VAL namespace was allocated independently by per-phase reports.
**Fix:** In a separately authorized provenance maintenance change, qualify the model test marker with its historical phase 05 report path and VAL-003 ID. Retain original IDs, add explicit source qualification to historical citations, and allocate new family-wide VAL identifiers monotonically. Do not invent a new compound ID namespace or rewrite historical findings in this run.
**Verify:** new check needed: resolve every shipped VAL marker to one phase/report/ID tuple; ensure newly allocated VAL-006..008 have no historical collision.
**Related:** Namespace ruling in Appendix B.

#### VAL-007 — [LOW] Controlled template generates a nonexistent severity path

**Anchor:** `.codex/templates/audit-findings.md::"severity_taxonomy:"`
**Disambiguator:** `.codex/audit-phases.md{NN}-audit-{phase-name}.md#severity-taxonomy`
**Checked:** static
**Why this band:** Phase 99 LOW covers template metadata that does not resolve. The input already supplies the correct workflow path, so no content-finding remediation is blocked today.
**Problem:** Substituting phase 01 into the controlled template yields `.codex/audit-phases.md01-audit-entry-architecture.md`, not the actual workflow or registry path.
**Impact:** Future generated reports can point readers to a nonexistent grading reference.
**Root cause:** Migration replaced a directory prefix with a registry filename while retaining the concatenated legacy phase filename pattern.
**Fix:** In separate template maintenance, change `.codex/templates/audit-findings.md`'s `severity_taxonomy` field to the actual `.agents/skills/{NN}-{phase-name}/references/workflow.md#severity-taxonomy` pattern; verify every registered phase substitution. This validation leaves the controlled template unchanged.
**Verify:** new check needed: resolve generated severity_taxonomy paths for every registry phase.
**Related:** None.

## Appendices

### Appendix A — Independently repeated isolated runtime evidence

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/entry_probe.py` (exit 0). The script was inspected before execution: it invokes real entry functions, patches only settings locations into scratch, and uses the CPython running-loop guard explicitly. It calls no vendor API.

```text
import_state_unchanged: True
singleton_after_import: None
missing_config_exit: 1
missing_config_exception: FileNotFoundError [Errno 2] No such file or directory: 'C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5'
missing_config_cli_output: "No such file or directory: C:\\py_dev\\mko_get_mediascope_data\\.ai\\tmp\\audit-2026-10-21\\entry-missing\\settings\\log_config.json5 — [Errno 2] No such file or directory: 'C:\\\\py_dev\\\\mko_get_mediascope_data\\\\.ai\\\\tmp\\\\audit-2026-10-21\\\\entry-missing\\\\settings\\\\log_config.json5'\n"
missing_config_init_hint: False
missing_list_exit: 1
missing_list_exception: FileNotFoundError
isolated_init_exit: 0
isolated_init_app_config_exists: True
isolated_init_log_config_exists: True
running_loop_exception: asyncio.run() cannot be called from a running event loop
running_loop_warnings: ["coroutine 'run_report_async' was never awaited"]
```

The first two lines describe setup, not approval of all import safety. The guard simulation is narrower than a real Jupyter kernel; the frozen input separately records the approved real asyncio-loop measurement. Service bootstrap is never reached by the rejected host-loop call.


### Appendix B — Namespace, template and support rulings

- **ENT declaration/history:** The current workflow declares `ENT-`; historical phase 01 raw and validated reports at HEAD minted ENT-001..007. Current ENT-008/009 continue that same namespace without collision. Historical `id_prefix: "ENT"` is a metadata spelling difference, not a second identifier namespace.
- **ENT shipped provenance:** Search of `src`, `tests`, `docs` found three markers: ENT-004 in test_app, ENT-006 in test_services, ENT-001 in current-behavior. They refer to the historical remediation baseline; keep them qualified by their historical report/commit when future edits require resolution. Do not retire them as arbitrary drift or reuse their IDs. No phase-qualified compound ENT identifier is present in the current input, and no current sibling content phase reuses ENT.
- **VAL declaration/history:** Phase 99 declares VAL-. Historical validation reports at HEAD independently minted VAL-001..005, reusing at least VAL-001 across phases. One shipped VAL marker exists in the searched source/test/docs corpus: `tests/test_models.py::test_tv_report_template_entry0_has_options`, for historical phase 05 VAL-003. Its test expectation makes this provenance meaningful. Preserve IDs and repair qualification separately; do not pretend the same bare ID has one global historical meaning. This report allocates VAL-006..008 after checking the historical family maximum. These exact IDs and systemic issues should be referenced, rather than minted again, by later phase validations. No new phase-qualified compound identifier is introduced; a report path plus unchanged ID supplies disambiguation.
- **Controlled prefix list:** The controlled template has no closed prefix enumeration; it explicitly derives the prefix from each workflow. Its QLT example rows are examples, not a registry. Thus there is no absent-prefix finding. Registry contains the thirteen content phases and phase 99; workflows are the authority for ENT, CFG, ASY, PIPE, TPL, EXT, QLT, TST, OPS, PERF, AUT, OUT, NUM and VAL.
- **Template severity pointer:** The controlled pattern is malformed, as VAL-007 records. The one current raw content report carries the exact `severity_taxonomy` field once, with a resolvable phase workflow; omitted 0, renamed 0. This validated report also carries the exact field and honest dirty-tree scope. No claim is made that the malformed pattern resolved historically.
- **Per-finding completeness:** Both raw findings carry anchors, disambiguator, checked mode, band rationale, problem, reproduction, impact, cause, fix, verification and relationship fields. Both have a disposition here and retain their IDs. Fix Order supplies distribution, dependencies, merge/conflict analysis and rollout risk; Appendix A preserves the repeated output. Severity scales remain in separate tables.
- **Control existence and decision scope:** `test_run_report_sync_wrapper` exists but mocks both async wrapper and asyncio.run; it examines ordinary delegation once and does not evaluate active-loop behavior. `test_cli_run_missing_file` exercises argument validation of a nonexistent report, not absent user settings. `test_cli_init_invokes_init_project` mocks initialization and observes its call/output, not bootstrap failure recovery. None was run or used as confirming evidence in this validation. No green control or reported PASS is used to substantiate either finding.
- **Checklist dependence:** ENT-008 is supported by runtime invocation and source control flow, corresponding to entry-surface and process-topology blocks. ENT-009 is supported by command invocation and source placement, corresponding to bootstrap gate and actionable-message checks. Both are within the phase's enumerated angles; neither requires an angle absent from its list. These broad blocks are investigation topics, not logical predicates that could independently approve either claim.
- **Mechanically resolved anchors:** AST resolves app.run_report, app.run_report_async, app.run and app.list_reports; both primary disambiguator strings occur inside the corresponding function. Service app_config resolves as the class's cached-property function, and the test provenance anchor resolves to its actual test function. Literal README, template and raw-report tokens exist. Source locations are tracked; new audit/scratch artifacts are identified as generated current-run evidence rather than falsely claimed tracked source.

### Appendix C — Evidence limits and acceptance

No finding is unsettled. The bounded probe repeats both command failures and the host-loop guard condition; the existing approved real-loop output strengthens ENT-008 without being represented as a validator-owned real-loop experiment. This does not establish every Jupyter version or all user settings failures. No network request, credentials readout, production configuration change or source/test repair was performed.

The input is ready for tech-lead acceptance **with the two dispositions and final HIGH/HIGH distribution in this report**. It is not accepted unchanged. VAL-006 is addressed by using this report's grading; VAL-007/008 are separate audit-maintenance follow-ups and do not block the two concrete application/documentation recommendations.

Final local consistency check (existing venv Python; AST and report-text inspection, exit 0):

```text
semantic_anchors_resolved: 6
missing: []
dispositions: {'re-typed': 1, 're-graded': 1}
unique_input_ids: ['ENT-008', 'ENT-009']
content_headers: [('ENT-008', 'HIGH'), ('ENT-009', 'HIGH')]
validation_headers: [('VAL-006', 'HIGH'), ('VAL-008', 'MEDIUM'), ('VAL-007', 'LOW')]
report_status_validated: True
line_locators: []
```
