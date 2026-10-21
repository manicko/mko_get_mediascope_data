---
phase: "02"
phase_name: "config-secrets"
id_prefix: "CFG-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/02-config-secrets/references/workflow.md#severity-taxonomy"
code_ref: "4c08eb445f80584a00f16d929bc995239a1bd3d3"
---

# Audit Findings — Configuration & Secrets Management

## Summary

Two HIGH findings. A report that omits an advertisement filter loses the filter selected by its defaults, broadening the generated request. A supported authenticated proxy configuration exposes its password in captured console output before a request completes.

## Scope

**Examined:** All seven phase blocks: settings distribution/copy, credential entry seam, default/subtype/client merge, validation and credential guards, assigned-setting consumers, entry-surface parity, and posture controls. Inspected core models/services/paths/init_service/reports/network/utils, entry wrappers, bundled application/logging/default/report templates, relevant tests and settings architecture documentation. Vendor credential/authentication implementation was inspected as the installed downstream consumer.

**Snapshot:** Moving dirty working tree on 2026-10-21, not pristine `code_ref`. Initial evidence: `.ai/tmp/audit-2026-10-21/initial-hashes.json` and `initial-git-status.txt`. User-modified application settings and current-behavior/async-concurrency documents were preserved; coordinator subsequently reported external drift in async-concurrency and data-flow documents. Findings are grounded in current source and bounded runtime probes. Historical CFG-001–010 are reserved and not reused. Phase 01 remains frozen.

**Tools:** PowerShell read-only inspection, `rg`, Git object/path inspection and existing venv Python; no dependency synchronization. Probe: `.ai/tmp/audit-2026-10-21/config_probe.py`.

**Not verified:** Live API authentication, remote calculation/results, actual production credential validity, deployment ACLs or cross-environment credential reuse, full lint/type/test suite. Every attempted transport call in the credential probe is blocked before I/O; only synthetic credential files are used. No real credential value appears in evidence. No claim of an actual production credential compromise is made.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| CFG-011 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings` | Omitted client filter erases the selected subtype's advertisement restriction |
| CFG-012 | HIGH | `src/mko_get_mediascope_data/core/services.py::AppService.get_connection` | Authenticated proxy password reaches stdout during credential construction |

#### CFG-011 — [HIGH] Omitted client filter erases the selected subtype's advertisement restriction

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.merge_data_settings`
**Anchor:** `src/mko_get_mediascope_data/core/models.py::ReportSettings`
**Anchor:** `src/mko_get_mediascope_data/settings/defaults/tv/crosstab.json5::"TOP_NAT_TV_ADVERTISERS"`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DefaultTVTaskStrategy.generate`
**Disambiguator:** `report_settings.model_dump().items()`
**Checked:** ran
**Why this band:** A merged configuration value silently loses its data-selection constraint and is forwarded to the API builder without detecting that loss: HIGH under this phase's data-corruption criterion. Not CRITICAL: no secret compromise or application startup gate bypass is established.
**Problem:** The intended defaults → subtype → explicit client overlay includes model-generated defaults from `ReportSettings.model_dump()`. An omitted `ad_filter` is represented as `None`, overwriting a nonempty subtype filter. The documented contract says omitted client fields retain defaults.
**Repro:** Build a valid TV crosstab client selecting `TOP_NAT_TV_ADVERTISERS`, with a two-day explicit period and `data_lang='en'`, omitting `ad_filter`. Load bundled defaults via the real service, merge/validate them and advance the real task strategy once. The subtype filter exists, the client field is unset, and both validated data and generated task contain `ad_filter=None`.
**Impact:** Sellers selecting that subtype can request advertisements outside its intended status/type restriction, changing report totals without a configuration error. Remote values were not fetched; the observed consequence is removal of the request restriction before the API boundary.
**Root cause:** The merge treats synthesized Pydantic field defaults as explicit client overrides.
**Fix:** Make `AppService.merge_data_settings` preserve selected defaults for omitted client fields while retaining explicit null overrides. Benefit: requested data scope matches the selected configuration; effort small; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_omitted_ad_filter_preserves_subtype_and_explicit_null_overrides
**Related:** None.

**Evidence — `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/config_probe.py`** *(supports: omission changes the generated request constraint)*:
```text
subtype_ad_filter_nonempty: True
report_explicitly_sets_ad_filter: False
merged_ad_filter: None
generated_task_ad_filter: None
```

#### CFG-012 — [HIGH] Authenticated proxy password reaches stdout during credential construction

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Disambiguator:** `settings_filename=cred_path`
**Checked:** ran
**Why this band:** Secret values reach output: the phase explicitly rates that HIGH. Not CRITICAL: only a synthetic password is observed; production credential exposure or file access by unauthorized people is not demonstrated.
**Problem:** `get_connection` passes the credential file directly into the vendor constructor. Its supported `proxy_server` option is printed with the complete HTTPS proxy URL, including URL user information. The output bypasses application logging and argument redaction.
**Repro:** Write a synthetic credential file under the isolated scratch settings directory with an authenticated `https://...@example.invalid` proxy URL. Capture stdout while invoking the real `AppService.get_connection`, with `Session.request` patched to reject every outbound request. The synthetic password is already present in captured stdout. A direct vendor-constructor probe reproduces the same source.
**Impact:** CLI logs, notebook output and scheduler-captured stdout can retain a proxy password even when application error-argument redaction is enabled. An authentication failure cannot undo the earlier output.
**Root cause:** The installed vendor's `MediascopeApiNetwork.__init__` prints the unredacted proxy mapping during credential construction; the application consumes that path without output-safe handling.
**Fix:** Ensure `AppService.get_connection`'s supported proxy-credential path does not emit proxy user information, including during constructor failures. Benefit: proxy secrets stay out of persistent execution output; effort small to medium; advisory priority high. [SPEC-DEVIATION]
**Verify:** new test needed: test_get_connection_does_not_emit_authenticated_proxy_password
**Related:** Phase 15 may assess credential lifecycle; this finding owns the configuration-to-output seam.

**Evidence — same probe command** *(supports: secret present in output on the application's reachable path; value intentionally omitted)*:
```text
proxy_secret_in_constructor_stdout: True
proxy_secret_in_get_connection_stdout: True
```

## Fix Order

- **Distribution:** CRITICAL 0; HIGH 2; MEDIUM 0; LOW 0.
- **Dependencies:** None.
- **Merge candidates:** None; data overlay and vendor output have distinct causes.
- **Conflicting evidence:** Pydantic accepts the merged model in CFG-011, but schema acceptance does not preserve omitted-client semantics. This is not a request to replace the intended shallow overlay with deep merging or forbid legitimate API extras.
- **Cross-phase conflicts:** Missing-settings recovery remains ENT-009 in `.ai/audit/99-validation/01-entry-architecture-validated-findings.md`; it is not refiled here. Template conformance details belong to phase 05; retry behavior belongs to phase 09.
- **Rollout risk:** CFG-011 must retain explicit `ad_filter=null` behavior and ordinary subtype/client replacement; run service merge coverage and add the named omission-versus-null test. CFG-012 must retain proxy routing and error propagation while removing secret output; run connection/credential tests and the named proxy-output test. No shipped test specifically asserting either defect was identified in inspected service/model tests.

## Appendices

### Appendix A — Complete bounded probe output

Command: `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/config_probe.py` (exit 0).
The probe is isolated local evidence, not a real authentication attempt. `AssertionError` means the transport guard deliberately stopped a would-be outbound call. No secret values are printed; secret-output checks emit booleans only.

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
*(supports: CFG-011 and CFG-012; remaining lines record the required block methodology, not additional findings or claims of remote acceptance.)*

### Appendix B — Configuration and credential seam coverage

| Block | Evidence scope and ownership |
|---|---|
| 1: Distribution | CLI/module/library report paths converge on the same lazy service and user `APP_PATHS`. `init_project` copies packaged JSON5 templates; `_copy_file_atomic` uses a sibling temporary file and `os.replace`. Non-forced initialization preserved a deliberately edited scratch application config byte-for-byte. No claim of a transaction across all copied files. |
| 2: Credential seam | `AppPaths.connection_config` prefers user `connections/mediascope.json`, falls back to the legacy name with a filename-only warning. `get_connection` passes that path to `MediaVortexTask(check_version=False)`. Vendor JSON reader obtains username/password/client values and endpoint URLs; token creation sends password/client secret in an `application/x-www-form-urlencoded` POST body. Subsequent API calls place the bearer token in the Authorization header. Test transport intercepted the pre-send form representation; no network was used. |
| 3: Validation/merge | Defaults path is user `settings/defaults/<media>/<report_type>.json5`; `DataDefaults` permits subtype blocks. Base → subtype → client → language conversion → `Data` → task settings → builder. A string-valued `slices` field fails Pydantic with that field's named union locations. All 34 current bundled report entries and their selected merged Data values validated. CFG-011 concerns semantic loss despite successful validation. |
| 4: Guards | Application guard checks credential-file existence and gives an initialization hint. Vendor JSON parsing separately rejects empty bytes and malformed syntax. Vendor field guard checks None rather than empty strings: an empty-field object reached a blocked authentication call. No genuine credential was used, no auth success occurred, and no remote failure wording is claimed. Guard effectiveness is therefore bounded to local structural checks; semantic credential validity remains unverified. |
| 5: Liveness | Assigned application fields `export_folder`, `max_concurrent_requests`, `max_tasks`, `sleep_time`, `max_attempts`, `call_timeout`, `retry_5xx`, `log_errors_detail`, `poll`, `send_task` reach service/report/network consumers; `verbosity_mode` is consumed by the CLI. Poll's five fields and send-task's two fields reach their corresponding report call sites. Report's period/target/media/type/subtype/path/check_done/compression/language/multiple_files values have readers; `ad_filter` participates in the data overlay; regional extras reach TVRegTaskStrategy. No bundled extra setting name remained outside merged data or the explicit regions consumer. |
| 6: Parity | All report entry surfaces share settings and credentials. CLI verbosity overrides root/handler levels, whereas library calls retain user logging configuration; documented as CLI presentation behavior, so no unintended separation or independent production config tier is invented. Proxy output in CFG-012 bypasses both paths' logging controls. |
| 7: Posture | No application environment-variable settings override layer was found. Platformdirs selects the operator's user settings root; relative exports/logs resolve against the user root. Concurrency/retries/timeouts are typed app controls, export path is an operator-selected Path, and credential path is derived with a legacy filename fallback. `send_task` retry switches are documented controls whose transport consequences belong to phase 09; no separate hardened production variant exists to test for re-pinning. |

Installed vendor evidence for CFG-012 is in `mediascope_api/core/net.py::MediascopeApiNetwork.__init__`: the `proxy_server` branch formats `self.proxies` directly into `print`. Its auth seam is `MediascopeApiNetwork.get_token`; field loading is `mediascope_api/core/utils.py::load_settings`. These installed dependency files are evidence context, not tracked repository anchors. The primary tracked anchor is the application's constructor call, mechanically confirmed with `git ls-files` and source inspection.

The permissive synthetic collection and misspelled-setting probes show validation boundaries; they are not promoted to findings without a demonstrated current consumer consequence. Actual package credential strings were never emitted, and their validity was not tested. User credential stores outside scratch were not accessed.
