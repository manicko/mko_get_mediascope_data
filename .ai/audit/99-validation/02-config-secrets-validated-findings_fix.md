---
phase: "02"
phase_name: "config-secrets"
id_prefix: "CFG"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/02-audit-config-secrets.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Configuration & Secrets Management

## Summary

10 findings (2 HIGH, 3 MEDIUM, 4 LOW after re-grade; 3 HIGH, 4 MEDIUM, 3 LOW at
`code_ref d093d9c`). All 10 anchors resolved at `code_ref d093d9c`; CFG-005's central
claim no longer reproduces at HEAD and is rejected as falsified, CFG-006 is re-graded
MEDIUM → LOW and CFG-002 HIGH → MEDIUM (mechanism corrections), CFG-007's
mechanism/reachability is corrected, and CFG-010 is rejected as falsified at HEAD —
see the re-validation note. **8 confirmed
(2 re-graded), 2 rejected (falsified at HEAD), 0 unsettled.**

The phase 02 findings' severity taxonomy is the rubric of record. Each finding's
blast radius was assessed against the phase's own bands (CRITICAL–LOW). The 2026-10-15
re-validation pass found that post-`code_ref` remediation falsified CFG-005 and
CFG-010; its mechanism and the mechanisms of CFG-006 and CFG-002 changed, and those
findings are re-graded accordingly. The originally recorded imprecision paragraphs
for each finding are superseded. CFG-007's mechanism and reachability are corrected.
Original finding texts are preserved below as the `code_ref d093d9c` record.

> **Re-validation revision (2026-10-15, HEAD `335a11e`; code identical to baseline
> `b04d177`).** `yaml_to_dict` (`core/utils.py:343-367`) now returns `dict[str, Any]`
> and raises `FileNotFoundError`/`YAMLError` with logging instead of returning
> `None`. Consequently CFG-005 is **rejected as falsified at HEAD**; CFG-006 (missing
> defaults templates) and CFG-002 (malformed `tv_reg/crosstab.yaml`) keep their
> defects but lose the None-return amplification their original gradings cited, and
> are re-graded (MEDIUM → LOW and HIGH → MEDIUM respectively) under the phase rubric's
> present-state rule; CFG-007's mechanism and reachability are corrected (the
> `Data(statistics=None)` `ValidationError` is real but preempted for `ru` by the
> `en_to_ru(None)` `TypeError`). A second pass on the same day additionally
> **rejected CFG-010 as falsified at HEAD**: no active `print()` calls remain in
> `core/` (commit `606c899` replaced them with `logger.info()`), so the layer
> discipline the finding cited is restored; the five commented-out `print()` lines
> in `core/reports.py` (58, 102, 257, 290, 333) are hygiene drift, not violations.
> Original finding texts are preserved below as the `code_ref d093d9c` record.

## Scope

**Examined:** `core/paths.py`, `core/init_service.py`, `core/models.py`,
`core/services.py`, `core/network.py`, `core/utils.py`, `core/tasks.py`,
`core/reports.py`, `app.py`, `__main__.py`, `__init__.py`, all `settings/`
templates, `pyproject.toml`, `tests/conftest.py`, `tests/test_utils.py`,
`tests/test_services.py`, `tests/test_app.py`, `tests/test_models.py`,
`tests/test_network.py`, phase 02 phase file, phase 01 findings + validation
report (cross-phase), findings template.

**Runtime probes performed:**
- `ReportSettings(**rep)` for all 32 template entries → 2 PASS, 30 FAIL
- `yaml.safe_load` on `tv_reg/crosstab.yaml` → `ParserError` at line 162
- `yaml_to_dict('/nonexistent')` → returns `None` (confirmed at `code_ref d093d9c`; raises `FileNotFoundError` at HEAD — CFG-005 rejected)
- `yaml_to_dict('tv_reg/crosstab.yaml')` → returns `None` (at `code_ref d093d9c`; raises `ParserError` at HEAD)
- `LoggingSettings(**None)` → `TypeError: argument after ** must be a mapping` (at `code_ref d093d9c`; not reachable at HEAD)
- `AppService` instantiation with empty user dir → `TypeError` at import (confirmed at `code_ref d093d9c`; at HEAD the deferred singleton means the first `get_app_service()` call raises `FileNotFoundError` — no import-time crash)
- `Data(statistics=None)` → `ValidationError` (confirmed)
- `hasattr(defaults, subtype.value)` for all subtypes → 3 return `False` (at `code_ref d093d9c`; see CFG-007 for HEAD reachability)
- `print()` AST scan across all `core/*.py` → 4 active calls (2 commented) at `code_ref d093d9c`; 0 active at HEAD (CFG-010 rejected)
- `pydantic-settings` import search in `src/` → zero matches

**Not verified:** Live API calls (no credentials); log output during a real run.

## Conformance

**Validated against:** commit `d093d9c` (Phase 02 baseline)
**HEAD at re-validation:** `335a11e` (commit `e8fd1867` code-identical to `b04d177`)
**Survival at HEAD:** 8 confirmed (CFG-001/002/003/004/006/007/008/009; CFG-002 and CFG-006 re-graded), 2 rejected as falsified at HEAD (CFG-005, CFG-010), 0 unsettled
**Correction type:** git-HEAD evidence update — line 634 in Evidence Index cited `git rev-parse --short HEAD → d093d9c`; updated to `335a11e`.

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). Findings verified again at HEAD; CFG-007's two-path mechanism
(`en_to_ru(None)` `TypeError` for `data_lang: "ru"` vs `Data(**merged)`
`ValidationError` for `data_lang: "en"`) was re-probed and matches this report.
Mapping: CFG-001 → D-06 / D-11 (repair all bundled templates; nothing deleted;
templates intentionally incomplete) and G-05 / W-01; CFG-003 → D-12 / W-06
(credential best-practice decision pending); CFG-007 → G-05 / W-01 (template
repair covers adding the missing subtype blocks).

---

## Findings

| ID | Disposition | Severity | Band (rubric) vs. Carried | Anchor resolves |
|---|---|---|---|---|
| CFG-001 | confirmed | HIGH | HIGH vs. HIGH | yes |
| CFG-002 | confirmed (re-graded) | MEDIUM | HIGH vs. MEDIUM (re-graded at HEAD) | yes* |
| CFG-003 | confirmed | HIGH | HIGH vs. HIGH | yes |
| CFG-004 | confirmed | MEDIUM | MEDIUM vs. MEDIUM | yes |
| CFG-005 | rejected (falsified at HEAD) | MEDIUM (at code_ref) | MEDIUM vs. falsified at HEAD | yes |
| CFG-006 | confirmed (re-graded) | LOW | MEDIUM vs. LOW (re-graded at HEAD) | yes |
| CFG-007 | confirmed | MEDIUM | MEDIUM vs. MEDIUM | yes |
| CFG-008 | confirmed | LOW | LOW vs. LOW | yes |
| CFG-009 | confirmed | LOW | LOW vs. LOW | yes |
| CFG-010 | rejected (falsified at HEAD) | LOW (at code_ref) | LOW vs. falsified at HEAD | yes |

\* CFG-002's anchor `settings/defaults/tv_reg/crosstab.yaml::1` uses a bare line
number. The token `period_type` or `DATA_DEFAULTS` would be more precise (VAL-002).

### CFG-001 — confirmed — [HIGH] Templates use stale field names and enum values rejected by Pydantic

**Anchor resolved:** `settings/reports/nat_tv_brands_last.yaml::period_type` —
the `period_type` key exists in the file (confirmed via grep/read). Primary symbol
`ReportSettings` at `core/models.py:83`; `LastTimeModel` at `core/models.py:61`
with `extra="forbid"`.

**Claim tested:** 30 of 32 bundled report entries fail `ReportSettings(**rep)`.
Repro confirmed: `period.last_time.unit_count` required, `period_type`/`period_num`
forbidden; `report_type` enum rejects `TV_CROSSTAB`/`TV_DICT_CROSSTAB`/`REG_TV_*`
variants.

**Evidence (runtime probe — all 32 template entries):**
```text
TOTAL: 2 PASS, 30 FAIL
```
The 2 passing entries are in `nat_tv_brands_tefal.yaml` (entries [0] and [1])
which use correct `crosstab`/`time_unit`/`unit_count` and include `media`/`relative_path`.

**Rubric grading:** Phase 02 HIGH = "a merged config value that reaches the API
consumer without validation and causes silent data corruption." The templates do
NOT reach the API consumer — `ReportSettings(**rep)` raises `ValidationError` and
`get_report_settings` (services.py:57-64) re-raises via `except Exception`. The
blast radius is maximum (100% of shipped templates fail), which anchors the HIGH
rating on present-state impact. The rubric's "Config-name drift" (MEDIUM) describes
the mechanism type; the effect and blast radius (total tool failure) justify HIGH.
No re-grade.

**Load-bearing artefact:** Code (`core/models.py`) defines the schema; templates
(`settings/reports/**/*.yaml`) carry the stale values. Code is load-bearing —
fixing templates alone does not change the schema.

**Recommendation carry-out:** Rename `period_type→time_unit`, `period_num→unit_count`
in all templates; replace stale enum values with `crosstab`/`timeband`/`simple`;
add `media`/`relative_path`. Targets: all 15 template files. Stable. ✓

### CFG-002 — confirmed (re-graded) — [MEDIUM]

**REJECTED — falsified at HEAD `335a11e`.** The anchor `settings/defaults/tv_reg/crosstab.yaml::1`
no longer exists: the malformed YAML makes `yaml.safe_load` raise `ParserError` at line 162.

**Anchor resolved:** `settings/defaults/tv_reg/crosstab.yaml::1` — file exists,
line 1 is `{`. (See VAL-002 for anchor-format note.)

**Claim tested:** 13 `{` vs 12 `}` — unbalanced flow mapping. `yaml.safe_load`
raises `ParserError` at line 162. `yaml_to_dict` catches `YAMLError`, logs it,
and re-raises, so the `ParserError` surfaces to the caller.

**Evidence (runtime probes):**
```text
brace_count: open=13, close=12, difference=1
yaml.safe_load error: ParserError — "expected ',' or '}', but got '<stream end>'"
  at line 162, column 2
yaml_to_dict('tv_reg/crosstab.yaml') → raises ParserError (logged at utils.py:362-364)
```

**Asserted cause tested separately:** The missing `}` is at the end of the file
(line 162 is the closing `}` of the outer dict, but the inner `DATA_DEFAULTS` dict
on line 31 is never closed — the `}` on line 31 closes the `'options'` dict, not
`DATA_DEFAULTS`). `yaml_to_dict` (utils.py:343-367) logs and re-raises in both
`except` blocks; no `None` return exists at HEAD. Verified against source.

**Rubric grading (re-graded HIGH → MEDIUM at HEAD):** at `code_ref d093d9c` this
finding was graded HIGH because the `yaml_to_dict` silent `None`-return was
"a production control silently disabled" (HIGH) and the broken input triggered
that silent control failure. At HEAD the `ParserError` is raised and logged — the
silent control no longer exists — so the present-state grade is the unconsumable
shipped template itself: "ineffective or unconsumed configuration" (MEDIUM). The
malformed-YAML defect is unchanged and still fails every TV_REG defaults load.

**Load-bearing artefact:** Template file (`settings/defaults/tv_reg/crosstab.yaml`).

**Recommendation carry-out:** Add missing `}` after line 31 (or restructure as
block YAML). Target: `settings/defaults/tv_reg/crosstab.yaml`. Stable. ✓

### CFG-003 — confirmed — [HIGH] Credential file name mismatch

**Anchor resolved:** `core/paths.py::AppPaths.connection_config` (line 57-59) —
returns `Path(self.user_settings_dir, "connections/mediascope.json")`.

**Claim tested:** Template `settings/connections/mediascope_RENAME_.json` is copied
by `init_project()` → installed as `mediascope_RENAME_.json` in user dir. Code reads
`mediascope.json`. After init, the expected file does not exist.

**Evidence (static + runtime probe):**
```text
Template name: settings/connections/mediascope_RENAME_.json
Code expects:  AppPaths.connection_config → connections/mediascope.json
Template contents: {username, passw, client_id, client_secret, auth_server, root_url}
  with placeholders: "you username", "you password", "00000000-..."
```

**Asserted cause tested separately:** `_copy_templates` (init_service.py:7-27) copies
all files from `APP_PATHS.app_settings_dir` to `APP_PATHS.user_settings_dir` via
`shutil.copy2`, preserving the source filename. The template filename
`mediascope_RENAME_.json` is preserved verbatim. `APP_PATHS.connection_config`
hardcodes `mediascope.json`. No guard checks existence before passing to
`MediaVortexTask`. Verified against source.

**Rubric grading:** Phase 02 CRITICAL covers "a credential file that is empty,
invalid, or shared across environments." The template file exists but contains
placeholders (not real credentials) and sits at the wrong filename. This is
config-name drift (MEDIUM) elevated by the credential context to HIGH. The
credential itself is not leaked — it's a placeholder template that users are
expected to rename. HIGH is appropriate. No re-grade.

**Load-bearing artefact:** `core/paths.py::AppPaths.connection_config` is
load-bearing (code defines the expected path); the template filename is the
defect.

**Recommendation carry-out:** Rename template to `mediascope.json`, OR update
`connection_config` to resolve the `_RENAME_` path, OR add existence guard in
`get_connection`. All three targets exist and are stable. ✓

### CFG-004 — confirmed — [MEDIUM] No credential guard before API connection

**Anchor resolved:** `core/services.py::AppService.get_connection` (line 100-108).

**Claim tested:** `get_connection` passes `self.app_paths.connection_config`
directly to `MediaVortexTask` without checking file existence or placeholder
values. `NetworkClient.call` (network.py:88) catches `FileNotFoundError`,
logs the path, and re-raises — library-level, not actionable from project code.

**Evidence (runtime probe + source inspection):**
```text
get_connection body:
  connection_service = _CONNECTION_MAP[media_type]
  return connection_service(
      settings_filename=self.app_paths.connection_config,
      check_version=False,
  )
No os.path.exists() or Path.exists() check before instantiation.
NetworkClient.call catches FileNotFoundError at network.py:88, logs, re-raises.
```

**Asserted cause tested separately:** `MediaVortexTask.__init__` receives the
settings filename and presumably opens it internally. The project code never
validates the file's existence or contents. Placeholder values in the template
(`"you username"`, `"00000000-..."`) would pass through undetected. Verified
against source.

**Rubric grading:** "Ineffective or unconsumed configuration" (MEDIUM). The
credential guard is absent — ineffective configuration. No secret values reach
logs (the FileNotFoundError handler logs the file path, not credentials). MEDIUM
matches the rubric. No re-grade.

**Load-bearing artefact:** `core/services.py::AppService.get_connection`.

**Recommendation carry-out:** Add pre-check in `get_connection` — verify file
exists, raise project-specific error. Target: `AppService.get_connection` at
services.py:100. Stable. ✓

### CFG-005 — rejected (falsified at HEAD) — [MEDIUM at code_ref]

**REJECTED — falsified at HEAD `335a11e`.** `yaml_to_dict` (`core/utils.py:343-367`)
returns `dict[str, Any]` (signature line 343; body `return yaml.safe_load(cfg) or {}`
at line 361) and raises with `logger.error` logging on `yaml.YAMLError` (362-364)
and `FileNotFoundError` (365-367) — it never returns `None`. The mypy union errors
cited at `code_ref d093d9c` no longer reproduce (the return type no longer includes
`None`), and no shipped test asserts the old `None` behaviour. The phase-01 sibling
ENT-003 is rejected on the same ground. Residual observations (informational, no
finding filed): exception types outside `YAMLError`/`FileNotFoundError` (e.g.
`PermissionError`, `UnicodeDecodeError`) propagate uncaught and unlogged, and the
None-guards in `AppService.app_config` (`core/services.py:40-44`, `48-52`) are now
unreachable dead code. The claim, probes, and grading below are preserved as the
`code_ref d093d9c` record.

**Anchor resolved:** `core/utils.py::yaml_to_dict` (line 343-360).

**Claim tested:** `yaml_to_dict` catches `yaml.YAMLError` and `FileNotFoundError`,
logs, and falls through to implicit `return None`. Callers do
`Model(**utils.yaml_to_dict(...))` → `TypeError`. The module-level singleton
(`services.py:163`) crashes at import if config files are missing.

**Evidence (runtime probes):**
```text
yaml_to_dict('/nonexistent/path.yaml') → None
yaml_to_dict('tv_reg/crosstab.yaml') → None  (YAMLError caught)
AppService(APP_PATHS with empty user_dir) → TypeError:
  LoggingSettings() argument after ** must be a mapping, not NoneType
```

Also confirmed by phase 01 finding ENT-003 (CRITICAL) — same root cause,
different lens (import-time crash vs. config-loading error handling).

**Asserted cause tested separately:** Function body has `except yaml.YAMLError`
and `except FileNotFoundError` blocks with no `return` — implicit `return None`.
`UnicodeDecodeError` and `PermissionError` are not caught (they propagate as
unhandled exceptions). Return type annotation `dict[str, Any] | None` admits
the `None` path. Verified against source.

**Rubric grading:** "Ineffective or unconsumed configuration" (MEDIUM). The
None-return is ineffective error handling. Phase 01 rated it CRITICAL (import-side
effect) — phase 02 rates it MEDIUM (config-loading concern). Different rubrics,
both correct. No re-grade.

**Load-bearing artefact:** `core/utils.py::yaml_to_dict`.

**Recommendation carry-out:** Return `{}` on error or raise typed exception;
broaden `except` to `OSError`. Targets: `yaml_to_dict` at utils.py:343. Stable. ✓

### CFG-006 — confirmed (re-graded) — [LOW]

**Re-graded MEDIUM → LOW at HEAD `335a11e` (mechanism corrected).** The missing
templates remain (verified at HEAD: `TV/simple`, `TV_REG/timeband`, `TV_REG/simple`
absent). At HEAD a missing file makes `yaml_to_dict` raise a logged `FileNotFoundError`
(`core/utils.py:365-367`) from `get_default_data_settings` (`core/services.py:93-101`)
— the `DataDefaults(**None)` → `TypeError` path is unreachable. The None-return
amplification that justified MEDIUM at `code_ref d093d9c` no longer exists; the
present-state defect is the placeholder/template gap itself, graded LOW per the
phase-02 rubric. The text below is the `code_ref d093d9c` record.

**Anchor resolved:** `core/services.py::AppService.get_default_data_settings`
(services.py:93-101 at HEAD; 66-74 at `code_ref d093d9c`).

**Claim tested:** `get_default_data_settings` builds path
`defaults/{media}/{type}.yaml`. Missing: `TV/simple`, `TV_REG/timeband`,
`TV_REG/simple`. Missing file → `yaml_to_dict` raises `FileNotFoundError` at HEAD (returned `None` at
`code_ref d093d9c`) → `get_default_data_settings` fails with that exception.

**Evidence (runtime probe):**
```text
Defaults template matrix:
  TV/crosstab:     EXISTS
  TV/timeband:     EXISTS
  TV/simple:       MISSING
  TV_REG/crosstab: EXISTS (malformed — see CFG-002)
  TV_REG/timeband: MISSING
  TV_REG/simple:   MISSING
```

**Asserted cause tested separately:** `get_default_data_settings` (services.py:93-101;
66-74 at `code_ref d093d9c`) constructs the path from
`report_settings.media.value.lower()` and `report_settings.report_type.value.lower()`.
No fallback if the file is absent. The path is passed directly to `yaml_to_dict`,
which raises `FileNotFoundError` for missing files (it returned `None` at `code_ref
d093d9c`). Verified against source.

**Rubric grading:** Missing template files are "placeholder and template gaps"
(LOW), but the yaml_to_dict None-return amplifies the failure to a TypeError.
The finding rates MEDIUM because the defect is ineffective config handling
(not just a missing file). MEDIUM is defensible. No re-grade.

**Load-bearing artefact:** `core/services.py::AppService.get_default_data_settings`
and `settings/defaults/` templates.

**Recommendation carry-out:** Ship the 3 missing templates or raise a specific
error. Targets: `settings/defaults/` directory, `get_default_data_settings` at
services.py:93-101. Stable. ✓

### CFG-007 — confirmed — [MEDIUM] 3 ReportSubtype values have no matching defaults key

**Anchor resolved:** `core/services.py::AppService.merge_data_settings`
(L104-121 at HEAD; 77-93 at `code_ref d093d9c`).

**Claim tested:** `merge_data_settings` does
`hasattr(defaults, subtype.value)`. `NAT_TV_CHANNELS_ATV`, `NAT_TV_CHANNELS_BA`,
`NAT_TV_CHANNELS_SOC_DEM` have no key in any defaults template → override skipped
→ `statistics` stays `None` (from `DATA_DEFAULTS`) → `Data(statistics=None)`
fails (`dict|list` type doesn't allow `None`). (`statistics: dict[str, Any] | list[Any]`
at `models.py:127` — `default_factory=list` applies only when the key is absent,
not when it is explicitly `None`.)

**Evidence (runtime probes):**
```text
hasattr check on tv/crosstab defaults:
  DYNAMICS_BY_SPOTS: True
  DYNAMICS_BY_SPOTS_DICT: True
  TOP_NAT_TV_ADVERTISERS: True
  TOP_NAT_TV_PROGRAMS: True
  NAT_TV_CHANNELS_ATV: False ← crash
  NAT_TV_CHANNELS_BA: False ← crash
  NAT_TV_CHANNELS_SOC_DEM: False ← crash

Data(statistics=None) → ValidationError: Input should be a valid dict or list
Data() → statistics=[] (default_factory works when key absent)
# probe captured at code_ref d093d9c; at HEAD tv/crosstab.yaml and tv_reg/crosstab.yaml
# do not parse (ParserError L138 / L162), so the probe runs only for TV/timeband.
```

**Asserted cause tested separately:** `DATA_DEFAULTS` in templates has
`statistics: None`. When a subtype has no matching key, the `if
subtype.value and hasattr(defaults, subtype.value)` guard skips the merge,
leaving `statistics=None` in `data_settings`. `Data(**data_settings)` then
receives `statistics=None`, which violates `dict[str, Any] | list[Any]`.
Verified against source and runtime.

**Reachability at HEAD:** for `data_lang == "ru"` the earlier
`en_to_ru(None)` `TypeError` (`core/services.py:117-118`; phase-05 finding
TPL-003) preempts `Data(**data_settings)`; the `Data(statistics=None)`
`ValidationError` is reachable only for a non-`ru` report whose subtype has no
block in a parseable defaults file (`TV/timeband` today, since it has no subtype
blocks). The missing-subtype-block defect itself is unchanged. MEDIUM carried =
MEDIUM on the rubric ("ineffective or unconsumed configuration").

**Rubric grading:** "Ineffective or unconsumed configuration" (MEDIUM). Three
subtype values produce no configuration that reaches the data model. MEDIUM
matches the rubric. No re-grade.

**Load-bearing artefact:** `core/services.py::AppService.merge_data_settings`
and `core/models.py::Data` (statistics type).

**Recommendation carry-out:** Add subtype blocks to defaults, or guard `Data`
against `None`. Targets: `merge_data_settings` at services.py:104-121 (77-93 at
`code_ref d093d9c`), `Data` model at models.py:124-128. Stable. ✓

### CFG-008 — confirmed — [LOW] Non-atomic template copy

**Anchor resolved:** `core/init_service.py::_copy_templates` (line 7-27).

**Claim tested:** `_copy_templates` uses `shutil.copy2(item, target_path)`
per file. Interrupted `init` leaves a partial config tree. No temp-write +
atomic-rename pattern.

**Evidence (static read):**
```text
init_service.py:7: def _copy_templates(destination, force=False):
init_service.py:15-27: for item in source_dir.rglob('*'):
    ...
    shutil.copy2(item, target_path)  # line 27
No os.replace or tempfile.NamedTemporaryFile pattern.
```

**Rubric grading:** "A template copy that is non-atomic but never observed to
interleave" (LOW). Matches exactly. No re-grade.

**Load-bearing artefact:** `core/init_service.py::_copy_templates`.

**Recommendation carry-out:** Write to temp file, `os.replace` on success.
Target: `_copy_templates` at init_service.py:27. Stable. ✓

### CFG-009 — confirmed — [LOW] `pydantic-settings` declared but unused

**Anchor resolved:** `pyproject.toml::project.dependencies` (line 21) —
`"pydantic-settings>=2.12.0"`.

**Claim tested:** Zero imports of `pydantic_settings` in `src/`. No env-overridable
controls exist for concurrency, retry count, export path, or credential path.

**Evidence (grep):**
```text
rg -l 'pydantic_settings' src/ → no results
rg -l 'pydantic-settings' src/ → no results
pyproject.toml:21: "pydantic-settings>=2.12.0"
```

**Asserted cause tested separately:** `pydantic-settings` is listed as a
runtime dependency but never imported. `BaseSettings` is not used anywhere.
NetworkClient (network.py:49-53) hardcodes `max_concurrent_requests=5`,
`sleep_time=2`, `max_attempts=10`. No `os.getenv()` calls for these values
anywhere in `core/`. Verified against source.

**Rubric grading:** "Undocumented switches" (LOW) — an unused dependency that
looks like a configuration switch. LOW matches. No re-grade.

**Load-bearing artefact:** `pyproject.toml::project.dependencies`.

**Recommendation carry-out:** Implement `BaseSettings` for env overrides, or
remove the dependency. Targets: `pyproject.toml`, `core/network.py`. Stable. ✓

### CFG-010 — rejected (falsified at HEAD) — [LOW at code_ref] `print()` in core modules

**REJECTED — falsified at HEAD `335a11e`.** No active `print()` calls remain in
`src/`: the four call sites (`core/services.py:117/124/154`,
`core/reports.py:265`) were replaced with `logger.info()` in commit `606c899`, and
both modules now define a module-level logger. The five commented-out `print()`
lines in `core/reports.py` (58, 102, 257, 290, 333) are hygiene drift, not
layer-discipline violations. The claim, AST output, and grading below are
preserved as the `code_ref d093d9c` record and do not reproduce at HEAD.

**Anchor resolved:** `core/services.py::AppService.run_report` (line 110-160).
Print calls at services.py:117, 124, 154; reports.py:265.

**Claim tested:** 4 active `print()` calls in `core/` modules:
- `services.py:117` — start-time banner
- `services.py:124` — per-report progress
- `services.py:154` — end-time summary
- `reports.py:265` — progress dot

**Evidence (AST scan across core/):**
```text
core/reports.py:265: print("=", end="", flush=True)
core/services.py:117: print(f"\n{'-' * 10}  Обработка стартовала: ...")
core/services.py:124: print(f"\nОтчёт {idx} → {report_settings.report_subtype}\n")
core/services.py:154: print(f"\n{'-' * 10}  Обработка завершена: ...")
```

`app.py` uses `console.print()` (Rich Console) — allowed per AGENTS.md.
Commented-out `print()` calls in reports.py (lines 58, 102, 257, 290, 333)
are not violations.

Also confirmed by phase 01 finding ENT-005 (MEDIUM) — same root cause, different
rubric (entry-architecture vs. config-secrets).

**Rubric grading:** In the config-secrets context, `print()` is a minor
issue. Phase 02 LOW covers "placeholder and template gaps; undocumented switches;
missing explanatory comments." The print calls don't affect config or secrets,
but they are code-quality drift in `core/`. Phase 01 rated the same issue
MEDIUM. Different rubrics, both correct — this is a cross-reference, not a
conflict. No re-grade.

**Load-bearing artefact:** `core/services.py` and `core/reports.py`.
Governing standard: AGENTS.md rule 12; `doc-maintenance-rules.md:101`.

**Recommendation carry-out:** Replace `print()` with `logger.info()`. Targets:
`services.py` (3 calls), `reports.py:265` (1 call). A module-level
`logger = logging.getLogger(__name__)` must be added to `services.py`
(reports.py already has one at line 24). Stable. ✓

---

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | Scope references `core/net.py` (does not exist; actual file is `core/network.py`); mislabels `core/utils.py` and `core/errors.py` as "vendored mediascope_api library" |
| VAL-002 | LOW | confirmed | CFG-002 anchor `tv_reg/crosstab.yaml::1` uses line number instead of token per template anchor rule |

### VAL-001 — [LOW] Scope references non-existent `core/net.py`; mislabels project modules as vendored

**Template mandate:** Findings must accurately describe the examined surface.

**Observed:** The Scope section states:
> the vendored `mediascope_api` library (`core/net.py`, `core/utils.py`, `core/errors.py`)

Three errors:
1. `core/net.py` does not exist. The actual module is `core/network.py`
   (NetworkClient with retry/backoff/semaphore).
2. `core/utils.py` is the project's own utility module, not vendored
   `mediascope_api` code.
3. `core/errors.py` defines two project exception classes (`NoReportFoundError`,
   `WrongDataSettings`) — not vendored `mediascope_api` code.

The actual `mediascope_api` library is a pip-installed package, imported as
`from mediascope_api.mediavortex.tasks import MediaVortexTask` (services.py:7,
reports.py:9) and `from mediascope_api.core.errors import AuthorizationError,
BadRequestError` (network.py:10). No vendored copy exists in `src/`.

**Impact:** A reader following the scope to reproduce findings will fail to find
`core/net.py` and may waste time searching for vendored library code that
doesn't exist.

**Disposition:** confirmed. The findings themselves are valid (the examined files
exist under their correct names), but the scope description is inaccurate.

### VAL-002 — [LOW] CFG-002 anchor uses line number instead of token

**Template mandate:** `.ai/audit/templates/audit-findings.md:42` —
"Non-code targets: `path::"exact token"` (template block, settings/YAML key)."

**Observed:** CFG-002's anchor is
`settings/defaults/tv_reg/crosstab.yaml::1` — a bare line number (`::1`),
not a token from inside the file. The template's anchor rule for non-code
targets requires an exact token (e.g., `DATA_DEFAULTS` or the unmatched
`{` on line 1). By contrast, CFG-001's anchor
`settings/reports/nat_tv_brands_last.yaml::period_type` correctly uses a YAML
key as the token.

**Impact:** Format drift only — the file exists and line 1 is `{` (the start of
the malformed flow mapping). No finding is misidentified. But the anchor does
not follow the controlled template pattern.

**Disposition:** confirmed. Does not affect finding accuracy.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 01 finding | Phase 02 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| yaml_to_dict None return | ENT-003 (CRITICAL) | CFG-005 (MEDIUM) | `utils.py:yaml_to_dict` catches errors, returns `None` instead of raising | cross-reference; both rejected at HEAD — `yaml_to_dict` raises (utils.py:343-367) |
| print() in core | ENT-005 (MEDIUM) | CFG-010 (LOW) | `print()` calls in `core/services.py` and `core/reports.py` | cross-reference; both rejected at HEAD — `print()` replaced with `logger.info()` (commit 606c899) |

**Severity divergence rationale:** Phase 01 (entry-architecture) graded
`yaml_to_dict` returning `None` as CRITICAL (import-time crash made the tool
unusable) and Phase 02 grades it MEDIUM (ineffective config error handling);
both gradings were anchored at `code_ref d093d9c`, and at HEAD both findings are
rejected because `yaml_to_dict` raises. Phase 01 graded `print()` as MEDIUM
(layer-discipline at entry layer) and Phase 02 as LOW (minor issue in
config context); both gradings were likewise anchored at `code_ref d093d9c`, and
at HEAD both findings are rejected because the `print()` calls were replaced
with `logger.info()` (commit `606c899`). These are
cross-references, not conflicts.

No findings were filed under a phase that does not own the concern. No two
phases reach opposite conclusions about the same fact.

---

## Rollout Safety Assessment

**CFG-002 fix (add missing `}`) must land before CFG-006 can be fully validated
for TV_REG** — the malformed YAML makes `yaml_to_dict` raise `ParserError`,
so the missing `TV_REG/timeband` and `TV_REG/simple` templates cannot be tested
until the existing `TV_REG/crosstab.yaml` parses.

**CFG-001 fix must land before CFG-007 can be tested end-to-end** — report
templates must use valid field names/enum values before the data-settings merge
chain (defaults → overlay → validate) can be exercised.

**CFG-005 and CFG-006 shared the None-return root cause** — at HEAD `yaml_to_dict`
raising is already the implemented behaviour (CFG-005 rejected); CFG-006's remaining
remedy is shipping the missing templates or raising a specific error from
`get_default_data_settings` (services.py:93-101).

**CFG-003 rename rollout risk:** Renaming the template from
`mediascope_RENAME_.json` to `mediascope.json` may invalidate users who
already manually renamed the file. A migration guard (check both filenames,
warn on the `_RENAME_` variant) mitigates this. `test_app.py::test_cli_init`
only asserts `--help` output and will not catch a rename regression — a test
asserting the post-init credential filename is recommended.

**CFG-001 + CFG-003 + CFG-005 combined:** The phase 01 validation report
confirmed that fixing the singleton bootstrap (ENT-002/003) and the
`pydantic-settings` dependency (CFG-009) would require care in `tests/` —
`test_services.py:6` imports `app_service` directly at module scope (resolved at
HEAD: commit `63bb1c0` migrated the tests to `get_app_service()`).

All 10 recommendations name concrete, stable targets (functions, classes,
template files, line-level code sites). All fix targets were verified to exist
in the `d093d9c` tree.

---

## Evidence Index

| Probe | Result | Supports |
|---|---|---|
| `ReportSettings(**rep)` on all 32 template entries | 2 PASS, 30 FAIL | CFG-001 |
| `yaml.safe_load(tv_reg/crosstab.yaml)` | `ParserError` at line 162 | CFG-002 |
| Brace count in `tv_reg/crosstab.yaml` | 13 open, 12 close | CFG-002 |
| `yaml_to_dict('/nonexistent')` | returns `None` at `code_ref d093d9c`; raises `FileNotFoundError` at HEAD | CFG-005 (rejected) |
| `yaml_to_dict('tv_reg/crosstab.yaml')` | returns `None` at `code_ref d093d9c`; raises `ParserError` at HEAD | CFG-002, CFG-005 |
| `AppService()` with empty user dir | `TypeError: LoggingSettings(**None)` at `code_ref d093d9c`; deferred singleton at HEAD — first `get_app_service()` raises `FileNotFoundError` | CFG-005, ENT-003 |
| `Data(statistics=None)` | `ValidationError` | CFG-007 |
| `hasattr(DataDefaults, 'NAT_TV_CHANNELS_ATV')` | `False` | CFG-007 |
| Defaults template matrix (6 combos) | 3 exist, 3 missing/malformed | CFG-002, CFG-006 |
| `print()` AST scan across `core/*.py` | 4 active calls at `code_ref d093d9c`; 0 active at HEAD (commit 606c899) | CFG-010, ENT-005 |
| `rg pydantic_settings src/` | no results | CFG-009 |
| `connection_config` path vs template name | `mediascope.json` vs `mediascope_RENAME_.json` | CFG-003 |
| `pyproject.toml` line 21 | `pydantic-settings>=2.12.0` | CFG-009 |
| `__init__.py` size | 0 bytes | ENT-004 |
| `git rev-parse --short HEAD` | `335a11e` (HEAD at re-validation; `code_ref` was `d093d9c`) | drift noted |
