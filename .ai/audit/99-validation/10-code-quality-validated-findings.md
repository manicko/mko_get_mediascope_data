---
phase: "10"
phase_name: "code-quality"
id_prefix: "QLT-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/10-audit-code-quality.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Code Quality

## Summary

All 16 findings in `.ai/audit/10-code-quality/findings.md` (code_ref `d093d9c`) are
**CONFIRMED** against live source inspection at HEAD `d093d9c`. mypy reproduces **61
errors in 7 files**; ruff is clean (exit 0); 12 shipped tests are green (they mock the
API / plain-dict `data_settings`, which is exactly the coverage gap each finding
identifies); 5 runtime probes reproduce the asserted crashes/returns verbatim; and
git-grep confirms the dead-code and unused-exception claims. The 3 HIGH findings
(QLT-005/006/007) each reproduce as an actual `AttributeError`/`TypeError` on a
**documented** configuration input. Five LOW-severity validation-level issues
(VAL-001–005) are recorded for imprecision in the input report's supporting narrative;
none alters a finding's verdict.

**16 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 merged, 0 unsettled.**

## Scope

**Examined:** all 10 modules under `src/mko_get_mediascope_data/core/`, `app.py`,
`pyproject.toml`, `AGENTS.md`, `tests/` (12 tests), and sibling findings/reports
(ENT-003/ENT-007, CFG-005, PIPE-002/PIPE-006 cross-refs).

**Tools:** `uv run mypy` 1.19.1 (61 errors); `uv run ruff check` (exit 0); `uv run pytest`
(12 passed); `uv run python -c` runtime probes; `git grep`; Grep (Cyrillic).

**Runtime probes performed (reproduced from the findings):**
- QLT-005 — `yaml_to_dict('nonexistent.yaml')` → `None`
- QLT-006 — `Data().get('slices')` → `AttributeError: 'Data' object has no attribute 'get'`
  (and `getattr(d,'slices',None)` → `[]` falsy, so the `or` evaluates `.get`)
- QLT-007 — `ReportSettings(..., period={'date_filter':[...]})` → `period.last_time` is `None`;
  `.last_time.time_unit` → `AttributeError: 'NoneType' object has no attribute 'time_unit'`
- QLT-018 — `en_to_ru([1,2,3])` → `AttributeError: 'int' object has no attribute 'replace'`
  (escapes the `except TypeError`)
- QLT-019 — `ReportSettings(relitive_path='test', ...)` → `pydantic.ValidationError:
  relative_path` *Field required* (the `relitive_path` typo was silently accepted)
- QLT-014 — `mypy --ignore-missing-imports` still yields the same 4 `import-untyped` errors

**Not verified:** live report pipeline (no Mediascope API credentials/network);
Jupyter/Airflow import behaviour (inferred from static analysis; ENT-001/002 own the
import-time entry layer).

---

## Findings

| ID | Disposition | Severity | Anchor resolves | Disambiguator exists |
|---|---|---|---|---|
| QLT-005 | confirmed | HIGH | services.py `AppService.app_config/get_report_settings/get_default_data_settings`; utils.py `yaml_to_dict` | services.py:37 `**utils.yaml_to_dict` ✓ |
| QLT-006 | confirmed | HIGH | reports.py `DictDataStrategy.prepare` | reports.py:461 `report.data_settings.get("slices", None)` ✓ |
| QLT-007 | confirmed | HIGH | reports.py `TVMediaReport.get_period` | reports.py:241 `last_time.time_unit` ✓ |
| QLT-008 | confirmed | MEDIUM | models.py `DataDefaults`; services.py `merge_data_settings/get_data_settings`; reports.py `DefaultTVTaskStrategy.generate` | models.py:121 `DATA_DEFAULTS: dict` ✓ |
| QLT-009 | confirmed | MEDIUM | network.py `NetworkClient.call` | network.py:65 `-> Any:` ✓ |
| QLT-010 | confirmed | MEDIUM | reports.py `ReportFactory.data_strategy` | reports.py:44 `"DICT" in self.report_subtype.value.upper()` ✓ |
| QLT-011 | confirmed | MEDIUM | utils.py `csv_to_file` | utils.py:188 `data_frame.to_csv(` ✓ |
| QLT-012 | confirmed | MEDIUM | utils.py `get_last_period` | utils.py:95 `today = date.today() if today is None else today` ✓ |
| QLT-013 | confirmed | MEDIUM | tasks.py `TVTask.__init__` | tasks.py:20 `self.key = None` ✓ |
| QLT-014 | confirmed | MEDIUM | pyproject.toml `tool.mypy` | line 145 `ignore_missing_imports = true` ✓ |
| QLT-015 | confirmed | LOW | tasks.py `Task` | tasks.py:10–11 `class Task: pass` ✓ |
| QLT-016 | confirmed | LOW | errors.py | errors.py:1 `class NoReportFoundError` ✓ |
| QLT-017 | confirmed | LOW | logger.py | file is empty (1 byte) ✓ |
| QLT-018 | confirmed | LOW | utils.py `en_to_ru` | utils.py:209 `except TypeError as err:` ✓ |
| QLT-019 | confirmed | LOW | models.py `ReportSettings` | models.py:84 `ConfigDict(extra="allow")` ✓ |
| QLT-020 | confirmed | LOW | utils.py `iter_period_slices` | utils.py:37 `Генерирует...` ✓ |

_Tally: 16 confirmed — 3 HIGH, 7 MEDIUM, 6 LOW — matching the input summary exactly._

---

### QLT-005 — confirmed — [HIGH] `yaml_to_dict` returns `dict[str, Any] \| None`; `**` unpacking ignores `None`

**Anchor resolved:** `core/utils.py::yaml_to_dict` (line 343); call sites in
`core/services.py` — `AppService.app_config` (lines 36–42), `AppService.get_report_settings`
(lines 57–64), `AppService.get_default_data_settings` (line 74). All exist at the cited code
(line numbers are stable: `code_ref d093d9c` == `HEAD d093d9c`).

**Claim tested on its own terms:** `yaml_to_dict` returns `None` on `FileNotFoundError`/
`YAMLError` (utils.py:353–360) and falls through to an implicit `return None`. The four
call sites unpack the result with `**` into `LoggingSettings` (line 38), `AppSettings`
(line 40), `ReportSettings` (line 61) and `DataDefaults` (line 74), or iterate it (line 60),
with no `None` guard.

**Evidence reproduced:**
```text
$ python -c "from mko_get_mediascope_data.core.utils import yaml_to_dict; print(repr(yaml_to_dict('nonexistent.yaml')))"
None   (logger.error fires at utils.py:359)

$ uv run mypy src/mko_get_mediascope_data/ --show-error-codes
src/.../core/services.py:37: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
src/.../core/services.py:40: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
src/.../core/services.py:60: error: Item "None" of "dict[str, Any] | None" has no attribute "__iter__"  [union-attr]
src/.../core/services.py:61: error: Argument after ** must be a mapping, not "str | Any"  [arg-type]
src/.../core/services.py:74: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
```
These are exactly the 5 errors the finding cites — reproduced verbatim.

**Rubric grading:** Phase 10 HIGH = "silent today, and the next change is guaranteed to be
made wrongly in the same place… a value with two homes." `yaml_to_dict` *declares*
`| None` but every consumer bypasses it. HIGH matches; no re-grade.

**Load-bearing artefact:** Code (`core/utils.py::yaml_to_dict`, `core/services.py::AppService`).
**Recommendation carry-out:** "Change `yaml_to_dict` to raise instead of returning `None`, or
add `None` guards." Target `utils.py::yaml_to_dict` exists and is stable. ✓
**Cross-phase:** Same root cause as ENT-003 (CRITICAL) and CFG-005 (MEDIUM) —
`yaml_to_dict` returns `None` — already cross-referenced in `.ai/audit/99-validation/01-...`
table line 411. Different phases own different angles (bootstrap / config / type-contract);
kept as cross-reference, not merged.

### QLT-006 — confirmed — [HIGH] `.get()` called on `Data` (Pydantic `BaseModel`), which has no `.get()`

**Anchor resolved:** `core/reports.py::DictDataStrategy.prepare` (lines 457–462). Line 461
is `) or report.data_settings.get("slices", None);` — reproduced.

**Claim tested on its own terms:** `Data` (models.py:124) is a `BaseModel`; Pydantic v2
models have no `.get()`. The expression `getattr(report.data_settings, "slices", None) or
report.data_settings.get(...)` short-circuits: `getattr` returns the default `[]`, which is
falsy, so `.get` is evaluated and raises `AttributeError`. `DictDataStrategy` is reached for
subtype `DYNAMICS_BY_SPOTS_DICT` → this is a documented config path.

**Evidence reproduced:**
```text
$ python -c "from mko_get_mediascope_data.core.models import Data; d=Data(); print(repr(getattr(d,'slices',None))); d.get('slices')"
[]      # falsy → `or` RHS evaluated
AttributeError: 'Data' object has no attribute 'get'

mypy: src/.../core/reports.py:461: error: "Data" has no attribute "get"  [attr-defined]
```
Both match the finding verbatim.

**Rubric grading:** Phase 10 HIGH = "a declared invariant nothing enforces." HIGH matches.
**Recommendation carry-out:** Replace `.get("slices", None)` with `getattr(...)`. Target
`DictDataStrategy.prepare` exists and is stable. ✓ (Note: QLT-005 and QLT-006 are the
finding's own "merge candidates"; QLT-006's distinct location/fix is preserved.)

### QLT-007 — confirmed — [HIGH] `period.last_time` dereferenced without `None` guard

**Anchor resolved:** `core/reports.py::TVMediaReport.get_period` (lines 223–250). Line 241
`frequency = self.report_settings.period.last_time.time_unit` — reproduced at line 241.

**Claim tested on its own terms:** `PeriodModel` (models.py:68) permits `last_time: LastTimeModel | None`; the
validator only requires one of `date_filter`/`last_time`. A `date_filter`-only config
(lines 239–240, the `else` branch) then falls through to line 241, which derefs
`last_time.time_unit` unconditionally → `AttributeError`.

**Evidence reproduced:**
```text
$ python -c "... ReportSettings(media='TV',..., period={'date_filter':['2025-01-01','2025-01-07']})"
rs.period.last_time  ->  None
rs.period.last_time.time_unit  ->  AttributeError: 'NoneType' object has no attribute 'time_unit'
mypy: src/.../core/reports.py:241: error: Item "None" of "LastTimeModel | None" has no attribute "time_unit"  [union-attr]
```
**Rubric grading:** HIGH ("declared invariant nothing enforces"). The `except Exception` at
line 248 would mask the root cause. No re-grade.
**Recommendation carry-out:** Add a `None` guard. Target `TVMediaReport.get_period` exists. ✓

### QLT-008 — confirmed — [MEDIUM] Bare `dict` annotations without type parameters

**Anchor resolved:** models.py:121 `DATA_DEFAULTS: dict`; services.py:79
`merge_data_settings(...) -> dict`; services.py:97 `data_settings: dict`; reports.py:365
`settings: dict`. All four produce `[type-arg]` under `disallow_any_generics = true`.

**Evidence reproduced (mypy):** `models.py:121`, `services.py:79`, `services.py:97`,
`reports.py:365` — all `[type-arg]`. Matches the finding's 4-site list.
**VAL-001 (LOW, completeness):** mypy also flags `tasks.py:16` (`settings: dict` param in
`TVTask.__init__`) and `utils.py:133` (`compression: str | dict = None`). The finding
enumerates four bare-`dict` sites but two more exist; the cited four are real, so the
finding is confirmed, but the evidence inventory is incomplete.
**Rubric grading:** MEDIUM ("a widened type on a value nothing branches on"). Matches.

### QLT-009 — confirmed — [MEDIUM] `NetworkClient.call` returns `Any`, defeating the type checker

**Anchor resolved:** `core/network.py::NetworkClient.call`, signature at lines 59–65;
`-> Any:` at line 65 — reproduced.
**Evidence reproduced (mypy/source):** `network.py:65: ) -> Any:`. Every caller
(`reports.py:197,258,280,301,309`, `services.py:131`) receives `Any`.
**VAL-004 (LOW, narration):** The finding "compound" citations are `utils.py:203
en_to_ru(...) -> list[Any]` and `utils.py:362 merge_dicts(...) -> dict[Any, Any]` — both
are `Any`-parameterised generics, not bare `-> Any`. The core claim (`NetworkClient.call ->
Any`) is exact; the compounding description is imprecise.
**Rubric grading:** MEDIUM ("a widened type"). Matches.

### QLT-010 — confirmed — [MEDIUM] Fixed-value `"DICT"` checked via string `in` on enum value

**Anchor resolved:** `core/reports.py::ReportFactory.data_strategy`, line 42–44.
`if "DICT" in self.report_subtype.value.upper():` at line 44 — reproduced.
**Evidence reproduced (source/grep):** The `ReportSubtype` enum (models.py:50–58) is the
mandated home; `DYNAMICS_BY_SPOTS_DICT = "DYNAMICS_BY_SPOTS_DICT"`. The branch bypasses it
with a raw substring literal — verified by reading line 44. A subtype whose `value`
contained `"DICT"` would be silently routed to `DictDataStrategy`.
**VAL-005 (LOW, narration):** The finding invokes the HIGH-band phrase "value with two
homes" but grades MEDIUM. The present-state blast radius (a *future* subtype containing
`DICT`) is MEDIUM; HIGH's "guaranteed to be made wrongly next change" does not hold. Grade
is defensible; the phrase is over-applied.
**Rubric grading:** MEDIUM. Recommendation (enum identity comparison) targets `ReportFactory.data_strategy`. ✓

### QLT-011 — confirmed — [MEDIUM] `csv_to_file` forwards `*args/**kwargs` to `to_csv` unchecked

**Anchor resolved:** `core/utils.py::csv_to_file` (lines 167–200). `*args, **kwargs` at
lines 177–178; `data_frame.to_csv(*args, ...)` at line 188.
**Evidence reproduced (mypy):** `utils.py:188: No overload variant of "to_csv" matches
argument types ... "tuple[Any, ...]", "Path", ... [call-overload]`. The sole call site
(reports.py:325–332) passes `df` positionally and all others by keyword → `*args` is dead.
**Rubric grading:** MEDIUM ("a widened type"). Matches.

### QLT-012 — confirmed — [MEDIUM] `get_last_period` mixes `date`/`datetime`; 5 `union-attr`

**Anchor resolved:** `core/utils.py::get_last_period` (lines 88–130). Line 95
`today = date.today() if today is None else today` — reproduced; param is `datetime | None`.
**Evidence reproduced (mypy, verbatim):** `utils.py:25 [assignment]`, `95 [assignment]`,
`98/99/100/104 [union-attr]`, `128 [union-attr]`, `130 [return-value]`. The 5 `union-attr` +
1 `assignment` + 1 `return-value` match the finding's Appendix-B listing exactly.
**Rubric grading:** MEDIUM. Matches.

### QLT-013 — confirmed — [MEDIUM] `TVTask.__init__` untyped; inferred `None` causes downstream conflicts

**Anchor resolved:** `core/tasks.py::TVTask.__init__` (lines 15–27). `settings: dict`
bare at line 16; `self.key = None` / `self.interval = None` / `self.target = None` at
lines 20–25 — reproduced. mypy config has `check_untyped_defs = true` but **no**
`disallow_untyped_defs` (pyproject.toml:134–145 confirms the omission) — the claim's root
cause holds.
**Evidence reproduced (mypy):** `tasks.py:16 [type-arg]`; `reports.py:262 [assignment]`
(`task.key = {}`); `reports.py:376/377 [assignment]` (`task.interval`/`task.target`).
**Rubric grading:** MEDIUM ("a widened type"). Matches.

### QLT-014 — confirmed — [MEDIUM] `ignore_missing_imports` does not suppress `[import-untyped]`; stubs absent

**Anchor resolved:** `pyproject.toml::tool.mypy`, line 145 `ignore_missing_imports = true`.
**Evidence reproduced:** The 4 `import-untyped` errors (tasks.py:5 `yaml`, network.py:11
`requests.exceptions`, utils.py:9 `yaml`, utils.py:10 `dateutil.relativedelta`) appear
**even though** `ignore_missing_imports = true` is set (my config-level run) **and** still
appear under the explicit `--ignore-missing-imports` flag — mypy's documented behaviour:
the flag suppresses `import-not-found`, not `import-untyped`. Dev deps (pyproject.toml:166–
177) ship `pandas-stubs` but not `types-PyYAML`, `types-requests`, or
`types-python-dateutil`.
**Rubric grading:** MEDIUM. Matches.

### QLT-015 — confirmed — [LOW] Empty `Task` base class, inherited by `TVTask`, defines nothing

**Anchor resolved:** `core/tasks.py::Task` (lines 10–11): `class Task:` / `    pass`.
`TVTask(Task)` (line 14) never calls `super().__init__()`. `git grep '\bTask\b'` in `src/`
returns only `tasks.py:10` (definition) and `tasks.py:14` (base). No import/usage elsewhere.
**Rubric grading:** LOW ("an unreferenced definition nobody has explained"). Matches.

### QLT-016 — confirmed — [LOW] `NoReportFoundError`/`WrongDataSettings` defined but never raised

**Anchor resolved:** `core/errors.py` lines 1–6. `git grep` for `raise NoReportFoundError |
raise WrongDataSettings` in `src/` → 0 matches; no `from ...errors import` anywhere in
`src/`. The only repo occurrences outside `errors.py` are AGENTS.md and prior audit text.
**Evidence reproduced (git grep):** `errors.py:1 class NoReportFoundError(Exception)`,
`errors.py:5 class WrongDataSettings(Exception)`; zero `raise` in `src/`. The cross-ref
claims hold: 05-config-templates findings.md:146 recommends raising `WrongDataSettings` in
`get_default_data_settings`; 04-data-pipeline findings.md:242 recommends `NoReportFoundError`
for empty task sets — both accurate.
**Rubric grading:** LOW. Matches.

### QLT-017 — confirmed — [LOW] `core/logger.py` empty placeholder, never imported

**Anchor resolved:** `core/logger.py` is 1 byte (a single newline); AGENTS.md:45 labels it
`(placeholder)`. `git grep` for `core.logger` / `from ...logger import` in `src/` and
`tests/` → 0 matches. Logging is actually configured via
`services.py:165 logging.config.dictConfig`.
**Rubric grading:** LOW. Matches.

### QLT-018 — confirmed — [LOW] `en_to_ru` catches `TypeError` but `.replace()` raises `AttributeError`

**Anchor resolved:** `core/utils.py::en_to_ru` (lines 203–211). `param.replace(...)` at line
207; `except TypeError` at line 209.
**Evidence reproduced:** `en_to_ru([1, 2, 3])` → `AttributeError: 'int' object has no
attribute 'replace'` originating at utils.py:207, propagating **uncaught** past the
`except TypeError` at line 209 — the handler never fires. (VAL-004-family precision: the
narrative's aside that `len(slices_en)` would raise `TypeError` is technically wrong —
`len` on a list never raises — but it is incidental to the verified core claim.)
**Rubric grading:** LOW. Matches.

### QLT-019 — confirmed — [LOW] `extra="allow"` silently accepts unknown YAML keys

**Anchor resolved:** `core/models.py` — `ReportSettings` (line 84), `DataDefaults`
(line 120), `Data` (line 125) all set `ConfigDict(extra="allow")`. (`LastTimeModel` line 62
sets `extra="forbid"`.)
**Evidence reproduced:** `ReportSettings(relitive_path='test', ...)` →
`ValidationError: relative_path — Field required` (the `relitive_path` typo was silently
accepted as an extra field). Confirmed.
**VAL-002 (LOW, narration):** The finding's narrative also claims "LastTimeModel and
PeriodModel use `extra='forbid'`." LastTimeModel does (line 62); `PeriodModel` has **no**
`model_config` (defaults to `"ignore"`, not `"forbid"`). The finding's *verdict* — the three
models accept extra keys — is unaffected (all three `allow` sites are real).
**Rubric grading:** LOW. Matches.

### QLT-020 — confirmed — [LOW] Russian comments/docstrings/log messages throughout `core/`

**Anchor resolved:** `core/utils.py::iter_period_slices`, line 37 docstring
`Генерирует последовательные интервалы...` — reproduced. AGENTS.md rule 1 mandates
English-only comments/logs/docstrings/error text; `doc-maintenance-rules.md` reinforces it.
**Evidence reproduced (Grep, Cyrillic `[А-яЁё]`):** 45 matches across **6** core modules:
utils.py (16), reports.py (12), network.py (8), services.py (5), models.py (3), tasks.py (1).
`logger.py`, `errors.py`, `paths.py`, `init_service.py` contain none.
**VAL-003 (LOW, scope):** The finding narrates "every module under `core/`" and "all seven
core/ modules," and its `rg --stats` lists 5. Actual: 6 of 10 core modules carry Cyrillic
(notably including `models.py`, which the evidence table omits). The violation is real and
broader than cited; the over-statement is a narration imprecision, not a falsehood.
**Rubric grading:** LOW ("documentation… drift"). Matches. **Cross-phase:** Complementary to
ENT-007 (app.py entry layer); QLT-020 correctly owns the `core/` scope.

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Issue |
|---|---|---|---|
| VAL-001 | LOW | recorded | QLT-008 enumerates 4 bare-`dict` sites but mypy flags 6 (missed tasks.py:16, utils.py:133) |
| VAL-002 | LOW | recorded | QLT-019 claims PeriodModel sets `extra="forbid"`; it has no `model_config` (defaults to `"ignore"`). LastTimeModel (line 62) alone is `forbid` |
| VAL-003 | LOW | recorded | QLT-020 narrates "every module"/"seven core/ modules"; actually 6 of 10 core modules carry Cyrillic, including `models.py` (omitted from its evidence table) |
| VAL-004 | LOW | recorded | QLT-009 calls `en_to_ru`→`list[Any]` and `merge_dicts`→`dict[Any, Any]` "Any-returning"; they are `Any`-parameterised generics, not bare `-> Any` |
| VAL-005 | LOW | recorded | QLT-010 invokes HIGH-band "value with two homes" language while grading MEDIUM; present-state blast radius is MEDIUM-grade |

### VAL-001 — [LOW] QLT-008 under-counts bare-`dict` sites
`uv run mypy --show-error-codes` reports `type-arg` on `tasks.py:16` (`settings: dict`) and
`utils.py:133` (`compression: str | dict = None`) in addition to the four cited. The four
cited sites are real; the enumeration is simply incomplete. Finding verdict unchanged.

### VAL-002 — [LOW] QLT-019 misstates PeriodModel's `extra` policy
`models.py`: line 62 `LastTimeModel` → `extra="forbid"`; lines 84/120/125 → `extra="allow"`;
`PeriodModel` (lines 68–80) has no `model_config` → Pydantic default `"ignore"`. The finding's
verdict (three models silently accept unknown keys) holds; the supporting sentence about
PeriodModel is inaccurate.

### VAL-003 — [LOW] QLT-020 over-states Cyrillic scope
Grep `[А-яЁё]` over `core/` = 45 hits in 6 modules (utils 16, reports 12, network 8,
services 5, models 3, tasks 1). `logger.py`/`errors.py`/`paths.py`/`init_service.py` are clean.
The violation is real and slightly broader than the narration.

### VAL-004 — [LOW] QLT-009 compounds imprecisely
`en_to_ru` returns `list[Any]`, `merge_dicts` returns `dict[Any, Any]` — `Any`-parameterised,
not a bare `-> Any`. The primary claim (`NetworkClient.call -> Any`, network.py:65) is exact.

### VAL-005 — [LOW] QLT-010 grading narration drift
The fix (enum identity comparison) is sound and the MEDIUM grade fits the future-subtype blast
radius; the "value with two homes" phrasing is HIGH-band language that does not strictly apply.

---

## Cross-Phase Analysis

| QLT finding | Sibling finding(s) | Shared subject | Disposition |
|---|---|---|---|
| QLT-005 | ENT-003 (CRIT) / CFG-005 (MED) | `yaml_to_dict` returns `None` | cross-reference; all confirmed; distinct owning phase/angle — no merge |
| QLT-005 + QLT-006 + QLT-007 | (within QLT) | None-safety at call sites / dict-style access on models | QLT's own "merge candidates"; kept as 3 distinct defects at 3 distinct symbols |
| QLT-006 | QLT-005 | missing-config / None cascade | cross-reference (acknowledged in QLT Fix Order) |
| QLT-016 | 05-config-templates:146, 04-data-pipeline:242 | `WrongDataSettings`/`NoReportFoundError` meant to be raised | cross-reference; recommendation provenance accurate |
| QLT-019 | QLT-005, QLT-007 | config-file None handling | adjacent (same cluster) |
| QLT-020 | ENT-007 (LOW) | English-only rule | complementary scope (app.py vs core/); no conflict |
| QLT-013 | QLT-008, QLT-009 | untyped `dict`/`Any` | adjacent, same cluster |

**No cross-phase conflicts.** QLT-005/006/007 are the input report's own "merge candidates"; I
keep them as three distinct confirmed defects because they sit at three distinct symbols
(`services.py` call sites vs `reports.py::DictDataStrategy.prepare` vs
`reports.py::TVMediaReport.get_period`) with three distinct fixes — merging would obscure the
remediation targets, exactly what phase 99 warns against ("record rather than silently edit a
merge"). ENT-003/CFG-005/QLT-005 share a root cause but each phase owns a distinct concern
(bootstrap / config / type-contract), matching the precedent set by the 01-validation
cross-phase table — cross-reference, not merge.

## Rollout Safety Assessment

**Validated from source (`pyproject.toml:166–177`) and `tests/` (12 green):**
- QLT-008 + QLT-013 are prerequisites for QLT-009 (annotating `TVTask` and bare `dict`s
  clarifies the types `NetworkClient.call`/`en_to_ru` should return). Sequencing holds.
- QLT-005/006/007 cluster: a single refactor of `yaml_to_dict` (raise instead of `None`)
  plus `.get`→`getattr` resolves all three; QLT-005's `yaml_to_dict` change is the
  prerequisite for QLT-016's recommendation (wire `WrongDataSettings` into
  `get_default_data_settings`).
- **QLT-019 rollout risk (user boundary):** changing `ReportSettings` to `extra="forbid"`
  can break existing report YAMLs that rely on undocumented pass-through keys. Mitigation:
  audit the 16 bundled `settings/reports/**/*.yaml` first; keep `test_models.py::
  test_report_settings_valid` green. `Data`/`DataDefaults` may retain `allow` if Mediascope
  passthrough keys are needed (document the rationale).
- QLT-014 (adding stubs), QLT-010 (enum comparison), QLT-018 (widened `except`), QLT-020
  (English translation) are non-breaking / behaviour-preserving.
- QLT-013 (annotating `TVTask`) will likely convert the current "inferred None" errors into
  real `assignment` errors at `reports.py:262/376/377` — intended, and part of the fix's
  value; sequenced after QLT-008.

**Backward compatibility:** Every recommendation preserves existing test contracts. The
12 shipped tests mock `create_report`/use plain-dict `data_settings`, so none assert the
buggy behaviour — no test is a remediation blocker. The sole behaviour change at a user
boundary is QLT-019's `ReportSettings` strictness, gated as above.

## Evidence Index

| Evidence | Source |
|---|---|
| `mypy src/mko_get_mediascope_data/ --show-error-codes` → "Found 61 errors in 7 files" | reproduced (mypy 1.19.1) |
| `mypy --ignore-missing-imports` → still 4 `import-untyped` | reproduced (QLT-014) |
| `ruff check src/mko_get_mediascope_data/` → exit 0, no violations | reproduced (supports "ruff passes cleanly") |
| `pytest` → 12 passed | reproduced (green controls; mocked paths confirm coverage gaps, not the findings) |
| `yaml_to_dict('nonexistent.yaml')` → `None` | reproduced (QLT-005) |
| `Data().get('slices')` → `AttributeError` | reproduced (QLT-006) |
| `ReportSettings(period=date_filter-only).period.last_time.time_unit` → `AttributeError` | reproduced (QLT-007) |
| `en_to_ru([1,2,3])` → `AttributeError` escapes `except TypeError` | reproduced (QLT-018) |
| `ReportSettings(relitive_path=...)` → `ValidationError: relative_path required` (typo swallowed) | reproduced (QLT-019) |
| `git grep '\bTask\b'` → only tasks.py:10,14 | reproduced (QLT-015) |
| `git grep raise NoReportFoundError\|WrongDataSettings` → 0; no `errors` import in src/ | reproduced (QLT-016) |
| `git grep core.logger` / `from ...logger import` → 0 | reproduced (QLT-017) |
| Grep Cyrillic `[А-яЁё]` in `core/` → 45 hits, 6 modules | reproduced (QLT-020) |
| `git rev-parse --short HEAD` → `d093d9c` | code_ref matches HEAD (no drift) |
| `pyproject.toml:134–145` (mypy lacks `disallow_untyped_defs`) | source (QLT-013 root cause) |
| `pyproject.toml:166–177` (dev deps: `pandas-stubs` only, no `types-*` stubs) | source (QLT-014) |
| `AGENTS.md` rule 1 (English Only) + rule 10 (Enums) + rule 9 (Type Safety) + rule 12 (no `print`) | source (QLT-010/019/020/016 anchors) |
| `.kilo/commands/audit/phases/10-audit-code-quality.md` severity taxonomy | rubric of record |
| `99-validation/01-...` (ENT-003/007 confirmed; yaml_to_dict cluster tabled) | cross-phase sibling |
| `99-validation/05-...` / `04-data-pipeline/findings.md:242` | QLT-016 recommendation provenance |
| `02-config-secrets/findings.md:41,108` (CFG-005 = yaml_to_dict None) (ENT-003/CFG-005) | cross-phase sibling |

## Disposition Tally

| Disposition | Count |
|---|---|
| confirmed | 16 |
| re-typed | 0 |
| re-graded | 0 |
| merged | 0 |
| not substantiated | 0 |
| unsettled | 0 |
| **Total** | **16** |

**Validation-level findings (VAL-): 5** (VAL-001, VAL-002, VAL-003, VAL-004, VAL-005 — all LOW)

**Namespace ruling (block 7):** prefix `QLT-` is declared by phase 10 and used only here;
no `QLT-` identifier exists in any shipped `.py`/`.yaml`/`.toml` (git grep across repo,
excluding `.ai/audit/`). The template's `QLT-001/002/004` are scaffolding for a different
project (`src/backend/apps/ads/`). `VAL-` is the validation namespace and occupies this file's own
section only. Ruling: namespace clean; no collision.


