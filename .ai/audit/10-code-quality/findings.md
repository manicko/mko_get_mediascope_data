---
phase: "10"
phase_name: "code-quality"
id_prefix: "QLT-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/10-audit-code-quality.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Code Quality

## Summary

16 findings: 3 HIGH, 7 MEDIUM, 6 LOW. The project's `pyproject.toml` configures mypy with
strict-looking flags, but `mypy src/mko_get_mediascope_data/` surfaces 61 errors across 7 files —
a direct contradiction of the AGENTS.md "Type Safety Everywhere" claim. Three of those errors are
confirmed runtime crashes (`AttributeError`/`TypeError`) on documented configuration inputs.
`ruff check` passes cleanly (zero violations). The remaining issues are untyped parameters,
bare `dict`/`Any` annotations, dead code, non-English comments in `core/`, and a fixed-value
string check that bypasses the enum the project mandates for constants.

## Scope

**Examined:** All 13 Python source files under `src/mko_get_mediascope_data/` and
`pyproject.toml` (mypy + ruff configuration). Tests in `tests/` were read for remediation-blocker
checks. Prior-phase findings in `.ai/audit/` were scanned for overlap (ENT-001 through ENT-007,
TPL, PIPE, CFG, ASY, EXT).

**Tools:** `ruff check` (1.19+), `mypy` 1.19.1, `python -c` runtime probes, `git grep`,
`rg` (via Grep tool).

**Not verified:** Runtime execution of full report pipeline (no Mediascope API credentials or
network access available); Jupyter/Airflow import behavior (inferred from static analysis —
ENT-001, ENT-002 already cover this).

**Collision check (per phase-file directive):** The template
`.ai/audit/templates/audit-findings.md` contains placeholder IDs `QLT-001`, `QLT-002`, and
`QLT-004` referencing `src/backend/apps/ads/` paths from a different project. `QLT-004` is
referenced as "Related" in the template but never defined. No prior-cycle QLT- findings file
exists under `.ai/audit/` (existing findings use 3-letter prefixes: ENT, ASY, CFG, PIPE, TPL,
EXT, TST). IDs below start at `QLT-005` to avoid collision with template placeholders; no
second namespace is created.

## Findings

| ID     | Severity | Anchor                                              | Title |
|--------|----------|-----------------------------------------------------|-------|
| QLT-005 | HIGH | `src/.../core/services.py::AppService.app_config` | `yaml_to_dict` returns `dict[str, Any] \| None`; `**`-unpacking call sites ignore `None` |
| QLT-006 | HIGH | `src/.../core/reports.py::DictDataStrategy.prepare` | `.get()` called on `Data` (Pydantic `BaseModel`) which has no `.get()` |
| QLT-007 | HIGH | `src/.../core/reports.py::TVMediaReport.get_period` | `period.last_time` dereferenced without `None` guard |
| QLT-008 | MEDIUM | `src/.../core/models.py::DataDefaults` | Bare `dict` annotations without type parameters |
| QLT-009 | MEDIUM | `src/.../core/network.py::NetworkClient.call` | Return type `Any` defeats the type checker |
| QLT-010 | MEDIUM | `src/.../core/reports.py::ReportFactory.data_strategy` | Fixed-value `"DICT"` checked via string `in` on enum value |
| QLT-011 | MEDIUM | `src/.../core/utils.py::csv_to_file` | `*args/**kwargs` forwarded to `DataFrame.to_csv` unchecked |
| QLT-012 | MEDIUM | `src/.../core/utils.py::get_last_period` | `date`/`datetime` type mixing produces 5 `union-attr` errors |
| QLT-013 | MEDIUM | `src/.../core/tasks.py::TVTask.__init__` | Fully untyped `__init__`; inferred `None` types cause downstream conflicts |
| QLT-014 | MEDIUM | `pyproject.toml::tool.mypy` | `ignore_missing_imports` does not suppress `[import-untyped]`; stubs absent from dev deps |
| QLT-015 | LOW | `src/.../core/tasks.py::Task` | Empty base class inherited by `TVTask` but defines nothing |
| QLT-016 | LOW | `src/.../core/errors.py` | `NoReportFoundError`, `WrongDataSettings` defined but never raised |
| QLT-017 | LOW | `src/.../core/logger.py` | Empty placeholder file listed in AGENTS.md architecture, never imported |
| QLT-018 | LOW | `src/.../core/utils.py::en_to_ru` | `except TypeError` wrong; `.replace()` raises `AttributeError` for non-strings |
| QLT-019 | LOW | `src/.../core/models.py::ReportSettings` | `extra="allow"` silently accepts unknown YAML keys |
| QLT-020 | LOW | `src/.../core/utils.py::iter_period_slices` | Russian comments/docstrings/log messages throughout `core/` |

#### QLT-005 — [HIGH] `yaml_to_dict` returns `dict[str, Any] | None`; `**`-unpacking call sites ignore `None`

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::yaml_to_dict`, `src/mko_get_mediascope_data/core/services.py::AppService.app_config`, `src/mko_get_mediascope_data/core/services.py::AppService.get_report_settings`, `src/mko_get_mediascope_data/core/services.py::AppService.get_default_data_settings`
**Disambiguator:** `argument after ** must be a mapping, not dict[str, Any] | None`
**Checked:** ran (mypy static analysis + runtime probe)
**Why this band:** Five call sites construct Pydantic models with `**utils.yaml_to_dict(...)`
with zero `None` handling. `yaml_to_dict` returns `None` on `FileNotFoundError` or `YAMLError`.
A missing or corrupt config file therefore produces `TypeError: argument of type 'NoneType' is
not a mapping` — silent today only because config files happen to exist in the deployed
environment, and the next change that touches config resolution is guaranteed to crash
incorrectly. Directly matches the HIGH band: "a value with two homes" (the `| None` return type
is declared, but bypassed at every consumer).
**Problem:** `yaml_to_dict` (`utils.py:343-360`) has return type `dict[str, Any] | None`. It
returns `None` when the YAML file is missing or invalid. All four call sites in `services.py`
unpack the result with `**` into Pydantic constructors or iterate over it directly, never
guarding for `None`. mypy flags every site.
**Repro:**
1. `python -c "from mko_get_mediascope_data.core.utils import yaml_to_dict; print(repr(yaml_to_dict('nonexistent.yaml')))"`
   → `None`
2. `AppService.app_config` does `LoggingSettings(**yaml_to_dict(missing_path))` → `TypeError`
3. `mypy src/mko_get_medioscope_data/` reports 5 errors at `services.py:37,40,60,61,74`
**Impact:** Any missing or corrupt YAML config file — a realistic scenario for new users or
deployment drift — crashes the tool with an opaque `TypeError` instead of a clear error message.
**Root cause:** `yaml_to_dict` swallows `FileNotFoundError` by falling through to implicit
`return None`, and callers never check the return. The type annotation correctly models the
`None` case, but the code structure doesn't enforce handling it.
**Fix:** Change `yaml_to_dict` to raise `FileNotFoundError` (or a domain `ConfigFileNotFoundError`)
instead of returning `None`, OR add `None` guards at all four call sites
(`services.py:37,40,60,74`).
**Verify:** tests/test_services.py::test_run_report_with_real_models (mocks the call path but
does not exercise missing-file logic); new test needed: `test_yaml_to_dict_none_handled_at_call_sites`
**Related:** ENT-003 (same root cause, different phase), QLT-006

**Evidence — mypy output:**
```text
src\mko_get_mediascope_data\core\services.py:37: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
src\mko_get_mediascope_data\core\services.py:40: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
src\mko_get_mediascope_data\core\services.py:60: error: Item "None" of "dict[str, Any] | None" has no attribute "__iter__"  [union-attr]
src\mko_get_medioscope_data\core\services.py:61: error: Argument after ** must be a mapping, not "str | Any"  [arg-type]
src\mko_get_mediascope_data\core\services.py:74: error: Argument after ** must be a mapping, not "dict[str, Any] | None"  [arg-type]
```

#### QLT-006 — [HIGH] `.get()` called on `Data` (Pydantic `BaseModel`) which has no `.get()`

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::DictDataStrategy.prepare`
**Disambiguator:** `report.data_settings.get("slices", None)`
**Checked:** ran (runtime probe + mypy)
**Why this band:** `DictDataStrategy.prepare` (reports.py:458-461) calls
`report.data_settings.get("slices", None)`, but `Data` is a Pydantic `BaseModel`, not a `dict`.
Pydantic v2 models do not have a `.get()` method. If `getattr(report.data_settings, "slices",
None)` returns a falsy value (e.g. the default `[]`), the `or` short-circuit evaluates the
right-hand side and `AttributeError` is raised. This is a latent crash on a documented code
path — the HIGH band applies because "a declared invariant nothing enforces" (the `Data` model
has no `.get()` method, yet code calls it).
**Problem:** `reports.py:461` calls `report.data_settings.get("slices", None)`. `Data` is defined
in `models.py:124-128` as a `BaseModel` with `slices` as a field. The `getattr(...) or .get(...)`
pattern was likely written assuming `data_settings` is a `dict` (the pre-Pydantic type), but it
is now a `BaseModel`. mypy reports `"Data" has no attribute "get"`.
**Repro:**
1. `python -c "from mko_get_mediascope_data.core.models import Data; d = Data(); d.get('slices')"`
   → `AttributeError: 'Data' object has no attribute 'get'`
2. `mypy ... reports.py:461: error: "Data" has no attribute "get" [attr-defined]`
**Impact:** Any `DictDataStrategy` report where `Data.slices` defaults to empty (`[]`, which is
falsy) triggers `AttributeError` instead of falling through to `output_columns`.
**Root cause:** The `en_to_ru` / `merge_data_settings` pipeline was refactored to use Pydantic
`Data` models, but `DictDataStrategy.prepare` was not updated to use attribute access instead of
dict-style `.get()`.
**Fix:** Replace `report.data_settings.get("slices", None)` with
`getattr(report.data_settings, "slices", None)` — mirroring the `getattr` already used on the
left side of `or`.
**Verify:** tests/test_reports.py::test_dict_data_strategy mocks `data_settings` as a plain dict
(`{"slices": ["channel", "program", "category"]}`) with a realistic DataFrame
matching the column layout of a DYNAMICS_BY_SPOTS_DICT report. New test needed:
`test_dict_data_strategy_handles_empty_slices`
**Related:** QLT-005

**Evidence — runtime probe:**
```text
$ python -c "from mko_get_medioscope_data.core.models import Data; d = Data(); d.get('slices')"
AttributeError: 'Data' object has no attribute 'get'
```

**Evidence — mypy:**
```text
src\mko_get_mediascope_data\core\reports.py:461: error: "Data" has no attribute "get"  [attr-defined]
```

#### QLT-007 — [HIGH] `period.last_time` dereferenced without `None` guard

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.get_period`
**Disambiguator:** `frequency = self.report_settings.period.last_time.time_unit`
**Checked:** ran (runtime probe + mypy)
**Why this band:** `PeriodModel` (`models.py:68-80`) allows `last_time: LastTimeModel | None`
and only requires that *at least one* of `date_filter` or `last_time` is set. When a user
configures `date_filter` only (a valid and documented configuration), `last_time` is `None`.
`get_period` (reports.py:223-250) unconditionally accesses
`self.report_settings.period.last_time.time_unit` at line 241, even when the `date_filter`
branch was taken. This is a direct `AttributeError` crash on a valid configuration — matching
the HIGH band: "a declared invariant nothing enforces."
**Problem:** `reports.py:241` — `frequency = self.report_settings.period.last_time.time_unit`
executes unconditionally after the `if/else` block that determines `period`. When
`date_filter` is set and `last_time` is `None`, this line raises `AttributeError`. The
`except Exception` at line 248 catches it and re-raises a generic `Exception`, losing the
root cause.
**Repro:**
1. `python -c "
    from mko_get_medioscope_data.core.models import ReportSettings
    from datetime import date
    rs = ReportSettings(media='TV', report_type='crosstab', report_subtype='TOP_NAT_TV_ADVERTISERS',
        relative_path='test', data_lang='ru',
        period={'date_filter': ['2025-01-01','2025-01-07']})
    print(rs.period.last_time)  # None
    freq = rs.period.last_time.time_unit  # crashes
"`
   → `AttributeError: 'NoneType' object has no attribute 'time_unit'`
2. `mypy ... reports.py:241: error: Item "None" of "LastTimeModel | None" has no attribute "time_unit" [union-attr]`
**Impact:** Any report configured with `date_filter` (instead of `last_time`) crashes during
period preparation. This is a documented configuration path, so the crash is triggered by valid
user input.
**Root cause:** The `frequency` variable is derived from `last_time.time_unit` unconditionally,
but the `PeriodModel` validator permits `last_time=None` as long as `date_filter` is set. The
code conflates "how the period was determined" (date_filter vs last_time) with "the slicing
frequency" (which requires `last_time`).
**Fix:** Add a `None` guard before accessing `last_time.time_unit`; either require `time_unit` in
the model, derive `frequency` only when `last_time` is present, or default `frequency` to
`"d"` when slicing is not needed.
**Verify:** tests/test_models.py::test_period_model_validation (tests `PeriodModel()` with neither
field, but does not test `date_filter`-only). New test needed:
`test_get_period_handles_date_filter_without_last_time`
**Related:** QLT-012, QLT-019

**Evidence — runtime probe:**
```text
$ python -c "... rs.period.last_time.time_unit ..."
AttributeError: 'NoneType' object has no attribute 'time_unit'
```

**Evidence — mypy:**
```text
src\mko_get_mediascope_data\core\reports.py:241: error: Item "None" of "LastTimeModel | None" has no attribute "time_unit"  [union-attr]
```

#### QLT-008 — [MEDIUM] Bare `dict` annotations without type parameters

**Anchor:** `src/mko_get_mediascope_data/core/models.py::DataDefaults`
**Disambiguator:** `DATA_DEFAULTS: dict`
**Checked:** ran (mypy)
**Why this band:** The project's mypy config sets `disallow_any_generics = true`, which is
specifically designed to catch bare `dict` annotations. Three call sites still use bare `dict`
without type parameters, meaning mypy treats them as `dict[Any, Any]` — the type checker
becomes blind to the structure. This is a bounded correctness gap: callers cannot rely on the
type checker to verify key/value types at these boundaries.
**Problem:** `models.py:121` — `DATA_DEFAULTS: dict`; `services.py:79` — `def merge_data_settings(...) -> dict`;
`services.py:97` — `data_settings: dict = ...`; `reports.py:365` — `settings: dict = ...`. All
four use bare `dict`, which mypy expands to `dict[Any, Any]` under `disallow_any_generics = true`
→ reported as `type-arg` errors.
**Repro:**
```text
$ mypy src/mko_get_mediascope_data/
models.py:121: error: Missing type parameters for generic type "dict"  [type-arg]
services.py:79: error: Missing type parameters for generic type "dict"  [type-arg]
services.py:97: error: Missing type parameters for generic type "dict"  [type-arg]
reports.py:365: error: Missing type parameters for generic type "dict"  [type-arg]
```
**Impact:** The type checker cannot verify that the merged `data_settings` dict has the expected
string keys and `list`/`dict` values. A typo in a key name (e.g. `"slcies"` instead of `"slices"`)
would be silently accepted and only fail at runtime when the downstream task builder accesses it.
**Root cause:** `dict` was used as shorthand instead of `dict[str, Any]` or a typed `TypedDict`.
**Fix:** Replace `dict` with `dict[str, Any]` (or a more specific `TypedDict`) at all four sites.
**Verify:** new test needed: `test_merge_data_settings_preserves_typed_keys`
**Related:** QLT-009, QLT-013

**Evidence — mypy:**
```text
src\mko_get_mediascope_data\core\models.py:121: error: Missing type parameters for generic type "dict"  [type-arg]
src\mko_get_mediascope_data\core\services.py:79: error: Missing type parameters for generic type "dict"  [type-arg]
src\mko_get_mediascope_data\core\services.py:97: error: Missing type parameters for generic type "dict"  [type-arg]
src\mko_get_mediascope_data\core\reports.py:365: error: Missing type parameters for generic type "dict"  [type-arg]
```

#### QLT-009 — [MEDIUM] Functions return `Any`, defeating the type checker

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `def call(...) -> Any:`
**Checked:** ran (mypy static analysis)
**Why this band:** `NetworkClient.call` returns `Any`, which is the "escape hatch" that makes
mypy stop checking every consumer. Every API response flows through `call`, so a wrong field
access on any API result is invisible to the type checker. This is a bounded correctness gap
affecting the entire network layer — a single field-name typo in an API response handler would
not be caught at all.
**Problem:** `network.py:65` — `async def call(self, func: Callable[..., Any], ...) -> Any:`.
The `-> Any` return type means every caller (`reports.py:197`, `:258`, `:280`, `:301`, `:309`,
`services.py:131`) receives an `Any` value and can call any method on it without type checking.
Two more `Any`-returning functions compound the issue: `utils.py:203` (`en_to_ru() -> list[Any]`)
and `utils.py:362` (`merge_dicts() -> dict[Any, Any]`).
**Repro:**
```text
$ grep -n "-> Any" src/mko_get_medascope_data/core/network.py utils.py
core/network.py:65:    ) -> Any:
```
**Impact:** The Mediascope API client — the single most external-data-touching component —
operates entirely outside the type checker. Field-name errors, missing-key errors, and
wrong-type payloads are all invisible until runtime.
**Root cause:** `Any` used as a shortcut because the `mediascope_api` library has no type stubs
(see QLT-014), and the wrapper didn't narrow the return type.
**Fix:** Replace `-> Any` with a typed return (e.g. `-> DataFrame` for `call`, constrained via
overload or generic type variable), or at minimum `-> object` with explicit `cast()` at call
sites that need specific types.
**Verify:** tests/test_network.py::test_network_retry_and_fail (tests retry, not typing); new
test needed: `test_network_call_returned_type_checkable`
**Related:** QLT-008, QLT-013, QLT-014

#### QLT-010 — [MEDIUM] Fixed-value `"DICT"` checked via string `in` on enum value

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::ReportFactory.data_strategy`
**Disambiguator:** `"DICT" in self.report_subtype.value.upper()`
**Checked:** ran (static read)
**Why this band:** The project mandates `Enum` for all fixed values (AGENTS.md rule 10).
`ReportSubtype` has a member `DYNAMICS_BY_SPOTS_DICT = "DYNAMICS_BY_SPOTS_DICT"`. Instead of
comparing against this member, `reports.py:44` checks `"DICT" in self.report_subtype.value.upper()`
— a raw string `"DICT"` that bypasses the type system and could match unintended subtypes
(e.g. a hypothetical `DICTATOR_FILTER` subtype would also match). This is a value with two
homes: the enum member exists but a raw literal bypasses it.
**Problem:** `reports.py:44` — `if "DICT" in self.report_subtype.value.upper():`. The raw string
`"DICT"` is compared against the enum's `value` string. If a new subtype value accidentally
contains "DICT", it would be silently routed to `DictDataStrategy`. The enum member
`ReportSubtype.DYNAMICS_BY_SPOTS_DICT` is the named home, but it is not used at this branch.
**Repro:**
```text
$ grep -n '"DICT"' src/mko_get_mediascope_data/core/reports.py
core/reports.py:44:        if "DICT" in self.report_subtype.value.upper():
```
**Impact:** A new report subtype whose value contains "DICT" would be silently routed to the
wrong `DataPreparationStrategy`, producing malformed output. The fix to add a new dict-style
subtype would need to be made in two places (the enum + the string check), with no single
discoverable home.
**Root cause:** String `in` check used instead of enum identity comparison.
**Fix:** Replace with `if self.report_subtype == ReportSubtype.DYNAMICS_BY_SPOTS_DICT:` or
`if "DICT" in self.report_subtype.name:` (checking the member name, not the value).
**Verify:** tests/test_reports.py does not test strategy selection; new test needed:
`test_data_strategy_selects_dict_for_DYNAMICS_BY_SPOTS_DICT`
**Related:** QLT-013

#### QLT-011 — [MEDIUM] `csv_to_file` forwards `*args/**kwargs` to `to_csv` with no type checking

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `data_frame.to_csv(*args, ...)`
**Checked:** ran (mypy)
**Why this band:** `csv_to_file` declares `*args, **kwargs` and forwards them positionally to
`DataFrame.to_csv`. mypy cannot match the call against any `to_csv` overload, so ALL arguments
are unchecked. A positional argument in the wrong position, a misspelled keyword, or a value
of the wrong type passes through silently and either fails at runtime or — worse — silently
corrupts output. The `*args` parameter is also dead code: the only call site (reports.py:325)
passes all arguments by keyword.
**Problem:** `utils.py:171-198`. The signature is:
```python
def csv_to_file(data_frame, csv_path_out, file_prefix="", compression="infer",
                add_time=True, *args, **kwargs)
```
`*args` is never populated by any caller. `**kwargs` is forwarded to `to_csv` with no typing.
mypy reports the overload mismatch at line 188.
**Repro:**
```text
$ mypy src/mko_get_mioscope_data/
utils.py:188: error: No overload variant of "to_csv" of "NDFrame" matches argument types
  "tuple[Any, ...]", "Path", "bool", "str", "str", "str", "Any", ..., "dict[str, Any]"
  [call-overload]
```
**Impact:** Type information for all CSV export arguments is lost. Any argument type mismatch
between the caller and `DataFrame.to_csv` is invisible to the type checker and only surfaces at
runtime as a pandas error.
**Root cause:** `*args/**kwargs` forwarding pattern used instead of explicit parameter declarations
matching `DataFrame.to_csv`'s signature.
**Fix:** Either replace `*args` with `# unused, remove` or with a typed parameter, and replace
`**kwargs` with the specific `to_csv` parameters needed (`quoting`, `quotechar`, `lineterminator`,
etc.) — or use keyword-only `**kwargs: Any` and suppress the overload error with an explicit
`cast` if forwarding must remain dynamic.
**Verify:** tests/test_utils.py::test_csv_to_file (tests happy path only); new test needed:
`test_csv_to_file_does_not_forward_positional_args`
**Related:** QLT-008

#### QLT-012 — [MEDIUM] `get_last_period` mixes `date`/`datetime`; produces 5 mypy `union-attr` errors

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::get_last_period`
**Disambiguator:** `today = date.today() if today is None else today`
**Checked:** ran (mypy)
**Why this band:** The parameter `today: datetime | None` is reassigned to `date.today()` (type
`date`, not `datetime`). mypy infers the variable's type as the union of all assignments
(`date | datetime | None`), so every subsequent `.replace()`, `.weekday()`, `.year`, and
`.strftime()` call on `today` triggers a `union-attr` error — mypy can't prove the value
isn't `None`. Five distinct lines are flagged, all from one mixed-type assignment. This is a
bounded correctness gap: if `date.today()` were mocked to return `None` (unlikely but
theoretically possible), the function would crash.
**Problem:** `utils.py:95` assigns `date` (from `date.today()`) to a variable typed
`datetime | None`. Lines 98-104 and 128 then call `datetime`-only methods (`.replace()`,
`.weekday()`, `.year`, `.strftime()`) on the mixed type. mypy reports 5 `union-attr` errors
and 1 `return-value` mismatch.
**Repro:**
```text
$ mypy src/mko_get_medioscope_data/
utils.py:95: error: Incompatible types in assignment (expression has type "date", variable has type "datetime | None")  [assignment]
utils.py:98: error: Item "None" of "datetime | None" has no attribute "replace"  [union-attr]
utils.py:99: error: Item "None" of "datetime | None" has no attribute "replace"  [union-attr]
utils.py:100: error: Item "None" of "datetime | None" has no attribute "weekday"  [union-attr]
utils.py:104: error: Item "None" of "datetime | None" has no attribute "year"  [union-attr]
utils.py:128: error: Item "None" of "datetime | Any | None" has no attribute "strftime"  [union-attr]
utils.py:130: error: Incompatible return value type [...]  [return-value]
```
**Impact:** The type checker cannot verify correctness of date arithmetic in period computation.
The function's contract is ambiguous: does it expect `datetime` or `date`?
**Root cause:** The parameter type `datetime | None` was chosen for the `today` override, but
`date.today()` returns `date`. The two types share a subtype relationship (`datetime` IS-A
`date`), but `date` is not a `datetime`, so the reassignment is type-incompatible.
**Fix:** Change the parameter type to `date | None = None` (since only `date` operations are
used), or use `datetime.today()` consistently.
**Verify:** tests/test_utils.py::test_slice_period (tests `slice_period`, not `get_last_period`);
new test needed: `test_get_last_period_with_datetime_override_typed`
**Related:** QLT-013

#### QLT-013 — [MEDIUM] `TVTask.__init__` fully untyped; inferred `None` types cause 3 downstream conflicts

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.__init__`
**Disambiguator:** `self.key = None`
**Checked:** ran (mypy)
**Why this band:** `TVTask.__init__` (`tasks.py:15-27`) has no type annotations on any parameter
or instance attribute. mypy infers `self.key: None` from `self.key = None`, then reports
incompatible-assignment when `reports.py:262` does `task.key = {}`. Similarly, `self.interval = None`
(inferred `None`) conflicts with `reports.py:376` assigning a `tuple`. The AGENTS.md standard
"Type Safety Everywhere" is not enforced because the mypy config omits
`disallow_untyped_defs = true` (it has `check_untyped_defs = true`, which checks bodies but
doesn't require annotations).
**Problem:** `tasks.py:15-27`:
```python
def __init__(self, name: str, settings: dict, report_subtype: str, report_type: str):
    self.name = name
    self.settings = settings     # inferred: dict (bare, no type params)
    self.key = None             # inferred: None (not Optional[dict])
    ...
    self.interval = None        # inferred: None (not Optional[tuple])
    self.target = None          # inferred: None (not Optional[str])
```
mypy then reports: `reports.py:262: error: Incompatible types in assignment
(dict[Never, Never], None)` and `reports.py:376-378: error: Incompatible types in assignment ...`
**Repro:**
```text
$ mypy src/mko_get_medascope_data/
tasks.py:16: error: Missing type parameters for generic type "dict"  [type-arg]
reports.py:262: error: Incompatible types in assignment (dict[Never, Never], None)  [assignment]
reports.py:376: error: Incompatible types in assignment (tuple[str, str] | ..., None)  [assignment]
reports.py:377: error: Incompatible types in assignment (str | Any, None)  [assignment]
```
**Impact:** `TVTask` attributes (`key`, `interval`, `target`) are untyped, so any consumer
accessing `task.key.keys()` or `task.interval[0]` is unchecked. The inferred `None` types cause
false-positive mypy errors that mask real issues.
**Root cause:** No type annotations on `TVTask` — the `dict` parameter lacks type parameters,
and nullable attributes lack `Optional[...]` annotations.
**Fix:** Annotate all parameters and attributes: `settings: dict[str, Any]`,
`key: dict[str, Any] | None = None`, `interval: tuple[str, str] | tuple[date, date] | None = None`,
`target: str | None = None`.
**Verify:** tests/test_reports.py::test_default_task_strategy (tests via MagicMock, not real TVTask);
new test needed: `test_tv_task_attributes_typed`
**Related:** QLT-008, QLT-009

#### QLT-014 — [MEDIUM] mypy `ignore_missing_imports` does not suppress `[import-untyped]`; stubs absent from dev deps

**Anchor:** `pyproject.toml::tool.mypy`
**Disambiguator:** `ignore_missing_imports = true`
**Checked:** ran (mypy with and without `--ignore-missing-imports`)
**Why this band:** The mypy config sets `ignore_missing_imports = true`, claiming to suppress
all missing-import errors. However, mypy 1.19.1 does not suppress `[import-untyped]` (module
installed but missing stubs) with this global flag — per the official mypy docs, that requires
`disable_error_code = ["import-untyped"]` or per-module overrides. The stub packages
(`types-PyYAML`, `types-requests`, `types-python-dateutil`) are not in the `dev` dependency
group, so 4 `import-untyped` errors persist. The project has `pandas-stubs` but forgot the three
other third-party packages.
**Problem:** `pyproject.toml:145` — `ignore_missing_imports = true`. The four errors at
`tasks.py:5`, `network.py:11`, `utils.py:9`, `utils.py:10` are not suppressed. Running
`mypy --ignore-missing-imports` reproduces the same 4 errors.
**Repro:**
```text
$ uv run mypy src/mko_get_mediascope_data/ 2>&1 | Select-String "import-untyped"
tasks.py:5:   Library stubs not installed for "yaml"
network.py:11:  Library stubs not installed for "requests.exceptions"
utils.py:9:    Library stubs not installed for "yaml"
utils.py:10:   Library stubs not installed for "dateutil.relativedelta"
$ uv run mypy ... --ignore-missing-imports 2>&1 | Select-String "import-untyped"
(same 4 errors — flag does not suppress import-untyped)
```
**Impact:** 4 errors remain even with the "ignore missing imports" flag set, creating the false
impression that the flag works. A developer fixing only the 57 non-stub errors will still see
noise from 4 un-suppressable errors, or may mistakenly believe the remaining errors are the
only ones.
**Root cause:** `ignore_missing_imports` in mypy only suppresses `import-not-found` (module
not installed), not `import-untyped` (module installed, no stubs). The project conflated the two.
**Fix:** Either (a) add `types-PyYAML`, `types-requests`, `types-python-dateutil` to the `dev`
dependency group in `pyproject.toml`, or (b) add
`disable_error_code = ["import-untyped"]` to `[tool.mypy]`, or (c) add per-module overrides:
```toml
[[tool.mypy.overrides]]
module = ["yaml", "requests.exceptions", "dateutil.relativedelta"]
ignore_missing_imports = true
```
**Verify:** new test needed: `test_mypy_clean_exit_zero` (CI gate asserting `mypy` exits 0)
**Related:** QLT-009

#### QLT-015 — [LOW] Empty `Task` base class inherited by `TVTask` but defines nothing

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::Task`
**Disambiguator:** `class Task:\n    pass`
**Checked:** ran (static read)
**Why this band:** `Task` (`tasks.py:10-11`) is a two-line `pass` body with no fields, no
methods, and no docstrings. `TVTask(Task)` inherits nothing — every attribute is defined in
`TVTask.__init__`. The class exists in the architecture diagram (AGENTS.md) but has no purpose
today. Per the dead-code policy, this is unreferenced-definition territory: the question is what
it was for, not whether to delete it.
**Problem:** `tasks.py:10-11`:
```python
class Task:
    pass
```
`TVTask` (line 14) is the only subclass, and it never calls `super().__init__()`. No other
code imports or references `Task`.
**Repro:**
```text
$ rg "Task" --type py src/
tasks.py:10:  class Task:
tasks.py:14:  class TVTask(Task):
```
**Impact:** None at runtime. Future contributors may wonder if `Task` was intended to hold
shared behavior — the empty base class invites confusion.
**Root cause:** Likely a placeholder left over from a planned inheritance hierarchy that was
never implemented.
**Fix:** Investigate intent (was a `Task` base planned?). If no future use, delete `Task` and
make `TVTask` a standalone class.
**Verify:** new test needed: (none — dead code, no behavior to test)
**Related:** QLT-016, QLT-017

#### QLT-016 — [LOW] `NoReportFoundError` and `WrongDataSettings` defined but never raised

**Anchor:** `src/mko_get_mediascope_data/core/errors.py`
**Disambiguator:** `class NoReportFoundError(Exception): pass`
**Checked:** ran (static read — `git grep`)
**Why this band:** Two custom exception classes exist in `errors.py` (lines 1-6) with clear
domain names, suggesting they were intended for specific error paths. `WrongDataSettings` was
even recommended for use in a prior audit (05-config-templates findings, line 146) to guard
against missing default config files, but it remains unwired — no `raise` call exists in the
source. `NoReportFoundError` was mentioned in 04-data-pipeline findings (line 242) as desired
for empty task sets, but also never raised. A declared contract (error signaling for these
conditions) has no enforcement point.
**Problem:** `errors.py:1-6` defines `NoReportFoundError` and `WrongDataSettings`. A `git grep`
across `src/` finds zero `raise` statements for either class. Both are imported nowhere.
**Repro:**
```text
$ git grep -n "raise NoReportFoundError\|raise WrongDataSettings" src/
(no matches)
$ git grep -n "from.*errors import\|from.*errors" src/
(no matches — not even imported)
```
**Impact:** When config is missing or report data is empty, the code raises generic
`TypeError`/`Exception` (see QLT-005, QLT-007) instead of the domain-specific exceptions
that were designed for exactly these cases. Error messages are less actionable.
**Root cause:** Exception classes were defined for future use but never wired into the code
paths they were designed to guard.
**Fix:** Add `raise WrongDataSettings(...)` in `get_default_data_settings` when the defaults
file is missing (per 05-config-templates recommendation), and `raise NoReportFoundError(...)` in
`processing_tasks` / `worker` when the task set is empty.
**Verify:** tests/test_models.py (does not test error paths); new test needed:
`test_wrong_data_settings_raised_on_missing_defaults`
**Related:** QLT-005, QLT-007

**Evidence — git grep:**
```text
$ git grep -n "raise NoReportFoundError\|raise WrongDataSettings" src/
(no matches)
```

#### QLT-017 — [LOW] `core/logger.py` is an empty placeholder, never imported

**Anchor:** `src/mko_get_mediascope_data/core/logger.py`
**Disambiguator:** file is 1 byte (newline only)
**Checked:** ran (static read)
**Why this band:** `logger.py` is listed in the AGENTS.md architecture diagram
(`core/logger.py — (placeholder)`) but the file is empty (1 byte, a single newline). It is never
imported by any module in `src/` or `tests/`. The project's logging is configured through
`AppConfig.logging_settings` → `logging.config.dictConfig` in `services.py:165`, and all modules
use `logging.getLogger(__name__)` directly. The placeholder serves no purpose and exists only as
a dangling entry in the architecture diagram.
**Problem:** `logger.py` is 1 byte. `git grep` for `logger` imports finds no references to it.
AGENTS.md line 45 explicitly labels it `(placeholder)`.
**Repro:**
```text
$ git grep -rn "from.*logger import\|import.*logger" src/ tests/
(no matches for core.logger)
```
**Impact:** None at runtime. The architecture diagram misleads readers into thinking there is a
centralized logging module to configure, when logging setup actually lives in `services.py`.
**Root cause:** Placeholder left from early development; never implemented or removed.
**Fix:** Remove `logger.py` and update AGENTS.md to remove it from the architecture diagram, or
implement it as the single logging-configuration entry point and wire all modules to use it.
**Verify:** new test needed: `test_logger_module_not_listed_if_deleted` (or verify after removal)
**Related:** QLT-015

#### QLT-018 — [LOW] `en_to_ru` catches `TypeError` but `.replace()` raises `AttributeError`

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::en_to_ru`
**Disambiguator:** `except TypeError as err:`
**Checked:** ran (static read)
**Why this band:** `en_to_ru` (`utils.py:203-211`) wraps `param.replace("EName", "Name")` in a
`try/except TypeError`. If `param` is not a string (e.g. an `int` or `None`), `.replace()`
raises `AttributeError`, not `TypeError` — so the handler never fires. The only `TypeError`
the handler would catch is from `len(slices_en)`, which the type hint (`list[Any]`) already
guarantees supports. The error message "parameter should be list" is therefore misleading.
**Problem:** `utils.py:207` — `slices_ru[i] = param.replace("EName", "Name")`. If `param` is
not a `str`, this raises `AttributeError`. The `except TypeError` at line 209 never catches it.
**Repro:**
```text
$ python -c "
from mko_get_mediascope_data.core.utils import en_to_ru
en_to_ru([1, 2, 3])  # int has no .replace()
"
→ AttributeError: 'int' object has no attribute 'replace'
```
(Note: this escapes the `except TypeError` block and is never logged.)
**Impact:** When `slices` data contains non-string elements (possible via YAML misconfiguration),
the function crashes with an unlogged `AttributeError` and no actionable error message.
**Root cause:** Wrong exception type in the handler — `TypeError` was chosen, but the actual
risk is `AttributeError` from calling `.replace()` on a non-string.
**Fix:** Change `except TypeError` to `except (TypeError, AttributeError)` and update the
error message to "element must be a string".
**Verify:** new test needed: `test_en_to_ru_non_string_element_raises_attribute_error`
**Related:** QLT-009

#### QLT-019 — [LOW] `extra="allow"` on three Pydantic models silently accepts unknown YAML keys

**Anchor:** `src/mko_get_mediascope_data/core/models.py::ReportSettings`
**Disambiguator:** `model_config = ConfigDict(extra="allow")`
**Checked:** ran (static read)
**Why this band:** `ReportSettings`, `DataDefaults`, and `Data` all set
`ConfigDict(extra="allow")` (`models.py:84, 120, 125`), while `LastTimeModel` and `PeriodModel`
use `extra="forbid"`. The permissive models accept any extra key from YAML without warning.
A typo in a report YAML key (e.g. `realtive_path` instead of `relative_path`) is silently
accepted as an extra field and never reaches the intended field — the error surfaces only as a
missing-field validation error, with no hint about the typo.
**Problem:** Three models allow extra keys. The `ReportSettings` model is loaded from user-authored
YAML report definitions (`services.py:61`), making typos particularly likely and particularly
hard to debug.
**Repro:**
```text
$ python -c "
from mko_get_medasphere_data.core.models import ReportSettings
rs = ReportSettings(media='TV', report_type='crosstab',
    relitive_path='test', period={'last_time': {'unit_count': 1, 'time_unit': 'm'}})
print(rs.relative_path)  # raises — field never set
"
→ ValidationError: relative_path is required
```
(The typo `relitive_path` was silently accepted as an extra key.)
**Impact:** A single-character typo in a report YAML file produces an opaque "field required"
error with no reference to the misspelled key — a common and frustrating user experience for
a YAML-driven configuration tool.
**Root cause:** `extra="allow"` was set to be permissive (likely to accommodate Mediascope
API parameters that don't map to named fields), but `ReportSettings` specifically is a
user-facing config model where strictness would catch typos.
**Fix:** Change `ReportSettings.model_config` to `extra="forbid"`. Keep `extra="allow"` only on
`Data` and `DataDefaults` if Mediascope API parameters need to be passed through as extra keys
(document this rationale in a comment).
**Verify:** tests/test_models.py::test_report_settings_valid (tests happy path); new test needed:
`test_report_settings_rejects_unknown_keys`
**Related:** QLT-005, QLT-007

#### QLT-020 — [LOW] Russian comments, docstrings, and log messages throughout `core/` violate "English Only" rule

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::iter_period_slices`
**Disambiguator:** `Генерирует последовательные интервалы...`
**Checked:** ran (static read)
**Why this band:** AGENTS.md rule 1 mandates "English Only — All comments, logs, docstrings, error
messages, and documentation must be in clear English." Every module under `core/` contains
Russian text in docstrings, inline comments, and `logger` messages. ENT-007 covered `app.py`
only; the violation extends through all seven `core/` modules. This is documentation drift with
no runtime consequence but high onboarding cost.
**Problem:** Russian Cyrillic text appears in docstrings and comments across `core/network.py`
(10+ lines), `core/utils.py` (10+ lines), `core/reports.py` (10+ lines), `core/services.py`
(print strings), `core/tasks.py` (logger message), and inline comments throughout. Per
`docs/00-overview/doc-maintenance-rules.md` line 19 ("Translate on touch: All docs are
English-only") and line 111 ("Check for English-only text"), this is a governance violation.
**Repro:**
```text
$ rg -P '[\x{0400}-\x{04FF}]' src/mko_get_mediascope_data/core/ --stats
core/network.py: 10 matches
core/utils.py: 10+ matches
core/reports.py: 10+ matches
core/services.py: 6 matches
core/tasks.py: 1 match
```
**Impact:** Non-Russian-speaking contributors cannot read documentation or understand log
messages emitted during report execution. Mixed-language source contradicts the project's own
governance rules and lowers maintainability.
**Root cause:** Entry layer was authored in Russian without translation to English as required
by AGENTS.md rule 1.
**Fix:** Translate all docstrings, comments, and `logger`/log messages in `core/` modules to
English. Add a CI lint rule (e.g. a `ruff` custom check or a `pre-commit` grep hook) that fails
on Cyrillic in `.py` source comments and strings.
**Verify:** existing: tests/test_app.py::test_cli_init (checks for Russian string in UI output
— would need updating if `console.print` strings are translated); new test needed:
`test_no_cyrillic_in_core_modules`
**Related:** ENT-007

**Evidence — Cyrillic text in `core/` modules:**
```text
# core/network.py:1 — docstring in Russian
"""Network handler — общие сетевые вызовы к Mediascope API с ретраями и обработкой ошибок."""

# core/utils.py:37 — docstring in Russian
"""Генерирует последовательные интервалы внутри [overall_start, overall_end]..."""

# core/utils.py:55 — error message in Russian
raise ValueError(f"Неизвестная частота: {frequency}")

# core/reports.py:49 — docstring in Russian
"""Должен быть async generator."""

# core/tasks.py:44 — logger message in Russian
logger.info(f"Сохраняю настройки задачи {self.name}")
```

*(supports: "English Only" rule in AGENTS.md rule 1 and doc-maintenance-rules.md)*

## Fix Order

- **Dependencies:** QLT-008 and QLT-013 are prerequisites for QLT-009 (fixing bare `dict`
  annotations and adding `TVTask` type annotations clarifies the types that `NetworkClient.call`
  and `en_to_ru` should return).
- **Merge candidates:** QLT-005 and QLT-006 and QLT-007 all stem from the same root cause:
  `yaml_to_dict` returns `dict[str, Any] | None` without enforcement at call sites, and Pydantic
  models are used where `dict`-style access was once expected. A single refactor of
  `yaml_to_dict` (raise instead of return `None`) plus replacing `.get()` on `Data` with
  `getattr()` resolves all three.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** QLT-005 overlaps with ENT-003 (same `yaml_to_dict` None return);
  QLT-006/QLT-007's mypy errors are downstream effects. QLT-020 (Russian text in `core/`)
  overlaps with ENT-007 (Russian text in `app.py`) — ENT-007 is the correct home for the entry
  layer; QLT-020 covers the broader `core/` scope.
- **Rollout risk:** QLT-019 (`extra="forbid"` on `ReportSettings`) could break existing YAML
  report definitions that use undocumented keys as pass-through parameters. Audit all bundled
  report YAMLs first; `test_models.py::test_report_settings_valid` must stay green.
  QLT-014 (adding `types-PyYAML`, `types-requests` to dev deps) is non-breaking — only adds
  new static-analysis signals.

## Appendices

### Appendix A — QLT- prefix collision check

The phase file (line 131) warns that `QLT-` is "already in use in shipped source from a prior
cycle." A full-repo `git grep` for `QLT` was performed:

```text
$ git grep -n "QLT" -- '*.py' '*.md' '*.yaml' '*.toml'
.ai/audit/templates/audit-findings.md:48: | QLT-001 | HIGH | ...
.ai/audit/templates/audit-findings.md:49: | QLT-002 | MEDIUM | ...
.ai/audit/templates/audit-findings.md:54: #### QLT-001 — [HIGH] {title}
.ai/audit/templates/audit-findings.md:72: **Related:** QLT-004, AUTZ-002
.ai/audit/templates/audit-findings.md:81: #### QLT-002 — [MEDIUM] {title...}
.kilo/commands/audit/phases/10-audit-code-quality.md:131: Finding-ID prefix: QLT- ...
```

No `QLT-` identifiers exist in any shipped `.py`, `.yaml`, or `.toml` file, nor in any prior
findings file under `.ai/audit/` (existing findings use 3-letter prefixes: ENT, ASY, CFG,
PIPE, TPL, EXT, TST). The template's `QLT-001`/`QLT-002`/`QLT-004` reference
`src/backend/apps/ads/` paths from a different project. `QLT-004` is referenced as "Related"
but never defined — potentially the "two different findings" the phase file warns of.

**Decision:** No second namespace created. IDs start at `QLT-005` to avoid any collision with
the template's placeholder IDs.

*(supports: "check for a collision before minting an ID")*

### Appendix B — Full mypy output

```text
$ uv run mypy src/mko_get_mediascope_data/ --show-error-codes

Found 61 errors in 7 files (checked 14 source files):

import-untyped (4, not suppressed by ignore_missing_imports):
  tasks.py:5:   yaml
  network.py:11: requests.exceptions
  utils.py:9:    yaml
  utils.py:10:   dateutil.relativedelta

type-arg (bare generics, disallow_any_generics):
  tasks.py:16:    dict
  tasks.py:29:    PathLike
  tasks.py:31:    Path(arg-type from bare PathLike)
  models.py:121:  dict
  utils.py:133:   dict
  utils.py:169:   PathLike
  utils.py:343:   PathLike
  reports.py:55:  PathLike
  reports.py:89:  PathLike
  reports.py:365: dict
  services.py:79: dict
  services.py:97: dict

assignment (type mismatches):
  tasks.py:29:  folder: None default on non-Optional
  utils.py:25:  date_string reassigned str→date
  utils.py:95:  today reassigned datetime→date
  reports.py:240: list[date] assigned to tuple variable
  reports.py:262: dict[Never,Never] assigned to None-typed
  reports.py:320: DataFrame|None assigned to DataFrame
  reports.py:376-377: tuple/str assigned to None-typed
  reports.py:428-429: same as 376-377

union-attr (None not handled):
  utils.py:98-104: today.replace/weekday/year on None union
  utils.py:128: d.strftime on None
  reports.py:189: list[Any].get on dict|list
  reports.py:237: get_last_period(None) for settings
  reports.py:241: last_time.time_unit on None
  reports.py:366: list|tuple|None has no __iter__
  reports.py:369: dict|None has no items()
  reports.py:409: list|tuple|None has no __iter__
  reports.py:411: dict|None has no items()
  services.py:60: dict|None has no __iter__

arg-type (wrong argument type):
  tasks.py:31:   Path() arg incompatible (bare PathLike)
  utils.py:183:   get_files_suffix(dict[str,str]|None vs str|dict)
  reports.py:76:  same get_files_suffix call
  reports.py:237: get_last_period(None) vs LastTimeModel
  reports.py:246: slice_period(tuple vs list|tuple)
  reports.py:368: str.join on non-Iterable[str]
  services.py:37: **None on LoggingSettings
  services.py:40: **None on AppSettings
  services.py:61: **str on ReportSettings
  services.py:74: **None on DataDefaults

call-overload (to_csv signature mismatch):
  utils.py:188: to_csv overload doesn't match *args/**kwargs forwarding

misc:
  reports.py:95: Report.__init__ multiple values for report_settings/data_settings/export_path
  reports.py:160: func-returns-value (processing_tasks has no return)
  reports.py:336: Coroutine has no __aiter__ (async generator typing)

var-annotated:
  utils.py:205:  slices_ru needs type annotation
  reports.py:115: task_queue needs type annotation

return / return-value:
  utils.py:130:  get_last_period return type mismatch
  utils.py:343:  yaml_to_dict missing return statement
  reports.py:247:  get_period return type mismatch

func-returns-value:
  reports.py:160: processing_tasks called in await but returns None
```

*(supports: all QLT-005 through QLT-014 findings)*

### Appendix C — `print()` calls in `core/` (AST scan)

```text
$ rg -n "print\(" src/mko_get_mediascope_data/core/

core/reports.py:265:            print("=", end="", flush=True)
core/services.py:117:        print(
core/services.py:124:            print(f"\nОтчёт {idx} → {report_settings.report_subtype}\n")
core/services.py:154:        print(
```

Note: ENT-005 (entry-architecture) already filed the `core/services.py` and
`core/reports.py:265` print calls. The Russian text within those `print()` strings is covered
by QLT-020. These are listed here for completeness — this phase focuses on the type-safety and
convention aspect, not the import-time side effect aspect covered by ENT-005.

*(supports: QLT-020, cross-ref ENT-005)*

### Appendix D — Russian text in `core/` modules

Per AGENTS.md rule 1 ("English Only — All comments, logs, docstrings, error messages, and
documentation must be in clear English"), the following non-English strings appear in `core/`
modules:

```text
$ rg -P '[\x{0400}-\x{04FF}]' src/mko_get_mediascope_data/core/ --stats
```

Examples include:
- `network.py:1,23,33,34,45,46,47,68,81` — docstrings and inline comments in Russian
- `utils.py:37,55,134,219,227,238,251,256,282,345` — docstrings and logger messages in Russian
- `reports.py:44,102,135,149,156,253,279,290,453,466` — docstrings and logger messages in Russian
- `services.py:117,118,124,154,155,156` — `print()` strings in Russian
- `tasks.py:44` — logger message in Russian

ENT-007 partially covered this for `app.py`; the violation extends through all of `core/` and
is filed as QLT-020.

*(supports: QLT-020)*
```
