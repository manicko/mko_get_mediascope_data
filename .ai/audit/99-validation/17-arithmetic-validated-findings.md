---
phase: "17"
phase_name: "arithmetic"
id_prefix: "NUM-"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Arithmetic & Derived-Quantity Verification

## Summary

All four findings in `.ai/audit/17-arithmetic/findings.md` (code_ref `d093d9c`) are
**CONFIRMED** against live source inspection at HEAD `d093d9c`. Three runtime probes
reproduce the asserted behaviour verbatim; one finding (NUM-004) is static and confirmed
by code read. The `code_ref` matches `git rev-parse --short HEAD` with zero drift. The
findings' root-cause analysis, call-chain tracing, and severity grading are all supported
by source evidence. Three minor presentation inaccuracies — all LOW-severity and none
affecting a verdict — are recorded as VAL-001 through VAL-003.

**4 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 merged, 0 unsettled.**

## Scope

**Examined:** `core/utils.py` (`pivot_df_frequency`, `csv_to_file`, date arithmetics),
`core/reports.py` (`DefaultDataStrategy.prepare`, `TVMediaReport.extract_data`, worker),
`core/network.py` (`NetworkClient.call`), `core/models.py` (`Data`, `FrequencyDistPer`
extras), `app.py` (`run` command, success message), `settings/defaults/tv/crosstab.yaml`
and `settings/reports/last_category/nat_tv_brands_weekly_reach.yaml` (interval conditions),
`tests/test_utils.py` and `tests/test_reports.py` (coverage gaps), `mediascope_api`
`result2table`/`merge_rows` source (None→NaN contract), sibling reports Phase 05, 10, 16,
and the Phase 04/16 PIPE-001 cross-references.

**Tools:** `uv run python` runtime probes (pivot crash, NaN/None→empty, float artifacts,
`to_csv` defaults, `Data` model extra fields), `git rev-parse --short HEAD`,
`git ls-files` (cited-path tracking), `inspect.getsource` on `result2table`/`merge_rows`.

**Runtime probes performed (reproduced from the findings):**
- NUM-001 CASE B — range intervals `[0-1]…[8-9]` → `ValueError: invalid literal for int() with base 10: '0-'`; returned shape (18,4), columns include `frequencyDistInterval` (long format, pivot skipped)
- NUM-001 CASE A — open-ended `[1+]…[20+]` → pivots correctly, shape (3,11), no `frequencyDistInterval` column
- NUM-002/003 — `csv_to_file` on df with `NaN` and `None` → `Tula;;` (both empty); `0,30000000000000004` and `0,3333333333333333` (raw float artifacts)
- `Data` model extra-field verification — `frequency_dist_conditions` stored as `__pydantic_extra__`, `getattr` returns the dict

**Not verified:** live Mediascope API calls (no credentials); the API-side `frequencyDistInterval` rendering contract for range conditions (inferred from `mediascope-jupyter` examples cited in the findings). The `result2table` source confirms `None→NaN` via `df.replace(to_replace=[None], value=np.nan)`.

### Template Compliance (GATES)

| Gate | Status | Detail |
|---|---|---|
| 1. Every cited path in `git ls-files` | PASS | `core/utils.py`, `core/reports.py`, `core/network.py`, `app.py`, settings YAMLs, `test_utils.py`, `test_reports.py` all tracked |
| 2. Symbol + disambiguator token exists in primary anchor | PASS | `pivot_df_frequency`, `csv_to_file`, `DefaultDataStrategy.prepare`, `extract_data` all exist; `int(x[1:-2])`, `decimal=","`/`sep=";"`, `aggfunc="first"` all present |
| 3. No `:<digits>` in any locator (Anchor) field | PASS | All anchors use `path::Symbol` format; line numbers appear only inside evidence fences |
| 4. Every "ran" claim has output pasted | PASS | NUM-001/002/003 carry runtime probes; NUM-004 is `static` (no runtime claim) |

## Findings

| ID | Disposition | Severity | Anchor resolves | Disambiguator exists |
|---|---|---|---|---|
| NUM-001 | confirmed | CRITICAL | utils.py `pivot_df_frequency`; reports.py `DefaultDataStrategy.prepare`/`extract_data`; app.py `run` | utils.py:241 `int(x[1:-2])` ✓ |
| NUM-002 | confirmed | HIGH | utils.py `csv_to_file`; mediascope_api `result2table` (None→NaN) | utils.py:193-194 `decimal=","`/`sep=";"` ✓ |
| NUM-003 | confirmed | LOW | utils.py `csv_to_file` | utils.py:188 `to_csv(...)` ✓ |
| NUM-004 | confirmed | LOW | utils.py `pivot_df_frequency` | utils.py:234/225 `aggfunc="first"`/`.first()` ✓ |

_Tally: 4 confirmed — 1 CRITICAL, 1 HIGH, 2 LOW — matching the input summary._

---

### NUM-001 — confirmed — [CRITICAL] Range-format intervals crash the sort and pivot is silently skipped

**Anchor resolved:** `core/utils.py::pivot_df_frequency` (starts line 214). The `try`
block is at line 218; the `sorted(... key=lambda x: int(x[1:-2]) ...)` is at lines
239–244; the `except Exception` handler is at lines 250–252. All match the finding.

**Claim tested on its own terms:** For `frequencyDistInterval` values `"[0-1]"`,
`"[2-3]"`, etc., `x[1:-2]` yields `"0-"`, `"2-1"` — neither is a valid `int`, so
`ValueError` is raised. The `except Exception` at line 250 catches it, logs in Russian,
and `return df` (line 252) yields the original long-format frame.

**Asserted cause tested separately:** The finding attributes the silent-success flow to
(a) the broad `except Exception`, (b) `DefaultDataStrategy.prepare` catching only
`KeyError`. Both are verified:
- `pivot_df_frequency` except clause: `except Exception as err:` → `logger.exception(...)`
  → `return df` (utils.py:250-252). ✓
- `DefaultDataStrategy.prepare` except clause: `except KeyError as err:` → `return None`
  (reports.py:452-454). `ValueError` never reaches this handler because
  `pivot_df_frequency` swallows it internally. ✓

**Evidence reproduced:**
```text
Ошибка 'invalid literal for int() with base 10: '0-'' при обработке частот.
Traceback (most recent call last):
  File "...core/utils.py", line 240, in pivot_df_frequency
    sorted(
  File "...core/utils.py", line 241, in <lambda>
    freq_df.columns, key=lambda x: int(x[1:-2]) if x.startswith("[") else -1
ValueError: invalid literal for int() with base 10: '0-'
INPUT shape: (18, 4)
INPUT columns: ['regionName', 'frequencyDistInterval', 'FrequencyDistPer', 'RtgPerSum']
RESULT shape: (18, 4)
RESULT columns: ['regionName', 'frequencyDistInterval', 'FrequencyDistPer', 'RtgPerSum']
Has frequencyDistInterval col? True   # pivot NOT applied → long format returned
```
Open-ended `[N+]` format pivots correctly:
```text
RESULT shape: (3, 11)
RESULT columns: ['regionName', 'FrequencyDistPer', 'RtgPerSum', '[1+]', '[2+]', '[3+]', '[5+]', '[7+]', '[10+]', '[15+]', '[20+]']
Has frequencyDistInterval col? True   # pivot applied → wide format
```

**Silent-success flow verified (end-to-end from source):**
1. `extract_data` (reports.py:309-310) calls `result2table`; no `to_lists` param → `False` (default).
2. `prepare_data` → `DefaultDataStrategy.prepare` (reports.py:436-454): selects output columns, then
   `pivot_df_frequency` is called (reports.py:450). The broad `except Exception` inside
   `pivot_df_frequency` returns the long-format `df`; `prepare`'s `except KeyError` never fires.
3. `prepare` returns the non-`None` long-format df (reports.py:451).
4. `extract_data` (reports.py:320-332): `df is None` is `False`, so `csv_to_file` writes the
   wrong-shape CSV.
5. Worker (reports.py:152): `logger.info(f"Задача {item.name} гетова.")` — no failure surfaced.
6. `app.py:52`: `console.print("[green]✅ Отчёт успешно завершён[/green]")` — success reported.

**Severity grading checked against Phase 17 taxonomy:**
- Band assigned: CRITICAL ✓
- Rubric: "a derived quantity whose aggregation semantics silently produce a wrong value
  consumed as authoritative, so a total, rate, or pivot is incorrect without any error; null/NaN
  values silently dropped in a sum that a downstream reader treats as complete, producing a figure
  with no distinguishable failure state." — NUM-001 is "pivot is incorrect without any error;
  no distinguishable failure state." Exact match. ✓
- No downgrade: the mechanism (ValueError swallow) is correctly classified as CRITICAL, not
  HIGH or MEDIUM.

**Recommendation carry-out:** Target `pivot_df_frequency` interval sort + exception handling.
The symbol exists and is stable at the cited location. Narrowing the `except` and returning
`None` (so `DefaultDataStrategy.prepare` raises) is a sound, implementable fix. ✓

**Shipped test as remediation blocker:** No shipped test exercises `pivot_df_frequency` at all
(`tests/test_utils.py` covers `str_to_date`, `slice_period`, `csv_to_file` file-count only,
`get_files_suffix`; `tests/test_reports.py` covers task generation and `DictDataStrategy`).
The finding correctly records `new test needed: test_pivot_df_frequency_handles_range_intervals`. ✓

### NUM-002 — confirmed — [HIGH] NaN/None statistic values render as empty CSV cells

**Anchor resolved:** `core/utils.py::csv_to_file` (lines 167-200). The `to_csv` call with
`decimal=","`/`sep=";"` and no `na_rep` is at lines 188-198. ✓

**Claim tested on its own terms:** `csv_to_file` calls `data_frame.to_csv(..., decimal=",",
sep=";", ...)` with no `na_rep`. pandas 3.0.0 defaults `na_rep` to `""` (verified:
`inspect.signature(pd.DataFrame.to_csv)['na_rep'].default == ''`). `result2table` converts
`None` → `np.nan` via `df.replace(to_replace=[None], value=np.nan, inplace=True)`.
No null-validation exists in `DefaultDataStrategy.prepare` or `extract_data` before writing.

**Evidence reproduced:**
```text
=== CSV OUTPUT ===
regionName;FrequencyDistPer;RtgPerSum
Msk;0,30000000000000004;33,33333333333333
SPb;0,3333333333333333;66,66666666666667
Tula;;

HAS float_format: False
HAS na_rep: False
HAS round: False
```

**Severity grading checked against Phase 17 taxonomy:**
- Band assigned: HIGH ✓
- Rubric: "a NaN that propagates to the rendered CSV without being caught or reported." —
  NUM-002 is exactly this. ✓

**Recommendation carry-out:** Set explicit `na_rep` or surface a per-column null count via
logger. Target symbols `csv_to_file` and `DefaultDataStrategy.prepare` exist. ✓

**Shipped test as remediation blocker:** `test_csv_to_file` (test_utils.py:23-29) asserts only
`len(files) == 1`; no assertion on `na_rep`, null content, or sentinel. The finding correctly
records `new test needed: test_csv_to_file_na_rep_is_distinct_or_warned`. ✓

### NUM-003 — confirmed — [LOW] No float-formatting policy or declared tolerance

**Anchor resolved:** `core/utils.py::csv_to_file`. ✓

**Claim tested on its own terms:** `to_csv` is called with no `float_format` and no pre-export
`round()`. `grep` for `float_format|na_rep|\.round\(|tolerance|rounding` across `src/` and
`*.yaml` returns zero hits. pandas 3.0.0 `to_csv` defaults: `float_format=None`,
`na_rep=''`. `decimal=","` does not truncate — full IEEE-754 precision is preserved.

**Evidence reproduced:** `0.1+0.2` renders as `0,30000000000000004`; `1/3` renders as
`0,3333333333333333`. No constants, settings, or docstring declares a rounding/tolerance
contract for `RtgPerSum`, `CumReachPer`, `FrequencyDistPer`, or `ShareAvg`.

**Severity grading checked against Phase 17 taxonomy:**
- Band assigned: LOW ✓
- Rubric LOW: "rounding applied at the wrong stage but within tolerance; an aggregation
  function whose result has no downstream consumer." — NUM-003 is a precision-policy gap
  with no wrong-value consequence (values are numerically faithful). Fits LOW, not HIGH
  (no accumulation beyond a declared bound — no bound exists and no in-tool summation
  occurs). ✓

**Recommendation carry-out:** Add `float_format="%.4f"` to `to_csv` or round before export;
document the tolerance in a constant/setting. Target symbol `csv_to_file` exists. ✓

**Shipped test as remediation blocker:** No shipped test asserts float formatting or
precision. The finding correctly records `new test needed: test_csv_to_file_float_format_is_declared`. ✓

### NUM-004 — confirmed — [LOW] `first` aggregation has no determinism contract or dedup guard

**Anchor resolved:** `core/utils.py::pivot_df_frequency`. ✓

**Claim tested on its own terms (static):** No `sort_values`, `drop_duplicates`, or uniqueness
assertion exists between `result2table` output and the `first` aggregation. The `pivot_table`
uses `aggfunc="first"` (utils.py:234); the totals frame uses `.groupby(dim_cols,
as_index=False).first()` (utils.py:225). pandas `"first"` returns the first non-null value
within each group — dependent on row order. No comment, model field, or docstring declares
one row per `(dim_cols, frequencyDistInterval)`. `extract_data` calls `result2table` with
`to_lists=False` (confirmed: default is `False`), so `merge_rows` is not invoked in the
current path; `merge_rows` itself also uses `.groupby("adSpotId").agg("first")`, so the
ordering dependency is structurally the same.

**Severity grading checked against Phase 17 taxonomy:**
- Band assigned: LOW ✓
- Rubric LOW: latent robustness gap with "no downstream consumer" — correct, as no duplicate
  rows were observed in the single-task export path. ✓

**Recommendation carry-out:** Add a contract (docstring + defensive `groupby(...).size()` check
or deterministic `aggfunc`). Target symbol `pivot_df_frequency` exists. ✓

**Shipped test as remediation blocker:** No shipped test covers ordering-invariance or group
uniqueness. The finding correctly records `new test needed:
test_pivot_df_frequency_is_order_invariant`. ✓

---

## Validation-Level Findings (VAL-)

### VAL-001 — [LOW] Success-message location attribution is imprecise

The finding states: "the worker marks the task ready (reports.py:152), printing
`✅ Отчёт успешно завершён`."

- reports.py:152 is `logger.info(f"Задача {item.name} гетова.")` — it does **not** contain
  the string `✅ Отчёт успешно завершён`.
- The string `✅ Отчёт успешно завершён` lives in `app.py:52`, inside the `run` Typer command's
  `try/except` block — printed only when `app_service.run_report()` completes without
  raising.

**Effect on verdict:** The end-to-end claim (silent wrong-output + run reports success) is
correct. The success path is: `extract_data` writes without raising → worker logs "ready" →
`processing_tasks` completes → `create_report` returns → `run_report` returns → `app.py:52`
prints success. The attribution of the specific string to `reports.py:152` is wrong, but it
does not affect the finding's validity or the silent-success mechanism.

### VAL-002 — [LOW] Line-number references in prose are slightly imprecise

The finding cites `utils.py:186-198` for `csv_to_file` — the function body starts at line 167;
`186-198` covers the `try:` + `to_csv` call, which is approximately correct but not the full
function. It cites `reports.py:450-451` for `DefaultDataStrategy.prepare` — line 449 is the
`if` condition, 450 is the pivot call, 451 is `return df`. These are prose references, not
locator fields (the Anchor column uses `::Symbol` per template rules); the symbols and
disambiguator tokens all resolve. The template's gate 3 ("no `:<digits>` in any locator
field") is satisfied because line numbers appear only in evidence fences and prose, not in the
Anchor column.

### VAL-003 — [LOW] Cross-phase ownership attribution for Phase 05 is imprecise

The finding states Phase 05 owns "the CSV format contract." Phase 05 owns "template-to-model
field conformance" (per `05-audit-config-templates.md:28`), not CSV format. The CSV format
contract is owned by Phase 16 (OUT-004: "No declared or versioned CSV output-format contract,"
same anchor `csv_to_file`). The overlap NUM-003 ↔ OUT-004 is valid and correctly flagged.
The Phase 05 mention is an over-broad attribution but does not affect NUM-003's verdict. The
Phase 10/QLT-011 overlap (type-safety of `*args/**kwargs` forwarding) is a distinct angle and
correctly noted as complementary.

---

## Cross-Phase Analysis

| NUM finding | Sibling finding(s) | Shared subject | Disposition |
|---|---|---|---|
| NUM-001 | (none) | Frequency pivot crash + silent success | standalone |
| NUM-002 | (none) | NaN/None rendering | standalone |
| NUM-003 | OUT-004 (Phase 16, LOW) | `csv_to_file` format parameters (float_format, sep, decimal, encoding) | cross-reference; both confirmed; fix together by extracting format params into a declared constant |
| NUM-004 | (none) | `first` aggregation determinism | standalone |

**No cross-phase conflicts.** NUM-003 and OUT-004 share the `csv_to_file` anchor and the
"hardcoded format parameters, no declared contract" root cause; they operate at different
angles (17: arithmetic/precision policy; 16: output-contract discoverability) but their fixes
converge on the same refactor — extracting `sep`/`decimal`/`encoding`/`float_format` into a
named constant. The Phase 16 Fix Order already notes OUT-004's recommendation to extract format
params into a constant; NUM-003's `float_format` addition is a natural part of that same
constant. This is a cross-reference and merge-opportunity, not a conflict.

The Phase 10/QLT-011 finding (`*args/**kwargs` forwarding in `csv_to_file`) is at the same
anchor but a different concern (type-checking of forwarded kwargs). VAL-003 notes that Phase
05 does not own the CSV format contract — the 17-arithmetic finding's attribution was
imprecise.

## Rollout Safety Assessment

**Validated from source (`reports.py`, `utils.py`, `app.py`) and `tests/`:**

- **NUM-001** (highest priority): Fixing the interval sort to be range-aware and narrowing
  the `except` to return `None` so `pivot_df_frequency` signals failure to the caller.
  `DefaultDataStrategy.prepare` returns that `None` (its `except KeyError` is for missing
  columns, not this path), and `extract_data` (reports.py:321-322) sets `task.status = False`,
  producing the error YAML instead of a wrong-shape CSV. The failure surfaces at the
  `prepare_data` → `df is None` check, not at `csv_to_file` itself (which also returns `None`
  on skip per Phase 16 OUT-003).
  `test_csv_to_file` (asserts file count) stays green since it doesn't exercise the pivot.
  Sequencing: NUM-001 must precede any range-interval seller config.

- **NUM-002** (HIGH): Adding an explicit `na_rep` (e.g. `"NaN"` or `"#NA"`) changes how nulls
  render. Consumers that currently rely on empty cells for missing data would see the sentinel
  instead. The find-order note is correct: `test_csv_to_file` asserts only file count, so it
  stays green. Recommendation to add a per-column null count log is non-breaking.

- **NUM-003** (LOW): Adding `float_format="%.4f"` or pre-export `round()` changes the
  rendered precision of all float columns. A new test (`test_csv_to_file_float_format_is_declared`)
  is needed; existing `test_csv_to_file` would stay green (no float content assertion).

- **NUM-004** (LOW): Adding a determinism guard (e.g. `groupby(...).size()` assertion or
  explicit sort) is non-breaking for single-row groups. If duplicates exist, it would
  raise — which is the intended behaviour change (fail loudly instead of silently
  nondeterministic). No shipped test asserts current `first`-behaviour on duplicates.

**Backward compatibility:** NUM-001 and NUM-002 carry output-format changes that could affect
CSV consumers. The shipped config (`nat_tv_brands_weekly_reach.yaml`) uses only `{'from': N}`
(open-ended) intervals, so NUM-001 fixes would initially have no impact on the shipped report
— the defect is latent for the bundled config but active for any seller using range intervals.

## Evidence Index

| Evidence | Source |
|---|---|
| `git rev-parse --short HEAD` → `d093d9c` | matches code_ref (no drift) |
| `pivot_df_frequency` with range `[0-1]…[8-9]` → `ValueError`, long-format returned | reproduced (NUM-001) |
| `pivot_df_frequency` with `[1+]…[20+]` → wide pivot, shape (3,11) | reproduced (NUM-001) |
| `csv_to_file` output: `Tula;;` (NaN+None→empty), `0,30000000000000004` (float artifact) | reproduced (NUM-002/003) |
| `inspect.signature(to_csv)['na_rep'].default` → `''`; `['float_format'].default` → `None` | pandas 3.0.0 (NUM-002/003) |
| `csv_to_file` source: no `float_format`, no `na_rep`, no `round` in source | inspected (NUM-003) |
| `grep float_format\|na_rep\|\.round(\|tolerance\|rounding` in `src/` + `*.yaml` → 0 hits | confirms NUM-003 |
| `DefaultDataStrategy.prepare` except clause: `except KeyError` only (reports.py:452) | source (NUM-001) |
| `extract_data` calls `result2table` without `to_lists` (reports.py:309-310) | source (NUM-004) |
| `result2table` source: `df.replace(to_replace=[None], value=np.nan)` | mediascope_api installed pkg (NUM-002) |
| `to_lists` default = `False` | inspect.signature (NUM-004) |
| `app.py:52`: `console.print("[green]✅ Отчёт успешно завершён[/green]")` | source (VAL-001) |
| `reports.py:152`: `logger.info(f"Задача {item.name} гетова.")` | source (VAL-001) |
| `Data` model: `frequency_dist_conditions` in `__pydantic_extra__`, `getattr` returns dict | reproduced (NUM-001 context) |
| Shipped config `nat_tv_brands_weekly_reach.yaml`: intervals all `{'from': N}` (no `'to'`) | settings file (NUM-001) |
| No test exercises `pivot_df_frequency`; `test_csv_to_file` asserts only file count | tests/test_utils.py, tests/test_reports.py (all findings) |
| Phase 16 OUT-004: "No declared or versioned CSV output-format contract" at `csv_to_file` | .ai/audit/16-rendered-surface/findings.md (NUM-003 cross-ref) |
| Phase 17 severity taxonomy | .kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy |
| Phase 10 QLT-011: `*args/**kwargs` forwarding in `csv_to_file` | .ai/audit/10-code-quality/findings.md (VAL-003 context) |

## Disposition Tally

| Disposition | Count |
|---|---|
| confirmed | 4 |
| re-typed | 0 |
| re-graded | 0 |
| merged | 0 |
| not substantiated | 0 |
| unsettled | 0 |
| **Total** | **4** |

**Validation-level findings (VAL-): 3** (VAL-001, VAL-002, VAL-003 — all LOW)

**Namespace ruling (block 7):** prefix `NUM-` is declared by phase 17
(`17-audit-arithmetic.md:119`) and used only in `.ai/audit/17-arithmetic/findings.md`. No
`NUM-` identifier exists in any shipped `.py`/`.yaml`/`.toml` (verified: grep across `src/`
for `NUM-` returns zero hits). The template's placeholder IDs (QLT-001 etc.) are from a
different project and do not collide with `NUM-` (3-letter prefixes are distinct: `NUM` vs
`QLT`). `VAL-` is the validation namespace; used only in this file's dedicated section.
Ruling: namespace clean; no collision.

**Report template as controlled artefact (block 8):** the Phase 17 findings file follows
`.ai/audit/templates/audit-findings.md` front-matter (`phase`, `phase_name`, `id_prefix`,
`mode`, `report_status`, `severity_taxonomy`, `code_ref`). `report_status` is `"draft"` in
the input — this validation sets it to `"validated"`. All mandated per-finding fields present:
Anchor, Disambiguator, Checked, Why this band, Problem, Repro, Impact, Root cause, Fix,
Verify, Related, Evidence. The template's prefix enumeration covers phases 01-17 (odd
selectors); `17-arithmetic` is consistent with the `NUM-` prefix declared in the phase file.
