---
phase: "17"
phase_name: "arithmetic"
id_prefix: "NUM"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Arithmetic & Derived-Quantity Verification

## Summary

Three findings across four audit blocks. One CRITICAL defect: the frequency-distribution
pivot silently swallows a `ValueError` and writes a wrong-shaped (long-format) CSV instead of
the pivoted wide table whenever the Mediascope API emits range intervals `[N-M]` — a format the
API documents and supports. One HIGH defect: `NaN`/`None` statistic values render as empty CSV
cells with no sentinel or warning, so a missing derived value is indistinguishable from
absent data. One LOW gap: no `float_format`/rounding policy and no declared tolerance for any
derived quantity, so raw float artifacts (e.g. `0.30000000000000004`) leak into the locale CSV.
No multi-implementation divergence was found (derived quantities are computed server-side; the
tool only pivots them). Date-arithmetic helpers were verified correct and produced no findings.

## Scope

**Examined:** `core/utils.py::pivot_df_frequency` (frequency pivot + interval sort + `first`
aggregation), `core/utils.py::csv_to_file` (CSV rendering: `decimal=","`, `sep=";"`, no
`float_format`/`na_rep`), `core/utils.py::iter_period_slices`/`slice_period`/`get_last_period`
(date interval arithmetic), `core/reports.py::DefaultDataStrategy.prepare` (pivot invocation and
failure propagation), `core/network.py::NetworkClient.call` (retry/failure semantics), the
`result2table` contract in `mediascope_api` (None→NaN, row ordering), `settings/defaults/tv/...`
and `settings/reports/.../*frequency*` (interval condition declarations), `tests/test_utils.py`
& `tests/test_reports.py` (arithmetic test coverage).

**Tools:** `uv run python` runtime probes (pivot crash on range intervals, NaN→empty rendering,
float-artifact rendering, `csv_to_file` format params), `rg` content search, static read of the
data-preparation and pivot code, `git ls-files` to confirm cited paths are tracked.

**Not verified:** Live Mediascope API calls (no credentials); the exact `frequencyDistInterval`
rendering contract (inferred from the official `mediascope-jupyter` examples and the project's own
`result2table` usage); multi-implementation agreement (no second implementation exists in-tool, so
no divergence is possible — derived quantities are computed server-side).

<!-- FORBIDDEN items omitted per template; gates below. -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| NUM-001 | CRITICAL | `src/.../core/utils.py::pivot_df_frequency` | Interval sort key `int(x[1:-2])` raises `ValueError` on range intervals `[N-M]`; broad `except Exception` swallows it and returns the un-pivoted long-format DataFrame |
| NUM-002 | HIGH | `src/.../core/utils.py::csv_to_file` | `NaN`/`None` statistic values render as empty CSV cells; no sentinel or warning, indistinguishable from absent data |
| NUM-003 | LOW | `src/.../core/utils.py::csv_to_file` | No `float_format`/rounding policy; raw float artifacts leak into the locale CSV; no declared tolerance for any derived quantity |
| NUM-004 | LOW | `src/.../core/utils.py::pivot_df_frequency` | `first` aggregation has no determinism contract or dedup guard; output depends on undocumented API row order |

#### NUM-001 — [CRITICAL] Range-format frequency intervals `[N-M]` crash the interval sort and the pivot is silently skipped

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::pivot_df_frequency`
**Disambiguator:** `int(x[1:-2])`
**Checked:** ran
**Why this band:** CRITICAL — "a pivot that is incorrect without any error; no distinguishable failure state." When the Mediascope API returns range intervals (e.g. `[0-1]`, `[2-3]`, `[4-5]`), the sort key evaluates `int(x[1:-2])` = `int("0-")` → `ValueError`. The `try/except Exception` at utils.py:218 logs in Russian and `return df` (the original long-format frame). `DefaultDataStrategy.prepare` (reports.py:450-451) returns that frame, `extract_data` (reports.py:320-332) writes it, and the worker marks the task ready (reports.py:152), printing `✅ Отчёт успешно завершён` — a wrong-shape CSV is delivered with no failure surfaced to the seller or the pipeline.
**Problem:** The frequency-distribution pivot is the derived quantity `FrequencyDistPer` reshaped into one column per interval. `pivot_df_frequency` (utils.py:239-244) orders interval columns with `key=lambda x: int(x[1:-2]) if x.startswith("[") else -1`. This only parses the open-ended `[N+]` form (`"[1+]"`→`"1"`→`1`, `"[20+]"`→`"20"`→`20`). A range interval `"[0-1]"` yields `x[1:-2]="0-"`, `"[10-14]"` yields `"10-1"` — neither parses as `int`, raising `ValueError`. The broad `except Exception` (utils.py:250-252) catches it and returns the unmodified long-format `df`, so the CSV keeps `frequencyDistInterval` + `FrequencyDistPer` as ordinary columns instead of pivoted `[0-1]`/`[2-3]`/... columns. Range conditions are a supported, documented Mediascope API feature: the official `mediascope-jupyter` example `crosstab_spots_03_frequency_distribution_n+.ipynb` declares `{'from': 1, 'to': 3}`, `{'from': 4, 'to': 7}`, `{'from': 8, 'to': 20}`. The shipped config (`settings/reports/last_category/nat_tv_brands_weekly_reach.yaml`) uses only `{'from': N}` (open-ended), so it renders `[N+]` and does not trigger — the defect is latent for the shipped config but active for any seller using range intervals.
**Repro:**
1. Build a DataFrame with `frequencyDistInterval` values `['-', '[0-1]', '[2-3]', '[4-5]', '[6-7]', '[8-9]']` plus `FrequencyDistPer` and call `pivot_df_frequency(df, ['regionName'])`.
2. Observe `ValueError: invalid literal for int() with base 10: '0-'` logged (Russian `Ошибка ... при обработке частот.`), then the returned frame has columns `['regionName', 'frequencyDistInterval', 'FrequencyDistPer', 'RtgPerSum']` — the un-pivoted long form (18 rows × 4 cols), not the wide pivot.
3. The same input built with `'[1+]','[2+]','[3+]','[5+]','[20+]'` pivots correctly (2 rows × 10 cols). The outcome depends entirely on the bracket format.
**Impact:** Sellers consuming the frequency-distribution CSV expect one column per contact-count interval. On range intervals the file is long-format and structurally wrong; consumers reading it as authoritative (e.g. a BI load expecting `[0-1]` columns) ingest a corrupted schema while the run reports success.
**Root cause:** The sort key assumes the single-token `[N+]` rendering and has no fallback for `[N-M]` ranges; the bare `except Exception: return df` turns an unhandled format into silent success.
**Fix:** Replace the `int(x[1:-2])` parse with a range-aware key that extracts the lower bound from both `[N+]` and `[N-M]` (e.g. `int(re.match(r"\[(\d+)", x).group(1))`), and narrow the `except` to a logged failure that returns `None` so `DefaultDataStrategy.prepare` raises/skips instead of writing a wrong-shape CSV. Target symbol: `pivot_df_frequency` interval sort + exception handling.
**Verify:** No shipped test exercises `pivot_df_frequency` with intervals. new test needed: `test_pivot_df_frequency_handles_range_intervals`
**Related:** NUM-004 (same unguarded `first`/sort path)

**Evidence — runtime probe (range format raises and returns long form):**
```text
Ошибка 'invalid literal for int() with base 10: '0-'' при обработке частот.
Traceback (most recent call last):
  File ".../core/utils.py", line 240, in pivot_df_frequency
    sorted(
  File ".../core/utils.py", line 241, in <lambda>
    freq_df.columns, key=lambda x: int(x[1:-2]) if x.startswith("[") else -1
ValueError: invalid literal for int() with base 10: '0-'
RESULT_COLUMNS: ['regionName', 'frequencyDistInterval', 'FrequencyDistPer', 'RtgPerSum']
Shape after call: (18, 4)        # returned LONG format, not the expected wide pivot
```

**Evidence — runtime probe (open-ended `[N+]` format pivots correctly):**
```text
RESULT_COLUMNS: ['regionName', 'FrequencyDistPer', 'RtgPerSum', '[1+]', '[2+]', '[3+]', '[5+]', '[7+]', '[10+]', '[15+]', '[20+]']
pivoted? True
```

**Evidence — source (`pivot_df_frequency`, utils.py:214-252):**
```text
def pivot_df_frequency(df: DataFrame, dim_cols) -> DataFrame:
    if "frequencyDistInterval" not in df.columns:
        return df
    try:
        base_df = (
            df[df["frequencyDistInterval"] == "-"]
            .drop(columns="frequencyDistInterval")
            .dropna(axis=1, how="all")
            .groupby(dim_cols, as_index=False)
            .first()
        )
        freq_df = (
            df[df["frequencyDistInterval"] != "-"]
            .pivot_table(
                index=dim_cols,
                columns="frequencyDistInterval",
                values="FrequencyDistPer",
                aggfunc="first",
            )
            .reset_index()
        )
        freq_df = freq_df.reindex(
            sorted(
                freq_df.columns, key=lambda x: int(x[1:-2]) if x.startswith("[") else -1
            ),
            axis=1,
        )
        result = base_df.merge(freq_df, on=dim_cols, how="left")
        return result
    except Exception as err:
        logger.exception(f"Ошибка '{err}' при обработке частот.")
        return df
```

#### NUM-002 — [HIGH] `NaN`/`None` derived quantities render as empty CSV cells with no sentinel or failure

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `data_frame.to_csv(..., decimal=",", sep=";")` (no `na_rep`)
**Checked:** ran
**Why this band:** HIGH — "a NaN that propagates to the rendered CSV without being caught or reported." `csv_to_file` (utils.py:186-198) calls `data_frame.to_csv(..., decimal=",", sep=";", ...)` with no `na_rep`; pandas 3.0.0 defaults `na_rep` to `""`, so `NaN` and `None` (which `result2table` converts to `NaN`) both render as empty fields. No warning, no sentinel, and `default_data_strategy`/`extract_data` never validate non-nullness before writing, so a missing derived value reaches the seller's CSV silently.
**Problem:** A derived quantity such as `FrequencyDistPer` or `RtgPerSum` that the Mediascope API could not compute for a (dimension, interval) cell is returned by `result2table` as `None`→`NaN`. `csv_to_file` writes it as an empty cell. A seller opening the CSV in Excel or a BI tool sees a blank and — depending on the tool — treats it as 0 in a `SUM` or arithmetic, or skips it silently, with no indication that data was missing for that cell. There is no distinct failure state: the task is still marked ready and the run reports success.
**Repro:**
1. `df = pd.DataFrame({'regionName': ['Msk','SPb','Tula'], 'FrequencyDistPer': [0.30000000000000004, 0.3333333333333333, float('nan')], 'RtgPerSum': [33.33333333333333, 66.66666666666667, None]})`.
2. `csv_to_file(df, tmp, file_prefix='arith', add_time=False, compression=None)`.
3. Inspect the emitted CSV: the `Tula` row renders as `Tula;;` — both `NaN` and `None` are empty, indistinguishable from one another and from a genuinely missing trailing value.
**Impact:** A seller aggregates reach/frequency values across intervals; a blank cell is silently treated as 0 by Excel-style consumers, producing a total that is too low (or skipped), with no error raised. The run still prints success.
**Root cause:** No `na_rep` / no null-validation is configured in the export path; the pipeline trusts the API payload as complete.
**Fix:** Set an explicit `na_rep` (e.g. `na_rep=""` is acceptable only if a consumer contract documents it) or surface a per-column null count via the logger before export and require a non-null expectation for `statistics` columns. Target symbol: `csv_to_file` and `DefaultDataStrategy.prepare`.
**Verify:** No shipped test asserts `na_rep` or null handling. new test needed: `test_csv_to_file_na_rep_is_distinct_or_warned`
**Related:** NUM-003 (same `csv_to_file` export path)

**Evidence — runtime probe (NaN and None both render as empty):**
```text
=== CSV OUTPUT ===
regionName;FrequencyDistPer;RtgPerSum
Msk;0,30000000000000004;33,33333333333333
SPb;0,3333333333333333;66,66666666666667
Tula;;
```

**Evidence — `csv_to_file` has no `na_rep`/`float_format`/`round` (static):**
```text
src/mko_get_mediascope_data/core/utils.py:186:    try:
src/mko_get_mediascope_data/core/utils.py:188:        data_frame.to_csv(
src/mko_get_mediascope_data/core/utils.py:193:            decimal=",",
src/mko_get_mediascope_data/core/utils.py:194:            sep=";",
src/mko_get_mediascope_data/core/utils.py:198:        )
HAS float_format: False
HAS na_rep: False
HAS round: False
```

#### NUM-003 — [LOW] No float-formatting policy or declared tolerance; raw float artifacts reach the locale CSV

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `data_frame.to_csv(..., decimal=",", sep=";", **kwargs)` (no `float_format`)
**Checked:** ran
**Why this band:** LOW — rendering/precision-policy gap with no wrong-value consequence. The pipeline performs no summation or `mean` of its own (derived values are computed server-side by the Mediascope API), so there is no in-tool float accumulation to bound; the gap is purely that `to_csv` is called with no `float_format` and no rounding, nor any declared tolerance for the derived statistics.
**Problem:** Because `float_format` is not passed to `to_csv`, pandas emits full IEEE-754 repr for float columns. For the locale format `decimal=","`, this renders contact-rate artifacts like `0,30000000000000004` (the float repr of `0.1 + 0.2`) directly into the seller-facing CSV. No constant, setting, or docstring declares a rounding/tolerance contract for `RtgPerSum`, `CumReachPer`, `FrequencyDistPer`, or `ShareAvg`, so the precision policy is implicit and unversioned. (Verified: `decimal=","` does **not** truncate — full precision is preserved.)
**Repro:** See NUM-002 evidence above: `FrequencyDistPer` for `0.1+0.2` renders as `0,30000000000000004`, and `1/3` renders as `0,3333333333333333`.
**Impact:** Cosmetic and parsing-nuisance: a seller sees `0,30000000000000004` for what should read `0,3`; a downstream parser expecting a fixed number of decimals may mis-split fields. Values are numerically faithful, so no wrong figure is computed.
**Root cause:** No `float_format` (or pre-export rounding) and no declared precision/tolerance contract anywhere in `settings/` or `docs/`.
**Fix:** Add an explicit `float_format` (e.g. `float_format="%.4f"`) to the `to_csv` call, or round percentage statistics to a declared number of decimals before export, and document the tolerance in a constants/setting. Target symbol: `csv_to_file`.
**Verify:** No shipped test asserts float formatting or precision. new test needed: `test_csv_to_file_float_format_is_declared`

**Evidence — `to_csv` call omits `float_format` (utils.py:188-198):**
```text
        data_frame.to_csv(
            *args,
            path_or_buf=out_file,
            index=False,
            mode="x",
            decimal=",",
            sep=";",
            encoding=encoding,
            compression=compression,
            **kwargs,
        )
```

#### NUM-004 — [LOW] `first` pivot/groupby aggregation has no determinism contract or dedup guard

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::pivot_df_frequency`
**Disambiguator:** `aggfunc="first"` (lines 234 and the `.first()` at 225)
**Checked:** static
**Why this band:** LOW — latent robustness gap; no duplicate rows were observed for the single-task export path.
**Problem:** `pivot_df_frequency` aggregates with `aggfunc="first"` in the `pivot_table` (utils.py:234) and `.groupby(dim_cols, as_index=False).first()` for the totals frame (utils.py:225). pandas `"first"` returns the first non-null value within each group, so the result depends on the row order supplied by `result2table`. The API row order is not guaranteed stable/deterministic, and `prepare` performs no pre-sort or uniqueness assertion on `(dim_cols, frequencyDistInterval)`. No comment, model field, or docstring declares that one row is expected per group, so a duplicate `(slice, interval)` row from the API would be collapsed non-deterministically, and a null-then-value ordering would pick a different cell than `sum` would. No shipped test asserts ordering-invariance or group uniqueness.
**Repro:** Static — there is no `sort_values`/`drop_duplicates`/`assert_unique` between `result2table` output and the `first` aggregation; the only ordering applied is the interval-column sort (utils.py:239), which itself crashes on ranges (NUM-001).
**Impact:** If the Mediascope API ever returns more than one row for the same `(dimension, frequencyDistInterval)` (e.g. via the `to_lists`/merge_rows path, or a multi-spot slice), `FrequencyDistPer` becomes silently nondeterministic across runs.
**Root cause:** The `first` aggregation choice is undocumented and unguarded.
**Fix:** Add an explicit contract (docstring/comment + a defensive `groupby(...).size()` check or a deterministic `aggfunc`) for the frequency pivot; if `first` is intentional (API already dedupes), document it. Target symbol: `pivot_df_frequency`.
**Verify:** No shipped test covers ordering-invariance or group uniqueness. new test needed: `test_pivot_df_frequency_is_order_invariant`

**Evidence — source (no sort/dedup before `first`, utils.py:220-237):**
```text
        base_df = (
            df[df["frequencyDistInterval"] == "-"]
            .drop(columns="frequencyDistInterval")
            .dropna(axis=1, how="all")
            .groupby(dim_cols, as_index=False)
            .first()
        )
        freq_df = (
            df[df["frequencyDistInterval"] != "-"]
            .pivot_table(
                index=dim_cols,
                columns="frequencyDistInterval",
                values="FrequencyDistPer",
                aggfunc="first",
            )
            .reset_index()
        )
```

## Fix Order

- **Dependencies:** None. NUM-001 must be fixed first (it blocks the entire frequency pivot); NUM-002/NUM-003/NUM-004 are independent export/pivot-guard improvements.
- **Merge candidates:** NUM-001 and NUM-004 both live in `pivot_df_frequency` — a single pass can fix the range-aware interval sort, the `na_rep` is elsewhere. All four touch `csv_to_file`/`pivot_df_frequency` only.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** Phase 05 (config-templates) and Phase 16 (rendered-surface/OUT-004) own the CSV format contract; NUM-003's `float_format` fix overlaps OUT-004's "no declared CSV contract" finding — fix together by extracting format params into a constant.
- **Rollout risk:** Fixing NUM-001 changes the interval sort to handle ranges (correct columns appear); fixing NUM-002 by adding a distinct `na_rep` changes how nulls render (consumers parsing empty cells would now see the sentinel) — `test_csv_to_file` (asserts only file count) must stay green.

## Appendices

### Appendix A — Runtime probe: `pivot_df_frequency` on range intervals (NUM-001)

```text
==== CASE B: range format ([0-1],[2-3],[4-5],[6-7],[8-9],[10-14],[15-19],[20+]) ====
Ошибка 'invalid literal for int() with base 10: '0-'' при обработке частот.
Traceback (most recent call last):
  File ".../core/utils.py", line 240, in pivot_df_frequency
    sorted(
  File ".../core/utils.py", line 241, in <lambda>
    freq_df.columns, key=lambda x: int(x[1:-2]) if x.startswith("[") else -1
ValueError: invalid literal for int() with base 10: '0-'
RESULT_COLUMNS: ['regionName', 'frequencyDistInterval', 'FrequencyDistPer', 'RtgPerSum']
Shape after call: (18, 4)        # LONG format returned, pivot NOT applied
pivoted? False
```

*(supports: "range format silently returns un-pivoted frame")*

### Appendix B — Runtime probe: `pivot_df_frequency` on open-ended `[N+]` format (NUM-001)

```text
==== CASE A: open-ended n+ format ====
RESULT_COLUMNS: ['regionName', 'FrequencyDistPer', 'RtgPerSum', '[1+]', '[2+]', '[3+]', '[5+]', '[7+]', '[10+]', '[15+]', '[20+]']
pivoted? True
```

*(supports: "the sort works only for `[N+]`; `[N-M]` ranges break it")*

### Appendix C — Runtime probe: `csv_to_file` float + NaN rendering (NUM-002, NUM-003)

```text
=== CSV OUTPUT ===
regionName;FrequencyDistPer;RtgPerSum
Msk;0,30000000000000004;33,33333333333333
SPb;0,3333333333333333;66,66666666666667
Tula;;
=== checks ===
HAS float_format: False
HAS na_rep: False
HAS round: False
```

*(supports: "NaN/None→empty cell; raw float artifacts like 0,30000000000000004; decimal="," preserves precision, no truncation")*

### Appendix D — Date-arithmetic helpers verified (no finding)

```text
get_last_period (anchor 2026-10-08 Thu):
  w n=2 inc=True  -> 2026-09-21 .. 2026-10-08
  w n=2 inc=False -> 2026-09-21 .. 2026-10-04
  w n=1 inc=False -> 2026-09-28 .. 2026-10-04
  m n=2 inc=False -> 2026-08-01 .. 2026-09-30
  m n=2 inc=True  -> 2026-08-01 .. 2026-10-08
  d n=3 inc=False -> 2026-10-05 .. 2026-10-07
  y n=2 inc=False -> 2024-01-01 .. 2025-12-31
slice_period weekly  (2026-09-28..2026-10-11): 2026-09-28..2026-10-04, 2026-10-05..2026-10-11
slice_period monthly (2026-01-01..2026-03-31): 2026-01-01..2026-01-31, 2026-02-01..2026-02-28, 2026-03-01..2026-03-31
```

*(supports: "date interval arithmetic is correct and contiguous; no off-by-one at month/week boundaries")*
