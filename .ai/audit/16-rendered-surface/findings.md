---
phase: "16"
phase_name: "rendered-surface"
id_prefix: "OUT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Rendered-Surface & Output Conformance

## Summary

Six findings: 3 MEDIUM, 3 LOW. The rendered output has no discoverable or
versioned contract — CSV format parameters are hardcoded magic values with no
schema document, and the error-file format is non-standard YAML that
`yaml.safe_load` cannot parse due to `!!python/tuple` tags. Line endings are
platform-dependent (`os.linesep`), and the pipeline has no mechanism to verify
that a reported-successful task actually produced a file. No test asserts any
rendered output format or error-file structure.

## Scope

**Examined:** `core/utils.py::csv_to_file` (CSV export path),
`core/tasks.py::TVTask.to_yaml` (error-file serialization),
`core/reports.py::extract_data` / `DefaultDataStrategy.prepare` /
`DictDataStrategy.prepare` (column selection and output preparation),
`core/models.py` (`Data`, `ReportSettings` — column schema derivation),
`tests/test_utils.py::test_csv_to_file` (test assertions on rendered output),
`tests/test_reports.py` (output-shape tests).

**Tools:** `uv run python` runtime probes (CSV byte-level format inspection,
YAML parse-back test, `to_yaml` path resolution), `rg` content search, static
read of `csv_to_file`/`to_yaml`/`DefaultDataStrategy` /`DictDataStrategy`.

**Not verified:** Live API calls (no Mediascope credentials); downstream
consumer parsing of rendered CSVs (no consumer codebase available). Cross-
reference: PIPE-001 (Phase 04) covers the `mode="x"` / `FileExistsError`
swallowing mechanism; this phase focuses on the *conformance* and
*verifiability* of what is written, not the write mechanics.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| OUT-001 | MEDIUM | `src/.../core/tasks.py::TVTask.to_yaml` | Error YAML uses `yaml.dump` (not `safe_dump`), emitting `!!python/tuple` tags that `yaml.safe_load` cannot parse |
| OUT-002 | MEDIUM | `src/.../core/utils.py::csv_to_file` | CSV `lineterminator` not set; defaults to `os.linesep` (CRLF on Windows, LF on Unix) — platform-dependent and undeclared |
| OUT-003 | MEDIUM | `src/.../core/utils.py::csv_to_file` | `csv_to_file` returns `None` always; `extract_data` cannot distinguish a successful write from a skipped `FileExistsError` |
| OUT-004 | LOW | `src/.../core/utils.py::csv_to_file` | No declared or versioned CSV output-format contract; `sep`, `decimal`, `encoding` are hardcoded magic values |
| OUT-005 | LOW | `src/.../core/tasks.py::TVTask.to_yaml` | `interval` field (set on `TVTask` in both task strategies) is missing from serialized error YAML |
| OUT-006 | LOW | `src/.../core/tasks.py::TVTask.to_yaml` | Error YAML keys emitted in alphabetical order (PyYAML `sort_keys=True` default); insertion order not declared |

#### OUT-001 — [MEDIUM] Error YAML serialized with `yaml.dump` emits `!!python/tuple` tags; unparseable by `yaml.safe_load`

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_yaml`
**Disambiguator:** `allow_unicode=True`
**Checked:** ran
**Why this band:** MEDIUM — "an error file whose structure or fields do not match the declared contract." The error file uses `yaml.dump` (full mode) rather than `yaml.safe_dump`. The `settings` dict contains `date_filter = [interval]` where `interval` is a Python `tuple` (set in `DefaultTVTaskStrategy.generate`, reports.py:367). Full `yaml.dump` serializes the tuple with the `!!python/tuple` tag, producing YAML that standard parsers — including `yaml.safe_load` — reject with `ConstructorError`.

**Problem:** `TVTask.to_yaml` (tasks.py:5) imports `dump` (not `safe_dump`) and calls it at line 46-51 with `default_flow_style=False, allow_unicode=True, encoding=None`. The `settings` dict passed to `dump_settings` includes `date_filter`, which in the real pipeline is `[("2025-01-01", "2025-01-07")]` — a list containing a Python tuple. `yaml.dump` renders this as:
```yaml
settings:
  date_filter:
  - !!python/tuple
    - '2025-01-01'
    - '2025-01-07'
```
Any consumer calling `yaml.safe_load()` on the error file raises `ConstructorError: could not determine a constructor for the tag 'tag:yaml.org,2002:python/tuple'`. Recovery mechanisms, re-run detection, or downstream reporting tools that parse error files are broken.

**Repro:**
1. Construct a `TVTask` with `settings = {'date_filter': [('2025-01-01', '2025-01-07')]}`, `error = 'Test'`, call `task.to_yaml(export_path)`.
2. Read the `.yaml` error file; observe `!!python/tuple` in `settings.date_filter`.
3. Call `yaml.safe_load(content)` → `ConstructorError`.

**Impact:** A downstream consumer or re-run-recovery tool that loads error YAML files with `yaml.safe_load` (the standard safe parser) crashes instead of reading the failure context. Error files are non-portable to any non-Python YAML parser.

**Root cause:** `to_yaml` uses `yaml.dump` (full representer set) instead of `yaml.safe_dump`, which would render tuples as standard YAML sequences.

**Fix:** Replace `from yaml import dump` with `from yaml import safe_dump` in `TVTask.to_yaml`, and change the `dump(...)` call to `safe_dump(...)`. Alternatively, convert tuples to lists before serialization (e.g. `settings['date_filter'] = list(settings['date_filter'])`).

**Verify:** No shipped test covers `TVTask.to_yaml`. new test needed: `test_to_yaml_safe_load_compatible`

**Evidence — runtime probe (error YAML contains `!!python/tuple`):**
```text
=== RAW ERROR YAML CONTENT ===
error: Task.json is not returned
name: 2025-01-01_2025-01-07
report_subtype: DYNAMICS_BY_SPOTS
report_type: crosstab
settings:
  basedemo_filter: age >= 18
  date_filter:
  - !!python/tuple
    - '2025-01-01'
    - '2025-01-07'
target: all 18+

Contains !!python/tuple tag: True
```

**Evidence — `yaml.safe_load` fails on the serialized error file:**
```text
yaml.constructor.ConstructorError: could not determine a constructor for the tag
'tag:yaml.org,2002:python/tuple'
  in "<unicode string>", line 8, column 5:
    - !!python/tuple
      ^
```

**Evidence — source (`tasks.py`):**
```text
src/mko_get_mediascope_data/core/tasks.py:5:  from yaml import dump
src/mko_get_mediascope_data/core/tasks.py:46:  dump(dump_settings, outfile, default_flow_style=False, allow_unicode=True, encoding=None)
```

#### OUT-002 — [MEDIUM] CSV line endings default to `os.linesep`; platform-dependent and undeclared

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `data_frame.to_csv(*args, ..., )` (no `lineterminator`)
**Checked:** ran
**Why this band:** MEDIUM — "an encoding or line-ending mismatch that is locale-dependent and undeclared." `csv_to_file` (utils.py:186-198) calls `data_frame.to_csv(...)` without passing `lineterminator`. pandas 3.0.0 defaults `lineterminator` to `None`, which resolves to `os.linesep` — `\r\n` on Windows, `\n` on Unix. No explicit value is declared in code, template, or documentation.

**Problem:** `csv_to_file` (utils.py:188-198) does not set `lineterminator` in the `to_csv` call. On the project's target platform (Windows, per `pyproject.toml` classifiers `OS Independent` and `platforms = ['Linux', 'Windows']`), the rendered CSV has CRLF (`\r\n`) line endings. On Unix it would produce LF (`\n`). The BOM, separator (`;`), and decimal mark (`,`) are all correct, but the line-ending style is platform-dependent and silently varies.

**Repro:**
1. Write a CSV via `csv_to_file` with `sep=';'`, `decimal=',', encoding='utf-8-sig'` on Windows.
2. Decompress the gzip and inspect raw bytes: line endings are `\r\n` (CRLF).
3. Explicitly pass `lineterminator='\\n'` to `to_csv` → LF only.

**Impact:** A downstream consumer (e.g., a Snowflake COPY, a pandas read, or a line-by-line parser) deployed on a different platform than the producer sees different line endings. Consumers that split on `\n` without stripping `\r` silently carry `\r` in the last field of each row.

**Root cause:** `lineterminator` is not passed to `DataFrame.to_csv`, relying on the platform-default `os.linesep`.

**Fix:** Add `lineterminator='\n'` to the `to_csv` call in `csv_to_file`, or route the call through a named parameter so the line-ending style is explicit and platform-independent.

**Verify:** `tests/test_utils.py::test_csv_to_file` only asserts file count (`len(files) == 1`); new test needed: `test_csv_to_file_line_endings_are_lf`

**Evidence — runtime probe (CSV rendered on Windows):**
```text
First bytes: efbbbf72657365617263  (efbbbf = UTF-8 BOM, then 'researchDate')
BOM present: True
CRLF: True
Content: 'researchDate;RtgPerSum\r\n2025-01-01;42,5\r\n'
```

**Evidence — lineterminator default (pandas 3.0.0):**
```text
lineterminator param default: None
Without lineterminator - Has CRLF: True
With lineterminator='\n' - Has CRLF: False
```

#### OUT-003 — [MEDIUM] `csv_to_file` returns `None` always; pipeline cannot verify output presence

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `except FileExistsError: logger.warning(...); return` (no return value)
**Checked:** ran (static read + cross-reference to PIPE-001)
**Why this band:** The pipeline has no mechanism to assert that a task reported as "ready" actually produced a conformant file. `csv_to_file` always returns `None` — even when the write is skipped by `mode="x"` + `FileExistsError`. `TVMediaReport.extract_data` (reports.py:325-332) calls `csv_to_file` via `asyncio.to_thread` without inspecting any return value, so a skipped write is indistinguishable from a successful one at the task level. This is the Phase-16 output-presence gap that enables PIPE-001 (Phase 04, CRITICAL).

**Problem:** `csv_to_file` (utils.py:167-200) returns `None` in all paths — success and `FileExistsError` skip alike. `extract_data` (reports.py:325-332) does not check the return value or verify the file exists after writing. The worker (reports.py:148-152) marks a task as `status is True` → "ready" in all cases where `extract_data` completes without raising, regardless of whether a CSV was actually written.

**Repro:**
1. Write `report1.csv.gz` once via `csv_to_file(df, out, file_prefix="report1", add_time=False, ...)`.
2. Call `csv_to_file` again with the same prefix → `FileExistsError` is caught, warning logged, `None` returned.
3. `extract_data` proceeds unchanged; `task.status` remains `True`; worker logs "ready" and calls `task_done()`.
4. No CSV was (re)written, yet the run reports success.

**Impact:** A re-run with `check_done=False`, or two tasks colliding on a filename, silently produces no CSV while the CLI prints `✅ Отчёт успешно завершён`. The pipeline cannot assert output presence for any task.

**Root cause:** `csv_to_file` provides no return signal (path, boolean, or sentinel), and `extract_data` never verifies the file exists on disk after the write.

**Fix:** Have `csv_to_file` return the `Path` of the written file (or `None` on skip); in `extract_data`, check the return and set `task.status = False` with a distinct error when the write was skipped or failed. Target symbol: `csv_to_file` and `TVMediaReport.extract_data`.

**Verify:** `tests/test_utils.py::test_csv_to_file` asserts only `len(files) == 1` — does not check return value or post-write existence. Ships test `test_csv_to_file_duplicate_write_does_not_report_success` is blocked (see PIPE-001, Phase 04). new test needed: `test_csv_to_file_returns_path_on_success`

**Related:** PIPE-001 (Phase 04, CRITICAL — same `mode="x"` + `FileExistsError` mechanism)

**Evidence — `extract_data` ignores return value (reports.py):**
```text
src/mko_get_mediascope_data/core/reports.py:325:  await asyncio.to_thread(
src/mko_get_mediascope_data/core/reports.py:326:      utils.csv_to_file,
src/mko_get_mediascope_data/core/reports.py:327:      df,
src/mko_get_mediascope_data/core/reports.py:328:      file_prefix=task.name,
src/mko_get_mediascope_data/core/reports.py:329:      add_time=False,
src/mko_get_mediascope_data/core/reports.py:330:      csv_path_out=self.export_path,
src/mko_get_mediascope_data/core/reports.py:331:      compression=self.report_settings.compression,
src/mko_get_mediascope_data/core/reports.py:332:  )
```
*(supports: "no return value inspected")*

**Evidence — `csv_to_file` returns `None` in all paths (utils.py):**
```text
src/mko_get_mediascope_data/core/utils.py:167:  def csv_to_file(...):
src/mko_get_mediascope_data/core/utils.py:199:      except FileExistsError:
src/mko_get_mediascope_data/core/utils.py:200:          logger.warning(f"File report {out_file} already exists. Skip it.")
```
*(supports: "no return on success or skip")*

#### OUT-004 — [LOW] No declared or versioned CSV output-format contract

**Anchor:** `src/mko_get_mediascope_data/core/utils.py::csv_to_file`
**Disambiguator:** `decimal=",", sep=";", encoding="utf-8-sig", mode="x"`
**Checked:** ran (static read + runtime probe)
**Why this band:** LOW — "documentation or comment drift about output format or schema, with no runtime consequence." The CSV format parameters are hardcoded as magic values inside `csv_to_file` with no constants module, YAML setting, or documentation declaring them as the output contract. A consumer, a new contributor, or a downstream data-engineering team has no discoverable specification for the rendered CSV format. The values themselves are consistent (sep `;`, decimal `,`, encoding `utf-8-sig`, no index, exclusive-create mode), but the contract is not versioned or findable.

**Problem:** `csv_to_file` (utils.py:187-196) hardcodes `decimal=","`, `sep=";"`, `encoding="utf-8-sig"` (via `kwargs.pop("encoding", "utf-8-sig")`), `mode="x"`, `index=False`. No constant, config key, or doc declares these as the output contract. There is no format-specification file under `settings/` or `docs/`.

**Repro:**
```text
$ rg -n "utf-8-sig|decimal|sep=" src/mko_get_mediascope_data/core/utils.py
src/mko_get_mediascope_data/core/utils.py:187:  encoding = kwargs.pop("encoding", "utf-8-sig")
src/mko_get_mediascope_data/core/utils.py:193:  decimal=",",
src/mko_get_mediascope_data/core/utils.py:194:  sep=";",
```
No matching declaration exists in `settings/`, `docs/`, or `AGENTS.md`.

**Impact:** The format contract lives only as implicit knowledge in one function. A format change (e.g., switching to `.`/`sep=`,` for an international consumer) would require finding and editing the magic values, with no test or spec to catch the drift. New contributors cannot determine the output format without reading `csv_to_file` source.

**Root cause:** Format parameters were hardcoded at implementation time with no follow-up to extract them into a declared, versioned contract.

**Fix:** Extract the format parameters into a named, documented module-level constant (e.g. `CSV_OUTPUT_FORMAT` in `utils.py` or a `core/constants.py`) with a docstring declaring the contract, and reference it in `csv_to_file`. Optionally add a test asserting the rendered format matches the constant.

**Verify:** new test needed: `test_csv_to_file_format_matches_declared_contract`

#### OUT-005 — [LOW] `interval` field missing from serialized error YAML

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_yaml`
**Disambiguator:** `dump_settings = {"name": ..., "report_type": ..., "report_subtype": ..., "target": ..., "error": ..., "settings": ...}`
**Checked:** ran (runtime probe)
**Why this band:** LOW — the interval is recoverable from `settings.date_filter`, but the phase's error-file context checklist ("settings, interval, target, error message") has a gap. `TVTask.interval` is set in both `DefaultTVTaskStrategy.generate` (reports.py:376) and `TVRegTaskStrategy.generate` (reports.py:428) but is not in the `dump_settings` dict (tasks.py:36-43).

**Problem:** `TVTask.to_yaml` serializes six fields: `name`, `report_type`, `report_subtype`, `target`, `error`, `settings`. The `interval` attribute (a tuple like `("2025-01-01", "2025-01-07")`) is not serialized as a top-level field. It is only indirectly recoverable via `settings.date_filter` (which contains `[interval]` as a nested tuple — itself non-standard YAML per OUT-001).

**Repro:**
1. Set `task.interval = ('2025-01-01', '2025-01-07')`.
2. Call `task.to_yaml(export_path)`.
3. Parse the error YAML; observe no top-level `interval` key. Verify: `'interval' in parsed` → `False`.

**Impact:** An operator or recovery tool reading the error file must dig into `settings.date_filter` (which is a non-standard `!!python/tuple` structure) to recover the interval, rather than reading a clean top-level `interval` field.

**Root cause:** The `dump_settings` dict in `to_yaml` omits `self.interval`.

**Fix:** Add `"interval": self.interval` to the `dump_settings` dict in `TVTask.to_yaml`.

**Verify:** no shipped test covers error-file field inventory. new test needed: `test_to_yaml_includes_interval_field`

**Evidence — runtime probe (error YAML field inventory):**
```text
Fields in YAML: ['error', 'name', 'report_subtype', 'report_type', 'settings', 'target']
Has 'interval' field: False
task.interval value: ('2025-01-01', '2025-01-07')
```

#### OUT-006 — [LOW] Error file YAML keys emitted in alphabetical order, not insertion order

**Anchor:** `src/mko_get_mediascope_data/core/tasks.py::TVTask.to_yaml`
**Disambiguator:** `default_flow_style=False`
**Checked:** ran (runtime probe)
**Why this band:** LOW — "documentation or comment drift about output format or schema, with no runtime consequence." PyYAML's `dump()` defaults to `sort_keys=True`, which sorts top-level keys alphabetically. The `dump_settings` dict defines fields in a specific logical order (name, report_type, report_subtype, target, error, settings), but the rendered YAML emits them as: `error`, `name`, `report_subtype`, `report_type`, `settings`, `target`. No `sort_keys=False` is passed, and no contract declares the expected field order.

**Problem:** `TVTask.to_yaml` (tasks.py:46-51) calls `dump(dump_settings, outfile, ...)` without `sort_keys=False`. PyYAML sorts all mapping keys alphabetically by default. The rendered error YAML fields appear as: `error`, `name`, `report_subtype`, `report_type`, `settings`, `target` — alphabetical, not the insertion order from the `dump_settings` dict literal.

**Repro:**
1. Set task fields and call `to_yaml`.
2. Read the rendered YAML line by line.
3. Top-level keys appear as: `error`, `name`, `report_subtype`, `report_type`, `settings`, `target` (alphabetical).

**Impact:** A consumer or human reader expecting fields in the logical order defined by the code (settings last, name first) is surprised by alphabetical ordering. No contract declares the expected order, so the deviation is undocumented.

**Root cause:** `dump()` is called without `sort_keys=False`; the default `sort_keys=True` alphabetizes all keys.

**Fix:** Pass `sort_keys=False` to `dump()` (or `safe_dump()`) in `TVTask.to_yaml` so the declared insertion order is preserved.

**Verify:** no shipped test asserts error-file field order. new test needed: `test_to_yaml_preserves_field_order`

**Evidence — runtime probe (key order in rendered YAML):**
```text
Error YAML key order in file:
  error: Test error
  name: test_task
  report_subtype: DYNAMICS_BY_SPOTS
  report_type: crosstab
  settings:
  target: all 18+
```

## Fix Order

- **Dependencies:** OUT-003 (return signal from `csv_to_file`) is a prerequisite for the Phase 04 PIPE-001 fix (marking skipped writes as failed); fix together.
- **Merge candidates:** OUT-001, OUT-005, and OUT-006 all touch `TVTask.to_yaml` — a single pass to switch from `dump` to `safe_dump`, add `sort_keys=False`, and include `interval` resolves all three.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** OUT-003's `mode="x"` mechanism overlaps PIPE-001 (Phase 04, CRITICAL). OUT-001's `!!python/tuple` serialization overlaps QLT-011 (Phase 10, MEDIUM — `*args/**kwargs` forwarding in `csv_to_file`) only superficially (different module).
- **Rollout risk:** Fixing OUT-001 (switching to `safe_dump`) changes the error YAML format (removes `!!python/tuple` tags); any existing consumer that relies on `yaml.full_load`/`yaml.unsafe_load` to parse error files would still work (`safe_dump` output is a subset). Fixing OUT-003 (returning a `Path`) changes `csv_to_file`'s return type from `None` to `Path | None` — `extract_data` (reports.py:325-332) and any external callers must not break on the new return value. `test_csv_to_file` must stay green.

## Appendices

### Appendix A — CSV rendered format on Windows (pandas 3.0.0)

```text
=== CSV FORMAT PROBE ===
First bytes: efbbbf72657365617263  (efbbbf = UTF-8 BOM, then 'researchDate')
BOM present: True
CRLF: True
Content: 'researchDate;RtgPerSum\r\n2025-01-01;42,5\r\n'
=== lineterminator probe ===
lineterminator param default: None
Without lineterminator - Has CRLF: True
With lineterminator='\n' - Has CRLF: False
```

*(supports: OUT-002 — format is correct except line endings are platform-dependent)*

### Appendix B — Error YAML rendered format

```text
=== RAW ERROR YAML CONTENT ===
error: Task.json is not returned
name: 2025-01-01_2025-01-07
report_subtype: DYNAMICS_BY_SPOTS
report_type: crosstab
settings:
  basedemo_filter: age >= 18
  date_filter:
  - !!python/tuple
    - '2025-01-01'
    - '2025-01-07'
target: all 18+

Contains !!python/tuple tag: True
Line ending CRLF: True
```

*(supports: OUT-001, OUT-005, OUT-006)*

### Appendix C — `yaml.safe_load` failure on rendered error YAML

```text
yaml.constructor.ConstructorError: could not determine a constructor for the
tag 'tag:yaml.org,2002:python/tuple'
  in "<unicode string>", line 8, column 5:
    - !!python/tuple
      ^
```

*(supports: OUT-001 — standard YAML parser cannot read the error file)*

### Appendix D — `csv_to_file` source (utils.py:167-200)

```text
def csv_to_file(
    data_frame: DataFrame,
    csv_path_out: PathLike,
    file_prefix: str = "",
    compression: ... = "infer",
    add_time: bool = True,
    *args,
    **kwargs,
):
    ...
    encoding = kwargs.pop("encoding", "utf-8-sig")
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
    except FileExistsError:
        logger.warning(f"File report {out_file} already exists. Skip it.")
```

*(supports: OUT-002 — no `lineterminator` passed; OUT-003 — returns `None` on all paths; OUT-004 — hardcoded format values)*

### Appendix E — Existing test coverage gap

```text
$ rg -n "csv_to_file|to_yaml|lineterminator|sort_keys|safe_dump" tests/
tests/test_utils.py:23:  def test_csv_to_file(tmp_path):
tests/test_utils.py:25:    csv_to_file(...)
tests/test_utils.py:28:    files = list(tmp_path.glob("*.csv.gz"))
tests/test_utils.py:29:    assert len(files) == 1
```

`test_csv_to_file` asserts only file count — no format, schema, encoding, BOM, line-ending, or error-file assertions exist anywhere in `tests/`.

*(supports: all findings — rendered output is untested)*
