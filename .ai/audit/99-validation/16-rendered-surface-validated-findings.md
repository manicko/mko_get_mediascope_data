---
phase: "16"
phase_name: "rendered-surface"
id_prefix: "OUT"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

<!-- JSON5-MIGRATION TODO: This validated-findings doc references pre-migration YAML
  symbols (TVTask.to_yaml, yaml.safe_dump, yaml.safe_load, error YAML) that no longer
  exist after the JSON5 settings templates migration. Anchors need re-verification in a
  future audit pass; this does NOT block the implementation PR. Verdicts remain
  factually accurate as historical records of the code_ref (d093d9c). -->

# Audit Findings — Validation Report: Rendered-Surface & Output Conformance

## Summary

Six findings: 3 MEDIUM, 3 LOW. All six anchors resolve to the cited
locations, all disambiguator tokens exist inside their primary anchors, and all
six claims reproduce under runtime probe or static inspection. **6 confirmed,
0 rejected, 1 re-graded, 0 re-typed, 0 unsettled.**

OUT-003 is re-graded **MEDIUM → HIGH**: its impact ("silently produces no CSV
while the CLI prints success") matches the Phase 16 HIGH band's "mode='x'
skipping an existing file so a stale result is reported as fresh." The Phase 16
MEDIUM band's three sub-bullets (encoding/line-ending mismatch, format
deviation, error-file structure) do not cover output-presence verification.

One validation-level finding (VAL-001) is recorded: a cross-reference
inaccuracy in the findings' cross-phase analysis.

## Scope

**Examined:** `core/tasks.py` (full file, lines 1–52), `core/utils.py::csv_to_file`
(lines 167–200), `core/reports.py` (`extract_data` 295–333, `worker` 134–153,
`DefaultTVTaskStrategy.generate` 358–381, `TVRegTaskStrategy.generate` 384–432),
`core/models.py::PeriodModel` (lines 68–80), `tests/test_utils.py` (full),
`tests/test_reports.py` (full), `tests/conftest.py` (full), `pyproject.toml`,
`.kilo/commands/audit/phases/16-audit-rendered-surface.md`,
`.ai/audit/10-code-quality/findings.md` (QLT-011),
`.ai/audit/99-validation/04-data-pipeline-validated-findings.md` (PIPE-001),
`.ai/audit/templates/audit-findings.md`.

**Runtime probes performed:**
- `yaml.dump` with `{'date_filter': [('2025-01-01', '2025-01-07')]}` → emits `!!python/tuple`; `yaml.safe_load(content)` → `ConstructorError`
- `csv_to_file` via `DataFrame.to_csv` without `lineterminator` on Windows → CRLF (`\r\n`); with `lineterminator='\n'` → LF only
- pandas version: `3.0.0` (dependency floor: `>=2.2.0`); `lineterminator` param default: `None`
- PyYAML `6.0.3`: `yaml.dump` default `sort_keys=True` → keys alphabetical; `sort_keys=False` → insertion order preserved
- `git rev-parse --short HEAD` → `d093d9c` (matches `code_ref`)

**Not verified:** Live Mediascope API calls (no credentials). The Phase 04
PIPE-001 runtime probe (REPRO C: `csv_to_file` duplicate write → `FileExistsError`
swallowed) was taken from the already-validated Phase 04 report, not re-run
here; the source claim is independently verifiable by static read.

---

## Conformance

**Validated against:** Phase 16 baseline; **HEAD at review:** `8ed385f`
(code-identical to `b04d177`).

**Conformance review 2026-10-15** — reviewed against the code at HEAD and the
owner decision register (D-05: the audit is a lead source, not a source of
truth). All 6 findings re-verified at HEAD; OUT-001 and OUT-003 were
**re-probed** rather than read (`.ai/tmp/doc_probe_reverify_audit.py`): the
error YAML written for a failed task contains `!!python/tuple` and
`yaml.safe_load` raises `ConstructorError`; the duplicate-write path returns
`None` and raises nothing (one file remains on disk). OUT-003's HIGH re-grade
is therefore corroborated by execution, not only by inspection. Mapping:
OUT-001 → D-09 (the failed-task description is critical and must be
machine-readable) and W-02 slice 2 (`yaml.safe_dump`); OUT-002 → D-13 / W-04
(platform-dependent line endings become configurable); OUT-003 → D-09 / D-17,
W-02 slice 1; OUT-004 → D-13 / W-04 (declared output contract); OUT-005 →
D-09 (add `interval` to the error artefact); OUT-006 → W-02 slice 2.

---

## Findings

| ID | Disposition | Severity | Band (rubric) vs. Carried | Anchor resolves |
|---|---|---|---|---|
| OUT-001 | confirmed | MEDIUM | MEDIUM vs. MEDIUM | yes |
| OUT-002 | confirmed | MEDIUM | MEDIUM vs. MEDIUM | yes |
| OUT-003 | re-graded | MEDIUM → HIGH | HIGH vs. MEDIUM | yes |
| OUT-004 | confirmed | LOW | LOW vs. LOW | yes |
| OUT-005 | confirmed | LOW | LOW vs. LOW | yes |
| OUT-006 | confirmed | LOW | LOW vs. LOW | yes |

### OUT-001 — confirmed — [MEDIUM] Error YAML uses `yaml.dump` emitting `!!python/tuple`

**Anchor resolved:** `core/tasks.py::TVTask.to_yaml` — method at line 29;
`from yaml import dump` at line 5; `dump(...)` call at lines 46–51.

**Claim tested:** `TVTask.to_yaml` imports `dump` (not `safe_dump`) and calls
`dump(dump_settings, outfile, default_flow_style=False, allow_unicode=True,
encoding=None)`. The `settings` dict contains `date_filter` = `[interval]`
where `interval` is a Python tuple (reports.py:367 in `DefaultTVTaskStrategy`,
reports.py:410 in `TVRegTaskStrategy`). `yaml.dump` (full representer set)
renders the tuple with `!!python/tuple`, producing YAML that
`yaml.safe_load` rejects with `ConstructorError`.

**Evidence (runtime probe):**
```text
=== yaml.dump output ===
error: Task.json is not returned
name: test
report_subtype: DYNAMICS_BY_SPOTS
report_type: crosstab
settings:
  date_filter:
  - !!python/tuple
    - '2025-01-01'
    - '2025-01-07'
target: all 18+

Contains !!python/tuple tag: True
yaml.constructor.ConstructorError: could not determine a constructor for the
tag 'tag:yaml.org,2002:python/tuple'
```

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/tasks.py:5:  from yaml import dump
src/mko_get_mediascope_data/core/tasks.py:46:  dump(dump_settings, outfile, default_flow_style=False, allow_unicode=True, encoding=None)
src/mko_get_mediascope_data/core/reports.py:367:  settings["date_filter"] = [interval]
src/mko_get_mediascope_data/core/reports.py:410:  settings["date_filter"] = [interval]
```

**Rubric grading:** Phase 16 MEDIUM = "an error file whose structure or fields
do not match the declared contract." An error file rendered as non-standard
YAML (`!!python/tuple`) that `yaml.safe_load` cannot parse matches. No
re-grade.

**Load-bearing artefact:** Code (`core/tasks.py::TVTask.to_yaml`). The `interval`
tuple originates in `DefaultTVTaskStrategy.generate` (reports.py:366–367) and
`TVRegTaskStrategy.generate` (reports.py:409–410), is stored on
`TVTask.settings` (tasks.py:19), and reaches `to_yaml` via `settings` →
`dump_settings["settings"]` (tasks.py:42).

**Recommendation carry-out:** Switch `from yaml import dump` to
`from yaml import safe_dump` and change the `dump(...)` call to `safe_dump(...)`.
`safe_dump` renders tuples as standard YAML sequences. Runtime probe confirms
safe_dump output parses successfully with `yaml.safe_load`. Target
`TVTask.to_yaml` exists and is stable. ✓

### OUT-002 — confirmed — [MEDIUM] CSV `lineterminator` not set; platform-dependent

**Anchor resolved:** `core/utils.py::csv_to_file` — function at line 167;
`data_frame.to_csv(...)` call at lines 188–198.

**Claim tested:** `csv_to_file` calls `data_frame.to_csv(...)` without passing
`lineterminator`. pandas 3.0.0 defaults `lineterminator` to `None`, which
resolves to `os.linesep` — `\r\n` on Windows, `\n` on Unix. No explicit
line-ending value is declared in code, template, or documentation.

**Evidence (runtime probe):**
```text
=== csv_to_file behavior (no lineterminator) ===
BOM present: True
CRLF present: True
Content: b'\xef\xbb\xbfresearchDate;RtgPerSum\r\n2025-01-01;42,5\r\n'

=== with lineterminator='\n' ===
CRLF present: False
```

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/utils.py:188:  data_frame.to_csv(
src/mko_get_mediascope_data/core/utils.py:196:  compression=compression,
src/mko_get_mediascope_data/core/utils.py:197:  **kwargs,
(no lineterminator parameter in the call)
```

**Rubric grading:** Phase 16 MEDIUM = "an encoding or line-ending mismatch
that is locale-dependent and undeclared." CRLF on Windows, LF on Unix, with no
explicit declaration. Matches. No re-grade.

**Load-bearing artefact:** Code (`core/utils.py::csv_to_file`).

**Recommendation carry-out:** Add `lineterminator='\n'` to the `to_csv` call.
Target `csv_to_file` exists and is stable. Must not break `test_csv_to_file`
(which asserts only file count). ✓

### OUT-003 — re-graded — [MEDIUM → HIGH] `csv_to_file` returns `None`; pipeline cannot verify output presence

**Anchor resolved:** `core/utils.py::csv_to_file` — function at line 167
(no `return` statement anywhere; returns `None` implicitly on all paths);
`core/reports.py::TVMediaReport.extract_data` — method at line 295,
`asyncio.to_thread(utils.csv_to_file, ...)` at lines 325–332 with return value
discarded.

**Claim tested:** `csv_to_file` always returns `None` — success and
`FileExistsError` skip paths alike. `extract_data` does not inspect the return
value or verify the file exists on disk after writing. The worker
(reports.py:134–153) marks a task as `status is True` → "ready" in all cases
where `extract_data` completes without raising, regardless of whether a CSV was
actually written.

**Evidence (static read):**
```text
src/mko_get_mediascope_data/core/utils.py:167:  def csv_to_file(...):
(no return statement between 167 and 200)
src/mko_get_mediascope_data/core/utils.py:199:  except FileExistsError:
src/mko_get_mediascope_data/core/utils.py:200:  logger.warning(f"File report {out_file} already exists. Skip it.")
src/mko_get_mediascope_data/core/reports.py:325:  await asyncio.to_thread(
src/mko_get_mediascope_data/core/reports.py:326:      utils.csv_to_file,
(return value not assigned)
src/mko_get_mediascope_data/core/reports.py:144:  if item.status is True:
src/mko_get_mediascope_data/core/reports.py:147:      await self.extract_data(item)
src/mko_get_mediascope_data/core/reports.py:151:  else:
src/mko_get_mediascope_data/core/reports.py:152:      logger.info(f"Задача {item.name} готова.")
```

**Evidence (Phase 04 REPRO C, taken from validated Phase 04 report):**
```text
1st write -> files: ['report1.csv.gz']
2nd write raised: None
files after 2nd: 1 (swallowed FileExistsError -> no exception to caller)
```

**Rubric grading:** Phase 16 MEDIUM band has three sub-bullets: "(1) an
encoding or line-ending mismatch that is locale-dependent and undeclared; (2) a
format deviation (separator, BOM) that a downstream consumer tolerates but
should not; (3) an error file whose structure or fields do not match the
declared contract." None cover output-presence verification.

Phase 16 HIGH band: "mode='x' skipping an existing file so a stale result is
reported as fresh." The finding's impact states: "A re-run with
`check_done=False`, or two tasks colliding on a filename, silently produces no
CSV while the CLI prints `✅ ...`." This is the stale-result-as-fresh effect.
**Re-grade: MEDIUM → HIGH.**

**Load-bearing artefact:** Code (`core/utils.py::csv_to_file` and
`core/reports.py::TVMediaReport.extract_data`). PIPE-001 (Phase 04, CRITICAL)
covers the same `mode="x"` + `FileExistsError` mechanism from a pipeline-mechanics
angle; OUT-003 covers the Phase 16 output-presence verification gap. No conflict —
different phases, complementary angles.

**Recommendation carry-out:** Have `csv_to_file` return the `Path` of the
written file (or `None` on skip); in `extract_data`, check the return and set
`task.status = False` when the write was skipped. Targets `csv_to_file` and
`TVMediaReport.extract_data` exist and are stable. ✓

### OUT-004 — confirmed — [LOW] No declared or versioned CSV output-format contract

**Anchor resolved:** `core/utils.py::csv_to_file` — `encoding="utf-8-sig"` at line
187 (via `kwargs.pop`), `decimal=","` at line 193, `sep=";"` at line 194,
`mode="x"` at line 192, `index=False` at line 191.

**Claim tested:** Format parameters are hardcoded as magic values with no
constants module, YAML setting, or documentation declaring them as the output
contract. No format-specification file exists under `settings/` or `docs/`.

**Evidence (grep):** `rg "utf-8-sig|decimal|sep=" src/mko_get_mediascope_data/core/utils.py`
→ matches at lines 187, 193, 194 only. No matches in `docs/`, `settings/`, or
`AGENTS.md`.

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/utils.py:187:  encoding = kwargs.pop("encoding", "utf-8-sig")
src/mko_get_mediascope_data/core/utils.py:193:  decimal=",",
src/mko_get_mediascope_data/core/utils.py:194:  sep=";",
```

**Rubric grading:** Phase 16 LOW = "documentation or comment drift about output
format or schema, with no runtime consequence." The values are consistent and
produce correct output (BOM ✓, sep ✓, decimal ✓); only the contract is
undocumented. Matches. No re-grade.

**Load-bearing artefact:** Code (`core/utils.py::csv_to_file`).

**Recommendation carry-out:** Extract format parameters into a named, documented
module-level constant. Target `csv_to_file` exists and is stable. ✓

### OUT-005 — confirmed — [LOW] `interval` field missing from serialized error YAML

**Anchor resolved:** `core/tasks.py::TVTask.to_yaml` — `dump_settings` dict at
lines 36–43 (six fields: `name`, `report_type`, `report_subtype`, `target`,
`error`, `settings` — no `interval`). `TVTask.__init__` sets `self.interval = None`
at line 24. `DefaultTVTaskStrategy.generate` sets `task.interval = interval` at
reports.py:376; `TVRegTaskStrategy.generate` sets `task.interval = interval` at
reports.py:428.

**Claim tested:** `interval` is set on `TVTask` in both task strategies but is
not in the `dump_settings` dict, so it is missing from the serialized error YAML.

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/tasks.py:24:  self.interval = None
src/mko_get_mediascope_data/core/tasks.py:36:  dump_settings = {
src/mko_get_mediascope_data/core/tasks.py:37:      "name": self.name,
src/mko_get_mediascope_data/core/tasks.py:38:      "report_type": self.report_type,
src/mko_get_mediascope_data/core/tasks.py:39:      "report_subtype": self.report_subtype,
src/mko_get_mediascope_data/core/tasks.py:40:      "target": self.target,
src/mko_get_mediascope_data/core/tasks.py:41:      "error": self.error,
src/mko_get_mediascope_data/core/tasks.py:42:      "settings": self.settings,
(src/mko_get_mediascope_data/core/tasks.py — no "interval" key in dump_settings)
src/mko_get_mediascope_data/core/reports.py:376:  task.interval = interval
src/mko_get_mediascope_data/core/reports.py:428:  task.interval = interval
```

**Rubric grading:** Phase 16 LOW = "documentation or comment drift about output
format or schema, with no runtime consequence." The `interval` is recoverable
from `settings.date_filter` (which contains `[interval]`); it is not lost, only
inconvenient. No runtime consequence. Matches. No re-grade.

**Load-bearing artefact:** Code (`core/tasks.py::TVTask.to_yaml` and
`core/reports.py` task strategies).

**Recommendation carry-out:** Add `"interval": self.interval` to the
`dump_settings` dict in `TVTask.to_yaml`. Target exists and is stable. ✓

### OUT-006 — confirmed — [LOW] Error YAML keys emitted in alphabetical order

**Anchor resolved:** `core/tasks.py::TVTask.to_yaml` — `dump(...)` call at lines
46–51. `sort_keys` is not passed, so it defaults to `True`.

**Claim tested:** `yaml.dump` (and `yaml.safe_dump`) default `sort_keys=True`,
which sorts all mapping keys alphabetically. The `dump_settings` dict defines
fields in insertion order (name, report_type, report_subtype, target, error,
settings) but the rendered YAML emits them alphabetically: error, name,
report_subtype, report_type, settings, target.

**Evidence (runtime probe):**
```text
=== yaml.dump (default) ===
Top-level key order (default dump): ['error', 'name', 'settings']
=== with sort_keys=False ===
Top-level key order (sort_keys=False): ['name', 'error', 'settings']
```

**Evidence (source):**
```text
src/mko_get_mediascope_data/core/tasks.py:46:  dump(
src/mko_get_mediascope_data/core/tasks.py:47:      dump_settings,
src/mko_get_mediascope_data/core/tasks.py:48:      outfile,
src/mko_get_mediascope_data/core/tasks.py:49:      default_flow_style=False,
src/mko_get_mediascope_data/core/tasks.py:50:      allow_unicode=True,
src/mko_get_mediascope_data/core/tasks.py:51:      encoding=None,
(no sort_keys parameter -> defaults to True)
```

**Rubric grading:** Phase 16 LOW = "documentation or comment drift about output
format or schema, with no runtime consequence." Key ordering is cosmetic; no
consumer should depend on positional key order in YAML. Matches. No re-grade.

**Load-bearing artefact:** Code (`core/tasks.py::TVTask.to_yaml`).

**Recommendation carry-out:** Pass `sort_keys=False` to `dump`/`safe_dump`.
Target exists and is stable. ✓

---

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | Cross-reference inaccuracy: OUT-001 attributed to QLT-011 overlap; actual overlap is OUT-002 |

### VAL-001 — [LOW] Cross-reference inaccuracy in Phase 16 cross-phase analysis

**Template mandate:** Findings' cross-phase analysis must accurately attribute
overlaps. The Phase 16 template (`.kilo/commands/audit/phases/16-audit-rendered-surface.md,
line 26`) owns "rendered-output conformance"; Phase 10 owns "fixed-value enum
discipline"; Phase 04 owns "export mechanics, file-existence handling."

**Observed:** The findings' Fix Order section states: "OUT-001's `!!python/tuple`
serialization overlaps QLT-011 (Phase 10, MEDIUM — `*args/**kwargs` forwarding in
`csv_to_file`) only superficially (different module)."

**Issue:** OUT-001 concerns `tasks.py::TVTask.to_yaml` (YAML serialization). QLT-011
concerns `utils.py::csv_to_file` (`*args/**kwargs` forwarding to `to_csv`). These
are different modules and different subjects — there is no overlap at all. The
actual Phase 16 ↔ Phase 10 overlap is **OUT-002** (`csv_to_file` missing
`lineterminator`) with **QLT-011** (untyped `**kwargs` forwarded to `to_csv`):
both are in `utils.py::csv_to_file`, both concern CSV format parameters, and
QLT-011's fix ("replace `**kwargs` with the specific `to_csv` parameters needed
(`quoting`, `quotechar`, `lineterminator`, etc.)") would directly address
OUT-002.

**Evidence (source — QLT-011):**
```text
src/mko_get_mediascope_data/.ai/audit/10-code-quality/findings.md:55:  | QLT-011 | MEDIUM | `src/.../core/utils.py::csv_to_file` | `*args/**kwargs` forwarded to `DataFrame.to_csv` unchecked
src/mko_get_mediascope_data/.ai/audit/10-code-quality/findings.md:339: `**kwargs` with the specific `to_csv` parameters needed (`quoting`, `quotechar`, `lineterminator`, etc.)`
```

**Evidence (source — OUT-002):**
```text
src/mko_get_mediascope_data/core/utils.py:167:  def csv_to_file(...)
src/mko_get_mediascope_data/core/utils.py:188:      data_frame.to_csv(
(no lineterminator parameter)
```

**Impact:** The conclusion (no conflict, superficial overlap only) is correct.
The attribution is wrong — it points at OUT-001 when the real overlap is OUT-002.
This does not affect any finding's validity; it is a cross-reference drift.

**Disposition:** confirmed. Does not affect finding accuracy or verdict.

---

## Cross-Phase Analysis

| Cross-phase pair | Phase 04 finding | Phase 10 finding | Phase 16 finding | Shared root cause | Disposition |
|---|---|---|---|---|---|
| `csv_to_file` mode="x" + FileExistsError | PIPE-001 (CRITICAL) | — | OUT-003 (HIGH) | `mode="x"` + swallowed `FileExistsError` → no output, reported as success | cross-reference; Phase 04 = mechanics, Phase 16 = output presence; both confirmed; no conflict |
| `csv_to_file` CSV format parameters | — | QLT-011 (MEDIUM, `*args/**kwargs`) | OUT-002 (MEDIUM, no `lineterminator`), OUT-004 (LOW, no contract) | `csv_to_file` in `utils.py` with untyped/hardcoded `to_csv` parameters | cross-reference; both confirmed; QLT-011's fix addresses OUT-002; no conflict |
| `TVTask.to_yaml` error serialization | PIPE-007 (MEDIUM), PIPE-010 (LOW) | — | OUT-001 (MEDIUM), OUT-005 (LOW), OUT-006 (LOW) | `to_yaml` in `tasks.py` using `yaml.dump` without `safe_dump`, `sort_keys=False`, or `interval` field | cross-reference; Pipe 04 covers worker exception-bypass + dead path; Phase 16 covers format conformance; no conflict |

**Phase ownership check:** Phase 16 examines `csv_to_file` (CSV format, presence),
`TVTask.to_yaml` (error YAML format), and the worker/extract_data paths that
connect them — all rendered-output conformance concerns. Phase 04 owns the
write mechanics (`mode="x"`, `FileExistsError` swallowing, worker exception
isolation, `to_yaml` dead path). Phase 10 owns code-quality/type-checking
(`*args/**kwargs` forwarding, mypy overload mismatch). No Phase 16 finding
claims a concern owned exclusively by another phase and reaches an opposite
conclusion.

**Findings' claimed overlap vs. actual overlap:** The findings' Fix Order states
"OUT-001's `!!python/tuple` serialization overlaps QLT-011." As recorded in
VAL-001, this is inaccurate — OUT-001 (`to_yaml`, `tasks.py`) and QLT-011
(`csv_to_file`, `utils.py`) share no module or mechanism. The actual
Phase-16/Phase-10 overlap is OUT-002/OUT-004 (CSV format in `csv_to_file`)
with QLT-011 (also `csv_to_file`). See VAL-001.

---

## Rollout Safety Assessment

**Dependencies identified in source (validated):**
- OUT-001 + OUT-005 + OUT-006 are merge candidates — all modify `TVTask.to_yaml`
  (tasks.py:36–51). A single pass switching to `safe_dump`, adding
  `sort_keys=False`, and including `"interval": self.interval` in `dump_settings`
  resolves all three.
- OUT-003 (return signal from `csv_to_file`) is a prerequisite for PIPE-001's
  Phase 04 fix (marking skipped writes as failed). The findings' dependency
  claim is valid: `extract_data` must inspect the return value before
  `task.status` can be set to `False` on a skip.

**Rollout risk: test impact**
- `tests/test_utils.py::test_csv_to_file` writes once and asserts `len(files) == 1`
  — performs a single successful write, never a duplicate. Returns `None`
  currently; after OUT-003 fix it would return a `Path`, but the test does not
  inspect the return value. Must stay green. ✓
- `tests/test_reports.py` tests `DefaultTVTaskStrategy.generate` and
  `DictDataStrategy.prepare` only — does not exercise `to_yaml`,
  `extract_data`, or `wait_task`. No existing test blocks OUT fixes. ✓
- No test in `tests/` covers `TVTask.to_yaml` (grep for `to_yaml` in tests/ →
  0 matches). OUT-001/005/006 fixes will require new tests. ✓

**Rollout risk: behavioural changes**
- Fixing OUT-001 (switch to `safe_dump`) removes `!!python/tuple` tags from
  error YAML. Any consumer using `yaml.full_load`/`yaml.unsafe_load` would still
  parse the output (safe_dump output is a strict subset). Safe. ✓
- Fixing OUT-002 (add `lineterminator='\n'`) changes line endings from CRLF to
  LF on Windows. This is a behavioural change for Windows consumers that split
  on `\r\n`. The findings note this as the intended correction. ✓
- Fixing OUT-003 (return `Path | None`) changes `csv_to_file`'s return type
  from `None` to `Path | None`. `extract_data` (reports.py:325–332) discards the
  return value today — backward compatible at the call site. ✓
- Fixing OUT-005 (add `interval` to `dump_settings`) is additive — new top-level
  field in error YAML. No breakage. ✓
- Fixing OUT-006 (add `sort_keys=False`) changes key order in error YAML from
  alphabetical to insertion order. Only affects consumers that parse by
  positional key order in YAML (unusual). ✓

All six recommendations name concrete, stable targets that were verified to
exist in the `d093d9c` tree. No fix has been applied — all findings remain in
the current tree unmodified.

## Evidence Index

| Probe | Result | Supports |
|---|---|---|
| `yaml.dump` with tuple in `date_filter` | emits `!!python/tuple`; `safe_load` → `ConstructorError` | OUT-001 |
| `safe_dump` with tuple in `date_filter` | no `!!python/tuple`; `safe_load` succeeds | OUT-001 (fix verification) |
| `DataFrame.to_csv` without `lineterminator` (Windows) | CRLF present | OUT-002 |
| `DataFrame.to_csv` with `lineterminator='\n'` | no CRLF | OUT-002 (fix verification) |
| `inspect.signature(to_csv).lineterminator` | default `None` | OUT-002 |
| pandas version | `3.0.0` | OUT-002 |
| PyYAML version | `6.0.3`; `dump` default `sort_keys=True` | OUT-006 |
| `dump(..., sort_keys=False)` | preserves insertion order | OUT-006 (fix verification) |
| `csv_to_file` source read (lines 167–200) | no `return` statement; `mode="x"`; no `lineterminator` | OUT-002, OUT-003, OUT-004 |
| `extract_data` source read (lines 325–332) | `to_thread` return value discarded | OUT-003 |
| `worker` source read (lines 134–153) | `status is True` → "ready" after `extract_data` | OUT-003 |
| `dump_settings` source read (lines 36–43) | 6 fields, no `interval` | OUT-005 |
| `TVTask.__init__` (line 24) + strategies (376, 428) | `interval` set but not serialized | OUT-005 |
| `dump` call (lines 46–51) | no `sort_keys` parameter | OUT-006 |
| grep `to_yaml` in `tests/` | 0 matches | OUT-001, OUT-005, OUT-006 |
| grep `lineterminator` in `tests/` | 0 matches | OUT-002 |
| grep `utf-8-sig\|decimal\|sep=` in `docs/` + `settings/` | 0 matches (except log_config `encoding`) | OUT-004 |
| Phase 04 REPRO C (`csv_to_file` duplicate write) | 2nd write returns `None`, 1 file remains | OUT-003 (source cross-check) |
| `git rev-parse --short HEAD` | `d093d9c` | matches `code_ref` |
| `PeriodModel.date_filter` (models.py:69) | `list[date] \| None` → converted to tuple in `get_period` | OUT-001 (tuple source) |
