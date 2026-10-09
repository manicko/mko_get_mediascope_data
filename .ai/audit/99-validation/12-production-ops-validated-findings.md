---
phase: "12"
phase_name: "production-ops"
id_prefix: "OPS"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/12-audit-production-ops.md#severity-taxonomy"
code_ref: "d093d9c"
validator: "99-validate"
---

# Audit Findings — Validation Report: Production Operations, Security & Observability

## Summary

Six findings (2 CRITICAL, 1 HIGH, 3 MEDIUM). All six are **confirmed** — every anchor resolves to
the cited location, every disambiguator exists at its site, and every claim reproduces under
re-execution of the controls (`mypy`/`ruff`/`pytest`/`uv build`) and direct source inspection,
corroborated by sibling phases (TST-002/004/005, ASY-001/007, PIPE-002).

**6 confirmed, 0 rejected, 0 re-graded, 0 re-typed, 0 merged, 0 unsettled.**
One validation-level finding: VAL-001 (LOW) — stale namespace assertion in the phase-12 instruction.

## Scope

**Examined:** `pyproject.toml` (`[tool.mypy]`, `[tool.setuptools]`, `[project]`), `MANIFEST.in`,
`README.md`, `core/network.py::NetworkClient.call`, `core/reports.py::MediaReport.worker`/
`processing_tasks`, `core/services.py` (module-scope singleton + `print()`), `core/utils.py::yaml_to_dict`,
`core/paths.py::AppPaths`, `core/init_service.py::_copy_templates`, `settings/log_config.yaml`,
`__init__.py`, `__main__.py`, `app.py` (`run` signature), `tests/test_network.py`.
**Controls re-run:** `uv run mypy src/` → 61 errors / 7 files / 14 checked; `uv run ruff check src/ tests/` → green; `uv run pytest` → 12 passed; `uv build` → wheel namelist omits `settings/`.
**Cross-phase corroboration:** `.ai/audit/11-test-coverage/findings.md` (TST-002/004/005),
`.ai/audit/03-async-concurrency` + its validated report (ASY-001/007), `.ai/audit/04-data-pipeline/findings.md` (PIPE-002), `.ai/audit/09-external-api/findings.md` (EXT-007).
**Not verified:** live Mediascope API run for OPS-004's runtime `log.log` repro (no credentials); a
fully clean fresh-venv install (build is deterministic from committed config — wheel namelist reproduced).
OPS-002's crash mechanism reproduced directly: `yaml_to_dict(missing) → None → LoggingSettings(**None) → TypeError`.

## Findings

| ID | Disposition | Severity | Type | Anchor resolves | Disambiguator exists |
|---|---|---|---|---|---|
| OPS-001 | confirmed | CRITICAL | SPEC-DEVIATION | `pyproject.toml::tool.mypy` | `[tool.mypy]` ✓ |
| OPS-002 | confirmed | CRITICAL | SPEC-DEVIATION | `MANIFEST.in::"recursive-include mko_get_mediascope_data *.json *.yaml"` | `recursive-include` ✓ |
| OPS-004 | confirmed | HIGH | SPEC-DEVIATION | `core/network.py::NetworkClient.call` | `"Network retry limit exceeded"` ✓ |
| OPS-003 | confirmed | MEDIUM | SPEC-DEVIATION | `settings/log_config.yaml::loggers` | `loggers` ✓ |
| OPS-005 | confirmed | MEDIUM | DOC-UPDATE | `README.md::"uv run -m mko_get_medioscope_data run settings/reports/nat_tv_brands_last.yaml"` | `nat_tv_brands_last.yaml` ✓ |
| OPS-006 | confirmed | MEDIUM | BEST-PRACTICE | `pyproject.toml::project.version` | `version = "1.0.8"` ✓ |

Index order follows severity (CRITICAL→HIGH→MEDIUM) per the audited index; ID numbers preserved.

### OPS-001 — confirmed — [CRITICAL] No CI/CD gate; release ships manually with mypy RED (61 errors)

**Anchor resolved:** `pyproject.toml::tool.mypy` — `[tool.mypy]` at `pyproject.toml:135-145`.
**Claim tested:** (a) No CI exists: `.github/`, `Makefile`, `Makefile.ps1`, `.pre-commit-config.yaml`
are absent both tracked (`git ls-files`) and on disk (`Test-Path` → False). (b) mypy is documented
(`AGENTS.md`: "Typecheck: uv run mypy <path>") and configured (`[tool.mypy]`) but RED.
(c) Release is manual `uv build` + `twine upload` (dev dep `twine>=5.0.0`). `uv run mypy src/` →
"Found 61 errors in 7 files (checked 14 source files)".

**Disambiguator:** `[tool.mypy]` block present. ✓
**Evidence (controls re-executed):**
```text
$ uv run mypy src/
Found 61 errors in 7 files (checked 14 source files)   # tests excluded (src/ only)
$ git ls-files | findstr /i /r 'github\|makefile\|pre-commit'   # (no matches)
$ uv run ruff check src/ tests/
All checks passed!
$ uv run pytest
12 passed in 19.00s
```
Cross-confirmed: Phase 11 TST-004 ("no merge gate loaded") and TST-005 ("mypy red, 61 in 7 src files, 0 tests").
**Rubric grading:** Phase 12 CRITICAL = "a control believed to be active that is not, on the path
that decides whether a change ships or a release publishes." mypy is believed active (documented +
configured) but RED, and no CI enforces it on the publish path. CRITICAL matches. No re-grade.
**Load-bearing artefact:** Absence of CI config (no file) + `pyproject.toml::tool.mypy`.
**Recommendation carry-out:** "Add `.github/workflows/ci.yml` + `.pre-commit-config.yaml` gating
ruff/mypy/pytest and blocking `twine upload`." Target is to-be-created (actionable). Dependency: the
gate is only meaningful once mypy is green or scoped to the still-green set — else it blocks the
pipeline it intends to protect.
### OPS-003 — confirmed — [MEDIUM] Configured named loggers are dead; services.py progress bypasses the log file via print()

**Anchor resolved:** `settings/log_config.yaml::loggers` — `loggers` block (`log_config.yaml:44-61`), declaring `__main__`, `tests`, `app`.
**Claim tested:** (a) No module registers a logger named `__main__`, `tests`, or `app`.
`getLogger` in `src/mko_get_medioscope_data` appears only at `tasks.py:7`, `utils.py:15`,
`network.py:21`, `reports.py:24` — all `getLogger(__name__)` → real names `mko_get_medioscope_data.core.{tasks,utils,network,reports}`, none of which match. `app.py`/`services.py`/`__main__.py`
define no logger (`__main__.py` is `from .app import app`). So the three named-logger configs never fire;
real loggers fall through to `root` (`console` + `rotating_file`). (b) `AppService.run_report` emits
lifecycle markers via `print()` at `services.py:117/124/154` → stdout only, never the `rotating_file` (`log.log`).

**Disambiguator:** `loggers` — present at `log_config.yaml:44`. ✓
**Evidence:**
```text
$ grep -rn "getLogger" src/mko_get_medioscope_data
  core/tasks.py:7    logger = logging.getLogger(__name__)
  core/utils.py:15   logger = logging.getLogger(__name__)
  core/network.py:21 logger = logging.getLogger(__name__)
  core/reports.py:24 logger = logging.getLogger(__name__)
  # services.py & app.py: no getLogger
services.py:117: print(...)  # "Обработка стартована"
services.py:124: print(...)  # "Отчёт {idx} → ..."
services.py:154: print(...)  # "Обработка завершена"
```
**Rubric grading:** Phase 12 MEDIUM = "degraded operability with a workaround." Lifecycle progress is
observable on stdout but absent from the configured `log.log`; the named-logger config is dead weight.
MEDIUM matches. No re-grade. (Mirrors Phase 01 ENT-005 / Phase 02 CFG-010.)
**Type:** SPEC-DEVIATION — `print()` in `core/` violates AGENTS.md rule #12 ("No print() Statements — Use logging.getLogger(__name__) in core/ modules"); named-logger config unaligned to code.
**Load-bearing artefact:** `settings/log_config.yaml::loggers`; `core/services.py` (`print()` 117/124/154).
**Recommendation carry-out:** "Align `log_config.yaml` named loggers to the real module logger names (or rely on root); replace `print()` in `core/services.py` with `logger.info(...)`." Targets exist and are stable. ✓
Rollout: no shipped test asserts stdout markers; `test_services.py` mocks `create_report`.

### OPS-004 — confirmed — [HIGH] Retry exhaustion & worker-task failure are raised/swallowed without a terminal log or counter

**Anchor resolved:** `core/network.py::NetworkClient.call` — method `call` at `network.py:59`; terminal raise at `network.py:126`.
**Claim tested:** (a) After the retry `for` loop, `network.py:126` `raise RuntimeError("Network retry limit exceeded")` is preceded by **no** `logger.error`/`logger.exception` — only per-attempt `logger.warning` inside the loop (`network.py:96-99`). (b) `MediaReport.worker` (`reports.py:134-153`) has no `try/except/finally`; `self.task_queue.task_done()` is at `reports.py:153`, reached only on the happy path. (c) `processing_tasks` (`reports.py:155-162`) cancels via `[t.cancel() for t in task_group]` (line 162) — never `await`ed/`gather`ed, so un-retrieved task exceptions surface only via asyncio's default stderr handler.
**Disambiguator:** `"Network retry limit exceeded"` — present at `network.py:126`. ✓
**Evidence (source inspection):**
```text
network.py:126: raise RuntimeError("Network retry limit exceeded")   # no logger.error before
reports.py:143:     await self.send_task(item)       # raises -> skips task_done (153)
reports.py:147:     await self.extract_data(item)    # raises -> skips task_done (153)
reports.py:153:     self.task_queue.task_done()      # happy path only
reports.py:162: [t.cancel() for t in task_group]   # no await/gather
```
Cross-confirmed: Phase 03 ASY-007 (network.py:126 generic `RuntimeError`), ASY-001 (worker no `try/except` → `task_done` skipped), Phase 04 PIPE-002 ("identical root cause" at `reports.py:141-153`). `tests/test_network.py:15` asserts `RuntimeError` is raised (with `max_attempts=3`, explaining the "attempt 3/3" repro sample) — a `logger.error` before the raise leaves that assertion green.
**Rubric grading:** Phase 12 HIGH = "a failure state with no counter and no log, so a quiet tool is indistinguishable from a working one." Exhaustion is raised, not logged; no counter. HIGH matches. No re-grade.
**Type:** SPEC-DEVIATION — the terminal-failure path violates phase 12 §5's observability expectation that a failure state carry a counter or log entry.
**Load-bearing artefact:** `network.py::NetworkClient.call` (line 126), `reports.py::MediaReport.worker` (134-153), `reports.py::MediaReport.processing_tasks` (162).
**Recommendation carry-out:** "log ERROR before raising; wrap `worker` body in `try/except/finally` logging + guaranteeing `task_done()`; add counters (EXT-007)." Targets exist and are stable. ✓ The `task_done` fix overlaps ASY-001/PIPE-002 — ship together.

#### OPS-005 — confirmed — [MEDIUM] README runbook invocation is inaccurate and non-reproducible from a clean checkout

**Anchor resolved:** `README.md::"uv run -m mko_get_medioscope_data run settings/reports/nat_tv_brands_last.yaml"` — `README.md:21` (Quick Start) and `README.md:73` (Examples).
**Claim tested:** (a) The runbook path `settings/reports/nat_tv_brands_last.yaml` is repo-root-relative; no `settings/` exists at repo root (reports live under `src/mko_get_medioscope_data/settings/reports/nat_tv_brands_last.yaml`, confirmed in `git ls-files`). (b) `run`'s Typer signature is a single `Path` arg with `exists=True, dir_okay=False` (`app.py:40-43`) — not "one or more files" as `README.md:67` claims ("Run one or more reports from YAML file(s)"). `exists=True` rejects the documented (non-existent) path.
**Disambiguator:** `nat_tv_brands_last.yaml` — present in `README.md:21/73`. ✓
**Evidence:**
```text
$ Test-Path "settings/reports/nat_tv_brands_last.yaml"   # from repo root
False
$ uv run -m mko_get_medioscope_data run --help
Arguments: *    report      FILE  ... [required]   # single file, not "one or more"
```
(app.py:42: `typer.Argument(exists=True, dir_okay=False, ...)`.)
**Rubric grading:** Phase 12 MEDIUM = "a documented procedure or claim that is inaccurate, but whose consequence an operator can work around." An operator can supply the absolute user-config path after `init`. MEDIUM matches. No re-grade.
**Type:** DOC-UPDATE — the code signature is correct; the README path/claim is outdated.
**Load-bearing artefact:** `README.md` (lines 21, 67, 73).
**Recommendation carry-out:** "README should use the absolute user-config path and state `run` takes one file; add a CI/doc check invoking the documented command after `init`." Target (`README.md`) exists and is editable. ✓

#### OPS-006 — confirmed — [MEDIUM] No version↔commit traceability; deps unpinned in the published artifact

**Anchor resolved:** `pyproject.toml::project.version` — `pyproject.toml:7` (`version = "1.0.8"`); `project.dynamic` absent (`pyproject.toml:6-8`).
**Claim tested:** (a) `version = "1.0.8"` hardcoded; `project.get('dynamic')` is `None` (no VCS versioning). (b) `git tag` empty; `git describe --tags --always` → `d093d9c` (commit hash, no tag). (c) Published `Requires-Dist` are lower-bound `>=` only (`pyproject.toml:16-28`); `uv.lock` pins transitive deps but is dev-only and not shipped (wheel namelist has no `uv.lock`).
**Disambiguator:** `version = "1.0.8"` — present at `pyproject.toml:7`. ✓
**Evidence:**
```text
$ git tag                          # (empty)
$ git describe --tags --always
d093d9c
$ uv run python - <<'PY'
import tomllib
with open('pyproject.toml','rb') as f: d = tomllib.load(f)
print('version =', d['project']['version'])
print('dynamic =', d['project'].get('dynamic'))
PY
version = 1.0.8
dynamic = None
```
**Rubric grading:** Phase 12 MEDIUM = "degraded operability with a workaround." Rollback to a pinned wheel fetches an immutable artifact, but identity/traceability is unverifiable and end-user `uv pip install` can pull newer transitive versions than the maintainer tested. MEDIUM matches. No re-grade.
**Type:** BEST-PRACTICE — no written spec mandates VCS versioning or runtime pins; release-process improvement.
**Load-bearing artefact:** `pyproject.toml` (`project.version`, `[project] dependencies`).
**Recommendation carry-out:** "Adopt dynamic versioning from git tags (setuptools-scm/hatch-vcs) and tag releases; pin critical runtime deps or ship a constraints file; add a release job that tags then publishes." Target (`pyproject.toml`) exists and is stable. ✓

## Validation-Level Findings (VAL-)

| ID | Severity | Disposition | Description |
|---|---|---|---|
| VAL-001 | LOW | confirmed | Phase-12 instruction asserts `OPS-` is "already in use in shipped source, tests, and workflows from a prior cycle"; `git grep 'OPS-'` shows zero such usage — stale namespace assertion |

### VAL-001 — [LOW] Phase-12 instruction's `OPS-` namespace assertion is stale

**Template mandate:** `99-audit-validate.md` block 7 — "render a ruling rather than only counting collisions."
**Observed:** `.kilo/commands/audit/phases/12-audit-production-ops.md:169-171` states the `OPS-` prefix is "already in use in shipped source, tests, and workflows from a prior cycle." A repo-wide `git grep 'OPS-'` returns **zero** matches in tracked source/tests/workflows — the only hit is the instruction's own line 169. (Prior prefixes per the audited report: `ENT`/`CFG`/`ASY`/`PIPE`/`TPL`/`EXT`/`QLT`/`TST`/`VAL`.)
**Ruling:** `OPS-` is the single declared namespace; no collision exists; it is safe to mint — as the audited report did. The instruction's "already in use from a prior cycle" assertion is drift to retire.
**Impact:** None on the audited findings (the auditor minted `OPS-` safely and created no second namespace).
**Cause:** Stale instruction text predating the current tree.
**Disposition:** confirmed.

## Cross-Phase Analysis

| Concern | Sibling finding (phase) | Phase 12 finding | Shared root cause | Disposition |
|---|---|---|---|---|
| import-time singleton + dictConfig | ENT-002 (01) | OPS-002 | `services.py:163` `app_service` at module scope | cross-reference; both confirmed; 01=bootstrap, 12=packaging crash |
| None-swallow → TypeError | ENT-003 (01) | OPS-002 | `utils.py:343` `yaml_to_dict` returns None | cross-reference; both confirmed; distinct framing |
| print() in core/ bypasses logging | ENT-005 (01) / CFG-010 (02) | OPS-003 | `services.py:117/124/154` | cross-reference; both confirmed; OPS-003 scoped to observability; disclosed as "mirrors" |
| worker no try/except → task_done skipped | ASY-001 (03) / PIPE-002 (04) | OPS-004 (supporting) | `reports.py:134-153` worker | cross-reference; both confirmed; OPS-004 primary claim is network.py:126 |
| no retry-exhaustion terminal log/counter | EXT-007 (09) | OPS-004 | `NetworkClient.call` exhaustion | cross-reference; both confirmed; 09=metrics, 12=terminal log |
| no release/merge gate | TST-004 (11) | OPS-001 | absence of CI/Makefile/pre-commit | cross-reference; both confirmed; 11=tests, 12=release/types |
| mypy red (61) | TST-005 (11) | OPS-001 | `uv run mypy src/` | cross-reference; both confirmed |
| non-hermetic pytest | TST-002 (11) | OPS-001 (support) | `services.py:163` singleton at import | cross-reference; both confirmed |
| credential name mismatch | CFG-003 (02) | OPS-002 (Appendix E) | `mediascope_RENAME_.json` vs `mediascope.json` | cross-reference; consistent |
| non-atomic init copy | CFG-008 (02) | OPS-002 (Appendix E) | `init_service.py:23` | cross-reference; consistent |
| empty logger.py placeholder | QLT-017 (10) | OPS-003 (related) | `core/logger.py` | cross-reference; consistent |
| README public-API import fails | ENT-004 (01) | OPS-005 (related) | `__init__.py` empty | cross-reference; consistent |

**Ownership check:** Phase 12 owns CI/CD gates (§2/§3), packaging/artifact identity (§4), observability/logging (§5), runbooks (§6), release/rollback (§7). All six OPS findings fall squarely within these domains. Every cited sibling finding is independently confirmed and reaches the **same** conclusion — no contradictory conclusion about any shared subject. The phase-12 author correctly scoped each finding to phase-12 ownership and cited overlaps as related, not replacements.

**No cross-phase conflicts detected.** No finding was withdrawn, downgraded, or re-graded due to cross-phase interaction. One merge candidate: OPS-003's `print()` claim (services.py:117/124/154) overlaps ENT-005/CFG-010 — disclosed by the author, so not hidden, but a candidate for consolidation.

## Rollout Safety Assessment

**Dependencies identified (validated from source):**
- OPS-002 (packaging) and OPS-001 (CI gate) are independent but reinforcing: a CI wheel-content assertion (OPS-002 Verify) + the CI gate (OPS-001) would catch the empty `settings/` before publish.
- OPS-004's worker `task_done` fix overlaps ASY-001/PIPE-002 (same `try/finally` on `worker`); must ship together or the `queue.join()` hang persists.
- OPS-003, OPS-005, OPS-006 are independent of the extraction math (phases 01-11) and of each other.

**Fix ordering:** OPS-002 + OPS-001 first (restore a functional, gated artifact); OPS-003/004/005/006 in parallel.

**Rollout risk / test contracts preserved:**
- OPS-002 packaging additions are additive to the wheel; keep `uv run pytest` green (12 passing).
- OPS-003 `print()`→`logger`: no shipped test asserts stdout markers; `test_services.py` mocks `create_report`.
- OPS-004 logging-before-raise: `tests/test_network.py:15` asserts `RuntimeError` is raised — a `logger.error` before the raise preserves that assertion.
- OPS-005 README: no test contract.
- OPS-006 versioning: don't break `test_models`/`test_utils` (immutable identifiers); adopt `setuptools-scm` so `pyproject.toml` name/version still resolve.
- No fix alters report-extraction arithmetic (phases 01-11).

**Backward compatibility:** OPS-002 is purely additive packaging; OPS-004 preserves the `RuntimeError` API (logs before raising); OPS-003 preserves stdout output (adds file logging).

## Evidence Index

| Evidence reference | Source |
|---|---|
| `pyproject.toml:7,16-28,52-56,135-145,176` | version=`1.0.8`, no `dynamic`; deps `>=`; src-layout + `include-package-data`; `[tool.mypy]`; `twine>=5.0.0` dev |
| `MANIFEST.in:4` | `recursive-include mko_get_medioscope_data *.json *.yaml` (wrong dir; build warns) |
| `core/services.py:117,124,154,163,166` | `print()` lifecycle markers; module-scope singleton + `dictConfig` |
| `core/network.py:96,126` | per-attempt `logger.warning`; unlogged `raise RuntimeError("Network retry limit exceeded")` |
| `core/reports.py:134-153,155-162` | `worker` no try/except; `task_done` at 153 (happy path); `t.cancel()` no gather at 162 |
| `core/utils.py:343,358` | `yaml_to_dict` returns None on FileNotFoundError (mypy: `Missing return statement`) |
| `core/paths.py:42-59` | `app_settings_dir`/`user_settings_dir`; `connection_config` → `connections/mediascope.json` |
| `core/init_service.py:23` | `_copy_templates` skip-existing (idempotent, non-atomic) |
| `settings/log_config.yaml:44-61,62-65` | dead named loggers `__main__`/`tests`/`app`; root `console`+`rotating_file` |
| `app.py:40-43` | `run` single `Path` arg, `exists=True, dir_okay=False` |
| `__init__.py` / `__main__.py` | empty / `from .app import app` (ENT-004 cross-ref) |
| `tests/test_network.py:15` | asserts `RuntimeError("Network retry limit exceeded")`, `max_attempts=3` |
| `uv run mypy src/` | `Found 61 errors in 7 files (checked 14 source files)` |
| `uv run ruff check src/ tests/` | `All checks passed!` |
| `uv run pytest` | `12 passed in 19.00s` |
| `uv build` | wheel namelist omits `settings/`; `warning: no files found matching '*.yaml'` |
| `yaml_to_dict(missing)→None→TypeError` | reproduced in-situ (matches findings Appendix C) |
| `git tag` / `git describe --tags --always` | empty / `d093d9c` |
| `git ls-files` / `Test-Path` (CI, Makefile, pre-commit) | none committed; none on disk |
| `git grep 'OPS-'` | only phase instruction line 169 |
| `grep -rn getLogger src/mko_get_medioscope_data` | `utils.py:15`, `tasks.py:7`, `network.py:21`, `reports.py:24` only |
| `.ai/audit/11-test-coverage/findings.md` | TST-002/004/005 corroborate OPS-001/002 |
| `.ai/audit/03-async-concurrency` (validated) | ASY-001/007 confirm OPS-004 mechanism |
| `.ai/audit/04-data-pipeline/findings.md` | PIPE-002 identical worker defect |
| `.ai/audit/09-external-api/findings.md` | EXT-007 metrics gap (related) |
| `12-audit-production-ops.md` | phase-12 rubric + scope + namespace assertion |
| `99-audit-validate.md` | validation methodology (blocks 1-10) |
| `templates/audit-findings.md` | report template |

## Disposition Tally

| Disposition | Count |
|---|---|
| confirmed | 6 |
| re-typed | 0 |
| re-graded | 0 |
| merged | 0 |
| not substantiated | 0 |
| unsettled | 0 |
| **Total** | **6** |

**Validation-level findings (VAL-): 1** (VAL-001 — LOW; stale phase-12 namespace assertion)
