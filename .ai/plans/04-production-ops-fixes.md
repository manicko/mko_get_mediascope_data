# Execution Plan — Phase 12 Production Ops Remaining Findings

## Source

- **Plan file:** `.ai/audit/99-validation/12-production-ops-validated-findings.md`
- **Code context:** `.ai/audit/12-production-ops/code_context.md` (Auditor output, 2026-10-19)
- **Architecture reference:** `docs/00-overview/`, `AGENTS.md`

## Current State (HEAD `7709d7e`)

Commit 7709d7e ("fix(prod-ops): resolve OPS-002 through OPS-006") resolved
OPS-002, OPS-003, OPS-004, and partially OPS-005 and OPS-006. The JSON5
migration is complete; CI config exists; src/ mypy is green; 109 tests pass.

**Remaining open work:**

| Finding | Severity | Status at HEAD | Action Needed |
|---------|----------|----------------|---------------|
| OPS-001 | CRITICAL | PARTIALLY RESOLVED | Fix 62 mypy errors in test files; CI `mypy src tests` is RED |
| OPS-005 | MEDIUM | PARTIALLY RESOLVED | Fix "one or more reports" claim; fix relative path examples in README |
| OPS-006 | MEDIUM | PARTIALLY RESOLVED | Add VCS-based versioning (setuptools-scm); add git tags; pin deps |
| VAL-001 | LOW | NOT APPLICABLE | Instruction file drift only — no code change |

---

## Work Blocks

### Block A — OPS-001: Fix CI mypy gate on test type annotations (CRITICAL)

**Goal:** Make `uv run mypy src tests` green (currently 62 errors, all in test files).

**Current state:**
- `mypy src/` → 0 errors (production code is clean)
- `mypy src tests/` → 62 errors:
  - `tests/test_models.py` — 1 error (LastTimeModel | None attr access)
  - `tests/test_reports_lifecycle.py` — 37 errors (str typed as Path; type vs instance passed to strategies; frequency_dist_conditions kwarg on Data model)
  - `tests/test_services.py` — 24 errors (str typed as Path; dict invariant violations in **kwargs; missing attributes)

**Dependencies:** None. Independent (src/ is already green).

**Approach:**
- Fix type annotations in test files to satisfy mypy strict mode
- Per AGENTS.md rule #2: "If tests conflict with architecture or business logic, fix or remove the tests. Never distort production code for tests."
- Production code must NOT be changed to accommodate test type errors

**Required tests:**
- Existing tests must remain green (109 passing)
- mypy src tests/ must return 0 errors

**Risk:** Medium. 62 test-only type fixes; risk of changing test behavior. Must verify pytest still passes.

**Sequence:** Auditor (inspect test files) → Implementor (fix types) → validate

---

### Block B — OPS-005: Fix README runbook accuracy (MEDIUM)

**Goal:** Fix remaining README inaccuracies after JSON5 migration.

**Current state (already resolved by commit 7709d7e):**
- `.yaml` → `.json5` in all README paths ✓
- `list_reports` → `list-reports` ✓
- `--no-verbose` removed ✓
- PyPI → git install instructions ✓
- Migration note documented ✓

**Remaining open sub-claims:**
1. README line 74: "Run one or more reports from JSON5 file(s)" — but `run` takes a single `Path` argument (not variadic). Should be "Run a report from a JSON5 file".
2. README lines 22, 33, 80: `run reports/nat_tv_brands_last.json5` and `run reports/nat_tv_tvreport.json5` — relative paths that don't exist at repo root. After `init`, reports are in `~/.config/mko_get_mediascope_data/settings/reports/`. The Public API section (line 92) still uses `settings/reports/nat_tv_brands_last.json5`.

**Dependencies:** None. Independent.

**Approach:**
- Fix `run` description: "Run a report from a JSON5 file"
- Fix relative paths: clarify that paths are absolute user-config paths (e.g., `~/.config/mko_get_mediascope_data/settings/reports/nat_tv_brands_last.json5`)

**Required tests:**
- No test contract changes needed
- Verify CLI `--help` output matches README

**Risk:** Low. Documentation only.

**Sequence:** Implementor (fix README) → validate

---

### Block C — OPS-006: VCS-based versioning + dependency pinning (MEDIUM)

**Goal:** Implement version traceability from git tags and pin critical runtime dependencies.

**Current state (already resolved by commit 7709d7e):**
- `version = "1.0.8"` → `dynamic = ["version"]` in pyproject.toml:7
- Version read from `__init__.py:__version__`

**Remaining open sub-claims:**
1. Version is dynamic but NOT VCS-based — still hardcoded string in `__init__.py:3`. No setuptools-scm or hatch-vcs.
2. No git tags exist (`git tag` → empty; `git describe --tags --always` → `7709d7e`).
3. Dependencies remain unpinned — all `>=` lower bounds only (pyproject.toml:15-28). `uv.lock` pins transitive deps but is dev-only, not in wheel.

**Dependencies:** None (independent of extraction math).

**Approach (to be researched):**
- Evaluate setuptools-scm vs hatch-vcs vs manual tag-based approach
- Per D-19: no PyPI publication; intended path is `pip install <github_url>`
- Add a CI release job that creates git tags on merge to main

**Required tests:**
- `test_models`/`test_utils` must remain green (immutable identifiers)
- Verify `__version__` still resolves at import time

**Risk:** Medium. Versioning changes affect packaging; must ensure `uv build` still works.

**Sequence:** Researcher (evaluate approaches) → Implementor (apply chosen approach) → validate

---

## Implementation Sequence

```
Block A (OPS-001: fix test mypy errors)   → commit 1
Block B (OPS-005: fix README)             → commit 2
Block C (OPS-006: VCS versioning + pins)  → commit 3
Documentation (Doc-specialist)            → commit 4
Final validation (Validator)              → commit 5 (if needed)
```

Blocks B and C are independent of each other and of Block A.
Block A is CRITICAL and should be done first (it blocks the CI gate).

## Validation Strategy

After each block:
```bash
uv run ruff check src tests
uv run mypy src tests
uv run pytest
git build                          # (Block C only)
```

All existing tests must remain green. mypy src tests/ must be 0 errors after Block A.

## Do NOT Change

- Production code in `src/` must not be modified to satisfy test type errors (AGENTS.md rule #2)
- The `max_retries` / retry semantics in `core/network.py` (already resolved, OPS-004)
- The report extraction arithmetic (phases 01-11)
- D-19 decision: no PyPI publication
