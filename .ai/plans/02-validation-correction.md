---
execution_plan_id: "02-validation-correction"
source_audit: ".ai/audit/99-validation/01-entry-architecture-validated-findings.md"
source_plan: ".ai/plans/01-entry-architecture-fixes.md"
phase: "01-entry-architecture"
status: "completed"
---

# Execution Plan — Audit Validation Corpus Correction for ENT-006

## 1. Plan Overview and Scope

### Purpose

The audit validation report at `.ai/audit/99-validation/01-entry-architecture-validated-findings.md`
identifies ENT-006 as the one confirmed finding requiring action: `app_config` is a
`@cached_property` on the `AppService` singleton with no invalidation hook, causing stale
config in long-lived processes (Jupyter/Airflow). The recommendation is to "add a
`reload_config()` method to `AppService`."

### What the Auditor Found

**ENT-006's code remediation is already complete at HEAD.** Commit `755523e`
(*feat(core): add reload_config() to AppService for dynamic config reload (ENT-006)*)
added `reload_config()` to `AppService` at `core/services.py:66-76`, with 5 dedicated
tests in `tests/test_services.py:102-145`. `git merge-base --is-ancestor 755523e HEAD`
confirms the commit is in HEAD's ancestry. All 23 tests pass.

All six other findings (ENT-001 through ENT-005, ENT-007) were also already remediated
at HEAD prior to this workflow:

| Finding | Remediation Commit | Status |
|---------|-------------------|--------|
| ENT-001 | `706eed0` | Fixed — `sys.stdout.reconfigure` moved into `run` command body |
| ENT-002 | `706eed0` | Fixed — singleton deferred behind `get_app_service()` factory |
| ENT-003 | `706eed0` | Fixed — `yaml_to_dict` raises instead of returning `None` |
| ENT-004 | `934cf9c0` | Fixed — PEP 562 lazy `__getattr__` re-exports in `__init__.py` |
| ENT-005 | `606c899` | Fixed — `print()` replaced with `logger.info()` in `core/` |
| ENT-006 | `755523e` | **Fixed** — `reload_config()` method added to `AppService` |
| ENT-007 | `b04d177` | Fixed — Russian strings translated to English |

### Scope

**In scope:**
- `.ai/audit/99-validation/01-entry-architecture-validated-findings.md` — update ENT-006
  disposition from "confirmed" to "fixed at HEAD"
- `.ai/plans/01-entry-architecture-fixes.md` — update `status` from `draft` to `completed`

**Out of scope:**
- No production code changes — all fixes are already implemented and tested
- No new features — `reload_config()` is not added to `__all__` (the original finding
  recommendation was a method on `AppService`, which exists)
- `docs/00-overview/current-behavior.md` — already references `reload_config()` correctly
  at line 245; no update needed

### Key constraints

1. **No code changes** — the implementation is already complete and tests pass
2. **Minimal documentation correction** — only the stale validation report and plan file
3. **Never change existing audit phase files** (raw findings in `.ai/audit/01-entry-architecture/`)
   — only the validation report in `99-validation/` and the plan file are in scope

---

## 2. Discrepancy Analysis

### 2.1 The audit validation report is stale for ENT-006

The validation report (written at HEAD `8ed385f`/`e8fd1867`) claims:

> "**Asserted cause tested separately:** `@cached_property` has no invalidation hook. The
> singleton is process-global. **No `reload_config()` method exists.** Verified by source
> inspection — search for 'reload' returns no results." — lines 391-393

This is **falsified** at current HEAD (`fa74b86`). `reload_config()` exists at
`core/services.py:66-76` and is covered by 5 tests. The report's summary line
"Only ENT-006 remains confirmed" and the entire ENT-006 finding section (lines 367-404)
need to be updated to match the treatment of the other six findings (fixed at HEAD).

### 2.2 The plan file is stale

`.ai/plans/01-entry-architecture-fixes.md` has `status: draft` and describes 5 blocks of
implementation work (A through E). All blocks have already been implemented at HEAD. The
plan file should be updated to reflect `status: completed`.

### 2.3 What is NOT needed

- No Implementor block — all code is committed and tested
- No Researcher block — no design decisions remain
- No new public API surface — `reload_config()` exists as a method on `AppService` as the
  finding recommended; it is accessible via `get_app_service().reload_config()`

---

## 3. Work Blocks

### Block 1 — Correct the audit validation report (ENT-006 disposition)

| Field | Value |
|-------|-------|
| **Block ID** | 1 |
| **Finding** | ENT-006 — stale "confirmed" disposition |
| **Files** | `.ai/audit/99-validation/01-entry-architecture-validated-findings.md` |
| **Required agents** | Doc-specialist |

**Scope:**
- Update the findings table (line 98): ENT-006 row — change "confirmed" to "fixed at HEAD (claim falsified at code_ref)"; change the Band column
- Add a re-validation note section for ENT-006 (following the pattern of ENT-001)
- Update the ENT-006 finding section (lines 367-404) to record it as fixed at HEAD
- Update the Summary section (lines 14-22, 75) to reflect "1 fixed, 1 confirmed→fixed, 5 rejected as falsified at HEAD" → "1 fixed (ENT-001), 1 fixed (ENT-006), 5 rejected as falsified at HEAD"
- Update line 22: "Only ENT-006 remains confirmed" → remove this claim
- Update the Evidence Index and Rollout Safety Assessment references for ENT-006

### Block 2 — Update plan file status

| Field | Value |
|-------|-------|
| **Block ID** | 2 |
| **Type** | Status update |
| **File** | `.ai/plans/01-entry-architecture-fixes.md` |
| **Required agents** | Doc-specialist |

**Scope:**
- Change `status: draft` to `status: completed` in the frontmatter (line 6)
- Add a completion note documenting which commits remediated each block

### Block 3 — Final validation

| Field | Value |
|-------|-------|
| **Block ID** | 3 |
| **Required agents** | Validator |

**Scope:**
- Run `uv run pytest` — all 23 tests pass
- Run `uv run ruff check src/` — no lint errors
- Verify audit validation report and plan file are internally consistent
- Verify no unrelated changes were made

---

## 4. Execution Sequence

```text
┌─────────────────────────────────────────┐
│  BLOCK 1: Correct audit validation      │
│  report (ENT-006 → fixed at HEAD)       │
│  [Doc-specialist → 01-entry-…-findings.md]│
├─────────────────────────────────────────┤
│  BLOCK 2: Update plan file status     │
│  [Doc-specialist → 01-entry-…-fixes.md]│
├─────────────────────────────────────────┤
│  BLOCK 3: Final validation            │
│  [Validator → tests, lint, consistency]│
└─────────────────────────────────────────┘
```

Rationale: No implementation blocks are needed because all seven findings were
already remediated at HEAD before this workflow began. The only work is correcting
stale validation documentation (Block 1), updating the plan status (Block 2), and
independent validation (Block 3).

---

## 5. Verification Matrix

| Finding | Before | After | How verified |
|---------|--------|-------|--------------|
| ENT-006 disposition | "confirmed" in validation report | "fixed at HEAD" | Doc-specialist edits + Validator review |
| `reload_config()` exists | stale report says "no method" | report acknowledges `755523e` | Already verified by Auditor (code + tests) |
| Plan file status | `draft` | `completed` | Doc-specialist edit |
| Test suite | 23 passed (Auditor verified) | 23 passed | `uv run pytest` |
| Lint | clean | clean | `uv run ruff check src/` |

---

## 6. Risks

| Risk | Assessment | Mitigation |
|------|-----------|------------|
| Over-editing the validation report beyond ENT-006 | Low | Only update ENT-006-related sections; leave the other six findings' "fixed at HEAD" treatment as-is |
| The plan file's `status: completed` is misleading since blocks were completed by prior commits | Low | Add a completion note with commit references for each block |
| Validator may flag the audit report as "unchanged code + changed doc" | Low | This is the intended outcome — documentation correction, not a code change |

---

*End of execution plan.*
