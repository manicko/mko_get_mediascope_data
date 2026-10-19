# Execution Plan — Phase 10 Code Quality: QLT-009 Remediation

## Source

- **Plan file:** `.ai/audit/99-validation/10-code-quality-validated-findings.md`
- **Audit findings:** `.ai/audit/10-code-quality/findings.md`

## Current State (HEAD `ae832d2`)

The codebase has been heavily remediated since the audit baseline (`code_ref d093d9c`).
**13 of 14 confirmed findings are already resolved.** Only **QLT-009** remains open.

### Finding Status Summary

| Finding | Severity | Status | Action Needed |
|---------|----------|--------|---------------|
| QLT-005 | HIGH | RESOLVED (rejected at HEAD) | None — `yaml_to_dict` → `json5_to_dict` raises instead of returning None |
| QLT-006 | HIGH | RESOLVED | None — `DictDataStrategy.prepare` uses `getattr` for `Data` model |
| QLT-007 | HIGH | RESOLVED | None — `time_unit` moved to `PeriodModel`; `last_time` guarded |
| QLT-008 | MEDIUM | RESOLVED | None — bare `dict` → `dict[str, Any]` everywhere |
| QLT-009 | MEDIUM | **OPEN** | **Make `NetworkClient.call` type-safe via generic `TypeVar`** |
| QLT-010 | MEDIUM | RESOLVED | None — uses `ReportSubtype.DYNAMICS_BY_SPOTS_DICT` enum comparison |
| QLT-011 | MEDIUM | RESOLVED | None — `*args` removed from `csv_to_file` |
| QLT-012 | MEDIUM | RESOLVED | None — `get_last_period` uses `date` consistently |
| QLT-013 | MEDIUM | RESOLVED | None — `TVTask.__init__` fully typed |
| QLT-014 | MEDIUM | RESOLVED | None — `types-python-dateutil`, `types-requests` added; yaml import removed |
| QLT-015 | LOW | RESOLVED | None — empty `Task` base class removed |
| QLT-016 | LOW | RESOLVED | None — exceptions now raised in `services.py`/`reports.py` |
| QLT-017 | LOW | RESOLVED | None — `logger.py` placeholder removed |
| QLT-018 | LOW | RESOLVED | None — `en_to_ru` catches `(TypeError, AttributeError)` |
| QLT-019 | LOW | RESOLVED (re-framed) | None — `extra="allow"` is intended; `PeriodModel` now `extra="forbid"` |
| QLT-020 | LOW | RESOLVED (rejected at HEAD) | None — Russian comments translated to English |

### Tooling State at HEAD

- `uv run mypy src/mko_get_mediascope_data/` → **Success: no issues found in 13 source files**
- `uv run ruff check src/mko_get_mediascope_data/` → **All checks passed**
- `uv run pytest --no-cov` → **57 passed**
- `uv run mypy tests/` → 6 errors (all "Unused type: ignore" comments — pre-existing, out of scope)

## Work Block

### Block A — QLT-009: Type-parameterise `NetworkClient.call` (MEDIUM)

**Goal:** Replace the `-> Any` return type on `NetworkClient.call` with a generic
`TypeVar` bound return type, so callers receive the correct type inferred from
the wrapped callable's return annotation.

**Dependencies:** None. Independent.

**Background:** The audit finding (QLT-009) states that `NetworkClient.call`
returns `Any`, defeating the type checker. Every caller in `reports.py`
receives `Any` and must either annotate or cast. The fix is the standard
Python generic-wrapper pattern using `TypeVar`.

**Current code** (`core/network.py`, lines 61-67):

```python
async def call(
    self,
    func: Callable[..., Any],
    *args,
    sleep_time: int | float = 2,
    **kwargs,
) -> Any:
```

---

#### File: `src/mko_get_mediascope_data/core/network.py`

**Change 1 — Add `TypeVar` import**

Replace the existing `from typing import Any` (line 8) with:

```python
from typing import Any, TypeVar
```

**Change 2 — Define module-level type variable**

Add after the imports (before `RETRIABLE_EXCEPTIONS` or after `logger`):

```python
T = TypeVar("T")
```

**Change 3 — Type-parameterise `call`**

Replace the signature and body type annotations:

```python
async def call(
    self,
    func: Callable[..., T],
    *args: Any,
    sleep_time: int | float = 2,
    **kwargs: Any,
) -> T:
```

Only the signature changes; the body (`*args`/`**kwargs` passthrough, retry loop,
return statement) is unchanged. `asyncio.to_thread(func, *args, **kwargs)` already
returns `T` when called through the generic.

**Change 4 — Add `*args: Any` / `**kwargs: Any` annotations**

The current `*args` and `**kwargs` lack type annotations, which triggers
`check_untyped_defs`. Annotate them as `*args: Any, **kwargs: Any` for
completeness.

#### File: `tests/test_network.py`

**Test to add:**

- `test_call_preserves_return_type` — Verify that a typed callable's return value
  is correctly forwarded and that the return type annotation is preserved. Use a
  function with a specific return type (e.g., `dict[str, str]`) and assert the
  result type matches. This test validates the generic contract without needing
  a live type-checker at runtime.

#### Files NOT to change

- Do **not** change `extra="allow"` on `Data`/`DataDefaults`/`ReportSettings` (QLT-019) — intended two-layer transport.
- Do **not** remove `warn_unused_ignores` or touch test-side `# type: ignore` comments (out of scope).

## Implementation Sequence

```
Block A (QLT-009: type-parameterise NetworkClient.call + test) → commit
```

## Validation Strategy

After implementation:

```bash
uv run ruff check src/mko_get_mediascope_data
uv run mypy src/mko_get_mediascope_data/ --show-error-codes
uv run pytest --no-cov
```

- mypy `src/` must remain 0 errors
- ruff must remain clean
- All 57 existing tests + new test must pass
