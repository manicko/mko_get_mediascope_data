---
name: audit-tests-full
description: audit-tests-full
agent: auditor
alwaysApply: false
---

# Test Quality Audit

## Step 0 — Test Environment

This project runs tests directly via `uv run pytest` — no Docker or database
is required. Confirm the environment works before executing any test commands:

```powershell
uv run ruff check src tests
uv run mypy src tests
uv run pytest            # runs all tests
uv run pytest -m "not slow"   # fast feedback (skips slow tests)
```

## Objective

Perform a complete audit of test coverage to:
- Identify and delete/rewrite tests that don't match the current architecture and code style
- Ensure tests verify **real system behavior**, not mocks or implementation details
- Verify sufficient coverage of all key system parts
- Guarantee high diagnostic value of tests
- Recommend missing test coverage for critical business flows

**Core principle:** Production code is the source of truth. Tests adapt to it, not the other way around. When tests and docs conflict with production code, update tests and docs — not the code.

## Recommendation Types

Label every finding:
- `[TEST-DELETE]` — test is harmful or worthless; delete it
- `[TEST-REWRITE]` — test intent is right but implementation is wrong; rewrite
- `[TEST-UPDATE]` — test needs minor updates to match current code
- `[BEST-PRACTICE]` — missing test coverage or quality improvement; advisory
- `[DOC-UPDATE]` — test reveals that docs/spec are wrong, not the code

## Research

Use `websearch` to verify current best practices for:
- pytest async testing patterns (pytest-asyncio)
- Mocking strategies for external API clients
- Meaningful test coverage metrics vs coverage for its own sake

## Anti-Patterns (subject to deletion or complete rewrite)

### 1. Architecture / Contract Mismatch
- Sync/async mismatch
- Use of deprecated methods, DTOs, data types, or contracts
- Testing removed or renamed functionality
- Forces keeping legacy code just to pass tests
- Tests against old response shapes (e.g., changed API field names)

### 2. Overmocking
- Mock completely replaces business logic
- Assertions check mock values, not real results
- Excessive `patch`/`mock` without verifying side effects
- Mocking the `NetworkClient` when real API fixtures would be more valuable

### 3. Tautological / Useless Tests
- `assert True`, missing `assert`, or trivial checks
- Verify obvious things ("object created", "status 200")
- Repeat implementation instead of verifying business contract

### 4. Wrong Abstraction Level
- Test private methods, internal details, specific HTTP request construction
- Verify call order instead of final result and business rules

### 5. Fragile Tests
- Break on refactoring without behavior change
- Depend on test execution order, `sleep`, or shared mutable state
- Don't use `pytest.mark.asyncio` properly

### 6. Poor Business Coverage
- Ignore negative scenarios and boundary conditions
- Use primitive data instead of realistic data
- Don't verify error handling and side effects

## Additional Checks

- Duplication, copy-paste tests and fixtures
- Giant fixtures, hidden dependencies, shared mutable state
- **Mutation resistance** — test must fail when business logic is broken

## pytest Standards

- All tests use **pytest** (not `unittest.TestCase`)
- Fixtures extracted to `conftest.py`
- Mocking only via `unittest.mock` or `pytest-mock`
- Async tests use `pytest.mark.asyncio` (auto-mode enabled via config)
- Test file naming: `test_*.py`
- Test function naming: `test_*`

## System Coverage

Verify tests exist for all key system parts:

### Required Coverage Areas

- **Network layer** — `NetworkClient` (retries, backoff, timeout, semaphore concurrency)
- **Core models** — `MediaType`, `ReportType`, `ReportSubtype` enums, Pydantic settings models
- **Report pipeline** — `TVMediaReport.create_report()` (task generation, polling, data fetching, CSV export)
- **Settings management** — `init_service` (template copying), `paths` (config dir resolution), Pydantic validation
- **CLI layer** — `app.py` commands (`init`, `run`, `list-reports`)
- **Utilities** — date/period parsing, JSON5/CSV I/O helpers

### Special Attention
- Negative scenario and boundary condition coverage
- Error handling paths (network failures, expired reports, invalid settings)
- Async concurrency behavior (semaphore limiting, concurrent task polling)
- Config validation edge cases (missing fields, invalid values)

## Audit Result Format

Create file: `.ai/audit/tests/audit_report_<number>.md` (next available number)

**Report Structure:**

1. **Statistics**
   - Total tests: N
   - Critical problems: M
   - Recommended for deletion: K
   - Coverage by module

2. **Problematic Tests Table**

| File | Test | Type | Category | Problem | Action | Priority |
|------|------|------|----------|---------|--------|----------|
| tests/test_network.py | test_no_retry_on_4xx | [TEST-DELETE] | Architecture | Tests implementation detail, not contract | Delete | HIGH |
| tests/test_models.py | test_invalid_enum | [TEST-REWRITE] | Contract | Uses old enum value, not current | Rewrite | MEDIUM |

3. **Coverage Assessment** — which important areas/scenarios are uncovered or weakly covered

4. **Key Findings** (with examples)

5. **Action Plan**
   - Delete Required (tests that force wrong production code changes)
   - Rewrite Required (tests with right intent, wrong implementation)
   - Improve (add negative cases, integration tests, etc.)
   - Doc Updates (tests that revealed spec/docs are wrong)

6. **Blocked Refactorings** (if any)

## Acceptance Criteria

- Clear separation: what to delete / rewrite / improve
- Prioritization of problems
- Recommendations for improving test culture
- Critical coverage gaps identified

**Rule:** Better to delete a questionable test than to keep a test that misleads or slows down project development.
