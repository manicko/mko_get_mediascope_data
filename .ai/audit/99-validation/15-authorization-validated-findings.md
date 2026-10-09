---
phase: 15
phase_name: authorization
validated_by: validator
code_ref: d093d9c
total_findings: 3
confirmed: 3
rejected: 0
reclassified: 0
val_findings: 1
problems_only: true
---

# Phase 15 (Authorization) — Validated Findings

## Summary

All 3 findings in `.ai/audit/15-authorization/findings.md` (code_ref `d093d9c`) are **CONFIRMED**
against live source inspection of `src/mko_get_mediascope_data/`. AUT-001 and AUT-002 are
backed by direct runtime reproduction (induced 401/403 via `uv run python` + `NetworkClient.call`);
AUT-003 is statically confirmed (no scope/authority references anywhere in `core/`). All three
are correctly graded against Phase 15's own severity taxonomy. One VAL finding (VAL-001)
records a cross-phase ownership-attribution imprecision in AUT-002's "Related" note.

## Per-Finding Validation

### AUT-001 — confirmed — [HIGH] Declared `refresh_token_func` hook unimplemented; no refresh-on-401

<!-- finding_ref: AUT-001 -->

| field | value |
|---|---|
| type | AUT-001 |
| severity | HIGH |
| anchor | `src/mko_get_mediascope_data/core/network.py::NetworkClient.call` / `NetworkClient` |
| status | CONFIRMED |

**Claim rederived:** `NetworkClient.call` catches `AuthorizationError` (401) and re-raises
immediately on attempt 1 with no refresh attempt and no retry. The `refresh_token_func` hook
exists only as a TODO in the class docstring; `__init__` exposes no such parameter.

**Evidence:**
- `network.py:49-57` — `NetworkClient.__init__` signature:
  ```text
  (self, max_concurrent_requests: int = 5, sleep_time: int = 2, max_attempts: int = 10)
  ```
  No `refresh_token_func` parameter. ✓ (matches findings Appendix A)
- `network.py:80-86` — `except AuthorizationError as err:` block logs and re-raises without
  any refresh call. ✓ (matches findings Appendix B, lines 80-86)
- `network.py:46` — `TODO (Architect): в будущем можно добавить refresh_token_func:
  Callable | None = None` — declared only in docstring, not in code. ✓
- `network.py:10` — `from mediascope_api.core.errors import AuthorizationError,
  BadRequestError` — `AccessForbiddenError` not imported (relevant to AUT-002). ✓
- Runtime repro (`uv run python`):
  ```text
  refresh_token_func param present: False
  Authorization error (token invalid/expired) in "func" with args="()" and kwargs="{}": [Error code 401] 401 auth error
  401 -> attempts: 1 | re-raised immediately (NOT retried)
  ```
  Constructed `NetworkClient(max_attempts=3, sleep_time=0)`, invoked `call` with a function
  raising `AuthorizationError`; observed `attempts == 1`, immediate re-raise. ✓
- Vendor `mediascope_api/core/net.py:127-138` — `refresh_token()` refreshes only on
  `expires_in` timing, never on a 401. ✓
- Vendor `mediascope_api/core/net.py:467-482` — `_raise_error` raises `AuthorizationError`
  on 401 (line 471-472). ✓

**Disambiguator:** `"AuthorizationError"` — present in the `except AuthorizationError` branch
(network.py:80). ✓

**Severity grading (Phase 15 rubric):**
- HIGH band: "a declared token-refresh hook that is unimplemented, leaving expiry or
  rotation unhandled" — direct match. ✓
- NOT CRITICAL: the 401 is re-raised on attempt 1 (not retried as transient), so it does not
  "spin through the full retry budget while masking the real problem." ✓
- NOT MEDIUM: the hook is not merely "wired but never invoked" (MEDIUM) — it is not wired at
  all, which is the stricter HIGH case. ✓
- Grade is correct; no re-grade.

**Load-bearing artefact:** Code (`NetworkClient.call` + `__init__`). The docstring TODO is
acknowledgement, not the defect. The finding survives regardless of the TODO comment — the
executing path does not refresh on 401.

**Recommendation carry-out:** Target `NetworkClient.__init__` (add
`refresh_token_func: Callable[[], Awaitable[None]] | None = None`) and `NetworkClient.call`
(invoke once in the `AuthorizationError` branch, retry before re-raise). Both symbols exist
and are stable. Additive change with `None` default — backward compatible. `test_network_retry_and_fail`
(`NetworkClient(max_attempts=3, sleep_time=0)`) and `mock_network_client` fixture
(`NetworkClient(max_attempts=2, sleep_time=0)`) remain green.

**Verdict:** CONFIRMED.

---

### AUT-002 — confirmed — [MEDIUM] 403 `AccessForbiddenError` has no handler, misrouted to "Unexpected error"

<!-- finding_ref: AUT-002 -->

| field | value |
|---|---|
| type | AUT-002 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/network.py::NetworkClient.call` |
| status | CONFIRMED |

**Claim rederived:** `AccessForbiddenError` (403) is never imported or caught in
`NetworkClient.call`. It falls through to the catch-all `except Exception` branch, logged via
`logger.exception` as "Unexpected error" with a full traceback, then re-raised on attempt 1.

**Evidence:**
- `network.py:10` — imports only `AuthorizationError, BadRequestError`. ✓
- Static check: `AccessForbiddenError referenced in network module: False`. ✓
- `network.py:113-118` — catch-all `except Exception as err:` logs "Unexpected error …"
  via `logger.exception` (full traceback) and re-raises. ✓ (matches findings Appendix B)
- Runtime repro (`uv run python`):
  ```text
  Unexpected error in "func" with args="()" and kwargs="{}": [Error code 403] 403 forbidden
  Traceback (most recent call last):
    ...
  mediascope_api.core.errors.AccessForbiddenError: [Error code 403] 403 forbidden
  403 -> attempts: 1 | fell through to generic Exception (NOT retried, misrouted)
  ```
  403 was re-raised on attempt 1 (not retried) but logged as "Unexpected error" with a
  traceback. ✓
- Vendor `mediascope_api/core/errors.py:29-34` — `AccessForbiddenError` exists, code=403. ✓
- Vendor `mediascope_api/core/net.py:467-482` — `_raise_error` raises
  `AccessForbiddenError` on 403 (line 473-474). ✓

**Disambiguator:** `"except Exception"` — present at network.py:113. ✓

**Severity grading (Phase 15 rubric):**
- MEDIUM band: "an auth-failure message that is opaque or actionability-free" — "Unexpected error"
  + traceback for a 403 is opaque; a permission denial is indistinguishable from a crash. ✓
- NOT HIGH: the HIGH band requires "a 403 misclassified as a transient network error, so a
  permission failure **is retried** instead of surfaced." The 403 is NOT retried (re-raised on
  attempt 1); it is merely misrouted to the generic handler. ✓
- NOT LOW: this has a runtime consequence (misleading log + traceback persisted to `log.log`),
  not just documentation drift. ✓
- Grade is correct; no re-grade.

**Load-bearing artefact:** Code (`NetworkClient.call` exception handler chain). The fix is
a code change in `core/`, not a documentation change.

**Recommendation carry-out:** Target `NetworkClient.call` (add `except AccessForbiddenError`
branch before the catch-all) and the import line (network.py:10). Both exist and are stable.
403 was already re-raised (not swallowed), so adding a dedicated branch only changes the log
message — no caller-visible behavior change.

**Verdict:** CONFIRMED.

---

### AUT-003 — confirmed — [MEDIUM] No credential-scope validation; under-scoped credential yields silent partial data

<!-- finding_ref: AUT-003 -->

| field | value |
|---|---|
| type | AUT-003 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate` / `src/mko_get_mediascope_data/core/services.py::AppService.get_connection` |
| status | CONFIRMED |

**Claim rederived:** No scope/authority validation exists anywhere in `src/`.
`TVRegTaskStrategy.generate` builds the region list directly from
`report.catalogs.get_tv_region()` (API-returned, credential-limited) with no comparison to an
expected region set. An under-scoped credential yields a partial report with no warning.

**Evidence:**
- `rg -n 'scope|permission' src/mko_get_mediascope_data/core/` → word-boundary search
  `\b(scope|permission|accessible|authority)` → 0 matches. ✓ (matches findings Appendix C)
- `reports.py:384-391` — `TVRegTaskStrategy.generate`:
  ```python
  available_regions = report.catalogs.get_tv_region()
  regions_list = (
      report.report_settings.model_dump().get("regions")
      or available_regions["id"].to_list()
  )
  ```
  `available_regions` is API-returned and credential-limited; the `for reg_id in regions_list`
  loop (line 405) has no completeness assertion. ✓ (matches findings Appendix B)
- `services.py:100-108` — `AppService.get_connection` creates a `MediaVortexTask`
  connection; no scope/permission check. ✓
- Vendor `mediascope_api/core/net.py:127-138` — `refresh_token()` / token flow has no
  scope probe. ✓

**Disambiguator:** `"get_tv_region"` — present inside `TVRegTaskStrategy.generate`
(reports.py:387). ✓

**Severity grading (Phase 15 rubric):**
- MEDIUM band: "scope mismatch not detected, so an under-scoped credential returns partial
  data without warning" — direct match. ✓
- Grade is correct; no re-grade.

**Load-bearing artefact:** Code (absence of validation in `core/reports.py` and
`core/services.py`). The spec corpus (Phase 15 block 4: "Establish whether the credential's
scope or authority is validated") expects a check that does not exist — a missing integration,
not dead code.

**Recommendation carry-out:** Target `AppService.get_connection` (services.py:100). The
target exists and is stable, **but** the recommendation is substantively correct while
imprecise about data availability: `get_connection` receives only `media_type: MediaType`,
not the report settings or catalogs. The expected region set is assembled later in
`TVRegTaskStrategy.generate` (reports.py:387-391). The fix can be carried out — either by
passing `report_settings` to `get_connection`, or by placing the scope check in
`TVRegTaskStrategy.generate` where both expected and available regions are in scope. The
credential's accessible region set is available via `report.catalogs.get_tv_region()` (already
called at line 387), so the comparison is feasible.

**Verdict:** CONFIRMED.

---

## VAL Findings (Validation Precision)

| id | source_finding | issue | reclassification |
|---|---|---|---|
| VAL-001 | AUT-002 | "Related" note attributes worker error-handling ownership to "the error-handling phase (09/10)"; Phase 03 (ASY-001, CRITICAL) is the confirmed owner of the `MediaReport.worker` try/except + `task_done()` gap. Phase 04 (PIPE-002) covers the same defect. The observation itself (no try/except in worker) is correct; the ownership attribution is imprecise. | VAL (finding remains CONFIRMED) |

## Cross-Phase Analysis

| phase | finding | relationship | direction |
|---|---|---|---|
| 03 | ASY-001 | `MediaReport.worker` has no try/except; `task_done()` is skipped when any exception (including a 401/403 from AUT-001/AUT-002) propagates out of `send_task`/`wait_task`/`extract_data`, stalling `queue.join()`. This is the concrete mechanism behind AUT-002's "Related" note. | coupled (AUT-001/002 depend) |
| 04 | PIPE-002 | Same `MediaReport.worker` defect (no try/finally around `task_done`); Phase 03 graded it CRITICAL, Phase 04 graded HIGH — each phase's grading is correct against its own rubric. | overlapping |
| 09 | EXT-001 | Non-idempotent task submission (POST retried) compounds auth-failure blast radius — a 401/403 retry would duplicate tasks. | amplifies |
| 09 | EXT-002 | No timeout cap on `asyncio.to_thread` in `call` — an auth error path is already uncovered by timeouts. | supporting |

**No cross-phase conflicts.** Phase 15 owns authorization policy, auth-failure classification,
and scope validation (Phase 10 line 18 explicitly defers "authorization (15)"). The worker
error-handling gap that AUT-002 references is correctly treated as a *related* concern owned
by Phase 03, not a competing claim.

## Rollout Safety Assessment

| concern | status | notes |
|---|---|---|
| AUT-001 `refresh_token_func` adds `__init__` parameter | SAFE | Additive, defaults to `None`. `test_network_retry_and_fail` and `mock_network_client` fixture unaffected. |
| AUT-002 `AccessForbiddenError` import + handler | SAFE | 403 was already re-raised; only changes the log message from "Unexpected error" to a distinct permission-failure log. No caller-visible behavior change. |
| AUT-003 scope probe in `get_connection` | LOW RISK | May emit warnings on existing reports if expected scope data is unavailable. Guard with optional/None check so the probe is non-fatal. |
| AUT-002 before AUT-001 ordering | SAFE | Distinct 403 classification clarifies which failures are refreshable. No circular dependency. |
| Worker task_done() gap (ASY-001) before auth fixes | RISK | If AUT-001/AUT-002 ship without ASY-001's `try/finally: task_done()`, an auth error raised in `send_task`/`wait_task`/`extract_data` will skip `task_done()` and deadlock `queue.join()`. ASY-001 should ship first or alongside. |
| No sibling validated reports reference AUT-* | SAFE | Repo-wide grep for `\bAUT-\d\b` returns 0 matches outside this phase's own findings. Prefix is clean. |

## Required Fixes (mandatory)

1. **AUT-001** — Add `refresh_token_func: Callable[[], Awaitable[None]] | None = None` to
   `NetworkClient.__init__` (network.py) and, in `call`'s `except AuthorizationError` branch,
   invoke it once and retry the call before re-raising. Target `NetworkClient.__init__` and
   `NetworkClient.call` — both exist and are stable.
2. **AUT-002** — Import `AccessForbiddenError` from `mediascope_api.core.errors` (network.py:10)
   and add a dedicated `except AccessForbiddenError` branch in `NetworkClient.call` that logs a
   distinct permission-failure message and re-raises without retry, placed before the catch-all
   `except Exception` (network.py:113).
3. **AUT-003** — Add a scope probe comparing the credential's accessible region set
   (`report.catalogs.get_tv_region()`) to the report's expected scope, surfacing a distinct
   warning when the visible set is a strict subset. Implementation location: either
   `AppService.get_connection` (requires passing `report_settings`) or
   `TVRegTaskStrategy.generate` (where both sets are already in scope).

## Advisory Recommendations

1. **AUT-002 → AUT-001 ordering** — Ship the distinct 403 handler (AUT-002) before wiring
   `refresh_token_func` (AUT-001) so the retry decision logic can distinguish "refreshable 401"
   from "non-refreshable 403." The findings' Fix Order already documents this; preserve it.
2. **AUT-003 scope-probe placement** — Place the scope check in `TVRegTaskStrategy.generate`
   (reports.py:387-391) rather than `get_connection`, because that is where the expected region
   set (`report.report_settings.model_dump().get("regions")`) and the available region set
   (`available_regions`) are both already assembled. This avoids threading report settings
   through `get_connection`.
3. **Worker error handling (ASY-001)** — The `MediaReport.worker` method has no
   `try/finally` around `task_done()` (reports.py:134-153). Any auth error raised during
   `send_task`/`wait_task`/`extract_data` skips `task_done()` and deadlocks
   `task_queue.join()`. This is Phase 03's ASY-001 (CRITICAL) — ship it alongside or before
   the AUT-001/AUT-002 fixes to prevent queue poisoning.
4. **Test coverage** — No tests cover 401 refresh-on-retry, 403 classification, or scope
   validation. The findings' Verify sections call for `test_401_invokes_refresh_hook_and_retries_once`,
   `test_403_not_retried_and_logged_as_permission_failure`, and
   `test_under_scoped_credential_warns_on_scope_gap`. `test_network.py` currently has a single
   test (`test_network_retry_and_fail`, `ConnectTimeout` only). Add the above as part of the
   fix PRs.
