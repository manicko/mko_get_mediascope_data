# Execution Plan — Phase 09 External API: Fix Rollout

## Source

- **Plan file (audit):** `.ai/audit/09-external-api/findings.md` (baseline `code_ref d093d9c`)
- **Validated findings:** `.ai/audit/99-validation/09-external-api-validated-findings.md` (re-verified `2026-10-15`, code-ref `d093d9c`)
- **Live code context:** verified against working tree at HEAD `a97373e` (`4842506` is the Phase-09 baseline; 3 trailing commits `0047c7a`, `a97373e` are doc/`tpl-006` template-only changes — no production-code impact on the EXT anchors)
- **Vendor library:** `.venv/Lib/site-packages/mediascope_api/` (`core/net.py`, `core/errors.py`, `mediavortex/tasks.py`) — inspected live

## Current State

The audit's "Required Fixes" assumed EXT-003 and the EXT-002/ASY-001 coupling were open. **Live re-inspection at HEAD corrects this:**

1. **EXT-003 is RESOLVED in code.** `TVMediaReport.wait_task` (reports.py:404-408) already resets `task.status = False` on `FAILED`/`CANCELLED`; `worker` (reports.py:159-188) only logs "is ready" after the `if item.status is False:` branch, so failed/cancelled tasks never emit the success log. The plan's Required Fix #3 is a **no-op / revert risk** if re-implemented. Only the **test** for it is missing.
2. **EXT-002/ASY-001 coupling is RESOLVED.** `worker` (reports.py:149-188) wraps `_process_item` (reports.py:137-147) with `asyncio.wait_for` (reports.py:161-162) when a timeout is given, and uses `try/except TimeoutError/finally: task_done()` (reports.py:173-188). A per-call `asyncio.wait_for` inside `NetworkClient.call` is therefore safe: its `asyncio.TimeoutError` is in `RETRIABLE_EXCEPTIONS` (network.py:28) so the call is retried, and the worker's `finally: task_done()` still drains the queue even if the timeout propagates. The audit's rollout risk ("EXT-002 ships alone poisons the queue") no longer applies.
3. **ALL cross-phase dependencies are RESOLVED at HEAD** — see the Cross-Phase Status table below. Every ASY-* and PIPE-* prerequisite named in the audit survives only as a RESOLVED marker; no cross-phase work is in scope here.
4. **EXT-001 is vendor-blocked for the `Idempotency-Key` path.** Live grep across `mediascope_api/` for `idempotency` → **0 matches**. Vendor `send_request` (net.py:140, headers at net.py:189-190) hardcodes only `Authorization` + `Content-Type`; `_send_task`/`send_task` (tasks.py:944-979) call `send_request('post', url, data)` with no header parameter; the vendor `Session`/`HTTPAdapter` exposes no injection hook. **The only viable self-contained mitigation is a non-retried POST call path** — a technical constraint, not business uncertainty.

### Cross-Phase Status (RESOLVED at HEAD)

| Cross-phase | EXT ref | Status | Evidence (HEAD) |
|---|---|---|---|
| ASY-001 | EXT-002 | **RESOLVED** | `worker` `try/finally: task_done()` (reports.py:187-188); `asyncio.wait_for` wraps `_process_item` (reports.py:161-162) |
| ASY-002 | EXT-003 | **RESOLVED** | `get_tv_region` routed through `network_client.call` |
| ASY-003 | EXT-003 | **RESOLVED** | no bare `cancel()` |
| ASY-005 | EXT-004 | **RESOLVED** | `wait_task` bounded by `max_polls=60, poll_deadline=3600.0` (reports.py:363) |
| ASY-007 | EXT-007 | **RESOLVED** | `MaxRetriesExceeded` raised (network.py:130, errors.py:12-17) |
| PIPE-001 | — | **RESOLVED** | `csv_to_file` uses `mode="w"` |
| PIPE-002 | — | **RESOLVED** | worker `try/except/finally` |
| PIPE-003 | — | **RESOLVED** | `time_unit` on `PeriodModel` |
| PIPE-004 | — | **RESOLVED** | `NoReportFoundError` on zero tasks |
| PIPE-005 | — | **RESOLVED** | inverted period raises |
| PIPE-006 | — | **RESOLVED** | `max_polls`/`poll_deadline` |
| PIPE-007 | — | **RESOLVED** | inner `except TimeoutError` + `to_file` |
| PIPE-008 | — | **RESOLVED** | `task.name` on `KeyError` |
| PIPE-009 | — | **RESOLVED** | worker reads `log_error` |
| PIPE-010 | — | **RESOLVED** | `to_file` raises on `folder=None` |
| PIPE-011 | — | **RESOLVED** | TVReg skip logs `warning` |

## Finding Status Summary

| Finding | Severity | Status | Action Needed |
|---------|----------|--------|---------------|
| EXT-001 | CRITICAL | **OPEN** | Make the task-submission POST non-retried (vendor forbids `Idempotency-Key`). **Decision gate.** |
| EXT-002 | HIGH | **OPEN** | Wrap `asyncio.to_thread` in `NetworkClient.call` with `asyncio.wait_for`; pick a default `call_timeout`. |
| EXT-003 | HIGH | **RESOLVED (code)** | No code change. Only the **status-reset test** is missing (Block F). |
| EXT-004 | MEDIUM | **OPEN (partially mitigated)** | `sleep_time=40` still passed to `get_status` (reports.py:392) → backoff amplification. Bounded loop, but inflated cadence. **In-scope (minimal)** per scope decision. |
| EXT-005 | MEDIUM | **OPEN** | 429 (`TooManyRequestsError`) not retried at project layer; dead `except HTTPError` (network.py:105-115); no `Retry-After` parsing; AGENTS.md:57 misleading. **Decision gate** (retry-boundary alignment). |
| EXT-006 | MEDIUM | **OPEN (advisory)** | New `Session` per report via `get_connection` (services.py:150-164). **Deferred** (R-01 connection-layer rework). |
| EXT-007 | LOW | **OPEN (advisory)** | No metrics counters in `NetworkClient.call`. **Deferred** (no metrics infra). |
| EXT-008 | MEDIUM | **OPEN** | Error handlers log verbatim `args`/`kwargs` (network.py:86-89, 93-95, 118-121) containing `userName`/`task_json`. Redact. |
| AGENTS.md:57 | doc | **OPEN** | "client errors (4xx) raise immediately" is wrong for 429. Correct. |

## Test Coverage Gaps (Required Fix #4) — mapped to findings

| Test to add | Maps to | Where it lands |
|---|---|---|
| idempotency / non-retried-POST test | **EXT-001** | Block A — `test_send_task_not_retried_on_retryable_error` |
| real timeout-enforcement test | **EXT-002** | Block B — `test_call_raises_timeout_after_call_timeout` |
| 429 / 5xx classification test | **EXT-005** | Block D — `test_call_retries_on_429`, `test_call_5xx_classification` |
| FAILED/CANCELLED status-reset test | **EXT-003** | Block F — `test_wait_task_resets_status_on_failed`, `...on_cancelled` |
| PII redaction test | **EXT-008** | Block E — `test_call_redacts_pii_in_error_logs` |

## Scope Decisions

| Finding | In-scope? | Rationale |
|---|---|---|
| EXT-001 | **In-scope** (CRITICAL) | Duplicate task creation. Mitigation must be vendor-free (non-retried POST). |
| EXT-002 | **In-scope** (HIGH) | Hung HTTP calls block worker threads; bounded by semaphore exhaustion under load. |
| EXT-003 | **In-scope** (test only) | Code already fixed; closing the test gap prevents regression of the queue-integrity invariant. |
| EXT-004 | **In-scope (minimal)** | One-line constant change in `wait_task`; loop already bounded by `max_polls`/`poll_deadline`, so risk is low and the quota/latency win is real. Full "adaptive polling" redesign is **out of scope** (speculative). |
| EXT-005 | **In-scope** (MEDIUM) | 429 silently bypasses retry — a correctness gap in the retry contract. Dead `except HTTPError` is confusing dead code. AGENTS.md correction is part of this. |
| EXT-006 | **Deferred** | Per audit decision mapping (EXT-006 → "no dedicated work item; session churn accepted until R-01 reworks the connection layer"). Requires vendored `Session`/`MediaVortexTask` lifecycle changes — out of minimal-scope. |
| EXT-007 | **Deferred** | LOW severity; no metrics library in project, no `log_config` metrics sink. Adding counters is a separate observability effort, not required for the external-API reliability fix. |
| EXT-008 | **In-scope** (MEDIUM) | PII (`userName`) logged to disk/file — a compliance-adjacent risk. Redaction is a localized, behavior-preserving edit to log statements. |
| AGENTS.md:57 | **In-scope** (doc) | 1-line correction aligning docs with the (corrected) retry contract; ships with Block D. |

## Decision Gates (do NOT force a choice)

These two findings carry real technical uncertainty that cannot be resolved without an owner call. The plan **presents alternatives and flags them as decisions** rather than prescribing a single path. They block the corresponding implementor block until resolved.

### D-EXT-001 — EXT-001: how to make the task-submission POST non-retried

**Constraint:** vendor `send_request` (net.py:140) declares no header-injection parameter and hardcodes `Authorization` + `Content-Type` (net.py:189-190). `send_task`/`_send_task` (tasks.py:944-979) accept no headers. `Idempotency-Key` is therefore **impossible** without a vendor patch the owner cannot ship.

| Option | Mechanism | Pros | Cons | Status |
|---|---|---|---|---|
| **A1** (recommended) | Add `retryable: bool = True` to `NetworkClient.call`; `send_task` (reports.py:354) calls `call(self.sender, task_json, retryable=False)` → single attempt, no backoff. | Minimal; reuses existing `call` infra (semaphore/backoff/logging); backward compatible (default `True`). | Touches the shared `call` signature; `call_timeout` (Block B) must also apply to the non-retried path (it does — `wait_for` is outside the retry loop). | **Choice A1** |
| A2 | In `send_task`, call `asyncio.to_thread(self.sender, task_json)` directly, bypassing `call` for the POST only (no retry, no semaphore). | Leaves shared `call` unchanged. | Duplicates concurrency/backoff logic; loses semaphore bound on the submission thread; diverges from the centralized retry abstraction. | Alternative |
| A3 | Add `Idempotency-Key` header to the task-creation POST. | Ideal — dedup at server. | **Vendor-blocked** — no header-injection hook in `send_request`/`send_task`. Rejected. | Rejected |

**Default if no decision:** implement A1 (minimal, backward-compatible, reuses existing retry abstraction).

### D-EXT-005 — EXT-005: retry-boundary alignment for 429 / 5xx

**Constraint:** vendor `_raise_error` (net.py:468) maps 429→`TooManyRequestsError` (a `MediascopeApiError` subclass, not in `RETRIABLE_EXCEPTIONS`). Vendor `urllib3.Retry` (net.py:75-78) retries only `[502,503,504]` at transport level and does **not** parse `Retry-After`. Project `call` (network.py:117) `except Exception: raise` swallows 429 without retry. The `except HTTPError` (network.py:105-115) is dead code (vendor never raises `requests.HTTPError`).

| Option | Mechanism | Pros | Cons | Status |
|---|---|---|---|---|
| **D1** (recommended) | Add `except TooManyRequestsError` (and optionally `ServerError`/5xx `MediascopeApiError`) in `call`; on 429, parse `Retry-After` from the exception (`err.code`/response), sleep, retry within the existing loop. Remove the dead `except HTTPError`. | Closes the 429 gap at the project layer; respects `Retry-After`; `except HTTPError` cleanup removes confusion. | Extends the retryable set — must ensure 400/401/403/404 are NOT retried (they aren't: distinct classes already re-raised). 5xx handling overlaps with vendor transport retry (see D2). | **Choice D1** |
| D2 | Keep 429 un-retried; only parse `Retry-After` and re-raise with a structured message for the caller to back off on. | Smallest change; no retry-semantics risk. | Does not fix the "429 bypasses retry" correctness gap — caller must implement backoff. | Alternative |
| D3 | Disable vendor `urllib3.Retry`; make project layer the single retry authority for all 4xx/5xx + `Retry-After`. | Clean single boundary. | Requires monkey-patching vendor `Retry([502,503,504])` off — high blast radius, speculative redesign. **Rejected.** | Rejected |

**Default if no decision:** implement D1 (retry 429 with `Retry-After` parsing at the project layer; leave vendor transport retry for 502/503/504 untouched; document the boundary in AGENTS.md). The 5xx-project-retry question is folded into D1 as an owner-configurable toggle (`retry_5xx: bool`, default `False` to avoid changing current 5xx behavior).

---

## Work Blocks

All blocks target `src/mko_get_mediascope_data/` and `tests/`. Each block is atomic, independently reviewable, and includes its regression test. Implementation runs **one implementor at a time** (see Implementation Sequence). Decision gates are resolved by research/subagent work that can run in parallel (≤2 subagents).

---

### Block A — EXT-001: Non-retried task-submission POST

**Goal:** Ensure the POST that creates-and-launches a Mediascope task is never retried, eliminating duplicate-task creation. The vendor cannot add `Idempotency-Key`, so the mitigation is a project-layer non-retryable call path (Option A1).

**Dependencies:**
- Block-level: none.
- Cross-phase: none (all ASY-*/PIPE-* RESOLVED at HEAD).
- Decision gate: **D-EXT-001** must resolve A1 vs A2 before implementation. (If A2 chosen, Block A is replaced by a `send_task` edit that bypasses `call`.)
- Ordering: Block B depends on A1's `call` signature (the `call_timeout` wrap sits alongside the `retryable` branch).

**Current code location:**
- `src/mko_get_mediascope_data/core/network.py:63-130` — `NetworkClient.call` (`asyncio.to_thread(func, *args, **kwargs)` at network.py:78; retry loop at 75-128; `MaxRetriesExceeded` at 130).
- `src/mko_get_mediascope_data/core/network.py:53-61` — `NetworkClient.__init__`.
- `src/mko_get_mediascope_data/core/reports.py:343-360` — `TVMediaReport.send_task`; `task_json` POST via `self.network_client.call(self.sender, task_json)` at reports.py:354.

**Implementation approach (A1 — default):**
1. Add `retryable: bool = True` parameter to `NetworkClient.call` (after `sleep_time`).
2. In `call`, wrap the retry `for` loop: when `retryable is False`, execute the attempt exactly once (single `to_thread` call) and on `RETRIABLE_EXCEPTIONS` re-raise immediately instead of looping/backoff. The non-retryable path still respects `call_timeout` (Block B) and the semaphore.
3. In `TVMediaReport.send_task` (reports.py:354), change to `await self.network_client.call(self.sender, task_json, retryable=False)`. Leave the `builder` call (reports.py:349) retried — it is idempotent (task JSON construction).
4. Keep `retryable` out of the pre-call smoothing so the single attempt still gets the jittered delay (preserves existing pacing).

**Tests to add:** `tests/test_network.py`
- `test_send_task_not_retried_on_retryable_error` — configure `send_task` doubled to raise `ConnectTimeout` twice; assert it is invoked **exactly once** (no retry) and the error propagates. (idempotency test for EXT-001)

**Tests to keep green:** all 9 existing `tests/test_network.py` tests; `test_send_task` path in `test_reports_lifecycle.py` (DONE status).

**Risk assessment:** Low. `retryable` defaults to `True` (backward compatible). Only the unsafe POST opts out; GET/status/result calls remain retried. Worst case of a wrong decision: a transient network blip on the POST raises instead of retrying — acceptable (caller sees the error; no duplicate task). The non-retried POST still flows through the worker `try/except/finally` (reports.py:159-188) so `task_done()` is always called.

**Task spec (task_template.yaml shape):**

```yaml
id: task_EXT-001
title: Make task-submission POST non-retried via retryable=False
priority: high
depends_on: [decision_D-EXT-001]
description: >
  Vendor mediascope_api has no Idempotency-Key support (0 grep matches);
  send_request hardcodes Authorization+Content-Type only. The non-retried
  POST is the only vendor-free mitigation against duplicate task creation.
goals:
  - task-submission POST never retried (no duplicate tasks)
  - idempotent builder call stays retried
  - backward compatible: retryable defaults True
files:
  - path: src/mko_get_mediascope_data/core/network.py
    targets:
      - type: method
        name: NetworkClient.call
    semantic_anchors:
      insert_before: "for attempt in range(1, self._max_attempts + 1):"
  - path: src/mko_get_mediascope_data/core/reports.py
    targets:
      - type: method
        name: TVMediaReport.send_task
    semantic_anchors:
      insert_after: "task_json = await self.network_client.call(self.builder, task.report_type, **task.settings)"
changes:
  - action: add_parameter
    target: NetworkClient.call
    parameter: "retryable: bool = True"
  - action: add_code
    description: "Single-attempt branch when retryable is False; re-raise RETRIABLE_EXCEPTIONS immediately."
  - action: add_code
    target: TVMediaReport.send_task
    code_hint: "self.sender call gains retryable=False"
acceptance_criteria:
  - send_task POST invoked once on ConnectTimeout (no retry)
  - builder call still retried
  - all existing network tests pass
```

---

### Block B — EXT-002: Per-call `asyncio.wait_for` timeout in `NetworkClient.call`

**Goal:** Cap the wall-clock time a single HTTP call may block a worker thread, so a hung upstream cannot exhaust the semaphore-bounded thread pool.

**Dependencies:**
- Block-level: depends on Block A (if A1) — both extend `call`; B's `call_timeout` wrap must sit outside the retry loop and coexist with the `retryable` branch. Implement A first.
- Cross-phase: ASY-001 (`finally: task_done()`) is RESOLVED (reports.py:187-188), so a propagated `asyncio.TimeoutError` cannot poison the queue — safe to ship.
- Decision: **default `call_timeout` value** — recommend `300.0` (5 min); `None` = no cap (full backward compat for callers that need it).

**Current code location:**
- `src/mko_get_mediascope_data/core/network.py:78` — `return await asyncio.to_thread(func, *args, **kwargs)` (no timeout wrapper).
- `src/mko_get_mediascope_data/core/network.py:53-61` — `__init__` (add `call_timeout`).
- `src/mko_get_mediascope_data/core/network.py:28-39` — `asyncio.TimeoutError` is already in `RETRIABLE_EXCEPTIONS`, so a timed-out attempt is retried (desired) up to `max_attempts`.

**Implementation approach:**
1. Add `call_timeout: float | None = 300.0` to `NetworkClient.__init__`.
2. In `call`, replace `return await asyncio.to_thread(func, *args, **kwargs)` with `return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=self._call_timeout)` — but only when `self._call_timeout` is not `None` (preserve current no-cap behavior when explicitly disabled).
3. `asyncio.TimeoutError` from `wait_for` is already caught by `except RETRIABLE_EXCEPTIONS` → retried with backoff. After `max_attempts` → `MaxRetriesExceeded`. No new except branch needed.
4. Document limitation: `asyncio.to_thread` is not cancellable, so the underlying blocking `requests` call may continue in its thread after the timeout fires; the async wait is bounded, which is the safety property we need (semaphore slot released on retry/raise).

**Tests to add:** `tests/test_network.py`
- `test_call_raises_timeout_after_call_timeout` — a blocking `time.sleep` func with `NetworkClient(max_attempts=1, call_timeout=0.5)`; assert `asyncio.TimeoutError` (or `MaxRetriesExceeded` if max_attempts>1) is raised within bounded wall-clock, and `call` does not hang. (real timeout-enforcement test for EXT-002)

**Tests to keep green:** all 9 existing `test_network.py` tests (mock `asyncio.sleep` is already used; new `call_timeout` default of 300s won't affect them since they don't block).

**Risk assessment:** Medium. Behavioral change — adds a real timeout cap. A too-small default could abort valid slow Mediascope responses (status polls carry a 2s pre-call smoothing + vendor latency). Mitigations: generous default (300s), `None` to disable, only affects the `to_thread` await (not the pre-call sleep). Backward compatible default choice is the tuning knob (D note above).

**Task spec:**

```yaml
id: task_EXT-002
title: Add asyncio.wait_for timeout to NetworkClient.call
priority: high
depends_on: [task_EXT-001]
description: >
  Wrap asyncio.to_thread(...) in asyncio.wait_for so a hung upstream HTTP
  call cannot block a semaphore slot indefinitely. ASY-001's finally:task_done()
  guarantees queue integrity even if TimeoutError propagates.
goals:
  - per-call wall-clock cap configurable via call_timeout
  - timed-out attempt retried via existing RETRIABLE_EXCEPTIONS path
  - backward compatible: None disables the cap
files:
  - path: src/mko_get_mediascope_data/core/network.py
    targets:
      - type: method
        name: NetworkClient.__init__
      - type: method
        name: NetworkClient.call
    semantic_anchors:
      insert_after: "self._max_attempts = max_attempts"
changes:
  - action: add_parameter
    target: NetworkClient.__init__
    parameter: "call_timeout: float | None = 300.0"
  - action: add_code
    description: "self._call_timeout = call_timeout"
  - action: add_code
    target: NetworkClient.call
    code_hint: |
      await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=self._call_timeout)
acceptance_criteria:
  - blocking call raises asyncio.TimeoutError within call_timeout
  - retry-on-timeout uses existing backoff
  - existing 9 tests pass unchanged
```

---

### Block C — EXT-004: Lower poll-interval amplification in `wait_task` (in-scope, minimal)

**Goal:** Stop passing `sleep_time=40` to the `get_status` poll call so the pre-call smoothing drops from `uniform(20, 80)` to `uniform(1, 4)` per poll, reducing API-quota waste and failure-detection latency. The poll loop is already bounded by `max_polls`/`poll_deadline`.

**Dependencies:**
- Block-level: none (touches `reports.py:392`, not `network.py`).
- Cross-phase: ASY-005 (`max_polls`/`poll_deadline`) is RESOLVED (reports.py:362-363).
- Can run in the implementor stream anywhere after Block B (different file, independent).

**Current code location:**
- `src/mko_get_mediascope_data/core/reports.py:391-393` — `get_status(task.key, sleep_time=40)`.
- `src/mko_get_mediascope_data/core/network.py:67` — `call`'s `sleep_time: int | float = 2` default; smoothing at network.py:73 `uniform(_sleep_time * 0.5, _sleep_time * 2)`.

**Implementation approach (minimal):** change `sleep_time=40` → `sleep_time=2` in the `get_status` call at reports.py:392 (aligns with the `NetworkClient` default and the rest of the report pipeline). This is a one-line, behavior-only change. The full "adaptive/early-return-on-terminal-status" redesign from Advisory Recommendation #1 is **out of scope** (speculative).

**Tests to add:** none strictly required — `test_wait_task_max_polls_limit` (test_reports_lifecycle.py) already exercises the poll path. A light assertion on poll cadence is optional and deferred.

**Tests to keep green:** `test_wait_task_max_polls_limit`, `test_create_report_timeout_does_not_hang`, `test_create_report_writes_csv_and_marks_queue_done`.

**Risk assessment:** Low. Loop is bounded (`max_polls=60, poll_deadline=3600.0`, reports.py:363); semaphore (`max_concurrent_requests=5`, network.py:59) bounds concurrency. Faster polling only increases request rate, which the Mediascope API rate limit absorbs (and EXT-005 adds 429 handling). No structural change.

**Task spec:**

```yaml
id: task_EXT-004
title: Lower get_status poll sleep_time from 40 to project default
priority: medium
depends_on: []
description: >
  sleep_time=40 feeds NetworkClient.call's pre-call smoothing uniform(20,80),
  inflating every poll gap. Lowering to the NetworkClient default (2s) gives
  uniform(1s, 4s). The poll loop is already bounded by max_polls/poll_deadline.
goals:
  - eliminate sleep_time=40 backoff amplification
  - keep poll loop fully bounded
files:
  - path: src/mko_get_mediascope_data/core/reports.py
    targets:
      - type: method
        name: TVMediaReport.wait_task
    semantic_anchors:
      insert_after: "status = await self.network_client.call("
changes:
  - action: add_code
    description: "Change sleep_time=40 -> sleep_time=2 on the get_status call."
acceptance_criteria:
  - get_status called with sleep_time=2
  - max_polls_limit lifecycle test still passes
```

---

### Block D — EXT-005: 429 retry classification + remove dead `except HTTPError` + AGENTS.md

**Goal:** Close the gap where HTTP 429 (`TooManyRequestsError`) bypasses project-layer retry while 502/503/504 are retried only by the hidden vendor `urllib3.Retry`. Parse `Retry-After`; remove the dead `except HTTPError`; correct AGENTS.md:57.

**Dependencies:**
- Block-level: depends on Block B (the `call` except-block region must be stable before restructuring it). Also depends on Decision gate **D-EXT-005** (whether to also retry 5xx at the project layer — default D1 with `retry_5xx=False` toggle).
- Cross-phase: ASY-005 (retry-boundary discussion) is RESOLVED; this implements the project-layer half.
- Note: EXT-005 and the AGENTS.md correction are the same logical concern (the retry-contract narrative), so the AGENTS.md edit ships in this block (Block G below is dropped to keep the doc change co-located with the code it documents).

**Current code location:**
- `src/mko_get_mediascope_data/core/network.py:105-115` — dead `except HTTPError` (vendor never raises `requests.HTTPError`).
- `src/mko_get_mediascope_data/core/network.py:117-122` — `except Exception` swallows 429.
- `src/mko_get_mediascope_data/core/network.py:99-103` — existing `RETRIABLE_EXCEPTIONS` handler.
- Vendor `mediascope_api/core/net.py:468-482` (`_raise_error`: 429→`TooManyRequestsError`, 400→`BadRequestError`, 401→`AuthorizationError`, else→`MediascopeApiError`).
- Vendor `mediascope_api/core/net.py:75-78` (`Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])`).
- `AGENTS.md:57` — "client errors (4xx) raise immediately".

**Implementation approach (D1 — default):**
1. Import `TooManyRequestsError`, `MediascopeApiError` from `mediascope_api.core.errors` (network.py:10 already imports `AuthorizationError, BadRequestError`).
2. Add a dedicated `except TooManyRequestsError as err:` branch **before** `except RETRIABLE_EXCEPTIONS` is impossible (order: `BadRequestError`, `AuthorizationError`, `FileNotFoundError`, `TooManyRequestsError`, then `RETRIABLE_EXCEPTIONS`, then remove/replace `HTTPError`, then `Exception`). On 429: parse `Retry-After` (`getattr(err, 'retry_after', None)` or `err.code`); sleep `min(retry_after, cap)`; this falls through to the existing backoff+jitter and the loop retries.
   - `TooManyRequestsError.message` is the Russian text; the exception carries `code=429`. `Retry-After` is a response header not exposed by the vendor exception — so `Retry-After` parsing is **best-effort**: parse from `err.message` if present, else fall back to exponential backoff. Document this vendor limitation.
3. Remove the dead `except HTTPError` branch (network.py:105-115) and the now-unused `HTTPError` import (network.py:11) **only if** no other reference exists — verify via grep first (lint gate).
4. Add `retry_5xx: bool = False` to `__init__`; when `True`, retry `MediascopeApiError` whose `code` is 5xx (except those already handled by 4xx classes). Default `False` preserves current 5xx behavior (no project-layer retry).
5. AGENTS.md:57 — change "client errors (4xx) raise immediately" → "client errors (4xx) except 429 raise immediately; 429 (`TooManyRequestsError`) is retried with `Retry-After` backoff; 502/503/504 retry at the vendor transport layer (`urllib3.Retry`)." Add the same contract summary near the Architecture "Key patterns" bullet.

**Tests to add:** `tests/test_network.py`
- `test_call_retries_on_429` — doubled func raises `TooManyRequestsError`; assert it is retried (call count > 1) up to `max_attempts` then `MaxRetriesExceeded`. (429 classification test for EXT-005)
- `test_call_5xx_classification` — parametrized over `ServerError`(500) and generic `MediascopeApiError`(500); with `retry_5xx=False` (default), assert single attempt + raise; with `retry_5xx=True`, assert retried. (5xx classification boundary)
- Existing `test_call_raises_immediately_on_bad_request` / `..._on_auth_error` stay green (guard: 400/401 still raise immediately — must not regress).

**Tests to keep green:** `test_call_raises_immediately_on_bad_request`, `test_call_raises_immediately_on_auth_error`, all 9 existing tests.

**Risk assessment:** Medium. Extends the retryable exception set; must NOT accidentally retry 400/401/403/404 (those are distinct classes already re-raised ahead of the new branch). `Retry-After` parsing is best-effort because the vendor exception doesn't expose the response header — document the limitation. Removing `except HTTPError` is safe only if grep confirms zero `requests.HTTPError` raises from the vendor path (verification gate). The `retry_5xx` toggle (default `False`) keeps current 5xx behavior unchanged, limiting blast radius.

**Task spec:**

```yaml
id: task_EXT-005
title: Retry 429 with Retry-After; remove dead except HTTPError; fix AGENTS.md
priority: medium
depends_on: [task_EXT-002, decision_D-EXT-005]
description: >
  Vendor maps 429 to TooManyRequestsError (mediascope_api.core.errors), which
  currently falls through to 'except Exception: raise' (no retry). Retry-After
  is not exposed by the vendor exception -> parse best-effort; otherwise use
  exponential backoff. Vendor urllib3.Retry already handles 502/503/504 at
  transport level; do not disable it. AGENTS.md:57 corrected to reflect the
  4xx-except-429 contract.
goals:
  - 429 retried at project layer (respecting Retry-After when available)
  - 400/401/403/404 still raise immediately (no regression)
  - dead except HTTPError removed (verified unreachable)
  - AGENTS.md retry contract corrected
files:
  - path: src/mko_get_mediascope_data/core/network.py
    targets:
      - type: method
        name: NetworkClient.__init__
      - type: method
        name: NetworkClient.call
    semantic_anchors:
      insert_before: "except RETRIABLE_EXCEPTIONS as err:"
  - path: AGENTS.md
    targets:
      - type: text
        name: async-pipeline-bullet-line-57
changes:
  - action: add_import
    import_text: "from mediascope_api.core.errors import TooManyRequestsError, MediascopeApiError"
  - action: add_parameter
    target: NetworkClient.__init__
    parameter: "retry_5xx: bool = False"
  - action: add_code
    description: "except TooManyRequestsError -> parse Retry-After best-effort, retry"
  - action: delete_in_body
    description: "remove dead 'except HTTPError' branch if grep confirms unreachable"
  - action: add_code
    target: AGENTS.md
    code_hint: "4xx except 429 raise immediately; 429 retried; 502/503/504 @ vendor transport layer"
acceptance_criteria:
  - 429 retried up to max_attempts
  - 400/401 raise immediately (existing tests green)
  - Retry-After best-effort backoff does not crash
  - AGENTS.md:57 corrected
gates:
  - grep vendor raises requests.HTTPError (expect 0) before removing except HTTPError
```

---

### Block E — EXT-008: Redact PII from error log handlers

**Goal:** Stop logging verbatim `args`/`kwargs` (which contain `task.key` with `userName`, and the `task_json` POST body) in `NetworkClient.call`'s error handlers.

**Dependencies:**
- Block-level: depends on Block D — both restructure the `call` except-block region (D removes the dead `HTTPError` branch and adds `TooManyRequestsError`; E then applies redaction to the surviving handlers). Implementing E against the pre-D structure would create a merge conflict.
- Cross-phase: none.

**Current code location:**
- `src/mko_get_mediascope_data/core/network.py:86-89` — `AuthorizationError` log: `with args="{args}" and kwargs="{kwargs}"`.
- `src/mko_get_mediascope_data/core/network.py:93-95` — `FileNotFoundError` log: same pattern.
- `src/mko_get_mediascope_data/core/network.py:118-121` — `except Exception` log: same pattern.
- `src/mko_get_mediascope_data/core/reports.py:354` (`send_task` args = `task_json`); `reports.py:392` (`get_status(task.key, ...)` where `task.key` includes `userName`).

**Implementation approach (minimal, behavior-preserving):**
1. Add a module-level `_redact(obj: Any) -> str` helper in `network.py`. It masks values whose **key name** is in a sensitive set (`userName`, `password`, `passw`, `token`, `accessToken`, `secretKey`, `secret`) for `dict`/`list`/`tuple` args (covers the `get_status` path where `task.key` is a dict containing `userName`). For the `send_task` path, `args = (task_json,)` is a JSON **string** — `_redact` parses it best-effort (`json5.loads`), redacts sensitive keys, and on parse failure falls back to a length-capped `repr` (never raises). Scalar non-JSON args are length-capped.
2. Replace the verbatim `args="{args}"`/`kwargs="{kwargs}"` interpolations in the three handlers with `args="{_redact(args)}"` / `kwargs="{_redact(kwargs)}"`. Keep `func.__name__` and the exception `err` intact (those are not PII).
3. Do **not** change control flow — error handlers still re-raise. Redaction is log-only.
   - Optional (deferred, out of scope): a debug-level full-dump toggle. Skip to keep scope minimal.

**Tests to add:** `tests/test_network.py`
- `test_call_redacts_pii_in_error_logs` — doubled func raises `ConnectTimeout` with kwargs containing `userName`/`password`; use `caplog` to assert the captured WARNING/ERROR does **not** contain the raw secret and **does** contain a masked marker. (PII redaction test for EXT-008)

**Tests to keep green:** all 9 existing `test_network.py` tests (none assert on raw args content).

**Risk assessment:** Low. Log-content change only; no control-flow or return-value change. Must ensure `_redact` never raises (wrap in `try/except` → fallback to `repr`). Length-capping prevents log flooding from large `task_json`.

**Task spec:**

```yaml
id: task_EXT-008
title: Redact PII (userName/password/token) from NetworkClient.call error logs
priority: medium
depends_on: [task_EXT-005]
description: >
  Error handlers log args/kwargs verbatim; send_task args contain task_json and
  get_status args contain task.key (which includes userName). Redact known
  sensitive keys in log statements only; preserve function name + exception
  context. Do not alter retry/re-raise control flow.
goals:
  - sensitive keys masked in all NetworkClient.call error handlers
  - function name and exception type preserved in logs
  - _redact never raises (fallback to repr)
files:
  - path: src/mko_get_mediascope_data/core/network.py
    targets:
      - type: function
        name: _redact
      - type: method
        name: NetworkClient.call
    semantic_anchors:
      insert_at: top
changes:
  - action: add_top_level
    description: "def _redact(obj) -> str: mask sensitive keys, length-cap repr"
  - action: replace_in_body
    target: NetworkClient.call
    description: "Replace args/kwargs raw interpolation with _redact(...) in AuthorizationError/FileNotFoundError/Exception handlers"
acceptance_criteria:
  - caplog for ConnectTimeout with userName/password kwargs shows masked values
  - function name still present in log
  - existing 9 network tests pass
```

---

### Block F — EXT-003: Add status-reset regression test (code already fixed)

**Goal:** Close the missing test for the already-implemented EXT-003 behavior, so the queue-integrity invariant does not silently regress. **No production code change** — the plan's Required Fix #3 (flip `task.status=False`) is already done at reports.py:404-408; re-implementing it is a revert risk.

**Dependencies:**
- Block-level: none.
- Cross-phase: ASY-002 (worker cancellation propagation) RESOLVED — the test asserts the worker does not emit "is ready" for failed/cancelled tasks.

**Current code location:**
- `src/mko_get_mediascope_data/core/reports.py:404-408` — terminal-status branch already resets `task.status = False`, `log_error = True`, `error = f"Task {terminal}"`.
- `src/mko_get_mediascope_data/core/reports.py:172` — `"Task {item.name} is ready."` only after the `if item.status is False:` branch.
- `src/mko_get_mediascope_data/core/reports.py:144-147` — `_process_item` gates `wait_task`/`extract_data` on `item.status is True`.

**Implementation approach:** Test-only. Add to `tests/test_reports_lifecycle.py` using the existing hermetic pattern (`_build_hermetic_app_service`, doubled `MediaVortexTask`, real `NetworkClient` with `sleep_time=0, max_attempts=1`, mocked `asyncio.sleep`).

**Tests to add:** `tests/test_reports_lifecycle.py`
- `test_wait_task_resets_status_on_failed` — `get_status` returns `{"taskStatus": "FAILED"}`; assert `task.status is False`, `log_error is True`, `error == "Task FAILED"`, and `extract_data` was **not** invoked.
- `test_wait_task_resets_status_on_cancelled` — same for `"CANCELLED"`.
- `test_worker_does_not_log_ready_on_failed` — assert the worker logs "Calculation: … will be skipped" (not "is ready") for a FAILED task, and asserts the `to_file` error artifact is written.

**Tests to keep green:** `test_wait_task_max_polls_limit`, `test_create_report_timeout_does_not_hang`, `test_worker_handles_exception_and_writes_error_json5`.

**Risk assessment:** None. Test-only; validates existing, intended behavior.

**Task spec:**

```yaml
id: task_EXT-003-test
title: Add regression test for EXT-003 status-reset on FAILED/CANCELLED
priority: high
depends_on: []
description: >
  EXT-003 is already fixed in code (reports.py:404-408 resets task.status=False
  on FAILED/CANCELLED; worker only logs 'is ready' when status is True). This
  block adds the missing test — it does NOT touch production code.
goals:
  - FAILED terminal status sets status=False, extracts skipped
  - CANCELLED terminal status sets status=False, extracts skipped
  - worker logs "will be skipped" (not "is ready") for failed tasks
files:
  - path: tests/test_reports_lifecycle.py
changes:
  - action: add_code
    description: "Three new async tests mirroring the wait_task/worker pattern"
acceptance_criteria:
  - FAILED -> status False, log_error True, error 'Task FAILED', no extract_data
  - CANCELLED -> status False, log_error True, error 'Task CANCELLED'
  - worker logs 'will be skipped' for failed tasks
  - all existing lifecycle tests green
```

---

### Block G — AGENTS.md retry-contract correction

**Goal:** Correct AGENTS.md:57 so it no longer claims "4xx raise immediately" (false for 429), aligning the developer-facing contract with the implemented behavior (EXT-005, Block D).

> This block was originally scoped as a standalone doc fix. It is **folded into Block D** to keep the contract narrative and its code together — one commit updates both `network.py` retry branches and the AGENTS.md line they implement. See Block D's `files` list. (Listed here only to record that the doc correction is accounted for.)

---

## Implementation Sequence

Single implementor stream (1 implementor at a time). Decision gates D-EXT-001 and D-EXT-005 are resolved by research/subagent work (≤2 parallel) before the blocks they gate are implemented.

```
[Research]  Resolve D-EXT-001 (A1 vs A2)            ─┐
[Research]  Resolve D-EXT-005 (D1 vs D2, retry_5xx) ─┘   (parallel, ≤2 subagents)
   │
1.  Block F  (EXT-003 test only)            — validates already-fixed code; green baseline
2.  Block A  (EXT-001 non-retried POST)     — needs D-EXT-001
3.  Block B  (EXT-002 wait_for timeout)     — needs A (call signature stable)
4.  Block C  (EXT-004 lower sleep_time)     — independent; after B
5.  Block D  (EXT-005 429 retry + dead except + AGENTS.md:57)  — needs B; needs D-EXT-005
6.  Block E  (EXT-008 PII redaction)        — needs D (shared except-block region)
```

**Rationale for ordering:**
- F first: zero-risk test-only block establishes a green baseline and locks EXT-003's invariant before touching `network.py`.
- A → B: both extend `NetworkClient.call`; A adds the `retryable` branch, B adds the `call_timeout` wrap. Sequencing avoids re-editing the same `to_thread` line twice.
- C placed after B: independent file (`reports.py`), but keeping it after the `network.py` core changes keeps the "external-API reliability" story contiguous.
- D after B: D restructures the `call` except-block region; B's `wait_for` edit lands first so D edits a stable structure.
- E after D: E redacts args/kwargs in the surviving handlers; doing it after D avoids a conflict with D's dead-`HTTPError` removal.
- AGENTS.md:57 correction ships **inside** Block D (single commit: code + the doc line that describes it).

**Parallelism note:** the two decision gates are the only parallelizable steps (≤2 subagents). All implementor blocks are strictly sequential.

## Validation Strategy

Run after **each** implementor block (A–E) and as a final gate:

```bash
# Lint (CI order: ruff then pytest)
uv run ruff check src tests
# Auto-fixable only (review before accepting)
uv run ruff check --fix src tests

# Typecheck (mypy checks src + tests per pyproject.toml)
uv run mypy src tests --show-error-codes

# Tests (skip slow to match CI fast gate)
uv run pytest -m "not slow"

# Full suite (incl. slow) when touching retry/timeout semantics
uv run pytest
```

**Per-block gates:**
- Block A — `test_send_task_not_retried_on_retryable_error` new; `--no-cov` fast run must stay ≥ existing count.
- Block B — `test_call_raises_timeout_after_call_timeout` new; assert wall-clock < `call_timeout + 1s`; existing 9 network tests green (no `call_timeout` impact since default 300s).
- Block C — `test_wait_task_max_polls_limit` green; no new test (cadre covered).
- Block D — `test_call_retries_on_429`, `test_call_5xx_classification` new; **require** `test_call_raises_immediately_on_bad_request` + `_on_auth_error` green (no 4xx regression); **grep gate:** confirm 0 vendor raises of `requests.HTTPError` before removing `except HTTPError`.
- Block E — `test_call_redacts_pii_in_error_logs` new; caplog asserts secret absent.
- Block F — `test_wait_task_resets_status_on_failed/cancelled`, `test_worker_does_not_log_ready_on_failed` new; `test_wait_task_max_polls_limit` green.
- Block D/AGENTS.md — `ruff`/`mypy` clean after `HTTPError` import removal (if unused).

**Final gate:** `uv run ruff check src tests` (0 errors) → `uv run mypy src tests --show-error-codes` (0 errors) → `uv run pytest -m "not slow"` (all green, coverage ≥ current gate).

## Deferred (not in this plan)

- EXT-006 (Session-per-report) — deferred to R-01 connection-layer rework (audit decision mapping).
- EXT-007 (no metrics) — deferred; requires metrics infra addition (separate observability task).
- EXT-004 full "adaptive polling" redesign — deferred (speculative; minimal constant change in-scope).
- EXT-008 opt-in full-dump debug toggle — deferred (scope).
