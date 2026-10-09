---
phase: 09
phase_name: external-api
validated_by: validator
code_ref: d093d9c
total_findings: 8
confirmed: 8
rejected: 0
reclassified: 0
val_findings: 4
problems_only: true
---

# Phase 09 (External API) — Validated Findings

## Summary

All 8 findings in `.ai/audit/09-external-api/findings.md` (code_ref `d093d9c`) are **CONFIRMED** against live source inspection. Runtime probe `uv run pytest tests/test_network.py` → 1 passed in ~20s (matches findings' 19.77s). Four VAL findings recorded for minor citation/precision issues.

## Per-Finding Validation

### EXT-001 — Non-idempotent task submission can produce duplicate task creation
<!-- finding_ref: EXT-001 -->

| field | value |
|---|---|
| type | EXT-001 |
| severity | CRITICAL |
| anchor | `src/mko_get_mediascope_data/core/network.py` / `core/reports.py` / vendor `net.py`/`tasks.py` |
| status | CONFIRMED |

**Claim rederived:** `NetworkClient.call` wraps every outbound request with retries for `RETRIABLE_EXCEPTIONS` (network.py:24-35). `TVMediaReport.send_task` routes the POST to create-and-launch a task through the retried `call` — i.e. the task-submission POST is retried on retryable exceptions. Vendor `MediaVortexTask.send_task` POSTs task JSON with no `Idempotency-Key` header. A network hiccup → POST retry → duplicate task objects on the vendor side.

**Evidence:**
- `network.py:24-35`: `RETRIABLE_EXCEPTIONS = (asyncio.TimeoutError, aiohttp.ClientError, ConnectionError, OSError, socket.error)`.
- `network.py:90-98`: `call` catches, logs, and re-raises; retry loop in `call` (network.py:72-74: `asyncio.to_thread(func, *args, **kwargs)`) re-invokes on those exceptions.
- `reports.py:263`: `send_task` builds `body` and calls `self._network.call(self.api_client.send_task, body, task=self.task)`.
- vendor `net.py:189-193`: `send_request`/`_send_request` — no `Idempotency-Key` or dedup header set.
- vendor `tasks.py:944-979`: `_send_task`/`send_task` POSTs without dedup semantics.
- `tests/test_services.py`: mocks `create_report`; no idempotency test exists.

**Disambiguator:** Retry applies to all `call` invocations (GET, POST, task-launch POST). The POST is the unsafe (non-idempotent) operation that becomes dangerous under retry.

**Verdict:** CONFIRMED.

---

### EXT-002 — No timeout enforced on HTTP requests; vendor defaults rely on `requests` (blocking, no cap)
<!-- finding_ref: EXT-002 -->

| field | value |
|---|---|
| type | EXT-002 |
| severity | HIGH |
| anchor | `src/mko_get_mediascope_data/core/network.py` / vendor `net.py` |
| status | CONFIRMED |

**Claim rederived:** `NetworkClient.call` invokes vendor functions via `asyncio.to_thread(func, *args, **kwargs)` with NO `asyncio.wait_for` wrapper — no timeout cap on the coroutine. Vendor `send_request`/`get_token` pass no `timeout=` to `requests`. `grep timeout` in vendor `net.py` → 0 matches. A hung upstream blocks a worker thread indefinitely; under load the semaphore-bounded threads can exhaust.

**Evidence:**
- `network.py:72-74`:
  ```python
  async def call(self, func, *args, **kwargs):
      ...
      return await asyncio.to_thread(func, *args, **kwargs)  # no wait_for
  ```
- vendor `net.py` `send_request`/`get_token`: no `timeout=` kwarg forwarded to `requests`.
- `bash: grep timeout vendor/net.py` → 0 matches.
- `tests/test_network.py`: only tests `ConnectTimeout` path (simulated), no real timeout enforcement test.

**Disambiguator:** Distinguishes the project-layer timeout gap (no `asyncio.wait_for`) from the vendor-layer gap (no `requests` timeout). Both are true and both contribute.

**Verdict:** CONFIRMED.

---

### EXT-003 — Unhandled terminal statuses (CANCELLED) cause workers to emit "готова" for failed/cancelled tasks
<!-- finding_ref: EXT-003 -->

| field | value |
|---|---|
| type | EXT-003 |
| severity | HIGH |
| anchor | `src/mko_get_mediascope_data/core/reports.py` `wait_task` / `worker` |
| status | CONFIRMED |

**Claim rederived:** `wait_task` (reports.py:271-293) polls task status and breaks on `("DONE", "FAILED", "CANCELLED")` with no branch handling — it does not set `task.status = False` for FAILED/CANCELLED. Because `task.status` stays `True`, the worker proceeds to `extract_data` and downstream logging emits "готова" (ready/done) even for failed or cancelled tasks. `worker` (reports.py:134-153) calls `task_done()` only on success path via `self._tasks_queue.task_done()`.

**Evidence:**
- `reports.py:288-293`:
  ```python
  if task.status in ("DONE", "FAILED", "CANCELLED"):
      break
  ```
  No `else`/branch — `task.status` never flipped to `False` on failure.
- `reports.py:292-293`: worker logs `"готова"` (ready) regardless of terminal status because `task.status` remains truthy.
- `reports.py:148-153`: `extract_data` called without checking terminal status.
- `tests/test_reports.py`: does not cover `wait_task` terminal-status branches.

**Disambiguator:** Bug is in the polling-break logic (missing status reset), not in the polling interval.

**Verdict:** CONFIRMED.

---

### EXT-004 — Suboptimal polling interval wastes API quota and delays failure detection
<!-- finding_ref: EXT-004 -->

| field | value |
|---|---|
| type | EXT-004 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/reports.py` `wait_task` / `src/mko_get_mediascope_data/core/network.py` `get_status` |
| status | CONFIRMED |

**Claim rederived:** `wait_task` calls `get_status` with `sleep_time=40` (reports.py:281). `get_status` computes `_sleep_time = max(40, 2) = 40`, producing initial `uniform(20, 80)` then exponential backoff `40·1.2^(attempt-1)` (network.py:66-69, 121-124). Aggressive fixed baseline inflates total runtime vs. adaptive polling; failure detection is delayed.

**Evidence:**
- `reports.py:281`: `get_status(task.key, sleep_time=40, ...)`.
- `network.py:66-69`: `initial = uniform(20, 80)`; `network.py:121-124`: backoff `base * (1.2 ** (attempt - 1))`.
- `tests/test_network.py`: timing matches findings' 19.77s runtime on the no-sleep_time=0 (ConnectTimeout) path.

**VAL-003 (reclassified):** Findings file cites `1198`s total backoff; recomputed `Σ(uniform(20,80)) + Σ(40·1.2^(a-1))` for default 10-attempt path ≈ 1038s. Arithmetic is imprecise (factor ~0.87x) but band (HIGH quota waste) is unaffected. Reclassified as VAL-003, finding remains CONFIRMED.

**Disambiguator:** Fixed `sleep_time=40` baseline vs. adaptive/early-return-on-terminal-status optimization.

**Verdict:** CONFIRMED (with VAL-003 arithmetic note).

---

### EXT-005 — Error classification conflates HTTP 429 (retryable) with 502/503/504 retry policy
<!-- finding_ref: EXT-005 -->

| field | value |
|---|---|
| type | EXT-005 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/network.py` `call` / vendor `net.py` `_raise_error` |
| status | CONFIRMED |

**Claim rederived:** Vendor raises `MediascopeApiError` subclasses: 429→`TooManyRequestsError`, 500→`MediascopeApiError` (else branch). Both land in `NetworkClient.call` `except Exception` → raise (network.py:113). 502/503/504 are retried only by HIDDEN `urllib3.Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])` inside vendor `net.py:74-75` — invisible to project-level retry/logging. AGENTS.md line 52 ("client errors (4xx) raise immediately") is misleading: 429 is retryable-by-spec, so the project-level "4xx raise immediately" claim is inaccurate for 429.

**Evidence:**
- vendor `net.py:468-482`: `_raise_error` maps status→`MediascopeApiError` subclass (429→`TooManyRequestsError`, 500→`MediascopeApiError`).
- `network.py:113`: `except Exception: raise` in `call` — does not special-case 429.
- vendor `net.py:74-75`: `requests.adapters.HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504]))`.
- `AGENTS.md:52`: "client errors (4xx) raise immediately" — contradicts 429 retryability.
- `tests/test_network.py`: only `ConnectTimeout`; no 429/502/503/504 classification test.

**VAL-002 (reclassified):** Findings file does not cite AGENTS.md line 52 as "the brief" source for the 4xx claim. Traceability gap, not a finding flaw. Reclassified as VAL-002; finding remains CONFIRMED.

**Disambiguator:** Distinguishes vendor-hidden `urllib3.Retry` (502/503/504) from project-visible `call` retry (transient/connection errors). 429 falls in the gap: not retried at project layer, not in vendor `status_forcelist`.

**Verdict:** CONFIRMED (with VAL-002 traceability note).

---

### EXT-006 — New `Session` per report via `MediaVortexTask` instantiation; no close/teardown
<!-- finding_ref: EXT-006 -->

| field | value |
|---|---|
| type | EXT-006 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/services.py` `get_connection` / vendor `tasks.py` `MediaVortexTask` |
| status | CONFIRMED |

**Claim rederived:** `get_connection` calls `MediaVortexTask(...)` per report (services.py:100-108). Vendor `MediaVortexTask` is a singleton via `__new__` (tasks.py:30-35) — BUT `__init__` re-runs on every `MediaVortexTask(...)` call, constructing a fresh `MediascopeApiNetwork` → new `Session()` + `HTTPAdapter` each time (vendor net.py:73-82). No `close`/context manager on vendor classes; `MediaReport.__exit__` is a no-op (reports.py:119-120). Sessions accumulate per report.

**Evidence:**
- `services.py:100-108` (actual lines 101-107): `get_connection` → `MediaVortexTask(...)` per report.
- vendor `tasks.py:30-35`: `__new__` returns cached instance; `__init__` not guarded by `self._initialized`.
- vendor `net.py:73-82`: `Session()` + `HTTPAdapter` constructed in `MediascopeApiNetwork.__init__`.
- `reports.py:119-120`: `__exit__` does not close any network resource.

**VAL-001 (reclassified):** Findings file cites "try:" at line 102 as the `get_connection` call site; actual line is 101. Citation line drift of ±1. Reclassified as VAL-001; finding remains CONFIRMED.

**VAL-004 (reclassified):** Findings file states "Evidence: vendor tasks.py, get_curl_request at line 445" for EXT-008; actual location is `net.py:402-448`. (See EXT-008.) Not an EXT-006 issue — moved to VAL-004.

**Disambiguator:** "Fresh MediaVortexTask per report" is imprecise about object identity (singleton) but correct about new Session per `__init__` re-run.

**Verdict:** CONFIRMED (with VAL-001 citation note).

---

### EXT-007 — No operational metrics or observability emitted by `NetworkClient`
<!-- finding_ref: EXT-007 -->

| field | value |
|---|---|
| type | EXT-007 |
| severity | LOW |
| anchor | `src/mko_get_mediascope_data/core/network.py` `call` / `log_config.yaml` |
| status | CONFIRMED |

**Claim rederived:** `call` emits only `logger.warning` (network.py:96-99) + `logger.exception` (network.py:114-117); no counters for retries/attempts/success/latency. `log_config.yaml` has only `StreamHandler` + `RotatingFileHandler`; no statsd/prometheus/logging-based metrics. `grep` for metrics libs (`prometheus_client`, `statsd`, `prometheus-fastapi`) in `src/` → 0 matches.

**Evidence:**
- `network.py:90-99, 113-117`: only `logger` calls; no counters.
- `settings/log_config.yaml`: handlers = `StreamHandler`, `RotatingFileHandler`; no `QueueHandler`/metrics.
- `bash: grep -riE 'prometheus|statsd' src/` → 0 matches.
- `tests/test_network.py`: no metrics/counter assertions.

**Disambiguator:** Logging exists (warnings/exceptions); structured observability metrics (counters, histograms) do not.

**Verdict:** CONFIRMED.

---

### EXT-008 — Error handlers emit request body/args containing PII (`userName`) to logs
<!-- finding_ref: EXT-008 -->

| field | value |
|---|---|
| type | EXT-008 |
| severity | MEDIUM |
| anchor | `src/mko_get_mediascope_data/core/network.py` error handlers / vendor `net.py` |
| status | CONFIRMED |

**Claim rederived:** Error handlers log verbatim `args`/`kwargs` (network.py:84, 91, 116). `send_task` args = `(task_json,)` (full task payload). `get_status` args include `task.key` containing `userName`. Vendor `get_curl_request` (net.py:402-448) serializes the full request — if invoked/logged, PII in request body is exposed.

**Evidence:**
- `network.py:84`: `logger.debug("function %r args %r kwargs %r", func, args, kwargs)` — logs full args.
- `network.py:91`: same pattern in retry path.
- `network.py:116`: `logger.exception(... args args kwargs kwargs)` on final raise.
- `reports.py:263`: `send_task` body = task JSON (contains task metadata; payload may include `userName`).
- `reports.py:281`: `get_status(task.key, ...)` — `task.key` includes `userName` fragment.
- vendor `net.py:402-448`: `get_curl_request` — full request serialization.

**VAL-004 (reclassified):** Findings file states vendor `get_curl_request` is at `net.py:445`; actual definition is `net.py:402`, body spans 402-448. Line-citation drift. Reclassified as VAL-004; finding remains CONFIRMED.

**Disambiguator:** PII exposure risk = (a) project error-handler logging full args (confirmed active), (b) vendor `get_curl_request` exposure (latent — only if called and logged). Both are real; project layer is the active path.

**Verdict:** CONFIRMED (with VAL-004 citation note).

---

## VAL Findings (Citation / Precision)

| id | source_finding | issue | reclassification |
|---|---|---|---|
| VAL-001 | EXT-006 | Cites "try:" at line 102 as `get_connection` site; actual is line 101 | VAL (finding remains CONFIRMED) |
| VAL-002 | EXT-005 | Does not cite AGENTS.md:52 as "the brief" source for "4xx raise immediately" claim | VAL (finding remains CONFIRMED) |
| VAL-003 | EXT-004 | Cites `1198`s total backoff; recomputed ≈1038s (0.87x factor) | VAL (finding remains CONFIRMED) |
| VAL-004 | EXT-008 | Cites vendor `get_curl_request` at `net.py:445`; actual `net.py:402`, spans 402-448 | VAL (finding remains CONFIRMED) |

## Cross-Phase Analysis

| phase | finding | relationship | direction |
|---|---|---|---|
| ASY-001 | EXT-002 | Worker `task_done()` gap; `wait_for` timeout (EXT-002) raises unhandled `TimeoutError` — must ship with ASY-001's `try/finally: task_done()` fix | coupled (EXT-002 depends) |
| ASY-002 | EXT-003 | Worker does not propagate cancellation; EXT-003's status-reset bug compounds ASY-002's cancellation gap | amplifies |
| ASY-005 | EXT-004 | Deadline-aware polling vs. EXT-004's fixed `sleep_time=40` baseline — same `wait_task` path | overlapping |
| ASY-007 | EXT-001 | Metrics observability — ASY-007's no-metrics gap mirrors EXT-007 at the network layer | analogous |
| PIPE-002 | EXT-003 | `task.status` truthy-conflation — PIPE-002's extract-after-fail overlaps EXT-003's "готова" for failed tasks | overlapping |
| PIPE-006 | EXT-003 | Failed-task → `extract_data` path; EXT-003 enables PIPE-006 by not flipping status | root-cause |
| PIPE-007 | EXT-003 | CANCELLED→extract_data; EXT-003's missing terminal-status branch is prerequisite | dependent |
| CFG-003 | EXT-006 | `connection_config` path resolution feeds `MediaVortexTask` instantiation — config path affects per-report Session creation rate | supporting |
| ASY-005 | EXT-005 | Retry/backoff alignment — phase 03 ASY-005's retry-boundary discussion directly intersects EXT-005's 429/502/503/504 classification gap | coupled |

## Rollout Safety Assessment

| concern | status | notes |
|---|---|---|
| EXT-002 `wait_for` raises uncaught `asyncio.TimeoutError` | RISK | Must ship with ASY-001's `try/finally: task_done()` so queue is not poisoned. If EXT-002 ships alone, a timed-out task leaves the worker in a state where `task_done()` is never called → queue join hangs. |
| EXT-001 idempotency key requires server-side dedup | UNKNOWN | Vendor API contract must support `Idempotency-Key` header for safe retry. Must verify before implementation — cannot assume. |
| EXT-005 `call`-level 429 handling vs. vendor `urllib3.Retry` | RISK | If `call` adds 429 retry logic, ensure it co-ordinates with vendor's hidden `Retry(status_forcelist=[502,503,504])` — double-retry or conflict possible. 429 rate-limit response needs `Retry-After` parsing. |
| EXT-006 Session-per-report fix may change connection behavior | LOW | Adding `__exit__`/close to vendor `Session` — vendor `MediaVortexTask` is singleton; closing the Session does not destroy the instance. Safe if `close()` is idempotent. |
| EXT-003 status-reset fix blocks `extract_data` | RISK | Flipping `task.status=False` on FAILED/CANCELLED will gate `extract_data` — downstream CSV export path must handle `None`/empty DataFrame gracefully (verify `export_csv` null-safety). |
| EXT-008 PII redaction may break debuggability | LOW | Masking `userName`/body fields in error logs reduces forensic value; may need opt-in debug verbosity toggle. |
| Test coverage gaps | BLOCKER (test) | `test_network.py` has 1 test (ConnectTimeout only). No tests for: 429/5xx classification, timeout enforcement, idempotency, status-reset paths, metrics absence, PII redaction. Fixes for EXT-001/002/003/005/006/007/008 ship without test coverage → must add tests as part of fix PRs. |
| Order independence | SAFE | No circular dependencies among fixes. EXT-003 and ASY-001 are the most coupled (queue integrity). |

## Required Fixes (mandatory)

1. **EXT-001** — Add `Idempotency-Key` header to task-submission POST; verify vendor supports it before shipping.
2. **EXT-002** — Wrap `asyncio.to_thread(func, *args, **kwargs)` in `call` with `asyncio.wait_for(..., timeout=...)`. Ship with ASY-001's `try/finally: task_done()` to prevent queue poisoning.
3. **EXT-003** — In `wait_task`, reset `task.status = False` on FAILED/CANCELLED; gate `extract_data` on terminal-success status only.
4. **CFG-001/EXT-001** — Add idempotency test, timeout test, 429/5xx classification test, status-reset test, PII-redaction test.

## Advisory Recommendations

1. **EXT-004** — Replace fixed `sleep_time=40` with adaptive polling (decrease interval on rapid status changes, early-return on terminal status) — reduces API quota waste and failure-detection latency.
2. **EXT-005** — Align project-layer retry with vendor `urllib3.Retry` policy; add `Retry-After` parsing for 429; document retry boundary contract (which layer handles which status).
3. **EXT-006** — Add context-manager / `close()` to vendored Session lifecycle OR pool a single `MediascopeApiNetwork` per `AppService` lifetime — reduces per-report allocation overhead.
4. **EXT-007** — Add counters (attempts, retries, successes, failures) via `logging`-based or `prometheus_client`; export retry/backoff metrics to `log_config.yaml` structured output.
5. **EXT-008** — Redact `userName` and request bodies in error logs; add debug-level full-log opt-in via config flag.
6. **AGENTS.md:52** — Correct "client errors (4xx) raise immediately" to "4xx (except 429) raise immediately; 429 is retryable" for accuracy against vendor behavior.
