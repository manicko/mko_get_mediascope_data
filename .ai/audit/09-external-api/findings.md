---
phase: "09"
phase_name: "external-api"
id_prefix: "EXT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/09-audit-external-api.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — External Integrations & API

## Summary

8 findings (1 CRITICAL, 3 HIGH, 3 MEDIUM, 1 LOW). The Mediascope integration can duplicate billable compute tasks on a lost response, silently mark a far-side FAILED task as "ready", or hang an entire report run — while emitting no metrics to distinguish a slow API from a deadlocked one. The CRITICAL is the retry layer re-POSTing a non-idempotent task-creation write after the server already accepted it.

## Scope

**Examined:** `core/network.py` (`NetworkClient.call` retry/backoff/semaphore/to_thread), `core/reports.py` (`TVMediaReport.send_task`/`wait_task`/`extract_data`/`worker`/`processing_tasks`, `MediaReport.__exit__`), `core/services.py` (`AppService.get_connection`), `core/tasks.py`/`core/paths.py`/`app.py`; vendored Mediascope client `mediascope_api/core/net.py` (`send_request`/`get_token`/`MediascopeApiNetwork.__init__`) and `mediascope_api/mediavortex/tasks.py` (`MediaVortexTask`/`_send_task`/`get_status`/`get_result`/`result2table`).

**Tools:** `grep` (content search), `uv run python` (formula inspection), `uv run pytest tests/test_network.py`.

**Not verified:** live Mediascope API (no credentials); server-side task de-duplication on re-POST; server behaviour of `/task/result/{id}` for a FAILED task; bearer-token lifecycle. Source/constructor analysis used in their place.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| EXT-001 | CRITICAL | `core/network.py::NetworkClient.call` | Retry replays the non-idempotent task-creation POST — duplicates a remote task on a lost response |
| EXT-002 | HIGH | `core/network.py::NetworkClient.call` | No request timeout on the `to_thread` bridge — a hung server stalls a worker thread indefinitely |
| EXT-003 | HIGH | `core/reports.py::TVMediaReport.wait_task` | FAILED/CANCELLED terminal statuses conflated with DONE — failed task proceeds as "ready" |
| EXT-004 | HIGH | `core/reports.py::TVMediaReport.wait_task` | `get_status` called with `sleep_time=40` — 20–80s stall per poll, ~22min retry window |
| EXT-005 | MEDIUM | `core/network.py::NetworkClient.call` | HTTP 429/500 not retried; 502/503/504 hidden behind an undocumented second `urllib3.Retry` layer |
| EXT-006 | MEDIUM | `core/services.py::AppService.get_connection` | New `requests.Session` per report, never closed — connection/FD leak in long-running use |
| EXT-007 | MEDIUM | `core/network.py::NetworkClient.call` | No metrics/counters for retry, exhaustion, or dropped state — only unstructured logs |
| EXT-008 | LOW | `core/network.py::NetworkClient.call` | Error handlers emit full `args`/`kwargs` (incl. report payload) with no sanitiser |

#### EXT-001 — [CRITICAL] Retry replays the non-idempotent task-creation POST

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `asyncio.to_thread(func, *args, **kwargs)`
**Checked:** static
**Why this band:** CRITICAL = "a retryable write that is not idempotent, so a lost response triggers a duplicate side effect at the far side." `send_task` POSTs to `/task/{type}` (creates a billable remote task); `NetworkClient.call` retries `RETRIABLE_EXCEPTIONS` (transport errors raised when the server already accepted the request); no idempotency key exists.
**Problem:** `call` (network.py:71-124) retries `ConnectTimeout, ConnectionError, Timeout, RetryError, ChunkedEncodingError, ConnectionResetError, socket.timeout, ProtocolError, NameResolutionError, socket.gaierror` (network.py:24-35). `TVMediaReport.send_task` (reports.py:263) routes the POST through `await self.network_client.call(self.sender, task_json)` where `self.sender = report_service.send_task`. The vendored `_send_task` POSTs the same body each time (`send_request('post', ...)`); a `ConnectionResetError`/`ChunkedEncodingError` raised by `requests` *after* the server accepted the request is retriable, so the POST is re-issued → a second `taskId`/second compute. The vendor sets only `Authorization` + `Content-Type` headers (no `Idempotency-Key`).
**Repro:** `NetworkClient.call(report_service.send_task, task_json)` with a `ConnectionResetError` injected during response reading → `call` re-invokes `send_task(task_json)` up to `max_attempts`.
**Impact:** A dropped response duplicates Mediascope compute tasks (billable, parallel) and may produce duplicate result sets; the duplicate is silent — only a `WARNING` is logged, and `task.key` is taken from whichever response returns last.
**Root cause:** The retry surface does not distinguish reads from writes; the client provides no write idempotency.
**Fix:** Make `send_task`'s POST non-retryable — route it through a dedicated non-retried path (or pass `retryable=False`), so only idempotent GETs/status polls are retried. Target symbol: `TVMediaReport.send_task`.
**Verify:** new test needed: `test_send_task_not_retried_on_connection_reset`
**Related:** EXT-002, EXT-007

**Evidence (`reports.py` routes the POST through the retried `call`):**
```text
core/reports.py:110:     self.builder = self.report_service.build_task
core/reports.py:111:     self.sender = self.report_service.send_task
core/reports.py:263:     task.key = await self.network_client.call(self.sender, task_json)
```
**Evidence (retry-eligible transport exceptions, `network.py:24-35, 71-74`):**
```text
core/network.py:24:  RETRIABLE_EXCEPTIONS = (
core/network.py:25:      ConnectTimeout, ConnectionError, Timeout, RetryError,
core/network.py:26:      ChunkedEncodingError, ConnectionResetError, socket.timeout,
core/network.py:29:      socket.gaierror,
core/network.py:74:     return await asyncio.to_thread(func, *args, **kwargs)
```

#### EXT-002 — [HIGH] No request timeout on the `to_thread` bridge

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `asyncio.to_thread(func, *args, **kwargs)`
**Checked:** static
**Why this band:** HIGH = "a per-process bound that is unbounded … stall." `call` runs the vendored `requests.Session` call via `asyncio.to_thread` (network.py:74) with no `asyncio.wait_for`; the vendor passes no `timeout=` to `requests` (default `None` = infinite). A server that accepts the TCP connection but never responds hangs the worker thread with no exception — so retry never fires and the call stalls forever.
**Problem:** `return await asyncio.to_thread(func, *args, **kwargs)` (network.py:73-74) is the only bridge to the synchronous client. The vendor `send_request` (`mediascope_api/core/net.py:192`) calls `getattr(self.session, method)(url=url, headers=..., data=..., proxies=...)` with **no `timeout=`**; `grep` for `timeout` in `mediascope_api/core/net.py` returns no match. `get_token` (`mediascope_api/core/net.py:103`) is equally timeout-free. Because retry is triggered only by *raised* exceptions, a silent hang neither retries nor logs.
**Repro:** Point `MediaVortexTask` at a TCP server that accepts but never replies; `NetworkClient.call(get_status, key, sleep_time=40)` never returns or raises.
**Impact:** A hung Mediascope endpoint pins a thread and a Semaphore slot indefinitely. With `max_concurrent_requests=5`, all five network slots can be consumed by hung calls, and with ASY-001 the worker also skips `task_done()` → `queue.join()` hangs.
**Root cause:** No end-to-end deadline on the blocking bridge; timeout responsibility delegated to a vendor that never sets one.
**Fix:** Wrap `asyncio.to_thread(...)` in `asyncio.wait_for(..., timeout=...)` (add a `request_timeout` ctor arg) and pass `timeout=` through to the vendor session where possible. Target symbol: `NetworkClient.call`.
**Verify:** new test needed: `test_call_applies_wait_for_deadline`
**Related:** ASY-001, ASY-002

**Evidence — `to_thread` with no deadline (`network.py:73-74`):**
```text
core/network.py:73:     async with self._semaphore:
core/network.py:74:         return await asyncio.to_thread(func, *args, **kwargs)
```

#### EXT-003 — [HIGH] FAILED/CANCELLED conflated with DONE

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task`
**Disambiguator:** `("DONE", "FAILED", "CANCELLED")`
**Checked:** static
**Why this band:** HIGH = "an operation reports success while the far side never received it" / "a substituted result stored where a real one was required and nothing can tell them apart." `wait_task` breaks on `DONE`, `FAILED`, and `CANCELLED` identically and leaves `task.status = True`; a failed/cancelled remote task proceeds to `extract_data` and may write a CSV logged as `готова` (ready).
**Problem:** `wait_task` (reports.py:277-293) does `if status.get("taskStatus") in ("DONE", "FAILED", "CANCELLED"): break` (reports.py:292) with no branch on the value and no `task.status = False`. `worker` then runs `extract_data` (reports.py:146-147) and, on the success branch, logs `Задача {item.name} готова.` (reports.py:152). A `FAILED`/`CANCELLED` task never produced valid data, yet local state marks it ready and still calls `/task/result/{id}`.
**Repro:** Drive `wait_task` with `taskStatus == "FAILED"`; assert `task.status` stays `True` (no error flag) and `extract_data` is still invoked.
**Impact:** A remote task that fails server-side is reported locally as ready with a CSV in the export folder; downstream consumers (Jupyter/Airflow reads) treat it as authoritative. Silent: no error YAML, no exception.
**Root cause:** The status contract conflates terminal-success with terminal-failure.
**Fix:** Branch on the terminal status in `wait_task`: set `task.status = False`, `task.log_error = True`, `task.error = "Remote task FAILED/CANCELLED"` for `FAILED`/`CANCELLED`; break only for `DONE`. Target symbol: `TVMediaReport.wait_task`.
**Verify:** new test needed: `test_wait_task_flags_remote_failed_and_cancelled`
**Related:** PIPE-006 (same loop — owns the no-deadline angle), PIPE-007

**Evidence (`reports.py:289-293`):**
```text
core/reports.py:289:     elif isinstance(status, dict):
core/reports.py:292:         if status.get("taskStatus", None) in ("DONE", "FAILED", "CANCELLED"):
core/reports.py:293:             break
```

#### EXT-004 — [HIGH] `get_status` with `sleep_time=40` amplifies backoff

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task`
**Disambiguator:** `sleep_time=40`
**Checked:** static (formula verified; `test_network_retry_and_fail` measured 19.77s for 3 attempts at `sleep_time=0`)
**Why this band:** HIGH = "a per-process bound that is unbounded once a single invocation multiplies its own retries." `get_status` is called via `NetworkClient.call(..., sleep_time=40)`, so `_sleep_time = max(40, 2) = 40`. The backoff base becomes `40 * 1.2^(attempt-1)` with a silent `uniform(20, 80)` initial sleep, inside `wait_task`'s unbounded `while True`.
**Problem:** `call` (network.py:66-69) sets `_sleep_time = max(sleep_time, self._sleep_time)` = 40, then `await asyncio.sleep(uniform(40*0.5, 40*2))` = **20–80s before the FIRST status attempt, on a healthy network** (not logged). The retry tail (network.py:121-124: `40 * 1.2^(attempt-1) + uniform(0,5)`) reaches ~22 minutes per poll across `max_attempts=10`. The `while True` has no overall deadline, so the cost multiplies unbounded.
**Repro:** `NetworkClient(max_attempts=10, sleep_time=2).call(failing_func, sleep_time=40)`; worst case ≈ `uniform(20,80)`(≤80) + `Σ_{a=1}^{10}(40·1.2^(a−1)+5)` ≈ 80 + 1198 + 50 ≈ 1328s (~22min).
**Impact:** Every status poll burns 20–80s even on a healthy API (throughput collapse for multi-task reports); a degraded API stalls a Semaphored slot ~22 min per poll with no operator signal.
**Root cause:** `sleep_time=40` is passed to a parameter whose semantics are backoff-base, not poll-interval; the two concepts are conflated.
**Fix:** Remove `sleep_time=40` from the `get_status` call (let the default apply), or separate poll-interval from retry-backoff. Target symbol: `TVMediaReport.wait_task`.
**Verify:** new test needed: `test_get_status_does_not_amplify_backoff_base`
**Related:** ASY-005, PIPE-006

**Evidence (`reports.py:280-282` call site; `network.py:66-74, 121-124` formula):**
```text
core/reports.py:280:     status = await self.network_client.call(
core/reports.py:281:         self.report_service.get_status, task.key, sleep_time=40
core/reports.py:282:     )
core/network.py:66:     _sleep_time = max(sleep_time, self._sleep_time)
core/network.py:69:     await asyncio.sleep(uniform(_sleep_time * 0.5, _sleep_time * 2))
core/network.py:122:    backoff = _sleep_time * (base ** (attempt - 1))
core/network.py:124:    await asyncio.sleep(backoff + jitter)
```

#### EXT-005 — [MEDIUM] HTTP 429/500 not retried; 502/503/504 behind an undocumented second retry layer

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `RETRIABLE_EXCEPTIONS`
**Checked:** static
**Why this band:** MEDIUM = "a guard applied on one path and absent from a sibling reaching the same dependency." `call` retries only transport-level `requests` exceptions. The vendor raises HTTP-status errors as `MediascopeApiError` subclasses (not `HTTPError`), so `except HTTPError` (network.py:101) never matches; `TooManyRequestsError` (429) and `ServerError`/`MediascopeApiError` (500…) fall to `except Exception` (network.py:113) and are re-raised immediately. The only 5xx retries come from a second, invisible `urllib3.Retry` on the vendor session.
**Problem:** The brief claims "Transient errors (timeouts, connection errors) are retried"; true for transport errors, but: (a) 429 (`mediascope_api/core/errors.py:53-59`) and 500 (`mediascope_api/core/errors.py:78-83`) are not in `RETRIABLE_EXCEPTIONS`, are not `HTTPError`, and are re-raised — no backoff; (b) 502/503/504 are retried only by the vendor's `Session` `Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])` (`mediascope_api/core/net.py:75-82`) — a second retry layer the project neither configures nor surfaces. A single logical call can retry 5×5×10 across two strategies, only the outer retries logged.
**Repro (static):** `send_request` raises `MediascopeApiError`, not `HTTPError`; 429/500 land in `except Exception` (network.py:113) → `raise`.
**Impact:** Rate-limited (429) or generic-server-error (500) requests fail the task immediately where a `Retry-After`-respecting backoff would recover; the hidden inner layer makes total retry cost non-deterministic.
**Root cause:** Transport exceptions and HTTP-status errors are handled by disjoint, uncoordinated retry mechanisms; the `HTTPError` branch assumes a vendor exception shape that never occurs.
**Fix:** Translate vendor HTTP-status errors into the retryable set (or catch `TooManyRequestsError`/`ServerError`), honour `Retry-After` on 429, and own all retries in one layer. Target symbol: `NetworkClient.call` / `RETRIABLE_EXCEPTIONS`.
**Verify:** new test needed: `test_429_and_500_retried_with_backoff`
**Related:** EXT-001, EXT-007

**Evidence — vendor session Retry, never tuned by the project.** (Vendor file is the *installed* package `mediascope_api`, git-ignored under `.venv`; verified by reading the installed source, not run. `grep` for `timeout` in `mediascope_api/core/net.py` returns no match.) `mediascope_api/core/net.py:73-82`:
```text
mediascope_api/core/net.py:73:     self.session = Session()
mediascope_api/core/net.py:76:         total=5,
mediascope_api/core/net.py:77:         backoff_factor=1,
mediascope_api/core/net.py:78:         status_forcelist=[502, 503, 504],
mediascope_api/core/net.py:82:     self.session.mount('https://', HTTPAdapter(max_retries=self.retries))
```
**Evidence — project only matches `HTTPError`, never vendor subclasses (`network.py:95-118`):**
```text
core/network.py:95:      except RETRIABLE_EXCEPTIONS as err:   # transport only
core/network.py:101:     except HTTPError as err:              # vendor never raises HTTPError
core/network.py:113:     except Exception as err:              # 429/500 land here -> raise
```

#### EXT-006 — [MEDIUM] New `requests.Session` per report, never closed

**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Disambiguator:** `settings_filename=self.app_paths.connection_config`
**Checked:** static
**Why this band:** MEDIUM = "a bounded resource … gap." `get_connection` builds a fresh `MediaVortexTask` (and `MediascopeApiNetwork`/`requests.Session`+pool) per report; nothing closes it; `MediaReport.__exit__` is a no-op. In the documented long-running paths (Jupyter/Airflow, `run_report`/`run_report_async`) sessions accumulate.
**Problem:** `run_report` (services.py:123-134) loops report definitions and calls `get_connection` per report; `get_connection` (services.py:100-108) does `return connection_service(settings_filename=..., check_version=False)` where `connection_service = MediaVortexTask`. The vendor `MediaVortexTask.__init__` creates `net.MediascopeApiNetwork(...)` (`mediascope_api/core/net.py:43-82`) → `Session()` + `HTTPAdapter(max_retries=...)`; the vendor class has no `close`/context manager. `MediaReport.__exit__` (reports.py:119-120) is `pass`. The Session is dereferenced but never `.close()`d; cleanup is left to GC.
**Repro:** Run a multi-report YAML via `run_report`; after completion the per-report Sessions are not closed.
**Impact:** In a long-lived Jupyter/Airflow process, each report leaks a connection pool (and its FDs / keep-alive sockets); over many reports this can exhaust FDs/sockets; a stale token cached on a discarded Session is never invalidated.
**Root cause:** No lifecycle hook closes the Mediascope connection; the report object does not own cleanup of its `report_service`.
**Fix:** Acquire the connection in a context manager / `try/finally` that closes the Session, or reuse one `MediaVortexTask` across reports instead of rebuilding. Target symbol: `AppService.get_connection` + the `run_report` loop.
**Verify:** new test needed: `test_get_connection_session_closed_after_report`
**Related:** CFG-003 (credential resolution, same call site)

**Evidence — per-report construction with no cleanup (`services.py:100-108, 131-134`):**
```text
core/services.py:102:     try:
core/services.py:105:         return connection_service(
core/services.py:106:             settings_filename=self.app_paths.connection_config,
core/services.py:107:             check_version=False,
core/services.py:108:         )
core/services.py:131:     report_service: MediaVortexTask = await network_client.call(
core/services.py:132:         self.get_connection,
```
**Evidence — no close path (`reports.py:119-120`, vendor has no `close`):**
```text
core/reports.py:119:     def __exit__(self, exc_type, exc_val, exc_tb):
core/reports.py:120:         pass
```

#### EXT-007 — [MEDIUM] No metrics/counters for retry/exhaustion/dropped state

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `attempt {attempt}/{self._max_attempts}`
**Checked:** static
**Why this band:** MEDIUM = "a fallback with a log but no counter." `call` emits unstructured `logging.warning`/`logger.exception` on retries and exhaustion, but exposes no counters; a retried call, an exhausted retry, or a dropped request is only a log line.
**Problem:** The only signal of retry/exhaustion is `logger.warning("Network error … attempt {attempt}/{max}: {err}")` (network.py:96-99) and `logger.exception` (network.py:114-117); the terminal state is `raise RuntimeError("Network retry limit exceeded")` (network.py:126). No counters, no structured fields (exception type / attempt as labels), no per-call outcome metric. `log_config.yaml` defines only file/stream handlers (see phase 02), not metrics. A quietly-degraded dependency (retrying but eventually succeeding) is indistinguishable from a healthy one.
**Repro:** Drive `call` with a func that fails twice then succeeds; only two `WARNING` lines appear — no counter is incrementable because none exists.
**Impact:** Operators cannot alert on retry rates, exhaustion, or per-endpoint failure; the integration's health is only diagnosable by tailing logs. This compounds EXT-002/EXT-004 (a hung/stalling call produces no metric).
**Root cause:** The integration layer reasons only in `logging`, never emits structured metrics.
**Fix:** Emit structured records (counters/labels for `attempt`, `outcome` ∈ success/retry/exhausted, `func.__name__`) rather than free-text logs. Target symbol: `NetworkClient.call` error/retry branches.
**Verify:** new test needed: `test_retry_outcome_emits_structured_fields`
**Related:** ASY-006 (queue-depth observability), EXT-002, EXT-008

**Evidence — only unstructured logs, no counters (`network.py:95-126`):**
```text
core/network.py:96:      logger.warning(
core/network.py:98:          f"attempt {attempt}/{self._max_attempts}: {err}"
core/network.py:114:     logger.exception(
core/network.py:126:     raise RuntimeError("Network retry limit exceeded")
```

#### EXT-008 — [LOW] Error handlers dump full `args`/`kwargs` with no sanitiser

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `with args="{args}"`
**Checked:** static
**Why this band:** LOW = "observability drift with no runtime consequence today." On error the handlers log the verbatim `args`/`kwargs` of the wrapped call. Currently no *credentials* flow through those parameters, so there is no active credential leak — but the logging is unsanitised and one new caller passing a secret in kwargs would leak it.
**Problem:** `call` logs `f'… with args="{args}" and kwargs="{kwargs}": {err}'` on `AuthorizationError`/`FileNotFoundError`/`Exception` (network.py:84, 91, 116). The wrapped `send_task` receives `args=(task_json,)` — the full report payload (slices, statistics, `options` incl. `kitId`, demographic filters produced by `build_task`, reports.py:258-260) — which is written to the log file on any error. `get_status` receives `task.key` (which contains `userName`, the Mediascope login). The bearer token itself is held in plaintext on `MediascopeApiNetwork.token` and `get_curl_request` (net.py:445) prints it, but that method is not reached from the project's call graph.
**Repro:** Force an `AuthorizationError`/`Exception` from `send_task`; the error log contains the report payload; a `get_status` error logs `userName`.
**Impact:** Report definitions and a Mediascope login name are persisted to the log file on every API error — minor information exposure. No active credential leak is reachable today, but the handler is one caller away from logging a secret.
**Root cause:** No log sanitiser / field redaction around the per-call argument dump.
**Fix:** Redact `args`/`kwargs` (or log only `func.__name__` + exception type/message), and never log request bodies. Target symbol: the three error handlers in `NetworkClient.call`.
**Verify:** new test needed: `test_error_handler_does_not_log_request_body`
**Related:** EXT-007

**Evidence — verbatim args/kwargs in error logs (`network.py:82-93, 113-118`):**
```text
core/network.py:84:      f'with args="{args}" and kwargs="{kwargs}": {err}'
core/network.py:91:      f'with args="{args}" and kwargs="{kwargs}"'
core/network.py:116:     f'with args="{args}" and kwargs="{kwargs}": {err}'
```

## Fix Order

- **Dependencies:** EXT-002 (add `wait_for` deadline on the bridge) should ship before or with any retry-count change so a hung call can't consume all retries. EXT-001 (make the POST non-retryable) and EXT-005 (retry-class fix) both touch `NetworkClient.call`'s retry boundary — align in one change.
- **Merge candidates:** EXT-002 + EXT-004 + EXT-003 + EXT-007 + EXT-008 are all edits to `NetworkClient.call` / `wait_task` — one coherent PR.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:**
  - EXT-002/EXT-004/EXT-003 touch `wait_task`/`call` also addressed by phase 03 (ASY-001, ASY-005) and phase 04 (PIPE-002, PIPE-006, PIPE-007). This phase claims only the *outbound-integration* mechanics (retry eligibility, backoff coefficient, deadline on the bridge, terminal-status contract, log/counter surface); the worker-lifecycle isolation (`task_done`/`finally`, poll timeout) remains phase 03/04's fix.
  - EXT-007 (metrics) overlaps phase 12 (observability stack); this phase files only the integration-layer signal gap.
  - EXT-006 touches `get_connection`, also claimed by phase 02 (CFG-003); fix the close path alongside the path-resolution fix.
- **Rollout risk:** EXT-001's fix (stop retrying the POST) changes success semantics — a `ConnectionError` on submit now fails the task instead of silently duplicating it. No shipped test asserts duplicate-POST behaviour (`test_network.py` only tests `ConnectTimeout`; `test_services.py` mocks `create_report`). EXT-002's new `wait_for` raises `TimeoutError`, which `worker` does not catch — must ship with the ASY-001 `task_done` fix or it re-creates the hang.

## Appendices

### Appendix A — Credential seam (block 1): no active leak; residual unverified

Outbound call set (from `mediascope_api/core/net.py`): `get_token` (POST `keycloak_url`) and `send_request` (POST/GET to `root_url + endpoint`).

| call | credential holding surface | wire form |
|---|---|---|
| token request `get_token` (net.py:103) | `self.username/passw/client_id/client_secret` plaintext attrs (net.py:43-48) | request **body** (form-urlencoded) |
| API calls `send_request` (net.py:188-193) | `self.token['access_token']` (net.py:63) | `Authorization: Bearer …` **header** |

States: **absent** → `load_settings` `FileNotFoundError` / vendor `ValueError`, re-raised by `call` (FileNotFoundError handler, network.py:88-93, not retried); **empty key** → `KeyError` in `load_settings` → `except Exception` → re-raised; **malformed** → `ValueError`/`KeyError` → re-raised. All three fail **closed** (raise). On current call paths no credential reaches `logger`: `get_connection` args=(MediaType,), `send_task` args=(report payload, **not** credentials), `get_status` args=(task.key,).

Vendor evidence in this report is drawn from the *installed* `mediascope_api` package (git-ignored under `.venv`); assertions about it were verified by reading the installed source. Project-side assertions cite git-tracked paths under `src/mko_get_mediascope_data/`.

Residual unverified: (1) whether 401 response `req.text` ever echoes user-identifying data (logged via `{err}`, network.py:84/116); (2) server-side token revocation/rotation; (3) `get_curl_request` (net.py:445) prints the bearer token but is **not** called by the project. Secret-at-rest in the plaintext credentials file (`connections/mediascope_RENAME_.json`, 7 keys) is owned by phase 02 (CFG-003).

### Appendix B — Reproduction: measured backoff magnitude

`uv run pytest tests/test_network.py -v`:
```text
tests/test_network.py::test_network_retry_and_fail PASSED in 19.77s
```
At the site `sleep_time=40`, worst-case single call ≈ `uniform(20,80)`(≤80) + `Σ_{a=1}^{10}(40·1.2^(a−1) + 5)` ≈ 80 + 1198 + 50 ≈ 1328s (~22 min).

### Appendix C — Cross-phase references (already filed, not re-filed here)

- **ASY-001** (phase 03): worker has no `try/except/finally` → exception skips `task_done()` → `queue.join()` deadlock. EXT-002/EXT-004 describe the integration triggers that *cause* that hang but do not re-file the worker fix.
- **ASY-002** (phase 03): `TVRegTaskStrategy.generate` calls `catalogs.get_tv_region()` synchronously (block 4 notes it; not re-filed).
- **ASY-005** (phase 03) / **PIPE-006** (phase 04): `wait_task` `while True` has no deadline/iteration cap.
- **PIPE-002** (phase 04): raised exceptions bypass `task_done`; **PIPE-007**: raised exceptions bypass `TVTask.to_yaml`.
- **CFG-003** (phase 02): credential template name ≠ path the code reads.
