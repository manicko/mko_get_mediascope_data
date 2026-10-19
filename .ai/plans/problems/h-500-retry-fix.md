# Problem: Mass Task Failures from Server-Side Throttling (429) / Connection Resets

## Context
- **Date:** 2026-10-19
- **Component:** `core/network.py`, `core/reports.py`, `core/models.py`, `core/services.py`, `settings/app_config.json5`
- **Symptoms:** HTTP 429 (TooManyRequestsError), `RemoteDisconnected`, `ChunkedEncodingError`, `MaxRetriesExceeded` — mass task failures during `get_status` polling
- **Root cause:** Commit `131f125` (EXT-004) lowered `get_status` poll `sleep_time` from `40` → `2`, making requests ~20× more frequent and triggering server-side throttling

## Root Cause Analysis

### Primary regression — commit `131f125` (EXT-004)
- `wait_task` polls `get_status` via `network_client.call(self.report_service.get_status, task.key, sleep_time=2)` (reports.py:423)
- `sleep_time=2` feeds `NetworkClient.call`'s pre-call smoothing: `asyncio.sleep(uniform(1, 4))` (network.py:107)
- Plus the outer `asyncio.sleep(sleep_interval)` starts at 2.0s in the poll loop (reports.py:406)
- Effective poll gap ~2–6 seconds → ~10–40 polls per 40s task → server-side 429 rate limiting
- At `a9c20a3b` (before regression): `sleep_time=40` → smoothing `uniform(20, 80)`s → ~1–2 polls per 40s task → no throttling

### Secondary issue — `retry_5xx` default (commit `dd5dd8d`, EXT-005)
- `NetworkClient.__init__` has `retry_5xx: bool = False` (network.py:64)
- HTTP 500/502/503/504 (`ServerError`, subclass of `MediascopeApiError`) are re-raised immediately without retry
- Vendor `net.py` retries 502/503/504 at transport layer, but our client only catches `ServerError` when `retry_5xx=True`

### Tertiary issue — `IncompleteRead` not in `RETRIABLE_EXCEPTIONS`
- `requests.exceptions.IncompleteRead` has MRO `IncompleteRead → HTTPException → Exception`
- `HTTPException` is not in `RETRIABLE_EXCEPTIONS` → connection-reset-induced `IncompleteRead` is re-raised without retry
- Secondary: contributes to cascading failures under load

## User-Approved Design Decisions
1. **`get_status` `sleep_time=30`** — restores pre-EXT-004 value; pre-call smoothing `uniform(15, 60)`s
2. **Readiness-poll cadence: 30 → 50 → 60 (increasing)** — outer `asyncio.sleep` in `wait_task` starts at 30s, increases to 50s, then 60s (capped)
3. **Add `max_concurrent_requests` to `AppConfig`** — make HTTP semaphore configurable via `app_config.json5` (default `5`)

## Proposed Solution

### Change 1: Restore `get_status` `sleep_time=30` (fixes EXT-004)
- **File:** `src/mko_get_mediascope_data/core/reports.py`, line 423
- **Before:** `self.network_client.call(self.report_service.get_status, task.key, sleep_time=2)`
- **After:** `self.network_client.call(self.report_service.get_status, task.key, sleep_time=30)`
- **Rationale:** Restores pre-registration throttle behavior. With `sleep_time=30`, pre-call smoothing is `uniform(15, 60)`s, reducing poll frequency ~20×.

### Change 2: Readiness-poll cadence 30 → 50 → 60 (user-approved)
- **File:** `src/mko_get_mediascope_data/core/reports.py`, `wait_task` method
- **Before:** `sleep_interval = 2.0` (line 406), `sleep_interval = min(sleep_interval * 2, 60.0)` (line 442)
- **After:** `sleep_interval = 30.0` (start), incremental increase: 30 → 50 → 60, capped at 60.0
  ```python
  sleep_interval = 30.0
  # In loop, after each poll:
  if sleep_interval < 50.0:
      sleep_interval = 50.0
  elif sleep_interval < 60.0:
      sleep_interval = 60.0
  # Cap at 60.0 — already enforced by the elif above
  ```
- **Rationale:** API needs ~40s to process after task submission. Gentle ramp 30→50→60 avoids both initial over-polling and excessive wait for completion.

### Change 3: Restore `retry_5xx=True` default (fixes EXT-005)
- **File:** `src/mko_get_mediascope_data/core/network.py`, line 64
- **Before:** `retry_5xx: bool = False`
- **After:** `retry_5xx: bool = True`
- **Rationale:** 5xx errors are transient server errors that should be retried, not immediately raised. This restores pre-`dd5dd8d` behavior.

### Change 4: Add `max_concurrent_requests` to `AppConfig` / `AppSettings`
- **File:** `src/mko_get_mediascope_data/core/models.py`, `AppSettings` class
- **Add:** `max_concurrent_requests: PositiveInt = 5`
- **File:** `src/mko_get_mediascope_data/settings/app_config.json5`
- **Add:** `"max_concurrent_requests": 5` (commented or explicit)
- **File:** `src/mko_get_mediascope_data/core/services.py`, line 185
- **Before:** `network_client: NetworkClient = NetworkClient()`
- **After:** `network_client: NetworkClient = NetworkClient(max_concurrent_requests=self.app_config.app_settings.max_concurrent_requests)`
- **Rationale:** Makes HTTP concurrency configurable per the user-approved design.

### Change 5: Add `IncompleteRead` to `RETRIABLE_EXCEPTIONS`
- **File:** `src/mko_get_mediascope_data/core/network.py`, lines 32–44
- **Add:** `IncompleteRead` import from `requests.exceptions` and include in tuple
- **Rationale:** Connection resets under load can produce `IncompleteRead` which currently escapes retry. This is a robustness improvement.

### Change 6: Update tests
- **File:** `tests/test_network.py`
  - `test_call_5xx_not_retried_by_default` → `test_call_5xx_retried_by_default` (change `retry_5xx=False` assertion to `retry_5xx=True`)
  - `test_call_5xx_retried_when_enabled` — keep as-is (tests explicit `retry_5xx=True`)
- **File:** `tests/test_reports_lifecycle.py`
  - `test_wait_task_max_polls_limit` — verify `sleep_interval` starts at 30.0 (mock `asyncio.sleep` and check first call args)
- **File:** `tests/test_services.py`
  - Add test verifying `max_concurrent_requests` is read from `AppConfig`

### Change 7: Update documentation
- **File:** `docs/00-overview/async-concurrency-architecture.md` (lines 87, 136, 176, 204)
  - Update poll cadence from 1–4s to 15–60s
  - Update `retry_5xx` default from `False` to `True`
  - Update call-site table for `get_status`
- **File:** `docs/00-overview/current-behavior.md` (lines 169, 214–220, 335)
  - Update poll cadence description
  - Update NetworkClient defaults
- **File:** `docs/00-overview/system-architecture.md` (lines 214–220)
  - Update poll cadence description

## Files Affected
1. `src/mko_get_mediascope_data/core/network.py` — `retry_5xx` default, `RETRIABLE_EXCEPTIONS`
2. `src/mko_get_mediascope_data/core/reports.py` — `wait_task` poll cadence, `sleep_time=30`
3. `src/mko_get_mediascope_data/core/models.py` — `AppSettings` add `max_concurrent_requests`
4. `src/mko_get_mediascope_data/core/services.py` — `NetworkClient()` construction reads config
5. `src/mko_get_mediascope_data/settings/app_config.json5` — add `max_concurrent_requests`
6. `tests/test_network.py` — flip 5xx test assertions
7. `tests/test_reports_lifecycle.py` — verify poll cadence
8. `tests/test_services.py` — verify `max_concurrent_requests` from config
9. `docs/00-overview/async-concurrency-architecture.md` — update defaults/cadence docs
10. `docs/00-overview/current-behavior.md` — update poll cadence docs
11. `docs/00-overview/system-architecture.md` — update runtime knobs

## Verification
- `uv run ruff check src tests`
- `uv run pytest` (all existing tests must pass)
- `uv run mypy src tests` (no new errors in touched files)
- Confirm `sleep_time=30` and `sleep_interval=30.0` in code
- Confirm `retry_5xx=True` default
- Confirm `max_concurrent_requests` flows from `app_config.json5` → `AppSettings` → `NetworkClient`

## Approval
- User-approved design decisions (2026-10-19): `get_status sleep_time=30`; readiness-poll cadence 30 → 50 → 60 (increasing); add `max_concurrent_requests` to AppConfig
- `retry_5xx=True` and `IncompleteRead` inclusion combined from both solutions per user: "We need to get best of 2 solutions - your and the one i sent"

## Implementation (applied)

All changes applied and verified:

| # | File | Change |
|---|---|---|
| 1 | `core/reports.py:406` | `sleep_interval = 2.0` → `30.0` |
| 2 | `core/reports.py:423` | `sleep_time=2` → `sleep_time=30` |
| 3 | `core/reports.py:442-447` | `sleep_interval * 2` doubling → 30→50→60 ramp |
| 4 | `core/network.py:62` | `retry_5xx: bool = False` → `True` |
| 5 | `core/network.py:24` | Added `IncompleteRead` to imports from `urllib3.exceptions` |
| 6 | `core/network.py:42` | Added `IncompleteRead` to `RETRIABLE_EXCEPTIONS` |
| 7 | `core/models.py:31` | Added `max_concurrent_requests: PositiveInt = 5` to `AppSettings` |
| 8 | `core/services.py:185` | `NetworkClient()` reads `max_concurrent_requests` from config |
| 9 | `settings/app_config.json5` | Added `"max_concurrent_requests": 5` |
| 10 | `tests/test_network.py` | Flipped 5xx tests (retried by default, opt-out when disabled); added `test_call_retries_on_incomplete_read` |
| 11 | `tests/test_reports_lifecycle.py` | Added `test_wait_task_poll_cadence_starts_at_30` |
| 12 | `tests/test_services.py` | Added `test_app_config_max_concurrent_requests_default`; extended `test_app_config_is_cached` |
| 13 | `tests/conftest.py` | `_APP_CONFIG_JSON5` includes `max_concurrent_requests` |
| 14 | `docs/*.md` (3 files) | Updated poll cadence, retry_5xx default, max_concurrent_requests docs |

### Verification
- `uv run ruff check src tests` → all checks passed
- `uv run pytest` → 115 passed
- `uv run mypy src/mko_get_mediascope_data/core/{network,models,reports,services}.py` → 0 errors
- `retry_5xx=True` and `IncompleteRead` inclusion are secondary fixes that support the primary throttle fix
