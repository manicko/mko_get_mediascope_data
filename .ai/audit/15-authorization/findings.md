---
phase: "15"
phase_name: "authorization"
id_prefix: "AUT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/15-audit-authorization.md#severity-taxonomy"
code_ref: "d093d9c"
---

# Audit Findings — Authorization & Credential Lifecycle

## Summary

Three findings, highest severity HIGH. The tool fetches Mediascope reports through a
`NetworkClient` that declares a token-refresh hook it never implements and a 403 path it
never classifies, and it validates no credential scope before emitting data — so an
under-scoped credential can silently yield a partial report presented as complete.

## Scope

**Examined:** `core/network.py` (`NetworkClient.call` retry/error classification),
`core/reports.py` (`TVMediaReport` send/wait/extract pipeline + `TVRegTaskStrategy.generate`),
`core/services.py` (`AppService.get_connection` / `run_report`), `core/tasks.py`
(`TVTask.to_yaml`), `core/paths.py` (`connection_config`), `app.py` (error output),
`settings/connections/*`, `settings/log_config.yaml`, plus the auth backend
`mediascope_api/core/errors.py` and `mediascope_api/core/net.py` (`refresh_token`, `_raise_error`).
**Tools:** `uv run python` (runtime repro + `inspect`), `grep` (scope/permission scan),
`read`, `git ls-files` / `git log`.
**Not verified:** live 401/403 against the Mediascope API (no credentials present);
credential-at-rest protection (owned by Phase 02, out of scope here).

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| AUT-001 | HIGH | `core/network.py::NetworkClient.call` | Declared `refresh_token_func` hook unimplemented; no refresh-on-401 |
| AUT-002 | MEDIUM | `core/network.py::NetworkClient.call` | 403 `AccessForbiddenError` has no handler, misrouted to "Unexpected error" |
| AUT-003 | MEDIUM | `core/reports.py::TVRegTaskStrategy.generate` | No credential-scope validation; under-scoped credential yields silent partial data |

#### AUT-001 — [HIGH] Declared `refresh_token_func` hook unimplemented; no refresh-on-401

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient`
**Disambiguator:** `"AuthorizationError"`
**Checked:** ran
**Why this band:** matches taxonomy "declared token-refresh hook that is unimplemented,
leaving expiry or rotation unhandled." NOT CRITICAL because the 401 is *not* retried as
transient (re-raised on attempt 1, no backoff spin); NOT MEDIUM because the hook is not
wired at all (only a TODO), which is the stricter HIGH case.
**Problem:** `NetworkClient.call` catches `AuthorizationError` (401) and re-raises
immediately (network.py `except AuthorizationError` block) with no refresh attempt and no
retry. The refresh hook is declared only as a TODO in the `NetworkClient` docstring
(`refresh_token_func: Callable | None = None`); `__init__` exposes no such parameter.
The only refresh logic is `mediascope_api`'s `refresh_token()` (net.py:127-138), which
refreshes only on `expires_in` timing, never on a 401, so a revoked/clock-skewed token
that still appears unexpired cannot be recovered and the whole run fails.
**Repro:** Construct `NetworkClient(max_attempts=3, sleep_time=0)`; invoke `call` with a
func raising `AuthorizationError`; observe attempts=1 and immediate re-raise
(see Appendix A). `inspect.signature(NetworkClient.__init__)` shows no `refresh_token_func`
param (see Appendix A).
**Impact:** Operators must re-run and re-authenticate a full report run on any mid-session
token invalidation (revocation, clock skew); the declared recovery path is dead code.
**Root cause:** Refresh-on-auth-failure was parked as a docstring TODO and never integrated
into `NetworkClient.call`, despite `AuthorizationError` being a distinct, non-retriable
class.
**Fix:** Add `refresh_token_func: Callable[[], Awaitable[None]] | None = None` to
`NetworkClient.__init__` (network.py::NetworkClient) and, in `call`'s `AuthorizationError`
branch, invoke it once and retry the call before re-raising.
**Verify:** new test needed: `test_401_invokes_refresh_hook_and_retries_once`
**Related:** AUT-002, AUT-003

#### AUT-002 — [MEDIUM] 403 `AccessForbiddenError` has no handler, misrouted to "Unexpected error"

**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `"except Exception"`
**Checked:** ran
**Why this band:** the 403 failure class is not observably distinct from a genuine bug — it
is logged as "Unexpected error" with a full traceback. NOT HIGH because 403 is *not*
retried as a transient network error (re-raised on attempt 1); NOT LOW because it has a
runtime consequence (misleading log + traceback persisted to `log.log`).
**Problem:** `network.py` imports only `AuthorizationError, BadRequestError` (line 10);
`AccessForbiddenError` (mediascope_api 403) is never imported or caught. It falls through to
the catch-all `except Exception` branch (network.py:113-118), logged via `logger.exception`
("Unexpected error … with args/kwargs: {err}") with a full traceback, then re-raised. A
permission denial is therefore indistinguishable in logs from an unexpected crash.
**Repro:** Invoke `call` with a func raising `AccessForbiddenError`; observe it is logged as
"Unexpected error" with a traceback and re-raised on attempt 1 (Appendix A). Static check:
`AccessForbiddenError in dir(network_module)` → `False` (Appendix A).
**Impact:** Operators diagnosing a permission denial see a generic "Unexpected error" +
traceback instead of a permission-failure signal; triage treats a routine 403 as a bug.
**Root cause:** The except-clause ordering omits the mediascope_api 403 class, leaving it in
the catch-all.
**Fix:** Import `AccessForbiddenError` and add a dedicated `except AccessForbiddenError`
branch in `NetworkClient.call` that logs a distinct permission-failure message and
re-raises without retry.
**Verify:** new test needed: `test_403_not_retried_and_logged_as_permission_failure`
**Related:** AUT-001; reports.py::worker (`send_task`/`wait_task`/`extract_data` have no
try/except, so an unhandled 401/403 also drops `task_done()` and stalls
`task_queue.join()` — a separate error-handling gap).

#### AUT-003 — [MEDIUM] No credential-scope validation; under-scoped credential yields silent partial data

**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVRegTaskStrategy.generate`
**Anchor:** `src/mko_get_mediascope_data/core/services.py::AppService.get_connection`
**Disambiguator:** `"get_tv_region"`
**Checked:** static
**Why this band:** matches taxonomy "scope mismatch not detected, so an under-scoped
credential returns partial data without warning." NOT HIGH (no 401-retried-as-transient);
NOT LOW (runtime consequence: silent partial data treated as complete).
**Problem:** No scope/authority probe exists anywhere in `src/` (grep for scope|permission
returned no hits). For TV_REG, `TVRegTaskStrategy.generate` builds the region list directly
from `report.catalogs.get_tv_region()` — i.e. only the regions the credential can see — and
iterates those with no check that the expected/requested region set was fully returned. An
under-scoped credential therefore yields a partial report with no warning. A scope mismatch
is not distinguishable from a normal run (silent partial) or an outright 401/403.
**Repro (static):** reports.py:387-391 — `regions_list = report.report_settings.model_dump().get("regions") or available_regions["id"].to_list()`; the loop is silent about any absent regions.
**Impact:** Sellers can publish an incomplete TV_REG export believing it is complete,
attributing missing regions to the dataset rather than to credential scope.
**Root cause:** Scope is delegated entirely to the API's per-call permission check with no
project-level validation or completeness assertion.
**Fix:** Add a scope probe in `AppService.get_connection` (services.py::AppService.get_connection)
that enumerates the credential's accessible regions/kits and compares them to the report's
expected scope, surfacing a distinct warning when the visible set is a strict subset.
**Verify:** new test needed: `test_under_scoped_credential_warns_on_scope_gap`
**Related:** AUT-001, AUT-002

## Fix Order

- **Dependencies:** AUT-002 should land before AUT-001 (distinct 403 classification clarifies
  which failures are refreshable). AUT-003 is independent.
- **Merge candidates:** AUT-001 and AUT-003 both touch the credential lifecycle; implement
  scope validation (AUT-003) before refresh wiring (AUT-001) so refresh is only attempted
  for scope-aligned, time-expired 401s.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** Worker error-handling (`reports.py::worker`) is likely owned by
  the error-handling phase (09/10); the 401/403 hang consequence under AUT-002 is noted
  there, not fixed here.
- **Rollout risk:** Wiring `refresh_token_func` changes `NetworkClient.__init__` signature
  (additive, default `None`) — backward compatible. `test_network_retry_and_fail` and the
  `mock_network_client` fixture must keep `call` semantics for injected mocks.

## Appendices

### Appendix A — Runtime reproduction (401/403) and static hook check

`uv run python` repro (excerpt):
```text
refresh_token_func param present: False
AccessForbiddenError referenced in network module: False
Authorization error (token invalid/expired) in "raise_401" with args="()" and kwargs="{}": [Error code 401] auth error
401 -> attempts: 1 | re-raised immediately (NOT retried)
Unexpected error in "raise_403" with args="()" and kwargs="{}": [Error code 403] forbidden
Traceback (most recent call last):
  ...
mediascope_api.core.errors.AccessForbiddenError: [Error code 403] forbidden
403 -> attempts: 1 | fell through to generic Exception (NOT retried, misrouted)
```
*(supports: AUT-001 hook absent; AUT-002 403 misrouted as "Unexpected error")*

`inspect.signature(NetworkClient.__init__)`:
```text
(self, max_concurrent_requests: int = 5, sleep_time: int = 2, max_attempts: int = 10)
```
*(supports: no refresh_token_func parameter — AUT-001)*

### Appendix B — Source evidence (project + library auth backend)

`network.py` AuthorizationError branch (re-raise, no refresh; lines 80-86):
```text
except AuthorizationError as err:
    logger.error(
        f'Authorization error (token invalid/expired) in "{func.__name__}" '
        f'with args="{args}" and kwargs="{kwargs}": {err}'
    )
    raise
```
*(supports: AUT-001 — 401 not refreshed before re-raise; request args logged)*

`network.py` `AccessForbiddenError` not imported (line 10); the catch-all
`except Exception` (lines 113-118) is the only handler 403 reaches:
```text
from mediascope_api.core.errors import AuthorizationError, BadRequestError
except Exception as err:
    logger.exception(
        f'Unexpected error in "{func.__name__}" '
        f'with args="{args}" and kwargs="{kwargs}": {err}'
    )
    raise
```
*(supports: AUT-002 — 403 misrouted to "Unexpected error" + traceback)*

`network.py` class docstring TODO (declared, unimplemented hook):
```text
TODO (Architect): в будущем можно добавить refresh_token_func: Callable | None = None
```
*(supports: AUT-001 — declared hook never implemented)*

`mediascope_api/core/net.py` `refresh_token` (timing-only, no 401 recovery):
```text
def refresh_token(self):
    if 'now' in self.token:
        now = time.mktime(datetime.datetime.now().timetuple())
        token_time = time.mktime(self.token['now'].timetuple())
        token_exp = self.token['expires_in']
        if token_time + token_exp - now <= 0:
            self.token = self.get_token(self.username, self.passw)
    else:
        self.token = self.get_token(self.username, self.passw)
```
*(supports: AUT-001 — no refresh-on-401 path in the auth backend)*

`mediascope_api/core/net.py` `_raise_error` (401/403 raised, body embedded in message):
```text
elif req.status_code == 401:
    raise errors.AuthorizationError(f'Ошибка авторизации: "{req.text}"', req.status_code)
elif req.status_code == 403:
    raise errors.AccessForbiddenError(f'Доступ запрещен: "{req.text}"', req.status_code)
```
*(supports: AUT-001/AUT-002 — 401/403 classes emitted by the backend)*

`reports.py` TV_REG region iteration (silent partial data):
```text
available_regions = report.catalogs.get_tv_region()
regions_list = (
    report.report_settings.model_dump().get("regions")
    or available_regions["id"].to_list()
)
...
for reg_id in regions_list:
```
*(supports: AUT-003 — no completeness/warning against credential's visible region set)*

### Appendix C — Scope-validation scan

`grep -E 'scope|permission' src/mko_get_mediascope_data/core/` → 0 matches.
*(supports: AUT-003 — no project-level scope/authority validation exists)*
