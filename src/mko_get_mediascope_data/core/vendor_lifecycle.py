"""Deterministic release of the vendor sessions owned by one report entry.

``mediascope-api-lib`` (verified 1.8.3, newest available) exposes no close
lifecycle on :class:`~mediascope_api.core.net.MediascopeApiNetwork`.  Its
``MediaVortexTask``/``MediaVortexCats`` singletons are constructed once per
report entry and each construction allocates *two* ``requests.Session`` objects
(one via ``network_module`` and one via ``cats.msapi_network``), replacing the
previous entry's sessions on the singleton.  Without an explicit release
decision those sessions are left to garbage collection, which is not a
deterministic report-boundary guarantee (EXT-010).

This module owns that decision: :func:`capture_sessions` snapshots the two
sessions right after construction, and :func:`release_sessions` closes them
idempotently under a ``finally`` in :meth:`AppService.run_report`, guarded so a
session is never closed while a later entry already replaced it or while a
timed-out thread still holds it (ASY-009).
"""

import logging
from dataclasses import dataclass
from weakref import WeakSet

from mediascope_api.mediavortex.catalogs import MediaVortexCats
from mediascope_api.mediavortex.tasks import MediaVortexTask
from requests import Session

logger = logging.getLogger(__name__)

# Sessions already handed to :func:`release_sessions`, so the two release paths
# (per-retry-attempt cleanup in ``AppService`` and the per-entry ``finally``)
# compose without double-closing the same session.  A ``WeakSet`` never keeps a
# session alive and cannot collide on a re-used ``id()``; a fresh entry always
# allocates new ``Session`` objects, so a closed session is never re-released.
_released_sessions: "WeakSet[Session]" = WeakSet()


@dataclass(frozen=True)
class CapturedSessions:
    """The two vendor sessions (and their owning networks) for one report entry.

    The owners are retained alongside the sessions so release can re-check
    ownership by identity: a session is closed only while the *same* vendor
    network object still holds it, never one a later entry already replaced.
    """

    task_session: Session
    cats_session: Session
    task_owner: object
    cats_owner: object


def capture_sessions(report_service: MediaVortexTask) -> CapturedSessions | None:
    """Snapshot the vendor sessions held by ``report_service``.

    The vendor exposes the task session as ``network_module.session`` and the
    catalog session as ``cats.msapi_network.session``.  Returns ``None`` when
    either path is unavailable (e.g. a partially-initialized singleton or a
    future vendor refactor) so callers never have to guard against a missing
    attribute; the function never raises on a missing/differing vendor shape.
    """
    return _capture_from(
        getattr(report_service, "network_module", None),
        getattr(report_service, "cats", None),
    )


def capture_singleton_sessions() -> CapturedSessions | None:
    """Snapshot the sessions of the partially-initialized vendor singletons.

    Used when the vendor constructor raised before returning, so no
    ``MediaVortexTask`` instance was handed back.  ``MediaVortexTask.__init__``
    assigns ``self.network_module`` (task session) before ``self.cats``, and
    ``MediaVortexCats.__init__`` assigns ``self.msapi_network`` (catalog
    session) before its catalog I/O — so a constructor that fails mid-way
    leaves both sessions allocated, the catalog one reachable only through the
    separate ``MediaVortexCats`` singleton.  Returns ``None`` when the
    singletons or the session paths are absent.
    """
    task_instance = getattr(MediaVortexTask, "instance", None)
    cats_instance = getattr(MediaVortexCats, "instance", None)
    network_module = (
        getattr(task_instance, "network_module", None)
        if task_instance is not None
        else None
    )
    msapi_network = (
        getattr(cats_instance, "msapi_network", None)
        if cats_instance is not None
        else None
    )
    return _capture_from(network_module, msapi_network, cats_is_network=True)


def _capture_from(
    network_module: object,
    cats: object,
    *,
    cats_is_network: bool = False,
) -> CapturedSessions | None:
    """Read the two vendor sessions from the given vendor sub-objects."""
    if network_module is None or cats is None:
        return None

    task_session = getattr(network_module, "session", None)
    cats_owner = cats if cats_is_network else getattr(cats, "msapi_network", None)
    cats_session = (
        getattr(cats_owner, "session", None) if cats_owner is not None else None
    )
    if not isinstance(task_session, Session) or not isinstance(cats_session, Session):
        return None
    return CapturedSessions(
        task_session=task_session,
        cats_session=cats_session,
        task_owner=network_module,
        cats_owner=cats_owner,
    )


def release_sessions(
    captured: CapturedSessions | None,
    *,
    has_outstanding_work: bool = False,
) -> None:
    """Close the sessions captured for one report entry.

    Idempotent and exception-safe: each ``close()`` is wrapped so one failing
    session cannot mask the other or the original error that triggered release.

    A session is closed only when both guards hold:

    * identity — the vendor singleton still holds *this* entry's owner
      (``MediaVortexTask.instance.network_module is captured.task_owner`` /
      ``MediaVortexCats.instance.msapi_network is captured.cats_owner``).  A
      later entry that already replaced the owner is left untouched, so a live
      session is never closed beneath the new owner.
    * deferred work — ``has_outstanding_work`` is ``False``.  When a timed-out
      thread may still reference the sessions (ASY-009) the release is skipped
      and the residual is logged rather than closing beneath in-flight work.
    """
    if captured is None:
        return
    if has_outstanding_work:
        logger.warning(
            "Skipping vendor session release: a retained operation is still "
            "outstanding; sessions will be left to connection-pool cleanup."
        )
        return

    if _current_task_owner() is captured.task_owner:
        _close_quietly(captured.task_session, "task")
    if _current_cats_owner() is captured.cats_owner:
        _close_quietly(captured.cats_session, "cats")


def _current_task_owner() -> object | None:
    """Return the task session's current owner on the vendor singleton."""
    instance = getattr(MediaVortexTask, "instance", None)
    return getattr(instance, "network_module", None) if instance is not None else None


def _current_cats_owner() -> object | None:
    """Return the catalog session's current owner on the vendor singleton."""
    instance = getattr(MediaVortexCats, "instance", None)
    return getattr(instance, "msapi_network", None) if instance is not None else None


def _close_quietly(session: Session, label: str) -> None:
    """Close ``session``, logging (never raising) on failure.

    Idempotent across the two release paths: a session already released by an
    intermediate retry attempt (EXT-011) is skipped so the per-entry ``finally``
    does not close it a second time.
    """
    if session in _released_sessions:
        return
    _released_sessions.add(session)
    try:
        session.close()
    except Exception:  # noqa: BLE001 — release must not mask the original error
        logger.exception("Failed to close vendor %s session", label)
