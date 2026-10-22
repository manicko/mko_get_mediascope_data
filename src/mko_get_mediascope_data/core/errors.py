import asyncio
from typing import Any


class NoReportFoundError(Exception):
    """Raised when no report data or tasks can be produced.

    Covers scenarios where the report configuration yields nothing to
    process — e.g. empty ``target_audiences``, an inverted or fully
    out-of-range period, or a zero-task pipeline after all filtering.
    Per D-08 this is a warning-level condition surfaced to the caller
    rather than a silent failure.
    """


class CallTimeoutError(TimeoutError):
    """Raised when a call exceeds ``call_timeout`` while its thread may still run.

    ``thread_task`` retains the inner task wrapping ``asyncio.to_thread`` so callers
    can defer permit release until the synchronous work actually completes and consume
    its terminal exception.  Subclasses ``TimeoutError`` for retry classification.
    """

    def __init__(self, thread_task: asyncio.Future[Any] | None = None) -> None:
        super().__init__()
        self.thread_task = thread_task


class MaxRetriesExceeded(RuntimeError):
    """Raised when ``NetworkClient.call`` exhausts all retry attempts.

    Subclasses ``RuntimeError`` so existing ``pytest.raises(RuntimeError)``
    assertions continue to pass (backward compatible).
    """


class WrongDataSettings(Exception):
    """Raised when report data settings cannot be resolved.

    Raised by ``AppService.get_default_data_settings`` when the bundled
    defaults template for the requested ``(media, report_type)`` pair is
    missing from the settings directory.
    """


class WorkerFailureError(Exception):
    """Raised when a report worker exits unexpectedly before the queue drains.

    Raised by ``MediaReport.processing_tasks`` from its ``finally`` block as a
    fail-fast supervision guard (ASY-008): if a worker terminated with an
    unexpected non-``asyncio.CancelledError`` exception while no other
    exception was already in flight, the coordinator surfaces it here instead
    of silently leaving the report half-processed.  The original worker
    exception is chained via ``from`` so its traceback is preserved.
    """
