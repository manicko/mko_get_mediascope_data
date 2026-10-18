class NoReportFoundError(Exception):
    """Raised when no report data or tasks can be produced.

    Covers scenarios where the report configuration yields nothing to
    process — e.g. empty ``target_audiences``, an inverted or fully
    out-of-range period, or a zero-task pipeline after all filtering.
    Per D-08 this is a warning-level condition surfaced to the caller
    rather than a silent failure.
    """


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
