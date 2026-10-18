"""Tests for ``TVTask.to_file`` (PIPE-010).

``TVTask.to_file`` must raise ``TypeError`` when *folder* is ``None`` and
must write a valid JSON5 round-trip to ``folder/errors/<name>.json5`` when a
valid folder is supplied.
"""

from datetime import date

import json5
import pytest

from mko_get_mediascope_data.core.tasks import TVTask


def test_to_file_raises_on_none_folder():
    """PIPE-010: ``TVTask.to_file(None)`` raises ``TypeError``.

    The method guards against a ``None`` folder — without a destination
    directory there is nowhere to write the error artifact.
    """
    task = TVTask("test_task", {}, "crosstab", "TV")
    with pytest.raises(TypeError, match="folder must be provided"):
        task.to_file(None)


def test_to_file_writes_valid_json5(tmp_path):
    """PIPE-010: ``TVTask.to_file`` writes valid JSON5 to ``folder/errors/name.json5``.

    The round-tripped JSON5 must contain all 8 ``dump_settings`` keys
    (``name``, ``report_type``, ``report_subtype``, ``target``, ``error``,
    ``log_error``, ``interval``, ``settings``) with the values set on the task.

    Uses ``date`` objects for ``interval`` (matching production, which
    produces ``tuple[date, date]``) to guard against date-serialization
    regressions — the ``default`` serializer must produce ``isoformat()``.
    """
    task = TVTask("test_task", {}, "DYNAMICS_BY_SPOTS", "crosstab")
    task.interval = (date(2025, 1, 1), date(2025, 1, 7))
    task.target = "all 18+"
    task.error = "some error"
    task.log_error = True

    task.to_file(tmp_path)

    json5_file = tmp_path / "errors" / "test_task.json5"
    assert json5_file.exists()

    with json5_file.open(encoding="utf-8") as f:
        data = json5.load(f)
    assert data["name"] == "test_task"
    assert data["report_type"] == "crosstab"
    assert data["report_subtype"] == "DYNAMICS_BY_SPOTS"
    assert data["target"] == "all 18+"
    assert data["error"] == "some error"
    assert data["log_error"] is True
    assert data["interval"] == ["2025-01-01", "2025-01-07"]
    assert data["settings"] == {}

    # Raw-byte formatting assertions:
    # quote_keys=True → keys are double-quoted ("name":), not bare (name:)
    # indent=2         → 2-space indentation for first-level keys
    # trailing_commas=False → no trailing comma after the settings dict
    text = json5_file.read_text(encoding="utf-8")
    assert '"name":' in text
    assert '  "name":' in text
    assert '"settings": {}' in text
    assert '"settings": {},' not in text
