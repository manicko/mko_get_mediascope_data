"""Tests for ``TVTask.to_file`` (PIPE-010).

``TVTask.to_file`` must raise ``TypeError`` when *folder* is ``None`` and
must write a valid JSON5 round-trip to ``folder/errors/<name>.json5`` when a
valid folder is supplied.
"""

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

    The round-tripped JSON5 must contain ``name``, ``error``, ``log_error``,
    ``interval``, and ``target`` with the values set on the task.
    """
    task = TVTask("test_task", {}, "crosstab", "TV")
    task.interval = ("2025-01-01", "2025-01-07")
    task.target = "all 18+"
    task.error = "some error"
    task.log_error = True

    task.to_file(tmp_path)

    json5_file = tmp_path / "errors" / "test_task.json5"
    assert json5_file.exists()

    data = json5.load(json5_file.open(encoding="utf-8"))
    assert data["name"] == "test_task"
    assert data["error"] == "some error"
    assert data["log_error"] is True
    assert data["interval"] == ["2025-01-01", "2025-01-07"]
    assert data["target"] == "all 18+"
