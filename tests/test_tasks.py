"""Tests for ``TVTask.to_yaml`` (PIPE-010).

``TVTask.to_yaml`` must raise ``TypeError`` when *folder* is ``None`` and
must write a valid YAML round-trip to ``folder/errors/<name>.yaml`` when a
valid folder is supplied.
"""

import pytest
import yaml

from mko_get_mediascope_data.core.tasks import TVTask


def test_to_yaml_raises_on_none_folder():
    """PIPE-010: ``TVTask.to_yaml(None)`` raises ``TypeError``.

    The method guards against a ``None`` folder — without a destination
    directory there is nowhere to write the error artifact.
    """
    task = TVTask("test_task", {}, "crosstab", "TV")
    with pytest.raises(TypeError, match="folder must be provided"):
        task.to_yaml(None)


def test_to_yaml_writes_valid_yaml(tmp_path):
    """PIPE-010: ``TVTask.to_yaml`` writes valid YAML to ``folder/errors/name.yaml``.

    The round-tripped YAML must contain ``name``, ``error``, ``log_error``,
    ``interval``, and ``target`` with the values set on the task.
    """
    task = TVTask("test_task", {}, "crosstab", "TV")
    task.interval = ("2025-01-01", "2025-01-07")
    task.target = "all 18+"
    task.error = "some error"
    task.log_error = True

    task.to_yaml(tmp_path)

    yaml_file = tmp_path / "errors" / "test_task.yaml"
    assert yaml_file.exists()

    data = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
    assert data["name"] == "test_task"
    assert data["error"] == "some error"
    assert data["log_error"] is True
    assert data["interval"] == ["2025-01-01", "2025-01-07"]
    assert data["target"] == "all 18+"
