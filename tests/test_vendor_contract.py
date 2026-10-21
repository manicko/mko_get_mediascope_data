"""Vendor-handoff contract test for the submission-key fixture.

The positive ``send_task`` fixtures return :data:`MOCK_TASK_KEY` — a mapping
``{"taskId": ...}`` — matching the installed ``mediascope_api`` vendor contract.
This module pins that contract: it routes the *same* fixture key through the
real ``MediaVortexTask.get_status`` / ``get_result`` methods, with the network
boundary (``network_module``) stubbed, and asserts the fixture identifier
reaches the expected endpoint.

The construction uses ``object.__new__(MediaVortexTask)`` to bypass the
vendor's singleton ``__new__`` and credential-bound ``__init__``, so the test is
hermetic (no settings file, no singleton reset) and exercises only the real
status/result methods.
"""

from unittest.mock import MagicMock

import pytest
from mediascope_api.mediavortex.tasks import MediaVortexTask

from tests.conftest import MOCK_TASK_KEY


def test_submission_key_handoff_matches_installed_vendor_consumer() -> None:
    """A positive fixture key reaches the vendor's state/result endpoints.

    Contract (installed vendor):
      * ``get_status(tsk)`` reads ``tsk['taskId']`` and calls
        ``send_request('get', f'/task/state/{tid}')``.
      * ``get_result(tsk)`` reads ``tsk['taskId']`` and calls
        ``send_request('get', f'/task/result/{tsk["taskId"]}')``.

    A non-mapping (bare string) regression must fail *before* the network
    boundary is reached — the vendor calls ``tsk.get(...)``, so a ``str``
    raises ``AttributeError`` and ``send_request`` is never invoked.
    """
    # ---- Bypass vendor singleton __new__ + credential __init__ -----------
    task = object.__new__(MediaVortexTask)
    task.network_module = MagicMock()

    task_id = MOCK_TASK_KEY["taskId"]

    # ---- get_status routes the fixture identifier to the state endpoint ---
    task.get_status(MOCK_TASK_KEY)
    task.network_module.send_request.assert_any_call("get", f"/task/state/{task_id}")

    # ---- get_result routes the fixture identifier to the result endpoint --
    task.get_result(MOCK_TASK_KEY)
    task.network_module.send_request.assert_any_call("get", f"/task/result/{task_id}")

    # ---- Non-mapping regression fails before the network boundary ---------
    task.network_module.send_request.reset_mock()
    with pytest.raises(AttributeError):
        task.get_status("mock_task_key_123")
    with pytest.raises(AttributeError):
        task.get_result("mock_task_key_123")
    task.network_module.send_request.assert_not_called()

    # ---- Falsy/missing key is an explicit failure, not an export ----------
    assert task.get_result({"taskId": None}) is None
    assert task.get_result(None) is None
    task.network_module.send_request.assert_not_called()
