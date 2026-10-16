import json
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from mko_get_mediascope_data.core.models import MediaType, ReportSubtype, ReportType
from mko_get_mediascope_data.core.network import NetworkClient

# Matches reports.py:189: data_settings.options.get("kitId", 1)
DEFAULT_KIT_ID = "1"


@pytest.fixture
def mock_network_client():
    client = NetworkClient(max_attempts=2, sleep_time=0)
    client.call = AsyncMock()  # type: ignore[method-assign]
    return client


@pytest.fixture
def dummy_df():
    return pd.DataFrame(
        {"id": [DEFAULT_KIT_ID], "periodFrom": ["2025-01-01"], "periodTo": ["2025-12-31"]}
    )


@pytest.fixture
def tmp_report_dir(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    return reports


@pytest.fixture
def tv_yaml_content() -> str:
    """Report definition built with enum ``.value`` references for type safety."""
    content = [
        {
            "report_type": ReportType.crosstab.value,
            "report_subtype": ReportSubtype.DYNAMICS_BY_SPOTS.value,
            "media": MediaType.TV.value,
            "compression": {"method": "gzip"},
            "data_lang": "ru",
            "relative_path": "finance/raw_data",
            "multiple_files": True,
            "ad_filter": "articleLevel2Id = 2272 and adIssueStatusId = R",
            "target_audiences": {
                "all 18+": "age >= 18",
            },
            "period": {"date_filter": ["2025-01-01", "2025-01-07"]},
        }
    ]
    return json.dumps(content)
