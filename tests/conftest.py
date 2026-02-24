import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pandas as pd

from mko_get_mediascope_data.core.network import NetworkClient


@pytest.fixture
def mock_network_client():
    client = NetworkClient(max_attempts=2, sleep_time=0)
    client.call = AsyncMock()
    return client


@pytest.fixture
def dummy_df():
    return pd.DataFrame({
        'id': ['1'],
        'periodFrom': ['2025-01-01'],
        'periodTo': ['2025-12-31']
    })


@pytest.fixture
def tmp_report_dir(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    return reports



@pytest.fixture
def tv_yaml_content():
    yaml_content = """[
      {
        "report_type": "crosstab",
        "report_subtype": "DYNAMICS_BY_SPOTS",
        "media": "TV",
        "compression": {"method": "gzip"},
        "data_lang": "ru",
        "relative_path": "finance/raw_data",
        "multiple_files": true,
        "ad_filter": "articleLevel2Id = 2272 and adIssueStatusId = R",
        "target_audiences": {
          "all 18+": "age >= 18"
        },
        "period": {
          "date_filter": ["2025-01-01", "2025-01-07"],
          "last_time": null
        }
      }
    ]"""

    return yaml_content