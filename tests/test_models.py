import pytest
from pydantic import ValidationError

from mko_get_mediascope_data.core.models import (
    LastTimeModel,
    MediaType,
    PeriodModel,
    ReportSettings,
    ReportType,
)


def test_report_settings_valid():
    data = {
        "media": "TV",
        "report_type": "crosstab",
        "report_subtype": "TOP_NAT_TV_ADVERTISERS",
        "relative_path": "tv/test",
        "period": {"last_time": {"unit_count": 1, "time_unit": "m"}},
    }
    settings = ReportSettings(**data)
    assert settings.media == MediaType.TV
    assert settings.report_type == ReportType.crosstab


def test_period_model_validation():
    with pytest.raises(ValidationError):
        PeriodModel()  # ни date_filter, ни last_time

    valid = PeriodModel(last_time=LastTimeModel(**{"unit_count": 4, "time_unit": "m"}))
    assert valid.last_time.unit_count == 4
