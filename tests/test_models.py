import pytest
from pydantic import ValidationError

from mko_get_mediascope_data.core.models import (
    DataDefaults,
    LastTimeModel,
    MediaType,
    PeriodModel,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.services import AppService


def test_report_settings_valid():
    data = {
        "media": MediaType.TV,
        "report_type": ReportType.crosstab,
        "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        "relative_path": "tv/test",
        "period": {"last_time": {"unit_count": 1, "time_unit": "m"}},
    }
    settings = ReportSettings.model_validate(data)
    assert settings.media == MediaType.TV
    assert settings.report_type == ReportType.crosstab


def test_period_model_validation():
    with pytest.raises(ValidationError):
        PeriodModel()  # neither date_filter nor last_time

    valid = PeriodModel(last_time=LastTimeModel.model_validate({"unit_count": 4, "time_unit": "m"}))
    assert valid.last_time is not None
    assert valid.last_time.unit_count == 4


def test_merge_data_settings_en_does_not_translate(mocker):
    """Verify merge_data_settings does NOT call en_to_ru when data_lang='en'.

    The 'en' path (services.py) must skip the translation step entirely.
    """
    mock_en_to_ru = mocker.patch("mko_get_mediascope_data.core.utils.en_to_ru")

    defaults = DataDefaults(DATA_DEFAULTS={"slices": ["EName_col"]})

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        relative_path="tv/test",  # type: ignore[arg-type]
        data_lang="en",
        period={"last_time": {"unit_count": 1, "time_unit": "m"}},  # type: ignore[arg-type]
    )

    result = AppService.merge_data_settings(defaults, report_settings)

    # en_to_ru must NOT be called when data_lang is 'en'
    mock_en_to_ru.assert_not_called()
    # slices remain unchanged (no translation applied)
    assert result["slices"] == ["EName_col"]
