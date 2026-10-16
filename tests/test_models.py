import pytest
import yaml
from datetime import date
from pydantic import ValidationError

from mko_get_mediascope_data.core.models import (
    Data,
    DataDefaults,
    LastTimeModel,
    MediaType,
    PeriodModel,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.paths import APP_PATHS


def test_report_settings_valid():
    data = {
        "media": MediaType.TV,
        "report_type": ReportType.crosstab,
        "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        "relative_path": "tv/test",
        "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
    }
    settings = ReportSettings.model_validate(data)
    assert settings.media == MediaType.TV
    assert settings.report_type == ReportType.crosstab


def test_period_model_validation():
    with pytest.raises(ValidationError):
        PeriodModel()  # neither date_filter nor last_time

    valid = PeriodModel(last_time=LastTimeModel.model_validate({"unit_count": 4}), time_unit="m")
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
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )

    result = AppService.merge_data_settings(defaults, report_settings)

    # en_to_ru must NOT be called when data_lang is 'en'
    mock_en_to_ru.assert_not_called()
    # slices remain unchanged (no translation applied)
    assert result["slices"] == ["EName_col"]


def test_bundled_templates_validate():
    """All 32 entries across 16 bundled YAML report templates must pass
    ReportSettings validation.

    Reads from the package's bundled templates via APP_PATHS.app_settings_dir
    (read-only), not the user config directory.
    """
    reports_dir = APP_PATHS.app_settings_dir / "reports"
    yaml_files = sorted(reports_dir.rglob("*.yaml"))
    assert yaml_files, "No YAML files found in bundled reports directory"

    total_entries = 0
    failures: list[str] = []

    for yaml_file in yaml_files:
        data = yaml.safe_load(yaml_file.read_text())
        entries = data if isinstance(data, list) else [data]
        for idx, entry in enumerate(entries):
            total_entries += 1
            try:
                ReportSettings.model_validate(entry)
            except Exception as exc:
                rel = yaml_file.relative_to(reports_dir)
                failures.append(f"{rel} entry[{idx}]: {exc}")

    if failures:
        msg = f"{len(failures)} of {total_entries} entries failed:\n" + "\n".join(failures)
        pytest.fail(msg)

    assert total_entries == 32, f"Expected 32 entries, found {total_entries}"


def test_data_defaults_covers_all_subtypes():
    """All 7 ReportSubtype values must have matching defaults keys in tv/crosstab.yaml.

    After Path A fixes, hasattr(defaults, subtype.value) must return True
    for all non-default subtypes. This verifies that the two-path failure
    mechanism (TypeError for ru, ValidationError for en) is eliminated.
    """
    defaults_path = APP_PATHS.app_settings_dir / "defaults" / "tv" / "crosstab.yaml"
    data = yaml.safe_load(defaults_path.read_text())
    defaults = DataDefaults(**data)

    for subtype in ReportSubtype:
        if subtype == ReportSubtype.default_value:
            continue
        assert hasattr(defaults, subtype.value), (
            f"Missing defaults key for subtype: {subtype.value}"
        )


def test_data_coerces_none_to_empty():
    """Data model coerces None values to empty lists for collection fields."""
    data = Data(options=None, statistics=None, slices=None)  # type: ignore[arg-type]
    assert data.options == []
    assert data.statistics == []
    assert data.slices == []


def test_period_model_rejects_both_date_filter_and_last_time():
    """PeriodModel must not allow both date_filter and last_time simultaneously."""
    with pytest.raises(ValidationError, match="not both"):
        PeriodModel(
            date_filter=[date(2025, 1, 1), date(2025, 1, 7)],
            last_time=LastTimeModel.model_validate(
                {"unit_count": 1}
            ),
            time_unit="m",
        )
