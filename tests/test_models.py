import pytest
import json5
import re
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

    valid = PeriodModel(
        last_time=LastTimeModel.model_validate({"unit_count": 4}), time_unit="m"
    )
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


def test_merge_data_settings_ru_with_none_slices():
    """merge_data_settings must not crash when data_lang='ru' and slices is None.

    Reproduces TPL-003: defaults with slices=null and data_lang='ru' should
    not raise TypeError from en_to_ru(None).
    """
    defaults = DataDefaults(DATA_DEFAULTS={"slices": None})

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        relative_path="tv/test",  # type: ignore[arg-type]
        data_lang="ru",
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )

    result = AppService.merge_data_settings(defaults, report_settings)
    # slices remains None (not translated); coerce_none_to_empty on Data will
    # convert it to [] later.
    assert result["slices"] is None


def test_bundled_templates_validate():
    """All 32 entries across 16 bundled JSON5 report templates must pass
    ReportSettings validation.

    Reads from the package's bundled templates via APP_PATHS.app_settings_dir
    (read-only), not the user config directory.
    """
    reports_dir = APP_PATHS.app_settings_dir / "reports"
    json5_files = sorted(reports_dir.rglob("*.json5"))
    assert json5_files, "No JSON5 files found in bundled reports directory"

    total_entries = 0
    failures: list[str] = []

    for json5_file in json5_files:
        data = json5.loads(json5_file.read_text())
        entries = data if isinstance(data, list) else [data]
        for idx, entry in enumerate(entries):
            total_entries += 1
            try:
                ReportSettings.model_validate(entry)
            except Exception as exc:
                rel = json5_file.relative_to(reports_dir)
                failures.append(f"{rel} entry[{idx}]: {exc}")

    if failures:
        msg = f"{len(failures)} of {total_entries} entries failed:\n" + "\n".join(
            failures
        )
        pytest.fail(msg)

    assert total_entries == 32, f"Expected 32 entries, found {total_entries}"


def test_data_defaults_covers_all_subtypes():
    """All 7 ReportSubtype values must have matching defaults keys in tv/crosstab.json5.

    After Path A fixes, hasattr(defaults, subtype.value) must return True
    for all non-default subtypes. This verifies that the two-path failure
    mechanism (TypeError for ru, ValidationError for en) is eliminated.
    """
    defaults_path = APP_PATHS.app_settings_dir / "defaults" / "tv" / "crosstab.json5"
    data = json5.loads(defaults_path.read_text())
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
            last_time=LastTimeModel.model_validate({"unit_count": 1}),
            time_unit="m",
        )


def test_bundled_templates_kitId_conformance() -> None:
    """All kitId values in defaults and report templates must be from the
    valid legend set {1,2,3,4,5,6,7,9,10}. No undefined kitId values.

    Also verifies TOP_NAT_TV_ADVERTISERS in defaults has a kitId defined.
    """
    valid_kit_ids = {1, 2, 3, 4, 5, 6, 7, 9, 10}
    reports_dir = APP_PATHS.app_settings_dir / "reports"
    defaults_dir = APP_PATHS.app_settings_dir / "defaults"

    # Check all JSON5 files for kitId values
    all_json5 = list(reports_dir.rglob("*.json5")) + list(defaults_dir.rglob("*.json5"))
    for f in all_json5:
        text = f.read_text()
        kit_ids = [int(m) for m in re.findall(r"kitId\s*:\s*(\d+)", text)]
        for kid in kit_ids:
            assert kid in valid_kit_ids, (
                f"Invalid kitId={kid} in {f.relative_to(APP_PATHS.app_settings_dir)}"
            )

    # Verify TOP_NAT_TV_ADVERTISERS has kitId in defaults
    tv_crosstab = defaults_dir / "tv" / "crosstab.json5"
    text = tv_crosstab.read_text()
    assert '"kitId": 1' in text or "'kitId': 1" in text or "kitId: 1" in text, (
        "TOP_NAT_TV_ADVERTISERS default must define kitId"
    )


def test_tv_report_template_entry0_has_options() -> None:
    """VAL-003: nat_tv_tvreport.json5 entry[0] (TOP_NAT_TV_ADVERTISERS)
    must have an explicit options block (no longer relying on silent default).
    """
    report_path = APP_PATHS.app_settings_dir / "reports" / "nat_tv_tvreport.json5"
    data = json5.loads(report_path.read_text())
    entry0 = data[0]
    assert "options" in entry0, (
        "Entry[0] TOP_NAT_TV_ADVERTISERS must have options block"
    )
    assert entry0["options"]["kitId"] == 1, "kitId must be 1 (TV Index All Russia)"
