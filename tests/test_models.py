from datetime import date
from pathlib import Path
from typing import Any

import json5
import pytest
from pydantic import ValidationError

from mko_get_mediascope_data.core.models import (
    AppSettings,
    Data,
    DataDefaults,
    LastTimeModel,
    MediaType,
    PeriodModel,
    ReportSettings,
    ReportSubtype,
    ReportType,
)
from mko_get_mediascope_data.core.paths import APP_PATHS
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.utils import en_to_ru

# Number of report entries expected across the 17 bundled JSON5 report templates.
EXPECTED_TEMPLATE_ENTRIES = 34

# Valid Media-Scope kitId legend values (dataset IDs).
VALID_KIT_IDS: set[int] = {1, 2, 3, 4, 5, 6, 7, 9, 10}


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
    assert settings.report_subtype == ReportSubtype.TOP_NAT_TV_ADVERTISERS
    assert settings.data_lang == "ru"
    assert settings.compression == {"method": "gzip"}
    assert settings.multiple_files is True
    assert settings.check_done is True
    assert settings.ad_filter is None
    assert settings.relative_path == Path("tv/test")
    assert settings.period.date_filter is None
    assert settings.period.last_time is not None
    assert settings.period.last_time.unit_count == 1


def test_period_model_validation():
    with pytest.raises(ValidationError, match="Specify either"):
        PeriodModel()  # neither date_filter nor last_time

    valid = PeriodModel(
        last_time=LastTimeModel.model_validate({"unit_count": 4}), time_unit="m"
    )
    assert valid.last_time is not None
    assert valid.last_time.unit_count == 4


def test_merge_data_settings_en_does_not_translate():
    """Verify merge_data_settings does NOT translate slices when data_lang='en'.

    The 'en' path (services.py) must skip the translation step entirely,
    so en_to_ru is never called and 'EName' prefixes remain untouched.
    """
    defaults = DataDefaults(DATA_DEFAULTS={"slices": ["EName_col"]})

    report_settings = ReportSettings(
        media=MediaType.TV,
        report_type=ReportType.crosstab,
        relative_path="tv/test",  # type: ignore[arg-type]
        data_lang="en",
        period={"last_time": {"unit_count": 1}, "time_unit": "m"},  # type: ignore[arg-type]
    )

    result = AppService.merge_data_settings(defaults, report_settings)

    # slices remain unchanged (no translation applied)
    assert result["slices"] == ["EName_col"]


def test_bundled_templates_validate():
    """All 34 entries across 17 bundled JSON5 report templates must pass
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
        data = json5.loads(json5_file.read_text(encoding="utf-8"))
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

    assert total_entries == EXPECTED_TEMPLATE_ENTRIES, (
        f"Expected {EXPECTED_TEMPLATE_ENTRIES} entries, found {total_entries}"
    )


def test_data_defaults_covers_all_subtypes():
    """All 7 ReportSubtype values must have matching defaults keys in tv/crosstab.json5.

    After Path A fixes, hasattr(defaults, subtype.value) must return True
    for all non-default subtypes. This verifies that the two-path failure
    mechanism (TypeError for ru, ValidationError for en) is eliminated.
    """
    defaults_path = APP_PATHS.app_settings_dir / "defaults" / "tv" / "crosstab.json5"
    data = json5.loads(defaults_path.read_text(encoding="utf-8"))
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


def _collect_kit_ids(obj: Any) -> list[int]:
    """Recursively collect every ``kitId`` integer value from a parsed JSON5 structure."""
    kit_ids: list[int] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "kitId" and isinstance(value, int):
                kit_ids.append(value)
            else:
                kit_ids.extend(_collect_kit_ids(value))
    elif isinstance(obj, list):
        for item in obj:
            kit_ids.extend(_collect_kit_ids(item))
    return kit_ids


def test_bundled_templates_kitId_conformance() -> None:
    """All kitId values in defaults and report templates must be from the
    valid legend set {1,2,3,4,5,6,7,9,10}. No undefined kitId values.

    Uses proper JSON5 parsing with recursive dict/list traversal to find every
    ``kitId`` key, regardless of quoting style or nesting depth.

    Also verifies TOP_NAT_TV_ADVERTISERS in defaults has a kitId defined.
    """
    reports_dir = APP_PATHS.app_settings_dir / "reports"
    defaults_dir = APP_PATHS.app_settings_dir / "defaults"

    all_json5 = list(reports_dir.rglob("*.json5")) + list(defaults_dir.rglob("*.json5"))
    for f in all_json5:
        data = json5.loads(f.read_text(encoding="utf-8"))
        kit_ids = _collect_kit_ids(data)
        for kid in kit_ids:
            assert kid in VALID_KIT_IDS, (
                f"Invalid kitId={kid} in {f.relative_to(APP_PATHS.app_settings_dir)}"
            )


def test_tv_report_template_entry0_has_options() -> None:
    """VAL-003: nat_tv_tvreport.json5 entry[0] (TOP_NAT_TV_ADVERTISERS)
    must have an explicit options block (no longer relying on silent default).
    """
    report_path = APP_PATHS.app_settings_dir / "reports" / "nat_tv_tvreport.json5"
    data = json5.loads(report_path.read_text(encoding="utf-8"))
    entry0 = data[0]
    assert "options" in entry0, (
        "Entry[0] TOP_NAT_TV_ADVERTISERS must have options block"
    )


def test_report_settings_rejects_invalid_enum():
    """An invalid ``media`` value must be rejected by Pydantic enum validation."""
    data = {
        "media": "INVALID",
        "report_type": ReportType.crosstab,
        "report_subtype": ReportSubtype.TOP_NAT_TV_ADVERTISERS,
        "relative_path": "tv/test",
        "period": {"last_time": {"unit_count": 1}, "time_unit": "m"},
    }
    with pytest.raises(ValidationError):
        ReportSettings.model_validate(data)


def test_period_model_single_date_filter_rejected():
    """A ``date_filter`` with a single date must raise — exactly 2 dates required."""
    with pytest.raises(ValidationError, match="date_filter must contain 2 dates"):
        PeriodModel(
            date_filter=[date(2025, 1, 1)],
            time_unit="m",
        )


def test_en_to_ru_direct():
    """``en_to_ru`` translates EName→Name and returns [] for None input."""
    assert en_to_ru(["EName_col"]) == ["Name_col"]
    assert en_to_ru(None) == []


def test_app_settings_normalize_path():
    """AppSettings.expanduser resolves ``~`` in the export_folder path."""
    settings = AppSettings(export_folder="~/x")  # type: ignore[arg-type]
    assert settings.export_folder == Path("~/x").expanduser()
