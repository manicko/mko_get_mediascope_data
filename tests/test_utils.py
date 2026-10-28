from datetime import date

import gzip
import pandas as pd
import pytest

from mko_get_mediascope_data.core.utils import (
    CSV_TEMP_PREFIX,
    MAX_PERIOD_DAYS,
    csv_to_file,
    en_to_ru,
    get_files_suffix,
    iter_year_slices,
    slice_period,
    split_period_by_year,
    str_to_date,
)


def test_str_to_date():
    assert str_to_date("2025-12-08") == pd.Timestamp("2025-12-08").date()


@pytest.mark.parametrize("freq,expected_len", [("m", 3), ("w", 13)])
def test_slice_period(freq, expected_len):
    period = (pd.Timestamp("2025-09-01").date(), pd.Timestamp("2025-11-30").date())
    result = slice_period(period, freq)
    assert len(result) == expected_len


def test_csv_to_file(tmp_path):
    df = pd.DataFrame({"col": [1, 2]})
    csv_to_file(
        df, tmp_path, file_prefix="test", add_time=False, compression={"method": "gzip"}
    )
    files = list(tmp_path.glob("*.csv.gz"))
    assert len(files) == 1


def test_get_files_suffix():
    assert get_files_suffix({"method": "gzip"}) == ".csv.gz"
    assert get_files_suffix(None) == ".csv"


def test_csv_to_file_writes_to_report_dir(tmp_report_dir):
    """Verify csv_to_file writes a compressed CSV into the report directory."""
    df = pd.DataFrame({"col": [1, 2]})
    csv_to_file(
        df,
        tmp_report_dir,
        file_prefix="report",
        add_time=False,
        compression={"method": "gzip"},
    )
    files = list(tmp_report_dir.glob("*.csv.gz"))
    assert len(files) == 1
    assert files[0].parent == tmp_report_dir


def test_en_to_ru_handles_none():
    """en_to_ru(None) returns [] rather than raising TypeError."""
    assert en_to_ru(None) == []


def test_csv_to_file_renders_nulls_as_empty_cells(tmp_path):
    """Null/NaN values render as empty CSV cells per the declared output contract.

    The null policy is deliberate: ``na_rep`` is set explicitly in ``csv_to_file``
    rather than relying on the pandas default. This test asserts the policy so
    that an accidental change to the default or an override via ``**kwargs`` is
    caught.
    """
    df = pd.DataFrame(
        {
            "regionName": ["Msk", "SPb", "Tula"],
            "FrequencyDistPer": [0.3, 0.5, float("nan")],
            "RtgPerSum": [33.0, None, 0.0],
        }
    )
    csv_to_file(
        df,
        tmp_path,
        file_prefix="nulls",
        add_time=False,
        compression={"method": "gzip"},
    )
    files = list(tmp_path.glob("*.csv.gz"))
    assert len(files) == 1

    content = gzip.decompress(files[0].read_bytes()).decode("utf-8-sig")
    lines = content.strip().splitlines()

    expected = [
        "regionName;FrequencyDistPer;RtgPerSum",
        "Msk;0,3;33,0",
        "SPb;0,5;",
        "Tula;;0,0",
    ]
    assert lines == expected


def test_csv_to_file_atomic_success_publishes_final(tmp_path):
    """A successful call publishes the final file and leaves no temp artifact."""
    df = pd.DataFrame({"regionName": ["Msk", "SPb"], "RtgPerSum": [10.0, 20.0]})
    csv_to_file(df, tmp_path, file_prefix="atomic", add_time=False, compression=None)

    files = list(tmp_path.glob("*.csv"))
    assert len(files) == 1
    final_file = files[0]
    assert final_file.exists()
    assert not final_file.name.startswith(CSV_TEMP_PREFIX)

    lines = final_file.read_text(encoding="utf-8-sig").strip().splitlines()
    assert lines == [
        "regionName;RtgPerSum",
        "Msk;10,0",
        "SPb;20,0",
    ]

    leftovers = [p for p in tmp_path.iterdir() if p.name.startswith(CSV_TEMP_PREFIX)]
    assert leftovers == []


def test_csv_to_file_atomic_failure_preserves_prior_final(tmp_path, mocker):
    """A failure after the temp file is created preserves the prior final file.

    The prior published artifact must remain byte-identical, the temp artifact
    must be removed, and the original exception must propagate.
    """
    df = pd.DataFrame({"col": [1, 2]})
    csv_to_file(df, tmp_path, file_prefix="prior", add_time=False, compression=None)

    published = list(tmp_path.glob("prior*.csv"))
    assert len(published) == 1
    prior_final = published[0]
    prior_bytes = prior_final.read_bytes()

    real_to_csv = pd.DataFrame.to_csv

    def failing_to_csv(self, path_or_buf=None, *args, **kwargs):
        # Simulate a partial write to the temp file before failing.
        if path_or_buf is not None:
            with open(path_or_buf, "wb") as handle:
                handle.write(b"partial,corrupt")
        raise OSError("simulated write failure")

    mocker.patch.object(pd.DataFrame, "to_csv", failing_to_csv)

    with pytest.raises(OSError):
        csv_to_file(df, tmp_path, file_prefix="prior", add_time=False, compression=None)

    mocker.patch.object(pd.DataFrame, "to_csv", real_to_csv)

    assert prior_final.read_bytes() == prior_bytes
    leftovers = [p for p in tmp_path.iterdir() if p.name.startswith(CSV_TEMP_PREFIX)]
    assert leftovers == []


# ---------------------------------------------------------------------------
# Year splitting — 366-day API limit (utils.iter_year_slices, split_period_by_year)
# ---------------------------------------------------------------------------


def test_iter_year_slices_single_year():
    """A period within one calendar year yields a single chunk."""
    result = list(iter_year_slices(date(2025, 1, 1), date(2025, 1, 7)))
    assert len(result) == 1
    assert result[0] == (date(2025, 1, 1), date(2025, 1, 7))


def test_iter_year_slices_multi_year():
    """A 3-year period yields 3 chunks aligned to calendar-year boundaries."""
    result = list(
        iter_year_slices(date(2023, 1, 1), date(2025, 12, 31))
    )
    # One chunk per calendar year, each spanning the full year.
    assert result == [
        (date(2023, 1, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_iter_year_slices_leap_year():
    """A chunk containing Feb 29 never exceeds 366 days."""
    # Full leap year (2024) — 366 days inclusive.
    result = list(
        iter_year_slices(date(2024, 1, 1), date(2024, 12, 31))
    )
    assert len(result) == 1
    chunk_start, chunk_end = result[0]
    assert (chunk_end - chunk_start).days + 1 <= MAX_PERIOD_DAYS


def test_iter_year_slices_exact_boundaries():
    """Jan 1 to Dec 31 boundary — first chunk starts at Jan 1, not before."""
    result = list(
        iter_year_slices(date(2023, 1, 1), date(2025, 12, 31))
    )
    assert result == [
        (date(2023, 1, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_iter_year_slices_partial_start_year():
    """Mid-year start produces a shorter first chunk."""
    result = list(
        iter_year_slices(date(2023, 6, 15), date(2025, 3, 10))
    )
    assert result == [
        (date(2023, 6, 15), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 3, 10)),
    ]


def test_split_period_by_year_returns_strings():
    """split_period_by_year returns list of ISO-format string tuples."""
    result = split_period_by_year(
        (date(2023, 1, 1), date(2025, 12, 31))
    )
    assert isinstance(result, list)
    assert all(isinstance(interval, tuple) for interval in result)
    assert all(
        isinstance(d, str) for interval in result for d in interval
    )
    assert result == [
        ("2023-01-01", "2023-12-31"),
        ("2024-01-01", "2024-12-31"),
        ("2025-01-01", "2025-12-31"),
    ]


def test_split_period_by_year_all_chunks_leq_366():
    """Every chunk from split_period_by_year is <= 366 days inclusive."""
    # 4-year span including a leap year.
    result = split_period_by_year(
        (date(2023, 1, 1), date(2026, 12, 31))
    )
    assert len(result) > 1  # multi-year → multiple chunks
    for start_str, end_str in result:
        start_date = str_to_date(start_str)
        end_date = str_to_date(end_str)
        duration = (end_date - start_date).days + 1
        assert duration <= MAX_PERIOD_DAYS, (
            f"Chunk ({start_str}, {end_str}) spans {duration} days, "
            f"exceeding MAX_PERIOD_DAYS ({MAX_PERIOD_DAYS})"
        )


def test_split_period_by_year_single_year_short():
    """A <366-day single-year period returns one chunk."""
    result = split_period_by_year(
        (date(2025, 1, 1), date(2025, 1, 7))
    )
    assert len(result) == 1
    assert result[0] == ("2025-01-01", "2025-01-07")
