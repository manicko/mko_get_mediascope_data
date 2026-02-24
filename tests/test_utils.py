import pytest
import pandas as pd
from mko_get_mediascope_data.core.utils import (
    str_to_date, slice_period, get_files_suffix, csv_to_file
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
    csv_to_file(df, tmp_path, file_prefix="test", add_time=False, compression={"method": "gzip"})
    files = list(tmp_path.glob("*.csv.gz"))
    assert len(files) == 1


def test_get_files_suffix():
    assert get_files_suffix({"method": "gzip"}) == ".csv.gz"
    assert get_files_suffix(None) == ".csv"
