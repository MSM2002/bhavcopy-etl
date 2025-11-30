import pytest
from unittest.mock import patch, MagicMock
import pyarrow as pa
from datetime import date, datetime, timezone
from bhavcopy_etl.dates import fetch_column_from_remote_parquet, get_holidays, get_special_sessions, is_trading_day, get_last_trading_day

# Sample data to use in mocks
MOCK_COLUMN_NAME = "Date"
MOCK_DATA = ["2025-01-01", "2025-08-15", "2025-10-02", "2025-10-01"]

# Fixture: mock pyarrow table with our fake column
@pytest.fixture
def mock_table():
    return pa.table({MOCK_COLUMN_NAME: MOCK_DATA})


def test_fetch_column_success(mock_table):
    with patch("fsspec.filesystem") as mock_fs, \
         patch("pyarrow.parquet.read_table", return_value=mock_table) as mock_read:

        mock_file = MagicMock()
        mock_fs.return_value.open.return_value.__enter__.return_value = mock_file

        result = fetch_column_from_remote_parquet("http://example.com/fake.parquet", MOCK_COLUMN_NAME)

        assert result == MOCK_DATA
        mock_fs.assert_called_once_with("http")
        mock_read.assert_called_once_with(mock_file, columns=[MOCK_COLUMN_NAME])


def test_fetch_column_failure():
    with patch("fsspec.filesystem") as mock_fs:
        mock_fs.return_value.open.side_effect = Exception("Connection error")

        result = fetch_column_from_remote_parquet("http://badurl.com/file.parquet", MOCK_COLUMN_NAME)

        assert result == []  # Should return empty list on error


def test_get_holidays(mock_table):
    with patch("fsspec.filesystem") as mock_fs, \
         patch("pyarrow.parquet.read_table", return_value=mock_table):

        mock_file = MagicMock()
        mock_fs.return_value.open.return_value.__enter__.return_value = mock_file

        result = get_holidays()
        assert result == MOCK_DATA


def test_get_special_sessions(mock_table):
    with patch("fsspec.filesystem") as mock_fs, \
         patch("pyarrow.parquet.read_table", return_value=mock_table):

        mock_file = MagicMock()
        mock_fs.return_value.open.return_value.__enter__.return_value = mock_file

        result = get_special_sessions()
        assert result == MOCK_DATA

# --- is_trading_day tests ---
def test_is_trading_day_weekday_not_holiday():
    some_date = date(2025, 10, 1)  # Wednesday
    holidays = {date(2025, 10, 2)}  # Different day
    special_sessions = set()
    assert is_trading_day(some_date, holidays, special_sessions) is True

def test_is_trading_day_weekend():
    some_date = date(2025, 10, 5)  # Sunday
    assert is_trading_day(some_date, set(), set()) is False

def test_is_trading_day_holiday():
    some_date = date(2025, 10, 2)  # Thursday
    holidays = {some_date}
    assert is_trading_day(some_date, holidays, set()) is False

def test_is_trading_day_special_session():
    some_date = date(2025, 10, 6)  # Monday
    special_sessions = {some_date}
    assert is_trading_day(some_date, set(), special_sessions) is True

# --- get_last_trading_day tests ---
def test_get_last_trading_day_after_cutoff():
    mock_now = datetime(2025, 10, 4, 19, 0, tzinfo=timezone.utc)  # After 6:30 PM IST
    with patch("bhavcopy_etl.dates.get_holidays", return_value=[date(2025, 10, 2)]), \
         patch("bhavcopy_etl.dates.get_special_sessions", return_value=[]), \
         patch("bhavcopy_etl.dates.datetime") as mock_datetime:

        # Control datetime.now()
        mock_datetime.now.return_value = mock_now
        mock_datetime.now.timezone = timezone.utc
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        result = get_last_trading_day()
        assert result == date(2025, 10, 3)  

def test_get_last_trading_day_before_cutoff_on_holiday():
    mock_now = datetime(2025, 10, 2, 16, 0, tzinfo=timezone.utc)  # Before 6:30 PM IST
    with patch("bhavcopy_etl.dates.get_holidays", return_value=[
                date(2025, 10, 1), date(2025, 10, 2)]), \
         patch("bhavcopy_etl.dates.get_special_sessions", return_value=[]), \
         patch("bhavcopy_etl.dates.datetime") as mock_datetime:

        # Mock datetime.now() to return Oct 2, before cutoff
        mock_datetime.now.return_value = mock_now
        mock_datetime.now.timezone = timezone.utc
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        result = get_last_trading_day()

        # Oct 2 and Oct 1 are holidays, expect Sep 30
        assert result == date(2025, 9, 30)
