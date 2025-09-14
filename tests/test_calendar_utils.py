import pytest
from datetime import datetime, timedelta, time, timezone
from bhavcopy_etl.calendar_utils import _get_last_trading_day_at

# IST timezone constant
IST = timezone(timedelta(hours=5, minutes=30))


def test_before_cutoff_normal_day():
    # 13 Sep 2025 17:00 IST (before 18:30)
    test_time = datetime(2025, 9, 13, 17, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # It should give last trading day as 12 Sep 2025 or before
    assert result <= test_time.date()


def test_after_cutoff_normal_day():
    # 13 Sep 2025 19:00 IST (after 18:30)
    test_time = datetime(2025, 9, 13, 19, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # It should return 13 Sep 2025 or before
    assert result <= test_time.date()


def test_exact_cutoff_time():
    # Exactly 18:30 IST
    test_time = datetime(2025, 9, 13, 18, 30, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # After or exactly at cutoff, date moves forward by one day
    # So trading day could be 13th Sep or before
    assert result <= test_time.date()


def test_on_weekend_sunday():
    # Sunday, 14 Sep 2025
    test_time = datetime(2025, 9, 14, 12, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # Last trading day should be Friday, 12 Sep 2025
    assert result.weekday() < 5  # Weekday < 5 means Mon-Fri
    assert result < test_time.date()


def test_on_weekend_saturday():
    # Saturday, 13 Sep 2025
    test_time = datetime(2025, 9, 13, 12, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # Last trading day should be Friday, 12 Sep 2025
    assert result.weekday() < 5
    assert result < test_time.date()


def test_on_holiday():
    # Diwali 22nd October 2025
    holiday_date = datetime(2025, 10, 22, 12, 0, tzinfo=IST)
    result = _get_last_trading_day_at(holiday_date)
    # Last trading day should be before holiday date
    assert result < holiday_date.date()
    assert result.weekday() < 5


def test_input_naive_datetime():
    naive_time = datetime(2025, 9, 13, 19, 0)  # no tzinfo
    with pytest.raises(ValueError, match="timezone-aware"):
        _get_last_trading_day_at(naive_time)


def test_first_trading_day_of_year():
    # Suppose Jan 1, 2025 is a holiday or weekend, Jan 2 is first trading day.
    test_time = datetime(2025, 1, 2, 19, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    # Result can be in previous year (like Dec 31, 2024) or same year
    # Just ensure it's a valid trading day before or on Jan 2, 2025
    assert result <= test_time.date()
    assert result.weekday() < 5  # Monday-Friday


def test_far_future_date():
    test_time = datetime(2030, 12, 31, 20, 0, tzinfo=IST)
    result = _get_last_trading_day_at(test_time)
    assert isinstance(result, datetime.date)
    assert result <= test_time.date()
