import pyarrow.parquet as pq
import fsspec
from datetime import datetime, timedelta, timezone, date
from typing import Set, Optional

# The single, reusable function for fetching a column from a remote Parquet file
def fetch_column_from_remote_parquet(full_url: str, column_name: str) -> list:
    """
    Reads a single column from a remote Parquet file using PyArrow's
    projection pushdown for efficiency and returns the data as a Python list.
    
    Args:
        full_url: The direct URL to the remote Parquet file.
        column_name: The name of the column to extract.
        
    Returns:
        A list containing the data from the specified column.
    """
    # 1. Initialize fsspec to handle HTTP access
    fs = fsspec.filesystem("http")

    try:
        with fs.open(full_url, 'rb') as f:
            # 2. Read only the specified column using projection pushdown
            table = pq.read_table(
                f, 
                columns=[column_name]
            )
            
        # 3. Convert the PyArrow Array to a Python list
        return table[column_name].to_pylist()
        
    except Exception as e:
        print(f"Error fetching data from {full_url}: {e}")
        return [] # Return an empty list or raise the exception as appropriate


def get_holidays():
    """Fetches the 'Date' column from the Holidays Parquet file."""
    COLUMN_NAME = 'Date' 
    FULL_URL = "https://huggingface.co/datasets/MSM02/bhavcopy-calendar/resolve/main/data/Holidays/Holidays.parquet"
    return fetch_column_from_remote_parquet(FULL_URL, COLUMN_NAME)

def get_special_sessions():
    """Fetches the 'Date' column from the Special Sessions Parquet file."""
    COLUMN_NAME = 'Date' 
    FULL_URL = "https://huggingface.co/datasets/MSM02/bhavcopy-calendar/resolve/main/data/SpecialSessions/Special%20Sessions.parquet"
    return fetch_column_from_remote_parquet(FULL_URL, COLUMN_NAME)

def get_last_trading_day():
    """Fetches the last trading date based on the timestamp that this function is called"""

# Define the Indian Standard Time (IST) timezone
# IST is UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

def is_trading_day(date_obj: date, holidays: Set[date], special_sessions: Set[date]) -> bool:
    """
    Checks if a given date object is a trading day based on the rules.
    Lookups are performed using datetime.date objects.
    """
    # Rule 1: Is it a special session? (Set contains date objects)
    if date_obj in special_sessions:
        return True
    
    # Rule 2: Is it a weekday (Monday=0 to Friday=4) AND not a regular holiday?
    is_weekday = date_obj.weekday() < 5
    is_holiday = date_obj in holidays # Set contains date objects
    
    return is_weekday and not is_holiday


def get_last_trading_day() -> Optional[date]:
    """
    Finds the latest valid trading date by checking:
    1. Current time (before/after 6:30 PM IST).
    2. Holidays and Special Sessions lists (using date objects for lookups).
    
    Returns:
        The last trading date as a datetime.date object, or None if not found within 365 days.
    """
    # 1. Setup time and date
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    
    # The cutoff time is 6:30 PM IST (18:30)
    cutoff_time = now_ist.replace(hour=18, minute=30, second=0, microsecond=0)

    # 2. Determine the starting date for the backward search
    if now_ist >= cutoff_time:
        # If it is past 6:30 PM IST, start checking from TODAY
        search_date = now_ist.date()
    else:
        # If it is before 6:30 PM IST, start checking from YESTERDAY
        search_date = now_ist.date() - timedelta(days=1)

    # 3. Prepare lookup sets (Sets now contain datetime.date objects)
    try:
        # get_holidays() and get_special_sessions() return List[date], which is converted to Set[date]
        holidays = set(get_holidays())
        special_sessions = set(get_special_sessions())
    except Exception as e:
        print(f"Failed to fetch date lists: {e}")
        return None

    # 4. Iterate backward to find the latest trading day
    MAX_SEARCH_DAYS = 365 # Prevent infinite loop
    for _ in range(MAX_SEARCH_DAYS):
        if is_trading_day(search_date, holidays, special_sessions):
            # Found the last trading day!
            return search_date # Returns the datetime.date object
        
        # Move to the previous day
        search_date -= timedelta(days=1)
        
    return None # Fallback if no trading day is found (e.g., if lists are empty)
