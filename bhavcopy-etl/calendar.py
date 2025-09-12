import exchange_calendars
from datetime import date, datetime, time, timedelta, timezone

def get_last_trading_day():
    """
    Function to find the last trading day for end date based on when the bhavcopies are uploaded
    """
    # Working in Indian timezone 
    IST = timezone(timedelta(hours=5, minutes=30))

    # Current timestamp
    now_ist = datetime.now(IST)

    # Getting exchange calendar data of BSE
    xbom = exchange_calendars.get_calendar("XBOM")

    # Checking if it is past 6:30 pm IST to get today's bhavcopy or use yesterday's
