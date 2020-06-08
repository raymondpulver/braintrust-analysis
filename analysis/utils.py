from datetime import datetime

def timestamp_to_date(ts):
    """Returns a datetime object for the ts - whichis assumed to be in UTC time."""
    utc_dt = datetime.utcfromtimestamp(ts)
    aware_utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    return aware_utc_dt
