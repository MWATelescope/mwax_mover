"""GPS time conversion helpers, built on astropy.

get_gpstime_of_datetime() converts a UTC datetime to integer GPS seconds;
get_gpstime_of_now() is the current-time convenience wrapper around it.
"""

import datetime

from astropy import time as astrotime


# For a given datetime, return the GPS seconds as an integer
def get_gpstime_of_datetime(date_time: datetime.datetime) -> int:
    """
    Convert a UTC datetime to an integer GPS time (seconds since GPS epoch).

    Args:
        date_time: A timezone-aware or naive UTC ``datetime.datetime`` object.

    Returns:
        The GPS time as an integer number of seconds since the GPS epoch
        (6 January 1980 00:00:00 UTC).
    """
    utc_datetime = astrotime.Time(date_time, scale="utc")
    # astropy's stubs type `.gps` as possibly `Masked` for array inputs;
    # not possible here since `date_time` is a scalar datetime.
    return round(utc_datetime.gps)


# Return the GPS seconds as an integer of Now
def get_gpstime_of_now() -> int:
    """
    Return the current time as an integer GPS time (seconds since GPS epoch).

    Returns:
        The current GPS time as an integer number of seconds since the GPS
        epoch (6 January 1980 00:00:00 UTC).
    """
    return get_gpstime_of_datetime(datetime.datetime.now(datetime.UTC))
