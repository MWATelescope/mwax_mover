"""Small numeric/value conversion and checking helpers.

Unit conversions between bytes/gigabytes/gibibytes/gigabits, throughput
calculation (get_gbps), and a generic int-parseable check (is_int) used
across the CLI entry points for validating string arguments.
"""


def is_int(value) -> bool:
    """
    Check whether ``value`` can be interpreted as an integer.

    Args:
        value: Any value to test. Typically a string.

    Returns:
        True if ``int(value)`` succeeds without raising ``ValueError``,
        False otherwise.
    """
    try:
        int(value)
    except ValueError:
        return False
    else:
        return True


def gigabyte_to_gibibyte(gigabytes: float) -> float:
    """
    Convert a size in gigabytes (SI, base-10) to gibibytes (IEC, base-2).

    Args:
        gigabytes: Size in gigabytes (1 GB = 10^9 bytes).

    Returns:
        Equivalent size in gibibytes (1 GiB = 2^30 bytes), as a float.
    """
    return gigabytes * 10**9 / 2**30


def gigabytes_to_gigabits(gigabytes: float) -> float:
    """
    Convert a size in gigabytes (SI, base-10) to gigabits.

    Args:
        gigabytes: Size in gigabytes (1 GB = 10^9 bytes).

    Returns:
        Equivalent size in gigabits, as a float.
    """
    return gigabytes * 8


def bytes_to_gigabytes(num_bytes: int) -> float:
    """
    Convert a size in bytes to gigabytes (SI, base-10).

    Args:
        num_bytes: size in bytes

    Returns:
        Equivalent size in gigabytes: Size in gigabytes (1 GB = 10^9 bytes) as float.
    """
    return num_bytes / (1000.0 * 1000.0 * 1000.0)


def get_gbps(size_gigabytes: float, elapsed_seconds: float) -> float:
    """Calculate throughput in Gbps.

    Args:
        size_gigabytes: Transfer size in gigabytes.
        elapsed_seconds: Elapsed time of the transfer, in seconds. Clock-agnostic --
            pass whatever duration you already have, from time.monotonic()
            (preferred for elapsed durations) or time.time().

    Returns:
        Throughput in Gbps, or 0.0 if elapsed time is zero.
    """
    return gigabytes_to_gigabits(size_gigabytes) / elapsed_seconds if elapsed_seconds > 0 else 0.0
