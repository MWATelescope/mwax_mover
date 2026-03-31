"""
Version handling for mwax_mover
"""

import typing
import importlib_metadata  # part of setuptools


def get_mwax_mover_version_string() -> str:
    """Get the version string of mwax_mover.

    Returns:
        The package version as a string (e.g., "1.2.3").
    """
    package_version = importlib_metadata.version("mwax_mover")
    return package_version


def get_pmwax_mover_version_number() -> typing.Tuple[int, int, int]:
    """Get the version number of mwax_mover as a tuple.

    Returns:
        A tuple of (major, minor, patch) version numbers.

    Raises:
        Exception: If the version string cannot be parsed.
    """
    version = get_mwax_mover_version_string()
    try:
        return (
            int(version.split(".")[0]),
            int(version.split(".")[1]),
            int(version.split(".")[2]),
        )
    except Exception as catch_all_exception:
        raise Exception(
            f"Unabled to determine mwax_mover version: Got {version} which"
            f" could not be parsed. Error: {catch_all_exception}"
        ) from catch_all_exception
