"""
Version handling for mwax_mover
"""

from importlib.metadata import version as _get_version


def get_mwax_mover_version_string() -> str:
    """Get the version string of mwax_mover.

    Returns:
        The package version as a string (e.g., "1.2.3").
    """
    return _get_version("mwax_mover")
