"""
Version handling for mwax_mover
"""

import subprocess
from importlib.metadata import version as _get_version

UNKNOWN_BEAMFORMER_VERSION: str = "Unknown"


def get_mwax_beamformer_version(beamformer_binary_path: str) -> str:
    """Get the MWAX beamformer version string.

    Args:
        beamformer_binary_path: Full path of the MWAX beamformer binary.

    Returns:
        The version string of the MWAX beamformer software.
    """
    try:
        # Attempt to get the version from the beamformer binary
        result = subprocess.run(
            [beamformer_binary_path, "-V"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        mwax_beamformer_version = result.stdout.strip()
    except Exception:
        mwax_beamformer_version = UNKNOWN_BEAMFORMER_VERSION
    return mwax_beamformer_version


def get_mwax_mover_version_string() -> str:
    """Get the version string of mwax_mover.

    Returns:
        The package version as a string (e.g., "1.2.3").
    """
    return _get_version("mwax_mover")
