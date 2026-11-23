"""
Version handling for mwax_mover
"""

import subprocess
from importlib.metadata import version as _get_version

BF_PATH: str = "/home/mwa/mwax_cbf/build/beamformer/mwax_beamformer"


def get_mwax_beamformer_version() -> str:
    """Get the MWAX beamformer version string.

    Returns:
        The version string of the MWAX beamformer software.
    """
    try:
        # Attempt to get the version from the beamformer binary
        result = subprocess.run(
            [BF_PATH, "-V"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        mwax_beamformer_version = result.stdout.strip()
    except Exception:
        mwax_beamformer_version = "Unknown"
    return mwax_beamformer_version


def get_mwax_mover_version_string() -> str:
    """Get the version string of mwax_mover.

    Returns:
        The package version as a string (e.g., "1.2.3").
    """
    return _get_version("mwax_mover")
