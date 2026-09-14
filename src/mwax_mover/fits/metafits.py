"""Downloading and reading MWA metafits FITS files.

download_metafits_file() fetches a metafits file for an observation from the
MWA web services (via net.webservice.call_webservice) and writes it to disk.
The get_metafits_value* functions read individual FITS header keywords back
out of a metafits file's primary or a named HDU.
"""

import os

from astropy.io import fits

from mwax_mover.constants import MWA_WEBSERVICE_HOSTS
from mwax_mover.net.webservice import call_webservice


def download_metafits_file(obs_id: int, metafits_path: str) -> str:
    """
    Download a metafits FITS file for the given observation ID from MWA web services
    and write it to disk.

    Tries the MRO-local web service first, falling back to the public web service.
    Raises an exception if all URLs and retries are exhausted.

    Args:
        obs_id: The 10-digit MWA observation ID.
        metafits_path: Directory path where the downloaded metafits file will be written.
            The file will be named ``<obs_id>_metafits.fits``.

    Returns:
        new metafits full filename

    Raises:
        Exception: If the file could not be downloaded from any URL after all retries.
    """
    metafits_file_path = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    # Try the MRO one first
    urls = [f"{host}/metadata/fits?obs_id={obs_id}" for host in MWA_WEBSERVICE_HOSTS]

    # On failure of all urls and retries it will raise an exception
    response = call_webservice(obs_id, urls, None)

    metafits = response.content
    with open(metafits_file_path, "wb") as handler:
        handler.write(metafits)

    return metafits_file_path


def get_metafits_value(metafits_filename: str, key: str):
    """
    Read a single keyword value from the primary HDU of a metafits FITS file.

    Args:
        metafits_filename: Path to the metafits FITS file.
        key: The FITS header keyword to look up in the primary HDU.

    Returns:
        The value associated with ``key`` in the primary HDU header. The type
        matches whatever astropy returns for the keyword (str, int, float, bool, etc.).

    Raises:
        Exception: If the file cannot be opened or the keyword is not found,
            wrapping the underlying error with a descriptive message.
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from primary HDU
            return hdul[0].header[key]

    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}: {catch_all_exception}"
        ) from catch_all_exception


def get_metafits_value_from_hdu(metafits_filename: str, hdu_name: str, key: str):
    """
    Read a single keyword value from a named HDU of a metafits FITS file.

    Args:
        metafits_filename: Path to the metafits FITS file.
        hdu_name: The name of the HDU to read from (e.g. ``'TILEDATA'``).
        key: The FITS header keyword to look up in the specified HDU.

    Returns:
        The value associated with ``key`` in the named HDU header. The type
        matches whatever astropy returns for the keyword (str, int, float, bool, etc.).

    Raises:
        Exception: If the file cannot be opened, the HDU is not found, or the
            keyword is missing, wrapping the underlying error with a descriptive message.
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from the named HDU
            return hdul[hdu_name].header[key]

    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}: {catch_all_exception}"
        ) from catch_all_exception


def get_calibrator_info(metafits_filename: str) -> tuple[bool, str, str]:
    """
    Read calibrator status, project ID, and calibrator source from a metafits file.

    Args:
        metafits_filename: Path to the metafits FITS file.

    Returns:
        A three-element tuple ``(is_calibrator, project_id, calib_source)`` where:

        - ``is_calibrator`` (bool): True if the CALIBRAT keyword is set in the
          primary HDU header.
        - ``project_id`` (str): The value of the PROJECT keyword.
        - ``calib_source`` (str): The value of the CALIBSRC keyword if
          ``is_calibrator`` is True, otherwise an empty string.

    Raises:
        Exception: If the file cannot be opened or any expected keyword is missing,
            wrapping the underlying error with a descriptive message.
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from primary HDU- it is bool
            is_calibrator = hdul[0].header["CALIBRAT"]
            if is_calibrator:
                calib_source = hdul[0].header["CALIBSRC"]
            else:
                calib_source = ""
            project_id = hdul[0].header["PROJECT"]
            return is_calibrator, project_id, calib_source
    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}: {catch_all_exception}"
        ) from catch_all_exception
