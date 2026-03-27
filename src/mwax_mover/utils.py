"""Shared utility functions, enumerations, and helper classes for mwax_mover.

Key enumerations: MWAXSubfileDistirbutorMode, CorrelatorMode, MWADataFileType,
ArchiveLocation. Key classes: ValidationData (filename validation result).
Key functions: validate_filename(), metafits creation/reading, MD5 checksumming,
PSRDADA header parsing, Redis-based beamformer signalling, UDP multicast sending,
and config file helpers (read_config, read_optional_config, read_config_list).
"""

import base64
from astropy import time as astrotime
from configparser import ConfigParser
import datetime
from enum import Enum
import fcntl
import glob
import json
import logging
import os
import queue
import redis
import shutil
import socket
import struct
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_not_exception_type
import threading
import time
import typing
from pathlib import Path
from typing import List
from typing import Tuple, Optional
import astropy.io.fits as fits
import requests
from mwax_mover import mwax_command
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData

logger = logging.getLogger(__name__)

# number of lines of the PSRDADA header to read looking for keywords
PSRDADA_HEADER_BYTES = 4096

# PSRDADA keywords
PSRDADA_MODE = "MODE"
PSRDADA_TRANSFER_SIZE = "TRANSFER_SIZE"
PSRDADA_OBS_ID = "OBS_ID"
PSRDADA_SUBOBS_ID = "SUBOBS_ID"
PSRDADA_TRIGGER_ID = "TRIGGER_ID"
PSRDADA_NINPUTS = "NINPUTS"
PSRDADA_COARSE_CHANNEL = "COARSE_CHANNEL"

# This is global mutex so we don't try to create the same metafits
# file with multiple threads
metafits_file_lock = threading.Lock()


class MWAXSubfileDistirbutorMode(Enum):
    """Class representing the mode running"""

    UNKNOWN = ""
    BEAMFORMER = "B"
    CORRELATOR = "C"


class CorrelatorMode(Enum):
    """Class representing correlator mode"""

    NO_CAPTURE = "NO_CAPTURE"
    CORR_MODE_CHANGE = "CORR_MODE_CHANGE"
    MWAX_CORRELATOR = "MWAX_CORRELATOR"
    MWAX_VCS = "MWAX_VCS"
    MWAX_BUFFER = "MWAX_BUFFER"
    MWAX_BEAMFORMER = "MWAX_BEAMFORMER"
    MWAX_CORR_BF = "MWAX_CORR_BF"

    @staticmethod
    def is_no_capture(mode_string: str) -> bool:
        """Returns true if mode is a no capture or mode change- i.e. don't do anything"""
        return mode_string in [CorrelatorMode.NO_CAPTURE.value, CorrelatorMode.CORR_MODE_CHANGE.value]

    @staticmethod
    def is_correlator(mode_string: str) -> bool:
        """Returns true if mode is a correlator obs"""
        return mode_string in [
            CorrelatorMode.MWAX_CORRELATOR.value,
            CorrelatorMode.MWAX_CORR_BF.value,
        ]

    @staticmethod
    def is_vcs(mode_string: str) -> bool:
        """Returns true if mode is a vcs obs"""
        return mode_string in [
            CorrelatorMode.MWAX_VCS.value,
        ]

    @staticmethod
    def is_voltage_buffer(mode_string: str) -> bool:
        """Returns true if mode is voltage buffer"""
        return mode_string == CorrelatorMode.MWAX_BUFFER.value

    @staticmethod
    def is_beamformer(mode_string: str) -> bool:
        """Returns true if mode is beamformer"""
        return mode_string in [CorrelatorMode.MWAX_BEAMFORMER.value, CorrelatorMode.MWAX_CORR_BF.value]


class MWADataFileType(Enum):
    """Enum for the possible MWA data file types"""

    HW_LFILES = 8
    MWA_FLAG_FILE = 10
    MWA_PPD_FILE = 14
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18
    VDIF = 19
    FILTERBANK = 20


class ValidationData:
    """A struct for the return value of validate_filename"""

    valid: bool
    obs_id: int
    project_id: str
    filetype_id: int
    file_ext: str
    calibrator: bool
    validation_message: str

    def __init__(
        self,
        valid: bool,
        obs_id: int,
        project_id: str,
        filetype_id: int,
        file_ext: str,
        calibrator: bool,
        validation_message: str,
    ):
        """
        Initialise a ValidationData result object.

        Args:
            valid: Whether the filename passed all validation checks.
            obs_id: The 10-digit MWA observation ID parsed from the filename.
            project_id: The MWA project ID read from the associated metafits file.
            filetype_id: The numeric file type ID corresponding to a MWADataFileType value.
            file_ext: The file extension including the leading dot (e.g. '.fits').
            calibrator: True if the observation is a calibrator observation.
            validation_message: Human-readable description of any validation failure,
                or an empty string on success.
        """
        self.valid = valid
        self.obs_id = obs_id
        self.project_id = project_id
        self.filetype_id = filetype_id
        self.file_ext = file_ext
        self.calibrator = calibrator
        self.validation_message = validation_message


class ArchiveLocation(Enum):
    Unknown = 0
    DMF = 1
    AcaciaIngest = 2
    Banksia = 3
    AcaciaMWA = 4


def download_metafits_file(obs_id: int, metafits_path: str):
    """
    Download a metafits FITS file for the given observation ID from MWA web services
    and write it to disk.

    Tries the MRO-local web service first, falling back to the public web service.
    Raises an exception if all URLs and retries are exhausted.

    Args:
        obs_id: The 10-digit MWA observation ID.
        metafits_path: Directory path where the downloaded metafits file will be written.
            The file will be named ``<obs_id>_metafits.fits``.

    Raises:
        Exception: If the file could not be downloaded from any URL after all retries.
    """
    metafits_file_path = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    # Try the MRO one first
    urls = [
        f"http://mro.mwa128t.org/metadata/fits?obs_id={obs_id}",
        f"http://ws.mwatelescope.org/metadata/fits?obs_id={obs_id}",
    ]

    # On failure of all urls and retries it will raise an exception
    response = call_webservice(obs_id, urls, None)

    metafits = response.content
    with open(metafits_file_path, "wb") as handler:
        handler.write(metafits)


def validate_filename(
    filename: str,
    metafits_path: str,
) -> ValidationData:
    """
    Validate an MWA data filename and extract associated metadata.

    Performs the following checks in order:
    1. The filename has a recognised extension.
    2. The first 10 characters of the base name form a valid integer observation ID.
    3. The extension maps to a known MWADataFileType.
    4. The base name length matches the expected format for the detected file type.
    5. The associated metafits file exists (downloading it if necessary) and is
       readable, yielding the project ID and calibrator flag.

    Args:
        filename: Full or relative path to the file to validate.
        metafits_path: Directory containing (or to receive) the metafits file
            for the observation. Used for all file types except metafits files
            themselves.

    Returns:
        A ValidationData instance. On success ``valid`` is True and all fields
        are populated. On failure ``valid`` is False and ``validation_message``
        describes the reason.
    """

    valid: bool = True
    obs_id = 0
    project_id = ""
    calibrator = False
    validation_error: str = ""
    filetype_id: int = -1
    file_name_part: str = ""
    file_ext_part: str = ""

    # 1. Is there an extension?
    split_filename = os.path.splitext(filename)
    if len(split_filename) == 2:
        file_name_part = os.path.basename(split_filename[0])
        file_ext_part = split_filename[1]
    else:
        # Error no extension
        valid = False
        validation_error = "Filename has no extension- ignoring"

    # 2. check obs_id in the first 10 chars of the filename and is integer
    if valid:
        obs_id_check = file_name_part[0:10]

        if not obs_id_check.isdigit():
            valid = False
            validation_error = "Filename does not start with a 10 digit observation_id- ignoring"
        else:
            obs_id = int(obs_id_check)

    # 3. Check extension
    if valid:
        if file_ext_part.lower() == ".sub":
            filetype_id = MWADataFileType.MWAX_VOLTAGES.value
        elif file_ext_part.lower() == ".fits":
            # Could be metafits (e.g. 1316906688_metafits_ppds.fits) or
            # visibilitlies
            if file_name_part[10:] == "_metafits_ppds" or file_name_part[10:] == "_metafits":
                filetype_id = MWADataFileType.MWA_PPD_FILE.value
            else:
                filetype_id = MWADataFileType.MWAX_VISIBILITIES.value

        elif file_ext_part.lower() == ".metafits":
            # Could be metafits (e.g. 1316906688.metafits)
            filetype_id = MWADataFileType.MWA_PPD_FILE.value

        elif file_ext_part.lower() == ".zip":
            # flag file
            filetype_id = MWADataFileType.MWA_FLAG_FILE.value

        elif file_ext_part.lower() == ".vdif" or file_ext_part.lower() == ".hdr":
            # vdif file
            filetype_id = MWADataFileType.VDIF.value

        elif file_ext_part.lower() == ".fil":
            # filterbank file
            filetype_id = MWADataFileType.FILTERBANK.value

        else:
            # Error - unknown filetype
            valid = False
            validation_error = f"Unknown file extension {file_ext_part}- ignoring"

    # 4. Check length of filename
    if valid:
        if filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
            # filename format should be obsid_subobsid_XXX.sub
            # filename format should be obsid_subobsid_XX.sub
            # filename format should be obsid_subobsid_X.sub
            if len(file_name_part) < 23 or len(file_name_part) > 25:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_subobsid_XXX.sub)- ignoring"
                )
        elif filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            # filename format should be obsid_yyyymmddhhnnss_chXXX_XXX.fits
            if len(file_name_part) != 35:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_yyyymmddhhnnss_chXXX_XXX.fits)-"
                    " ignoring"
                )

        elif filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            # filename format should be obsid_metafits_ppds.fits or
            # obsid_metafits.fits or obsid.metafits
            if len(file_name_part) != 24 and len(file_name_part) != 19 and len(file_name_part) != 10:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_metafits_ppds.fits,"
                    " obsid_metafits.fits or obsid.metafits)- ignoring"
                )

        elif filetype_id == MWADataFileType.MWA_FLAG_FILE.value:
            # filename format should be obsid_flags.zip
            if len(file_name_part) != 16:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_flags.zip)- ignoring"
                )
        elif filetype_id == MWADataFileType.VDIF.value:
            # filename format should be:
            #   obsid_subobsid_chNNN_beamNN.vdif
            # or if stitched:
            #   obsid_chNNN_beamNN.vdif
            if len(file_name_part) != 23 and len(file_name_part) != 34:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_subobsid_chXXX_beamXX.vdif or "
                    "obsid_chXXX_beamXX.vdif)- ignoring"
                )
        elif filetype_id == MWADataFileType.FILTERBANK.value:
            # filename format should be:
            #   obsid_subobsid_chNNN_beamNN.fil
            # or if stitched:
            #   obsid_chNNN_beamNN.fil
            if len(file_name_part) != 23 and len(file_name_part) != 34:
                valid = False
                validation_error = (
                    "Filename (excluding extension) is not in the correct"
                    f" format (incorrect length ({len(file_name_part)})."
                    " Format should be obsid_subobsid_chXXX_beamXX.fil or "
                    "obsid_chXXX_beamXX.fil)- ignoring"
                )
    # 5. Get project id and calibrator info
    if valid:
        # Now check that the observation is a calibrator by
        # looking at the associated metafits file
        if filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            # this file IS a metafits! So check it
            metafits_filename = filename
        else:
            metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

        # Does the metafits file exist??
        # Obtain a lock so we can only do this inside one thread
        with metafits_file_lock:
            if not os.path.exists(metafits_filename):
                logger.info(f"Metafits file {metafits_filename} not found. Atempting to download it")
                try:
                    download_metafits_file(obs_id, metafits_path)
                except requests.RequestException as download_exception:
                    valid = False
                    validation_error = (
                        f"Metafits file {metafits_filename} did not exist and"
                        " could not download one from web service."
                        f" {download_exception}"
                    )

            if valid:
                calibrator, project_id, calib_source = get_metafits_values(metafits_filename)

                # if calib_source is SUN then ignore
                if calib_source.upper() == "SUN":
                    calibrator = False

    return ValidationData(
        valid,
        obs_id,
        project_id,
        filetype_id,
        file_ext_part,
        calibrator,
        validation_error,
    )


def determine_bucket(full_filename: str, location: ArchiveLocation) -> str:
    """
    Return the destination bucket name for a file given its target archive location.

    Currently supports Acacia (ingest and MWA) and Banksia locations. DMF and
    Versity are not yet implemented and will raise NotImplementedError.

    Args:
        full_filename: Full path to the file. Only the basename is used to
            derive the bucket name.
        location: The target ArchiveLocation for this file.

    Returns:
        The bucket name string derived from the observation ID in the filename.

    Raises:
        NotImplementedError: If ``location`` is not AcaciaIngest, AcaciaMWA,
            or Banksia.
    """
    filename = os.path.basename(full_filename)

    # acacia and banksia
    if (
        location == ArchiveLocation.AcaciaIngest
        or location == ArchiveLocation.Banksia
        or location == ArchiveLocation.AcaciaMWA
    ):
        # determine bucket name
        bucket = get_bucket_name_from_filename(filename)
        return bucket

    else:
        # DMF and Versity not yet implemented
        raise NotImplementedError(f"Location {location} is not supported.")


def get_bucket_name_from_filename(filename: str) -> str:
    """
    Derive the archive bucket name from an MWA data filename.

    Extracts the observation ID from the first 10 characters of the basename
    and delegates to ``get_bucket_name_from_obs_id``.

    Args:
        filename: The MWA data filename (basename or full path). The first 10
            characters of the basename must be a valid integer observation ID.

    Returns:
        The bucket name string (e.g. ``'mwaingest-12345'``).
    """
    file_part = os.path.split(filename)[1]
    return get_bucket_name_from_obs_id(int(file_part[0:10]))


def get_bucket_name_from_obs_id(obs_id: int) -> str:
    """
    Generate an archive bucket name from an MWA observation ID.

    Uses the first 5 digits of the observation ID as the bucket suffix.
    This approach creates a new bucket roughly every 27 hours, reducing the
    risk of VCS jobs filling a single bucket beyond the 100K-file limit.

    Args:
        obs_id: The 10-digit MWA observation ID (GPS seconds-based).

    Returns:
        A bucket name string of the form ``'mwaingest-NNNNN'``.
    """
    # return the first 5 digits of the obsid
    # This means there will be a new bucket every ~27 hours
    # This is to reduce the chances of vcs jobs filling a bucket to more than
    # 100K of files
    return f"mwaingest-{str(obs_id)[0:5]}"


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
            # Read key from primary HDU
            return hdul[hdu_name].header[key]

    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}: {catch_all_exception}"
        ) from catch_all_exception


def get_metafits_values(metafits_filename: str) -> Tuple[bool, str, str]:
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


def read_config(config: ConfigParser, section: str, key: str, b64encoded=False):
    """
    Read a required string value from a ConfigParser object.

    If ``b64encoded`` is True the stored value is treated as Base64-encoded
    UTF-8 and decoded before being returned. The decoded value is masked in
    log output.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section.
        b64encoded: If True, Base64-decode the raw string value before
            returning it. Defaults to False.

    Returns:
        The configuration value as a string, Base64-decoded if requested.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
    """
    raw_value = config.get(section, key)

    if b64encoded:
        value = base64.b64decode(raw_value).decode("utf-8")
        value_to_log = "*" * len(value)
    else:
        value = raw_value
        value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_optional_config(config: ConfigParser, section: str, key: str, b64encoded=False) -> Optional[str]:
    """
    Read an optional string value from a ConfigParser object.

    Returns None if the key is absent or its value is an empty string.
    If ``b64encoded`` is True the stored value is treated as Base64-encoded
    UTF-8 and decoded before being returned. The decoded value is masked in
    log output.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key. The section must
            exist or KeyError is raised.
        key: The key name within the section. If absent, returns None.
        b64encoded: If True, Base64-decode the raw string value before
            returning it. Defaults to False. Has no effect if the value is
            absent or empty.

    Returns:
        The configuration value as a string (Base64-decoded if requested),
        or None if the key is missing or empty.

    Raises:
        KeyError: If ``section`` does not exist in the config.
    """
    value = None
    value_to_log = ""

    if config.has_section(section):
        if config.has_option(section, key):
            raw_value = config.get(section, key)
        else:
            raw_value = ""
    else:
        raise KeyError(f"Section {section} not found in config file")

    if raw_value == "":
        value = None
        value_to_log = "None"
    else:
        if b64encoded:
            value = base64.b64decode(raw_value).decode("utf-8")
            value_to_log = "*" * len(value)
        else:
            value = raw_value
            value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_config_list(config: ConfigParser, section: str, key: str):
    """
    Read a comma-separated string value from a ConfigParser object and return it as a list.

    Leading and trailing whitespace is stripped from the raw value before
    splitting. An empty (or whitespace-only) value returns an empty list.

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section whose value is a comma-separated list.

    Returns:
        A list of strings split on commas. Returns an empty list if the value
        is blank.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
    """
    string_value = read_config(config, section, key, False)

    # Ensure we trim string_value
    string_value = string_value.rstrip().lstrip()

    if len(string_value) > 0:
        return_list = string_value.split(",")
    else:
        return_list = []

    logger.info(
        f"Read cfg [{section}].{key}: '{string_value}' converted to list of {len(return_list)} items: {return_list}"
    )
    return return_list


def read_config_bool(config: ConfigParser, section: str, key: str):
    """
    Read a boolean value from a ConfigParser object.

    Delegates to ConfigParser.getboolean(), which accepts the standard
    truthy/falsy strings (``'1'``, ``'yes'``, ``'true'``, ``'on'`` and their
    negatives).

    Args:
        config: A ConfigParser instance with the configuration already loaded.
        section: The INI section name containing the key.
        key: The key name within the section.

    Returns:
        The configuration value as a bool.

    Raises:
        configparser.NoSectionError: If the section does not exist.
        configparser.NoOptionError: If the key does not exist within the section.
        ValueError: If the value cannot be interpreted as a boolean.
    """
    value = config.getboolean(section, key)

    logger.info(f"Read cfg [{section}].{key} == {value}")
    return value


def get_hostname() -> str:
    """
    Return the short hostname of the running machine in lowercase.

    Any domain suffix (everything after the first ``.``) is stripped so that
    the fully-qualified domain name is never returned.

    Returns:
        The lowercase short hostname string (e.g. ``'mwax01'``).
    """
    hostname = socket.gethostname()

    # ensure we remove anything after a . in case we got the fqdn
    split_hostname = hostname.split(".")[0]

    return split_hostname.lower()


def process_mwax_stats(
    mwax_stats_dir: str,
    full_filename: str,
    numa_node: Optional[int],
    timeout: int,
    stats_dump_dir: str,
    metafits_path: str,
) -> bool:
    """
    Run the ``mwax_stats`` binary against an MWA visibility or voltage file.

    Constructs and executes a shell command of the form::

        <mwax_stats_dir>/mwax_stats -t <full_filename> -m <metafits_filename> -o <stats_dump_dir>

    Args:
        mwax_stats_dir: Directory containing the ``mwax_stats`` binary.
        full_filename: Full path to the data file to analyse. The observation ID
            is derived from the first 10 characters of the basename.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.
        stats_dump_dir: Directory where ``mwax_stats`` will write its output.
        metafits_path: Directory containing the metafits file for the observation.

    Returns:
        True if ``mwax_stats`` exited successfully, False otherwise.
    """
    # This code will execute the mwax stats command
    obs_id = str(os.path.basename(full_filename)[0:10])

    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    cmd = f"{mwax_stats_dir}/mwax_stats -t {full_filename} -m {metafits_filename} -o {stats_dump_dir}"

    logger.debug(f"{full_filename}- attempting to run stats: {cmd}")

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} mwax_stats success in {elapsed:.3f} seconds")
    else:
        logger.error(f"{full_filename} mwax_stats failed with error {stdout}")

    return return_value


def load_psrdada_ringbuffer(full_filename: str, ringbuffer_key: str, numa_node, timeout: int) -> bool:
    """
    Load a subfile into a PSRDADA ring buffer using ``dada_diskdb``.

    Constructs and executes a shell command of the form::

        dada_diskdb -k <ringbuffer_key> -f <full_filename>

    Logs the transfer rate in Gbps on success.

    Args:
        full_filename: Full path to the ``.sub`` subfile to load.
        ringbuffer_key: Hexadecimal PSRDADA ring buffer key (e.g. ``'dada'``).
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        True if ``dada_diskdb`` exited successfully, False otherwise.
    """
    logger.debug(f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}")

    cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(
            f"{full_filename} load_psrdada_ringbuffer success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} sec at"
            f" {gbps_per_sec:.3f} Gbps)"
        )
    else:
        logger.error(f"{full_filename} load_psrdada_ringbuffer failed with error {stdout}")

    return return_value


def run_mwax_packet_stats(mwax_stats_dir: str, full_filename: str, output_dir: str, numa_node, timeout: int) -> bool:
    """
    Run the ``mwax_packet_stats`` Rust binary against a subfile to generate packet statistics.

    Constructs and executes a shell command of the form::

        <mwax_stats_dir>/mwax_packet_stats -o <output_dir> -s <full_filename>

    Args:
        mwax_stats_dir: Directory containing the ``mwax_packet_stats`` binary.
        full_filename: Full path to the ``.sub`` subfile to analyse.
        output_dir: Directory where ``mwax_packet_stats`` will write its output.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        True if ``mwax_packet_stats`` exited successfully, False otherwise.
    """
    logger.debug(f"{full_filename}- attempting to execute mwax_packet_stats")

    cmd = f"{mwax_stats_dir}/mwax_packet_stats -o {output_dir} -s {full_filename}"

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} mwax_packet_stats success in {elapsed:.3f} sec")
    else:
        logger.error(f"{full_filename} mwax_packet_stats failed with error {stdout}")

    return return_value


def scan_for_existing_files_and_add_to_queue(
    watch_dir: str,
    pattern: str,
    recursive: bool,
    queue_target: queue.Queue,
    exclude_pattern=None,
):
    """
    Scan a directory for files matching a pattern and add them to a regular Queue.

    Files are sorted before being enqueued to provide a deterministic ordering.

    Args:
        watch_dir: Root directory to scan.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` internally.
        recursive: If True, scan all subdirectories recursively.
        queue_target: The ``queue.Queue`` instance to add matched filenames to.
        exclude_pattern: Optional glob suffix pattern. Files matching this
            pattern are excluded from the results. Defaults to None (no exclusion).
    """
    files = scan_directory(watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"{watch_dir}: Found {len(files)} files")

    for filename in files:
        queue_target.put(filename)
        logger.info(f"{watch_dir}: {os.path.basename(filename)} added to queue")


def scan_for_existing_files_and_add_to_priority_queue(
    metafits_path: str,
    watch_dir: str,
    pattern: str,
    recursive: bool,
    queue_target: queue.PriorityQueue,
    list_of_correlator_high_priority_projects: list,
    list_of_vcs_high_priority_projects: list,
    exclude_pattern=None,
):
    """
    Scan a directory for files matching a pattern and add them to a PriorityQueue.

    Each file's priority is determined by ``get_priority()``. Files are sorted
    before priority assignment to provide a deterministic ordering when multiple
    files share the same priority.

    Args:
        metafits_path: Directory containing metafits files, passed through to
            ``get_priority()`` for file type and project ID resolution.
        watch_dir: Root directory to scan.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` internally.
        recursive: If True, scan all subdirectories recursively.
        queue_target: The ``queue.PriorityQueue`` instance to add
            ``(priority, MWAXPriorityQueueData)`` tuples to.
        list_of_correlator_high_priority_projects: Project IDs that should
            receive elevated priority for correlator observations.
        list_of_vcs_high_priority_projects: Project IDs that should receive
            elevated priority for VCS observations.
        exclude_pattern: Optional glob suffix pattern. Files matching this
            pattern are excluded from the results. Defaults to None (no exclusion).
    """
    files = scan_directory(watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"{watch_dir}: Found {len(files)} files")

    for filename in files:
        priority = get_priority(
            filename,
            metafits_path,
            list_of_correlator_high_priority_projects,
            list_of_vcs_high_priority_projects,
        )
        queue_target.put((priority, MWAXPriorityQueueData(filename)))
        logger.info(f"{watch_dir}: {os.path.basename(filename)} added to queue with priority {priority}")


def scan_directory(watch_dir: str, pattern: str, recursive: bool, exclude_pattern) -> list:
    """
    Scan a directory for files matching a glob pattern and return them as a list.

    Args:
        watch_dir: Root directory to scan. Resolved to an absolute path internally.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` (and ``'**/'`` for recursive scans) internally.
        recursive: If True, search all subdirectories using ``**`` glob syntax.
        exclude_pattern: Glob suffix pattern for files to exclude. Files whose
            paths match ``<watch_dir>/*<exclude_pattern>`` are removed from
            the results. Pass None (or a falsy value) to skip exclusion.

    Returns:
        A list of absolute path strings for all matched (and non-excluded) files.
    """
    # Watch dir must end in a slash for the iglob to work
    # Just loop through all files and add them to the queue
    if recursive:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "**/*" + pattern)
        logger.info(f"{watch_dir}: Scanning recursively for files matching {find_pattern}...")
    else:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "*" + pattern)
        logger.info(f"{watch_dir}: Scanning for files matching *{pattern}...")

    files = glob.glob(find_pattern, recursive=recursive)

    # Now exclude files if they match the exclude pattern
    if exclude_pattern:
        exclude_glob = os.path.join(os.path.abspath(watch_dir), "*" + exclude_pattern)
        logger.info(f"{watch_dir}: Excluding files *{exclude_pattern}...")
        return [fn for fn in files if fn not in glob.glob(exclude_glob)]
    else:
        return files


def send_multicast(
    multicast_interface_ip: str,
    dest_multicast_ip: str,
    dest_multicast_port: int,
    message: bytes,
    ttl_hops: int,
):
    """
    Send a UDP datagram to an IP multicast group.

    Creates a UDP socket, configures the outbound multicast interface and
    TTL, then transmits ``message`` to the specified group address and port.
    The socket is closed in a finally block regardless of outcome.

    Args:
        multicast_interface_ip: IP address of the local network interface to
            use for outbound multicast traffic. Must be associated with a
            multicast-capable interface.
        dest_multicast_ip: Destination multicast group IP address
            (e.g. ``'239.255.0.1'``).
        dest_multicast_port: Destination UDP port number.
        message: The raw bytes payload to transmit.
        ttl_hops: IP multicast TTL / hop limit. Controls how many router hops
            the datagram may traverse.

    Raises:
        Exception: If ``sendto`` sends zero bytes, or if any socket operation
            raises an unexpected error.
    """

    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # Disable loopback so you do not receive your own datagrams.
    # loopback = 0
    # if sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP,
    #                    loopback) != 0:
    #    raise Exception("Error setsockopt IP_MULTICAST_LOOP failed")

    # Set the time-to-live for messages.
    hops = struct.pack("b", ttl_hops)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, hops)

    # Set local interface for outbound multicast datagrams.
    # The IP address specified must be associated with a local,
    # multicast - capable interface.
    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_MULTICAST_IF,
        socket.inet_aton(multicast_interface_ip),
    )

    try:
        # Send data to the multicast group
        if sock.sendto(message, (dest_multicast_ip, dest_multicast_port)) == 0:
            raise Exception("Error sock.sendto() sent 0 bytes")

    except Exception as catch_all_exception:
        raise catch_all_exception
    finally:
        sock.close()


def get_ip_address(ifname: str) -> str:
    """
    Return the IPv4 address assigned to a named network interface.

    Uses the ``SIOCGIFADDR`` ioctl to query the kernel directly.

    Args:
        ifname: The network interface name (e.g. ``'eth0'``, ``'bond0'``).
            Only the first 15 characters are used.

    Returns:
        The IPv4 address as a dotted-decimal string (e.g. ``'192.168.1.10'``).

    Raises:
        OSError: If the ioctl call fails (e.g. the interface does not exist).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            sock.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack("256s", bytes(ifname[:15], "utf-8")),
        )[20:24]
    )


def get_primary_ip_address() -> str:
    """
    Return the primary IPv4 address of this host.

    Resolves the fully-qualified domain name of the host and returns the
    corresponding IP address.

    Returns:
        The primary IPv4 address as a dotted-decimal string.
    """
    return socket.gethostbyname(socket.getfqdn())


def get_disk_space_bytes(path: str) -> typing.Tuple[int, int, int]:
    """
    Return disk usage statistics for the filesystem containing ``path``.

    Args:
        path: Any path on the filesystem to query (file or directory).

    Returns:
        A named tuple ``(total, used, free)`` with values in bytes, as
        returned by ``shutil.disk_usage``.
    """
    # Get disk space: total, used and free
    return shutil.disk_usage(path)


def do_checksum_md5(full_filename: str, numa_node: Optional[int], timeout: int) -> str:
    """
    Compute the MD5 checksum of a file by running the system ``md5sum`` command.

    Args:
        full_filename: Full path to the file to checksum.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        The 32-character lowercase hexadecimal MD5 digest string.

    Raises:
        Exception: If ``md5sum`` returns a non-zero exit code, or if the parsed
            checksum is not exactly 32 characters.
    """

    # default output of md5 hash command is:
    # "5ce49e5ebd72c41a1d70802340613757
    # /visdata/incoming/1320133480_20211105074422_ch055_000.fits"
    md5output = ""
    checksum = ""

    logger.debug(f"{full_filename}- running md5sum...")

    cmdline = f"md5sum {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, md5output = mwax_command.run_command_ext(cmdline, numa_node, timeout, False)
    elapsed = time.time() - start_time

    size_megabytes = size / (1000 * 1000)
    mb_per_sec = size_megabytes / elapsed

    if return_value:
        # md5sum output format is: "<hash>  <filename>"
        # Split on whitespace and take the first token to avoid any risk of
        # the filename appearing in the hash field if it contains unusual characters.
        checksum = md5output.split()[0]

        # MD5 hash is ALWAYS 32 characters
        if len(checksum) == 32:
            logger.info(
                f"{full_filename} md5sum success"
                f" {checksum} ({size_megabytes:.3f}MB in {elapsed:.3f} secs at"
                f" {mb_per_sec:.3f} MB/s)"
            )
            return checksum
        else:
            raise Exception(f"Calculated MD5 checksum is not valid: md5 output {md5output}")
    else:
        raise Exception(f"md5sum returned an unexpected return code {return_value}")


def get_priority(
    filename: str,
    metafits_path: str,
    list_of_correlator_high_priority_projects: list,
    list_of_vcs_high_priority_projects: list,
) -> int:
    """
    Determine the archive priority integer for a given MWA data file.

    A lower integer means higher priority (i.e. the file will be dequeued
    first from a ``PriorityQueue``). The priority scheme is:

    ====  ==========================================================
    1     Metafits / PPD files (small, quick to archive)
    2     Calibrator correlator observations
    3     Correlator observations for high-priority projects
    5     VDIF / filterbank files for high-priority VCS projects
    10    VDIF / filterbank files for normal VCS projects
    20    VCS voltage files for high-priority projects
    30    Normal correlator observations
    90    Normal VCS voltage observations
    100   Default / unrecognised (should not occur for valid files)
    ====  ==========================================================

    Args:
        filename: Full path to the MWA data file.
        metafits_path: Directory containing metafits files, used by
            ``validate_filename`` to resolve project ID and calibrator status.
        list_of_correlator_high_priority_projects: Project IDs that receive
            elevated priority (level 3) for correlator observations.
        list_of_vcs_high_priority_projects: Project IDs that receive elevated
            priority (levels 5 and 20) for VCS / beamformer observations.

    Returns:
        An integer priority value. Lower values are higher priority.

    Raises:
        Exception: If ``validate_filename`` reports the file as invalid.
    """
    return_priority = 100  # default if we don't do anything else

    # get info about this file
    val: ValidationData = validate_filename(filename, metafits_path)

    if val.valid:
        if val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            if val.calibrator:
                return_priority = 2
            else:
                if val.project_id in list_of_correlator_high_priority_projects:
                    return_priority = 3
                else:
                    return_priority = 30
        elif val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
            if val.project_id in list_of_vcs_high_priority_projects:
                return_priority = 20
            else:
                return_priority = 90
        elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            return_priority = 1
        elif val.filetype_id == MWADataFileType.VDIF.value or val.filetype_id == MWADataFileType.FILTERBANK.value:
            # VDIF and filterbank files are treated as high priority as they are small and quick to archive
            if val.project_id in list_of_vcs_high_priority_projects:
                return_priority = 5
            else:
                return_priority = 10
    else:
        raise Exception(f"File {filename} is not valid! Reason: {val.validation_message}")

    return return_priority


def inject_subfile_header(subfile_filename: str, key_value_pairs: str):
    """
    Overwrite the last line of a PSRDADA subfile header with new key-value pairs.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the subfile,
    replaces the final line with ``key_value_pairs`` (padded with null bytes
    to preserve the exact header size), and writes the modified header back
    in place.

    Multiple key-value pairs should be separated by ``'\\n'``. Each pair
    must be space-separated (``'KEY VALUE'``) and end with a newline.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to modify in place.
        key_value_pairs: String of one or more ``'KEY VALUE\\n'`` pairs to
            write into the last line of the header. Must not exceed the
            length of the existing last line.

    Raises:
        ValueError: If ``key_value_pairs`` is longer than the available space
            in the last line of the header.
        Exception: If the resulting header byte array is not exactly
            ``PSRDADA_HEADER_BYTES`` bytes long.
    """
    data = []

    # Read the psrdada header data in to a list (one line per item)
    with open(subfile_filename, "rb") as subfile:
        data = subfile.read(PSRDADA_HEADER_BYTES).decode("UTF-8").split("\n")

    last_line_index = len(data) - 1
    last_row_len = len(data[last_line_index])

    new_settings_len = len(key_value_pairs)

    if new_settings_len > last_row_len:
        raise ValueError(
            f"inject_subfile_header(): key_value_pairs length ({new_settings_len})"
            f" exceeds the available space in the last header line ({last_row_len})."
            " Cannot inject without corrupting the header."
        )

    null_trail = "\0" * (last_row_len - new_settings_len)
    data[last_line_index] = key_value_pairs + null_trail

    # convert our list of lines back to a byte array
    new_string = "\n".join(data)

    new_bytes = bytes(new_string, "UTF-8")
    if len(new_bytes) != PSRDADA_HEADER_BYTES:
        raise Exception(
            "inject_subfile_header(): new_bytes length is not"
            f" {PSRDADA_HEADER_BYTES} as expected it is {len(new_bytes)}."
            f" Newbytes = [{new_string}]"
        )

    # Overwrite the first 4096 bytes with our updated header
    with open(subfile_filename, "r+b") as subfile:
        subfile.seek(0)
        subfile.write(new_bytes)


def inject_beamformer_headers(subfile_filename: str, beamformer_settings: str):
    """
    Append beamformer settings to the existing PSRDADA subfile header.

    A thin wrapper around ``inject_subfile_header`` for the beamformer use case.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to modify in place.
        beamformer_settings: String of one or more ``'KEY VALUE\\n'`` pairs
            representing the beamformer configuration to inject.
    """
    inject_subfile_header(subfile_filename, beamformer_settings)


def read_subfile_value(filename: str, key: str) -> typing.Union[str, None]:
    """
    Read a single keyword value from a PSRDADA subfile header.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the file and
    searches line-by-line for a line with exactly two whitespace-separated
    tokens whose first token matches ``key``.

    Args:
        filename: Path to the ``.sub`` subfile to read.
        key: The PSRDADA header keyword to look up (case-sensitive).

    Returns:
        The value string associated with ``key``, or None if the keyword is
        not found in the header.
    """
    subfile_value = None

    with open(filename, "rb") as subfile:
        subfile_text = subfile.read(PSRDADA_HEADER_BYTES).decode()
        subfile_text_lines = subfile_text.splitlines()

        for line in subfile_text_lines:
            split_line = line.split()

            # We should have 2 items, keyword and value
            if len(split_line) == 2:
                keyword = split_line[0].strip()
                value = split_line[1].strip()

                if keyword == key:
                    subfile_value = value
                    break

    return subfile_value


def read_subfile_values(filename: str, keys: list[str]) -> dict:
    """
    Read multiple keyword values from a PSRDADA subfile header in a single pass.

    Reads the first ``PSRDADA_HEADER_BYTES`` (4096) bytes of the file and
    searches line-by-line for lines with exactly two whitespace-separated
    tokens. Stops early once all requested keys have been found.

    Args:
        filename: Path to the ``.sub`` subfile to read.
        keys: A list of PSRDADA header keywords to look up (case-sensitive).

    Returns:
        A dict mapping each key in ``keys`` to its value string, or None for
        any keyword not found in the header.
    """
    subfile_values = dict()
    found = 0

    # Create the dict with None values for all keys
    for key in keys:
        subfile_values[key] = None

    with open(filename, "rb") as subfile:
        subfile_text = subfile.read(PSRDADA_HEADER_BYTES).decode()
        subfile_text_lines = subfile_text.splitlines()

        for line in subfile_text_lines:
            split_line = line.split()

            # We should have 2 items, keyword and value
            if len(split_line) == 2:
                keyword = split_line[0].strip()
                value = split_line[1].strip()

                if keyword in keys:
                    subfile_values[keyword] = value
                    found += 1

                    if found == len(keys):
                        # Exit loop early if we have all the values
                        break

    return subfile_values


def read_subfile_trigger_value(subfile_filename: str):
    """
    Read the ``TRIGGER_ID`` value from a PSRDADA subfile header.

    A convenience wrapper around ``read_subfile_value`` that casts the
    result to an integer.

    Args:
        subfile_filename: Path to the ``.sub`` subfile to read.

    Returns:
        The trigger ID as an int, or None if the ``TRIGGER_ID`` keyword is
        not present in the header.
    """
    value = read_subfile_value(subfile_filename, PSRDADA_TRIGGER_ID)

    if value:
        return int(value)
    else:
        return None


def write_mock_subfile_from_header(output_filename, header):
    """
    Write a mock PSRDADA subfile from a pre-built header string (for use in tests).

    Pads the header to exactly 4096 bytes with null bytes, then appends 256
    bytes of incrementing data (0x00–0xFF) to simulate a minimal subfile payload.

    Args:
        output_filename: Path to write the mock subfile to.
        header: ASCII string containing the PSRDADA header content. Must be
            shorter than 4096 bytes to leave room for null padding.
    """

    # Append the remainder of the 4096 bytes
    remainder_len = 4096 - len(header)
    padding = [0x0 for _ in range(remainder_len)]
    assert len(padding) == remainder_len
    # add 255 bytes of data to this subfile
    data_padding = [x for x in range(256)]
    assert len(data_padding) == 256

    # Convert to bytes
    test_header_bytes = bytes(header, "UTF-8")

    # Generate a test sub file
    with open(output_filename, "wb") as write_file:
        write_file.write(test_header_bytes)
        write_file.write(bytearray(padding))
        write_file.write(bytearray(data_padding))


def write_mock_subfile(
    output_filename,
    obs_id,
    subobs_id,
    mode,
    obs_offset,
    rec_channel,
    corr_channel,
):
    """
    Write a complete mock PSRDADA subfile for use in tests.

    Constructs a realistic PSRDADA ASCII header using the supplied parameters,
    pads it to 4096 bytes, and appends a 256-byte dummy data payload.

    Args:
        output_filename: Path to write the mock subfile to.
        obs_id: MWA observation ID to embed in the header (``OBS_ID``).
        subobs_id: Sub-observation ID to embed in the header (``SUBOBS_ID``).
        mode: Correlator mode string to embed in the header (``MODE``).
        obs_offset: Offset in seconds from the start of the observation
            (``OBS_OFFSET``).
        rec_channel: Receiver coarse channel number (``COARSE_CHANNEL``).
        corr_channel: Correlator coarse channel number
            (``CORR_COARSE_CHANNEL``).
    """
    # Create ascii header
    header = (
        "HDR_SIZE 4096\n"
        "POPULATED 1\n"
        f"OBS_ID {obs_id}\n"
        f"SUBOBS_ID {subobs_id}\n"
        f"MODE {mode}\n"
        "UTC_START 2023-01-13-03:33:10\n"
        f"OBS_OFFSET {obs_offset}\n"
        "NBIT 8\n"
        "NPOL 2\n"
        "NTIMESAMPLES 64000\n"
        "NINPUTS 256\n"
        "NINPUTS_XGPU 256\n"
        "APPLY_PATH_WEIGHTS 0\n"
        "APPLY_PATH_DELAYS 1\n"
        "APPLY_PATH_PHASE_OFFSETS 1\n"
        "INT_TIME_MSEC 500\n"
        "FSCRUNCH_FACTOR 200\n"
        "APPLY_VIS_WEIGHTS 0\n"
        "TRANSFER_SIZE 5275648000\n"
        "PROJ_ID G0060\n"
        "EXPOSURE_SECS 200\n"
        f"COARSE_CHANNEL {rec_channel}\n"
        f"CORR_COARSE_CHANNEL {corr_channel}\n"
        "SECS_PER_SUBOBS 8\n"
        "UNIXTIME 1673580790\n"
        "UNIXTIME_MSEC 0\n"
        "FINE_CHAN_WIDTH_HZ 40000\n"
        "NFINE_CHAN 32\n"
        "BANDWIDTH_HZ 1280000\n"
        "SAMPLE_RATE 1280000\n"
        "MC_IP 0.0.0.0\n"
        "MC_PORT 0\n"
        "MC_SRC_IP 0.0.0.0\n"
        "MWAX_U2S_VER 2.09-87\n"
        "IDX_PACKET_MAP 0+200860892\n"
        "IDX_METAFITS 32+1\n"
        "IDX_DELAY_TABLE 16383744+0\n"
        "IDX_MARGIN_DATA 256+0\n"
        "MWAX_SUB_VER 2\n"
    )

    write_mock_subfile_from_header(output_filename, header)


def should_project_be_archived(project_id: str) -> bool:
    """
    Determine whether data for a given project ID should be archived.

    Project ``C123`` is a test/commissioning project whose data should not
    be archived. If this list grows or changes frequently it should be moved
    into a configuration file.

    Args:
        project_id: The MWA project ID string (case-insensitive).

    Returns:
        False if ``project_id`` is ``'C123'`` (case-insensitive), True for
        all other project IDs.
    """
    if project_id.upper() == "C123":
        return False
    else:
        return True


def call_webservice(obs_id: int, url_list: list[str], data, max_retries: int = 10, wait: int = 30) -> requests.Response:
    """
    Call a list of MWA web service URLs in order, retrying on transient failures.

    Iterates through ``url_list`` on each attempt, returning immediately on an
    HTTP 200 response. HTTP 4xx responses are treated as permanent failures and
    are not retried (``ValueError`` is raised and excluded from tenacity retries).
    All other failures (non-200 status codes, network exceptions) cause the next
    URL to be tried; if all URLs are exhausted the attempt fails and tenacity
    will retry the entire sequence up to ``max_retries`` times with a fixed
    ``wait``-second delay between attempts.

    Note: With the default arguments (``max_retries=10``, ``wait=30``) this
    function may block for up to ~5 minutes before raising.

    Args:
        obs_id: The MWA observation ID, used only for log messages.
        url_list: Ordered list of URLs to try. Each must accept a GET request
            with optional ``data`` parameters.
        data: Query parameters to pass to ``requests.get()``, or None.
        max_retries: Maximum number of retry attempts via tenacity. Defaults to 10.
        wait: Seconds to wait between tenacity retry attempts. Defaults to 30.

    Returns:
        The first successful ``requests.Response`` object (HTTP status 200).

    Raises:
        Exception: If any URL returns an HTTP 4xx response (permanent error,
            not retried).
        requests.RequestException: If all URLs fail on every attempt across
            all retries.
    """

    @retry(stop=stop_after_attempt(max_retries), wait=wait_fixed(wait), retry=retry_if_not_exception_type((ValueError)))
    def call_webservice_inner(obs_id: int, url_list: list[str], data) -> requests.Response:
        """Call each url in the list until: a response of
        200 (in which case return the response)
        """
        i = 0

        while i < len(url_list):
            url = url_list[i]

            logger.debug(f"{obs_id}: trying with {url} with data ({'' if data is None else data})")

            try:
                response = requests.get(url, data, timeout=30)

                if response.status_code == 200:
                    logger.debug(f"{obs_id}: returned 200 (success)")
                    return response

                elif response.status_code >= 400 and response.status_code < 500:
                    error_message = f"{obs_id}: returned {response.status_code} {response.text} (failure)"
                    logger.error(error_message)
                    raise ValueError(error_message)

                else:
                    # Non-200 status code- try next url
                    logger.warning(f"{obs_id}: returned {response.status_code} {response.text} (failure)")
            except ValueError:
                raise

            except Exception as e:
                # Exception raised- try next url
                logger.exception(f"{obs_id}: exception", e)

            # try next url
            i += 1

        # We tried all urls without success- up to tenacity to retry X times now
        logger.error(f"{obs_id}: failed after trying {len(url_list)} urls.")
        raise requests.RequestException(f"{obs_id}: call_webservice()- failed after trying {len(url_list)} urls.")

    return call_webservice_inner(obs_id, url_list, data)


def get_data_files_for_obsid_from_webservice(
    obs_id: int,
) -> list[str]:
    """
    Retrieve a list of data filenames for an observation from the MWA web service.

    Queries the ``metadata/data_files`` endpoint (MRO-local first, then public)
    for the given observation ID and returns filenames whose file type is
    ``MWAX_VISIBILITIES`` or ``HW_LFILES``. Results are sorted alphabetically.

    Args:
        obs_id: The 10-digit MWA observation ID to query.

    Returns:
        A sorted list of filename strings for all matching data files,
        regardless of whether they have been archived at Pawsey.

    Raises:
        Exception: If the web service cannot be reached after all retries.
    """
    urls = ["http://mro.mwa128t.org/metadata/data_files", "http://ws.mwatelescope.org/metadata/data_files"]
    data = {"obs_id": obs_id, "terse": False, "all_files": True}

    # On failure of all urls and retries it will raise an exception
    result = call_webservice(obs_id, urls, data)

    files = json.loads(result.text)
    file_list = [
        file
        for file in files
        if files[file]["filetype"] == MWADataFileType.MWAX_VISIBILITIES.value
        or files[file]["filetype"] == MWADataFileType.HW_LFILES.value
    ]
    file_list.sort()
    return file_list


def get_data_files_with_hostname_for_obsid_from_webservice(
    obs_id: int,
) -> list[tuple[str, str]]:
    """
    Retrieve data filenames and their host locations for an observation from the MWA web service.

    Queries the ``metadata/data_files`` endpoint (MRO-local first, then public)
    for the given observation ID and returns ``(filename, hostname)`` tuples
    for files whose type is ``MWAX_VISIBILITIES`` or ``HW_LFILES``. Results
    are sorted alphabetically by filename.

    Args:
        obs_id: The 10-digit MWA observation ID to query.

    Returns:
        A sorted list of ``(filename, hostname)`` tuples for all matching data
        files, regardless of whether they have been archived at Pawsey.

    Raises:
        Exception: If the web service cannot be reached after all retries.
    """
    urls = ["http://mro.mwa128t.org/metadata/data_files", "http://ws.mwatelescope.org/metadata/data_files"]
    data = {"obs_id": obs_id, "terse": False, "all_files": True}

    # On failure of all urls and retries it will raise an exception
    result = call_webservice(obs_id, urls, data)

    files = json.loads(result.text)
    file_list = [
        (file, files[file]["host"])
        for file in files
        if files[file]["filetype"] == MWADataFileType.MWAX_VISIBILITIES.value
        or files[file]["filetype"] == MWADataFileType.HW_LFILES.value
    ]
    file_list.sort()
    return file_list


@retry(stop=stop_after_attempt(5), wait=wait_fixed(10))
def remove_file(filename: str, raise_error: bool) -> bool:
    """
    Delete a file from the filesystem, with up to 5 automatic retries.

    Retries are handled by tenacity with a 10-second fixed wait between
    attempts. Retries only occur when ``raise_error`` is True and the deletion
    raises an exception; when ``raise_error`` is False a failed deletion is
    logged as a warning and True is returned without retrying.

    Note: When ``raise_error`` is False this function returns True even if the
    file was not successfully deleted (e.g. because it had already been moved
    or removed by another process). Callers that require confirmation of
    deletion should pass ``raise_error=True``.

    Args:
        filename: Full path to the file to delete.
        raise_error: If True, log an error and re-raise the exception on
            failure (triggering a tenacity retry). If False, log a warning
            and return True without raising.

    Returns:
        True if the file was deleted, or if ``raise_error`` is False and the
        deletion failed (assumed already gone).

    Raises:
        Exception: The underlying OS exception, if ``raise_error`` is True and
            all retry attempts are exhausted.
    """
    try:
        os.remove(filename)
        logger.info(f"{filename}- file deleted")
        return True

    except Exception as delete_exception:  # pylint: disable=broad-except
        if raise_error:
            logger.error(f"{filename}- Error deleting: {delete_exception}. Retrying up to 5 times.")
            raise delete_exception
        else:
            logger.warning(f"{filename}- Error deleting: {delete_exception}. File may have been moved or removed.")
            return True


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
    utc_datetime: astrotime.Time = astrotime.Time(date_time, scale="utc")
    current_gpstime = utc_datetime.gps
    return int(current_gpstime)


# Return the GPS seconds as an integer of Now
def get_gpstime_of_now() -> int:
    """
    Return the current time as an integer GPS time (seconds since GPS epoch).

    Returns:
        The current GPS time as an integer number of seconds since the GPS
        epoch (6 January 1980 00:00:00 UTC).
    """
    return get_gpstime_of_datetime(datetime.datetime.now(datetime.timezone.utc))


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
    return gigabytes / 1.07374


def delete_files_older_than(path: str, older_than_seconds: int, extensions: list[str]) -> list[str]:
    """
    Delete files in a directory that are older than a threshold and match given extensions.

    Scans ``path`` non-recursively and deletes any regular file whose
    modification time is at least ``older_than_seconds`` seconds in the past
    and whose extension (case-insensitive) is in ``extensions``. Files that
    cannot be stat-ed or deleted (e.g. due to permissions) are silently skipped.

    Args:
        path: Directory to scan. Must exist and be a directory.
        older_than_seconds: Age threshold in seconds (based on ``mtime``).
            Files with ``(now - mtime) >= older_than_seconds`` are candidates
            for deletion. Must be non-negative.
        extensions: List of file extensions to match, with or without a
            leading dot, case-insensitive (e.g. ``['log', '.tmp', '.TXT']``).
            An empty list matches nothing and no files will be deleted.

    Returns:
        A list of absolute path strings for every file successfully deleted.

    Raises:
        ValueError: If ``path`` is not an existing directory, or if
            ``older_than_seconds`` is negative.
    """
    # --- Validate inputs ---
    p = Path(path)
    if not p.exists() or not p.is_dir():
        raise ValueError(f"Path is not a directory or does not exist: {path}")

    if older_than_seconds < 0:
        raise ValueError("older_than_seconds must be non-negative")

    # Normalize extensions: make them lower-case and ensure they start with a dot.
    norm_exts = {("." + ext.lower().lstrip(".")) for ext in extensions}

    now = time.time()
    deleted: List[str] = []

    # Iterate non-recursively over files in the directory
    for entry in p.iterdir():
        # Only operate on files (skip directories, symlinks-to-directories, etc.)
        try:
            is_file = entry.is_file()
        except OSError:
            # Some entries may be inaccessible; skip them
            continue

        if not is_file:
            continue

        # Check extension match (case-insensitive)
        suffix = entry.suffix.lower()  # includes leading dot if any
        if norm_exts and suffix not in norm_exts:
            continue
        # If norm_exts is empty, treat as "match none" (requires explicit extensions)
        if not norm_exts:
            continue

        # Check age based on modification time (mtime)
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            # Could be permission issues; skip
            continue

        file_age = now - mtime
        if file_age >= older_than_seconds:
            # Attempt deletion
            try:
                os.remove(entry)  # pathlib's unlink() also works; os.remove is fine for files
                deleted.append(str(entry.resolve()))
            except OSError:
                # If deletion fails (permissions, locked files), skip silently or log if desired
                # You could collect failures separately if you want to report them.
                continue

    return deleted


def push_message_to_redis(redis_host: str, redis_queue_key: str, message_data):
    """
    Push a message onto a Redis list as a JSON-serialised string.

    Connects to Redis on port 6379 of ``redis_host``, serialises
    ``message_data`` to JSON, and pushes it onto the left end of the list at
    ``redis_queue_key`` using ``LPUSH``. Retries up to ``MAX_RETRIES`` (3)
    times with a 1-second delay on ``RedisError``.

    Args:
        redis_host: Hostname or IP address of the Redis server.
        redis_queue_key: The Redis list key to push the message onto.
        message_data: Any JSON-serialisable Python object to push as the message.

    Raises:
        redis.RedisError: If the push fails on all retry attempts.
    """
    MAX_RETRIES = 3

    json_message = json.dumps(message_data)

    # Connect to Redis
    attempt = 1
    while attempt <= MAX_RETRIES:
        try:
            with redis.Redis(host=redis_host, port=6379, decode_responses=True) as r:
                r.lpush(redis_queue_key, json_message)
                return

        except redis.RedisError as e:
            # wait 1 second between retries
            time.sleep(1)

            if attempt >= MAX_RETRIES:
                raise redis.RedisError(
                    f"Could not push message to Redis host {redis_host} [{redis_queue_key}] after {attempt} tries: {e}"
                ) from e

        attempt += 1


def copy_subfile_to_disk_cp(
    filename: str,
    numa_node: int,
    destination_path: str,
    timeout: int,
    destination_filename: str = ".",
) -> bool:
    """
    Copy a subfile to a destination directory using the system ``cp`` command.

    .. deprecated::
        Replaced by ``copy_subfile_to_disk_dd()``. Retained for reference.

    Args:
        filename: Full path to the source subfile.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        destination_path: Destination directory path.
        timeout: Maximum number of seconds to wait for the command to complete.
        destination_filename: Destination filename within ``destination_path``.
            Defaults to ``'.'`` (i.e. preserve the source filename).

    Returns:
        True if ``cp`` exited successfully, False otherwise.
    """
    logger.info(f"{filename}- Copying file into {destination_path}")

    command = f"cp {filename} {destination_path}/{destination_filename}"

    start_time = time.time()
    retval, stdout = mwax_command.run_command_ext(command, numa_node, timeout, False)
    elapsed = time.time() - start_time

    if retval:
        logger.info(
            f"{filename}- Copying file into"
            f" {destination_path}/{destination_filename} was successful"
            f" (took {elapsed:.3f} secs)."
        )
    else:
        logger.error(
            f"{filename}- Copying file into {destination_path}/{destination_filename} failed with error {stdout}"
        )

    return retval


def copy_subfile_to_disk_dd(
    filename: str,
    numa_node: int,
    destination_path: str,
    timeout: int,
    destination_filename: str,
    bytes_to_write: int,
) -> bool:
    """
    Copy the first N bytes of a subfile to disk using the system ``dd`` command.

    Used when subfiles are pre-allocated for a larger tile count than the
    observation actually uses (e.g. allocated for 144T but the observation is
    128T). The ``u2s`` process writes only the relevant data, so only the first
    ``bytes_to_write`` bytes need to be copied; the rest of the file is empty.

    Uses ``oflag=direct`` and ``iflag=count_bytes`` for efficient I/O, and logs
    the transfer rate in GB/sec on success.

    Note: Unlike ``copy_subfile_to_disk_cp``, ``destination_filename`` must be
    an actual filename — ``dd`` does not accept ``'.'`` as a destination.

    Args:
        filename: Full (or relative) path to the source subfile.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        destination_path: Destination directory path (no trailing slash needed).
        timeout: Maximum number of seconds to wait for the command to complete.
        destination_filename: Destination filename only (no path component).
            Must be a real filename, not ``'.'``.
        bytes_to_write: Number of bytes to copy from the start of the source file.

    Returns:
        True if ``dd`` exited successfully, False otherwise.
    """
    logger.debug(f"{filename}- Copying first {bytes_to_write} bytes of file into {destination_path}")

    command = f"dd if={filename} of={destination_path}/{destination_filename} bs=4M oflag=direct iflag=count_bytes count={bytes_to_write}"

    start_time = time.time()
    retval, stdout = mwax_command.run_command_ext(command, numa_node, timeout, False)

    if retval:
        elapsed = time.time() - start_time
        speed = (bytes_to_write / elapsed) / (1000.0 * 1000.0 * 1000.0)

        logger.info(
            f"{filename}- Copying first {bytes_to_write} bytes of file into"
            f" {destination_path}/{destination_filename} was successful"
            f" (took {elapsed:.3f} secs at {speed:.3f} GB/sec)."
        )
    else:
        logger.error(
            f"{filename}- Copying first {bytes_to_write} bytes of file into"
            f" {destination_path}/{destination_filename} failed with error"
            f" {stdout}"
        )

    return retval


def running_under_pytest() -> bool:
    """
    Detect whether the current process is running under pytest.

    Checks for the presence of the ``PYTEST_CURRENT_TEST`` environment variable,
    which pytest sets automatically during test execution.

    Returns:
        True if running inside a pytest session, False otherwise.
    """
    # Returns True if we are running as part of pytest
    return "PYTEST_CURRENT_TEST" in os.environ
