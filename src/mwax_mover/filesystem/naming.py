"""Filename/bucket-name parsing, validation, and MWA data-file classification.

Key enums/classes: MWADataFileType, ArchiveLocation, ValidationData (the
validate_filename() result). validate_filename() is the central check: it
classifies a filename, cross-references its metafits file (downloading one
via fits.metafits if needed), and reports the project ID and calibrator
status. get_bucket_name_for_location()/get_bucket_name_from_* derive archive bucket
names; get_priority() ranks files for archiving order; get_data_files_*
query which data files exist for an obs_id via the MWA web service -- kept
here rather than in net.webservice because they depend on MWADataFileType,
which would otherwise create an import cycle (naming -> fits.metafits ->
net.webservice -> naming).
"""

import json
import logging
import os
import re
import threading
from enum import Enum, IntEnum

import requests

from mwax_mover.constants import MWA_WEBSERVICE_HOSTS
from mwax_mover.fits.metafits import download_metafits_file, get_calibrator_info
from mwax_mover.net.webservice import call_webservice

logger = logging.getLogger(__name__)

# This is global mutex so we don't try to create the same metafits
# file with multiple threads
metafits_file_lock = threading.Lock()


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
    """Where a data file is (or should be) archived at Pawsey.

    The integer values are the ones stored in the MWA metadata database's
    data_files.remote_archived location column, so they must not be renumbered.
    DMF and Versity are defined for historical/database completeness but are not
    implemented by get_bucket_name_for_location().
    """

    Unknown = 0
    DMF = 1
    AcaciaIngest = 2
    Banksia = 3
    AcaciaMWA = 4


class ArchivePriority(IntEnum):
    """Archive queue priority. Lower dequeues first."""

    METAFITS_OR_PPD = 1
    CALIBRATOR_CORRELATOR = 2
    HIGH_PRIORITY_CORRELATOR = 3
    HIGH_PRIORITY_VCS_BEAMFORMED = 5
    NORMAL_VCS_BEAMFORMED = 10
    HIGH_PRIORITY_VCS_VOLTAGE = 20
    NORMAL_CORRELATOR = 30
    NORMAL_VCS_VOLTAGE = 90
    DEFAULT = 100


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
    # NOTE: this used to test `len(os.path.splitext(filename)) == 2`, which is
    # always true (splitext always returns a 2-tuple), so the "no extension"
    # branch was unreachable and such filenames fell through to be reported as
    # "Unknown file extension " by step 3 instead. Test the extension itself.
    file_name_part, file_ext_part = os.path.splitext(filename)
    file_name_part = os.path.basename(file_name_part)
    if not file_ext_part:
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
            # visibilities
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
                logger.info(f"Metafits file {metafits_filename} not found. Attempting to download it")
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
                calibrator, project_id, calib_source = get_calibrator_info(metafits_filename)

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


def get_bucket_name_for_location(full_filename: str, location: ArchiveLocation) -> str:
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


def get_priority(
    filename: str,
    metafits_path: str,
    high_priority_correlator_projects: list,
    high_priority_vcs_projects: list,
) -> int:
    """
    Determine the archive priority integer for a given MWA data file.

    A lower integer means higher priority (i.e. the file will be dequeued
    first from a ``PriorityQueue``). See ``ArchivePriority`` for the full
    priority scheme.

    Args:
        filename: Full path to the MWA data file.
        metafits_path: Directory containing metafits files, used by
            ``validate_filename`` to resolve project ID and calibrator status.
        high_priority_correlator_projects: Project IDs that receive
            elevated priority (level 3) for correlator observations.
        high_priority_vcs_projects: Project IDs that receive elevated
            priority (levels 5 and 20) for VCS / beamformer observations.

    Returns:
        An integer priority value. Lower values are higher priority.

    Raises:
        Exception: If ``validate_filename`` reports the file as invalid.
    """
    return_priority = ArchivePriority.DEFAULT  # default if we don't do anything else

    # get info about this file
    val: ValidationData = validate_filename(filename, metafits_path)

    if val.valid:
        if val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            if val.calibrator:
                return_priority = ArchivePriority.CALIBRATOR_CORRELATOR
            else:
                if val.project_id in high_priority_correlator_projects:
                    return_priority = ArchivePriority.HIGH_PRIORITY_CORRELATOR
                else:
                    return_priority = ArchivePriority.NORMAL_CORRELATOR
        elif val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
            if val.project_id in high_priority_vcs_projects:
                return_priority = ArchivePriority.HIGH_PRIORITY_VCS_VOLTAGE
            else:
                return_priority = ArchivePriority.NORMAL_VCS_VOLTAGE
        elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            return_priority = ArchivePriority.METAFITS_OR_PPD
        elif val.filetype_id == MWADataFileType.VDIF.value or val.filetype_id == MWADataFileType.FILTERBANK.value:
            # VDIF and filterbank files are treated as high priority as they are small and quick to archive
            if val.project_id in high_priority_vcs_projects:
                return_priority = ArchivePriority.HIGH_PRIORITY_VCS_BEAMFORMED
            else:
                return_priority = ArchivePriority.NORMAL_VCS_BEAMFORMED
    else:
        raise Exception(f"File {filename} is not valid! Reason: {val.validation_message}")

    return return_priority


def should_project_be_archived(project_id: str, do_not_archive_projectids: list[str]) -> bool:
    """
    Determine whether data for a given project ID should be archived.

    Args:
        project_id: The MWA project ID string (case-insensitive).
        do_not_archive_projectids: Project IDs whose data should not be
            archived (case-insensitive). ``C123`` is a test/commissioning
            project and is the default when a config has not been updated
            with this key (see docs/CLEANUP.md 4.3).

    Returns:
        False if ``project_id`` is in ``do_not_archive_projectids``
        (case-insensitive), True otherwise.
    """
    return project_id.upper() not in {p.upper() for p in do_not_archive_projectids}


def extract_channels_from_filename(filename: str) -> dict | None:
    """Extracts channel information from a filename.

    Handles two patterns:
    - Single channel: e.g. "1445856416_ch96_solutions_phases.png"
    - Channel range:  e.g. "1424383568_ch110-121_solutions_amps.png"

    Args:
        filename: The filename (not full path) to extract channel info from.

    Returns:
        A dict with a "start" key and optional "end" key if a channel pattern
        is found, or None if no channel pattern is present.
    """
    match = re.search(r"_ch(\d{1,3})(?:-(\d{1,3}))?_", filename)
    if not match:
        return None
    result = {"start": int(match.group(1))}
    if match.group(2) is not None:
        result["end"] = int(match.group(2))
    return result


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
    urls = [f"{host}/metadata/data_files" for host in MWA_WEBSERVICE_HOSTS]
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
    urls = [f"{host}/metadata/data_files" for host in MWA_WEBSERVICE_HOSTS]
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
