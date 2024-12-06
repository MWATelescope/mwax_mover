"""Various utility functions used throughout the package"""

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
import shutil
import socket
import struct
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_not_exception_type
import threading
import time
import typing
from typing import Tuple, Optional
import astropy.io.fits as fits
import requests
import numpy as np
from mwax_mover import mwax_command
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData

# number of lines of the PSRDADA header to read looking for keywords
PSRDADA_HEADER_BYTES = 4096

# PSRDADA keywords
PSRDADA_MODE = "MODE"
PSRDADA_SUBOBS_ID = "SUBOBS_ID"
PSRDADA_TRIGGER_ID = "TRIGGER_ID"
PSRDADA_NINPUTS = "NINPUTS"
PSRDADA_COARSE_CHANNEL = "COARSE_CHANNEL"

# This is global mutex so we don't try to create the same metafits
# file with multiple threads
metafits_file_lock = threading.Lock()


class CorrelatorMode(Enum):
    """Class representing correlator mode"""

    NO_CAPTURE = "NO_CAPTURE"
    MWAX_CORRELATOR = "MWAX_CORRELATOR"
    MWAX_VCS = "MWAX_VCS"
    MWAX_BUFFER = "MWAX_BUFFER"

    @staticmethod
    def is_no_capture(mode_string: str) -> bool:
        """Returns true if mode is a no capture"""
        return mode_string in [
            CorrelatorMode.NO_CAPTURE.value,
        ]

    @staticmethod
    def is_correlator(mode_string: str) -> bool:
        """Returns true if mode is a correlator obs"""
        return mode_string in [
            CorrelatorMode.MWAX_CORRELATOR.value,
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


class MWADataFileType(Enum):
    """Enum for the possible MWA data file types"""

    HW_LFILES = 8
    MWA_FLAG_FILE = 10
    MWA_PPD_FILE = 14
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18


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


def download_metafits_file(logger: logging.Logger, obs_id: int, metafits_path: str):
    """
    For a given obs_id, get the metafits file from webservices
    """
    metafits_file_path = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    # Try the MRO one first
    urls = [
        f"http://mro.mwa128t.org/metadata/fits?obs_id={obs_id}",
        f"http://ws.mwatelescope.org/metadata/fits?obs_id={obs_id}",
    ]

    # On failure of all urls and retries it will raise an exception
    response = call_webservice(logger, obs_id, urls, None)

    metafits = response.content
    with open(metafits_file_path, "wb") as handler:
        handler.write(metafits)


def validate_filename(
    logger,
    filename: str,
    metafits_path: str,
) -> ValidationData:
    """
    Takes a filename and determines various
    things about it.
    Returns:
        (valid (bool) did validation succeed
        ,obs_id (int) obs_id of file
        ,project_id (int) project id of observation
        ,filetype_id (int) file type id
        ,file_ext (string) file extension
        ,calibrator (bool) True if it's a calibrator
        ,validation_error (string) validation error
        )
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
            validation_error = "Filename does not start with a 10 digit observation_id-" " ignoring"
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
            filetype_id = MWADataFileType.MWA_FLAG_FILE.value

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
                logger.info(f"Metafits file {metafits_filename} not found. Atempting" " to download it")
                try:
                    download_metafits_file(logger, obs_id, metafits_path)
                except Exception as catch_all_exception:  # pylint: disable=broad-except
                    valid = False
                    validation_error = (
                        f"Metafits file {metafits_filename} did not exist and"
                        " could not download one from web service."
                        f" {catch_all_exception}"
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
    """Return the bucket and folder of the file to be archived,
    based on location."""
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
    """Generates a bucket name for a filename"""
    file_part = os.path.split(filename)[1]
    return get_bucket_name_from_obs_id(int(file_part[0:10]))


def get_bucket_name_from_obs_id(obs_id: int) -> str:
    """Generate bucket name given an obs_id"""
    # return the first 5 digits of the obsid
    # This means there will be a new bucket every ~27 hours
    # This is to reduce the chances of vcs jobs filling a bucket to more than
    # 100K of files
    return f"mwaingest-{str(obs_id)[0:5]}"


def get_metafits_value(metafits_filename: str, key: str):
    """
    Returns a metafits value based on key in primary HDU
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from primary HDU
            return hdul[0].header[key]  # type: ignore # pylint: disable=no-member

    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}:" f" {catch_all_exception}"
        ) from catch_all_exception


def get_metafits_values(metafits_filename: str) -> Tuple[bool, str, str]:
    """
    Returns a tuple of
    is_calibrator (bool) and
    project_id (string) and
    calib_source (string) from a metafits file.
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from primary HDU- it is bool
            is_calibrator = hdul[0].header["CALIBRAT"]  # type: ignore # pylint: disable=no-member
            if is_calibrator:
                calib_source = hdul[0].header["CALIBSRC"]  # type: ignore # pylint: disable=no-member
            else:
                calib_source = ""
            project_id = hdul[0].header["PROJECT"]  # type: ignore # pylint: disable=no-member
            return is_calibrator, project_id, calib_source
    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}: {catch_all_exception}"
        ) from catch_all_exception


def read_config(logger, config: ConfigParser, section: str, key: str, b64encoded=False):
    """Reads a value from a config file"""
    raw_value = config.get(section, key)

    if b64encoded:
        value = base64.b64decode(raw_value).decode("utf-8")
        value_to_log = "*" * len(value)
    else:
        value = raw_value
        value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_optional_config(logger, config: ConfigParser, section: str, key: str, b64encoded=False) -> Optional[str]:
    """Reads an optional value from a config file as Optional[str]"""
    raw_value = config.get(section, key)
    value = None
    value_to_log = ""

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


def read_config_list(logger, config: ConfigParser, section: str, key: str):
    """Reads a string from a config file, returning a list"""
    string_value = read_config(logger, config, section, key, False)

    # Ensure we trim string_value
    string_value = string_value.rstrip().lstrip()

    if len(string_value) > 0:
        return_list = string_value.split(",")
    else:
        return_list = []

    logger.info(
        f"Read cfg [{section}].{key}: '{string_value}' converted to list of" f" {len(return_list)} items: {return_list}"
    )
    return return_list


def read_config_bool(logger, config: ConfigParser, section: str, key: str):
    """Read a bool from a config file"""
    value = config.getboolean(section, key)

    logger.info(f"Read cfg [{section}].{key} == {value}")
    return value


def get_hostname() -> str:
    """Return the hostname of the running machine"""
    hostname = socket.gethostname()

    # ensure we remove anything after a . in case we got the fqdn
    split_hostname = hostname.split(".")[0]

    return split_hostname.lower()


def process_mwax_stats(
    logger,
    mwax_stats_executable: str,
    full_filename: str,
    numa_node: int,
    timeout: int,
    stats_dump_dir: str,
    metafits_path: str,
) -> bool:
    """Runs mwax_stats"""
    # This code will execute the mwax stats command
    obs_id = str(os.path.basename(full_filename)[0:10])

    metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    cmd = f"{mwax_stats_executable} -t {full_filename} -m" f" {metafits_filename} -o {stats_dump_dir}"

    logger.info(f"{full_filename}- attempting to run stats: {cmd}")

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(logger, cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} stats success in {elapsed:.3f} seconds")
    else:
        logger.error(f"{full_filename} stats failed with error {stdout}")

    return return_value


def load_psrdada_ringbuffer(logger, full_filename: str, ringbuffer_key: str, numa_node, timeout: int) -> bool:
    """Loads a subfile into a PSRDADA ringbuffer"""
    logger.info(f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}")

    cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(logger, cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(
            f"{full_filename} load_psrdada_ringbuffer success"
            f" ({size_gigabytes:.3f}GB in {elapsed} sec at"
            f" {gbps_per_sec:.3f} Gbps)"
        )
    else:
        logger.error(f"{full_filename} load_psrdada_ringbuffer failed with error" f" {stdout}")

    return return_value


def scan_for_existing_files_and_add_to_queue(
    logger,
    watch_dir: str,
    pattern: str,
    recursive: bool,
    queue_target: queue.Queue,
    exclude_pattern=None,
):
    """
    Scans a directory for a file pattern and then enqueues all items into
    a regular queue
    """
    files = scan_directory(logger, watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"Found {len(files)} files")

    for filename in files:
        queue_target.put(filename)
        logger.info(f"{filename} added to queue")


def scan_for_existing_files_and_add_to_priority_queue(
    logger,
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
    Scans a directory for a file pattern and then enqueues all items into
    a priority queue
    """
    files = scan_directory(logger, watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"Found {len(files)} files")

    for filename in files:
        priority = get_priority(
            logger,
            filename,
            metafits_path,
            list_of_correlator_high_priority_projects,
            list_of_vcs_high_priority_projects,
        )
        queue_target.put((priority, MWAXPriorityQueueData(filename)))
        logger.info(f"{filename} added to queue with priority {priority}")


def scan_directory(logger, watch_dir: str, pattern: str, recursive: bool, exclude_pattern) -> list:
    """Scan a directory based on a pattern and adds the files into a list"""
    # Watch dir must end in a slash for the iglob to work
    # Just loop through all files and add them to the queue
    if recursive:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "**/*" + pattern)
        logger.info(f"Scanning recursively for files matching {find_pattern}...")
    else:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "*" + pattern)
        logger.info(f"Scanning for files matching {find_pattern}...")

    files = glob.glob(find_pattern, recursive=recursive)

    # Now exclude files if they match the exclude pattern
    if exclude_pattern:
        exclude_glob = os.path.join(os.path.abspath(watch_dir), "*" + exclude_pattern)
        logger.info(f"Excluding files {exclude_glob}...")
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
    """Send data to an IP and port via multicast"""

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
    """Gets an IP address from an interface name"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            sock.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack("256s", bytes(ifname[:15], "utf-8")),
        )[20:24]
    )


def get_primary_ip_address() -> str:
    """Get primary IP of this host"""
    return socket.gethostbyname(socket.getfqdn())


def get_disk_space_bytes(path: str) -> typing.Tuple[int, int, int]:
    """Gets the total, used and free bytes from a path"""
    # Get disk space: total, used and free
    return shutil.disk_usage(path)


def do_checksum_md5(logger, full_filename: str, numa_node: Optional[int], timeout: int) -> str:
    """Return an md5 checksum of a file"""

    # default output of md5 hash command is:
    # "5ce49e5ebd72c41a1d70802340613757
    # /visdata/incoming/1320133480_20211105074422_ch055_000.fits"
    md5output = ""
    checksum = ""

    logger.info(f"{full_filename}- running md5sum...")

    cmdline = f"md5sum {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, md5output = mwax_command.run_command_ext(logger, cmdline, numa_node, timeout, False)
    elapsed = time.time() - start_time

    size_megabytes = size / (1000 * 1000)
    mb_per_sec = size_megabytes / elapsed

    if return_value:
        # the return value will contain a few spaces and then the filename
        # So remove the filename and then remove any whitespace
        checksum = md5output.replace(full_filename, "").rstrip()

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
    logger,
    filename: str,
    metafits_path: str,
    list_of_correlator_high_priority_projects: list,
    list_of_vcs_high_priority_projects: list,
) -> int:
    """
    metafits_path is the directory where all the current
    metafits files exist.

    Determines the priority to assign this file.
    The returned integer has is used by a PriorityQueue
    to determine priority. The lowest priority number
    in the queue gets returned first when q.get() is
    called.

    Priority order is:
    1 metafits_ppds (small and can be quickly archived)
    2 Calibrator correlator observations
    3 reserved for correlator high prioriry projects
    20 reserved for vcs high prioriry projects
    30 normal correlator observations
    90 vcs observations
    """
    return_priority = 100  # default if we don't do anything else

    # get info about this file
    val: ValidationData = validate_filename(logger, filename, metafits_path)

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
    else:
        raise Exception(f"File {filename} is not valid! Reason: {val.validation_message}")

    return return_priority


def inject_subfile_header(subfile_filename: str, key_value_pairs: str):
    """Appends the key_value_pair setting to the existing subfile header
    Multiple key value pairs should have a \n to split each setting.
    Each key_value_pair should be space separated: 'KEY VALUE' and
    end with a \n newline"""
    data = []

    # Read the psrdada header data in to a list (one line per item)
    with open(subfile_filename, "rb") as subfile:
        data = subfile.read(PSRDADA_HEADER_BYTES).decode("UTF-8").split("\n")

    last_line_index = len(data) - 1
    last_row_len = len(data[last_line_index])

    new_settings_len = len(key_value_pairs)
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
    """Appends the beamformer settings to the existing subfile header"""
    inject_subfile_header(subfile_filename, beamformer_settings)


def read_subfile_value(filename: str, key: str) -> typing.Union[str, None]:
    """Returns key as a string from a subfile header"""
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


def read_subfile_trigger_value(subfile_filename: str):
    """Reads the TRIGGER_ID values from a subfile header
    Returns trigger_id as an INT or None if not found"""
    value = read_subfile_value(subfile_filename, PSRDADA_TRIGGER_ID)

    if value:
        return int(value)
    else:
        return None


def write_mock_subfile_from_header(output_filename, header):
    """This is a utility so that tests
    can write subfiles! It will append some dummy data after the header and
    ensure the header is 4096 bytes padded by nuls"""

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
    """This is a utility so that tests
    can write subfiles! It will append some dummy data after the header and
    ensure the header is 4096 bytes padded by nuls"""
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
    """If the project code is C123 then do not archive the data!
    If we ever get more codes or they change more often this should
    move into the config file"""
    if project_id.upper() == "C123":
        return False
    else:
        return True


@retry(stop=stop_after_attempt(10), wait=wait_fixed(30), retry=retry_if_not_exception_type((ValueError)))
def call_webservice(logger: logging.Logger, obs_id: int, url_list: list[str], data) -> requests.Response:
    """Call each url in the list until: a response of
    200 (in which case return the response)
    """
    i = 0

    while i < len(url_list):
        url = url_list[i]

        logger.debug(f"{obs_id}: call_webservice() trying with {url} with data ({'' if data is None else data})")

        try:
            response = requests.get(url, data, timeout=30)

            if response.status_code == 200:
                logger.debug(f"{obs_id}: call_webservice() returned 200 (success)")
                return response

            elif response.status_code >= 400 and response.status_code < 500:
                error_message = (
                    f"{obs_id}: call_webservice() returned {response.status_code} " f"{response.text} (failure)"
                )
                logger.error(error_message)
                raise ValueError(error_message)

            else:
                # Non-200 status code- try next url
                logger.warning(
                    f"{obs_id}: call_webservice() returned {response.status_code} " f"{response.text} (failure)"
                )
        except ValueError:
            raise

        except Exception as e:
            # Exception raised- try next url
            logger.exception(f"{obs_id}: call_webservice() exception", e)

        # try next url
        i += 1

    # We tried all urls without success- up to tenacity to retry X times now
    logger.error(f"{obs_id}: call_webservice() - failed after trying {len(url_list)} urls.")
    raise Exception(f"{obs_id}: call_webservice()- failed after trying {len(url_list)} urls.")


def get_data_files_for_obsid_from_webservice(
    logger,
    obs_id: int,
) -> list[str]:
    """Calls an MWA webservice, passing in an obsid and returning a list of filenames
    of all the data_files (MWAX_VISIBILITIES or HW_LFILES) of the given filetype or None if there was an error.
    metadata_webservice_url is the base url - e.g. http://ws.mwatelescope.org
    - all_files: True means to get all files whether they are archived at Pawsey or not"""
    urls = ["http://mro.mwa128t.org/metadata/data_files", "http://mro.mwa128t.org/metadata/data_files"]
    data = {"obs_id": obs_id, "terse": False, "all_files": True}

    # On failure of all urls and retries it will raise an exception
    result = call_webservice(logger, obs_id, urls, data)

    files = json.loads(result.text)
    file_list = [
        file
        for file in files
        if files[file]["filetype"] == MWADataFileType.MWAX_VISIBILITIES.value
        or files[file]["filetype"] == MWADataFileType.HW_LFILES.value
    ]
    file_list.sort()
    return file_list


@retry(stop=stop_after_attempt(5), wait=wait_fixed(10))
def remove_file(logger, filename: str, raise_error: bool) -> bool:
    """Deletes a file from the filesystem"""
    try:
        os.remove(filename)
        logger.info(f"{filename}- file deleted")
        return True

    except Exception as delete_exception:  # pylint: disable=broad-except
        if raise_error:
            logger.error(f"{filename}- Error deleting: {delete_exception}. Retrying up" " to 5 times.")
            raise delete_exception
        else:
            logger.warning(f"{filename}- Error deleting: {delete_exception}. File may" " have been moved or removed.")
            return True


# For a given datetime, return the GPS seconds as an integer
def get_gpstime_of_datetime(date_time: datetime.datetime) -> int:
    utc_datetime: astrotime.Time = astrotime.Time(date_time, scale="utc")
    current_gpstime = utc_datetime.gps
    return int(current_gpstime)  # type: ignore


# Return the GPS seconds as an integer of Now
def get_gpstime_of_now() -> int:
    return get_gpstime_of_datetime(datetime.datetime.now(datetime.timezone.utc))


# Given a subfile filename, retrieve the packet map data (raw)
def get_subfile_packet_map_data(logger: logging.Logger, filename: str) -> Optional[bytes]:
    KEY_IDX_PACKET_MAP = "IDX_PACKET_MAP"

    base_filename = os.path.basename(filename)
    obs_id: int = int(base_filename[0:10])

    # Get the keys from the subfile header:
    # (info from:
    # https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/24970579/MWAX+PSRDADA+header#Block-0%3A-UDP-Packet-Map)
    # IDX_PACKET_MAP
    # Value read will be in the form of: XXX+YYY
    # Where:
    # XXX is the number of bytes into block 0 to start
    # and YYY is the number of bytes to read
    idx_packet_map_string: Optional[str] = read_subfile_value(filename, KEY_IDX_PACKET_MAP)

    if idx_packet_map_string is None:
        logger.warning(
            f"{obs_id}: {base_filename} does not have the {KEY_IDX_PACKET_MAP} keyword. "
            f"Ignoring packet stats for this subobservation."
        )
        return None

    # Now split the string by the "+" sign
    split_idx = idx_packet_map_string.split("+")

    if len(split_idx) != 2:
        logger.warning(
            f"{obs_id}: {base_filename} {KEY_IDX_PACKET_MAP} keyword value {idx_packet_map_string} did not "
            f"split by the + sign correctly. Ignoring packet stats for this subobservation."
        )
        return None

    # Ensure the two parts can be cast to int
    packet_map_start: int = 0
    packet_map_length: int = 0

    try:
        packet_map_start = int(split_idx[0])
        packet_map_length = int(split_idx[1])
    except ValueError:
        logger.warning(
            f"{obs_id}: {base_filename} {KEY_IDX_PACKET_MAP} keyword value {idx_packet_map_string} "
            f"components were not able to be cast to int. Ignoring packet stats for this subobservation."
        )
        return None

    # Now get the data
    block0_start = PSRDADA_HEADER_BYTES
    block0_packet_map_start = block0_start + packet_map_start
    return_data: bytes

    try:
        with open(filename, mode="rb") as subfile:
            subfile.seek(block0_packet_map_start)
            return_data = subfile.read(packet_map_length)

            # Ensure we read the right number of bytes!
            if len(return_data) != packet_map_length:
                raise Exception(
                    f"{obs_id}: {base_filename} read of packet map data failed- "
                    f"read {len(return_data)} bytes (should have been {packet_map_length} bytes)"
                )
    except FileNotFoundError:
        logger.exception(f"{obs_id}: {filename} not found when reading packet map.")
        return None
    except Exception:
        raise

    return return_data


# Take an array of bytes representing a bitmap, and return
# a new array where each element in the array is a 1 or 0 for each bit
# ie: input:  01111011
#     output: [0,1,1,1,1,0,1,1]
def convert_occupany_bitmap_to_array(bitmap: np.ndarray) -> np.ndarray:
    new_array = np.unpackbits(bitmap)

    return new_array


# Gets the packet map and gets the mean occupancy of each rfinput
# for a subobservation and returns
# an array of ints of since N, where N is number of rf_inputs
# and each int is a count of lost packets
#
def summarise_packet_map(num_rf_inputs: int, packet_map_bytes: bytes) -> np.ndarray:
    # Determine number of packets. There might be 625 (critically sampled)
    # or 800 (oversampled)
    num_packets_per_sec: int = int(len(packet_map_bytes) / num_rf_inputs)
    assert num_packets_per_sec == 625 or num_packets_per_sec == 800

    # 8 Seconds per subobs
    num_seconds_per_subobs: int = 8
    num_packets_per_subobs: int = num_packets_per_sec * num_seconds_per_subobs
    num_packet_map_bytes_per_subobs: int = int(num_packets_per_subobs / 8)

    # Example Structure of packet_map is:
    #                   Time (packet number)
    #              |<----------- 8 seconds ------------>|
    #              00000000 00000000 00000000 ...66666666
    #              00000000 00111111 11112222 ...11222222
    #              01234567 89012345 67890123 ...89012345
    #              ========================== ...========
    # rf_input 0   11111111 11111111 11111111    10110111
    # rf_input 1   01011111 00111111 11111111    11111011
    # ...
    # rf_input N   01111111 00111111 11111111    11111011

    # We want to output a int which represents the count of
    # packets lost per rf_input for those 8 seconds.

    # Define output array
    packets_lost = np.empty(shape=(num_rf_inputs), dtype=np.int16)

    # Convert the input bytes into a numpy array of bytes
    packet_map_np = np.frombuffer(packet_map_bytes, dtype=np.uint8)

    # Reshape the packet map to 2d (rfinputs, packets)
    packet_map_np = np.reshape(packet_map_np, (num_rf_inputs, num_packet_map_bytes_per_subobs))

    for rf_input_index in range(0, num_rf_inputs):
        bit_array = convert_occupany_bitmap_to_array(packet_map_np[rf_input_index])
        # we subtract total packets from the bnit array as we want lost packets (0's)
        packets_lost[rf_input_index] = num_packets_per_subobs - bit_array.sum()

    return packets_lost


def write_packet_stats(
    subobs_id: int,
    receiver_channel: int,
    hostname: str,
    num_tiles: int,
    packet_stats_dump_dir: str,
    packets_lost_array: np.ndarray,
):
    # Filename format is: packetstats_SSSSSSSSSS_tttT_chNNN_mwaxHH.dat
    # Where:
    # SSSSSSSSSS = subobsid
    # ttt=number of tiles (this helps you read the data as you know TTTx2 is the number of uint16 values to read)
    # N|NN|NNN=receiver channel number
    # mwaxHH=the host that generated it
    filename = f"packetstats_{subobs_id}_{num_tiles}T_ch{receiver_channel}_{hostname}.dat"

    # Assemble full filename
    full_filename = os.path.join(packet_stats_dump_dir, filename)

    # Write the data
    # Having sep & format as "" it should write the data as binary in "C" order
    packets_lost_array.tofile(full_filename, sep="", format="")
