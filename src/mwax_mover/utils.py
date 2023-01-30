"""Various utility functions used throughout the package"""
import base64
from configparser import ConfigParser
from enum import Enum
import fcntl
import glob
import os
import queue
import shutil
import socket
import struct
import threading
import time
import typing
import astropy.io.fits as fits
import requests
from mwax_mover import mwax_command
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData

# number of lines of the PSRDADA header to read looking for keywords
PSRDADA_HEADER_BYTES = 4096

# This is global mutex so we don't try to create the same metafits
# file with multiple threads
metafits_file_lock = threading.Lock()


class MWADataFileType(Enum):
    """Enum for the possible MWA data file types"""

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


def download_metafits_file(obs_id: int, metafits_path: str):
    """
    For a given obs_id, get the metafits file from webservices
    """
    metafits_file_path = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

    url = f"http://ws.mwatelescope.org/metadata/fits?obs_id={obs_id}"

    try:
        response = requests.get(url)
    except Exception as catch_all_exception:
        raise Exception(
            f"Unable to get metafits file. {catch_all_exception}"
        ) from catch_all_exception

    if response.status_code == 200:
        metafits = response.content
        with open(metafits_file_path, "wb") as handler:
            handler.write(metafits)
    else:
        raise Exception(
            "Unable to get metafits file. Response code"
            f" {response.status_code}"
        )

    return


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
            validation_error = (
                "Filename does not start with a 10 digit observation_id-"
                " ignoring"
            )
        else:
            obs_id = int(obs_id_check)

    # 3. Check extension
    if valid:
        if file_ext_part.lower() == ".sub":
            filetype_id = MWADataFileType.MWAX_VOLTAGES.value
        elif file_ext_part.lower() == ".fits":
            # Could be metafits (e.g. 1316906688_metafits_ppds.fits) or
            # visibilitlies
            if (
                file_name_part[10:] == "_metafits_ppds"
                or file_name_part[10:] == "_metafits"
            ):
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
            validation_error = (
                f"Unknown file extension {file_ext_part}- ignoring"
            )

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
            if (
                len(file_name_part) != 24
                and len(file_name_part) != 19
                and len(file_name_part) != 10
            ):
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

    # 5. Get project id and calibtator info
    if valid and filetype_id != MWADataFileType.MWA_PPD_FILE.value:
        # Now check that the observation is a calibrator by
        # looking at the associated metafits file
        metafits_filename = os.path.join(
            metafits_path, f"{obs_id}_metafits.fits"
        )

    elif valid and filetype_id == MWADataFileType.MWA_PPD_FILE.value:
        # this file IS a metafits! So check it
        metafits_filename = filename

    # Does the metafits file exist??
    # Obtain a lock so we can only do this inside one thread
    if valid:
        with metafits_file_lock:
            if not os.path.exists(metafits_filename):
                logger.info(
                    f"Metafits file {metafits_filename} not found. Atempting"
                    " to download it"
                )
                try:
                    download_metafits_file(obs_id, metafits_path)
                except Exception as catch_all_exception:  # pylint: disable=broad-except
                    valid = False
                    validation_error = (
                        f"Metafits file {metafits_filename} did not exist and"
                        " could not download one from web service."
                        f" {catch_all_exception}"
                    )

            if valid:
                (calibrator, project_id) = get_metafits_values(
                    metafits_filename
                )

    return ValidationData(
        valid,
        obs_id,
        project_id,
        filetype_id,
        file_ext_part,
        calibrator,
        validation_error,
    )


def determine_bucket_and_folder(full_filename, location):
    """Return the bucket and folder of the file to be archived,
    based on location."""
    filename = os.path.basename(full_filename)

    # acacia and banksia
    if location == 2 or location == 3:
        # determine bucket name
        bucket = get_bucket_name_from_filename(filename)
        folder = None
        return bucket, folder

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


def get_metafits_values(metafits_filename: str) -> (bool, str):
    """
    Returns a tuple of is_calibrator (bool) and
    the project_id (string) from a metafits file.
    """
    try:
        with fits.open(metafits_filename) as hdul:
            # Read key from primary HDU- it is bool
            is_calibrator = hdul[0].header[  # pylint: disable=no-member
                "CALIBRAT"
            ]
            project_id = hdul[0].header["PROJECT"]  # pylint: disable=no-member
            return is_calibrator, project_id
    except Exception as catch_all_exception:
        raise Exception(
            f"Error reading metafits file: {metafits_filename}:"
            f" {catch_all_exception}"
        ) from catch_all_exception


def read_config(
    logger, config: ConfigParser, section: str, key: str, b64encoded=False
):
    """Reads a value from a config file"""
    if b64encoded:
        value = base64.b64decode(config.get(section, key)).decode("utf-8")
        value_to_log = "*" * len(value)
    else:
        value = config.get(section, key)
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
        f"Read cfg [{section}].{key}: '{string_value}' converted to list of"
        f" {len(return_list)} items: {return_list}"
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

    cmd = (
        f"{mwax_stats_executable} -t {full_filename} -m"
        f" {metafits_filename} -o {stats_dump_dir}"
    )

    logger.info(f"{full_filename}- attempting to run stats: {cmd}")

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(
        logger, cmd, numa_node, timeout
    )
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} stats success in {elapsed} seconds")
    else:
        logger.error(f"{full_filename} stats failed with error {stdout}")

    return return_value


def load_psrdada_ringbuffer(
    logger, full_filename: str, ringbuffer_key: str, numa_node, timeout: int
) -> bool:
    """Loads a subfile into a PSRDADA ringbuffer"""
    logger.info(
        f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}"
    )

    cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(
        logger, cmd, numa_node, timeout
    )
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
        logger.error(
            f"{full_filename} load_psrdada_ringbuffer failed with error"
            f" {stdout}"
        )

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
    files = scan_directory(
        logger, watch_dir, pattern, recursive, exclude_pattern
    )
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
    files = scan_directory(
        logger, watch_dir, pattern, recursive, exclude_pattern
    )
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


def scan_directory(
    logger, watch_dir: str, pattern: str, recursive: bool, exclude_pattern
) -> list:
    """Scan a directory based on a pattern and adds the files into a list"""
    # Watch dir must end in a slash for the iglob to work
    # Just loop through all files and add them to the queue
    if recursive:
        find_pattern = os.path.join(
            os.path.abspath(watch_dir), "**/*" + pattern
        )
        logger.info(
            f"Scanning recursively for files matching {find_pattern}..."
        )
    else:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "*" + pattern)
        logger.info(f"Scanning for files matching {find_pattern}...")

    files = glob.glob(find_pattern, recursive=recursive)

    # Now exclude files if they match the exclude pattern
    if exclude_pattern:
        exclude_glob = os.path.join(
            os.path.abspath(watch_dir), "*" + exclude_pattern
        )
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


def do_checksum_md5(
    logger, full_filename: str, numa_node: int, timeout: int
) -> str:
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
    return_value, md5output = mwax_command.run_command_ext(
        logger, cmdline, numa_node, timeout, False
    )
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
                f" {checksum} ({size_megabytes:.3f}MB in {elapsed} secs at"
                f" {mb_per_sec:.3f} MB/s)"
            )
            return checksum
        else:
            raise Exception(
                f"Calculated MD5 checksum is not valid: md5 output {md5output}"
            )
    else:
        raise Exception(
            f"md5sum returned an unexpected return code {return_value}"
        )


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
    1 Calibrator correlator observations
    2 reserved for correlator high prioriry projects
    20 reserved for vcs high prioriry projects
    30 normal correlator observations
    90 vcs observations
    100 metafits_ppds
    """
    return_priority = 100  # default if we don't do anything else

    # get info about this file
    val: ValidationData = validate_filename(logger, filename, metafits_path)

    if val.valid:
        if val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            if val.calibrator:
                return_priority = 1
            else:
                if val.project_id in list_of_correlator_high_priority_projects:
                    return_priority = 2
                else:
                    return_priority = 30
        elif val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
            if val.project_id in list_of_vcs_high_priority_projects:
                return_priority = 20
            else:
                return_priority = 90
    else:
        raise Exception(
            f"File {filename} is not valid! Reason: {val.validation_message}"
        )

    return return_priority


def inject_beamformer_headers(subfile_filename: str, beamformer_settings: str):
    """Appends the beamformer settings to the existing subfile header"""
    data = []

    # Read the psrdada header data in to a list (one line per item)
    with open(subfile_filename, "rb") as subfile:
        data = subfile.read(PSRDADA_HEADER_BYTES).decode("UTF-8").split("\n")

    last_line_index = len(data) - 1
    last_row_len = len(data[last_line_index])

    beamformer_settings_len = len(beamformer_settings)
    null_trail = "\0" * (last_row_len - beamformer_settings_len)
    data[last_line_index] = beamformer_settings + null_trail

    # convert our list of lines back to a byte array
    new_string = "\n".join(data)

    new_bytes = bytes(new_string, "UTF-8")
    if len(new_bytes) != PSRDADA_HEADER_BYTES:
        raise Exception(
            "_inject_beamformer_headers(): new_bytes length is not"
            f" {PSRDADA_HEADER_BYTES} as expected it is {len(new_bytes)}."
            f" Newbytes = [{new_string}]"
        )

    # Overwrite the first 4096 bytes with our updated header
    with open(subfile_filename, "r+b") as subfile:
        subfile.seek(0)
        subfile.write(new_bytes)


def read_subfile_value(filename: str, key: str) -> str:
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


def write_mock_subfile(output_filename, obs_id, subobs_id, mode, obs_offset):
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
        "COARSE_CHANNEL 169\n"
        "CORR_COARSE_CHANNEL 12\n"
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
