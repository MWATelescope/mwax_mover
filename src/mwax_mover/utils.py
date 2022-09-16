"""Various utility functions used throughout the package"""
import base64
from configparser import ConfigParser
import fcntl
import glob
import os
import queue
import shutil
import socket
import struct
import time
import typing
import astropy.io.fits as fits
from mwax_mover import mwax_command, mwa_archiver
from mwax_mover.mwa_archiver import ValidationData


def get_metafits_values(metafits_filename: str) -> (bool, str):
    """
    Returns a tuple of is_calibrator (bool) and
    the project_id (string) from a metafits file.
    """
    with fits.open(metafits_filename) as hdul:
        # Read key from primary HDU- it is bool
        is_calibrator = hdul[0].header["CALIBRAT"]  # pylint: disable=no-member
        project_id = hdul[0].header["PROJECT"]  # pylint: disable=no-member
        return is_calibrator, project_id


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
        priority = get_priority(filename, metafits_path)
        queue_target.put((priority, filename))
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


def get_priority(filename: str, metafits_path: str) -> int:
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
    10 metafits PPD files
    20 reserved for vcs high prioriry projects
    30 normal correlator observations
    99 vcs observations
    100 ???
    """
    return_priority = 100  # default if we don't do anything else

    list_of_correlator_high_priority_projects = [
        "D0006",
    ]

    list_of_vcs_high_priority_projects = [
        "D0006",
    ]

    # get info about this file
    val: ValidationData = mwa_archiver.validate_filename(
        filename, metafits_path
    )

    if val.valid:
        if (
            val.filetype_id
            == mwa_archiver.MWADataFileType.MWAX_VISIBILITIES.value
        ):
            if val.calibrator:
                return_priority = 1
            else:
                if val.project_id in list_of_correlator_high_priority_projects:
                    return_priority = 2
                else:
                    return_priority = 30
        elif (
            val.filetype_id == mwa_archiver.MWADataFileType.MWA_PPD_FILE.value
        ):
            return_priority = 10
        elif (
            val.filetype_id == mwa_archiver.MWADataFileType.MWAX_VOLTAGES.value
        ):
            if val.project_id in list_of_vcs_high_priority_projects:
                return_priority = 20
            else:
                return_priority = 99

    return return_priority
