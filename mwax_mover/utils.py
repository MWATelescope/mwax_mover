from mwax_mover import mwax_command
import base64
from configparser import ConfigParser
import fcntl
import glob
import os
import shutil
import socket
import struct
import time

def read_config(logger, config: ConfigParser, section: str, key: str, b64encoded=False):
    if b64encoded:
        value = base64.b64decode(config.get(section, key)).decode('utf-8')
        value_to_log = '*' * len(value)
    else:
        value = config.get(section, key)
        value_to_log = value

    logger.info(f"Read cfg [{section}].{key} == {value_to_log}")
    return value


def read_config_bool(logger, config: ConfigParser, section: str, key: str):
    value = config.getboolean(section, key)

    logger.info(f"Read cfg [{section}].{key} == {value}")
    return value


def get_hostname() -> str:
    hostname = socket.gethostname()

    # ensure we remove anything after a . in case we got the fqdn
    split_hostname = hostname.split(".")[0]

    return split_hostname.lower()

def process_mwax_stats(logger, mwax_stats_executable: str, full_filename: str, numa_node: int, timeout: int) -> bool:
    # This code will execute the mwax stats command
    obs_id = str(os.path.basename(full_filename)[0:10])

    cmd = f"{mwax_stats_executable} {full_filename} -m /vulcan/metafits/{obs_id}_metafits.fits"

    logger.info(f"{full_filename}- attempting to run stats: {cmd}")

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(logger, cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    if return_value:
        logger.info(f"{full_filename} stats success in {elapsed} seconds")

    return return_value


def load_psrdada_ringbuffer(logger, full_filename: str, ringbuffer_key: str, numa_node: int, timeout: int) -> bool:
    logger.info(f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}")

    cmd = f"dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, stdout = mwax_command.run_command_ext(logger, cmd, numa_node, timeout)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(f"{full_filename} load_psrdada_ringbuffer success ({size_gigabytes:.3f}GB "
                    f"at {gbps_per_sec:.3f} Gbps)")

    return return_value


def scan_for_existing_files(logger, watch_dir: str, pattern: str, recursive: bool, q):
    files = scan_directory(logger, watch_dir, pattern, recursive)
    files = sorted(files)
    logger.info(f"Found {len(files)} files")

    for file in files:
        q.put(file)
        logger.info(f'{file} added to queue')


def scan_directory(logger, watch_dir: str, pattern: str, recursive: bool) -> list:
    # Watch dir must end in a slash for the iglob to work
    # Just loop through all files and add them to the queue
    if recursive:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "**/*" + pattern)
        logger.info(f"Scanning recursively for files matching {find_pattern}...")
    else:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "*" + pattern)
        logger.info(f"Scanning for files matching {find_pattern}...")

    files = glob.glob(find_pattern, recursive=recursive)
    return files


def send_multicast(multicast_interface_ip: str, dest_multicast_ip: str, dest_multicast_port: int, message: bytes, ttl_hops:int):
    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # Disable loopback so you do not receive your own datagrams.
    #loopback = 0
    #if sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, loopback) != 0:
    #    raise Exception("Error setsockopt IP_MULTICAST_LOOP failed")

    # Set the time-to-live for messages.
    hops = struct.pack('b', ttl_hops)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, hops)

    # Set local interface for outbound multicast datagrams.
    # The IP address specified must be associated with a local,
    # multicast - capable interface.
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(multicast_interface_ip))

    try:
        # Send data to the multicast group
        if sock.sendto(message, (dest_multicast_ip, dest_multicast_port)) == 0:
            raise Exception("Error sock.sendto() sent 0 bytes")

    except Exception as e:
        raise e
    finally:
        sock.close()

def get_ip_address(ifname: str) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
    )[20:24])

def get_primary_ip_address() -> str:
    return socket.gethostbyname(socket.getfqdn())

def get_disk_space_bytes(path: str) -> (int, int, int):
    # Get disk space: total, used and free
    return shutil.disk_usage(path)

def do_checksum_md5(logger, full_filename: str, numa_node: int, timeout: int) -> str:
    checksum = ""

    logger.info(f"{full_filename}- running md5sum...")

    cmdline = f"md5sum {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, checksum = mwax_command.run_command_ext(logger, cmdline, numa_node, timeout, False)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(f"{full_filename} md5sum success {checksum} ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

    return checksum