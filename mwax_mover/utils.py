from mwax_mover import mwax_command
import base64
from configparser import ConfigParser
import glob
import os
import socket
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

    return split_hostname


def archive_file_xrootd(logger, full_filename: str, archive_numa_node, archive_destination_host: str):
    logger.debug(f"{full_filename} attempting archive_file_xrootd...")

    numa_cmd = ""

    # If provided, launch using specific numa node. Passing None ignores this part of the command line
    if archive_numa_node:
        numa_cmd = f"numactl --cpunodebind={archive_numa_node} --membind={archive_numa_node} "

    size = os.path.getsize(full_filename)

    # Build final command line
    command = numa_cmd + \
              f"/usr/local/bin/xrdcp --force --cksum adler32 --silent --streams 2 --tlsnodata " \
              f"{full_filename} xroot://{archive_destination_host}"
    start_time = time.time()
    return_value = mwax_command.run_shell_command(logger, command)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000*1000*1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(f"{full_filename} archive_file_xrootd success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

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
