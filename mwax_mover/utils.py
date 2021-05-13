from mwax_mover import mwax_command
import base64
from configparser import ConfigParser
import glob
import os
import socket
import time
import ceph


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


def load_psrdada_ringbuffer(logger, full_filename: str, ringbuffer_key: str, numa_node: int, timeout: int) -> bool:
    logger.info(f"{full_filename}- attempting load_psrdada_ringbuffer {ringbuffer_key}")

    numa_cmdline = f"numactl --cpunodebind={str(numa_node)} --membind={str(numa_node)} dada_diskdb -k {ringbuffer_key} -f {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value = mwax_command.run_command(logger, numa_cmdline, timeout)
    elapsed = time.time() - start_time

    size_gigabytes = size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    if return_value:
        logger.info(f"{full_filename} load_psrdada_ringbuffer success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

    return return_value


def archive_file_xrootd(logger, full_filename: str, archive_numa_node, archive_destination_host: str, timeout: int):
    logger.debug(f"{full_filename} attempting archive_file_xrootd...")

    # If provided, launch using specific numa node. Passing None ignores this part of the command line
    if archive_numa_node:
        numa_cmdline = f"numactl --cpunodebind={str(archive_numa_node)} --membind={str(archive_numa_node)} "
    else:
        numa_cmdline = ""

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(f"Error determining file size for {full_filename}. Error {e}")
        return False

    # Build final command line
    cmdline = f"{numa_cmdline}/usr/local/bin/xrdcp --force --cksum adler32 " \
              f"--silent --streams 2 --tlsnodata {full_filename} xroot://{archive_destination_host}"

    start_time = time.time()

    # run xrdcp
    if mwax_command.run_command(logger, cmdline, timeout):
        elapsed = time.time() - start_time

        size_gigabytes = file_size / (1000*1000*1000)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(f"{full_filename} archive_file_xrootd success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

        return True
    else:
        return False


def archive_file_ceph(logger, full_filename: str, ceph_endpoint: str):
    logger.debug(f"{full_filename} attempting archive_file_ceph...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(f"Error determining file size for {full_filename}. Error {e}")
        return False

    # determine bucket name
    try:
        bucket_name = ceph.ceph_get_bucket_name_from_filename(full_filename)
    except Exception as e:
        logger.error(f"Error determining bucket name for {full_filename}. Error {e}")
        return False

    # get s3 object
    try:
        s3_object = ceph.ceph_get_s3_object(ceph_endpoint)
    except Exception as e:
        logger.error(f"Error connecting to S3 endpoint {ceph_endpoint}. Error {e}")
        return False

    # create bucket if required
    try:
        ceph.ceph_create_bucket(s3_object, bucket_name)
    except Exception as e:
        logger.error(f"Error creating/checking existence of S3 bucket {bucket_name} on {ceph_endpoint}. Error {e}")
        return False

    # start timer
    start_time = time.time()

    # Do upload
    try:
        ceph.ceph_upload_file(s3_object, bucket_name, full_filename)
    except Exception as e:
        logger.error(f"Error uploading {full_filename} to S3 bucket {bucket_name} on {ceph_endpoint}. Error {e}")
        return False

    # end timer
    elapsed = time.time() - start_time

    size_gigabytes = file_size / (1000 * 1000 * 1000)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    logger.info(f"{full_filename} archive_file_ceph success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")
    return True


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


def validate_filename(filename: str) -> (bool, int, int, str, str):
    # Returns valid, obs_id, filetype_id, file_ext, validation_error
    valid: bool = True
    obs_id = 0
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

    # 2. Check extension
    if valid:
        if file_ext_part.lower() == ".sub":
            filetype_id = 17
        elif file_ext_part.lower() == ".fits":
            filetype_id = 18
        else:
            # Error - unknown filetype
            valid = False
            validation_error = f"Unknown file extension {file_ext_part}- ignoring"

    # 3. Check length of filename
    if valid:
        if filetype_id == 17:
            # filename format should be obsid_subobsid_XXX.sub
            # filename format should be obsid_subobsid_XX.sub
            # filename format should be obsid_subobsid_X.sub
            if len(file_name_part) < 23 or len(file_name_part) > 25:
                valid = False
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_subobsid_XXX.sub)- ignoring"
        elif filetype_id == 18:
            # filename format should be obsid_yyyymmddhhnnss_chXXX_XXX.fits
            if len(file_name_part) != 35:
                valid = False
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_yyyymmddhhnnss_chXXX_XXX.fits)- ignoring"

    # 4. check obs_id in the first 10 chars of the filename and is integer
    if valid:
        obs_id_check = file_name_part[0:10]

        if not obs_id_check.isdigit():
            valid = False
            validation_error = f"Filename does not start with a 10 digit observation_id- ignoring"
        else:
            obs_id = int(obs_id_check)

    return valid, obs_id, filetype_id, file_ext_part, validation_error
