"""Contains various methods dealing with archiving"""

import os
import hashlib
import random
import time
import uuid
import logging

from mwax_mover.mwax_command import run_command_ext


def copy_file_rsync(
    logger: logging.Logger,
    source: str,
    destination: str,
    timeout: int,
):
    """Copies a file via rsync"""
    logger.debug(f"{source} attempting copy_file_rsync...")

    # get file size if file is not remote!
    # a remote source will include user@host:path
    if ":/" not in source:
        try:
            file_size = os.path.getsize(source)
        except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
            logger.error(f"{source}: Error determining source file size. Error {catch_all_exceptiion}")
            return False
    else:
        file_size = -1

    # Build final command line
    # --no-compress ensures we don't try to compress (it's going to be quite
    # uncompressible)
    cmdline = (
        "rsync --no-compress -e 'ssh -T -c aes128-ctr -o"
        " StrictHostKeyChecking=no -o Compression=no -x' "
        f"{source} {destination}"
    )

    start_time = time.time()

    # run rsync
    return_val, stdout = run_command_ext(logger, cmdline, None, timeout, False)

    if return_val:
        # if source was remote, then we can now check the file size
        if file_size == -1:
            try:
                file_size = os.path.getsize(destination)
            except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
                logger.error(f"{source}: Error determining destination file size. Error {catch_all_exceptiion}")
                return False

        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(
            f"{source} copy_file_rsync success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
            f" {gbps_per_sec:.3f} Gbps)"
        )
        return True
    else:
        logger.error(f"{source} copy_file_rsync failed. Error {stdout}")
        return False


def archive_file_xrootd(
    logger: logging.Logger,
    full_filename: str,
    archive_numa_node: int,
    archive_destination_host: str,
    timeout: int,
):
    """Archive a file via xrootd"""
    logger.debug(f"{full_filename} attempting archive_file_xrootd...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
        logger.error(f"{full_filename}: Error determining file size. Error {catch_all_exceptiion}")
        return False

    # Gather some info for later
    filename = os.path.basename(full_filename)
    temp_filename = f"{filename}.part{uuid.uuid4()}"
    # Archive destination host looks like: "192.168.120.110://volume2/incoming"
    # so just get the bit before the ":" for the host and the bit after for
    # the path
    destination_host = archive_destination_host.split(":")[0]
    destination_path = archive_destination_host.split(":")[1]
    full_destination_temp_filename = os.path.join(destination_path, temp_filename)
    full_destination_final_filename = os.path.join(destination_path, filename)

    # Build final command line
    #
    # --posc         = persist on successful copy. If copy fails either remove
    #                  the file or set it to 0 bytes. Setting to 0 bytes is
    #                  weird, but I'll take it
    # --rm-bad-cksum = Delete dest file if checksums do not match
    #
    cmdline = (
        "/usr/local/bin/xrdcp --cksum adler32 --posc --rm-bad-cksum --silent"
        " --streams 2 --tlsnodata"
        f" {full_filename} xroot://{archive_destination_host}/{temp_filename}"
    )

    start_time = time.time()

    # run xrdcp
    return_val, stdout = run_command_ext(logger, cmdline, archive_numa_node, timeout, False)

    if return_val:
        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(
            f"{full_filename} archive_file_xrootd success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
            f" {gbps_per_sec:.3f} Gbps)"
        )

        cmdline = (
            f"ssh -o StrictHostKeyChecking=no mwa@{destination_host} 'mv"
            f" {full_destination_temp_filename}"
            f" {full_destination_final_filename}'"
        )

        # run the mv command to rename the temp file to the final file
        # If this works, then mwacache will actually do its thing
        return_val, stdout = run_command_ext(logger, cmdline, archive_numa_node, timeout, False)

        if return_val:
            logger.info(
                f"{full_filename} archive_file_xrootd successfully renamed"
                f" {full_destination_temp_filename} to"
                f" {full_destination_final_filename} on the remote host"
                f" {destination_host}"
            )
            return True
        else:
            logger.error(f"{full_filename} archive_file_xrootd rename failed. Error {stdout}")
            return False
    else:
        logger.error(f"{full_filename} archive_file_xrootd failed. Error {stdout}")
        return False


def archive_file_rclone(
    logger,
    rclone_profile: str,
    endpoints: list,
    full_filename: str,
    bucket_name: str,
    md5hash: str,
):
    """Archive file via rclone"""
    logger.debug(f"{full_filename} attempting archive_file_rclone...")

    # Get just the filename
    filename = os.path.basename(full_filename)

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception:
        logger.exception(f"{full_filename}: Error determining file size.")
        return False

    # Start fresh with a list of all possible endpoints (from the config file)
    endpoints = endpoints.copy()
    start_time = time.time()

    while len(endpoints) > 0:
        # Get a random endpoint
        endpoint_url = random.choice(endpoints)

        # rclone will create bucket if required
        logger.debug(f"{full_filename} attempting upload to {rclone_profile} {endpoint_url} bucket {bucket_name}...")

        # Do upload
        #
        # rclone copyto -M --metadata-set "md5=123abc" --s3-endpoint=https://vss-1.pawsey.org.au:9000
        #  test.txt banksia:/mwaingest-14322
        #
        try:
            cmdline = f'/usr/bin/rclone copyto -M --metadata-set "md5={md5hash}" --s3-endpoint={endpoint_url} {full_filename} {rclone_profile}:/{bucket_name}/{filename}'

            # run rclone copyto
            return_val, stdout = run_command_ext(logger, cmdline, None, 600, False)

            if return_val:
                elapsed = time.time() - start_time
                size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
                gbps_per_sec = (size_gigabytes * 8) / elapsed

                # Success - now verify the file at the remote
                logger.debug(
                    f"{full_filename} attempting check against {rclone_profile} {endpoint_url} bucket {bucket_name}..."
                )
                cmdline = f"/usr/bin/rclone check --s3-endpoint={endpoint_url} {full_filename} {rclone_profile}:/{bucket_name}"

                # run rclone check
                return_val, stdout = run_command_ext(logger, cmdline, None, 600, False)

                if return_val:
                    # If checksums match then rclone returns exit code 0. Otherwise !=0.
                    # run_command_ext returns True for 0 and False for anything else
                    check_elapsed = time.time() - start_time

                    logger.info(
                        f"{full_filename} archive_file_rclone success."
                        f"Copied ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
                        f" {gbps_per_sec:.3f} Gbps). Check took {check_elapsed:.3f} seconds."
                    )
                    return True
                else:
                    raise Exception(stdout)
            else:
                raise Exception(stdout)
        except Exception:
            logger.exception(
                f"{full_filename}: Error uploading to {endpoint_url} bucket {bucket_name} via rclone."
                f"Endpoint: {1 + len(endpoints) - len(endpoints)} of {len(endpoints)}."
            )
            # Remove this endpoint from the list for this file and try again if there are more
            # endpoints left.
            # It is possible the error is nothing to do with THIS endpoint but it's very difficult
            # to go down to that level. If we blow through all endpoints (e.g. Banksia has 6) and
            # We still hit the exception, then either all endpoints are down or it's some other
            # error in which case we return False which will put us in a retry/backoff cycle
            endpoints.remove(endpoint_url)
            continue

    if len(endpoints) > 0:
        raise Exception(f"Transfer failed but some endpoints ({len(endpoints)}) are unused. This should not happen!")
    else:
        # We tried with all available endpoints but still did not succeed
        logger.warning(
            f"{full_filename} could not be archived via rclone after trying all {len(endpoints)} endpoint(s)."
        )
        return False


#
# NOTE: this code relies on the fact that the machine/user running this code
# should already have a valid
# cat ~/.aws/config file which provides:
#
# [default]
# aws_access_key_id=XXXXXXXXXXXXXX
# aws_secret_access_key=XXXXXXXXXXXXXXXXXXXXXXXXX
#
# Boto3 will use this file to authenticate and fail if it is not there or is
# not valid
#
#
# Dervied from: https://github.com/tlastowka/calculate_multipart_etag/blob
# /master/calculate_multipart_etag.py
#
def ceph_get_s3_md5_etag(filename: str, chunk_size_bytes: int) -> str:
    """
    Determine what a Ceph etag should be
    given filename and chunk size
    """
    md5s = []

    with open(filename, "rb") as file_handle:
        while True:
            data = file_handle.read(chunk_size_bytes)

            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) > 1:
        digests = b"".join(m.digest() for m in md5s)
        new_md5 = hashlib.md5(digests)
        new_etag = f'"{new_md5.hexdigest()}-{len(md5s)}"'

    elif len(md5s) == 1:  # file smaller than chunk size
        new_etag = f'"{md5s[0].hexdigest()}"'

    else:  # empty file
        new_etag = '""'

    return new_etag
