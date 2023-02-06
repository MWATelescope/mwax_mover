"""Contains various methods dealing with archiving"""
import os
import hashlib
import random
import time
import uuid
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from mwax_mover import mwax_command


def archive_file_rsync(
    logger,
    full_filename: str,
    archive_numa_node: int,
    archive_destination_host: str,
    archive_destination_path: str,
    timeout: int,
):
    """Archives a file via rsync"""
    logger.debug(f"{full_filename} attempting archive_file_rsync...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
        logger.error(
            f"{full_filename}: Error determining file size. Error"
            f" {catch_all_exceptiion}"
        )
        return False

    # Build final command line
    # --no-compress ensures we don't try to compress (it's going to be quite
    # uncompressible)
    # The -e "xxx" is there to remove as much encryption/compression of the
    # ssh connection as possible to speed up the xfer
    cmdline = (
        "rsync --no-compress -e 'ssh -T -c aes128-cbc -o"
        " StrictHostKeyChecking=no -o Compression=no -x ' "
        f"-r {full_filename} {archive_destination_host}:"
        f"{archive_destination_path}"
    )

    start_time = time.time()

    # run xrdcp
    return_val, stdout = mwax_command.run_command_ext(
        logger, cmdline, archive_numa_node, timeout, False
    )

    if return_val:
        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(
            f"{full_filename} archive_file_rsync success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
            f" {gbps_per_sec:.3f} Gbps)"
        )
        return True
    else:
        logger.error(
            f"{full_filename} archive_file_rsync failed. Error {stdout}"
        )
        return False


def archive_file_xrootd(
    logger,
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
        logger.error(
            f"{full_filename}: Error determining file size. Error"
            f" {catch_all_exceptiion}"
        )
        return False

    # Gather some info for later
    filename = os.path.basename(full_filename)
    temp_filename = f"{filename}.part{uuid.uuid4()}"
    # Archive destination host looks like: "192.168.120.110://volume2/incoming"
    # so just get the bit before the ":" for the host and the bit after for
    # the path
    destination_host = archive_destination_host.split(":")[0]
    destination_path = archive_destination_host.split(":")[1]
    full_destination_temp_filename = os.path.join(
        destination_path, temp_filename
    )
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
    return_val, stdout = mwax_command.run_command_ext(
        logger, cmdline, archive_numa_node, timeout, False
    )

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
        return_val, stdout = mwax_command.run_command_ext(
            logger, cmdline, archive_numa_node, timeout, False
        )

        if return_val:
            logger.info(
                f"{full_filename} archive_file_xrootd successfully renamed"
                f" {full_destination_temp_filename} to"
                f" {full_destination_final_filename} on the remote host"
                f" {destination_host}"
            )
            return True
        else:
            logger.error(
                f"{full_filename} archive_file_xrootd rename failed. Error"
                f" {stdout}"
            )
            return False
    else:
        logger.error(
            f"{full_filename} archive_file_xrootd failed. Error {stdout}"
        )
        return False


def archive_file_ceph(
    logger,
    ceph_session: boto3.Session,
    ceph_endpoints: list,
    full_filename: str,
    bucket_name: str,
    md5hash: str,
    multipart_threshold_bytes: int = None,
    chunk_size_bytes: int = None,
    max_concurrency: int = None,
):
    """Archive file via ceph"""
    logger.debug(f"{full_filename} attempting archive_file_ceph...")

    # get file size
    logger.debug(f"{full_filename} attempting to get file size...")
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
        logger.error(
            f"{full_filename}: Error determining file size. Error"
            f" {catch_all_exceptiion}"
        )
        return False

    # Get a valid s3 resource
    ceph_resource: boto3.resource = ceph_get_s3_resource(
        logger, ceph_session, ceph_endpoints
    )

    # create bucket if required
    logger.debug(
        f"{full_filename} creating S3 bucket {bucket_name} (if required)..."
    )
    try:
        ceph_create_bucket(ceph_resource, bucket_name)
    except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
        logger.error(
            f"{full_filename}: Error creating/checking existence of S3 bucket"
            f" {bucket_name}. Error {catch_all_exceptiion}"
        )
        return False

    logger.debug(
        f"{full_filename} attempting upload to S3 bucket {bucket_name}..."
    )

    # start timer
    start_time = time.time()

    # Do upload
    try:
        ceph_upload_file(
            ceph_resource,
            bucket_name,
            full_filename,
            md5hash,
            multipart_threshold_bytes,
            chunk_size_bytes,
            max_concurrency,
        )
    except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
        logger.error(
            f"{full_filename}: Error uploading to S3 bucket {bucket_name}."
            f"Error {catch_all_exceptiion}"
        )
        return False

    # end timer
    elapsed = time.time() - start_time

    size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    logger.info(
        f"{full_filename} archive_file_ceph success. ({size_gigabytes:.3f}GB"
        f" in {elapsed:.3f} seconds at {gbps_per_sec:.3f} Gbps)"
    )
    return True


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


def ceph_get_s3_session(profile: str) -> boto3.Session:
    """Returns a boto3 session given the profile name"""
    session = boto3.Session(profile_name=profile)
    return session


def ceph_get_s3_resource(
    logger, session: boto3.Session, endpoints: list
) -> boto3.resource:
    """Returns an S3 resource object"""
    # This ensures the default boto retries and timeouts don't leave us
    # hanging too long
    config = Config(connect_timeout=5, retries={"mode": "standard"})

    endpoint_count = len(endpoints)

    # Try all of the available endpoints
    for attempt in range(endpoint_count):
        try:
            # Get an enpoint at random from what is left on the list
            endpoint = random.choice(endpoints)

            s3_resource = session.resource(
                "s3", endpoint_url=endpoint, config=config
            )
            logger.debug(f"Using endpoint {endpoint}")

        except Exception as catch_all_exceptiion:  # pylint: disable=broad-except
            # Attempt failed! Probably due to end point being down.
            logger.warning(
                f"Could not connect to Ceph endpoint (attempt {attempt + 1} of"
                f" {endpoint_count}) {endpoint}: {catch_all_exceptiion}"
            )
            # Remove this endpoint from our list
            endpoints.remove(endpoint)
        else:
            return s3_resource

    raise Exception(f"Could not connect to any Ceph endpoints ({endpoints})")


def ceph_create_bucket(s3_resource: boto3.resource, bucket_name: str):
    """Create a bucket via S3"""
    bucket = s3_resource.Bucket(bucket_name)
    bucket.create()


def ceph_list_bucket(s3_resource: boto3.resource, bucket_name: str) -> list:
    """List contents of a bucket"""
    bucket = s3_resource.Bucket(bucket_name)
    return list(bucket.objects.all())


def ceph_upload_file(
    ceph_resource: boto3.resource,
    bucket_name: str,
    filename: str,
    md5hash: str,
    multipart_threshold_bytes: int = None,
    chunk_size_bytes: int = None,
    max_concurrency: int = None,
) -> bool:
    """upload a file via ceph/s3"""
    # get key
    key = os.path.split(filename)[1]

    # get reference to bucket
    bucket = ceph_resource.Bucket(bucket_name)

    # configure the xfer to use multiparts if specified
    if (
        multipart_threshold_bytes is not None
        and chunk_size_bytes is not None
        and max_concurrency is not None
    ):
        # 5GB is the limit Ceph has for parts, so only split if >= 2GB
        config = TransferConfig(
            multipart_threshold=multipart_threshold_bytes,
            multipart_chunksize=chunk_size_bytes,
            use_threads=True,
            max_concurrency=max_concurrency,
        )

        # Upload the file and include the md5sum as metadata
        bucket.upload_file(
            Filename=filename,
            Key=key,
            Config=config,
            ExtraArgs={"Metadata": {"md5": md5hash}},
        )
    else:
        # Upload the file without a transferconfig
        bucket.upload_file(
            Filename=filename,
            Key=key,
            ExtraArgs={"Metadata": {"md5": md5hash}},
        )
    return True
