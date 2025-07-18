"""Tests for the mwacache_archiver ceph primitives"""

import argparse
import logging
import os
import time
import boto3
from mwax_mover.mwa_archiver import (
    ceph_get_s3_session,
    ceph_get_s3_resource,
    archive_file_boto3,
)
from mwax_mover.utils import do_checksum_md5


def check_pawsey_lts(
    logger,
    profile_name: str,
    endpoint: str,
    input_filename: str,
    test_bucket: str,
    output_filename: str,
    leave_file_on_ceph: bool,
):
    # Remove old file from ceph if it exists
    ceph_remove_file(logger, test_bucket, input_filename, profile_name, endpoint)

    # Get checksum of local file
    filename_checksum = do_checksum_md5(logger, input_filename, -1, 600)

    ceph_upload_to_pawsey(logger, test_bucket, input_filename, profile_name, endpoint, filename_checksum)

    logger.info("Delaying download by 5 seconds to allow Ceph to settle")
    time.sleep(5)

    ceph_download_from_pawsey(logger, test_bucket, input_filename, output_filename, profile_name, endpoint)

    temp_filename_checksum = do_checksum_md5(logger, output_filename, -1, 600)

    # Check they match
    if filename_checksum != temp_filename_checksum:
        logger.error(f"Checksums do not match! Local: {filename_checksum} vs Downloaded" f" {temp_filename_checksum}")
        exit(-1)

    logger.info(f"Checksums match OK: Local: {filename_checksum} vs Downloaded" f" {temp_filename_checksum}")

    # Remove temp file
    logger.info(f"Removing output file: {output_filename}")
    os.remove(output_filename)

    # Remove file from ceph too
    if not leave_file_on_ceph:
        ceph_remove_file(logger, test_bucket, input_filename, profile_name, endpoint)


def ceph_remove_file(logger, bucket, filename, profile_name, endpoint):
    session = ceph_get_s3_session(profile_name)

    s3_resource, endpoint = ceph_get_s3_resource(logger, session, [endpoint])

    key = os.path.basename(filename)
    logger.info(f"About to delete {endpoint}/{bucket}/{key}")

    # Delete file
    try:
        s3_object = s3_resource.Object(bucket, key)  # type: ignore
        s3_object.delete()
    except Exception:
        logger.exception(f"Failed to delete {bucket}/{key} from {profile_name}")


def ceph_upload_to_pawsey(
    logger,
    bucket: str,
    filename: str,
    profile_name: str,
    endpoint: str,
    md5_checksum: str,
):
    session: boto3.Session = ceph_get_s3_session(profile_name)

    # Test archive
    archive_success = archive_file_boto3(
        logger,
        session,
        [endpoint],
        filename,
        bucket,
        md5_checksum,
        None,
        None,
        None,
    )

    if not archive_success:
        exit(-1)


def ceph_download_from_pawsey(
    logger,
    bucket: str,
    filename: str,
    output_filename: str,
    profile_name: str,
    endpoint: str,
):
    session = ceph_get_s3_session(profile_name)

    s3_resource, endpoint = ceph_get_s3_resource(logger, session, [endpoint])

    # Download file
    key = os.path.basename(filename)
    s3_object = s3_resource.Object(bucket, key)  # type: ignore
    s3_object.download_file(output_filename)

    if not os.path.exists(output_filename):
        logger.error(f"Failed to download {bucket}/{key}")
        exit(-1)


def main():
    logger = logging.getLogger("mwacache_checks")
    logger.setLevel(logging.DEBUG)
    console_log = logging.StreamHandler()
    console_log.setLevel(logging.DEBUG)
    console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
    logger.addHandler(console_log)

    # Get command line args
    parser = argparse.ArgumentParser()
    parser.description = "mwacache_checks- tests to ensure Acacia and Banksia are working OK.\n"

    parser.add_argument(
        "-p",
        "--profile_name",
        required=True,
        help="S3 profile name to test with (from ~/.aws/config).\n",
    )
    parser.add_argument("-e", "--endpoint", required=True, help="S3 endpoint.\n")
    parser.add_argument(
        "-b",
        "--bucket",
        default="mwacache-checks",
        required=True,
        help="Ceph Bucket name (will be created if doesn't exist).\n",
    )
    parser.add_argument("-i", "--input_file", required=True, help="Input filename.\n")
    parser.add_argument("-o", "--output_file", required=True, help="Output filename.\n")
    parser.add_argument(
        "-l",
        "--leave_file_on_ceph",
        action="store_true",
        help=("Leave uploaded file on Ceph (i.e. don't delete after the test" " completes).\n"),
    )

    args = vars(parser.parse_args())

    # Check that config file exists
    endpoint = args["endpoint"]
    profile_name = args["profile_name"]
    input_filename = args["input_file"]
    output_filename = args["output_file"]
    leave_file_on_ceph = args["leave_file_on_ceph"]
    bucket = args["bucket"]

    if not os.path.exists(input_filename):
        print(f"Parameter: input_file {input_filename} does not exist. Exiting.")
        exit(-2)

    if os.path.exists(output_filename):
        print(
            f"Parameter: output_file {output_filename} already exists. Please specify a"
            " filename which does NOT exist. Exiting."
        )
        exit(-2)

    logger.info("Starting mwacache_checks to ensure Acacia and Banksia are working OK.")
    logger.info("Parameters:")
    logger.info(f"\tProfile           : {profile_name}")
    logger.info(f"\tEndpoint          : {endpoint}")
    logger.info(f"\tInput file        : {input_filename}")
    logger.info(f"\tOutput file       : {output_filename}")
    logger.info(f"\tBucket            : {bucket}")
    logger.info(f"\tLeave file on Ceph: {leave_file_on_ceph}")

    check_pawsey_lts(
        logger,
        profile_name,
        endpoint,
        input_filename,
        bucket,
        output_filename,
        leave_file_on_ceph,
    )


if __name__ == "__main__":
    main()
