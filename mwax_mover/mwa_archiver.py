import os
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from enum import Enum
import random
from datetime import datetime
from mwax_mover import mwax_command
import time

class MWADataFileType(Enum):
    MWA_FLAG_FILE=10
    MWA_PPD_FILE=14
    MWAX_VOLTAGES=17
    MWAX_VISIBILITIES=18


def validate_filename(filename: str, location: int) -> (bool, int, int, str, str, str, str):
    # Returns valid, obs_id, filetype_id, file_ext, prefix, dmf_host, validation_error
    valid: bool = True
    obs_id = 0
    validation_error: str = ""
    filetype_id: int = -1
    file_name_part: str = ""
    file_ext_part: str = ""
    prefix = ""
    dmf_host = ""

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
            validation_error = f"Filename does not start with a 10 digit observation_id- ignoring"
        else:
            obs_id = int(obs_id_check)

    # 3. Check extension
    if valid:
        if file_ext_part.lower() == ".sub":
            filetype_id = MWADataFileType.MWAX_VOLTAGES.value
        elif file_ext_part.lower() == ".fits":
            # Could be metafits (e.g. 1316906688_metafits_ppds.fits) or visibilitlies
            if file_name_part[10:] == "_metafits_ppds" or  file_name_part[10:] == "_metafits":
                filetype_id = MWADataFileType.MWA_PPD_FILE.value
            else:
                filetype_id = MWADataFileType.MWAX_VISIBILITIES.value

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
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_subobsid_XXX.sub)- ignoring"
        elif filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            # filename format should be obsid_yyyymmddhhnnss_chXXX_XXX.fits
            if len(file_name_part) != 35:
                valid = False
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_yyyymmddhhnnss_chXXX_XXX.fits)- ignoring"

        elif filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            # filename format should be obsid_metafits_ppds.fits
            if len(file_name_part) != 24 and len(file_name_part) != 19:
                valid = False
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_metafits_ppds.fits or obsid_metafits.fits)- ignoring"

        elif filetype_id == MWADataFileType.MWA_FLAG_FILE.value:
            # filename format should be obsid_flags.zip
            if len(file_name_part) != 16:
                valid = False
                validation_error = f"Filename (excluding extension) is not in the correct format " \
                                   f"(incorrect length ({len(file_name_part)}). Format should be " \
                                   f"obsid_flags.zip)- ignoring"

    # Now actually archive it
    if valid:
        if location == 1:  # DMF
            # We need a deterministic way of getting the dmf fs number (01,02,03,04), so if we run this multiple
            # times for the same file we get the same answer
            filename_sum = 0
            for c in filename:
                # Get the ascii code for this letter
                filename_sum = filename_sum + ord(c)

            if filetype_id == MWADataFileType.MWAX_VOLTAGES.value: # VCS
                dmf_fs = "volt01fs"
            else:  # Correlator, Flags, PPDs
                # Determine which filesystem to use
                fs_number = (filename_sum % 4) + 1
                dmf_fs = f"mwa0{fs_number}fs"

            # use any dmf host
            dmf_host = random.choice(["fe1.pawsey.org.au","fe2.pawsey.org.au","fe4.pawsey.org.au"])

            yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")
            prefix = f"/mnt/{dmf_fs}/MWA/ngas_data_volume/mfa/{yyyy_mm_dd}/"
        else:
            # Ceph and Versity not yet implemented
            raise NotImplementedError

    return valid, obs_id, filetype_id, file_ext_part, prefix, dmf_host, validation_error


def archive_file_rsync(logger, full_filename: str, archive_numa_node: int, archive_destination_host: str,
                       archive_destination_path: str, timeout: int):
    logger.debug(f"{full_filename} attempting archive_file_rsync...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(f"Error determining file size for {full_filename}. Error {e}")
        return False

    # Build final command line
    # --no-compress ensures we don't try to compress (it's going to be quite uncompressible)
    # The -e "xxx" is there to remove as much encryption/compression of the ssh connection as possible to speed up the xfer
    cmdline = f"rsync --no-compress -e 'ssh -T -c aes128-cbc -o StrictHostKeyChecking=no -o Compression=no -x ' " \
              f"-r {full_filename} {archive_destination_host}:{archive_destination_path}"

    start_time = time.time()

    # run xrdcp
    return_val, stdout = mwax_command.run_command_ext(logger, cmdline, archive_numa_node, timeout, False)

    if return_val:
        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000. * 1000. * 1000.)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(f"{full_filename} archive_file_rsync success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")
        return True
    else:
        return False


def archive_file_xrootd(logger, full_filename: str, archive_numa_node: int, archive_destination_host: str, timeout: int):
    logger.debug(f"{full_filename} attempting archive_file_xrootd...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(f"Error determining file size for {full_filename}. Error {e}")
        return False

    # Build final command line
    cmdline = f"/usr/local/bin/xrdcp --force --cksum adler32 " \
              f"--silent --streams 2 --tlsnodata {full_filename} xroot://{archive_destination_host}"

    start_time = time.time()

    # run xrdcp
    return_val, stdout = mwax_command.run_command_ext(logger, cmdline, archive_numa_node, timeout, False)

    if return_val:
        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000. * 1000. * 1000.)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(f"{full_filename} archive_file_xrootd success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

        return True
    else:
        return False


def archive_file_ceph(logger, full_filename: str, ceph_endpoint: str):
    logger.debug(f"{full_filename} attempting archive_file_ceph...")

    # get file size
    logger.debug(f"{full_filename} attempting to get file size...")
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(f"Error determining file size for {full_filename}. Error {e}")
        return False

    # determine bucket name
    logger.debug(f"{full_filename} determining destination bucket name...")
    try:
        bucket_name = ceph_get_bucket_name_from_filename(full_filename)
    except Exception as e:
        logger.error(f"Error determining bucket name for {full_filename}. Error {e}")
        return False

    # get s3 object
    logger.debug(f"{full_filename} getting S3 bucket reference: {bucket_name}...")
    try:
        s3_object = ceph_get_s3_object(ceph_endpoint)
    except Exception as e:
        logger.error(f"Error connecting to S3 endpoint: {ceph_endpoint}. Error {e}")
        return False

    # create bucket if required
    logger.debug(f"{full_filename} creating S3 bucket {bucket_name} (if required)...")
    try:
        ceph_create_bucket(s3_object, bucket_name)
    except Exception as e:
        logger.error(f"Error creating/checking existence of S3 bucket {bucket_name} on {ceph_endpoint}. Error {e}")
        return False

    logger.debug(f"{full_filename} attempting upload to S3 bucket {bucket_name}...")

    # start timer
    start_time = time.time()

    # Do upload
    try:
        ceph_upload_file(s3_object, bucket_name, full_filename)
    except Exception as e:
        logger.error(f"Error uploading {full_filename} to S3 bucket {bucket_name} on {ceph_endpoint}. Error {e}")
        return False

    # end timer
    elapsed = time.time() - start_time

    size_gigabytes = float(file_size) / (1000. * 1000. * 1000.)
    gbps_per_sec = (size_gigabytes * 8) / elapsed

    logger.info(f"{full_filename} archive_file_ceph success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")
    return True

#
# NOTE: this code relies on the fact that the machine/user running this code should already have a valid
# cat ~/.aws/config file which provides:
#
# [default]
# aws_access_key_id=XXXXXXXXXXXXXX
# aws_secret_access_key=XXXXXXXXXXXXXXXXXXXXXXXXX
#
# Boto3 will use this file to authenticate and fail if it is not there or is not valid
#
def ceph_get_s3_object(endpoint: str):
    # This ensures the default boto retries and timeouts don't leave us hanging too long
    config = Config(connect_timeout=20, retries={'max_attempts': 2})

    s3_object = boto3.resource('s3',
                               endpoint_url=endpoint, config=config)

    return s3_object

def ceph_get_bucket_name_from_filename(filename: str) -> str:
    file_part = os.path.split(filename)[1]
    return ceph_get_bucket_name_from_obs_id(int(file_part[0:10]))

def ceph_get_bucket_name_from_obs_id(obs_id: int) -> str:
    return str(obs_id)[0:4]

def ceph_create_bucket(s3_object, bucket_name: str):
    bucket = s3_object.Bucket(bucket_name)
    bucket.create()

def ceph_list_bucket(s3_object, bucket_name: str) -> list:
    bucket = s3_object.Bucket(bucket_name)
    return list(bucket.objects.all())

def ceph_upload_file(s3_object, bucket_name: str, filename: str) -> bool:
    # Set number of bytes in 1 MB
    MB = (1024 * 1024)

    # get key
    key = os.path.split(filename)[1]

    # get reference to bucket
    bucket = s3_object.Bucket(bucket_name)

    # configure the xfer to use multiparts
    config = TransferConfig(multipart_threshold=100 * MB, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)

    # Upload the file
    bucket.upload_file(Filename=filename, Key=key, Config=config)
    return True
