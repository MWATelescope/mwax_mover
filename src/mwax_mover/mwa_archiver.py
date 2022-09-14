"""Contains various methods dealing with archiving"""
import os
import hashlib
from enum import Enum
import time
import uuid
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from mwax_mover import mwax_command, utils

# Set number of bytes in 1 GB
MB = 1024 * 1024
GB = MB * 1024


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


def validate_filename(
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
    if valid:
        # Now check that the observation is a calibrator by
        # looking at the associated metafits file
        metafits_filename = os.path.join(
            metafits_path, f"{obs_id}_metafits.fits"
        )

        (calibrator, project_id) = utils.get_metafits_values(metafits_filename)

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
        bucket = ceph_get_bucket_name_from_filename(filename)
        folder = None
        return bucket, folder

    else:
        # DMF and Versity not yet implemented
        raise NotImplementedError(f"Location {location} is not supported.")


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
    except Exception as e:
        logger.error(
            f"{full_filename}: Error determining file size. Error {e}"
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
    except Exception as e:
        logger.error(
            f"{full_filename}: Error determining file size. Error {e}"
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
            return False
    else:
        return False


def archive_file_ceph(
    logger,
    ceph_session,
    full_filename: str,
    bucket_name: str,
    md5hash: str,
    profile: str,
    ceph_endpoint: str,
    multipart_threshold_bytes: int,
    chunk_size_bytes: int,
    max_concurrency: int,
):
    """Archive file via ceph"""
    logger.debug(f"{full_filename} attempting archive_file_ceph...")

    # get file size
    logger.debug(f"{full_filename} attempting to get file size...")
    try:
        file_size = os.path.getsize(full_filename)
    except Exception as e:
        logger.error(
            f"{full_filename}: Error determining file size. Error {e}"
        )
        return False

    # create bucket if required
    logger.debug(
        f"{full_filename} creating S3 bucket {bucket_name} (if required)..."
    )
    try:
        ceph_create_bucket(ceph_session, bucket_name)
    except Exception as e:
        logger.error(
            f"{full_filename}: Error creating/checking existence of S3 bucket"
            f" {bucket_name} on {ceph_endpoint}. Error {e}"
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
            ceph_session,
            bucket_name,
            full_filename,
            md5hash,
            multipart_threshold_bytes,
            chunk_size_bytes,
            max_concurrency,
        )
    except Exception as e:
        logger.error(
            f"{full_filename}: Error uploading to S3 bucket {bucket_name} on"
            f" {ceph_endpoint}. Error {e}"
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

    with open(filename, "rb") as fp:
        while True:
            data = fp.read(chunk_size_bytes)

            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) > 1:
        digests = b"".join(m.digest() for m in md5s)
        new_md5 = hashlib.md5(digests)
        new_etag = '"%s-%s"' % (new_md5.hexdigest(), len(md5s))

    elif len(md5s) == 1:  # file smaller than chunk size
        new_etag = '"%s"' % md5s[0].hexdigest()

    else:  # empty file
        new_etag = '""'

    return new_etag


def ceph_get_s3_object(profile: str, endpoint: str):
    """Returns an S3 resource object"""
    # create a session based on the profile name
    session = boto3.Session(profile_name=profile)

    # This ensures the default boto retries and timeouts don't leave us
    # hanging too long
    config = Config(connect_timeout=20, retries={"max_attempts": 2})

    s3_object = session.resource("s3", endpoint_url=endpoint, config=config)

    return s3_object


def ceph_get_bucket_name_from_filename(filename: str) -> str:
    """Generates a bucket name for a filename"""
    file_part = os.path.split(filename)[1]
    return ceph_get_bucket_name_from_obs_id(int(file_part[0:10]))


def ceph_get_bucket_name_from_obs_id(obs_id: int) -> str:
    """Generate bucket name given an obs_id"""
    # return the first 5 digits of the obsid
    # This means there will be a new bucket every ~27 hours
    # This is to reduce the chances of vcs jobs filling a bucket to more than
    # 100K of files
    return f"mwaingest-{str(obs_id)[0:5]}"


def ceph_create_bucket(s3_object, bucket_name: str):
    """Create a bucket via S3"""
    bucket = s3_object.Bucket(bucket_name)
    bucket.create()


def ceph_list_bucket(s3_object, bucket_name: str) -> list:
    """List contents of a bucket"""
    bucket = s3_object.Bucket(bucket_name)
    return list(bucket.objects.all())


def ceph_upload_file(
    s3_object,
    bucket_name: str,
    filename: str,
    md5hash: str,
    multipart_threshold_bytes: int,
    chunk_size_bytes: int,
    max_concurrency: int,
) -> bool:
    """upload a file via ceph/s3"""
    # get key
    key = os.path.split(filename)[1]

    # get reference to bucket
    bucket = s3_object.Bucket(bucket_name)

    # configure the xfer to use multiparts
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
    return True
