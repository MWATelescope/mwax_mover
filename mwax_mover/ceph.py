import os
import boto3
from boto3.s3.transfer import TransferConfig
import time

def ceph_get_s3_object(endpoint: str):
    s3_object = boto3.resource('s3',
                               endpoint_url=endpoint)

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
    config = TransferConfig(multipart_threshold=100 * MB)

    # beging timing
    start_time = time.time()

    # Upload the file
    bucket.upload_file(Filename=filename, Key=key, Config=config)
    return True
