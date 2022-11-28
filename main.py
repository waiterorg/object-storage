# this is simple apis for s3 object storage

import boto3
import logging 
from botocore.exceptions import ClientError
from pathlib import Path

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent


def credential():
    try:
        endpoint_url = 'https://s3.ir-thr-at1.arvanstorage.ir/'
        access_key = ''
        secret_key = ''
        s3_resource = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        return s3_resource
    except Exception as exc:
        logging.info(exc)


def create_bucket(name):
    s3_resource = credential()
    try:
        bucket_name = name
        bucket = s3_resource.Bucket(bucket_name)
        bucket.create(ACL='public-read') # ACL='private'|'public-read'
        logging.info(f'bucket successfuly created ! : {bucket}')
        return bucket
    except ClientError as exc:
        logging.error(exc)


def list_objcets(bucket_name):
    s3_resource = credential()
    try:
        bucket = s3_resource.Bucket(bucket_name)

        for obj in bucket.objects.all():
            logging.info(f"object_name: {obj.key}, last_modified: {obj.last_modified}")

    except ClientError as e:
        logging.error(e)


def get_object_acl(bucket_name, object_name):
    s3_resource = credential()
    try:
        bucket_name = bucket_name
        object_name = object_name

        bucket = s3_resource.Bucket(bucket_name)
        object_acl = bucket.Object(object_name).Acl()
        logging.info(object_acl.grants)
    except ClientError as e:
        logging.error(e)


def set_object_acl(bucket_name, acl):
    s3_resource = credential()
    try:
        bucket_name = bucket_name
        bucket_acl = s3_resource.BucketAcl(bucket_name)
        bucket_acl.put(ACL=acl)  # ACL='private'|'public-read'|'public-read-write'

    except ClientError as e:
        logging.error(e)

def download_object(bucket_name, object_name):
    s3_resource = credential()
    try:
        # bucket
        bucket = s3_resource.Bucket(bucket_name)

        object_name = object_name
        download_path = ''

        bucket.download_file(
            object_name,
            download_path
        )
        logging.info(f'dowloaded successfully on path : {download_path}')
    except ClientError as e:
        logging.error(e)


def upload_object(bucket_name, object_name):
    s3_resource = credential()
    try:
        bucket = s3_resource.Bucket(bucket_name)
        file_path = ''
        object_name = object_name

        with open(file_path, "rb") as file:
            bucket.put_object(
                ACL='private',
                Body=file,
                Key=object_name
            )
        logging.info(f'uploaded successfully !')
    except ClientError as e:
        logging.error(e)


list_objcets('this-is-test-2')
# list_objcets('this-is-test')
# get_object_acl('this-is-test', 'Screenshot from 2022-10-08 15-27-54.png') # private acl
# get_object_acl('this-is-test-2', 'Screenshot from 2022-10-08 15-35-29.png') # public-read acl
get_object_acl('this-is-test-2', 'Screenshot from 2022-10-08 15-35-29.png')
download_object('this-is-test-2', 'Screenshot from 2022-10-08 15-35-29.png')
upload_object('this-is-test-2', 'test.png')
# download_object('this-is-test', 'Screenshot from 2022-10-08 15-27-54.png')
