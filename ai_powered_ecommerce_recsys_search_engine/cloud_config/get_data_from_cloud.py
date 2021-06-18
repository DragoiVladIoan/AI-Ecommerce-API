import os
import boto3
from ibm_botocore.client import Config, ClientError
import yaml

from clonevirtualenv import logger


def load_config():
    with open("cloud_config/config.yaml", 'r') as stream:
        try:
            data_location = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data_location


def get_file_cloud(data_location, filename):
    try:
        logger.debug("PWD is: " + str(os.getcwd()))
        session = boto3.session.Session()
        cos = session.client(
            service_name='s3',
            aws_access_key_id=data_location["connection"]["access_key_id"],
            aws_secret_access_key=data_location["connection"]["secret_access_key"],
            endpoint_url=data_location["connection"]["endpoint"],
            verify=False
        )
        file_obj = cos.get_object(Bucket=data_location["location"]["bucket"], Key=data_location["location"]["files"][filename])
        file_bytes = file_obj['Body'].read()
        file_content = file_bytes.decode("utf-8")
        file_content = file_content.rstrip()
        return file_content
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))
    return None


def upload_file_cloud(data_location, file_path, file_name):
    try:
        logger.debug("PWD is: " + str(os.getcwd()))
        session = boto3.session.Session()
        cos = session.client(
            service_name='s3',
            aws_access_key_id=data_location["connection"]["access_key_id"],
            aws_secret_access_key=data_location["connection"]["secret_access_key"],
            endpoint_url=data_location["connection"]["endpoint"],
            verify=False
        )
        cos.upload_file(Filename=file_path, Bucket=data_location["location"]["bucket"], Key=data_location["location"]["files"][file_name])
        return 0
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))


def download_file_cloud(data_location, file_path, file_name):
    try:
        logger.debug("PWD is: " + str(os.getcwd()))
        session = boto3.session.Session()
        cos = session.client(
            service_name='s3',
            aws_access_key_id=data_location["connection"]["access_key_id"],
            aws_secret_access_key=data_location["connection"]["secret_access_key"],
            endpoint_url=data_location["connection"]["endpoint"],
            verify=False
        )
        cos.download_file(Bucket=data_location["location"]["bucket"], Key=data_location["location"]["files"][file_name], Fileobj=file_path)
        return 0
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

