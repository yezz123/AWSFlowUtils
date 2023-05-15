import glob
import os
from typing import List, Union

import boto3
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from awsflowutils.boto import boto_create_session


def s3_get_bucket(
    bucket: str,
    profile_name: str = "default",
    region_name: str = "us-west-2",
) -> boto3.resource("s3").Bucket:
    """Creates and returns a boto3 bucket object"""
    session = boto_create_session(profile_name=profile_name, region_name=region_name)
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(bucket)
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except ClientError as e:
        # Check if bucket exists, if not raise error
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            raise NameError("404 Bucket does not exist") from e
        if error_code == 400:
            raise NameError("400 The credentials were expired or incorrect.") from e
    return my_bucket


def s3_download(
    bucket: str,
    s3_filepath: Union[str, List[str]],
    local_filepath: Union[str, List[str]],
    profile_name: str = "default",
    region_name: str = "us-west-2",
    multipart_threshold: int = 8388608,
    multipart_chunksize: int = 8388608,
) -> None:
    """Downloads a file or collection of files from S3"""
    _download_upload_filepath_validator(
        s3_filepath=s3_filepath, local_filepath=local_filepath
    )
    my_bucket = s3_get_bucket(
        bucket=bucket, profile_name=profile_name, region_name=region_name
    )
    config = TransferConfig(
        multipart_threshold=multipart_threshold, multipart_chunksize=multipart_chunksize
    )
    if isinstance(s3_filepath, str):
        if "*" in s3_filepath:
            s3_filepath = _s3_glob(s3_filepath=s3_filepath, my_bucket=my_bucket)
            local_filepath = [
                os.path.join(local_filepath, key.split("/")[-1]) for key in s3_filepath
            ]
        else:
            s3_filepath = [s3_filepath]
            local_filepath = [local_filepath]
    for s3_key, local_file in zip(s3_filepath, local_filepath):
        try:
            my_bucket.download_file(s3_key, local_file, Config=config)
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 400:
                raise NameError(
                    f"The credentials are expired or not valid. {str(e)}"
                ) from e
            else:
                raise e
    return


def s3_upload(
    bucket: str,
    local_filepath: Union[str, List[str]],
    s3_filepath: Union[str, List[str]],
    profile_name: str = "default",
    region_name: str = "us-west-2",
    multipart_threshold: int = 8388608,
    multipart_chunksize: int = 8388608,
) -> None:
    """Uploads a file or collection of files to S3"""
    _download_upload_filepath_validator(
        s3_filepath=s3_filepath, local_filepath=local_filepath
    )
    my_bucket = s3_get_bucket(
        bucket=bucket, profile_name=profile_name, region_name=region_name
    )
    config = TransferConfig(
        multipart_threshold=multipart_threshold, multipart_chunksize=multipart_chunksize
    )
    if isinstance(local_filepath, str):
        if "*" in local_filepath:
            items = glob.glob(local_filepath)
            # filter out directories
            local_filepath = [item for item in items if os.path.isfile(item)]
            tmp_s3_filepath = [s3_filepath + f.split("/")[-1] for f in local_filepath]
            s3_filepath = tmp_s3_filepath
        else:
            local_filepath = [local_filepath]
            s3_filepath = [s3_filepath]
    # upload all files to S3
    for local_file, s3_key in zip(local_filepath, s3_filepath):
        try:
            my_bucket.upload_file(local_file, s3_key, Config=config)
        except S3UploadFailedError as e:
            raise S3UploadFailedError(str(e)) from e
    return


def s3_delete(
    bucket: str,
    s3_filepath: Union[str, List[str]],
    profile_name="default",
    region_name="us-west-2",
) -> List[str]:
    """Deletes a file or collection of files from S3"""
    _delete_filepath_validator(s3_filepath=s3_filepath)
    my_bucket = s3_get_bucket(
        bucket=bucket, profile_name=profile_name, region_name=region_name
    )
    if isinstance(s3_filepath, str):
        if "*" in s3_filepath:
            s3_filepath = _s3_glob(s3_filepath=s3_filepath, my_bucket=my_bucket)
            if not s3_filepath:
                return []
        else:
            s3_filepath = [s3_filepath]
    objects = [{"Key": key} for key in s3_filepath]
    del_dict = {"Objects": objects}
    response = my_bucket.delete_objects(Delete=del_dict)
    return response["Deleted"]


def _download_upload_filepath_validator(
    s3_filepath: Union[str, List[str]],
    local_filepath: Union[str, List[str]],
) -> None:
    """Validates the s3_filepath and local_filepath arguments and raises clear errors"""
    for arg in (s3_filepath, local_filepath):
        if not isinstance(arg, (list, str)):
            raise TypeError(
                "Both s3_filepath and local_filepath must be of type list or str"
            )
    if type(s3_filepath) != type(local_filepath):
        raise TypeError("Both s3_filepath and local_filepath must be of same type")
    if isinstance(s3_filepath, list):
        for f in s3_filepath + local_filepath:
            if not isinstance(f, str):
                raise TypeError(
                    "If s3_filepath and local_filepath are lists, they must contain strings"
                )
            if "*" in f:
                raise ValueError(
                    "Wildcards (*) are not permitted within a list of filepaths"
                )
        if len(s3_filepath) != len(local_filepath):
            raise ValueError(
                "The s3_filepath list must the same number of elements as the local_filepath list"
            )
    return


def _delete_filepath_validator(s3_filepath: Union[str, List[str]]) -> None:
    """Validates the s3_filepath argument and raises clear errors"""
    if not isinstance(s3_filepath, (list, str)):
        raise TypeError("s3_filepath must be of type list or str")
    if isinstance(s3_filepath, list):
        for f in s3_filepath:
            if not isinstance(f, str):
                raise TypeError("If s3_filepath is a list, it must contain strings")
            if "*" in f:
                raise ValueError(
                    "Wildcards (*) are not permitted within a list of filepaths"
                )
    return


def _s3_glob(
    s3_filepath: str,
    my_bucket: str,
) -> List[str]:
    """Searches a directory in an S3 bucket and returns keys matching the wildcard"""
    # use left and right for pattern matching
    left, _, right = s3_filepath.partition("*")
    # construct s3_path without wildcard
    s3_path = "/".join(s3_filepath.split("/")[:-1]) + "/"
    filtered_s3_filepath = []
    for item in my_bucket.objects.filter(Prefix=s3_path):
        # filter out directories
        if item.key[-1] != "/":
            p1, p2, p3 = item.key.partition(left)
            # pattern matching
            if p1 == "" and p2 == left and right in p3:
                filtered_s3_filepath.append(item.key)
    return filtered_s3_filepath
