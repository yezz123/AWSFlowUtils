from typing import Optional

import boto3


def boto_create_session(
    profile_name: str = "default",
    region_name: str = "us-west-2",
) -> boto3.Session:
    """Instantiates and returns a boto3 session object"""
    return boto3.Session(profile_name=profile_name, region_name=region_name)


def boto_get_creds(
    profile_name: str = "default",
    region_name: str = "us-west-2",
    session: Optional[boto3.Session] = None,
) -> str:
    """Generates and returns an S3 credential string"""
    if session is None:
        session = boto_create_session(
            profile_name=profile_name, region_name=region_name
        )
    access_key = session.get_credentials().access_key
    secret_key = session.get_credentials().secret_key
    token = session.get_credentials().token
    return f"""aws_access_key_id={access_key};aws_secret_access_key={secret_key};token={token}"""
