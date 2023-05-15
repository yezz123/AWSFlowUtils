from awsflowutils import boto


def test_boto_create_session_type(mocker):
    # Mock the Boto3 Session class
    mock_session = mocker.patch("boto3.Session")

    # Create a mock session instance
    session_instance = mock_session.return_value

    # Call the function under test
    result = boto.boto_create_session()

    # Assertions
    assert result == session_instance


def test_boto_get_creds_type(mocker):
    # Mock the Boto3 Session class
    mock_session = mocker.patch("boto3.Session")

    # Create a mock session instance
    session_instance = mock_session.return_value

    # Mock the Boto3 Credentials class
    mock_credentials = mocker.patch.object(session_instance, "get_credentials")

    # Create a mock credentials instance
    credentials_instance = mock_credentials.return_value

    # Mock the access key, secret key, and token
    mock_access_key = credentials_instance.access_key
    mock_secret_key = credentials_instance.secret_key
    mock_token = credentials_instance.token

    # Call the function under test
    result = boto.boto_get_creds()

    # Assertions
    assert (
        result
        == f"aws_access_key_id={mock_access_key};aws_secret_access_key={mock_secret_key};token={mock_token}"
    )
