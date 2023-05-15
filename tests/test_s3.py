import pytest

from awsflowutils import s3

download_upload_TypeError_args = [
    (1, "foo"),
    ("foo", 1),
    (1, ["foo"]),
    (["foo"], 1),
    ([1], "foo"),
    ("foo", [1]),
    (["foo"], "bar"),
    ("foo", ["bar"]),
]


@pytest.mark.parametrize("s3_filepath,local_filepath", download_upload_TypeError_args)
def test_s3_download_type_error(s3_filepath, local_filepath):
    # test whether s3_download() raises the proper error
    with pytest.raises(TypeError):
        s3.s3_download(
            bucket="test", s3_filepath=s3_filepath, local_filepath=local_filepath
        )


@pytest.mark.parametrize("s3_filepath,local_filepath", download_upload_TypeError_args)
def test_s3_upload_type_error(s3_filepath, local_filepath):
    # test whether s3_upload() raises the proper error
    with pytest.raises(TypeError):
        s3.s3_upload(
            bucket="test", local_filepath=local_filepath, s3_filepath=s3_filepath
        )


download_upload_ValueError_args = [
    (["foo", "bar"], ["baz"]),
    (["foo"], ["bar", "baz"]),
    (["f*"], ["bar"]),
    (["foo"], ["b*"]),
]


@pytest.mark.parametrize("s3_filepath,local_filepath", download_upload_ValueError_args)
def test_s3_download_value_error(s3_filepath, local_filepath):
    # test whether s3_download() raises the proper error
    with pytest.raises(ValueError):
        s3.s3_download(
            bucket="test", s3_filepath=s3_filepath, local_filepath=local_filepath
        )


@pytest.mark.parametrize("s3_filepath,local_filepath", download_upload_ValueError_args)
def test_s3_upload_value_error(s3_filepath, local_filepath):
    # test whether s3_upload() raises the proper error
    with pytest.raises(ValueError):
        s3.s3_upload(
            bucket="test", local_filepath=local_filepath, s3_filepath=s3_filepath
        )


delete_TypeError_args = [(1,), ([1],)]


@pytest.mark.parametrize("s3_filepath", delete_TypeError_args)
def test_s3_delete_type_error(s3_filepath):
    # test whether s3_delete() raises the proper error
    with pytest.raises(TypeError):
        s3.s3_delete(bucket="test", s3_filepath=s3_filepath)


def test_s3_delete_value_error():
    # test whether s3_delete() raises the proper error
    with pytest.raises(ValueError):
        s3.s3_delete(bucket="test", s3_filepath=["*"])
