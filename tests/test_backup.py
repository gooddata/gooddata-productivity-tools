# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
)

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest import mock

import boto3
import pytest
from gooddata_sdk.sdk import GoodDataSdk
from moto import mock_s3

import scripts.backup as backup

LOGGER_NAME = "scripts.backup"
MOCK_DL_TARGET = Path("overlays.zip")
TEST_CONF_PATH = "tests/data/backup/test_conf.yaml"
TEST_LOCAL_CONF_PATH = "tests/data/backup/test_local_conf.yaml"

S3_BACKUP_PATH = "some/s3/backup/path/org_id/"
S3_BUCKET = "some-s3-bucket"

MOCK_SDK = GoodDataSdk.create("host", "token")


class MockGdWorkspace:
    def __init__(self, id: str) -> None:
        self.id = id


class MockResponse:
    def __init__(self, status_code, json_response=None, text: str = ""):
        self.status_code = status_code
        self.json_response = json_response if json_response else {}
        self.text = text

    def json(self):
        return self.json_response


def mock_requests_get(**kwargs):
    body: dict[str, list[Any]] = {"userDataFilters": []}
    return MockResponse(200, body)


def mock_requests():
    requests = mock.Mock()
    requests.get.side_effect = mock_requests_get
    return requests


@pytest.fixture(scope="function")
def aws_credentials():
    """
    Mocked AWS Credentials for moto.
    Ensures no locally set AWS credential envvars are used.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(scope="function")
def s3_bucket(s3):
    s3.create_bucket(Bucket=S3_BUCKET)
    yield s3.Bucket(S3_BUCKET)


@pytest.fixture(scope="function")
def create_backups_in_bucket(s3_bucket):
    def create_backups(ws_ids: list[str], is_e2e: bool = False, suffix: str = "bla"):
        # If used within e2e test, add some suffix to path
        # in order to simulate a more realistic scenario
        path_suffix = f"/{suffix}" if is_e2e else ""

        for ws_id in ws_ids:
            s3_bucket.put_object(
                Bucket=S3_BUCKET, Key=f"{S3_BACKUP_PATH}{ws_id}{path_suffix}/"
            )
            s3_bucket.put_object(
                Bucket=S3_BUCKET,
                Key=f"{S3_BACKUP_PATH}{ws_id}{path_suffix}/gooddata_layouts.zip",
            )

    return create_backups


def assert_not_called_with(target, *args, **kwargs):
    try:
        target.assert_called_with(*args, **kwargs)
    except AssertionError:
        return
    formatted_call = target._format_mock_call_signature(args, kwargs)
    raise AssertionError(f"Expected {formatted_call} to not have been called.")


@mock.patch.dict(os.environ, {"GDC_HOSTNAME": "hostname", "GDC_AUTH_TOKEN": "token"})
@mock.patch("gooddata_sdk.GoodDataSdk.create_from_profile")
@mock.patch("gooddata_sdk.GoodDataSdk.create")
def test_gd_client_env(client_create_env, client_create_profile):
    backup.create_client(argparse.Namespace())
    client_create_env.assert_called_once_with("hostname", "token")
    client_create_profile.assert_not_called()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch("scripts.backup.create_api_client_from_profile")
@mock.patch("gooddata_sdk.GoodDataSdk.create_from_profile")
@mock.patch("gooddata_sdk.GoodDataSdk.create")
@mock.patch("os.path.exists")
def test_gd_client_profile(
    path_exists,
    client_create_env,
    client_create_profile,
    create_api_client_from_profile,
):
    path_exists.return_value = True
    args = argparse.Namespace(
        profile_config="gdc_profile_config_path",
        profile="gdc_profile",
    )
    backup.create_client(args)
    client_create_env.assert_not_called()
    client_create_profile.assert_called_once_with(
        "gdc_profile", "gdc_profile_config_path"
    )
    create_api_client_from_profile.assert_called_once_with(
        "gdc_profile", "gdc_profile_config_path"
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_gd_client_no_creds_raises_error():
    args = argparse.Namespace(
        profile_config="",
        profile="",
    )
    with pytest.raises(RuntimeError):
        backup.create_client(args)


# Incorrect ws_csv and conf args throw error
@pytest.mark.parametrize("conf_path", ["", "configuration_nonexist.yaml"])
@pytest.mark.parametrize("csv_path", ["", "input_nonexist.csv"])
def test_wrong_wscsv_conf_raise_error(csv_path, conf_path):
    args = argparse.Namespace(
        ws_csv=csv_path, conf=conf_path, input_type="list-of-workspaces", verbose=False
    )
    with pytest.raises(RuntimeError):
        backup.validate_args(args)


def test_wrong_input_type_raises_error():
    args = argparse.Namespace(
        ws_csv="input.csv", conf="conf.yaml", input_type="wrong-input-type"
    )
    with pytest.raises(RuntimeError):
        backup.validate_args(args)


def test_get_s3_storage():
    s3_storage_type = backup.get_storage("s3")
    assert s3_storage_type == backup.S3Storage


def test_get_local_storage():
    local_storage_type = backup.get_storage("local")
    assert local_storage_type == backup.LocalStorage


def test_get_unknown_storage_raises_error():
    with pytest.raises(RuntimeError):
        backup.get_storage("unknown_storage")


# Test that zipping gooddata_layouts folder works
def test_archive_gooddata_layouts_to_zip():
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copytree(
            Path("tests/data/backup/test_exports/services/"), Path(tmpdir + "/services")
        )
        backup.archive_gooddata_layouts_to_zip(str(Path(tmpdir, "services")))

        zip_exists = os.path.isfile(
            Path(
                tmpdir, "services/wsid1/20230713-132759-1_3_1_dev5/gooddata_layouts.zip"
            )
        )
        gooddata_layouts_dir_exists = os.path.isdir(
            Path(tmpdir, "services/wsid1/20230713-132759-1_3_1_dev5/gooddata_layouts")
        )

        assert gooddata_layouts_dir_exists is False
        assert zip_exists

        zip_exists = os.path.isfile(
            Path(
                tmpdir, "services/wsid2/20230713-132759-1_3_1_dev5/gooddata_layouts.zip"
            )
        )
        gooddata_layouts_dir_exists = os.path.isdir(
            Path(tmpdir, "services/wsid2/20230713-132759-1_3_1_dev5/gooddata_layouts")
        )

        assert gooddata_layouts_dir_exists is False
        assert zip_exists

        zip_exists = os.path.isfile(
            Path(
                tmpdir, "services/wsid3/20230713-132759-1_3_1_dev5/gooddata_layouts.zip"
            )
        )
        gooddata_layouts_dir_exists = os.path.isdir(
            Path(tmpdir, "services/wsid3/20230713-132759-1_3_1_dev5/gooddata_layouts")
        )

        assert gooddata_layouts_dir_exists is False
        assert zip_exists


@mock.patch("utils.gd_api.requests", new_callable=mock_requests)
def test_get_user_data_filters_normal_response(requests):
    api = backup.GDApi("some.host.com", "token")

    response = backup.get_user_data_filters(
        api,
        "workspace",
    )
    assert response == {"userDataFilters": []}


def test_store_user_data_filters():
    user_data_filters = {
        "userDataFilters": [
            {
                "id": "datafilter2",
                "maql": '{label/campaign_channels.category} = "1"',
                "title": "Status filter",
                "user": {"id": "5c867a8a-12af-45bf-8d85-c7d16bedebd1", "type": "user"},
            },
            {
                "id": "datafilter4",
                "maql": '{label/campaign_channels.category} = "1"',
                "title": "Status filter",
                "user": {"id": "5c867a8a-12af-45bf-8d85-c7d16bedebd1", "type": "user"},
            },
        ]
    }
    user_data_filter_folderlocation = Path(
        "tests/data/backup/test_exports/services/wsid1/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid1/user_data_filters"
    )
    backup.store_user_data_filters(
        user_data_filters,
        Path(
            "tests/data/backup/test_exports/services/wsid1/20230713-132759-1_3_1_dev5"
        ),
        "services",
        "wsid1",
    )
    user_data_filter_folder = os.path.isdir(Path(user_data_filter_folderlocation))
    user_data_filter2 = os.path.isfile(
        Path(f"{user_data_filter_folderlocation}/datafilter2.yaml")
    )
    user_data_filter4 = os.path.isfile(
        Path(f"{user_data_filter_folderlocation}/datafilter4.yaml")
    )
    assert user_data_filter_folder
    assert user_data_filter2
    assert user_data_filter4

    count = 0
    for path in os.listdir(user_data_filter_folderlocation):
        if os.path.isfile(os.path.join(user_data_filter_folderlocation, path)):
            count += 1

    assert count == 2

    shutil.rmtree(
        "tests/data/backup/test_exports/services/wsid1/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid1/user_data_filters"
    )


def test_local_storage_export():
    with tempfile.TemporaryDirectory() as tmpdir:
        org_store_location = Path(tmpdir + "/services")
        shutil.copytree(
            Path("tests/data/backup/test_exports/services/"), org_store_location
        )

        backup.LocalStorage.export(
            self=backup.LocalStorage(backup.BackupRestoreConfig(TEST_LOCAL_CONF_PATH)),
            folder=tmpdir,
            org_id="services",
            export_folder="tests/data/local_export",
        )
        local_export_folder_exist = os.path.isdir(
            Path(
                "tests/data/local_export/services/wsid1/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid1/analytics_model"
            )
        )
        local_export_folder2_exist = os.path.isdir(
            Path(
                "tests/data/local_export/services/wsid3/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid3/ldm"
            )
        )

        local_export_folder3_exist = os.path.isdir(
            Path(
                "tests/data/local_export/services/wsid3/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid3/user_data_filters"
            )
        )

        local_export_file_exist = os.path.isfile(
            Path(
                "tests/data/local_export/services/wsid2/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid2/analytics_model/analytical_dashboards/id.yaml"
            )
        )
        assert local_export_folder_exist
        assert local_export_folder2_exist
        assert local_export_folder3_exist
        assert local_export_file_exist
        shutil.rmtree("tests/data/local_export")


def test_file_upload(s3, s3_bucket):
    conf = backup.BackupRestoreConfig(TEST_CONF_PATH)
    s3storage = backup.get_storage("s3")(conf)
    s3storage.export("tests/data/backup/test_exports", "services")
    s3.Object(
        S3_BUCKET,
        "some/s3/backup/path/org_id/services/wsid2/20230713-132759-1_3_1_dev5/gooddata_layouts/services/workspaces/wsid2/analytics_model/filter_contexts/id.yaml",
    ).load()
