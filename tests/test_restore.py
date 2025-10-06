# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
)

import argparse
import json
import logging
import tempfile
from pathlib import Path
from unittest import mock

import boto3
import pytest
from gooddata_sdk.sdk import GoodDataSdk
from moto import mock_aws

from scripts import restore

LOGGER_NAME = "restore.py"
MOCK_DL_TARGET = Path("overlays.zip")
TEST_CONF_PATH = "tests/data/restore/test_conf.yaml"
TEST_CSV_PATH = "tests/data/restore/test.csv"
TEST_LDM_PATH = Path("tests/data/restore/test_ldm_load")
TEST_UDF_PATH = Path("tests/data/restore/test_user_data_filters/")

S3_BACKUP_PATH = "some/s3/backup/path/org_id/"
S3_BUCKET = "some-s3-bucket"


class MockGdWorkspace:
    def __init__(self, id: str) -> None:
        self.id = id


@pytest.fixture
def s3(aws_credentials: None):
    """Yields a mocked S3 client that can be used for testing."""
    with mock_aws():
        yield boto3.resource("s3", region_name="us-east-1")


@pytest.fixture()
def s3_bucket(s3):
    s3.create_bucket(Bucket=S3_BUCKET)
    yield s3.Bucket(S3_BUCKET)


@mock_aws
@pytest.fixture()
def create_backups_in_bucket(s3_bucket):
    def create_backups(ws_ids: list[str], is_e2e: bool = False, suffix: str = "bla"):
        # If used within e2e test, add some suffix to path
        # in order to simulate a more realistic scenario
        path_suffix = f"/{suffix}" if is_e2e else ""

        for ws_id in ws_ids:
            s3_bucket.put_object(Key=f"{S3_BACKUP_PATH}{ws_id}{path_suffix}/")
            s3_bucket.put_object(
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
    restore.create_client(argparse.Namespace())
    client_create_env.assert_called_once_with("hostname", "token")
    client_create_profile.assert_not_called()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch("gooddata_sdk.GoodDataSdk.create_from_profile")
@mock.patch("scripts.restore.create_api_client_from_profile")
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
    restore.create_client(args)
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
        restore.create_client(args)


@pytest.mark.parametrize("csv_path", ["", "bad/path"])
@mock.patch("scripts.restore.create_client")
def test_bad_csv_path_raises_error(_, csv_path):
    args = argparse.Namespace(ws_csv=csv_path, verbose=False)
    with pytest.raises(RuntimeError):
        restore.validate_args(args)


@pytest.mark.parametrize("conf_path", ["", "bad/path"])
@mock.patch("scripts.restore.create_client")
def test_bad_conf_path_raises_error(_, conf_path):
    args = argparse.Namespace(conf=conf_path, ws_csv=".", verbose=False)
    with pytest.raises(RuntimeError):
        restore.validate_args(args)


def test_get_s3_storage():
    s3_storage_type = restore.get_storage("s3")
    assert s3_storage_type == restore.S3Storage


def test_get_unknown_storage_raises_error():
    with pytest.raises(RuntimeError):
        restore.get_storage("unknown_storage")


@mock_aws
def test_s3_storage(mock_boto_session, create_backups_in_bucket):
    create_backups_in_bucket(["ws_id"])
    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    storage = restore.S3Storage(conf)

    with tempfile.TemporaryDirectory() as tempdir:
        target_path = Path(tempdir, MOCK_DL_TARGET)
        storage.get_ws_declaration("ws_id/", target_path)


def test_s3_storage_no_target_only_dir(mock_boto_session, s3_bucket):
    s3_bucket.put_object(Bucket=S3_BUCKET, Key=f"{S3_BACKUP_PATH}/ws_id/")
    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    storage = restore.S3Storage(conf)
    with pytest.raises(restore.BackupRestoreError):
        storage.get_ws_declaration("ws_id/", MOCK_DL_TARGET)


def test_s3_storage_no_target(mock_boto_session, s3_bucket):
    s3_bucket.put_object(Bucket=S3_BUCKET, Key=f"{S3_BACKUP_PATH}/bla/")
    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    storage = restore.S3Storage(conf)
    with pytest.raises(restore.BackupRestoreError):
        storage.get_ws_declaration("bad_target/", MOCK_DL_TARGET)


def test_init_ldm_with_ws_data_filter_cols():
    # Regression test - this doesn't work for sdk 1.3 and lesser
    sdk = GoodDataSdk.create("", "")
    model = sdk.catalog_workspace_content.load_ldm_from_disk(TEST_LDM_PATH)
    assert model.ldm is not None
    assert len(model.ldm.datasets) == 1


def test_validate_targets(caplog):
    sdk = mock.Mock()
    sdk.catalog_workspace.list_workspaces.return_value = [
        MockGdWorkspace(id=f"ws_id_{i}") for i in range(4)
    ]

    ws_paths = {f"ws_id_{i}": "" for i in range(2, 6)}

    restore.validate_targets(sdk, ws_paths)

    assert len(caplog.record_tuples) == 1
    logger, level, msg = caplog.record_tuples[0]
    assert logger == LOGGER_NAME
    assert level == logging.ERROR
    for i in range(4, 6):
        assert f"ws_id_{i}" in msg


def test_bad_s3_bucket_raises_error(s3):
    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    with pytest.raises(RuntimeError):
        restore.S3Storage(conf)


def test_bad_s3_path_raises_error(s3_bucket):
    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    with pytest.raises(RuntimeError):
        restore.S3Storage(conf)


@mock.patch("scripts.restore.zipfile.ZipFile")
def test_restore_empty_ws(zipfile):
    def create_empty_ws(tempdir):
        os.mkdir(tempdir / "gooddata_layouts")
        os.mkdir(tempdir / "gooddata_layouts" / "ldm")
        os.mkdir(tempdir / "gooddata_layouts" / "analytics_model")
        os.mkdir(tempdir / "gooddata_layouts" / "user_data_filters")
        os.mkdir(tempdir / "gooddata_layouts" / "filter_views")
        os.mkdir(tempdir / "gooddata_layouts" / "automations")

    zipfile.return_value.__enter__.return_value.extractall = create_empty_ws
    sdk = mock.Mock()
    sdk.catalog_workspace.get_declarative_automations.return_value = []
    api = mock.Mock()
    storage = mock.Mock()
    ws_paths = {"ws_id": "some/ws/path"}

    worker = restore.RestoreWorker(sdk, api, storage, ws_paths)
    worker.incremental_restore()

    sdk.catalog_workspace_content.put_declarative_ldm.assert_called_once_with(
        "ws_id", mock.ANY
    )
    sdk.catalog_workspace_content.put_declarative_analytics_model.assert_called_once_with(
        "ws_id", mock.ANY
    )


@mock.patch("scripts.restore.zipfile.ZipFile")
def test_invalid_ws_on_disk_skipped(zipfile):
    def create_invalid_ws(tempdir):
        # Missing AM directory
        os.mkdir(tempdir / "gooddata_layouts")
        os.mkdir(tempdir / "gooddata_layouts" / "ldm")

    zipfile.return_value.__enter__.return_value.extractall = create_invalid_ws

    sdk = mock.Mock()
    api = mock.Mock()
    storage = mock.Mock()
    ws_paths = {"ws_id": "some/ws/path"}

    worker = restore.RestoreWorker(sdk, api, storage, ws_paths)
    worker.incremental_restore()

    sdk.catalog_workspace_content.put_declarative_ldm.assert_not_called()
    sdk.catalog_workspace_content.put_declarative_analytics_model.assert_not_called()


# e2e tests


def prepare_catalog_mocks():
    ldm = mock.Mock()
    ldm.to_dict.return_value = {"ldm": {"foo": "bar"}}
    ws_catalog = mock.MagicMock()
    return ldm, ws_catalog


# No longer need create_backups_in_bucket or mock_boto_session for this specific test
@mock.patch("scripts.restore.RestoreWorker._load_user_data_filters")
@mock.patch("scripts.restore.zipfile")
def test_incremental_restore(zipfile_mock, _, mocker):
    """
    Tests the RestoreWorker's incremental logic by providing a mock S3Storage object.
    """
    # Prepare sdk-related mocks (this is your existing setup)
    ldm, ws_catalog = prepare_catalog_mocks()
    ws_catalog.load_ldm_from_disk.return_value = ldm
    sdk = mock.Mock()
    sdk.catalog_workspace.get_declarative_automations.return_value = []
    api = mock.Mock()
    sdk.catalog_workspace_content = ws_catalog

    # 1. Create a mock of an S3Storage INSTANCE using mocker
    # This mock will behave like an S3Storage object, with the same methods.
    mock_storage = mocker.create_autospec(restore.S3Storage, instance=True)

    # 2. Define the behavior of the mock. The worker calls `get_ws_declaration`.
    # We can just tell it to do nothing, because the next step in the worker
    # (`_extract_zip_archive`) uses `zipfile.ZipFile`, which is already
    # mocked by the decorator on this test.
    mock_storage.get_ws_declaration.return_value = None

    # 3. Inject the mock dependency into the worker
    ws_paths = {"ws_id_1": "ws_id_1", "ws_id_2": "ws_id_2"}
    worker = restore.RestoreWorker(sdk, api, mock_storage, ws_paths)

    # 4. Run the code under test
    with mock.patch("scripts.restore.RestoreWorker._check_workspace_is_valid"):
        worker.incremental_restore()

    # 5. Assert that the worker interacted with our mock as expected
    # This ensures the worker is calling the storage logic correctly.
    mock_storage.get_ws_declaration.assert_has_calls(
        [
            mock.call("ws_id_1", mock.ANY),
            mock.call("ws_id_2", mock.ANY),
        ]
    )

    ws_catalog.assert_has_calls(
        [
            mock.call.load_ldm_from_disk(mock.ANY),
            mock.call.load_analytics_model_from_disk(mock.ANY),
        ]
    )
    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_1", ldm),
            mock.call.put_declarative_analytics_model("ws_id_1", mock.ANY),
        ]
    )
    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_2", ldm),
            mock.call.put_declarative_analytics_model("ws_id_2", mock.ANY),
        ]
    )


@mock.patch("scripts.restore.RestoreWorker._load_user_data_filters")
@mock.patch("scripts.restore.zipfile")
def test_incremental_restore_different_ws_source(
    _, _load_user_data_filters, create_backups_in_bucket, mock_boto_session
):
    # Prepare sdk-related mocks
    ldm, ws_catalog = prepare_catalog_mocks()
    ws_catalog.load_ldm_from_disk.return_value = ldm
    sdk = mock.Mock()
    sdk.catalog_workspace_content = ws_catalog
    sdk.catalog_workspace.get_declarative_automations.return_value = []

    api = mock.Mock()

    create_backups_in_bucket(["ws_id_1"])

    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    storage = restore.S3Storage(conf)

    # 1 -> 1; 2 -> 1
    ws_paths = {"ws_id_1": "ws_id_1", "ws_id_2": "ws_id_1"}

    worker = restore.RestoreWorker(sdk, api, storage, ws_paths)
    with mock.patch("scripts.restore.RestoreWorker._check_workspace_is_valid") as _:
        worker.incremental_restore()

    ws_catalog.assert_has_calls(
        [
            mock.call.load_ldm_from_disk(mock.ANY),
            mock.call.load_analytics_model_from_disk(mock.ANY),
        ]
    )
    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_1", ldm),
            mock.call.put_declarative_analytics_model("ws_id_1", mock.ANY),
        ]
    )
    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_2", ldm),
            mock.call.put_declarative_analytics_model("ws_id_2", mock.ANY),
        ]
    )


@mock.patch("scripts.restore.RestoreWorker._load_user_data_filters")
@mock.patch("scripts.restore.zipfile")
def test_incremental_restore_one_succeeds_one_fails(
    _, _load_user_data_filters, create_backups_in_bucket, mock_boto_session
):
    # Prepare sdk-related mocks
    ldm, ws_catalog = prepare_catalog_mocks()
    # One load succeeds, one fails...
    ws_catalog.load_ldm_from_disk.side_effect = [ldm, Exception()]
    sdk = mock.Mock()
    sdk.catalog_workspace_content = ws_catalog
    sdk.catalog_workspace.get_declarative_automations.return_value = []

    api = mock.Mock()

    create_backups_in_bucket(["ws_id_1", "ws_id_2"])

    conf = restore.BackupRestoreConfig(TEST_CONF_PATH)
    storage = restore.S3Storage(conf)

    ws_paths = {"ws_id_1": "ws_id_1", "ws_id_2": "ws_id_1"}

    worker = restore.RestoreWorker(sdk, api, storage, ws_paths)
    with mock.patch("scripts.restore.RestoreWorker._check_workspace_is_valid") as _:
        worker.incremental_restore()

    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_1", ldm),
            mock.call.put_declarative_analytics_model("ws_id_1", mock.ANY),
        ]
    )
    # Ensure that despite the failure on ws_id_2 restore, we don't put anything
    assert_not_called_with(ws_catalog.put_declarative_ldm, "ws_id_2", mock.ANY)
    assert_not_called_with(
        ws_catalog.put_declarative_analytics_model, "ws_id_2", mock.ANY
    )


def test_load_user_data_filters():
    sdk = mock.Mock()
    api = mock.Mock()
    storage = mock.Mock()
    ws_paths = mock.Mock()

    worker = restore.RestoreWorker(sdk, api, storage, ws_paths)
    user_data_filters = worker._load_user_data_filters(TEST_UDF_PATH)
    user_data_filters_expected = {
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

    # Convert both the expected and actual filter lists to sorted lists of their JSON string representations
    sorted_user_data_filters = sorted(
        json.dumps(d, sort_keys=True) for d in user_data_filters["userDataFilters"]
    )
    sorted_user_data_filters_expected = sorted(
        json.dumps(d, sort_keys=True)
        for d in user_data_filters_expected["userDataFilters"]
    )

    assert sorted_user_data_filters == sorted_user_data_filters_expected


@mock.patch("scripts.restore.create_client")
@mock.patch("scripts.restore.RestoreWorker._load_user_data_filters")
@mock.patch("scripts.restore.zipfile")
@mock.patch("scripts.restore.create_parser")
def test_e2e(
    create_parser,
    zipfile_mock,
    _load_user_data_filters,
    create_client,
    create_backups_in_bucket,
    mock_boto_session,
):
    conf_path = TEST_CONF_PATH
    csv_path = TEST_CSV_PATH
    args = argparse.Namespace(conf=conf_path, ws_csv=csv_path, verbose=False)

    # Prepare sdk-related mocks
    ldm, ws_catalog = prepare_catalog_mocks()
    # On load_ldm_from_disk: Success, Fail, Success
    ws_catalog.load_ldm_from_disk.side_effect = [ldm, Exception(), ldm]
    sdk = mock.Mock()
    sdk.catalog_workspace.get_declarative_automations.return_value = []
    sdk.catalog_workspace_content = ws_catalog
    sdk.catalog_workspace.list_workspaces.return_value = [
        MockGdWorkspace(id=f"ws_id_{i}") for i in range(1, 4)
    ]

    api = mock.Mock()

    create_client.return_value = sdk, api

    create_backups_in_bucket(["ws_id_1", "ws_id_2"], is_e2e=True)

    # Mock parser and its parse_args to return our args namespace
    parser_mock = mock.Mock()
    parser_mock.parse_args.return_value = args
    create_parser.return_value = parser_mock

    with mock.patch("scripts.restore.RestoreWorker._check_workspace_is_valid") as _:
        restore.restore()

    assert_not_called_with(
        ws_catalog.put_declarative_ldm, "thiswsdoesnotexist", mock.ANY
    )
    assert_not_called_with(
        ws_catalog.put_declarative_analytics_model, "thiswsdoesnotexist", mock.ANY
    )

    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_1", ldm),
            mock.call.put_declarative_analytics_model("ws_id_1", mock.ANY),
        ]
    )

    # Ensure that in case of the failure on ws_id_2 restore, we don't PUT anything
    assert_not_called_with(ws_catalog.put_declarative_ldm, "ws_id_2", mock.ANY)
    assert_not_called_with(
        ws_catalog.put_declarative_analytics_model, "ws_id_2", mock.ANY
    )

    ws_catalog.assert_has_calls(
        [
            mock.call.put_declarative_ldm("ws_id_3", ldm),
            mock.call.put_declarative_analytics_model("ws_id_3", mock.ANY),
        ]
    )
