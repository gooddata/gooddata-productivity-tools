# (C) 2025 GoodData Corporation
import abc
import argparse
import datetime
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Type

import boto3  # type: ignore[import]
import requests
import yaml
from gooddata_api_client.exceptions import NotFoundException
from gooddata_sdk import __version__ as sdk_version  # type: ignore[import]
from gooddata_sdk.sdk import GoodDataSdk  # type: ignore[import]
from utils.backup_utils.input_loader import InputLoader  # type: ignore[import]
from utils.gd_api import (  # type: ignore[import]
    BEARER_TKN_PREFIX,
    GDApi,
    GoodDataRestApiError,
)
from utils.logger import logger  # type: ignore[import]

TIMESTAMP_SDK_FOLDER = (
    str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    + "-"
    + sdk_version.replace(".", "_")
)

PROFILES_FILE = "profiles.yaml"
PROFILES_DIRECTORY = ".gooddata"
PROFILES_FILE_PATH = Path.home() / PROFILES_DIRECTORY / PROFILES_FILE


LAYOUTS_DIR = "gooddata_layouts"
LDM_DIR = "ldm"

API_PAGE_SIZE = 100


# TODO: consider moving storage related logic to a separate module and reuse it in restore
class BackupRestoreConfig:
    def __init__(self, conf_path: str):
        with open(conf_path, "r") as stream:
            conf = yaml.safe_load(stream)
            self.storage_type = conf["storage_type"]
            self.storage = conf["storage"]
            self.api_page_size = conf.get("api_page_size", API_PAGE_SIZE)


class BackupStorage(abc.ABC):
    def __init__(self, conf: BackupRestoreConfig):
        return

    @abc.abstractmethod
    def export(self, folder, org_id):
        """Exports the content of the folder to the storage."""
        raise NotImplementedError


class S3Storage(BackupStorage):
    def __init__(self, conf: BackupRestoreConfig):
        self._config = conf.storage
        self._profile = self._config.get("profile", "default")
        self._session = self._create_boto_session(self._profile)
        self._api = self._session.resource("s3")
        self._bucket = self._api.Bucket(self._config["bucket"])  # type: ignore [missing library stubs]
        suffix = "/" if not self._config["backup_path"].endswith("/") else ""
        self._backup_path = self._config["backup_path"] + suffix

    @staticmethod
    def _create_boto_session(profile: str) -> boto3.Session:
        try:
            return boto3.Session(profile_name=profile)
        except Exception:
            logger.warning(
                'AWS profile "[default]" not found. Trying other fallback methods...'
            )

        return boto3.Session()

    def export(self, folder, org_id) -> None:
        """Uploads the content of the folder to S3 as backup."""
        storage_path = self._config["bucket"] + "/" + self._backup_path
        logger.info(f"Uploading {org_id} to {storage_path}")
        folder = folder + "/" + org_id
        for subdir, dirs, files in os.walk(folder):
            full_path = os.path.join(subdir)
            export_path = (
                self._backup_path + org_id + "/" + full_path[len(folder) + 1 :] + "/"
            )
            self._bucket.put_object(Key=export_path)

            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, "rb") as data:
                    export_path = (
                        self._backup_path + org_id + "/" + full_path[len(folder) + 1 :]
                    )
                    self._bucket.put_object(Key=export_path, Body=data)


class LocalStorage(BackupStorage):
    def __init__(self, conf: BackupRestoreConfig):
        return

    def export(self, folder, org_id, export_folder="local_backups"):
        """Copies the content of the folder to local storage as backup."""
        logger.info(f"Saving {org_id} to local storage")
        shutil.copytree(
            Path(folder), Path(Path.cwd(), export_folder), dirs_exist_ok=True
        )


def create_api_client_from_profile(profile: str, profile_config: Path) -> GDApi:
    """Creates a GoodData API client from the specified profile."""
    with open(profile_config, "r") as file:
        config = yaml.safe_load(file)

    if profile not in config:
        raise RuntimeError(
            f'Specified profile name "{profile}" not found in "{profile_config}".'
        )

    profile_conf = config[profile]
    hostname, token = profile_conf["host"], profile_conf["token"]
    return GDApi(hostname, token)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "ws_csv",
        help="Path to csv with IDs of GD workspaces to backup.",
        type=Path,
        nargs="?",
    )
    parser.add_argument(
        "conf", help="Path to backup storage configuration file.", type=Path
    )
    parser.add_argument(
        "-p",
        "--profile-config",
        type=Path,
        default=PROFILES_FILE_PATH,
        help="Optional path to GoodData profile config. "
        f'If no path is provided, "{PROFILES_FILE_PATH}" is used.',
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="default",
        help='GoodData profile to use. If not profile is provided, "default" is used.',
    )
    parser.add_argument(
        "-t",
        "--input-type",
        type=str,
        choices=["list-of-workspaces", "list-of-parents", "entire-organization"],
        default="list-of-workspaces",
        help="Type of input to use as the base of the backup. If not provided, `list-of-workspaces` is used as default.",
    )

    return parser


def write_to_yaml(folder, source):
    """Writes the source to a YAML file."""
    with open(folder, "w") as outfile:
        yaml.dump(source, outfile)


def get_storage(storage_type: str) -> Type[BackupStorage]:
    """Returns the storage class based on the storage type."""
    match storage_type:
        case "s3":
            logger.info("Storage type set to S3.")
            return S3Storage
        case "local":
            logger.info("Storage type set to local storage.")
            return LocalStorage
        case _:
            raise RuntimeError(f'Unsupported storage type "{storage_type}".')


def get_user_data_filters(api: GDApi, ws_id: str) -> dict | None:
    """Returns the user data filters for the specified workspace."""
    try:
        user_data_filters = api.get(f"/layout/workspaces/{ws_id}/userDataFilters", None)
        if user_data_filters:
            return user_data_filters.json()
    except GoodDataRestApiError as e:
        logger.error(f"UDF call for {ws_id} returned error: {e}")
    return None


def store_user_data_filters(
    user_data_filters: dict, export_path: Path, org_id: str, ws_id: str
):
    """Stores the user data filters in the specified export path."""
    os.mkdir(
        os.path.join(
            export_path,
            "gooddata_layouts",
            org_id,
            "workspaces",
            ws_id,
            "user_data_filters",
        )
    )

    for filter in user_data_filters["userDataFilters"]:
        udf_file_path = os.path.join(
            export_path,
            "gooddata_layouts",
            org_id,
            "workspaces",
            ws_id,
            "user_data_filters",
            filter["id"] + ".yaml",
        )
        write_to_yaml(udf_file_path, filter)


def move_folder(source: Path, destination: Path) -> None:
    """Moves the source folder to the destination."""
    shutil.move(source, destination)


def get_automations_from_api(api: GDApi, ws_id: str) -> Any:
    """Returns automations for the workspace as JSON."""
    response: requests.Response = requests.get(
        f"{api.endpoint}/entities/workspaces/{ws_id}/automations?include=ALL",
        headers={
            "Authorization": f"{BEARER_TKN_PREFIX} {api.api_token}",
            "Content-Type": "application/vnd.gooddata.api+json",
        },
    )
    content: Any = response.json()

    return content


def store_automations(api: GDApi, export_path: Path, org_id: str, ws_id: str) -> None:
    """Stores the automations in the specified export path."""
    # Get the automations from the API
    automations: Any = get_automations_from_api(api, ws_id)

    automations_folder_path: Path = Path(
        export_path, "gooddata_layouts", org_id, "workspaces", ws_id, "automations"
    )

    automations_file_path: Path = Path(automations_folder_path, "automations.json")

    os.mkdir(automations_folder_path)

    # Store the automations in a JSON file
    if len(automations["data"]) > 0:
        with open(automations_file_path, "w") as f:
            json.dump(automations, f)


def store_declarative_filter_views(
    sdk: GoodDataSdk, export_path: Path, org_id: str, ws_id: str
) -> None:
    """Stores the filter views in the specified export path."""
    # Get the filter views YAML files from the API
    sdk.catalog_workspace.store_declarative_filter_views(ws_id, export_path)

    # Move filter views to the subfolder containing analytics model
    move_folder(
        Path(export_path, "gooddata_layouts", org_id, "filter_views"),
        Path(
            export_path,
            "gooddata_layouts",
            org_id,
            "workspaces",
            ws_id,
            "filter_views",
        ),
    )


def get_workspace_export(
    sdk: GoodDataSdk,
    api: GDApi,
    local_target_path: str,
    org_id: str,
    workspaces_to_export: list[str],
) -> None:
    """
    Iterate over all workspaces in the workspaces_to_export list and store
    their declarative_workspace and their respective user data filters.
    """
    exported = False
    for ws_id in workspaces_to_export:
        export_path = Path(local_target_path, org_id, ws_id, TIMESTAMP_SDK_FOLDER)

        user_data_filters = get_user_data_filters(api, ws_id)
        if not user_data_filters:
            logger.error(
                f"Skipping backup of {ws_id} - user data filters returned None."
            )
            logger.error(f"Check if {ws_id} exists and the API is functional")
            continue

        try:
            sdk.catalog_workspace.store_declarative_workspace(ws_id, export_path)
            store_declarative_filter_views(sdk, export_path, org_id, ws_id)
            store_automations(api, export_path, org_id, ws_id)

            store_user_data_filters(user_data_filters, export_path, org_id, ws_id)
            logger.info(f"Stored export for {ws_id}")
            exported = True
        except NotFoundException:
            logger.error(f"Workspace {ws_id} does not exist. Skipping.")

    if not exported:
        raise RuntimeError(
            "None of the workspaces were exported."
            "Check source file and their existence."
        )


def archive_gooddata_layouts_to_zip(folder: str) -> None:
    """Archives the gooddata_layouts directory to a zip file."""
    target_subdir = ""
    for subdir, dirs, files in os.walk(folder):
        if LAYOUTS_DIR in dirs:
            target_subdir = os.path.join(subdir, dirs[0])
        if LDM_DIR in dirs:
            inner_layouts_dir = subdir + "/gooddata_layouts"
            os.mkdir(inner_layouts_dir)
            for dir in dirs:
                shutil.move(os.path.join(subdir, dir), os.path.join(inner_layouts_dir))
            shutil.make_archive(target_subdir, "zip", subdir)
            shutil.rmtree(target_subdir)


def create_client(args: argparse.Namespace) -> tuple[GoodDataSdk, GDApi]:
    """Creates a GoodData client."""
    gdc_auth_token = os.environ.get("GDC_AUTH_TOKEN")
    gdc_hostname = os.environ.get("GDC_HOSTNAME")

    if gdc_hostname and gdc_auth_token:
        logger.info("Using GDC_HOSTNAME and GDC_AUTH_TOKEN envvars.")
        sdk = GoodDataSdk.create(gdc_hostname, gdc_auth_token)
        api = GDApi(gdc_hostname, gdc_auth_token)
        return sdk, api

    profile_config, profile = args.profile_config, args.profile
    if os.path.exists(profile_config):
        logger.info(f"Using GoodData profile {profile} sourced from {profile_config}.")
        sdk = GoodDataSdk.create_from_profile(profile, profile_config)
        api = create_api_client_from_profile(profile, profile_config)
        return sdk, api

    raise RuntimeError(
        "No GoodData credentials provided. Please export required ENVVARS "
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to profile config."
    )


def validate_args(args: argparse.Namespace) -> None:
    """Validates the arguments provided."""
    if args.input_type != "entire-organization":
        if not args.ws_csv:
            raise RuntimeError("Path to csv with workspace IDs is required.")
        if not os.path.exists(args.ws_csv):
            raise RuntimeError("Invalid path to csv given.")

    if not os.path.exists(args.conf):
        raise RuntimeError("Invalid path to backup storage configuration given.")

    if args.input_type == "entire-organization" and args.ws_csv:
        logger.warning(
            "Input type is set to 'entire-organization', but a CSV file is provided. "
            "The CSV file will be ignored."
        )


def main(args: argparse.Namespace) -> None:
    """Main function for the backup script."""
    sdk, api = create_client(args)

    org_id: str = sdk.catalog_organization.organization_id

    conf: BackupRestoreConfig = BackupRestoreConfig(args.conf)

    storage_class: Type[BackupStorage] = get_storage(conf.storage_type)
    storage: BackupStorage = storage_class(conf)

    loader = InputLoader(api, conf.api_page_size)
    workspaces_to_export: list[str] = loader.get_ids_to_backup(
        args.input_type, args.ws_csv
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        get_workspace_export(sdk, api, tmpdir, org_id, workspaces_to_export)

        archive_gooddata_layouts_to_zip(str(Path(tmpdir, org_id)))

        storage.export(tmpdir, org_id)


if __name__ == "__main__":
    parser: argparse.ArgumentParser = create_parser()
    args: argparse.Namespace = parser.parse_args()
    validate_args(args)
    main(args)
