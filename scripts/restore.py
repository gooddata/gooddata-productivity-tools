# (C) 2025 GoodData Corporation
import abc
import argparse
import csv
import json
import logging
import os
import sys
import tempfile
import traceback
import zipfile
from pathlib import Path
from typing import Any, Optional, Type, TypeAlias

import boto3
import requests
import yaml
from gooddata_sdk import (
    CatalogDeclarativeAnalytics,
    CatalogDeclarativeAutomation,
    CatalogDeclarativeFilterView,
    CatalogDeclarativeModel,
    GoodDataSdk,
)

BEARER_TKN_PREFIX = "Bearer"
LAYOUTS_DIR = "gooddata_layouts"
AM_DIR = "analytics_model"
LDM_DIR = "ldm"
UDF_DIR = "user_data_filters"

PROFILES_FILE = "profiles.yaml"
PROFILES_DIRECTORY = ".gooddata"
PROFILES_FILE_PATH = Path.home() / PROFILES_DIRECTORY / PROFILES_FILE
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

GDWorkspace: TypeAlias = tuple[CatalogDeclarativeModel, CatalogDeclarativeAnalytics]


class GoodDataRestApiError(Exception):
    """Wrapper for errors occurring from interaction with GD REST API."""


class BackupRestoreError(Exception):
    def __init__(self, cause: str = "Unknown"):
        self.cause = cause


class BackupRestoreConfig:
    def __init__(self, conf_path: str):
        conf = self._load_conf(conf_path)
        self.storage_type = conf["storage_type"]
        self.storage = conf["storage"]

    @staticmethod
    def _load_conf(path: str) -> dict[str, Any]:
        with open(path, "r") as conf:
            return yaml.safe_load(conf)


# TODO: storage logic also defined in backup.py, consider moving to utils
class BackupStorage(abc.ABC):
    """
    Retrieves archive of backed up hierarchical export of workspace declaration.

    Implement this abstract base class for different kinds of storage providers.
    """

    @abc.abstractmethod
    def get_ws_declaration(self, target_path: str, local_target_path: Path) -> None:
        raise NotImplementedError


class S3StorageConfig:
    def __init__(self, storconf: dict[str, Any]):
        self.bucket: str = storconf["bucket"]
        suffix = "/" if not storconf["backup_path"].endswith("/") else ""
        self.backup_path: str = storconf["backup_path"] + suffix
        self.profile = storconf.get("profile", "default")


class S3Storage(BackupStorage):
    """
    Retrieves archive of backed up hierarchical export of workspace declaration from S3.
    """

    def __init__(self, conf: BackupRestoreConfig):
        self._config = S3StorageConfig(conf.storage)
        self._session = self._create_boto_session(self._config.profile)
        self._api = self._session.resource("s3")
        self._bucket = self._api.Bucket(self._config.bucket)
        self._validate_backup_path()

    @staticmethod
    def _create_boto_session(profile: str) -> boto3.Session:
        try:
            return boto3.Session(profile_name=profile)
        except Exception:
            logger.warning(
                'AWS profile "[default]" not found. Trying other fallback methods...'
            )

        return boto3.Session()

    def _validate_backup_path(self) -> None:
        """Validates if backup path exists in the S3 bucket."""
        objects_filter = self._bucket.objects.filter(Prefix=self._config.backup_path)

        try:
            objects = list(objects_filter)
        except Exception as e:
            raise RuntimeError(f"Error raised while validating s3 config. Error: {e}")

        if len(objects) == 0:
            raise RuntimeError("Provided s3 backup_path does not exist. Exiting...")

    def get_ws_declaration(self, s3_target_path: str, local_target_path: Path) -> None:
        """Retrieves workspace declaration from S3 bucket."""
        s3_backup_path = self._config.backup_path
        target_s3_prefix = f"{s3_backup_path}{s3_target_path}"

        objs_found = list(self._bucket.objects.filter(Prefix=target_s3_prefix))

        # Remove the included directory (which equals prefix) on hit
        objs_found = objs_found[1:] if len(objs_found) > 0 else objs_found

        if not objs_found:
            logger.error(f"No target backup found for {target_s3_prefix}.")
            raise BackupRestoreError(f"No target found for {target_s3_prefix}")

        if len(objs_found) > 1:
            logger.warning(
                f"Multiple backups found at {target_s3_prefix}."
                " Continuing with the first one, ignoring the rest..."
            )

        s3_obj = objs_found[0]
        self._bucket.download_file(s3_obj.key, local_target_path)


MaybeResponse: TypeAlias = Optional[requests.Response]


class GDApi:
    # TODO: also defined in utils, consider importing from there
    def __init__(self, host: str, api_token: str, headers: dict[str, Any] = {}):
        self.endpoint = self._handle_endpoint(host)
        self.api_token = api_token
        self.headers = headers
        self.wait_api_time = 10

    @staticmethod
    def _handle_endpoint(host: str) -> str:
        """Ensures that the endpoint URL is properly formatted."""
        return f"{host}api/v1" if host[-1] == "/" else f"{host}/api/v1"

    def put(
        self, path: str, request: dict[str, Any], ok_code: int = 200
    ) -> requests.Response:
        """Sends a PUT request to the GoodData API."""
        kwargs = self._prepare_request(path)
        kwargs["headers"]["Content-Type"] = "application/json"
        kwargs["json"] = request
        logger.debug(f"PUT request: {json.dumps(request)}")
        response = requests.put(**kwargs)
        resolved_response = self._resolve_return_code(
            response, ok_code, kwargs["url"], "RestApi.put"
        )
        assert resolved_response is not None
        return resolved_response

    def _prepare_request(self, path: str, params=None) -> dict[str, Any]:
        """Prepares the request to be sent to the GoodData API."""
        kwargs: dict[str, Any] = {
            "url": f"{self.endpoint}/{path}",
            "headers": self.headers.copy(),
        }
        if params:
            kwargs["params"] = params
        if self.api_token:
            kwargs["headers"]["Authorization"] = f"{BEARER_TKN_PREFIX} {self.api_token}"
        else:
            raise RuntimeError(
                "Token required for authentication against GD API is missing."
            )

        return kwargs

    @staticmethod
    def _resolve_return_code(
        response, ok_code: int, url, method, not_found_code: Optional[int] = None
    ) -> MaybeResponse:
        """Resolves the return code of the response."""
        if response.status_code == ok_code:
            logger.debug(f"{method} to {url} succeeded")
            return response
        if not_found_code and response.status_code == not_found_code:
            logger.debug(f"{method} to {url} failed - target not found")
            return None
        raise GoodDataRestApiError(
            f"{method} to {url} failed - "
            f"response_code={response.status_code} message={response.text}"
        )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "ws_csv", type=Path, help="Path to csv with IDs of GD workspaces to restore."
    )
    parser.add_argument(
        "conf", type=Path, help="Path to backup storage configuration file."
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
        "-v", "--verbose", action="store_true", help="Turns on the debug log output."
    )
    return parser


def read_targets_from_csv(csv_path: str) -> dict[str, str]:
    """Reads the csv file with workspace IDs and paths to backups."""
    # TODO - handling of csv files with and without headers
    # TODO - handling csv files with unsupported structure/schema
    ws_paths: dict[str, str] = {}
    with open(csv_path, "r") as f:
        reader = csv.reader(f, skipinitialspace=True)
        next(reader)  # Skip header
        for row in reader:
            ws_id, ws_path = row

            if ws_paths.get(ws_id) is not None:
                logger.warning(
                    f'Duplicate backup targets for ws_id "{ws_id}" found. '
                    f'Overwriting the target at "{ws_paths[ws_id]}" with "{ws_path}".'
                )

            ws_paths[ws_id] = ws_path

    return ws_paths


def validate_targets(sdk: GoodDataSdk, ws_paths: dict[str, str]) -> None:
    """Validates the targets provided.
    Since for now we don't support restore of deleted backups,
    we can let the user know in advance about unknown IDs.
    """
    ws_list = sdk.catalog_workspace.list_workspaces()
    available_ids = {ws.id for ws in ws_list}
    target_ids = set(ws_paths.keys())

    unknown_ids = target_ids - available_ids
    if unknown_ids:
        logger.error(
            "Unknown IDs specified in the input csv file. "
            f"These will be ignored. The unknown IDs are: {unknown_ids}."
        )

    for ws_id in unknown_ids:
        ws_paths.pop(ws_id)


def get_storage(storage_type: str) -> Type[BackupStorage]:
    """Factory method for creating storage providers."""
    match storage_type:
        case "s3":
            return S3Storage
        case _:
            raise RuntimeError(f'Unsupported storage type "{storage_type}".')


class RestoreWorker:
    def __init__(
        self,
        sdk: GoodDataSdk,
        api: GDApi,
        storage: BackupStorage,
        ws_paths: dict[str, str],
    ):
        self._sdk = sdk
        self._api = api
        self._storage = storage
        self._ws_paths = ws_paths
        self.org_id = sdk.catalog_organization.organization_id

    def _get_ws_declaration(self, ws_path: str, target: Path) -> None:
        """Fetches the backup of workspace declaration from storage provider."""
        try:
            self._storage.get_ws_declaration(ws_path, target)
        except Exception as e:
            logger.error("Failed to fetch restore backup for workspace.")
            raise BackupRestoreError(type(e).__name__)

    @staticmethod
    def _extract_zip_archive(target: Path, tempdir_path: Path) -> None:
        """Extracts the backup from zip archive."""
        try:
            with zipfile.ZipFile(target, "r") as zip_ref:
                zip_ref.extractall(tempdir_path)
        except Exception as e:
            logger.error("Failed to extract backup from zip archive.")
            raise BackupRestoreError(type(e).__name__)

    def _load_workspace_layout(self, src_path: Path) -> GDWorkspace:
        """Loads the workspace layout from the backup."""
        try:
            sdk_catalog = self._sdk.catalog_workspace_content

            ldm = sdk_catalog.load_ldm_from_disk(src_path)
            am = sdk_catalog.load_analytics_model_from_disk(src_path)

            return ldm, am
        except Exception as e:
            logger.error("Failed to load workspace declaration.")
            raise BackupRestoreError(type(e).__name__)

    @staticmethod
    def _convert_udf_files_to_api_body(src_path: Path) -> dict:
        """Converts UDF files to API body."""
        user_data_filters: dict = {"userDataFilters": []}
        user_data_filters_folder = os.path.join(src_path, UDF_DIR)
        for filename in os.listdir(user_data_filters_folder):
            f = os.path.join(user_data_filters_folder, filename)
            with open(f, "r") as file:
                user_data_filter = yaml.safe_load(file)
                user_data_filters["userDataFilters"].append(user_data_filter)

        return user_data_filters

    def _load_user_data_filters(self, src_path: Path) -> dict:
        try:
            return self._convert_udf_files_to_api_body(src_path)
        except Exception as e:
            logger.error("Failed to retrieve contents of user_data_filters folder.")
            raise BackupRestoreError(type(e).__name__)

    def _load_and_put_filter_views(self, ws_id: str, src_path: Path) -> None:
        """Loads and puts filter views into GoodData workspace."""
        filter_views: list[CatalogDeclarativeFilterView] = []
        if not (src_path / "filter_views").exists():
            # Skip if the filter_views directory does not exist
            return

        for file in Path(src_path / "filter_views").iterdir():
            filter_view_content: dict[str, Any] = dict(self._safe_load_yaml(file))
            filter_view: CatalogDeclarativeFilterView = (
                CatalogDeclarativeFilterView.from_dict(filter_view_content)
            )
            filter_views.append(filter_view)

        if filter_views:
            self._sdk.catalog_workspace.put_declarative_filter_views(
                ws_id, filter_views
            )

    def _load_and_post_automations(self, ws_id: str, source_path: Path) -> None:
        """Loads automations from specified json file and creates them in the workspace."""
        # Load automations from JSON
        path_to_json: Path = Path(source_path, "automations", "automations.json")

        if not (source_path.exists() and path_to_json.exists()):
            # Both the folder and the file must exist, otherwise skip
            return

        # Delete all automations from the workspace and restore the automations from the backup.
        self._delete_all_automations(ws_id)

        data: dict = self._load_json(path_to_json)
        automations: list[dict] = data["data"]

        for automation in automations:
            self._post_automation(ws_id, automation)

    def _delete_all_automations(self, ws_id: str) -> None:
        """Deletes all automations in the workspace."""
        automations: list[CatalogDeclarativeAutomation] = (
            self._sdk.catalog_workspace.get_declarative_automations(ws_id)
        )
        for automation in automations:
            requests.delete(
                f"{self._api.endpoint}/entities/workspaces/{ws_id}/automations/{automation.id}",
                headers={
                    "Authorization": f"{BEARER_TKN_PREFIX} {self._api.api_token}",
                    "Content-Type": "application/vnd.gooddata.api+json",
                },
            )

    def _post_automation(self, ws_id: str, automation: dict) -> None:
        """Posts a scheduled export to the workspace."""
        attributes: dict = automation["attributes"]
        relationships: dict = automation["relationships"]
        id: str = automation["id"]

        if attributes.get("schedule"):
            if attributes["schedule"].get("cronDescription"):
                # the cron description attribute is causing a 500 error ("No mapping found...")
                del attributes["schedule"]["cronDescription"]

        response: requests.Response = requests.post(
            f"{self._api.endpoint}/entities/workspaces/{ws_id}/automations",
            headers={
                "Authorization": f"{BEARER_TKN_PREFIX} {self._api.api_token}",
                "Content-Type": "application/vnd.gooddata.api+json",
            },
            data=json.dumps(
                {
                    "data": {
                        "attributes": attributes,
                        "id": id,
                        "type": "automation",
                        "relationships": relationships,
                    }
                },
            ),
        )

        if response.status_code != 201:
            logger.error(
                f"Failed to post automation ({response.status_code}): {response.text}"
            )

    def _safe_load_yaml(self, path: Path) -> Any:
        """Safely loads a yaml file at the given path."""
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _load_json(self, path: Path) -> Any:
        """Loads a json file at the given path."""
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def _check_workspace_is_valid(src_path: Path) -> None:
        """Checks if the workspace layout is valid."""
        # NOTE - this is a weaker, temporary validation.
        # Should be replaced upon SDK version bump.
        if not src_path.exists() or not src_path.is_dir():
            logger.error(
                "Invalid source path found upon backup fetch. "
                f"Got {src_path}. "
                "Check if target zip contains gooddata_layouts directory."
            )
            raise BackupRestoreError("Invalid source path upon load.")

        children = list(src_path.iterdir())
        am_path = src_path / AM_DIR
        ldm_path = src_path / LDM_DIR
        udf_path = src_path / UDF_DIR

        if (
            am_path not in children
            or ldm_path not in children
            or udf_path not in children
        ):
            logger.error(
                "LDM or AM directory missing in the workspace hierarchy. "
                "Check if gooddata_layouts contains "
                f"{AM_DIR}, {LDM_DIR} and {UDF_DIR} directories."
            )
            raise BackupRestoreError("LDM or AM directory missing.")

    def _put_workspace_layout(self, ws_id: str, workspace: GDWorkspace) -> None:
        """Puts the workspace layout into GoodData."""
        ldm, am = workspace
        try:
            sdk_catalog = self._sdk.catalog_workspace_content

            sdk_catalog.put_declarative_ldm(ws_id, ldm)
            sdk_catalog.put_declarative_analytics_model(ws_id, am)

        except Exception as e:
            logger.error("Failed to put workspace into GoodData.")
            raise BackupRestoreError(type(e).__name__)

    def _put_user_data_filters(self, ws_id: str, user_data_filters: dict):
        """Puts the user data filters into GoodData workspace."""
        try:
            self._api.put(
                f"layout/workspaces/{ws_id}/userDataFilters", user_data_filters, 204
            )
        except GoodDataRestApiError as e:
            logger.error(f"Failed to put user data filters into {ws_id}")
            raise BackupRestoreError(type(e).__name__)

    def _restore_backup(self, ws_id: str, tempdir: str) -> None:
        """Restores the backup of a workspace."""
        ws_path = self._ws_paths[ws_id]
        tempdir_path = Path(tempdir)
        zip_target = tempdir_path / f"{LAYOUTS_DIR}.zip"
        src_path = tempdir_path / LAYOUTS_DIR

        try:
            self._get_ws_declaration(ws_path, zip_target)
            self._extract_zip_archive(zip_target, tempdir_path)
            self._check_workspace_is_valid(src_path)
            workspace = self._load_workspace_layout(src_path)
            user_data_filters = self._load_user_data_filters(src_path)
            self._put_workspace_layout(ws_id, workspace)
            self._put_user_data_filters(ws_id, user_data_filters)
            self._load_and_put_filter_views(ws_id, src_path)
            self._load_and_post_automations(ws_id, src_path)
            logger.info(f"Finished backup restore of {ws_id} from {ws_path}.")
        except BackupRestoreError as e:
            logger.error(
                f"Failed to restore backup of {ws_id} from {ws_path}. "
                f"Error caused by {e.cause}."
            )
            trace = traceback.format_exc()
            logger.debug(
                f"Attempt to restore backup raised following error: {e.cause}. "
                f"Traceback:\n{trace}"
            )

    def incremental_restore(self):
        """Restores the backups of workspaces incrementally."""
        for ws_id in self._ws_paths.keys():
            with tempfile.TemporaryDirectory() as tempdir:
                self._restore_backup(ws_id, tempdir)


def create_api_client_from_profile(profile: str, profile_config: Path) -> GDApi:
    """Creates a GoodData API client from a profile."""
    with open(profile_config, "r") as file:
        config = yaml.safe_load(file)

    if profile not in config:
        raise RuntimeError(
            f'Specified profile name "{profile}" not found in "{profile_config}".'
        )

    profile_conf = config[profile]
    hostname, token = profile_conf["host"], profile_conf["token"]
    return GDApi(hostname, token)


def create_client(args: argparse.Namespace) -> tuple[GoodDataSdk, GDApi]:
    """Creates GoodData SDK and API clients."""
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
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to GD profile config."
    )


def main(args):
    """Main entry point of the script."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not os.path.exists(args.ws_csv):
        raise RuntimeError("Invalid path to csv given.")

    if not os.path.exists(args.conf):
        raise RuntimeError("Invalid path to backup storage configuration given.")

    sdk, api = create_client(args)

    conf = BackupRestoreConfig(args.conf)

    storage = get_storage(conf.storage_type)(conf)

    ws_paths = read_targets_from_csv(args.ws_csv)
    validate_targets(sdk, ws_paths)

    restore_worker = RestoreWorker(sdk, api, storage, ws_paths)

    logger.info("Starting incremental backup restore based on target csv file...")
    restore_worker.incremental_restore()


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    main(args)
