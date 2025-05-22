# (C) 2025 GoodData Corporation
import argparse
import csv
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional, TypeAlias

import gooddata_sdk as gd_sdk
from gooddata_api_client.exceptions import NotFoundException

USER_TYPE = "user"
USER_GROUP_TYPE = "userGroup"

PROFILES_FILE = "profiles.yaml"
PROFILES_DIRECTORY = ".gooddata"
PROFILES_FILE_PATH = Path.home() / PROFILES_DIRECTORY / PROFILES_FILE
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Management of workspace permissions.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Turns on the debug log output."
    )
    parser.add_argument(
        "perm_csv",
        type=Path,
        help=(
            "Path to (comma-delimited) csv with user/userGroup "
            "to workspace permission pairs."
        ),
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the user_csv.",
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
    return parser


TargetsPermissionDict: TypeAlias = dict[str, dict[str, bool]]


@dataclass(frozen=True)
class WSPermission:
    permission: str
    ws_id: str
    id: str
    type: str
    is_active: bool

    @classmethod
    def from_csv_row(cls, row: list[Any]) -> "WSPermission":
        """Construct WSPermission data object from csv row input."""
        user_id, user_group_id, ws_id, permission, is_active = row

        id = user_id if user_id else user_group_id
        target_type = USER_TYPE if user_id else USER_GROUP_TYPE

        return WSPermission(
            permission=permission,
            ws_id=ws_id,
            id=id,
            type=target_type,
            is_active=str(is_active).lower() == "true",
        )


@dataclass
class WSPermissionDeclaration:
    users: TargetsPermissionDict
    user_groups: TargetsPermissionDict

    @classmethod
    def from_sdk_api(
        cls, declaration: gd_sdk.CatalogDeclarativeWorkspacePermissions
    ) -> "WSPermissionDeclaration":
        """
        Constructs an WSPermissionDeclaration instance
        from GoodData SDK CatalogDeclarativeWorkspacePermissions.
        """
        users: TargetsPermissionDict = {}
        user_groups: TargetsPermissionDict = {}

        for permission in declaration.permissions:
            permission_type, id = permission.assignee.type, permission.assignee.id
            target_dict = users if permission_type == USER_TYPE else user_groups

            id_permissions = target_dict.get(id)
            if not id_permissions:
                target_dict[id] = dict()

            target_dict[id][permission.name] = True

        return WSPermissionDeclaration(users, user_groups)

    @staticmethod
    def _construct_upstream_permission(
        permission: str, assignee: gd_sdk.CatalogAssigneeIdentifier
    ) -> gd_sdk.CatalogDeclarativeSingleWorkspacePermission | None:
        """Constructs single permission declaration for the SDK API."""
        try:
            return gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
                name=permission, assignee=assignee
            )
        except Exception as e:
            logger.error(
                "Failed to construct SDK declaration "
                f'for type={assignee.type} id={assignee.id}. Error: "{e}".'
            )
        return None

    def _permissions_for_target(
        self, permissions: dict[str, bool], assignee: gd_sdk.CatalogAssigneeIdentifier
    ) -> Iterator[gd_sdk.CatalogDeclarativeSingleWorkspacePermission]:
        """Constructs permission declarations for a single target."""
        for permission, is_active in permissions.items():
            if not is_active:
                continue
            declaration = self._construct_upstream_permission(permission, assignee)
            if not declaration:
                continue
            yield declaration

    def to_sdk_api(self) -> gd_sdk.CatalogDeclarativeWorkspacePermissions:
        """
        Constructs the GoodData SDK CatalogDeclarativeWorkspacePermissions
        object from the WSPermissionDeclaration instance.
        """
        permission_declarations: list[
            gd_sdk.CatalogDeclarativeSingleWorkspacePermission
        ] = []

        for user_id, permissions in self.users.items():
            assignee = gd_sdk.CatalogAssigneeIdentifier(id=user_id, type=USER_TYPE)
            for declaration in self._permissions_for_target(permissions, assignee):
                permission_declarations.append(declaration)

        for ug_id, permissions in self.user_groups.items():
            assignee = gd_sdk.CatalogAssigneeIdentifier(id=ug_id, type=USER_GROUP_TYPE)
            for declaration in self._permissions_for_target(permissions, assignee):
                permission_declarations.append(declaration)

        return gd_sdk.CatalogDeclarativeWorkspacePermissions(
            permissions=permission_declarations
        )

    def add_permission(self, permission: WSPermission):
        """
        Adds WSPermission object into respective field within the instance.
        Handles duplicate permissions and different combinations of input
        and upstream is_active permission states.
        """
        target_dict = self.users if permission.type == USER_TYPE else self.user_groups

        if permission.id not in target_dict:
            target_dict[permission.id] = {}

        is_active = permission.is_active
        target_permissions = target_dict[permission.id]
        permission_value = permission.permission

        if permission_value not in target_permissions:
            target_permissions[permission_value] = is_active
        elif not is_active and target_permissions[permission_value] is True:
            logger.warning(
                "isActive=False provided after True has been specificed "
                f'for the same input. Skipping "{permission}".'
            )
        elif is_active and target_permissions[permission_value] is False:
            logger.warning(
                "isActive=True provided after False has been specified "
                f'for the same input. Overwriting "{permission}".'
            )
            target_permissions[permission_value] = is_active

    def upsert(self, other: "WSPermissionDeclaration"):
        """
        Modifies the owner object by merging with the other.
        Keeps the unmodified users/userGroups untouched.
        If some user/userGroup is modified, it gets overwritten with permissions
        defined in the input.
        """
        for user_id, permissions in other.users.items():
            self.users[user_id] = permissions

        for ug_id, permissions in other.user_groups.items():
            self.user_groups[ug_id] = permissions


WSPermissionsDeclarations: TypeAlias = dict[str, WSPermissionDeclaration]


class InvalidPermissionException(Exception):
    pass


class WSPermissionManager:
    def __init__(self, sdk: gd_sdk.GoodDataSdk):
        self._sdk = sdk

    def _get_ws_declaration(self, ws_id: str) -> WSPermissionDeclaration:
        users: TargetsPermissionDict = {}
        user_groups: TargetsPermissionDict = {}

        upstream_declaration = self._sdk.catalog_permission.get_declarative_permissions(
            ws_id
        )

        for permission in upstream_declaration.permissions:
            permission_type, id = permission.assignee.type, permission.assignee.id
            target_dict = users if permission_type == USER_TYPE else user_groups

            id_permissions = target_dict.get(id)
            if not id_permissions:
                target_dict[id] = dict()

            target_dict[id][permission.name] = True

        return WSPermissionDeclaration(users, user_groups)

    def _get_upstream_declaration(
        self, ws_id: str
    ) -> Optional[WSPermissionDeclaration]:
        """Retrieves upstream permission declaration for a workspace."""
        try:
            declaration = self._sdk.catalog_permission.get_declarative_permissions(
                ws_id
            )
            return WSPermissionDeclaration.from_sdk_api(declaration)
        except NotFoundException as e:
            logger.error(f"Workspace with id {ws_id} doesn't exist. Error: {e}")
        except Exception as e:
            logger.error(
                "Some error occured while retrieving workspace "
                f'permission declaration for workspace "{ws_id}". Error: "{e}"'
            )
        return None

    def _get_upstream_declarations(
        self, input_ws_ids: list[str]
    ) -> WSPermissionsDeclarations:
        """Retrieves upstream permission declarations for a list of workspaces."""
        ws_dict: WSPermissionsDeclarations = {}
        for ws_id in input_ws_ids:
            declaration = self._get_upstream_declaration(ws_id)
            if declaration:
                ws_dict[ws_id] = declaration
        return ws_dict

    @staticmethod
    def _construct_declarations(
        permissions: list[WSPermission],
    ) -> WSPermissionsDeclarations:
        """Constructs workspace permission declarations from the input permissions."""
        ws_dict: WSPermissionsDeclarations = {}
        for permission in permissions:
            ws_id = permission.ws_id

            if ws_id not in ws_dict:
                ws_dict[ws_id] = WSPermissionDeclaration({}, {})

            ws_dict[ws_id].add_permission(permission)
        return ws_dict

    def _check_user_exists(self, user_id: str):
        """Checks if user with provided ID exists."""
        try:
            self._sdk.catalog_user.get_user(user_id)
        except NotFoundException:
            raise InvalidPermissionException("Provided user ID does not exist.")

    def _check_user_group_exists(self, ug_id: str):
        """Checks if user group with provided ID exists."""
        try:
            self._sdk.catalog_user.get_user_group(ug_id)
        except NotFoundException:
            raise InvalidPermissionException("Provided user group ID does not exist.")

    def _validate_permission(self, permission: WSPermission):
        """Validates if the permission is correctly defined."""
        if permission.type == USER_TYPE:
            self._check_user_exists(permission.id)
        else:
            self._check_user_group_exists(permission.id)

    def _filter_invalid_permissions(
        self, permissions: list[WSPermission]
    ) -> list[WSPermission]:
        """Filters out invalid permissions from the input list."""
        valid_permissions: list[WSPermission] = []
        for permission in permissions:
            try:
                self._validate_permission(permission)
            except InvalidPermissionException as e:
                logger.error(
                    f'Invalid permission defined. Skipping "{permission}. Error: "{e}".'
                )
                continue
            valid_permissions.append(permission)
        return valid_permissions

    def manage_permissions(self, permissions: list[WSPermission]):
        """Manages permissions for a list of workspaces.
        Modify upstream workspace declarations for each input workspace and skip non-existent ws_ids
        """
        logger.info(
            f"Starting permission management run of {len(permissions)} permissions..."
        )
        valid_permissions = self._filter_invalid_permissions(permissions)

        input_declarations = self._construct_declarations(valid_permissions)

        input_ws_ids = list(input_declarations.keys())
        upstream_declarations = self._get_upstream_declarations(input_ws_ids)

        for ws_id, declaration in input_declarations.items():
            if ws_id not in upstream_declarations:
                continue

            upstream_declarations[ws_id].upsert(declaration)

            ws_permissions = upstream_declarations[ws_id].to_sdk_api()

            logger.info(f'Putting declarative permissions for workspace "{ws_id}".')
            try:
                self._sdk.catalog_permission.put_declarative_permissions(
                    ws_id, ws_permissions
                )
            except Exception as e:
                logger.error(
                    "Failed to update declarative workspace "
                    f'permissions for workspace "{ws_id}". Error: {e}'
                )
        logger.info("Finished permission management run.")


def csv_row_is_valid(row: list[Any]) -> bool:
    """Validates if the csv row is correctly defined."""
    try:
        user_id, user_group_id, ws_id, permission, is_active = row
    except Exception as e:
        logger.error(
            "Unable to parse csv row. "
            "Most probably an incorrect amount of values was defined. "
            f'Skipping following row: "{row}". Error: "{e}".'
        )
        return False

    if user_id and user_group_id:
        logger.error(
            "UserID and UserGroupID are mutually exclusive per csv row. "
            f'Skipping following row: "{row}".'
        )
        return False

    if not user_id and not user_group_id:
        logger.error(
            "Either UserID or UserGroupID have to be defined per csv row. "
            f'Skipping following row: "{row}".'
        )
        return False

    if not ws_id:
        logger.error(f'ws_id field seems to be empty. Skipping following row: "{row}".')
        return False

    if not permission:
        logger.error(
            f'permission field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    if not is_active:
        logger.error(
            f'is_active field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    return True


def read_permissions_from_csv(csv_path: str) -> list[WSPermission]:
    """Reads permissions from the input csv file."""
    permissions: list[WSPermission] = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f, skipinitialspace=True)
        next(reader)  # Skip header
        for row in reader:
            if not csv_row_is_valid(row):
                continue
            try:
                permission = WSPermission.from_csv_row(row)
            except Exception as e:
                logger.error(f'Unable to load following row: "{row}". Error: "{e}"')
                continue
            permissions.append(permission)
    return permissions


def create_client(args: argparse.Namespace) -> gd_sdk.GoodDataSdk:
    """Creates GoodData SDK client based on the input arguments."""
    gdc_auth_token = os.environ.get("GDC_AUTH_TOKEN")
    gdc_hostname = os.environ.get("GDC_HOSTNAME")

    if gdc_hostname and gdc_auth_token:
        logger.info("Using GDC_HOSTNAME and GDC_AUTH_TOKEN envvars.")
        return gd_sdk.GoodDataSdk.create(gdc_hostname, gdc_auth_token)

    profile_config, profile = args.profile_config, args.profile
    if os.path.exists(profile_config):
        logger.info(f"Using GoodData profile {profile} sourced from {profile_config}.")
        return gd_sdk.GoodDataSdk.create_from_profile(profile, profile_config)

    raise RuntimeError(
        "No GoodData credentials provided. Please export required ENVVARS "
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to GD profile config."
    )


def validate_args(args: argparse.Namespace) -> None:
    """Validates the input arguments."""
    if not os.path.exists(args.perm_csv):
        raise RuntimeError(
            "Invalid path to workspace permission management input csv given."
        )


def permission_mgmt(args):
    """Main function for the permission management script."""
    validate_args(args)
    permissions = read_permissions_from_csv(args.perm_csv)
    sdk = create_client(args)
    permission_manager = WSPermissionManager(sdk)
    permission_manager.manage_permissions(permissions)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    permission_mgmt(args)
