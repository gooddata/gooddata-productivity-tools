# BSD License
#
# Copyright (c) 2024, GoodData Corporation. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted, provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse
import csv
import logging
import os
import re
import sys

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from gooddata_sdk import GoodDataSdk
from gooddata_sdk.catalog.user.entity_model.user import CatalogUserGroup

UG_REGEX = r"^(?!\.)[.A-Za-z0-9_-]{1,255}$"

PROFILES_FILE = "profiles.yaml"
PROFILES_DIRECTORY = ".gooddata"
PROFILES_FILE_PATH = Path.home() / PROFILES_DIRECTORY / PROFILES_FILE
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# TODO - simplify after complete switch to SDK
def create_clients(args: argparse.Namespace) -> GoodDataSdk:
    """Creates GoodData SDK client."""
    gdc_auth_token = os.environ.get("GDC_AUTH_TOKEN")
    gdc_hostname = os.environ.get("GDC_HOSTNAME")

    if gdc_hostname and gdc_auth_token:
        logger.info("Using GDC_HOSTNAME and GDC_AUTH_TOKEN envvars.")
        return GoodDataSdk.create(gdc_hostname, gdc_auth_token)

    profile_config, profile = args.profile_config, args.profile
    if os.path.exists(profile_config):
        logger.info(
            f"Using GoodData profile {profile} " f"sourced from {profile_config}."
        )
        return GoodDataSdk.create_from_profile(profile, profile_config)

    raise RuntimeError(
        "No GoodData credentials provided. Please export required ENVVARS "
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to GD profile config."
    )


def create_parser() -> argparse.ArgumentParser:
    """Creates an argument parser."""
    parser = argparse.ArgumentParser(description="Management of users and userGroups.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Turns on the debug log output."
    )
    parser.add_argument(
        "user_group_csv", type=Path, help="Path to csv with user groups definition."
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the user_group_csv.",
    )
    parser.add_argument(
        "-u",
        "--ug_delimiter",
        type=str,
        default="|",
        help=(
            "Delimiter used to separate different parent user groups within "
            "the parent user group column in the user_group_csv. "
            'This must differ from the "delimiter" argument.'
        ),
    )
    parser.add_argument(
        "-q",
        "--quotechar",
        type=str,
        default='"',
        help=(
            "Character used for quoting (escaping) values "
            "which contain delimiters or quotechars."
        ),
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
        help='GoodData profile to use. If no profile is provided, "default" is used.',
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validates the arguments provided."""
    if not os.path.exists(args.user_group_csv):
        raise RuntimeError("Invalid path to user management input csv given.")

    if args.delimiter == args.ug_delimiter:
        raise RuntimeError(
            "Delimiter and ParentUserGroups Delimiter cannot be the same."
        )

    if args.ug_delimiter == "." or re.match(UG_REGEX, args.ug_delimiter):
        raise RuntimeError(
            'ParentUserGroups delimiter cannot be dot (".") '
            f'or match the following regex: "{UG_REGEX}".'
        )

    if len(args.quotechar) != 1:
        raise RuntimeError("The quotechar argument must be exactly one character long.")


@dataclass
class TargetUserGroup:
    user_group_id: str
    user_group_name: str
    parent_user_groups: list[str]
    is_active: bool = field(compare=False)

    @classmethod
    def from_csv_row(cls, row: list[Any], parent_user_group_delimiter: str = ","):
        """Creates GDUserGroupTarget from csv row."""
        user_group_id, user_group_name, parent_user_groups, is_active = row
        user_group_name_or_id = user_group_name or user_group_id
        parent_user_groups = (
            parent_user_groups.split(parent_user_group_delimiter)
            if parent_user_groups
            else []
        )
        return TargetUserGroup(
            user_group_id=user_group_id,
            user_group_name=user_group_name_or_id,
            parent_user_groups=parent_user_groups,
            is_active=str(is_active).lower() == "true",
        )


def read_users_groups_from_csv(args: argparse.Namespace) -> list[TargetUserGroup]:
    """Reads users from csv file."""
    # TODO - handling of csv files with and without headers
    user_groups: list[TargetUserGroup] = []
    with open(args.user_group_csv, "r") as f:
        reader = csv.reader(
            f, delimiter=args.delimiter, quotechar=args.quotechar, skipinitialspace=True
        )
        next(reader)  # Skip header
        for row in reader:
            if not csv_row_is_valid(row):
                continue
            try:
                user_group = TargetUserGroup.from_csv_row(row, args.ug_delimiter)
            except Exception as e:
                logger.error(f'Unable to load following row: "{row}". Error: "{e}"')
                continue
            user_groups.append(user_group)

    return user_groups


def csv_row_is_valid(row: list[Any]) -> bool:
    """Validates csv row."""
    try:
        user_group_id, user_group_name, parent_user_group, is_active = row
    except ValueError as e:
        logger.error(
            "Unable to parse csv row. "
            "Most probably an incorrect amount of values was defined. "
            f'Skipping following row: "{row}". Error: "{e}".'
        )
        return False

    if not user_group_id:
        logger.error(
            f'user_group_id field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    if not is_active:
        logger.error(
            f'is_active field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    return True


class UserGroupManager:
    def __init__(
        self, client_sdk: GoodDataSdk, target_user_groups: list[TargetUserGroup]
    ):
        self.sdk = client_sdk
        self.target_user_groups = target_user_groups
        self.gd_user_groups = self._get_gd_user_groups()

    def _get_gd_user_groups(self) -> list[CatalogUserGroup]:
        try:
            return self.sdk.catalog_user.list_user_groups()
        except Exception as e:
            logger.error(f"Failed to list user groups from GoodData: {e}")
            return []

    @staticmethod
    def _is_changed(group: TargetUserGroup, existing_group: CatalogUserGroup) -> bool:
        """Checks if user group has some changes and needs to be updated."""
        group.parent_user_groups.sort()
        parents_changed = group.parent_user_groups != existing_group.get_parents
        name_changed = group.user_group_name != existing_group.name
        return parents_changed or name_changed

    def _create_or_update_user_group(
        self, group_id, group_name, parent_user_groups, action
    ) -> None:
        """Creates or updates user group in the project."""
        catalog_user_group = CatalogUserGroup.init(
            user_group_id=group_id,
            user_group_name=group_name,
            user_group_parent_ids=parent_user_groups,
        )
        try:
            self.sdk.catalog_user.create_or_update_user_group(catalog_user_group)
            logger.info(f"Succeeded to {action} user group {group_id}")
        except Exception as e:
            if hasattr(e, "body") and e.body:
                message = eval(e.body).get("detail", e)
            else:
                message = e.args[0] if e.args else str(e)
            logger.error(f"Failed to {action} user group {group_id}: {message}")

    def _create_missing_user_groups(self, group_ids_to_create) -> None:
        """Provisions user groups that don't exist."""
        groups_to_create = [
            group
            for group in self.target_user_groups
            if group.user_group_id in group_ids_to_create
        ]

        for group in groups_to_create:
            logger.info(
                f'User group "{group.user_group_id}" does not exist, creating...'
            )
            self._create_or_update_user_group(
                group.user_group_id,
                group.user_group_name,
                group.parent_user_groups,
                "create",
            )

    def _update_existing_user_groups(self, group_ids_to_update) -> None:
        """Update existing user groups and update ws_permissions."""
        groups_to_update = [
            group
            for group in self.target_user_groups
            if group.user_group_id in group_ids_to_update
        ]

        existing_groups = {group.id: group for group in self.gd_user_groups}

        for group in groups_to_update:
            existing_group = existing_groups[group.user_group_id]
            if self._is_changed(group, existing_group):
                logger.info(f"Updating user group {group.user_group_id}...")
                self._create_or_update_user_group(
                    group.user_group_id,
                    group.user_group_name,
                    group.parent_user_groups,
                    "update",
                )

    def _delete_user_group(self, group_ids_to_delete) -> None:
        """Deletes user group from the project."""
        for user_group_id in group_ids_to_delete:
            try:
                logger.info(f'Deleting user group"{user_group_id}"')
                self.sdk.catalog_user.delete_user_group(user_group_id)
            except Exception as e:
                logger.error(f'Failed to deleted user group "{user_group_id}": {e}')

    def manage_user_groups(self) -> None:
        """Manages multiple users groups based on the provided input."""

        logger.info(
            f"Starting user group management run of {len(self.target_user_groups)} user groups..."
        )

        gd_group_ids = {group.id for group in self.gd_user_groups}

        active_target_groups = {
            group.user_group_id
            for group in self.target_user_groups
            if group.is_active is True
        }
        inactive_target_groups = {
            group.user_group_id
            for group in self.target_user_groups
            if group.is_active is False
        }

        group_ids_to_create = active_target_groups.difference(gd_group_ids)
        self._create_missing_user_groups(group_ids_to_create)

        group_ids_to_update = active_target_groups.intersection(gd_group_ids)
        self._update_existing_user_groups(group_ids_to_update)

        group_ids_to_delete = inactive_target_groups.intersection(gd_group_ids)
        self._delete_user_group(group_ids_to_delete)

        logger.info("User group management run finished.")


def user_group_mgmt(args):
    """Main function for user management."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        validate_args(args)
        client_sdk = create_clients(args)
        target_user_groups = read_users_groups_from_csv(args)
        user_group_manager = UserGroupManager(client_sdk, target_user_groups)
        user_group_manager.manage_user_groups()
    except RuntimeError as e:
        logger.error(f"Runtime error has occurred: {e}")


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    user_group_mgmt(args)
