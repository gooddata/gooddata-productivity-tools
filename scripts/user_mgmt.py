# (C) 2025 GoodData Corporation
import argparse
import csv
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import gooddata_sdk as gd_sdk
from gooddata_api_client.exceptions import NotFoundException

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


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Management of users and userGroups.")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Turns on the debug log output."
    )
    parser.add_argument(
        "user_csv", type=Path, help="Path to csv with user definitions."
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the user_csv.",
    )
    parser.add_argument(
        "-u",
        "--ug_delimiter",
        type=str,
        default="|",
        help=(
            "Delimiter used to separate different user groups within "
            "the relevant user groups column in the user_csv. "
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


class GoodDataRestApiError(Exception):
    """Wrapper for errors occurring from interaction with GD REST API."""


def optional(string: str) -> Optional[str]:
    """
    Ensures conversion of empty string to None.

    CSV reader parses empty fields as empty strings.

    Returns string or None.
    """
    return string if string else None


@dataclass
class GDUserTarget:
    user_id: str
    firstname: Optional[str]
    lastname: Optional[str]
    email: Optional[str]
    auth_id: Optional[str]
    user_groups: list[str]
    is_active: bool = field(compare=False)

    @classmethod
    def from_csv_row(
        cls, row: list[Any], user_group_delim: str = ","
    ) -> "GDUserTarget":
        """Creates GDUserTarget from csv row."""
        user_id, firstname, lastname, email, auth_id, user_groups, is_active = row
        user_groups_list = user_groups.split(user_group_delim) if user_groups else []
        return GDUserTarget(
            user_id=user_id,
            firstname=optional(firstname),
            lastname=optional(lastname),
            email=optional(email),
            auth_id=optional(auth_id),
            user_groups=user_groups_list,
            is_active=str(is_active).lower() == "true",
        )

    @classmethod
    def from_sdk_obj(cls, obj: gd_sdk.CatalogUser) -> "GDUserTarget":
        """Creates GDUserTarget from CatalogUser SDK object."""
        return GDUserTarget(
            user_id=obj.id,
            firstname=obj.attributes.firstname,
            lastname=obj.attributes.lastname,
            email=obj.attributes.email,
            auth_id=obj.attributes.authentication_id,
            user_groups=[ug.id for ug in obj.user_groups],
            is_active=True,
        )

    def to_sdk_obj(self) -> gd_sdk.CatalogUser:
        """Converts GDUserTarget to CatalogUser SDK object."""
        return gd_sdk.CatalogUser.init(
            user_id=self.user_id,
            firstname=self.firstname,
            lastname=self.lastname,
            email=self.email,
            authentication_id=self.auth_id,
            user_group_ids=self.user_groups,
        )


class UserManager:
    def __init__(self, sdk: gd_sdk.GoodDataSdk):
        self._sdk = sdk

    def _try_get_user(self, user: GDUserTarget) -> Optional[GDUserTarget]:
        try:
            user_sdk_obj = self._sdk.catalog_user.get_user(user.user_id)
            return GDUserTarget.from_sdk_obj(user_sdk_obj)
        except NotFoundException:
            return None

    def _get_or_create_user_groups(self, groups: list[str]):
        """Ensures that all user groups exist in the project."""
        # TODO - Can be optimized - preloading all user groups and checking on the go
        for group in groups:
            try:
                self._sdk.catalog_user.get_user_group(group)
            except NotFoundException:
                logger.info(f'UserGroup "{group}" doesn\'t exist - creating...')
                self._sdk.catalog_user.create_or_update_user_group(
                    gd_sdk.CatalogUserGroup.init(
                        user_group_id=group, user_group_name=group
                    )
                )

    def _create_or_update_user(self, user: GDUserTarget):
        """Creates or updates user in the project."""
        upstream_user = self._try_get_user(user)
        if user == upstream_user:
            logger.info(f'No action for user "{user.user_id}"')
            return
        if not upstream_user:
            logger.info(f'Creating user "{user.user_id}"...')
        else:
            logger.info(f'Updating user "{user.user_id}"...')

        self._get_or_create_user_groups(user.user_groups)
        self._sdk.catalog_user.create_or_update_user(user.to_sdk_obj())

    def _delete_user(self, user: GDUserTarget):
        """Deletes user from the project."""
        try:
            self._sdk.catalog_user.get_user(user.user_id)
        except NotFoundException:
            logger.info(f'No action for user "{user.user_id}"')
            return
        logger.info(f'Deleting user "{user.user_id}"')
        self._sdk.catalog_user.delete_user(user.user_id)

    def manage_user(self, user: GDUserTarget):
        """Manages user based on the provided GDUserTarget."""
        if user.is_active:
            self._create_or_update_user(user)
        else:
            self._delete_user(user)

    def manage_users(self, users: list[GDUserTarget]):
        """Manages multiple users based on the provided GDUserTargets."""
        logger.info(f"Starting user management run of {len(users)} users...")
        for user in users:
            try:
                self.manage_user(user)
            except GoodDataRestApiError as e:
                logger.error(f"API request for user failed: {e}")
            except Exception as e:
                logger.error(f"Something went wrong for {user.user_id}. Error: {e}")
        logger.info("User management run finished.")


# TODO - simplify after complete switch to SDK
def create_clients(args: argparse.Namespace) -> gd_sdk.GoodDataSdk:
    """Creates GoodData SDK client."""
    gdc_auth_token = os.environ.get("GDC_AUTH_TOKEN")
    gdc_hostname = os.environ.get("GDC_HOSTNAME")

    if gdc_hostname and gdc_auth_token:
        logger.info("Using GDC_HOSTNAME and GDC_AUTH_TOKEN envvars.")
        sdk = gd_sdk.GoodDataSdk.create(gdc_hostname, gdc_auth_token)
        return sdk

    profile_config, profile = args.profile_config, args.profile
    if os.path.exists(profile_config):
        logger.info(f"Using GoodData profile {profile} sourced from {profile_config}.")
        sdk = gd_sdk.GoodDataSdk.create_from_profile(profile, profile_config)
        return sdk

    raise RuntimeError(
        "No GoodData credentials provided. Please export required ENVVARS "
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to GD profile config."
    )


def csv_row_is_valid(row: list[Any]) -> bool:
    """Validates csv row."""
    try:
        user_id, firstname, lastname, email, auth_id, user_groups, is_active = row
    except Exception as e:
        logger.error(
            "Unable to parse csv row. "
            "Most probably an incorrect amount of values was defined. "
            f'Skipping following row: "{row}". Error: "{e}".'
        )
        return False

    if not user_id:
        logger.error(
            f'user_id field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    if not is_active:
        logger.error(
            f'is_active field seems to be empty. Skipping following row: "{row}".'
        )
        return False

    return True


def read_users_from_csv(args: argparse.Namespace) -> list[GDUserTarget]:
    """Reads users from csv file."""
    # TODO - handling of csv files with and without headers
    users: list[GDUserTarget] = []
    with open(args.user_csv, "r") as f:
        reader = csv.reader(
            f, delimiter=args.delimiter, quotechar=args.quotechar, skipinitialspace=True
        )
        next(reader)  # Skip header
        for row in reader:
            if not csv_row_is_valid(row):
                continue
            try:
                user = GDUserTarget.from_csv_row(row, args.ug_delimiter)
            except Exception as e:
                logger.error(f'Unable to load following row: "{row}". Error: "{e}"')
                continue
            users.append(user)

    return users


def validate_args(args: argparse.Namespace) -> None:
    """Validates the arguments provided."""
    if not os.path.exists(args.user_csv):
        raise RuntimeError("Invalid path to user management input csv given.")

    if args.delimiter == args.ug_delimiter:
        raise RuntimeError("Delimiter and UserGroups Delimiter cannot be the same.")

    if args.ug_delimiter == "." or re.match(UG_REGEX, args.ug_delimiter):
        raise RuntimeError(
            'Usergroup delimiter cannot be dot (".") '
            f'or match the following regex: "{UG_REGEX}".'
        )

    if len(args.quotechar) != 1:
        raise RuntimeError("The quotechar argument must be exactly one character long.")


def user_mgmt(args):
    """Main function for user management."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    validate_args(args)

    users = read_users_from_csv(args)

    sdk = create_clients(args)

    user_manager = UserManager(sdk)

    user_manager.manage_users(users)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    user_mgmt(args)
