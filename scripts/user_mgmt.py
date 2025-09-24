# (C) 2025 GoodData Corporation
import argparse
import csv
import logging
import os
import re
from pathlib import Path

from gooddata_pipelines import UserIncrementalLoad, UserProvisioner
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import setup_logging  # type: ignore[import]
from utils.utils import create_provisioner  # type: ignore[import]

setup_logging()
logger = logging.getLogger(__name__)

UG_REGEX = r"^(?!\.)[.A-Za-z0-9_-]{1,255}$"


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


def read_users_from_csv(
    path_to_csv: str, row_delimiter: str, quotechar: str, user_group_delimiter: str
) -> list[UserIncrementalLoad]:
    """Reads users from csv file."""

    users: list[UserIncrementalLoad] = []

    with open(path_to_csv, "r") as f:
        reader = csv.DictReader(
            f, delimiter=row_delimiter, quotechar=quotechar, skipinitialspace=True
        )
        for row in reader:
            try:
                user_id = row["user_id"]
                firstname = row["firstname"]
                lastname = row["lastname"]
                email = row["email"]
                auth_id = row["auth_id"]
                user_groups = row["user_groups"].split(user_group_delimiter)
                is_active = row["is_active"] == "True"

                user = UserIncrementalLoad(
                    user_id=user_id,
                    firstname=firstname,
                    lastname=lastname,
                    email=email,
                    auth_id=auth_id,
                    user_groups=user_groups,
                    is_active=is_active,
                )

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


def user_mgmt(args: argparse.Namespace) -> None:
    """Main function for user management."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    validate_args(args)

    users = read_users_from_csv(
        args.user_csv, args.delimiter, args.quotechar, args.ug_delimiter
    )

    provisioner = create_provisioner(UserProvisioner, args.profile_config, args.profile)

    provisioner.logger.subscribe(logger)

    provisioner.incremental_load(users)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    user_mgmt(args)
