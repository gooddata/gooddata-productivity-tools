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
import logging
import os
import re
from pathlib import Path

from gooddata_pipelines import (
    UserGroupIncrementalLoad,
    UserGroupProvisioner,
)
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import setup_logging  # type: ignore[import]
from utils.utils import (  # type: ignore[import]
    create_provisioner,
    read_csv_file_to_dict,
)

UG_REGEX = r"^(?!\.)[.A-Za-z0-9_-]{1,255}$"


setup_logging()
logger = logging.getLogger(__name__)


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


def read_users_groups_from_csv(
    args: argparse.Namespace,
) -> list[UserGroupIncrementalLoad]:
    """Reads users from csv file."""
    user_groups: list[UserGroupIncrementalLoad] = []
    raw_user_groups = read_csv_file_to_dict(
        args.user_group_csv, args.delimiter, args.quotechar
    )
    for raw_user_group in raw_user_groups:
        processed_user_group = dict(raw_user_group)
        parent_user_groups = raw_user_group["parent_user_groups"]

        if parent_user_groups:
            processed_user_group["parent_user_groups"] = parent_user_groups.split(
                args.ug_delimiter
            )
        else:
            processed_user_group["parent_user_groups"] = []

        try:
            user_group = UserGroupIncrementalLoad.model_validate(processed_user_group)
            user_groups.append(user_group)
        except Exception as e:
            logger.error(
                f'Unable to load following row: "{raw_user_group}". Error: "{e}"'
            )
            continue
    return user_groups


def user_group_mgmt(args):
    """Main function for user management."""
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        validate_args(args)

        provisioner = create_provisioner(
            UserGroupProvisioner, args.profile_config, args.profile
        )

        provisioner.logger.subscribe(logger)

        validated_user_groups = read_users_groups_from_csv(args)

        provisioner.incremental_load(validated_user_groups)

    except RuntimeError as e:
        logger.error(f"Runtime error has occurred: {e}")


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    user_group_mgmt(args)
