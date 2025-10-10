# (C) 2025 GoodData Corporation
import argparse
import os
from pathlib import Path
from typing import Any

from gooddata_pipelines import UserDataFilterFullLoad, UserDataFilterProvisioner
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import get_logger, setup_logging  # type: ignore[import]
from utils.utils import (  # type: ignore[import]
    create_client,
    read_csv_file_to_dict,
)

# Setup logging
setup_logging()
logger = get_logger(__name__)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Management of workspaces.")
    parser.add_argument(
        "filepath",
        type=Path,
        help="Path to csv with input data.",
    )
    parser.add_argument(
        "ldm_column_name",
        type=str,
        help="LDM column name.",
    )
    parser.add_argument(
        "maql_column_name",
        type=str,
        help="MAQL column name: {attribute/dataset.field}",
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the workspace_csv.",
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
    """Validates the input arguments."""
    if not os.path.exists(args.filepath):
        raise RuntimeError("Invalid path to input csv given.")


def validate_user_data_filter_data(
    raw_user_data_filters: list[dict[str, Any]],
) -> list[UserDataFilterFullLoad]:
    """Validate workspace against input model."""
    validated_user_data_filters: list[UserDataFilterFullLoad] = []
    for raw_user_data_filter in raw_user_data_filters:
        validated_user_data_filter = UserDataFilterFullLoad(
            workspace_id=raw_user_data_filter["workspace_id"],
            udf_id=raw_user_data_filter["udf_id"],
            udf_value=raw_user_data_filter["udf_value"],
        )

        validated_user_data_filters.append(validated_user_data_filter)

    return validated_user_data_filters


def udf_mgmt():
    """Main function for workspace management."""

    # Create parser and parse arguments
    parser = create_parser()
    args = parser.parse_args()

    validate_args(args)

    # Read CSV input
    raw_user_data_filters = read_csv_file_to_dict(
        args.filepath, args.delimiter, args.quotechar
    )

    # Validate user data filter data
    validated_user_data_filters = validate_user_data_filter_data(raw_user_data_filters)

    # Create provisioner and subscribe to logger
    provisioner: UserDataFilterProvisioner = create_client(
        UserDataFilterProvisioner, args.profile_config, args.profile
    )

    provisioner.set_ldm_column_name(args.ldm_column_name)
    provisioner.set_maql_column_name(args.maql_column_name)

    provisioner.logger.subscribe(logger)

    # Incremental load user data filters
    provisioner.full_load(validated_user_data_filters)


if __name__ == "__main__":
    # Main function
    udf_mgmt()
