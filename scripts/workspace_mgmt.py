# (C) 2025 GoodData Corporation
import argparse
import logging
import os
from pathlib import Path
from typing import Any

from gooddata_pipelines import WorkspaceIncrementalLoad, WorkspaceProvisioner
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import setup_logging  # type: ignore[import]
from utils.utils import (  # type: ignore[import]
    create_provisioner,
    read_csv_file_to_dict,
)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Management of workspaces.")
    parser.add_argument(
        "filepath",
        type=Path,
        help="Path to CSV file with input data.",
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the workspace_csv.",
    )
    parser.add_argument(
        "-i",
        "--inner-delimiter",
        type=str,
        default="|",
        help=(
            "Delimiter used to separate different inner values within "
            "the columns in the input csv which contain inner-delimiter separated values. "
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
    """Validates the input arguments."""
    if not os.path.exists(args.filepath):
        raise RuntimeError("Invalid path to input csv given.")

    if args.delimiter == args.inner_delimiter:
        raise RuntimeError(
            "Delimiter and Workspace Data Filter Delimiter cannot be the same."
        )


def validate_workspace_data(
    raw_workspaces: list[dict[str, Any]],
    wdf_delimiter: str,
) -> list[WorkspaceIncrementalLoad]:
    """Validate workspace against input model."""

    validated_workspaces: list[WorkspaceIncrementalLoad] = []

    for raw_workspace in raw_workspaces:
        try:
            if raw_workspace["workspace_data_filter_values"]:
                workspace_data_filter_values = raw_workspace[
                    "workspace_data_filter_values"
                ].split(wdf_delimiter)
            else:
                workspace_data_filter_values = None
            validated_workspace = WorkspaceIncrementalLoad(
                parent_id=raw_workspace["parent_id"],
                workspace_id=raw_workspace["workspace_id"],
                workspace_name=raw_workspace["workspace_name"],
                workspace_data_filter_id=raw_workspace["workspace_data_filter_id"],
                workspace_data_filter_values=workspace_data_filter_values,
                is_active=raw_workspace["is_active"],
            )

            validated_workspaces.append(validated_workspace)
        except Exception as e:
            logger.error(
                f'Unable to load following row: "{raw_workspace}". Error: "{e}"'
            )
            continue

    return validated_workspaces


def workspace_mgmt():
    """Main function for workspace management."""

    # Create parser and parse arguments
    parser = create_parser()
    args = parser.parse_args()

    validate_args(args)

    # Read CSV input
    raw_workspaces = read_csv_file_to_dict(
        args.filepath, args.delimiter, args.quotechar
    )

    # Validate workspace data
    validated_workspaces = validate_workspace_data(raw_workspaces, args.inner_delimiter)

    # Create provisioner and subscribe to logger
    provisioner = create_provisioner(
        WorkspaceProvisioner, args.profile_config, args.profile
    )

    provisioner.logger.subscribe(logger)

    # Incremental load workspaces
    provisioner.incremental_load(validated_workspaces)


if __name__ == "__main__":
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Main function
    workspace_mgmt()
