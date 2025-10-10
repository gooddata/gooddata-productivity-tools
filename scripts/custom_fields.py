# (C) 2025 GoodData Corporation
"""Top level script to manage custom datasets and fields in GoodData Cloud.

This script allows you to extend the Logical Data Model (LDM) of a child workspace.
Documentation and usage instructions are located in `docs/CUSTOM_FIELDS.md` file.
"""

import argparse
from pathlib import Path

from gooddata_pipelines import (
    CustomDatasetDefinition,
    CustomFieldDefinition,
    LdmExtensionManager,
)
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import get_logger, setup_logging  # type: ignore[import]
from utils.utils import (  # type: ignore[import]
    create_client,
    read_csv_file_to_dict,
)

setup_logging()
logger = get_logger(__name__)


def custom_fields() -> None:
    """Main function to run the custom fields script."""

    args: argparse.Namespace = parse_args()
    path_to_custom_datasets_csv = args.path_to_custom_datasets_csv
    path_to_custom_fields_csv = args.path_to_custom_fields_csv
    check_relations: bool = args.check_relations

    # Load input data from csv files
    raw_custom_datasets: list[dict[str, str]] = read_csv_file_to_dict(
        path_to_custom_datasets_csv
    )

    custom_datasets = [
        CustomDatasetDefinition.model_validate(raw_custom_dataset)
        for raw_custom_dataset in raw_custom_datasets
    ]

    raw_custom_fields: list[dict[str, str]] = read_csv_file_to_dict(
        path_to_custom_fields_csv
    )

    custom_fields = [
        CustomFieldDefinition.model_validate(raw_custom_field)
        for raw_custom_field in raw_custom_fields
    ]

    # Create instance of CustomFieldManager with host and token
    manager = create_client(LdmExtensionManager, args.profile_config, args.profile)

    # Subscribe to logs
    manager.logger.subscribe(logger)

    # Process the custom datasets and fields
    manager.process(custom_datasets, custom_fields, check_relations)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Custom Fields Script")
    parser.add_argument(
        "path_to_custom_datasets_csv",
        type=str,
        help="Path to the CSV file containing custom datasets definitions.",
    )

    parser.add_argument(
        "path_to_custom_fields_csv",
        type=str,
        help="Path to the CSV file containing custom fields definitions.",
    )

    parser.add_argument(
        "--no-relations-check",
        action="store_false",
        dest="check_relations",
        help="Check relations after updating LLM. "
        + "If new ivalid relations are found, the update is rolled back. "
        + "Boolean, defaults to True.",
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

    return parser.parse_args()


if __name__ == "__main__":
    custom_fields()
