# (C) 2025 GoodData Corporation
"""Top level script to manage custom datasets and fields in GoodData Cloud.

This script allows you to extend the Logical Data Model (LDM) of a child workspace.
Documentation and usage instructions are located in `docs/CUSTOM_FIELDS.md` file.
"""

import argparse
import os

from custom_fields.custom_field_manager import (  # type: ignore[import]
    CustomFieldManager,
)
from utils.utils import read_csv_file_to_dict  # type: ignore[import]


def main(
    path_to_custom_datasets_csv: str,
    path_to_custom_fields_csv: str,
    check_relations: bool,
) -> None:
    """Main function to run the custom fields script."""
    # Get host and token from environment variables
    # TODO: add option to load credentials from profile
    # TODO: (refactor) credentials should be handled in one place for the project
    host = os.environ.get("GDC_HOSTNAME")
    token = os.environ.get("GDC_AUTH_TOKEN")

    if not host:
        raise ValueError("GDC_HOSTNAME environment variable is not set.")
    if not token:
        raise ValueError("GDC_AUTH_TOKEN environment variable is not set.")

    # Load input data from csv files
    custom_datasets: list[dict[str, str]] = read_csv_file_to_dict(
        path_to_custom_datasets_csv
    )
    custom_fields: list[dict[str, str]] = read_csv_file_to_dict(
        path_to_custom_fields_csv
    )

    # Create instance of CustomFieldManager with host and token
    manager = CustomFieldManager(host, token)

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

    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = parse_args()
    path_to_custom_datasets_csv = args.path_to_custom_datasets_csv
    path_to_custom_fields_csv = args.path_to_custom_fields_csv
    check_relations: bool = args.check_relations
    main(path_to_custom_datasets_csv, path_to_custom_fields_csv, check_relations)
