# (C) 2025 GoodData Corporation
"""This module contains general utility functions."""

import csv
import logging
import os
from pathlib import Path
from typing import Type

from gooddata_pipelines.provisioning.provisioning import Provisioning

logger = logging.getLogger(__name__)


def read_csv_file_to_dict(file_path: str) -> list[dict[str, str]]:
    """Read a CSV file and return its content as a list of dictionaries.

    Args:
        file_path (str): The path to the CSV file.
    Returns:
        list[dict[str, str]]: A list of dictionaries where each dictionary represents
        a row in the CSV file, with keys as column headers and values as row values.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def create_provisioner(
    ProvisionerType: Type[Provisioning], profile_config: Path, profile: str
) -> Provisioning:
    """Creates GoodData SDK client."""
    gdc_auth_token = os.environ.get("GDC_AUTH_TOKEN")
    gdc_hostname = os.environ.get("GDC_HOSTNAME")

    if gdc_hostname and gdc_auth_token:
        logger.info("Using GDC_HOSTNAME and GDC_AUTH_TOKEN envvars.")
        return ProvisionerType.create(host=gdc_hostname, token=gdc_auth_token)

    if os.path.exists(profile_config):
        logger.info(f"Using GoodData profile {profile} sourced from {profile_config}.")
        return ProvisionerType.create_from_profile(
            profile=profile, profiles_path=profile_config
        )

    raise RuntimeError(
        "No GoodData credentials provided. Please export required ENVVARS "
        "(GDC_HOSTNAME, GDC_AUTH_TOKEN) or provide path to GD profile config."
    )
