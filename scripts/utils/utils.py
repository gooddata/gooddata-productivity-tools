# (C) 2025 GoodData Corporation
"""This module contains general utility functions."""

import csv


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
