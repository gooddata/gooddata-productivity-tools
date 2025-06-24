# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../scripts"))
)

import json

import pytest
from custom_fields.models.analytical_object import (
    AnalyticalObject,
    AnalyticalObjects,
)


@pytest.mark.parametrize(
    "file_path",
    [
        "tests/data/custom_fields/response_get_all_metrics.json",
        "tests/data/custom_fields/response_get_all_visualizations.json",
        "tests/data/custom_fields/response_get_all_dashboards.json",
    ],
)
def test_analytical_object_model_with_metrics(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    analytical_objects = AnalyticalObjects(**data)
    assert isinstance(analytical_objects, AnalyticalObjects)
    assert isinstance(analytical_objects.data, list)
    assert all(isinstance(obj, AnalyticalObject) for obj in analytical_objects.data)


@pytest.mark.parametrize(
    "response",
    [
        {
            "something": "unexpected",
        },
        {
            "data": [
                {
                    # "id": "metric1", # Missing id field
                    "type": "metric",
                    "attributes": {
                        "title": "Test Metric",
                        "areRelationsValid": True,
                    },
                }
            ]
        },
        {
            "data": [
                {
                    "id": 123,  # invalid id type
                    "type": "metric",
                    "attributes": {
                        "title": "Test Metric",
                        "areRelationsValid": True,
                    },
                }
            ]
        },
    ],
)
def test_analytical_object_model_with_invalid_response(response):
    with pytest.raises(ValueError):
        AnalyticalObjects(**response)
