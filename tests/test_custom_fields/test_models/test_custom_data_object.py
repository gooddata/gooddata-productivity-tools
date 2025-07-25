# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../scripts"))
)

import pytest
from pydantic import ValidationError

from scripts.custom_fields.models.custom_data_object import (
    ColumnDataType,
    CustomDataset,
    CustomDatasetDefinition,
    CustomFieldDefinition,
    CustomFieldType,
)


def make_valid_field_def(**kwargs):
    data = {
        "workspace_id": "ws1",
        "dataset_id": "ds1",
        "cf_id": "cf1",
        "cf_name": "Custom Field",
        "cf_type": CustomFieldType.ATTRIBUTE,
        "cf_source_column": "col1",
        "cf_source_column_data_type": ColumnDataType.STRING,
    }
    data.update(kwargs)
    return data


def make_valid_dataset_def(**kwargs):
    data = {
        "workspace_id": "ws1",
        "dataset_id": "ds1",
        "dataset_name": "Dataset",
        "dataset_datasource_id": "dsrc1",
        "dataset_source_table": "table1",
        "dataset_source_sql": None,
        "parent_dataset_reference": "parent_ds",
        "parent_dataset_reference_attribute_id": "parent_attr",
        "dataset_reference_source_column": "src_col",
        "dataset_reference_source_column_data_type": ColumnDataType.STRING,
        "wdf_id": "wdf1",
        "wdf_column_name": "col1",
    }
    data.update(kwargs)
    return data


def test_custom_field_definition_valid():
    field = CustomFieldDefinition(**make_valid_field_def())
    assert field.cf_id == "cf1"
    assert field.cf_type == CustomFieldType.ATTRIBUTE


def test_custom_field_definition_cf_id_equals_dataset_id_raises():
    data = make_valid_field_def(cf_id="ds1")
    with pytest.raises(ValidationError) as exc:
        CustomFieldDefinition(**data)
    assert "cannot be the same as dataset ID" in str(exc.value)


def test_custom_dataset_definition_valid_table():
    ds = CustomDatasetDefinition(**make_valid_dataset_def())
    assert ds.dataset_source_table == "table1"
    assert ds.dataset_source_sql is None


def test_custom_dataset_definition_valid_sql():
    data = make_valid_dataset_def(
        dataset_source_table=None, dataset_source_sql="SELECT 1"
    )
    ds = CustomDatasetDefinition(**data)
    assert ds.dataset_source_sql == "SELECT 1"
    assert ds.dataset_source_table is None


def test_custom_dataset_definition_both_none_raises():
    data = make_valid_dataset_def(dataset_source_table=None, dataset_source_sql=None)
    with pytest.raises(ValidationError) as exc:
        CustomDatasetDefinition(**data)
    assert "must be provided" in str(exc.value)


def test_custom_dataset_definition_both_provided_raises():
    data = make_valid_dataset_def(
        dataset_source_table="table1", dataset_source_sql="SELECT 1"
    )
    with pytest.raises(ValidationError) as exc:
        CustomDatasetDefinition(**data)
    assert (
        "Only one of dataset_source_table and dataset_source_sql can be provided"
        in str(exc.value)
    )


def test_custom_dataset_model():
    ds_def = CustomDatasetDefinition(**make_valid_dataset_def())
    field_def = CustomFieldDefinition(**make_valid_field_def())
    dataset = CustomDataset(definition=ds_def, custom_fields=[field_def])
    assert dataset.definition.dataset_id == "ds1"
    assert len(dataset.custom_fields) == 1
    assert dataset.custom_fields[0].cf_id == "cf1"
