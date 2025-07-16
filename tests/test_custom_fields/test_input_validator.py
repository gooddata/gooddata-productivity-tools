import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts"))
)

import pytest
from custom_fields.input_validator import (  # type: ignore[import]
    CustomFieldsDataValidator,
)
from custom_fields.models.custom_data_object import (  # type: ignore[import]
    CustomDataset,
)
from pydantic import ValidationError


@pytest.fixture
def valid_dataset_definitions():
    """Fixture to provide valid dataset definitions for testing."""
    return [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "dataset_name": "Dataset 1",
            "dataset_datasource_id": "ds_source_1",
            "dataset_source_table": "table1",
            "dataset_source_sql": None,
            "parent_dataset_reference": "parent1",
            "parent_dataset_reference_attribute_id": "parent1.id",
            "dataset_reference_source_column": "id",
            "dataset_reference_source_column_data_type": "STRING",
            "wdf_id": "wdf1",
            "wdf_column_name": "id",
        },
        {
            "workspace_id": "ws2",
            "dataset_id": "ds1",
            "dataset_name": "Dataset 2",
            "dataset_datasource_id": "ds_source_2",
            "dataset_source_table": "table2",
            "dataset_source_sql": None,
            "parent_dataset_reference": "parent2",
            "parent_dataset_reference_attribute_id": "parent2.id",
            "dataset_reference_source_column": "id",
            "dataset_reference_source_column_data_type": "INT",
            "wdf_id": "wdf2",
            "wdf_column_name": "id",
        },
    ]


@pytest.fixture
def valid_field_definitions():
    """Fixture to provide valid field definitions for testing."""
    return [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_id": "cf1",
            "cf_name": "Field 1",
            "cf_type": "attribute",
            "cf_source_column": "col1",
            "cf_source_column_data_type": "STRING",
        },
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_id": "cf2",
            "cf_name": "Field 2",
            "cf_type": "attribute",
            "cf_source_column": "col2",
            "cf_source_column_data_type": "STRING",
        },
        {
            "workspace_id": "ws2",
            "dataset_id": "ds1",
            "cf_id": "cf3",
            "cf_name": "Field 3",
            "cf_type": "attribute",
            "cf_source_column": "col3",
            "cf_source_column_data_type": "STRING",
        },
    ]


def test_validate_success(valid_dataset_definitions, valid_field_definitions):
    """Provide valid input data and expect successful validation."""
    validator = CustomFieldsDataValidator()
    result = validator.validate(valid_dataset_definitions, valid_field_definitions)
    assert isinstance(result, dict)
    assert "ws1" in result
    assert "ds1" in result["ws1"]
    assert isinstance(result["ws1"]["ds1"], CustomDataset)
    assert len(result["ws1"]["ds1"].custom_fields) == 2
    assert result["ws2"]["ds1"].custom_fields[0].cf_id == "cf3"


def test_duplicate_dataset_raises(valid_dataset_definitions):
    """Test that duplicate dataset definitions raise a ValueError."""
    # Add a duplicate dataset definition
    invalid = valid_dataset_definitions + [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "dataset_name": "Dataset 1",
            "dataset_datasource_id": "ds_source_1",
            "dataset_source_table": "table1",
            "dataset_source_sql": None,
            "parent_dataset_reference": "parent1",
            "parent_dataset_reference_attribute_id": "parent1.id",
            "dataset_reference_source_column": "id",
            "dataset_reference_source_column_data_type": "STRING",
            "wdf_id": "wdf1",
            "wdf_column_name": "id",
        }
    ]
    validator = CustomFieldsDataValidator()
    with pytest.raises(ValueError, match="Duplicate dataset definitions"):
        validator.validate(invalid, [])


def test_duplicate_field_workspace_level(valid_dataset_definitions):
    """Duplicate cf_id for ATTRIBUTE in same workspace. should raise ValueError."""
    fields = [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_id": "cf1",
            "cf_type": "attribute",
            "cf_name": "Field 1",
            "cf_source_column": "col1",
            "cf_source_column_data_type": "STRING",
        },
        {
            "workspace_id": "ws1",
            "dataset_id": "ds2",
            "cf_id": "cf1",
            "cf_type": "attribute",
            "cf_name": "Field 2",
            "cf_source_column": "col2",
            "cf_source_column_data_type": "STRING",
        },
    ]
    validator = CustomFieldsDataValidator()
    with pytest.raises(
        ValueError,
        match="Duplicate custom field found for workspace ws1 with field ID cf1",
    ):
        validator.validate(valid_dataset_definitions, fields)


def test_duplicate_field_dataset_level(valid_dataset_definitions):
    """Duplicate cf_id for DATE in same dataset. should raise ValueError."""
    fields = [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_id": "cf1",
            "cf_type": "date",
            "cf_name": "Field 1",
            "cf_source_column": "col1",
            "cf_source_column_data_type": "DATE",
        },
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_id": "cf1",
            "cf_type": "date",
            "cf_name": "Field 2",
            "cf_source_column": "col2",
            "cf_source_column_data_type": "DATE",
        },
    ]
    validator = CustomFieldsDataValidator()
    with pytest.raises(
        ValueError,
        match="Duplicate custom field found for dataset ds1 with field ID cf1",
    ):
        validator.validate(valid_dataset_definitions, fields)


def test_invalid_data_structure(valid_dataset_definitions):
    """Invalid shape of input data will raise ValidationError."""
    fields = [
        {
            "workspace_id": "ws1",
            "dataset_id": "ds1",
            "cf_type": "attribute",
            "cf_name": "Field 1",
        }
    ]
    validator = CustomFieldsDataValidator()
    with pytest.raises(ValidationError):
        validator.validate(valid_dataset_definitions, fields)


def test_invalid_dataset_model():
    """Missing fields will raise ValidationError."""
    datasets = [{"workspace_id": "ws1", "name": "Dataset 1"}]
    validator = CustomFieldsDataValidator()
    with pytest.raises(ValidationError):
        validator.validate(datasets, [])
