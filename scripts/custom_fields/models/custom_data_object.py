# (C) 2025 GoodData Corporation
"""This module defines enums and models used to represent the input data.

Models defined here are used to validate and structure the input data before
further processing.
"""

from enum import Enum

from pydantic import BaseModel, model_validator


class CustomFieldType(Enum):
    """GoodData field types."""

    ATTRIBUTE = "attribute"
    FACT = "fact"
    DATE = "date"


class ColumnDataType(Enum):
    """Supported data types"""

    INT = "INT"
    STRING = "STRING"
    DATE = "DATE"
    NUMERIC = "NUMERIC"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    BOOLEAN = "BOOLEAN"


class CustomFieldDefinition(BaseModel):
    """Input model for custom field definition."""

    workspace_id: str
    dataset_id: str
    cf_id: str
    cf_name: str
    cf_type: CustomFieldType
    cf_source_column: str
    cf_source_column_data_type: ColumnDataType

    @model_validator(mode="after")
    def check_ids_not_equal(self) -> "CustomFieldDefinition":
        """Check that custom field ID is not the same as dataset ID."""
        if self.cf_id == self.dataset_id:
            raise ValueError(
                f"Custom field ID {self.cf_id} cannot be the same as dataset ID {self.dataset_id}"
            )
        return self


class CustomDatasetDefinition(BaseModel):
    """Input model for custom dataset definition."""

    workspace_id: str
    dataset_id: str
    dataset_name: str
    dataset_datasource_id: str
    dataset_source_table: str | None
    dataset_source_sql: str | None
    parent_dataset_reference: str
    parent_dataset_reference_attribute_id: str
    dataset_reference_source_column: str
    dataset_reference_source_column_data_type: ColumnDataType
    wdf_id: str
    wdf_column_name: str

    @model_validator(mode="after")
    def check_source(self) -> "CustomDatasetDefinition":
        """At least one of dataset_source_table or dataset_source_sql is provided."""
        if not (self.dataset_source_table or self.dataset_source_sql):
            raise ValueError(
                "One of dataset_source_table and dataset_source_sql must be provided"
            )
        if self.dataset_source_table and self.dataset_source_sql:
            raise ValueError(
                "Only one of dataset_source_table and dataset_source_sql can be provided"
            )
        return self


class CustomDataset(BaseModel):
    """Custom dataset with its definition and custom fields."""

    definition: CustomDatasetDefinition
    custom_fields: list[CustomFieldDefinition]
