import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts"))
)

import pytest
from custom_fields.input_processor import (  # type: ignore[import]
    CustomFieldsDataProcessor,
)
from custom_fields.models.custom_data_object import (  # type: ignore[import]
    ColumnDataType,
    CustomDataset,
    CustomDatasetDefinition,
    CustomFieldDefinition,
    CustomFieldType,
)


@pytest.fixture
def mock_custom_field_attribute():
    return CustomFieldDefinition(
        workspace_id="workspace1",
        dataset_id="ds1",
        cf_id="attr1",
        cf_name="Attribute 1",
        cf_type=CustomFieldType.ATTRIBUTE,
        cf_source_column="col_attr1",
        cf_source_column_data_type=ColumnDataType.STRING,
    )


@pytest.fixture
def mock_custom_field_fact():
    return CustomFieldDefinition(
        workspace_id="workspace1",
        dataset_id="ds1",
        cf_id="fact1",
        cf_name="Fact 1",
        cf_type=CustomFieldType.FACT,
        cf_source_column="col_fact1",
        cf_source_column_data_type=ColumnDataType.INT,
    )


@pytest.fixture
def mock_custom_field_date():
    return CustomFieldDefinition(
        workspace_id="workspace1",
        dataset_id="ds1",
        cf_id="date1",
        cf_name="Date 1",
        cf_type=CustomFieldType.DATE,
        cf_source_column="col_date1",
        cf_source_column_data_type=ColumnDataType.DATE,
    )


@pytest.fixture
def mock_dataset_definition():
    return CustomDatasetDefinition(
        workspace_id="workspace1",
        dataset_id="ds1",
        dataset_name="Dataset 1",
        dataset_source_table="table1",
        dataset_datasource_id="ds_source",
        dataset_source_sql=None,
        parent_dataset_reference="parent_ds",
        parent_dataset_reference_attribute_id="parent_attr",
        dataset_reference_source_column="ref_col",
        dataset_reference_source_column_data_type=ColumnDataType.STRING,
        wdf_id="wdf1",
        wdf_column_name="col1",
    )


@pytest.fixture
def mock_custom_dataset(
    mock_dataset_definition,
    mock_custom_field_attribute,
    mock_custom_field_fact,
    mock_custom_field_date,
):
    return CustomDataset(
        definition=mock_dataset_definition,
        custom_fields=[
            mock_custom_field_attribute,
            mock_custom_field_fact,
            mock_custom_field_date,
        ],
    )


def test_attribute_from_field(mock_custom_field_attribute):
    attr = CustomFieldsDataProcessor._attribute_from_field(
        "dataset_name", mock_custom_field_attribute
    )
    assert attr.id == "attr1"
    assert attr.title == "Attribute 1"
    assert attr.source_column == "col_attr1"
    assert attr.source_column_data_type == ColumnDataType.STRING.value
    assert attr.tags == ["dataset_name"]


def test_fact_from_field(mock_custom_field_fact):
    fact = CustomFieldsDataProcessor._fact_from_field(
        "dataset_name", mock_custom_field_fact
    )
    assert fact.id == "fact1"
    assert fact.title == "Fact 1"
    assert fact.source_column == "col_fact1"
    assert fact.source_column_data_type == ColumnDataType.INT.value
    assert fact.tags == ["dataset_name"]


def test_date_from_field(mock_custom_field_date):
    processor = CustomFieldsDataProcessor()
    date_ds = processor._date_from_field("dataset_name", mock_custom_field_date)
    assert date_ds.id == "date1"
    assert date_ds.title == "Date 1"
    assert set(date_ds.granularities) == set(processor.DATE_GRANULARITIES)
    assert date_ds.tags == ["dataset_name"]


def test_date_ref_from_field(mock_custom_field_date):
    ref = CustomFieldsDataProcessor._date_ref_from_field(mock_custom_field_date)
    assert ref.identifier.id == "date1"
    assert ref.sources
    assert ref.sources[0].column == "col_date1"
    assert ref.sources[0].data_type == ColumnDataType.DATE.value


def test_get_sources_table_only(mock_dataset_definition):
    mock_dataset_definition.dataset_source_sql = None
    dataset = CustomDataset(definition=mock_dataset_definition, custom_fields=[])
    table_id, sql = CustomFieldsDataProcessor._get_sources(dataset)
    assert table_id is not None
    assert table_id.id == "table1"
    assert sql is None


def test_get_sources_sql_only(mock_dataset_definition):
    mock_dataset_definition.dataset_source_table = None
    mock_dataset_definition.dataset_source_sql = "SELECT * FROM foo"
    dataset = CustomDataset(definition=mock_dataset_definition, custom_fields=[])
    table_id, sql = CustomFieldsDataProcessor._get_sources(dataset)
    assert table_id is None
    assert sql is not None
    assert sql.statement == "SELECT * FROM foo"


def test_datasets_to_ldm(mock_custom_dataset):
    print(mock_custom_dataset)
    processor = CustomFieldsDataProcessor()
    datasets = {"ds1": mock_custom_dataset}
    model = processor.datasets_to_ldm(datasets)
    # Check that the model contains the expected dataset and date instance
    ldm = model.ldm
    assert ldm
    assert len(ldm.datasets) == 1
    ds = ldm.datasets[0]
    assert ds.id == "ds1"
    assert ds.title == "Dataset 1"
    assert ds.attributes
    assert ds.facts
    assert len(ds.attributes) == 1
    assert len(ds.facts) == 1
    assert len(ds.references) == 2  # 1 parent + 1 date
    assert ds.workspace_data_filter_columns
    assert ds.workspace_data_filter_references
    assert ds.workspace_data_filter_columns[0].name == "col1"
    assert ds.workspace_data_filter_references[0].filter_id.id == "wdf1"
    assert len(ldm.date_instances) == 1
    assert ldm.date_instances[0].id == "date1"
