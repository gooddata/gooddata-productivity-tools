# Custom Field Management

The `scripts/custom_fields.py` script will allow you to extend the Logical Data Model (LDM) of a child workspace by adding extra datasets which are not present in the parent workspaces' LDM.

## Environment setup

The script relies on `GDC_HOSTNAME` and `GDC_AUTH_TOKEN` environment variables. You can export these by running this in your terminal:

```shell
export GDC_HOSTNAME=https://your-gooddata-cloud-domain.com
export GDC_AUTH_TOKEN=your-personal-access-token
```

## Input files

The script works with input from two CSV files. These files should contain (a) custom dataset definitions and (b) custom field definitions.

The custom dataset defines the dataset entity, i.e., the box you would see in the GoodData Cloud UI. The custom fields, on the other hand, define the individual fields in that dataset. You can imagine it as first defining a table and then its columns.

Multiple datasets and fields can be defined in the files. However, the files need to be consistent with each other - you cannot define fields form datasets that are not defined in the datasets file.

### Custom dataset definitions

The first contains the definitions of the datasets you want to create. It should have following structure:

| workspace_id         | dataset_id        | dataset_name         | dataset_datasource_id | dataset_source_table | dataset_source_sql | parent_dataset_reference | parent_dataset_reference_attribute_id | dataset_reference_source_colum | wdf_id |
| -------------------- | ----------------- | -------------------- | --------------------- | -------------------- | ------------------ | ------------------------ | ------------------------------------- | ------------------------------ | ------ |
| child_workspace_id_1 | custom_dataset_id | Custom Dataset Title | datasource_id         | dataset_source_table |                    | parent_dataset_id        | parent_dataset.reference_field        | custom_dataset.reference_field | wdf_id |

#### Validity constraints

- The `dataset_source_table` and `dataset_source_sql` are mutually exclusive. Only one of those should be filled in, the other should be null (empty value). In case both values are present, the script will throw an error.

- `workspace_id` + `dataset_id` combination must be unique across all dataset definitions.

#### JSON representation

For readability, I include the data structure in JSON with comments. However, note that the script will only work with CSV files!

```json
{
  "workspace_id": "child_workspace_id_1", // child workspace id
  "dataset_id": "custom_dataset_id", // custom dataset id
  "dataset_name": "Custom Dataset Title", // custom dataset name
  "dataset_datasource_id": "datasource_id", // data source id -> in the UI, you see it when you go to "manage files"
  "dataset_source_table": "dataset_source_table", // the name of the table in the physical data model
  "dataset_source_sql": null, // SQL query defining the dataset
  "parent_dataset_reference": "products", // ID of the parent dataset to which the custom one will be connected
  "parent_dataset_reference_attribute_id": "products.product_id", // parent dataset column name used fot the "join"
  "dataset_reference_source_colum": "product_id", // custom dataset column name used for the "join"
  "wdf_id": "x__client_id" // workspace data filter id
}
```

### Custom fields definition

The individual files of the custom dataset are defined thusly:

| workspace_id         | dataset_id        | cf_id           | cf_name           | cf_type   | cf_source_column           | cf_source_column_data_type |
| -------------------- | ----------------- | --------------- | ----------------- | --------- | -------------------------- | -------------------------- |
| child_workspace_id_1 | custom_dataset_id | custom_field_id | Custom Field Name | attribute | custom_field_source_column | INT                        |

#### Validity constraints

The custom field definitions must comply with these criteria:

- **attributes** and **facts**: unique `workspace_id` + `cf_id` combinations
- **dates**: unique `dataset_id` and `cf_id` combinations

#### JSON representation

Again, here is a JSON definition with comments for readability:

```json
{
  "workspace_id": "child_workspace_id_1", // child workspace ID
  "dataset_id": "custom_dataset_id", // custom dataset ID
  "cf_id": "custom_field_id", // custom field ID
  "cf_name": "Custom Field Name", // custom field name
  "cf_type": "attribute", // GoodData type of the field*
  "cf_source_column": "custom_field_source_column", // name of the column in the physical data model
  "cf_source_column_data_type": "INT" // data type of the field*
}
```

\* Supported values of **_cf_type_** and **_cf_source_column_data_type_** are listed in `CustomFieldType` and `ColumnDataType` enums in [models](../scripts/custom_fields/models/custom_data_object.py)

## Usage

Now that your environment and input files are set up, let's have a look at how to run the script 🚀.

The script takes two positional arguments, which represent the paths to the input files we have discussed above.

```shell
python scripts/custom_fields.py custom_datasets.csv custom_fields.csv
```

There is also an optional flag: `--no-relations-check`. It's meaning is discussed in the next section.

### Check valid relations

Regardless of whether the flag is used or not, the script will always start by loading and validating the data from the provided files. The script will then iterate through workspaces.

#### If unused

If `--no-relations-check` is not used, the script will:

1. Store current workspace layout (analytical objects and LDM).
1. Check whether relations of metrics, visualizations and dashboards are valid. A set of current objects with invalid relations is created.
1. Push the updated LDM to GoodData Cloud.
1. Check object relations again. New set of objects with invalid relations is created.
1. The sets are compared.
   - If there is more objects with invalid references in the new set, it means the objects were invalidated. Rollback is required.
   - If the sets are not equal, rollback might be required
   - If there is fewer invalid references or the sets are equal, rollback is not required
1. In case rollback is required, the initally stored workspace layout will be pushed to GoodData Cloud again, reverting changes to the workspace.

#### If used

If you decide to use the `--no-relations-check` flag, the script will simply validate the data and push the LDM extension to GoodData Cloud without any additional checks or rollbacks.

```shell
python scripts/custom_fields.py custom_datasets.csv custom_fields.csv --no-relations-check
```
