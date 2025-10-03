# GD User Data Filter Management

Tool which helps manage User Data Filters in a GoodData organization.

User Data Filters can be created, updated, and deleted based on CSV input.

## Usage

The tool requires the following arguments on input:

- `filepath` - a path to a csv file defining user data filters, their values, and target workspace
- `ldm_column_name` - LDM column name
- `maql_column_name` - MAQL column name in the form `{attribute/dataset.field}`

Some other, _optional_, arguments are:

- `-d | --delimiter` - column delimiter for the csv files. Use this to define how the csv is parsed. Default value is `,`
- `-q | --quotechar` - quotation character used to escape special characters (such as the delimiter) within the column field value. Default value is `"` If you need to escape the quotechar itself, you have to embed it in quotechars and then double the quotation character (e.g.: `"some""string"` will yield `some"string`).
- `-p | --profile-config` - optional path to GoodData profile config. If no path is provided, the default profiles file is used.
- `--profile` - GoodData profile to use. If no profile is provided, `default` is used.

Use the tool like so:

```sh
python scripts/user_data_filter_mgmt.py path/to/udfs.csv ldm_column_name maql_column_name
```

If you would like to define custom delimiters, use the tool like so:

```sh
python scripts/user_data_filter_mgmt.py path/to/udfs.csv ldm_column_name maql_column_name -d ","
```

To show the help for using arguments, call:

```sh
python scripts/user_data_filter_mgmt.py -h
```

## Input CSV file

The input CSV file defines the user data filter values to be managed. All user data filters in all workspaces listed in the input will be overwritten based on the CSV content.

Following format of the csv is expected:

| workspace_id              | udf_id    | udf_value |
| ------------------------- | --------- | --------- |
| workspace_with_wdf_values | user_id_1 | 1         |
| workspace_with_wdf_values | user_id_2 | 2         |

Here, each `workspace_id` is the ID of the workspace where the user data filter applies.

The `user_data_filter_id` identifies the specific User Data Filter you want to assign or update for the given workspace. Should be equal to the ID of the user the UDF is applied to.

The `udf_value` field specifies the value to be set for that User Data Filter.
