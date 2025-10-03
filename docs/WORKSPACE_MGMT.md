# GD Workspace Management

Tool which helps manage child workspace entities in an GoodData organization.

Workspaces can be created, updated, and deleted. This includes applying Workspace Data Filter values, when provided in input.

## Usage

The tool requires the following argument on input:

- `filepath` - a path to a csv file defining workspace entities, their relevant attributes, workspace data filter configuration, and isActive state

Some other, _optional_, arguments are:

- `-d | --delimiter` - column delimiter for the csv files. Use this to define how the csv is parsed. Default value is `,`
- `-i | --inner-delimiter` - Workspace Data Filter values column delimiter. Use this to separate the different values defined in the `workspace_data_filter_values` column. Default value is `|`. Note that `--delimiter` and `--inner_delimiter` have to differ.
- `-q | --quotechar` - quotation character used to escape special characters (such as the delimiter) within the column field value. Default value is `"` If you need to escape the quotechar itself, you have to embed it in quotechars and then double the quotation character (e.g.: `"some""string"` will yield `some"string`).
- `-p | --profile-config` - optional path to GoodData profile config. If no path is provided, the default profiles file is used.
- `--profile` - GoodData profile to use. If no profile is provided, `default` is used.

Use the tool like so:

```sh
python scripts/workspace_mgmt.py path/to/workspace_definitions.csv
```

If you would like to define custom delimiters, use the tool like so:

```sh
python scripts/workspace_mgmt.py path/to/workspace_definitions.csv -d "," -i "|"
```

To show the help for using arguments, call:

```sh
python scripts/workspace_mgmt.py -h
```

## Input CSV file

The input CSV file defines the workspace entities which you might want to manage. Note that GD organization workspaces that are not defined in the input will not be modified in any way.

Following format of the csv is expected:

| parent_id           | workspace_id                 | workspace_name               | workspace_data_filter_id | workspace_data_filter_values | is_active |
| ------------------- | ---------------------------- | ---------------------------- | ------------------------ | ---------------------------- | --------- |
| parent_workspace_id | workspace_with_wdf_values    | Workspace With WDF Values    | wdf_id                   | 1&#124;2&#124;3              | true      |
| parent_workspace_id | workspace_without_wdf_values | Workspace Without WDF Values |                          |                              | true      |

Here, each `workspace_id` is the ID of the workspace to manage.

The `parent_id` specifies the parent workspace under which the workspace should be placed.

The `workspace_name` field specifies the display name of the workspace.

The `workspace_data_filter_id` and `workspace_data_filter_values` fields specify Workspace Data Filter configuration. Leave `workspace_data_filter_values` empty if no values should be set.

Lastly, the `is_active` field holds boolean values containing information about whether the workspace should or should not exist in the organization.
