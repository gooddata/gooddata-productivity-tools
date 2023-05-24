# GD User Group Management
This tool facilitates the management of user groups within a GoodData organization. It supports the creation, updating, and deletion of user groups, including the assignment of parent user groups as defined in the input details.

## Usage

The tool requires the following argument:
- `user_group_csv` - a path to a CSV file that defines the user groups, their names, parent user groups, and active status.

Optional arguments include:
- `-d | --delimiter` - column delimiter for the CSV files. This defines how the CSV is parsed. The default value is "`,`".
- `-u | --ug_delimiter` - delimiter used to separate different parent user groups within the parent user group column. This must differ from the "delimiter" argument. Default value is "`|`".
- `-q | --quotechar` - quotation character used to escape special characters (such as the delimiter) within the column values. The default value is '`"`'. If you need to escape the quotechar itself, you have to embed it in quotechars and then double the quotation character (e.g.: `"some""string"` will yield `some"string`).

Use the tool like so:
```sh
python scripts/user_group_mgmt.py user_group_csv
```
Where `user_group_csv` refers to the input CSV file.

For custom delimiters, use the command:
```sh
python scripts/user_group_mgmt.py user_group_csv -d "," -u "|"
```

To display help for using arguments, run:
```sh
python scripts/user_group_mgmt.py -h
```

## Input CSV File (`user_group_csv`)
The input CSV file defines the user groups to be managed. User groups not defined in the input file will not be modified.

[Example input CSV.](examples/user_group_mgmt/input.csv)

Expected CSV format:

| user_group_id  | user_group_name  | parent_user_groups | is_active |
|----------------|------------------|--------------------|-----------|
| ug_1           | Admins           |                    | True      |
| ug_2           | Developers       | ug_1               | True      |
| ug_3           | Testers          | ug_1, ug_2         | True      |
| ug_4           | TemporaryAccess  | ug_2               | False     |

Here, each `user_group_id` is the unique identifier for the user group.

The `user_group_name` field is an optional name for the user group, defaulting to the ID if not provided.

The `parent_user_groups` field specifies the parent user groups, defining hierarchical relationships.

The `is_active` field contains information about whether the user group should exist or be deleted from the organization. The `is_active` field is case-insensitive, recognizing `true` as the only affirmative value. Any other value is considered negative (e.g., `no` would evaluate to `False`).

This documentation provides a comprehensive guide to using the GD User Group Management tool effectively within your GoodData organization.